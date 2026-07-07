package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP documents a convergence hole in the G3 ctl refresh
// path. manifestBytes() intentionally serves a cached body for up to 30s. If roster-pull returns that
// still-valid but non-advancing manifest, refreshCtlEndpoints treats NATS as successful, never consults
// the HTTP bootstrap fallback, writes FetchedAt, and suppresses the next refresh for ctlRefreshTTL.
func TestG3ExternalReviewStaleNATSDoesNotMaskFreshHTTP(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	seed, pub := cliExternalAccount(t)
	actor := "Uactor-stale-nats"
	staleBody := g3SignedManifestBytes(t, seed, pub, 1)
	sub, err := nc.Subscribe(proto.SubjCtrlClusterRoster(actor), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(staleBody)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	freshBody := g3SignedManifestBytes(t, seed, pub, 7)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(freshBody)
	}))
	defer srv.Close()

	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: pub,
		FloorURL:      "wss://b:443",
		BootstrapURL:  srv.URL,
		RosterGen:     1,
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	refreshCtlEndpoints(context.Background(), nc, home, "wss://b:443", actor)
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil {
		t.Fatal("cache disappeared")
	}
	if ce.RosterGen != 7 {
		t.Fatalf("stale NATS manifest must not mask a fresher HTTP bootstrap manifest; RosterGen=%d, want 7 (FetchedAt=%q)", ce.RosterGen, ce.FetchedAt)
	}
}
