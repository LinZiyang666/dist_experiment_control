package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// g3_ctl_pull_test.go — G3 #17 改法二 ctl side: fetchManifestOverNATS pulls the signed manifest from the
// connected broker on the live conn, and returns nil (→ HTTP fallback) when no broker answers for this
// actor, so an old broker without the responder degrades gracefully instead of erroring.
func TestG3FetchManifestOverNATS(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	seed, pub := cliExternalAccount(t)
	now := time.Now().UTC()
	r, err := clusterroster.Build(seed, pub, 1,
		[]proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	s, err := clusterroster.BuildSeeds(seed, pub, 1, []string{"wss://b1.example.com:443"}, "",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: r, Seeds: s})
	sub, err := nc.Subscribe(proto.SubjCtrlClusterRoster("Uactor"), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(body)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	m := fetchManifestOverNATS(context.Background(), nc, "Uactor")
	if m == nil || m.Roster == nil || m.Seeds == nil {
		t.Fatal("expected a signed manifest over the NATS roster-pull")
	}
	if len(m.Roster.Brokers) != 1 || m.Roster.Brokers[0].PublicHost != "b1.example.com" {
		t.Errorf("wrong roster content: %+v", m.Roster)
	}

	// No responder for a DIFFERENT actor → nil (ctl falls back to HTTP). Short ctx so the test is fast.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if got := fetchManifestOverNATS(ctx, nc, "Uother"); got != nil {
		t.Error("no responder for this actor → must return nil (HTTP fallback), got a manifest")
	}
	// nil conn / empty actor → nil (no panic).
	if fetchManifestOverNATS(context.Background(), nil, "Uactor") != nil {
		t.Error("nil conn → nil")
	}
	if fetchManifestOverNATS(context.Background(), nc, "") != nil {
		t.Error("empty actor → nil")
	}
}

func g3SignedManifestBytes(t *testing.T, seed []byte, pub string, gen uint64) []byte {
	t.Helper()
	now := time.Now().UTC()
	r, err := clusterroster.Build(seed, pub, gen,
		[]proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	s, err := clusterroster.BuildSeeds(seed, pub, gen, []string{"wss://b1.example.com:443"}, "",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: r, Seeds: s})
	return body
}

// M4: 门A over a LIVE NATS responder — an UNPINNED ctl must NEVER TOFU-pin off a network responder's
// self-claimed account, even when a valid (rogue-account-signed) manifest is on the wire. The pin-gate
// early-returns BEFORE the pull. (The prior no-TOFU test used a nil conn + unreachable HTTP, so a removed
// gate would still pass; this drives a real responder.)
func TestRefreshCtlEndpointsGateAHoldsOverLiveNATS(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	rogueSeed, roguePub := cliExternalAccount(t) // an account the cache is NOT pinned to
	actor := "Uactor-live"
	body := g3SignedManifestBytes(t, rogueSeed, roguePub, 5)
	sub, err := nc.Subscribe(proto.SubjCtrlClusterRoster(actor), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(body)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{PinAccountPub: "", FloorURL: "wss://b:443"}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), nc, home, "wss://b:443", actor)
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil || ce.PinAccountPub != "" {
		t.Fatalf("门A: an unpinned ctl must NEVER TOFU-pin off a live NATS responder, got %+v", ce)
	}
	if ce.Roster != nil {
		t.Error("门A: an unpinned ctl must not adopt a roster over NATS")
	}
}

// M5: the 改法二 PRIMARY path end-to-end — a PINNED ctl adopts a higher-gen manifest pulled over the live
// NATS conn even when the HTTP bootstrap is unreachable (proving NATS-primary, not HTTP-fallback).
func TestRefreshCtlEndpointsAdoptsOverNATSPrimary(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	seed, pub := cliExternalAccount(t)
	actor := "Uactor-primary"
	body := g3SignedManifestBytes(t, seed, pub, 7)
	sub, err := nc.Subscribe(proto.SubjCtrlClusterRoster(actor), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(body)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	home := t.TempDir()
	// Pinned to account A; the bootstrap URL is unreachable so ONLY the NATS primary can succeed.
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: pub, FloorURL: "wss://b:443", BootstrapURL: "http://127.0.0.1:1/nope", RosterGen: 1,
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), nc, home, "wss://b:443", actor)
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil || ce.RosterGen != 7 {
		t.Fatalf("改法二 primary: RosterGen must advance to the NATS manifest gen 7, got %+v", ce)
	}
	if ce.Roster == nil {
		t.Error("改法二 primary: the roster must be adopted over NATS")
	}
	if ce.FetchedAt == "" {
		t.Error("改法二 primary: FetchedAt must be written on an accepted NATS refresh")
	}
}
