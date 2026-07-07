package main

import (
	"context"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
)

// 门A (#11 / G3 #17): refreshCtlEndpoints against an UNPINNED cache must NOT fetch the manifest nor
// populate a pin from it — the ctl trusts ONLY an out-of-band-established account_pub. The pin-gate returns
// before any NATS pull or HTTP fetch, so the cache stays unpinned (never TOFU off a network responder).
func TestRefreshCtlEndpointsNoHTTPTOFU(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: "", FloorURL: base, BootstrapURL: "http://127.0.0.1:1/never-dialed",
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), nil, home, base, "")
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil || ce == nil {
		t.Fatalf("read cache: %v", err)
	}
	if ce.PinAccountPub != "" {
		t.Fatalf("an unpinned cache must NOT be TOFU-populated; got pin=%q", ce.PinAccountPub)
	}
}

// G3 #17 改法一: gate B (FloorURL==base) is REMOVED. After an operator re-points broker_url (FloorURL=old,
// base=new), a refresh that adopts a signed manifest MUST migrate FloorURL to base so DialFor's expansion
// gate self-heals. (The cross-cluster guard now lives ONLY in DialFor's read path, not the refresh path.)
// The manifest is signed by the pinned account, so which URL delivered it is cryptographically irrelevant.
func TestRefreshCtlEndpointsSelfHealFloorAfterRepoint(t *testing.T) {
	seed, pub := cliExternalAccount(t)
	srv := cliExternalManifestServer(t, seed, pub, "wss://b1.example.com:443")
	defer srv.Close()
	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: pub, FloorURL: "wss://old:443", BootstrapURL: srv.URL,
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	// nil conn → the NATS roster-pull is skipped and the HTTP bootstrap fallback serves the manifest.
	refreshCtlEndpoints(context.Background(), nil, home, "wss://new:443", "")
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil {
		t.Fatal("cache disappeared")
	}
	if ce.FloorURL != "wss://new:443" {
		t.Errorf("改法一: FloorURL must migrate to base after an accepted refresh (self-heal), got %q", ce.FloorURL)
	}
	if ce.Roster == nil {
		t.Error("an accepted refresh must adopt the signed roster")
	}
}

// A rejected refresh (forged/foreign manifest) must leave the cache UNTOUCHED — no poison, and in
// particular FloorURL must NOT migrate on the reject path (only on accept).
func TestRefreshCtlEndpointsRejectLeavesCacheAndFloor(t *testing.T) {
	seed, pub := cliExternalAccount(t)
	_, otherPub := cliExternalAccount(t) // a DIFFERENT account than the cache is pinned to
	srv := cliExternalManifestServer(t, seed, pub, "wss://b1.example.com:443")
	defer srv.Close()
	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: otherPub, FloorURL: "wss://old:443", BootstrapURL: srv.URL,
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), nil, home, "wss://new:443", "")
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil || ce.FloorURL != "wss://old:443" {
		t.Fatalf("a rejected (foreign-signed) manifest must leave FloorURL untouched; cache=%+v", ce)
	}
	if ce.Roster != nil {
		t.Error("a rejected manifest must not populate the roster (no poison)")
	}
}
