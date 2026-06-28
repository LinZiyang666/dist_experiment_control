package main

import (
	"context"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
)

// #11 NO HTTP-TOFU: refreshCtlEndpoints against an UNPINNED cache must NOT fetch the manifest nor populate
// a pin from it — the ctl trusts ONLY an out-of-band-established account_pub. The bogus BootstrapURL would
// error if ever dialed; the pin-gate returns before that, so the cache stays unpinned.
func TestRefreshCtlEndpointsNoHTTPTOFU(t *testing.T) {
	home := t.TempDir()
	base := "wss://primary:443"
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: "", FloorURL: base, BootstrapURL: "http://127.0.0.1:1/never-dialed",
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), home, base)
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil || ce == nil {
		t.Fatalf("read cache: %v", err)
	}
	if ce.PinAccountPub != "" {
		t.Fatalf("an unpinned cache must NOT be TOFU-populated over HTTP; got pin=%q", ce.PinAccountPub)
	}
}

// A refresh whose cache FloorURL does not match the connect base is a no-op (cross-cluster guard) — never
// writes through the wrong-cluster manifest.
func TestRefreshCtlEndpointsFloorMismatchNoop(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: "A", FloorURL: "wss://old:443", BootstrapURL: "http://127.0.0.1:1/never-dialed",
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	refreshCtlEndpoints(context.Background(), home, "wss://new:443")
	ce, _ := cli.ReadClusterEndpoints(home)
	if ce == nil || ce.FloorURL != "wss://old:443" {
		t.Fatalf("cross-cluster refresh must be a no-op; cache=%+v", ce)
	}
}
