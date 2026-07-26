package cluster

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// recover_online_mtls_test.go — external-review F4: the in-process force-single swap exercised with a
// REAL production mTLS transport rebuild (NewMTLSTransport), not just the inmem factory. This closes the
// gap that the recover_online_test.go unit tests left: there, transportFactory returned an inmem
// transport; here it rebuilds an actual TLS NetworkTransport the way NewProduction does (R1: re-bind the
// same RaftBind after the old listener closes, Go sets SO_REUSEADDR). Proves a real-transport recover
// elects + stays writable, and the rebuilt transport is the live one.

func TestRecoverToSelfOnlineRealMTLSTransportRebuild(t *testing.T) {
	ca := newTestCA(t)
	id := raft.ServerID("brk-mtls")
	dir := t.TempDir()

	// First (bootstrap) transport: a real mTLS NetworkTransport on an ephemeral port.
	tr0, err := NewMTLSTransport(MTLSTransportConfig{
		BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
	})
	if err != nil {
		t.Fatalf("bootstrap transport: %v", err)
	}
	bind := string(tr0.LocalAddr()) // the concrete bound addr; the factory re-binds THIS (the production R1 path)

	n, err := New(Config{
		LocalID:            id,
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          tr0,
		HeartbeatTimeout:   MultinodeHeartbeatTimeout,
		ElectionTimeout:    MultinodeElectionTimeout,
		LeaderLeaseTimeout: MultinodeLeaderLeaseTimeout,
		ApplyTimeout:       5 * time.Second,
		// The online-recover seam rebuilds a REAL mTLS transport on the same bind addr — exactly what
		// NewProduction wires, so this test covers the production rebuild, not an inmem stand-in.
		TransportFactory: func() (raft.Transport, error) {
			return NewMTLSTransport(MTLSTransportConfig{
				BindAddr: bind, CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
			})
		},
	})
	if err != nil {
		t.Fatalf("New (real mTLS): %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })

	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("initial leader (real mTLS): %v", err)
	}
	if err := n.ApplyMetaSet("t:mtls", "before"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// In-process recover with a REAL mTLS transport rebuild.
	if err := n.RecoverToSelfOnline(bind); err != nil {
		t.Fatalf("RecoverToSelfOnline (real mTLS rebuild): %v", err)
	}
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("not leader after real-mTLS recover: %v", err)
	}
	// data preserved across the real-transport swap
	var v string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:mtls'`).Scan(&v); err != nil || v != "before" {
		t.Fatalf("data lost across real-mTLS recover: got %q err=%v", v, err)
	}
	// writable WITHOUT a restart, over the rebuilt mTLS transport
	if err := n.ApplyMetaSet("t:mtls2", "after"); err != nil {
		t.Fatalf("not writable after real-mTLS recover: %v", err)
	}
}
