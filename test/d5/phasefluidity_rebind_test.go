//go:build d5_integration

package d5_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// pfSeedSelfRoster commits a self roster row so SetRaftAddr's requireClusterNode passes (the rebind
// updates cluster_nodes.raft_addr; without a row it would be a no-op that still fails the precheck).
func pfSeedSelfRoster(t *testing.T, nd *cluster.Node, id string) {
	t.Helper()
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	nonce := "pf-" + id
	sig, _ := auth.SignWithSeed(seed, cluster.JoinSignBytes(id, pub, nonce))
	in := cluster.ClusterNodeUpsertInput{
		NodeID: id, Name: id, NodeIdentPub: pub, NatsServerID: "tether-" + id,
		RaftAddr: "127.0.0.1:7400", NatsRoute: "nats://127.0.0.1:6222", TunnelAddr: "127.0.0.1:7000",
		PublicHost: "h", CertFP: "sha256:ab", JoinNonce: nonce, JoinSigHex: sigHex(sig), Now: time.Now(),
	}
	if err := nd.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) }); err != nil {
		t.Fatalf("seed self roster: %v", err)
	}
}

func sigHex(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2], out[i*2+1] = hexd[c>>4], hexd[c&0xf]
	}
	return string(out)
}

// TestPhaseFluidityMTLSRebind (external-review R5) proves set-raft-addr's in-place AddVoter rebind on a
// REAL mTLS NetworkTransport (not InmemTransport): a lone mTLS voter rebinds its advertise ONLINE
// (in-place AddVoter at quorum=1, no wipe) and stays writable; and the F5 capability gate fails CLOSED
// on a real 2-node mTLS cluster when no health proof confirms the peer supports the ops.
func TestPhaseFluidityMTLSRebind(t *testing.T) {
	// (1) lone real-mTLS voter: online advertise rebind + still writable.
	ca := newRouteCA(t)
	tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.clusterLeaf(t)})
	if err != nil {
		t.Fatalf("mtls transport: %v", err)
	}
	dir := t.TempDir()
	nd, err := cluster.New(cluster.Config{
		LocalID: "pf-mtls", DataDir: dir, DBPath: filepath.Join(dir, "state.db"), Transport: tr,
		BootstrapPeers:   []raft.Server{{Suffrage: raft.Voter, ID: "pf-mtls", Address: tr.LocalAddr()}},
		HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
		LeaderLeaseTimeout: 75 * time.Millisecond, ApplyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("mtls node: %v", err)
	}
	t.Cleanup(func() { _ = nd.Shutdown() })
	if err := nd.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("mtls leader: %v", err)
	}
	pfSeedSelfRoster(t, nd, "pf-mtls")

	admin := broker.NewClusterAdmin(nd, nil)
	if err := admin.SetRaftAddr("", "203.0.113.7:7400", false); err != nil {
		t.Fatalf("SetRaftAddr on a real mTLS transport: %v", err)
	}
	cfg, err := nd.RaftConfiguration()
	if err != nil {
		t.Fatalf("raft config: %v", err)
	}
	got := ""
	for _, s := range cfg {
		if s.NodeID == "pf-mtls" {
			got = s.Addr
		}
	}
	if got != "203.0.113.7:7400" {
		t.Fatalf("real-mTLS rebind did not rewrite the config advertise in place: %q", got)
	}
	var col string
	_ = nd.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT raft_addr FROM cluster_nodes WHERE node_id='pf-mtls'`).Scan(&col)
	})
	if col != "203.0.113.7:7400" {
		t.Fatalf("roster raft_addr not updated to match the advertise: %q", col)
	}
	if err := nd.ApplyMetaSet("t:pf-mtls-postrebind", "ok"); err != nil {
		t.Fatalf("cluster not writable after a real-mTLS advertise rebind: %v", err)
	}

	// (2) real 2-node mTLS cluster: the F5 capability gate fails CLOSED with no health proof (the peer's
	// support for the v0.4.2 ops cannot be confirmed) — refusing to fork a mixed-version replica.
	c := startCluster5(t, 2)
	leader := c.nodes[0]
	for i := 0; i < 40 && !leader.IsLeader(); i++ {
		if c.nodes[1].IsLeader() {
			leader = c.nodes[1]
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !leader.IsLeader() {
		t.Fatal("no d5 leader for the gate sub-case")
	}
	pfSeedSelfRoster(t, leader, leaderID(leader))
	gateAdmin := broker.NewClusterAdmin(leader, nil) // healthPoll nil ⇒ no proof for the peer
	if err := gateAdmin.SetRaftAddr("", "198.51.100.9:7400", false); err == nil {
		t.Fatal("F5 gate must fail CLOSED on a real 2-node cluster with no health proof for the peer voter")
	}
}

func leaderID(nd *cluster.Node) string {
	_, id := nd.LeaderWithID()
	return id
}
