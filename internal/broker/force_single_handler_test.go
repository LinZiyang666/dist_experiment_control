package broker

import (
	"database/sql"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// force_single_handler_test.go — external-review F4: handler-level coverage of the online force-single
// arm/commit handlers driven through a REAL cluster.Node with cluster_nodes roster rows (self + a peer),
// not just the isolated dwell/token unit tests. Exercises: dry-run on a healthy (not-quorum-lost)
// cluster, the F2 self-id mismatch refuse, the peer-alive HARD-REFUSE, the commit-without-token refuse,
// and the full arm->commit success that persists force_single_active + the recovery epoch (F1).

// fsTestBackend builds a single-node cluster.Node (inmem raft + an inmem TransportFactory so the online
// recover can rebuild), admits self into cluster_nodes, and inserts ONE peer row (PENDING, no AddVoter)
// with caller-controlled addrs. Returns a minimal clusterAdminBackend wired with the node + an fsArm.
func fsTestBackend(t *testing.T, selfID, peerID, peerRaftAddr string) (*clusterAdminBackend, *cluster.Node) {
	t.Helper()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(selfID))
	dir := t.TempDir()
	n, err := cluster.New(cluster.Config{
		LocalID:            raft.ServerID(selfID),
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          trans,
		ApplyTimeout:       30 * time.Second,
		HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
		ElectionTimeout:    cluster.MultinodeElectionTimeout,
		LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
		// inmem rebuild bound to selfID so the recovered {self} config (raft_addr == selfID) elects.
		TransportFactory: func() (raft.Transport, error) {
			_, tr := raft.NewInmemTransport(raft.ServerAddress(selfID))
			return tr, nil
		},
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	admin := NewClusterAdmin(n, nil)
	// self row: AddVoter(self) is idempotent on the bootstrapped single node; raft_addr == selfID.
	selfIn := d7JoinInput(t, selfID, selfID)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(selfIn, selfID, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode self: %v", err)
	}
	// peer row: a node-upsert (PENDING; no AddVoter), with controllable addrs. The join PoP signs only
	// (node_id, pub, nonce), so overriding the addrs after d7JoinInput does NOT invalidate the sig.
	peerIn := d7JoinInput(t, peerID, peerRaftAddr)
	peerIn.NatsRoute = "127.0.0.1:1" // refused instantly (port 1 not listening) — fast "dead"
	peerIn.TunnelAddr = "127.0.0.1:1"
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(peerIn) }); err != nil {
		t.Fatalf("insert peer row: %v", err)
	}
	// EXTERNAL review B1: production always has a ClusterDataDir (it is where raft lives, so a
	// cluster-mode broker cannot exist without one), and the commit path now refuses to start an
	// irreversible rewrite it could not first record an intent for. Give the fixture the same.
	admin.dataDir = t.TempDir()
	return &clusterAdminBackend{admin: admin, fsArm: newForceSingleArm()}, n
}

// markQuorumLostPastDwell drives the dwell state machine to "continuously quorum-lost past the dwell".
func markQuorumLostPastDwell(b *clusterAdminBackend) {
	b.fsArm.observeLeadership(true, time.Now().Add(-time.Hour))
}

// TestForceSingleHandlerDryRunOnHealthyCluster: a --dry-run arm on a cluster that still HAS leader
// contact must NOT error — it returns OK with WouldProceed=false + a reason (the drill is runnable on a
// healthy cluster). This pins the dry-run semantics the external review questioned.
func TestForceSingleHandlerDryRunOnHealthyCluster(t *testing.T) {
	b, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	// no observeLeadership(true) => still has leader contact (not quorum-lost)
	resp := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: "brk-a", ConfirmPeersDead: []string{"brk-b"}, DryRun: true,
	})
	if !resp.OK {
		t.Fatalf("dry-run on a healthy cluster must return OK (a drill), got Code=%q err=%q", resp.Code, resp.Error)
	}
	if resp.ForceSingle == nil || resp.ForceSingle.WouldProceed {
		t.Fatalf("dry-run on a healthy cluster must report WouldProceed=false, got %+v", resp.ForceSingle)
	}
	if resp.ForceSingle.Reason == "" {
		t.Fatal("dry-run that would not proceed must carry a reason")
	}
	if resp.ForceSingle.ArmToken != "" {
		t.Fatal("a dry-run must NOT mint an arm token")
	}
	if resp.ForceSingle.BrokerSelfID != "brk-a" {
		t.Fatalf("report must carry the broker self id, got %q", resp.ForceSingle.BrokerSelfID)
	}
}

// TestForceSingleHandlerSelfIDMismatchRefused (F2): a mistyped --self-id (req.NodeID) that does not
// match the socket owner must be refused — even with all other gates satisfiable.
func TestForceSingleHandlerSelfIDMismatchRefused(t *testing.T) {
	b, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	markQuorumLostPastDwell(b)
	arm := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: "brk-WRONG", ConfirmPeersDead: []string{"brk-b"},
	})
	if arm.OK || arm.Code != adminsock.CodeBadRequest {
		t.Fatalf("a mismatched --self-id must be refused with CodeBadRequest, got OK=%v Code=%q", arm.OK, arm.Code)
	}
	commit := b.handleForceSingleCommit(adminsock.Request{
		Op: adminsock.OpClusterForceSingleCommit, NodeID: "brk-WRONG", ArmToken: "x", ConfirmPeersDead: []string{"brk-b"},
	})
	if commit.OK || commit.Code != adminsock.CodeBadRequest {
		t.Fatalf("commit with a mismatched --self-id must be refused with CodeBadRequest, got OK=%v Code=%q", commit.OK, commit.Code)
	}
}

// TestForceSingleHandlerPeerAliveRefused: a peer that answers a TCP probe (here a live listener on its
// raft_addr) must HARD-REFUSE the real (non-dry-run) arm with CodePeerAlive, and the report must mark
// that peer Alive with the port that answered.
func TestForceSingleHandlerPeerAliveRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	b, _ := fsTestBackend(t, "brk-a", "brk-b", ln.Addr().String()) // peer raft_addr is ALIVE
	markQuorumLostPastDwell(b)
	resp := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: "brk-a", ConfirmPeersDead: []string{"brk-b"},
	})
	if resp.OK || resp.Code != adminsock.CodePeerAlive {
		t.Fatalf("a live peer must HARD-REFUSE with CodePeerAlive, got OK=%v Code=%q err=%q", resp.OK, resp.Code, resp.Error)
	}
	if resp.ForceSingle == nil {
		t.Fatal("a peer-alive refusal must still carry the report")
	}
	var sawAlive bool
	for _, p := range resp.ForceSingle.Peers {
		if p.NodeID == "brk-b" && p.Alive && p.OnPort != "" {
			sawAlive = true
		}
	}
	if !sawAlive {
		t.Fatalf("report must mark brk-b Alive with the answering port, got %+v", resp.ForceSingle.Peers)
	}
}

// TestForceSingleHandlerCommitWithoutTokenRefused: a lone commit (no prior arm token) is refused.
func TestForceSingleHandlerCommitWithoutTokenRefused(t *testing.T) {
	b, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	markQuorumLostPastDwell(b)
	resp := b.handleForceSingleCommit(adminsock.Request{
		Op: adminsock.OpClusterForceSingleCommit, NodeID: "brk-a", ArmToken: "", ConfirmPeersDead: []string{"brk-b"},
	})
	if resp.OK || resp.Code != adminsock.CodeArmExpired {
		t.Fatalf("a commit with no fresh arm token must be refused with CodeArmExpired, got OK=%v Code=%q", resp.OK, resp.Code)
	}
}

// TestForceSingleHandlerArmCommitPersistsMarkerAndEpoch (F1): the full arm->commit success path with a
// dead peer must do the in-process recover AND persist force_single_active + the recovery epoch (the
// commit success boundary includes the marker — a missing marker = a writable broker with no visible
// emergency).
func TestForceSingleHandlerArmCommitPersistsMarkerAndEpoch(t *testing.T) {
	b, n := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1") // peer dead on all probed ports
	markQuorumLostPastDwell(b)

	arm := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: "brk-a", ConfirmPeersDead: []string{"brk-b"},
	})
	if !arm.OK || arm.ForceSingle == nil || arm.ForceSingle.ArmToken == "" {
		t.Fatalf("arm with a dead peer past dwell must succeed + mint a token, got OK=%v Code=%q err=%q", arm.OK, arm.Code, arm.Error)
	}
	if !arm.ForceSingle.WouldProceed {
		t.Fatal("arm that minted a token must report WouldProceed=true")
	}

	commit := b.handleForceSingleCommit(adminsock.Request{
		Op: adminsock.OpClusterForceSingleCommit, NodeID: "brk-a", ArmToken: arm.ForceSingle.ArmToken, ConfirmPeersDead: []string{"brk-b"},
	})
	if !commit.OK {
		t.Fatalf("commit must succeed, got Code=%q err=%q", commit.Code, commit.Error)
	}
	if commit.ForceSingle == nil || len(commit.ForceSingle.Abandoned) != 1 || commit.ForceSingle.Abandoned[0] != "brk-b" {
		t.Fatalf("commit must report the abandoned peer, got %+v", commit.ForceSingle)
	}

	// F1: the marker + epoch are persisted (part of the success boundary).
	var marker, epoch string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, cluster.MetaKeyForceSingle).Scan(&marker); err != nil {
		t.Fatalf("force_single_active not persisted after a reported-success commit: %v", err)
	}
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, cluster.MetaKeyForceSingleEpoch).Scan(&epoch); err != nil || epoch == "" {
		t.Fatalf("recovery epoch not persisted after a reported-success commit: epoch=%q err=%v", epoch, err)
	}
}
