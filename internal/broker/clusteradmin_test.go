package broker

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// clusteradmin_test.go — D7 §8.1 cheap orchestrator unit tests (make test) on a
// SINGLE-node raft: AddVoter(self) is idempotent, so the full two-phase AddNode
// walks self PENDING->CATCHING_UP->VOTER, ReconcileMembershipOnLeadership
// forward-completes a PENDING/VOTER_ADD_FAILED voter, a never-catching-up probe
// derives catch_up_stalled, a bare remove of a live VOTER is refused (no fork), and
// draining the only/leader voter bails (no half-drain). The MULTI-node drills
// (real-follower replication, forged-sig-on-follower, the {PENDING∧voter}
// reconcile, drain+retire, drain-leader transfer, force-single→recover) are gated
// d7_integration (test/d7 TestD7Matrix).

func d7SingleNode(t *testing.T, id string) (*cluster.Node, string) {
	t.Helper()
	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	dir := t.TempDir()
	n, err := cluster.New(cluster.Config{
		LocalID:            raft.ServerID(id),
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          trans,
		ApplyTimeout:       30 * time.Second,
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	return n, string(addr)
}

// TestB3RemoveForceRespectsPhaseGate (B3 review M2, the load-bearing safety property): --force on
// `cluster remove` bypasses ONLY the new expose-ownership probe — it must NOT weaken the raft
// phase-gate. A live VOTER (not RETIRING / VOTER_ADD_FAILED) is refused even WITH force=true.
func TestB3RemoveForceRespectsPhaseGate(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	// Unknown roster node → "no such roster node" (force irrelevant).
	if err := admin.RemoveNode("ghost", true); err == nil || !strings.Contains(err.Error(), "no such roster node") {
		t.Fatalf("remove of an unknown node: got %v", err)
	}
	// Admit single-1 → VOTER, then remove --force: must STILL hit the phase-gate (a live VOTER is
	// refused regardless of --force — the D7 no-silent-fork guarantee).
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := admin.RemoveNode("single-1", true) // force=true
	if err == nil || !strings.Contains(err.Error(), "raw remove only finishes") {
		t.Fatalf("remove --force of a live VOTER must hit the phase-gate, got %v", err)
	}
	// And it must NOT be the ownership-probe error (the phase-gate precedes + returns first).
	if strings.Contains(err.Error(), "still HOMES") {
		t.Error("a live VOTER must hit the phase-gate, never the ownership probe")
	}
}

func d7JoinInput(t *testing.T, nodeID, realRaftAddr string) cluster.ClusterNodeUpsertInput {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatalf("pub: %v", err)
	}
	nonce := "nonce-" + nodeID
	sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return cluster.ClusterNodeUpsertInput{
		NodeID:       nodeID,
		Name:         nodeID,
		NodeIdentPub: pub,
		NatsServerID: "tether-" + nodeID,
		RaftAddr:     realRaftAddr,
		NatsRoute:    "nats://10.0.0.1:6222",
		TunnelAddr:   "10.0.0.1:7000",
		PublicHost:   "host.example",
		CertFP:       "sha256:ab",
		JoinNonce:    nonce,
		JoinSigHex:   hex.EncodeToString(sig),
		Now:          time.Now(),
	}
}

func d7ReadPhase(t *testing.T, n *cluster.Node, nodeID string) (string, bool) {
	t.Helper()
	var phase string
	var found bool
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		err := db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&phase)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	}); err != nil {
		t.Fatalf("read phase: %v", err)
	}
	return phase, found
}

func TestD7AddNodeWalksToVoter(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr) // realRaftAddr == addr => AddVoter(self) is a no-op
	caughtUp := func(barrier uint64) (bool, error) {
		cur, err := n.AppliedIndex()
		return cur >= barrier, err
	}
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	phase, ok := d7ReadPhase(t, n, "single-1")
	if !ok || phase != phaseVoter {
		t.Fatalf("want phase VOTER, got %q (found=%v)", phase, ok)
	}
}

func TestD7ReconcilePendingVoterForwardCompletes(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	// Phase-1 only: roster row lands at PENDING, but the node is already a raft voter
	// (single-node bootstrap) — exactly the {PENDING ∧ raft-voter} post-crash state.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(in)
	}); err != nil {
		t.Fatalf("phase-1 upsert: %v", err)
	}
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phasePending {
		t.Fatalf("setup: want PENDING, got %q", phase)
	}
	if err := admin.ReconcileMembershipOnLeadership(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseCatchingUp {
		t.Fatalf("reconcile did not forward-complete PENDING->CATCHING_UP, got %q", phase)
	}
}

// TestD7RemoveRefusesLiveVoter (review B1): a bare remove of a healthy VOTER must be
// REFUSED (it would drop the raft voter but leave the roster stuck — a silent fork).
func TestD7RemoveRefusesLiveVoter(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseVoter {
		t.Fatalf("setup: want VOTER, got %q", phase)
	}
	err := admin.RemoveNode("single-1", false)
	if err == nil || !strings.Contains(err.Error(), "raw remove only finishes") {
		t.Fatalf("bare remove of a live VOTER must be refused, got %v", err)
	}
	// The roster row is untouched (no silent fork created).
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseVoter {
		t.Fatalf("refused remove must leave the roster intact, got %q", phase)
	}
}

// TestD7ReconcileVoterAddFailedForwardCorrects (review M6): a roster row at
// VOTER_ADD_FAILED whose node IS a raft voter (AddVoter timed out ≠ failed) must be
// forward-corrected to CATCHING_UP by the leader-startup reconciliation pass.
func TestD7ReconcileVoterAddFailedForwardCorrects(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) }); err != nil {
		t.Fatalf("phase-1: %v", err)
	}
	// Force the row into VOTER_ADD_FAILED (the half-success the reconcile heals).
	if err := admin.setPhase("single-1", phaseAddFailed, []string{phasePending}, "injected"); err != nil {
		t.Fatalf("set VOTER_ADD_FAILED: %v", err)
	}
	if err := admin.ReconcileMembershipOnLeadership(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseCatchingUp {
		t.Fatalf("reconcile did not forward-correct VOTER_ADD_FAILED->CATCHING_UP, got %q", phase)
	}
}

// TestD7DrainSelfLeaderDoesNotHalfDrain (review B5): draining the only voter (which
// is the leader) must bail at the leadership transfer (no other voter) — it must NOT
// raise the marker / migrate / half-advance the phase.
func TestD7DrainSelfLeaderDoesNotHalfDrain(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// Plain drain of the only voter (which is the leader): transferLeadershipOff finds
	// no target -> error; the phase must stay VOTER (no half-drain).
	err := admin.DrainNode("single-1", false, true, time.Now(), nil)
	if err == nil || !strings.Contains(err.Error(), "transfer leadership") {
		t.Fatalf("draining the only/leader voter should fail at leadership transfer, got %v", err)
	}
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseVoter {
		t.Fatalf("self-leader drain must NOT advance the phase, got %q", phase)
	}
}

func TestD7DrainUnknownNodeDoesNotRaiseMarker(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := admin.DrainNode("ghost", false, true, time.Now(), nil)
	if err == nil || !strings.Contains(err.Error(), "not in cluster_nodes") {
		t.Fatalf("drain unknown node should fail before marker, got %v", err)
	}
	var markers int
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM cluster_meta WHERE key='draining:ghost'`).Scan(&markers)
	}); err != nil {
		t.Fatalf("read markers: %v", err)
	}
	if markers != 0 {
		t.Fatalf("unknown drain raised marker rows=%d", markers)
	}
}

// TestD7TransferLeaderToRejectsBadTargets (external review F1): the targeted
// transfer rejects self / unknown / non-voter rather than silently transferring to
// some other peer.
func TestD7TransferLeaderToRejectsBadTargets(t *testing.T) {
	n, _ := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	if err := admin.TransferLeaderTo("single-1"); err == nil || !strings.Contains(err.Error(), "this node") {
		t.Fatalf("transfer to self must be rejected, got %v", err)
	}
	if err := admin.TransferLeaderTo("ghost"); err == nil || !strings.Contains(err.Error(), "not in the raft configuration") {
		t.Fatalf("transfer to an unknown node must be rejected, got %v", err)
	}
}

// TestD7RotateTunnelCertUpdatesPins (external review F2): rotation installs the new
// fp as cert_fp, moves the old to cert_fp_prev, and sets cert_fp_valid_until — a real
// implemented path, not a stub.
func TestD7RotateTunnelCertUpdatesPins(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	var hotSwapped bool
	admin.prepareTunnelCertRotate = func(newFP string) (func(), error) {
		if newFP != "sha256:NEW" {
			t.Fatalf("prepare fp = %q, want sha256:NEW", newFP)
		}
		return func() { hotSwapped = true }, nil
	}
	in := d7JoinInput(t, "single-1", addr) // CertFP defaults to "sha256:ab"
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := admin.RotateTunnelCert("single-1", "sha256:NEW", time.Hour); err != nil {
		t.Fatalf("RotateTunnelCert: %v", err)
	}
	var cur, prev string
	var validUntil sql.NullString
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT cert_fp, cert_fp_prev, cert_fp_valid_until FROM cluster_nodes WHERE node_id='single-1'`).Scan(&cur, &prev, &validUntil)
	}); err != nil {
		t.Fatalf("read pins: %v", err)
	}
	if cur != "sha256:NEW" {
		t.Fatalf("cert_fp = %q, want sha256:NEW", cur)
	}
	if prev != "sha256:ab" {
		t.Fatalf("cert_fp_prev = %q, want the old sha256:ab", prev)
	}
	if !validUntil.Valid || validUntil.String == "" {
		t.Fatal("cert_fp_valid_until not set on rotation")
	}
	if !hotSwapped {
		t.Fatal("cert rotation committed DB pins but did not hot-swap the live tunnel cert")
	}
}

func TestD9RotateTunnelCertRejectsRemoteTarget(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := admin.RotateTunnelCert("other-1", "sha256:NEW", time.Hour)
	if err == nil || !strings.Contains(err.Error(), "transfer leadership") {
		t.Fatalf("remote cert rotation must be rejected, got %v", err)
	}
}

func TestD7CatchUpStalled(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	admin.catchPoll = 5 * time.Millisecond
	in := d7JoinInput(t, "single-1", addr)
	never := func(uint64) (bool, error) { return false, nil }
	err := admin.AddNode(in, addr, never, 40*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "catch_up_stalled") {
		t.Fatalf("want catch_up_stalled error, got %v", err)
	}
	// Phase STAYS CATCHING_UP (a 7th phase would fail the 0008 CHECK); the stall is in
	// voter_add_error.
	if phase, _ := d7ReadPhase(t, n, "single-1"); phase != phaseCatchingUp {
		t.Fatalf("want phase CATCHING_UP after stall, got %q", phase)
	}
	var ve sql.NullString
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT voter_add_error FROM cluster_nodes WHERE node_id=?`, "single-1").Scan(&ve)
	}); err != nil {
		t.Fatalf("read voter_add_error: %v", err)
	}
	if ve.String != "catch_up_stalled" {
		t.Fatalf("voter_add_error = %q, want catch_up_stalled", ve.String)
	}
}
