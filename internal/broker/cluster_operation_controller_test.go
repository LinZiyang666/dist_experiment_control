package broker

import (
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// cluster_operation_controller_test.go (C4 Stage-C) — single-node FSM-backed tests of the operation
// controller's decision logic + the op-creation guards. The full multi-node kill-9 resume battery
// (BLOCKER-1/2 end-to-end: retire-after-removal, barrier-before-AddVoter) needs a clustered harness
// and is a gated test/c4 follow-up; these lock the gate logic + idempotency + idle-zero-writes here.

func makeJoinBundle(t *testing.T, nodeID, raftAddr, nonce string) (string, string) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := auth.PublicKeyFromSeed(seed)
	sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
	if err != nil {
		t.Fatal(err)
	}
	b := cluster.JoinBundle{
		NodeID: nodeID, Name: nodeID, NodeIdentPub: pub, NatsServerID: nodeID,
		RaftAddr: raftAddr, NatsRoute: "nats://" + raftAddr, TunnelAddr: nodeID + ":7000", // C8 D10: identity-complete
		JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig),
	}
	enc, err := cluster.EncodeJoinBundle(b)
	if err != nil {
		t.Fatal(err)
	}
	return enc, nonce
}

// TestC4ApproveIdempotent (M1): a plain double-approve of the SAME bundle is NOT refused as a replay;
// it yields the SAME deterministic op_id (re-commit is a self-heal, not a duplicate op).
func TestC4ApproveIdempotent(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	bundle, _ := makeJoinBundle(t, "join-2", "10.0.0.2:7400", "nonce-a")

	id1, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("first approve: %v", err)
	}
	id2, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("re-approve of the same bundle must NOT be refused as a replay: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("a deterministic op_id must be stable across re-approve: %q vs %q", id1, id2)
	}
	ops, _ := cluster.NonTerminalOperations(n.RODB())
	if len(ops) != 1 {
		t.Fatalf("re-approve must NOT create a second op: %d ops", len(ops))
	}
}

// TestC4JoinSecondApproveWhileActiveRefused (M2): a fresh-bundle approve for a node that already has an
// active op is REFUSED (no two-writer hole / identity clobber).
func TestC4JoinSecondApproveWhileActiveRefused(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	b1, _ := makeJoinBundle(t, "join-2", "10.0.0.2:7400", "nonce-a")
	if _, err := admin.StartJoinOperation(b1); err != nil {
		t.Fatalf("first approve: %v", err)
	}
	// A DIFFERENT bundle (fresh nonce ⇒ different op_id) for the same node while op-1 is active.
	b2, _ := makeJoinBundle(t, "join-2", "10.0.0.9:7400", "nonce-b")
	if _, err := admin.StartJoinOperation(b2); err == nil || !strings.Contains(err.Error(), "already in flight") {
		t.Fatalf("a fresh-bundle approve while an op is active must be refused: %v", err)
	}
}

// TestC4RetireGateLastVoter (BLOCKER-1 gate logic): driveRetire on the last voter routes to
// RETIRE_FAILED (never proceeds to RemoveServer), via the isVoter-aware retireGatePasses.
func TestC4RetireGateLastVoter(t *testing.T) {
	n, addr := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	// Admit ctl-1 as the sole VOTER.
	in := d7JoinInput(t, "ctl-1", addr)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(in, addr, caughtUp, 5_000_000_000); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// Create a retire op directly at DRAIN_REQUESTED (bypassing StartRetireOperation's sync last-voter
	// refusal) to exercise the CONTROLLER's gate.
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	op := cluster.OpStartInput{OpID: "op-r", Kind: cluster.OpKindRetire, TargetNode: "ctl-1",
		InitState: cluster.OpStateDrainRequested, Confirmed: true, Timeline: `[{"s":"DRAIN_REQUESTED"}]`}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(op, now) }); err != nil {
		t.Fatalf("seed retire op: %v", err)
	}
	admin.driveInFlightOperations()
	got, _ := cluster.OperationByID(n.RODB(), "op-r")
	if got == nil || got.OpState != cluster.OpStateRetireFailed || !got.Terminal {
		t.Fatalf("retire of the last voter must route to terminal RETIRE_FAILED, got %+v", got)
	}
}

// TestC4RetireGatePassesLogic (BLOCKER-1 + M3 decision logic): the isVoter-aware retire gate routes
// EVERY substrate case correctly — the heart of both the false-RETIRE_FAILED-on-resume fix and the
// F==0-re-check-at-the-irreversible-step fix. (The full multi-node kill-9 resume e2e is a gated test/c4
// follow-up; this locks the exact decision the resume window depends on.)
func TestC4RetireGatePassesLogic(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	seed := func(opID string, confirmed bool) *cluster.Operation {
		in := cluster.OpStartInput{OpID: opID, Kind: cluster.OpKindRetire, TargetNode: "n-" + opID,
			InitState: cluster.OpStateDrainRequested, Confirmed: confirmed, Timeline: `[{"s":"DRAIN_REQUESTED"}]`}
		if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
			t.Fatalf("seed %s: %v", opID, err)
		}
		op, _ := cluster.OperationByID(n.RODB(), opID)
		return op
	}
	state := func(opID string) string { o, _ := cluster.OperationByID(n.RODB(), opID); return o.OpState }

	// (a) already removed from the raft config (isVoter=false) ⇒ gate PASSES (the removal already
	// happened on resume) — this is the BLOCKER-1 fix (no false RETIRE_FAILED).
	if op := seed("op-removed", true); !admin.retireGatePasses(op, substrate{isVoter: false, numVoters: 1}) {
		t.Fatal("an already-removed target must PASS the gate (resume-after-removal), not false-fail")
	}
	// (b) last voter (isVoter=true, numVoters=1 ⇒ remaining 0) ⇒ RETIRE_FAILED.
	if op := seed("op-last", true); admin.retireGatePasses(op, substrate{isVoter: true, numVoters: 1}) {
		t.Fatal("the last voter must NOT pass")
	}
	if s := state("op-last"); s != cluster.OpStateRetireFailed {
		t.Fatalf("last-voter gate must route to RETIRE_FAILED, got %s", s)
	}
	// (c) F==0 unconfirmed (numVoters=2 ⇒ remaining 1, FT 0) ⇒ BLOCKED (the M3 re-check at the
	// irreversible step routes a worsened F==0 here, never barrels through).
	if op := seed("op-f0", false); admin.retireGatePasses(op, substrate{isVoter: true, numVoters: 2}) {
		t.Fatal("an F==0 unconfirmed retire must NOT pass")
	}
	if s := state("op-f0"); s != cluster.OpStateBlocked {
		t.Fatalf("F==0 unconfirmed must route to BLOCKED, got %s", s)
	}
	// (d) healthy (numVoters=4 ⇒ remaining 3, FT 1>0) ⇒ PASSES without a confirm.
	if op := seed("op-ok", false); !admin.retireGatePasses(op, substrate{isVoter: true, numVoters: 4}) {
		t.Fatal("a retire that keeps fault tolerance > 0 must pass")
	}
}

// TestC4JoinBarrierPersistedBeforeAddVoter (BLOCKER-2 logic): driving an op at ROSTER_COMMITTED
// captures + PERSISTS a non-zero barrier and advances to RAFT_ADDING BEFORE any AddVoter — so a
// kill-9-after-AddVoter resume re-enters CATCHING_UP against a REAL goalpost (never promotes a
// not-caught-up node).
func TestC4JoinBarrierPersistedBeforeAddVoter(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	in := cluster.OpStartInput{OpID: "op-j", Kind: cluster.OpKindJoin, TargetNode: "join-2",
		InitState: cluster.OpStateRosterCommitted, JoinNonce: "nz", Timeline: `[{"s":"ROSTER_COMMITTED"}]`}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
		t.Fatalf("seed join op: %v", err)
	}
	op, _ := cluster.OperationByID(n.RODB(), "op-j")
	admin.driveOne(op)
	got, _ := cluster.OperationByID(n.RODB(), "op-j")
	if got.OpState != cluster.OpStateRaftAdding {
		t.Fatalf("ROSTER_COMMITTED must advance to RAFT_ADDING, got %s", got.OpState)
	}
	if got.Barrier == 0 {
		t.Fatal("the catch-up barrier must be captured + persisted at ROSTER_COMMITTED, BEFORE any AddVoter (BLOCKER-2)")
	}
}

// TestC4DriveNoOpOnEmpty (guard): driveInFlightOperations is a safe no-op with no ops + a
// fresh admin (the leader-gate + empty work-list). Proves the controller never panics / writes idly.
func TestC4DriveNoOpOnEmpty(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	before, _ := n.AppliedIndex()
	admin.driveInFlightOperations()
	admin.driveInFlightOperations()
	after, _ := n.AppliedIndex()
	if after != before {
		t.Fatalf("driving zero operations must write nothing: applied %d -> %d", before, after)
	}
}
