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

// TestB2UpgradeLockBlocksMembership (External-review B2): while a `cluster upgrade` roll holds the
// cluster-scoped lock marker, StartJoinOperation AND StartRetireOperation must BOTH refuse — a concurrent
// grow/retire crossing a rolling broker restart could turn "temporarily down one voter" into a quorum
// break. The lock check is the FIRST gate (before nonce/roster checks), so the refusal is unconditional
// and does NOT consume the join nonce; releasing the marker restores membership.
// TestG4GrowMarkerFenceAndCarveout (review m1/M6): the cluster_grow_active marker is joiner-id-valued, so it
// (a) refuses a DIFFERENT-node join + any retire (strict serialize), yet (b) does NOT block the grow's OWN
// join op (the Q7 self-op carve-out — else the grow deadlocks its own membership change). Pins the asymmetry
// the review flagged (StartJoinOperation previously had no grow fence) + the carve-out.
func TestG4GrowMarkerFenceAndCarveout(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	bundleA, _ := makeJoinBundle(t, "join-A", "10.0.0.3:7400", "nonce-a")
	bundleB, _ := makeJoinBundle(t, "join-B", "10.0.0.4:7400", "nonce-b")

	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanSetGrowActive("join-A", time.Now()) }); err != nil {
		t.Fatalf("set grow marker: %v", err)
	}
	if growActiveJoiner(n.RODB()) != "join-A" {
		t.Fatalf("growActiveJoiner must read the joiner id, got %q", growActiveJoiner(n.RODB()))
	}
	if _, err := admin.StartJoinOperation(bundleB); err == nil || !strings.Contains(err.Error(), "grow") {
		t.Fatalf("a join of a DIFFERENT node must refuse during a grow (m1): %v", err)
	}
	if _, err := admin.StartRetireOperation("ctl-1", true); err == nil || !strings.Contains(err.Error(), "grow") {
		t.Fatalf("retire must refuse during a grow: %v", err)
	}
	// The grow's OWN join (join-A == the marker's joiner) is ALLOWED (self-op carve-out — M6: else self-deadlock).
	if _, err := admin.StartJoinOperation(bundleA); err != nil {
		t.Fatalf("the grow's OWN join (self-op carve-out) must be allowed while the marker is held: %v", err)
	}
	// External review M1: a release BOUND TO A DIFFERENT JOINER must be a NO-OP — clearing join-A's marker via a
	// release of "join-B" (a stale/errant re-run of another grow) must NOT drop this grow's mutex. The marker survives.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearGrowActive("join-B") }); err != nil {
		t.Fatalf("clear (wrong joiner) propose: %v", err)
	}
	if growActiveJoiner(n.RODB()) != "join-A" {
		t.Fatalf("M1: a release bound to a DIFFERENT joiner must NOT clear join-A's marker, got %q", growActiveJoiner(n.RODB()))
	}
	// A release bound to the OWNING joiner (join-A) clears it.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearGrowActive("join-A") }); err != nil {
		t.Fatalf("clear (owning joiner) propose: %v", err)
	}
	if growActiveJoiner(n.RODB()) != "" {
		t.Fatal("growActiveJoiner must be empty after the owning-joiner clear")
	}
	if _, err := admin.StartJoinOperation(bundleB); err != nil {
		t.Fatalf("a different-node join must resume after the grow marker clears: %v", err)
	}
}

func TestB2UpgradeLockBlocksMembership(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	bundle, _ := makeJoinBundle(t, "join-2", "10.0.0.2:7400", "nonce-a")

	// Acquire the roll lock (exactly what the orchestrator's acquire-lock trigger Proposes on the leader).
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanSetUpgradeActive(time.Now()) }); err != nil {
		t.Fatalf("set upgrade marker: %v", err)
	}
	if !upgradeActive(n.RODB()) {
		t.Fatal("upgradeActive must read the marker once it is committed")
	}
	if _, err := admin.StartJoinOperation(bundle); err == nil || !strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("join must refuse while a roll holds the upgrade lock: %v", err)
	}
	if _, err := admin.StartRetireOperation("ctl-1", true); err == nil || !strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("retire must refuse while a roll holds the upgrade lock: %v", err)
	}

	// Release → membership resumes. The refused join above never consumed the nonce, so the SAME bundle
	// now proceeds (proving the lock gate rejected BEFORE any state mutation).
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearUpgradeActive() }); err != nil {
		t.Fatalf("clear upgrade marker: %v", err)
	}
	if upgradeActive(n.RODB()) {
		t.Fatal("upgradeActive must be false after the marker is cleared")
	}
	if _, err := admin.StartJoinOperation(bundle); err != nil {
		t.Fatalf("join must proceed once the lock is released: %v", err)
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
