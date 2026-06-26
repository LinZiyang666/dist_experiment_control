package cluster

import (
	"testing"
	"time"
)

// operation_ops_test.go (C4) — the operation-log ops: single-active guard, predecessor-CAS idempotent
// resume, the read primitives, and FSM determinism of the baked literals.

func opNow() time.Time { return time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC) }

func startJoinOp(t *testing.T, f *fsm, idx uint64, opID, target string) {
	t.Helper()
	cmd, err := PlanClusterOpStart(OpStartInput{
		OpID: opID, Kind: OpKindJoin, TargetNode: target, InitState: OpStateJoinProofVerified,
		Timeline: `[{"s":"JOIN_PROOF_VERIFIED"}]`, JoinNonce: "nonce-" + target,
	}, opNow())
	if err != nil {
		t.Fatalf("plan op-start: %v", err)
	}
	d7Apply(t, f, idx, cmd)
}

func TestOpStartSingleActiveGuard(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-a", "brk-b")

	got, err := ActiveOperationForTarget(f.db, "brk-b")
	if err != nil || got == nil {
		t.Fatalf("active op must exist: %v %+v", err, got)
	}
	if got.OpState != OpStateJoinProofVerified || got.Terminal {
		t.Fatalf("unexpected op: %+v", got)
	}

	// A SECOND start for the same target (different op-id) must be a no-op (single-active guard).
	startJoinOp(t, f, 2, "op-a2", "brk-b")
	if o, _ := OperationByID(f.db, "op-a2"); o != nil {
		t.Fatal("a 2nd active op for the same target must NOT be inserted (single-active guard)")
	}

	// A start for a DIFFERENT target is allowed.
	startJoinOp(t, f, 3, "op-c", "brk-c")
	if o, _ := ActiveOperationForTarget(f.db, "brk-c"); o == nil {
		t.Fatal("a distinct target's op must be insertable")
	}
}

func TestOpTransitionPredecessorCAS(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-a", "brk-b")

	// CAS from the actual state advances.
	adv, _ := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateJoinProofVerified, ToState: OpStateRosterCommitted,
		Timeline: `[{"s":"ROSTER_COMMITTED"}]`,
	}, opNow())
	d7Apply(t, f, 2, adv)
	if o, _ := OperationByID(f.db, "op-a"); o == nil || o.OpState != OpStateRosterCommitted {
		t.Fatalf("CAS advance failed: %+v", o)
	}

	// A STALE CAS (from the OLD state) is a deterministic no-op — the idempotent-resume property.
	stale, _ := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateJoinProofVerified, ToState: OpStateRaftAdding,
		Timeline: `[{"s":"RAFT_ADDING"}]`,
	}, opNow())
	d7Apply(t, f, 3, stale)
	if o, _ := OperationByID(f.db, "op-a"); o.OpState != OpStateRosterCommitted {
		t.Fatalf("a stale predecessor-CAS must NOT advance the op: %+v", o)
	}

	// Terminal transition frees the active slot.
	term, _ := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateRosterCommitted, ToState: OpStateAborted, Terminal: true,
		Timeline: `[{"s":"ABORTED"}]`,
	}, opNow())
	d7Apply(t, f, 4, term)
	if o, _ := ActiveOperationForTarget(f.db, "brk-b"); o != nil {
		t.Fatal("a terminal op must free the active slot")
	}
	// ... so a fresh op for the same target can now start.
	startJoinOp(t, f, 5, "op-a3", "brk-b")
	if o, _ := ActiveOperationForTarget(f.db, "brk-b"); o == nil || o.OpID != "op-a3" {
		t.Fatalf("a new op must start after the prior one terminated: %+v", o)
	}
}

func TestOpNonceConsumed(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-a", "brk-b") // join_nonce = "nonce-brk-b", NON-terminal
	// C4-M1: a NON-terminal (in-flight) nonce is NOT "consumed" (handled by the single-active guard +
	// idempotent re-approve) — only a TERMINAL op deduplicates a replay-after-retire.
	if ok, _ := NonceConsumed(f.db, "brk-b", "nonce-brk-b"); ok {
		t.Fatal("a non-terminal nonce must NOT read as consumed (else a plain double-approve is refused)")
	}
	// Drive the op terminal (ABORTED) — now the nonce is consumed (replay dedup).
	term, _ := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateJoinProofVerified, ToState: OpStateAborted, Terminal: true,
		Timeline: `[{"s":"ABORTED"}]`,
	}, opNow())
	d7Apply(t, f, 2, term)
	if ok, _ := NonceConsumed(f.db, "brk-b", "nonce-brk-b"); !ok {
		t.Fatal("a TERMINAL op's nonce must read as consumed (replay-after-retire dedup)")
	}
	if ok, _ := NonceConsumed(f.db, "brk-b", "other"); ok {
		t.Fatal("an unseen nonce must NOT be consumed")
	}
	if ok, _ := NonceConsumed(f.db, "brk-b", ""); ok {
		t.Fatal("an empty nonce is never consumed")
	}
}

func TestOpStartRejectsBadEnum(t *testing.T) {
	if _, err := PlanClusterOpStart(OpStartInput{OpID: "x", Kind: "bogus", TargetNode: "n", InitState: OpStateServing}, opNow()); err == nil {
		t.Fatal("op-start must reject an invalid kind on the bake surface (no sql CHECK)")
	}
	if _, err := PlanClusterOpStart(OpStartInput{OpID: "x", Kind: OpKindJoin, TargetNode: "n", InitState: "BOGUS"}, opNow()); err == nil {
		t.Fatal("op-start must reject an invalid state on the bake surface")
	}
}
