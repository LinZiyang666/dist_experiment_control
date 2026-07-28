package cluster

import "testing"

// operation_barrier_columns_test.go — the three-state contract of the capture columns
// (barrier / catchup_deadline / topo_target_gen) on OpTransitionInput.
//
// WHY THIS FILE EXISTS (batch-B remainder, B8 half A)
// ---------------------------------------------------
// These three columns used to be gated by ONE bool, `SetBarrier`, and the SET clause then wrote all
// three UNCONDITIONALLY. The struct's own doc admitted the gap: "0 = leave unchanged is NOT
// expressible — the controller always passes the current values". "The controller always passes the
// current values" was the load-bearing part, and it was a convention held together by five
// hand-copied write-back sites in internal/broker/cluster_operation_controller.go (eight individual
// field copies). Miss one copy and the column was not merely left un-updated: it was written as
// ZERO, over a live value.
//
// For topo_target_gen zero is not a neutral value. topoConvergedForOp
// (cluster_operation_controller.go:1080-1082) opens with
//
//	if op.TopoTargetGen == 0 { return true, "" } // nothing to converge (no topology managed)
//
// while every OTHER branch of that function fails CLOSED (status unavailable, a voter not
// reporting/reachable, a voter behind). So zero is the single value that reads as "converged", and a
// forgotten field copy manufactured exactly it — the op then announced SERVING/RETIRED with the
// topology unconverged.
//
// The first test below was written against the OLD shape and was RED (`got 0, want 42`). That is the
// durable evidence the defect was real rather than theoretical: it is not a test written to fit the
// new code.
//
// The second test exists because the fix has an inverse trap, and the refactor roadmap fell into it.
// S1-refactor-roadmap.md:506 says "ConfirmOp's manual zeroing becomes passing nil". It must not:
// nil PRESERVES the column, and the column being preserved there is an already-expired rehome
// deadline. Passing nil would make `cluster ops confirm` re-block on the very next tick, i.e. a
// no-op — the exact defect the F2 reset was added to fix. A reset has to be OpVal(0).
func TestBarrierOnlyCaptureMustNotZeroTopoTargetGen(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-a", "brk-b")

	// Capture step 1: bake a barrier AND a topology target generation, the way the join path does.
	capture, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateJoinProofVerified, ToState: OpStateRosterCommitted,
		Timeline:        `[{"s":"ROSTER_COMMITTED"}]`,
		Barrier:         OpVal(uint64(7)),
		CatchupDeadline: OpVal(int64(1_000_000_000)),
		TopoTargetGen:   OpVal(uint64(42)),
	}, opNow())
	if err != nil {
		t.Fatalf("plan capture step: %v", err)
	}
	d7Apply(t, f, 2, capture)

	o, err := OperationByID(f.db, "op-a")
	if err != nil || o == nil {
		t.Fatalf("read op after capture: %v %+v", err, o)
	}
	if o.Barrier != 7 || o.CatchupDeadline != 1_000_000_000 || o.TopoTargetGen != 42 {
		t.Fatalf("fixture is not exercising a real capture step: barrier=%d deadline=%d topo_target_gen=%d "+
			"(want 7, 1000000000, 42)", o.Barrier, o.CatchupDeadline, o.TopoTargetGen)
	}

	// Capture step 2: re-bake ONLY the barrier. The other two are left nil — under the old bool this
	// was the forgotten hand-copy; under pointers it is the caller saying nothing about them.
	rebake, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-a", FromState: OpStateRosterCommitted, ToState: OpStateRaftAdding,
		Timeline: `[{"s":"RAFT_ADDING"}]`,
		Barrier:  OpVal(uint64(9)),
	}, opNow())
	if err != nil {
		t.Fatalf("plan re-bake step: %v", err)
	}
	d7Apply(t, f, 3, rebake)

	o, err = OperationByID(f.db, "op-a")
	if err != nil || o == nil {
		t.Fatalf("read op after re-bake: %v %+v", err, o)
	}
	if o.Barrier != 9 {
		t.Errorf("the re-baked barrier was not written: got %d, want 9", o.Barrier)
	}
	if o.TopoTargetGen != 42 {
		t.Errorf("topo_target_gen was destroyed by a transition that said nothing about it: got %d, want 42.\n"+
			"Zero is the ONE value topoConvergedForOp (cluster_operation_controller.go:1080-1082) reads as\n"+
			"\"nothing to converge\", so this turns a forgotten field into a topology gate that opens.\n"+
			"A column the input leaves nil must not appear in the SET clause.", o.TopoTargetGen)
	}
	if o.CatchupDeadline != 1_000_000_000 {
		t.Errorf("catchup_deadline was destroyed by a transition that said nothing about it: got %d, want 1000000000",
			o.CatchupDeadline)
	}
}

// TestExplicitZeroIsDistinguishableFromLeaveAlone is the guard against the inverse mistake: a reset
// expressed as nil. If OpVal(0) and nil ever collapse to the same SET clause, `cluster ops confirm`
// silently stops resetting the rehome convergence window and becomes a no-op that re-blocks on the
// next tick (cluster_operation_controller.go, the F2 branch).
func TestExplicitZeroIsDistinguishableFromLeaveAlone(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-z", "brk-z")

	seed, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-z", FromState: OpStateJoinProofVerified, ToState: OpStateRosterCommitted,
		Timeline:        `[{"s":"ROSTER_COMMITTED"}]`,
		Barrier:         OpVal(uint64(5)),
		CatchupDeadline: OpVal(int64(777)),
		TopoTargetGen:   OpVal(uint64(11)),
	}, opNow())
	if err != nil {
		t.Fatalf("plan seed: %v", err)
	}
	d7Apply(t, f, 2, seed)

	// An EXPLICIT reset of the deadline only. barrier + topo_target_gen must survive it.
	reset, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-z", FromState: OpStateRosterCommitted, ToState: OpStateRaftAdding,
		Timeline:        `[{"s":"RAFT_ADDING"}]`,
		CatchupDeadline: OpVal(int64(0)),
	}, opNow())
	if err != nil {
		t.Fatalf("plan reset: %v", err)
	}
	d7Apply(t, f, 3, reset)

	o, err := OperationByID(f.db, "op-z")
	if err != nil || o == nil {
		t.Fatalf("read op after reset: %v %+v", err, o)
	}
	if o.CatchupDeadline != 0 {
		t.Errorf("OpVal(0) must WRITE zero, not be treated as \"leave alone\": catchup_deadline = %d.\n"+
			"`cluster ops confirm` depends on this reset landing; if it does not, the op re-enters carrying\n"+
			"its already-expired deadline and boundRehomeConvergence re-BLOCKs it on the next tick.",
			o.CatchupDeadline)
	}
	if o.Barrier != 5 || o.TopoTargetGen != 11 {
		t.Errorf("an explicit reset of ONE column must not disturb the other two: barrier=%d topo_target_gen=%d "+
			"(want 5, 11)", o.Barrier, o.TopoTargetGen)
	}
}
