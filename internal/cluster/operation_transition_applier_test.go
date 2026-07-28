package cluster

import (
	"strings"
	"testing"
)

// operation_transition_applier_test.go — the missing half of B8 half A's wire argument.
//
// origin: batch-B remainder, lane B8 adversarial review, finding B8-5.
//
// PlanClusterOpTransition now emits a VARIABLE SET clause: whichever of barrier / catchup_deadline /
// topo_target_gen the caller supplied, and nothing else (operation_ops.go:181-198). The safety argument
// written above it — "a shorter SET clause is still a statement every version of the FSM applies
// identically — no wire concern" — is true, but NOT because of anything general about the FSM. It is
// true because THIS op is routed to the shared genericExecApplier (clustermeta.go:121), which execs the
// baked text and inspects nothing.
//
// The counter-example lives in this same package. clusterNodeUpsertApplier (membership_ops.go:186-201)
// reconstructs the exact contiguous VALUES fragments its planner bakes and refuses the entry with
// errAppliedRejected unless it finds them verbatim:
//
//	if !strings.Contains(body, identFrag) || !strings.Contains(body, joinFrag) { … }
//
// So this repo does contain an apply-side consumer for which the rendered SQL SHAPE is load-bearing, and
// giving OpClusterOpTransition an applier of that kind would turn B8's variable SET clause into a
// per-replica poison-skip — a silent FSM fork, on a face (cluster.apply.<verb>) that is broker-to-broker
// and cross-version. Nothing pinned the routing, so that change would have gone in green.
func TestOpTransitionUsesTheGenericApplier(t *testing.T) {
	appliers := defaultAppliers()

	got, ok := appliers[OpClusterOpTransition]
	if !ok {
		t.Fatal("OpClusterOpTransition has no applier registered at all")
	}
	if _, isGeneric := got.(genericExecApplier); !isGeneric {
		t.Fatalf("OpClusterOpTransition is applied by %T, not genericExecApplier.\n"+
			"PlanClusterOpTransition emits a SET clause whose COLUMN LIST varies with which of the three\n"+
			"capture pointers the caller supplied (operation_ops.go:187-195). That is only safe while the\n"+
			"applier execs the text without inspecting it. A shape-matching applier (see\n"+
			"clusterNodeUpsertApplier, membership_ops.go:186-201) would reject the shorter statement as\n"+
			"errAppliedRejected on replicas that expect the long one — a deterministic poison-skip that\n"+
			"forks the FSM across a mixed-release cluster. If this op genuinely needs a custom applier,\n"+
			"that applier must accept every subset of the three capture columns.", got)
	}

	// NON-VACUITY: the assertion above must not be able to pass by matching everything. The one op that
	// really does have a shape-sensitive custom applier has to still show up as one, on the SAME table.
	upsert, ok := appliers[OpClusterNodeUpsert]
	if !ok {
		t.Fatal("OpClusterNodeUpsert has no applier registered — the control for this test is gone")
	}
	if _, isCustom := upsert.(clusterNodeUpsertApplier); !isCustom {
		t.Fatalf("the non-vacuity control failed: OpClusterNodeUpsert is applied by %T, so this table no "+
			"longer distinguishes generic from custom appliers and the assertion above proves nothing",
			upsert)
	}
}

// TestOpTransitionOmitsUnsuppliedCaptureColumnsAndStillApplies pins the two halves of the three-state
// contract at the SQL-text and applier level, rather than through the FSM: a nil pointer must leave its
// column out of the statement entirely, and the resulting shorter statement must still be a statement
// that the applier surface accepts and that leaves the stored values alone.
//
// It deliberately drives ExecCommand — the §13.2 equivalence / D9 `--from-existing` migration entry
// point (command.go:264-280) — because that is a DIFFERENT applier entry point from the fsm.Apply path
// operation_barrier_columns_test.go exercises, and B8's claim is about how the text is applied, not about
// one caller.
func TestOpTransitionOmitsUnsuppliedCaptureColumnsAndStillApplies(t *testing.T) {
	f, db := freshFSM(t, t.TempDir())
	startJoinOp(t, f, 1, "op-shape", "brk-shape")

	seed, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-shape", FromState: OpStateJoinProofVerified, ToState: OpStateRosterCommitted,
		Timeline:        `[{"s":"ROSTER_COMMITTED"}]`,
		Barrier:         OpVal(uint64(31)),
		CatchupDeadline: OpVal(int64(4242)),
		TopoTargetGen:   OpVal(uint64(13)),
	}, opNow())
	if err != nil {
		t.Fatalf("plan seed: %v", err)
	}
	d7Apply(t, f, 2, seed)

	// All three nil: the statement must mention none of the three columns.
	bare, err := PlanClusterOpTransition(OpTransitionInput{
		OpID: "op-shape", FromState: OpStateRosterCommitted, ToState: OpStateRaftAdding,
		Timeline: `[{"s":"RAFT_ADDING"}]`,
	}, opNow())
	if err != nil {
		t.Fatalf("plan bare: %v", err)
	}
	if len(bare.Body) != 1 {
		t.Fatalf("expected exactly one baked statement, got %d", len(bare.Body))
	}
	sql := bare.Body[0].SQL
	for _, col := range []string{"barrier =", "catchup_deadline =", "topo_target_gen ="} {
		if strings.Contains(sql, col) {
			t.Errorf("a nil capture pointer must keep %q OUT of the SET clause; a column that is written "+
				"at all is a column that can be written as zero, which is the whole defect B8 removed.\n"+
				"rendered: %s", col, sql)
		}
	}
	// The guard the CAS depends on must survive the shortening.
	if !strings.Contains(sql, `WHERE op_id = `) || !strings.Contains(sql, ` AND op_state = `) {
		t.Fatalf("the predecessor-CAS guard was lost while trimming the SET clause: %s", sql)
	}

	// And the shorter statement must still apply, through the non-FSM applier surface, leaving the three
	// stored values untouched.
	if err := ExecCommand(db, bare); err != nil {
		t.Fatalf("a SET clause with fewer columns must still be a statement the applier accepts: %v", err)
	}
	o, err := OperationByID(db, "op-shape")
	if err != nil || o == nil {
		t.Fatalf("read op after the bare transition: %v %+v", err, o)
	}
	if o.OpState != OpStateRaftAdding {
		t.Errorf("the cursor did not advance: op_state = %q", o.OpState)
	}
	if o.Barrier != 31 || o.CatchupDeadline != 4242 || o.TopoTargetGen != 13 {
		t.Errorf("columns absent from the SET clause must keep their stored values: barrier=%d "+
			"catchup_deadline=%d topo_target_gen=%d (want 31, 4242, 13)",
			o.Barrier, o.CatchupDeadline, o.TopoTargetGen)
	}
}
