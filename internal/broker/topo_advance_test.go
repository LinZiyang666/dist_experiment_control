package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// topo_advance_test.go (R7b, #45) — the bounded NATS_ROLLED_OUT topology gate, and the ONE shape that must
// stay unbounded.
//
// THE RE-DIAGNOSIS THIS FILE ENCODES
// ----------------------------------
// The remediation ledger blamed rehome/migrate for the shrink/grow stall. It was wrong. The stall is
// topoConvergedForOp, five states later: it is fail-closed on ANY voter that is unreachable or not
// reporting a topology generation, and before R7b it had no counter, no deadline and no watchdog behind
// it. One silent voter pinned the op at NATS_ROLLED_OUT forever; because the op never went terminal,
// assertNoActiveOp refused the NEXT membership operation with "already in flight", and the whole membership
// plane wedged with no state `cluster ops confirm/abort` could act on.
//
// THE TRAP
// --------
// The obvious fix — "put a deadline on NATS_ROLLED_OUT" — breaks a CORRECT design. The final retire of an
// N=2 cluster is an intentional two-phase boundary: once the leaving voter is removed the survivor is
// alone, its on-disk nats.conf is still CLUSTERED, and a lone-self clustered render is invalid, so the
// survivor CANNOT observe the new topology generation until an operator explicitly de-clusters it. The op
// is supposed to sit non-terminal across that wait. deploy-tier drill 41 asserts exactly that
// (op_state == NATS_ROLLED_OUT && terminal != true) and calls it a PASS.
//
// So the tests below are adversarial in BOTH directions: the unbounded gate must now be bounded, and the
// N→1 boundary must be provably NOT bounded. A change that "fixes" the second one turns a documented,
// operator-gated wait into a fabricated failure — and drill 41 flips from PASS to FAIL.

// topoTestAdmin builds a single-node admin whose clock is controllable, plus a seeded op parked at
// NATS_ROLLED_OUT with an unreachable topology target (so topoConvergedForOp is false, which is the
// precondition every case here needs).
func topoTestAdmin(t *testing.T, opID, target string, deadline time.Time, now *time.Time) (*ClusterAdmin, *cluster.Operation) {
	t.Helper()
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return *now }

	in := cluster.OpStartInput{
		OpID: opID, Kind: cluster.OpKindRetire, TargetNode: target,
		InitState: cluster.OpStateNatsRolledOut, Confirmed: true,
		// A generation no voter has observed ⇒ the gate is fail-closed, which is the stalled shape.
		TopoTargetGen:   9999,
		CatchupDeadline: deadline.UnixNano(),
		Timeline:        `[{"s":"NATS_ROLLED_OUT"}]`,
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, *now)
	}); err != nil {
		t.Fatalf("seed %s: %v", opID, err)
	}
	op, err := cluster.OperationByID(n.RODB(), opID)
	if err != nil || op == nil {
		t.Fatalf("read back seeded op: %v", err)
	}
	if ok, _ := admin.topoConvergedForOp(op, false); ok {
		t.Fatal("fixture precondition failed: the topology gate must be NOT converged for these tests to mean anything")
	}
	return admin, op
}

func opState(t *testing.T, a *ClusterAdmin, opID string) (string, bool, string) {
	t.Helper()
	o, err := cluster.OperationByID(a.node.RODB(), opID)
	if err != nil || o == nil {
		t.Fatalf("read op %s: %v", opID, err)
	}
	return o.OpState, o.Terminal, o.LastError
}

// TestTopoAdvanceN1BoundaryStaysNonterminal is exit assertion 3: PROOF that the correct design was not
// "fixed". A retire whose removal left exactly one voter must never be blocked, never go terminal, and
// never run down a deadline — no matter how long it waits.
func TestTopoAdvanceN1BoundaryStaysNonterminal(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	// The deadline is set in the PAST at t0, so any deadline-driven path would fire on the very first
	// call. If the carve-out is ever removed, this test fails immediately and loudly.
	admin, op := topoTestAdmin(t, "op-n1", "brk-leaving", now.Add(-time.Hour), &now)

	for i, dt := range []time.Duration{0, time.Minute, time.Hour, 24 * time.Hour, 30 * 24 * time.Hour} {
		now = now.Add(dt)
		if admin.topoAdvance(op, substrate{numVoters: 1}, false) {
			t.Fatalf("tick %d (t+%s): the N=1 de-cluster boundary ADVANCED past the topology gate — that would declare a "+
				"retire complete while the survivor is still running a clustered nats.conf", i, dt)
		}
		state, terminal, lastErr := opState(t, admin, "op-n1")
		if state != cluster.OpStateNatsRolledOut {
			t.Fatalf("tick %d (t+%s): the op left NATS_ROLLED_OUT (now %s). deploy-tier drill 41 asserts this op stays "+
				"AT NATS_ROLLED_OUT until the operator de-clusters; the two-phase boundary is BY DESIGN, not a stall", i, dt, state)
		}
		if terminal {
			t.Fatalf("tick %d (t+%s): the op went TERMINAL at the N=1 boundary — drill 41 asserts terminal != true here", i, dt)
		}
		if state == cluster.OpStateBlocked {
			t.Fatalf("tick %d: the N=1 boundary was BLOCKED — a documented, operator-gated wait was turned into a fabricated failure", i)
		}
		// The operator must be told what to do, not just that something is unconverged.
		if !strings.Contains(lastErr, "--to-standalone") {
			t.Fatalf("tick %d: the recorded error must name the operator's next step (`cluster reconcile --to-standalone "+
				"--confirm-single`), got %q", i, lastErr)
		}
	}
	// Refresh from the store and re-assert, in case a transition wrote something the in-memory copy hid.
	state, terminal, _ := opState(t, admin, "op-n1")
	if state != cluster.OpStateNatsRolledOut || terminal {
		t.Fatalf("after 30 days at the N=1 boundary the op is %s (terminal=%v) — it must still be waiting", state, terminal)
	}
}

// TestTopoAdvanceBoundsTheUnboundedGate is the other half: for every shape that is NOT the N=1 boundary,
// the gate must terminate. "It will probably converge eventually" is exactly the answer #45 rejects.
func TestTopoAdvanceBoundsTheUnboundedGate(t *testing.T) {
	cases := []struct {
		name    string
		sub     substrate
		joining bool
	}{
		{"retire with survivors", substrate{numVoters: 3}, false},
		{"retire from N=2 leaving 2 configured voters", substrate{numVoters: 2}, false},
		{"join", substrate{numVoters: 3}, true},
		// A JOIN can never legitimately be at numVoters<=1 (it has just promoted its joiner), but pin it
		// anyway: the carve-out predicate must be retire-only, or a wedged join inherits an infinite wait.
		{"join with a single voter is NOT carved out", substrate{numVoters: 1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
			admin, op := topoTestAdmin(t, "op-"+strings.ReplaceAll(tc.name, " ", "-"), "brk-target",
				now.Add(opTopoConvergeTimeout), &now)

			// Before the deadline: non-terminal, but NOT blocked — a slow-but-healthy convergence
			// must not be false-failed.
			if admin.topoAdvance(op, tc.sub, tc.joining) {
				t.Fatal("advanced past an unconverged topology gate")
			}
			if state, _, _ := opState(t, admin, op.OpID); state != cluster.OpStateNatsRolledOut {
				t.Fatalf("the op left NATS_ROLLED_OUT before its deadline (now %s) — a healthy slow converge was false-failed", state)
			}

			// Past the deadline: it MUST terminate into an operator-actionable state.
			now = now.Add(opTopoConvergeTimeout + time.Second)
			if admin.topoAdvance(op, tc.sub, tc.joining) {
				t.Fatal("advanced past an unconverged topology gate after the deadline")
			}
			state, _, lastErr := opState(t, admin, op.OpID)
			if state != cluster.OpStateBlocked {
				t.Fatalf("the topology gate did not terminate after %s — it is still %s. An op that never leaves "+
					"NATS_ROLLED_OUT fences every subsequent membership operation via assertNoActiveOp, with no "+
					"state `cluster ops confirm/abort` can act on", opTopoConvergeTimeout, state)
			}
			// BLOCKED is the only state confirm/abort accept, and the operator needs both verbs named.
			for _, want := range []string{"ops confirm", "ops abort"} {
				if !strings.Contains(lastErr, want) {
					t.Errorf("the BLOCKED reason must name %q so the op is actionable; got %q", want, lastErr)
				}
			}
		})
	}
}

// TestTopoAdvanceDeadlineIsStampedOnEntry: the bound is only real if something writes it. toNatsRolledOut
// must re-stamp catchup_deadline with a FRESH topology window on entry — inheriting CATCHING_UP's
// already-elapsed deadline would BLOCK the op on its very first topology tick, converting a correct wait
// into an instant false failure.
func TestTopoAdvanceDeadlineIsStampedOnEntry(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return now }

	// An op arriving at NATS_ROLLED_OUT carrying an ALREADY-EXPIRED catch-up deadline.
	in := cluster.OpStartInput{
		OpID: "op-stamp", Kind: cluster.OpKindJoin, TargetNode: "brk-j",
		InitState: cluster.OpStateCatchingUp, Confirmed: true, Barrier: 7,
		CatchupDeadline: now.Add(-time.Hour).UnixNano(),
		Timeline:        `[{"s":"CATCHING_UP"}]`,
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
		t.Fatal(err)
	}
	op, _ := cluster.OperationByID(n.RODB(), "op-stamp")

	admin.toNatsRolledOut(op)

	after, _ := cluster.OperationByID(n.RODB(), "op-stamp")
	if after.OpState != cluster.OpStateNatsRolledOut {
		t.Fatalf("toNatsRolledOut did not transition (state %s)", after.OpState)
	}
	if after.CatchupDeadline <= now.UnixNano() {
		t.Fatalf("the op entered NATS_ROLLED_OUT still carrying an EXPIRED deadline (%d <= %d) — it would be BLOCKED on "+
			"its first topology tick, turning a correct wait into an instant false failure",
			after.CatchupDeadline, now.UnixNano())
	}
	if want := now.Add(opTopoConvergeTimeout).UnixNano(); after.CatchupDeadline != want {
		t.Fatalf("catchup_deadline = %d, want now+opTopoConvergeTimeout (%d)", after.CatchupDeadline, want)
	}
	// The barrier must survive the re-stamp — a resume re-entering CATCHING_UP needs a real goalpost.
	if after.Barrier != 7 {
		t.Fatalf("the barrier was lost across toNatsRolledOut (%d) — a kill-9 resume would promote a node that never caught up", after.Barrier)
	}
}

// TestCatchingUpIsAlwaysBounded is #47: a joiner must LEAVE CatchingUp or terminate with an explicit
// error, in bounded time. The two holes were (1) a probe that errors every tick returned before the
// deadline was ever consulted, and (2) an op with catchup_deadline == 0 had no deadline at all.
func TestCatchingUpIsAlwaysBounded(t *testing.T) {
	// (1) a probe that fails FOREVER must still hit the deadline.
	t.Run("a permanently failing probe still terminates", func(t *testing.T) {
		now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		n, _ := d7SingleNode(t, "ctl-1")
		admin := NewClusterAdmin(n, nil)
		admin.now = func() time.Time { return now }
		admin.caughtUpFn = func(string, uint64) (bool, error) { return false, errNeverReachable }

		in := cluster.OpStartInput{
			OpID: "op-probe", Kind: cluster.OpKindJoin, TargetNode: "brk-j",
			InitState: cluster.OpStateCatchingUp, Confirmed: true, Barrier: 5,
			CatchupDeadline: now.Add(2 * time.Minute).UnixNano(),
			Timeline:        `[{"s":"CATCHING_UP"}]`,
		}
		if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
			t.Fatal(err)
		}
		op, _ := cluster.OperationByID(n.RODB(), "op-probe")

		// Before the deadline the op stays put — a probe blip is not a failure.
		if admin.driveCatchingUp(op, substrate{phase: phaseCatchingUp}) {
			t.Fatal("a failing probe must not count as progress")
		}
		admin.boundCatchingUp(op)
		if s, _, _ := opState(t, admin, "op-probe"); s != cluster.OpStateCatchingUp {
			t.Fatalf("the op was terminated before its deadline (%s)", s)
		}

		// Past it, the op MUST become actionable.
		now = now.Add(3 * time.Minute)
		op, _ = cluster.OperationByID(n.RODB(), "op-probe")
		admin.boundCatchingUp(op)
		s, _, lastErr := opState(t, admin, "op-probe")
		if s != cluster.OpStateBlocked {
			t.Fatalf("a catch-up probe that fails EVERY tick left the op at %s forever — it never reaches the deadline "+
				"check, so membership stays fenced with nothing to confirm or abort", s)
		}
		if !strings.Contains(lastErr, "ops confirm") {
			t.Errorf("the BLOCKED reason must name the recovery verb, got %q", lastErr)
		}
	})

	// (2) an op with NO deadline must be GIVEN one, not blocked on the spot.
	t.Run("a deadline-less op is stamped, not failed", func(t *testing.T) {
		now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		n, _ := d7SingleNode(t, "ctl-1")
		admin := NewClusterAdmin(n, nil)
		admin.now = func() time.Time { return now }

		in := cluster.OpStartInput{
			OpID: "op-nodl", Kind: cluster.OpKindJoin, TargetNode: "brk-j",
			InitState: cluster.OpStateCatchingUp, Confirmed: true, Barrier: 5,
			CatchupDeadline: 0, // the gap: a row predating #7's capture step
			Timeline:        `[{"s":"CATCHING_UP"}]`,
		}
		if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
			t.Fatal(err)
		}
		op, _ := cluster.OperationByID(n.RODB(), "op-nodl")

		admin.boundCatchingUp(op)

		after, _ := cluster.OperationByID(n.RODB(), "op-nodl")
		if after.OpState == cluster.OpStateBlocked {
			t.Fatal("a missing deadline is a gap in the ladder, not evidence the joiner is dead — blocking here false-fails a healthy join")
		}
		if after.CatchupDeadline == 0 {
			t.Fatal("an op with no deadline was left with no deadline — CATCHING_UP is then unbounded, which is exactly #47")
		}
		if after.Barrier != 5 {
			t.Fatalf("the lazy deadline stamp lost the barrier (%d) — the catch-up gate would no longer be real", after.Barrier)
		}

		// And once stamped, it is bounded like any other op.
		now = now.Add(opCatchupMaxWindow + time.Hour)
		op2, _ := cluster.OperationByID(n.RODB(), "op-nodl")
		admin.boundCatchingUp(op2)
		if s, _, _ := opState(t, admin, "op-nodl"); s != cluster.OpStateBlocked {
			t.Fatalf("the freshly stamped deadline never fired (state %s)", s)
		}
	})

	// (3) the stamp must be a ONE-SHOT: re-stamping every tick would push the deadline forever, which is
	// an unbounded wait wearing a deadline's clothes.
	t.Run("the lazy stamp does not renew itself", func(t *testing.T) {
		now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		n, _ := d7SingleNode(t, "ctl-1")
		admin := NewClusterAdmin(n, nil)
		admin.now = func() time.Time { return now }

		in := cluster.OpStartInput{
			OpID: "op-once", Kind: cluster.OpKindJoin, TargetNode: "brk-j",
			InitState: cluster.OpStateCatchingUp, Confirmed: true, Barrier: 5,
			CatchupDeadline: 0, Timeline: `[{"s":"CATCHING_UP"}]`,
		}
		if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanClusterOpStart(in, now) }); err != nil {
			t.Fatal(err)
		}
		op, _ := cluster.OperationByID(n.RODB(), "op-once")
		admin.boundCatchingUp(op)
		first, _ := cluster.OperationByID(n.RODB(), "op-once")

		// Ten more ticks, clock advancing. The deadline must not move.
		for i := 0; i < 10; i++ {
			now = now.Add(10 * time.Second)
			cur, _ := cluster.OperationByID(n.RODB(), "op-once")
			admin.boundCatchingUp(cur)
		}
		last, _ := cluster.OperationByID(n.RODB(), "op-once")
		if last.CatchupDeadline != first.CatchupDeadline {
			t.Fatalf("the deadline moved from %d to %d across ticks — a self-renewing deadline never fires, which is #47 again",
				first.CatchupDeadline, last.CatchupDeadline)
		}
	})
}

// errNeverReachable models a catch-up probe that fails on every single tick.
var errNeverReachable = errProbe("joiner health endpoint unreachable")

type errProbe string

func (e errProbe) Error() string { return string(e) }
