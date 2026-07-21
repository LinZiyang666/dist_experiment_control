package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// catching_up_bounded_test.go — #47, second pass.
//
// R7b already implemented the fix (driveCatchingUp / boundCatchingUp) and covered its two holes. What it
// did NOT cover is:
//
//  1. WIRING. Every existing #47 test calls driveCatchingUp/boundCatchingUp DIRECTLY. Delete the
//     `a.boundCatchingUp(op)` line from driveJoin's CATCHING_UP arm and all of them still pass — the
//     exact half-wiring shape this project has been burned by twice. The tests here go through
//     driveInFlightOperations, the real controller entry.
//
//  2. The REACHABLE joiner. The roadmap's wording is "`cluster add` can leave a REACHABLE joiner in
//     CATCHING_UP forever". R7b's tests model an unreachable one (a probe that errors). A joiner whose
//     probe answers cleanly — it is up, it is talking, it just never crosses the barrier — is the
//     nastier shape, because nothing anywhere looks like an error. Same for a promotion that is
//     accepted and simply never reflects in the raft configuration.
//
// The exit criterion is absolute: leave CATCHING_UP, or terminate with an explicit error, in bounded
// time. "It will converge eventually" is not a third option.

// seedCatchingUpOp puts one join op into CATCHING_UP with a real barrier and deadline.
func seedCatchingUpOp(t *testing.T, n *cluster.Node, opID, target string, now time.Time, deadline int64) {
	t.Helper()
	in := cluster.OpStartInput{
		OpID: opID, Kind: cluster.OpKindJoin, TargetNode: target,
		InitState: cluster.OpStateCatchingUp, Confirmed: true, Barrier: 5,
		CatchupDeadline: deadline, Timeline: `[{"s":"CATCHING_UP"}]`,
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, now)
	}); err != nil {
		t.Fatalf("seed op: %v", err)
	}
}

// TestControllerBoundsCatchingUpForAReachableJoiner is the #47 exit assertion driven through the REAL
// controller loop, for a joiner that is perfectly healthy on the wire.
//
// The probe returns (false, nil) every tick: no error, no timeout, nothing to log — the joiner simply
// never crosses the barrier (an oversized InstallSnapshot on a slow link, a peer whose apply loop is
// wedged, a barrier captured against a leader that has since moved a long way forward). Without the
// deadline the op stays non-terminal forever, assertNoActiveOp fences the entire membership plane, and
// the operator cannot even abort: `cluster ops confirm/abort` only accepts BLOCKED.
//
// Mutation: remove `a.boundCatchingUp(op)` from driveJoin's CATCHING_UP arm → this test loops to its tick
// budget and fails, while every direct-call #47 test keeps passing.
func TestControllerBoundsCatchingUpForAReachableJoiner(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return now }

	probes := 0
	// Reachable and answering. Just never caught up.
	admin.caughtUpFn = func(string, uint64) (bool, error) { probes++; return false, nil }

	seedCatchingUpOp(t, n, "op-reachable", "brk-j", now, now.Add(2*time.Minute).UnixNano())

	// Tick the real loop with the clock advancing 30s per tick. A bounded op must terminate well
	// inside this budget; an unbounded one just keeps going.
	const maxTicks = 40
	terminatedAt := -1
	for i := 0; i < maxTicks; i++ {
		admin.driveInFlightOperations()
		s, _, _ := opState(t, admin, "op-reachable")
		if s != cluster.OpStateCatchingUp {
			terminatedAt = i
			break
		}
		now = now.Add(30 * time.Second)
	}
	if terminatedAt < 0 {
		t.Fatalf("after %d controller ticks (%s of simulated time) a REACHABLE joiner is STILL in CATCHING_UP. "+
			"The op is non-terminal, so assertNoActiveOp fences every later membership change, and the operator "+
			"cannot act on it — `cluster ops confirm/abort` only accepts BLOCKED. This is #47.",
			maxTicks, time.Duration(maxTicks)*30*time.Second)
	}
	if probes == 0 {
		t.Fatal("fixture precondition: the catch-up probe was never called, so the test proved nothing")
	}
	s, _, lastErr := opState(t, admin, "op-reachable")
	if s != cluster.OpStateBlocked {
		t.Fatalf("the op left CATCHING_UP for %s — the required exit is BLOCKED, the one state the operator's "+
			"confirm/abort verbs can act on", s)
	}
	if !strings.Contains(lastErr, "ops confirm") || !strings.Contains(lastErr, "ops abort") {
		t.Errorf("terminating without naming the recovery verbs is not an 'explicit error'; got %q", lastErr)
	}
	// The deadline must have been the reason, not luck: it had not passed on the first tick.
	if terminatedAt == 0 {
		t.Error("the op terminated on tick 0, before its deadline could have passed — a healthy slow joiner would be false-failed")
	}
}

// TestControllerBoundsAPromotionThatKeepsFailing is the second reachable-forever shape: the joiner IS
// caught up (its probe answers "yes" every tick), so the controller moves on to promotion — and the raft
// configuration change is refused every single time. Nothing here is unreachable; the op would simply
// re-issue AddVoter forever if the attempt counter did not bound it.
//
// This exits via blockAfterAttempts rather than the catch-up deadline, which is the correct bound for a
// failing side effect — but it is a DIFFERENT bound, so it is asserted separately. The op must reach
// BLOCKED, naming the recovery verbs, well inside the tick budget.
//
// Residual, disclosed rather than papered over: the THIRD shape — an AddVoter that SUCCEEDS but whose
// configuration change never reflects in sub.isVoter — is not reachable from this harness. Making raft
// accept a config change for a peer that then never appears requires a quorum the fake peer would have
// to participate in. driveCatchingUp deliberately returns false there (issuing is not progress), and that
// line is unmutable-by-test today; the honest verifier for it is a deploy-tier drill.
func TestControllerBoundsAPromotionThatKeepsFailing(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	n, addr := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return now }
	admin.caughtUpFn = func(string, uint64) (bool, error) { return true, nil } // caught up, ready to promote

	// A roster row so nodeRaftAddr resolves. The address collides with the sole existing voter, so raft
	// refuses the promotion on every attempt — a joiner that is up and answering, and still cannot join.
	seedRosterRow(t, n, "brk-j", addr)
	seedCatchingUpOp(t, n, "op-promote", "brk-j", now, now.Add(2*time.Minute).UnixNano())

	const maxTicks = 40
	for i := 0; i < maxTicks; i++ {
		admin.driveInFlightOperations()
		s, _, lastErr := opState(t, admin, "op-promote")
		if s == cluster.OpStateCatchingUp {
			now = now.Add(30 * time.Second)
			continue
		}
		if s != cluster.OpStateBlocked {
			t.Fatalf("the op left CATCHING_UP for %s; #47 requires BLOCKED — the only state the operator's "+
				"confirm/abort verbs accept", s)
		}
		if !strings.Contains(lastErr, "AddVoter") || !strings.Contains(lastErr, "ops confirm") {
			t.Errorf("BLOCKED without an explicit, actionable reason: %q", lastErr)
		}
		return
	}
	t.Fatalf("a promotion refused on every one of %d controller ticks left the op in CATCHING_UP. A retried "+
		"side effect that nothing counts is an infinite loop wearing a deadline's clothes (#47).", maxTicks)
}

// TestCatchingUpDeadlineIsNotSelfRenewingUnderTheRealLoop re-checks R7b's one-shot lazy stamp through the
// controller rather than by calling boundCatchingUp directly. A deadline that is re-stamped on every tick
// never fires, which would satisfy every "there is a deadline" assertion while remaining unbounded.
func TestCatchingUpDeadlineIsNotSelfRenewingUnderTheRealLoop(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return now }
	admin.caughtUpFn = func(string, uint64) (bool, error) { return false, errNeverReachable }

	// catchup_deadline == 0: the pre-#7 row shape. The first tick must GIVE it a deadline (blocking a
	// healthy join for a bookkeeping gap would be a false failure), and no later tick may move it.
	seedCatchingUpOp(t, n, "op-lazy", "brk-j", now, 0)

	admin.driveInFlightOperations()
	first, _ := cluster.OperationByID(n.RODB(), "op-lazy")
	if first.OpState != cluster.OpStateCatchingUp {
		t.Fatalf("a missing deadline is a gap in the ladder, not evidence the joiner is dead; got %s", first.OpState)
	}
	if first.CatchupDeadline == 0 {
		t.Fatal("the controller left a deadline-less op with no deadline — CATCHING_UP is then unbounded, which is #47")
	}

	// Advance to just BEFORE the stamped deadline, ticking all the way. If any tick re-stamped, the op
	// will still be alive after we cross it below.
	for now.UnixNano() < first.CatchupDeadline-int64(time.Minute) {
		now = now.Add(30 * time.Second)
		admin.driveInFlightOperations()
	}
	mid, _ := cluster.OperationByID(n.RODB(), "op-lazy")
	if mid.CatchupDeadline != first.CatchupDeadline {
		t.Fatalf("the deadline moved from %d to %d while the op sat in CATCHING_UP — a self-renewing deadline "+
			"never fires, which is #47 again", first.CatchupDeadline, mid.CatchupDeadline)
	}
	// Now cross it.
	now = time.Unix(0, first.CatchupDeadline).Add(time.Second)
	admin.driveInFlightOperations()
	if s, _, _ := opState(t, admin, "op-lazy"); s != cluster.OpStateBlocked {
		t.Fatalf("the lazily stamped deadline never fired (state %s)", s)
	}
}

// seedRosterRow inserts the cluster_nodes row nodeRaftAddr needs to resolve a target.
func seedRosterRow(t *testing.T, n *cluster.Node, nodeID, raftAddr string) {
	t.Helper()
	in := d7JoinInput(t, nodeID, raftAddr)
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(in)
	}); err != nil {
		t.Fatalf("seed roster row for %s: %v", nodeID, err)
	}
}
