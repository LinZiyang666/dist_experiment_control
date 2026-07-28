package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// reconcile_authority_test.go — the two properties B7 added to the registry: an authority resolved
// ONCE PER SWEEP, and duration/overrun observability measured on the injected clock.

// TestAuthorityIsResolvedOncePerSweep is the reason passAuthority is a value and not a per-pass
// closure.
//
// The roadmap asked for `leaderOnly bool -> authority func() bool`. With closures, N gated passes in
// one sweep would evaluate leadership N times, and leadership lost between the first and the last would
// let the first half of a tick act as leader and the second half not. One of the duties gated this way
// drives membership phase transitions, where a split view inside one tick is the no-silent-fork hazard
// §8.1 exists to prevent — so this is not a tidiness property.
//
// The old bool had the property by accident (runDue read one registry-level isLeader). This test makes
// it explicit, so a future change to closures fails here instead of in production.
func TestAuthorityIsResolvedOncePerSweep(t *testing.T) {
	var evaluations atomic.Int64
	r := newReconcileRegistry(silentLogger(), func() bool {
		evaluations.Add(1)
		return true
	})
	// FIVE gated passes, all due on the same sweep. If the answer were resolved per pass this would be
	// 5 evaluations; the contract is 1.
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		r.register(name, time.Second, authorityLeader, func(context.Context, time.Time) error { return nil })
	}
	// Plus an ungated one, to prove authorityAny costs no evaluation at all.
	r.register("ungated", time.Second, authorityAny, func(context.Context, time.Time) error { return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	ran := r.runDue(context.Background(), clk.advance(time.Second))

	if len(ran) != 6 {
		t.Fatalf("expected all 6 passes to run, got %d: %v", len(ran), ran)
	}
	if got := evaluations.Load(); got != 1 {
		t.Errorf("the leadership source was consulted %d times in ONE sweep, want exactly 1.\n"+
			"Every gated pass in a sweep must see the SAME answer: leadership lost mid-sweep would "+
			"otherwise let the first passes act as leader and the rest not, and one of them proposes "+
			"membership phase transitions.", got)
	}

	// A second sweep re-resolves it — the answer is cached for the sweep, not for the process.
	r.runDue(context.Background(), clk.advance(time.Second))
	if got := evaluations.Load(); got != 2 {
		t.Errorf("the second sweep must re-resolve authority (got %d evaluations total, want 2); caching "+
			"across sweeps would keep running leader-gated passes after leadership moved", got)
	}
}

// TestAuthorityAnyRunsWithoutALeadershipSource pins the single-mode default. The entire current fleet
// runs single-mode brokers, where there is no raft leader to ask: a nil leadership source means
// "permitted". A refactor that made a nil source mean "denied" would silently stop every leader-gated
// pass on every broker in production while every clustered test stayed green.
func TestAuthorityAnyRunsWithoutALeadershipSource(t *testing.T) {
	var leaderRuns, anyRuns int
	r := newReconcileRegistry(silentLogger(), nil) // nil == single mode
	r.register("gated", time.Second, authorityLeader, func(context.Context, time.Time) error { leaderRuns++; return nil })
	r.register("ungated", time.Second, authorityAny, func(context.Context, time.Time) error { anyRuns++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	r.runDue(context.Background(), clk.advance(time.Second))

	if anyRuns != 1 {
		t.Errorf("an authorityAny pass must always run: got %d runs", anyRuns)
	}
	if leaderRuns != 1 {
		t.Errorf("with NO leadership source (single mode) a leader-gated pass must still run: got %d runs.\n"+
			"Treating nil as \"not the leader\" disables ports, grow-lock, upgrade-lock and home-delivery "+
			"on every single-mode broker — which today is the whole fleet.", leaderRuns)
	}
}

// TestPassDurationAndOverrunAreRecorded pins the observability that replaced the timeout B7 refused to
// ship. It is measured on the INJECTED clock, so it is deterministic and adds no real sleep.
func TestPassDurationAndOverrunAreRecorded(t *testing.T) {
	clk := newFakeClock(regEpoch)
	r := newReconcileRegistry(silentLogger(), nil)
	r.now = clk.Now // the duration clock; production wires b.cfg.Now

	// "slow" advances the clock by 3× its own interval on its FIRST run only — an overrun that then
	// recovers. "quick" does not move the clock at all.
	//
	// The "first run only" part is load-bearing and was a real defect in this test: a body that
	// advances the clock EVERY time makes the second sweep measure 3s again, so `p.maxDur = dur`
	// (high-water property deleted) lands on the same 3s and the assertion at the bottom cannot tell
	// the difference. Mutation-verified in both directions — see the comment above that assertion.
	slowRuns := 0
	r.register("slow", time.Second, authorityAny, func(context.Context, time.Time) error {
		slowRuns++
		if slowRuns == 1 {
			clk.advance(3 * time.Second)
		}
		return nil
	})
	r.register("quick", time.Second, authorityAny, func(context.Context, time.Time) error { return nil })

	r.start(clk.Now())
	r.runDue(context.Background(), clk.advance(time.Second))

	byName := map[string]reconcilePassStatus{}
	for _, s := range r.status() {
		byName[s.Name] = s
	}
	slow, quick := byName["slow"], byName["quick"]

	if slow.LastDur != 3*time.Second {
		t.Errorf("slow LastDur = %v, want 3s (measured on the injected clock)", slow.LastDur)
	}
	if slow.MaxDur != 3*time.Second {
		t.Errorf("slow MaxDur = %v, want 3s", slow.MaxDur)
	}
	if slow.Overruns != 1 {
		t.Errorf("slow Overruns = %d, want 1 — it took 3s against a 1s interval, so it is no longer "+
			"keeping up with itself AND it held the sweep lock for every other pass", slow.Overruns)
	}
	if quick.Overruns != 0 {
		t.Errorf("quick Overruns = %d, want 0", quick.Overruns)
	}

	// MaxDur is a high-water mark: a later fast run must not erase the evidence of the slow one. This
	// is what makes the CLI's SLOWEST column useful after the fact.
	//
	// The second sweep RECOVERS (slowRuns == 2 does not move the clock), so LastDur drops to 0 while
	// MaxDur must stay at 3s. Asserting BOTH is what gives this arm teeth: under the mutation
	// `if dur > p.maxDur { p.maxDur = dur }` → `p.maxDur = dur` the two collapse onto each other and
	// MaxDur reads 0. Before the recovery was introduced this whole arm was VACUOUS — the pass took 3s
	// on every sweep, so max-vs-last was unobservable and the mutation ran green.
	r.runDue(context.Background(), clk.advance(time.Second))
	if slowRuns != 2 {
		t.Fatalf("the second sweep did not re-run slow (ran %d times) — without a SECOND completion "+
			"there is no later run to erase the high-water mark with, and this arm proves nothing", slowRuns)
	}
	for _, s := range r.status() {
		if s.Name != "slow" {
			continue
		}
		if s.LastDur != 0 {
			t.Errorf("slow LastDur = %v after the recovered run, want 0 — if the recovery did not "+
				"actually take zero clock time, MaxDur and LastDur are equal and the assertion below "+
				"cannot distinguish a high-water mark from a plain overwrite", s.LastDur)
		}
		if s.MaxDur != 3*time.Second {
			t.Errorf("MaxDur must be a high-water mark, got %v after a subsequent fast run — an operator "+
				"looking at a recovered pass would see no trace of the wedge, and the CLI's SLOWEST "+
				"column silently degrades into LAST", s.MaxDur)
		}
	}
}

// TestPassDurationWithoutAClockIsInertNotWrong: a registry with no duration clock (every test that does
// not care) must report zero durations and NEVER let that influence scheduling. The fake-clock
// equivalence proofs depend on this.
func TestPassDurationWithoutAClockIsInertNotWrong(t *testing.T) {
	var runs int
	r := newReconcileRegistry(silentLogger(), nil) // r.now left nil on purpose
	r.register("p", time.Second, authorityAny, func(context.Context, time.Time) error { runs++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	for i := 0; i < 5; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	if runs != 5 {
		t.Fatalf("scheduling must be unaffected by the absence of a duration clock: runs=%d, want 5", runs)
	}
	for _, s := range r.status() {
		if s.LastDur != 0 || s.MaxDur != 0 || s.Overruns != 0 {
			t.Errorf("with no duration clock the measurements must be zero, not garbage: %+v", s)
		}
	}
}
