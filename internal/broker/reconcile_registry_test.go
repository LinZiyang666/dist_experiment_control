package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reconcile_registry_test.go (R7a) — the registry's own contract, plus the
// FREEZE PROOF.
//
// The batch's freeze criterion is deliberately not "a reviewer read the
// interface and it looked general enough". It is: the four pre-R7 loop bodies
// must be re-expressible in the registry with IDENTICAL behavior, demonstrated
// under a fake clock. If they are not, the tuple is wrong, and four downstream
// batches would have had to reopen it. Everything in this file exists to make
// that demonstration mechanical rather than editorial.

// fakeClock is a deterministic, monotonically advanced clock. There is no real
// time anywhere in this file: the equivalence proof simulates ~30 minutes of
// broker uptime in microseconds, which is the only reason it can afford to
// compare tick-for-tick over thousands of ticks.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t0 time.Time) *fakeClock { return &fakeClock{now: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

var regEpoch = time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)

// --------------------------------------------------------------------------
// THE FREEZE PROOF: registry cadence == the pre-R7 dedicated tickers, exactly.
// --------------------------------------------------------------------------

// TestReconcileRegistryMatchesLegacyTickers is the fake-clock equivalence proof
// demanded by the batch's freeze criterion.
//
// The pre-R7 loop was:
//
//	ticker   := time.NewTicker(ReconcileInterval)  // node-states, ports, tunnel-sessions
//	gcTicker := time.NewTicker(ProcGCInterval)     // proc-gc
//
// A Go ticker created at t0 with period P fires at t0+P, t0+2P, … so after
// elapsed time T it has fired exactly floor(T/P) times. That closed form is the
// ORACLE here — it is computed independently of the registry, not read back
// from it — and the registry's anchored deadlines must reproduce it for every
// pass, on every one of the ~1800 simulated ticks.
//
// It also pins WHY the tuple needs a per-pass interval: proc-gc's expected count
// after 30 minutes is 6, while the other three are at 1800. A flat
// single-interval table would have to pick one of those numbers for all four.
func TestReconcileRegistryMatchesLegacyTickers(t *testing.T) {
	const (
		reconcileInterval = time.Second
		procGCInterval    = 5 * time.Minute
		horizon           = 30 * time.Minute
	)

	counts := map[string]int{}
	r := newReconcileRegistry(silentLogger(), func() bool { return true })
	count := func(name string) reconcilePassFn {
		return func(context.Context, time.Time) error { counts[name]++; return nil }
	}
	// Registered with the SAME intervals and the SAME leadership gates as
	// registerCoreReconcilePasses wires them in production.
	r.register("node-states", reconcileInterval, authorityAny, count("node-states"))
	r.register("ports", reconcileInterval, authorityLeader, count("ports"))
	r.register("tunnel-sessions", reconcileInterval, authorityAny, count("tunnel-sessions"))
	r.register("proc-gc", procGCInterval, authorityAny, count("proc-gc"))

	if g := r.granularity(); g != reconcileInterval {
		t.Fatalf("driving ticker period must be the shortest registered interval: got %v, want %v", g, reconcileInterval)
	}

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())

	// Drive exactly as Run does: wake on the granularity ticker, sweep.
	ticks := int(horizon / reconcileInterval)
	for i := 0; i < ticks; i++ {
		r.runDue(context.Background(), clk.advance(reconcileInterval))
	}

	// The oracle: floor(elapsed / interval), computed from the ticker contract.
	want := map[string]int{
		"node-states":     int(horizon / reconcileInterval),
		"ports":           int(horizon / reconcileInterval),
		"tunnel-sessions": int(horizon / reconcileInterval),
		"proc-gc":         int(horizon / procGCInterval),
	}
	for name, w := range want {
		if counts[name] != w {
			t.Errorf("pass %q fired %d times over %v; a dedicated time.NewTicker(%v) would have fired %d — the registry is NOT behavior-equivalent",
				name, counts[name], horizon, want[name], w)
		}
	}
	if want["proc-gc"] == want["node-states"] {
		t.Fatal("test is degenerate: proc-gc and node-states must have DIFFERENT expected counts, else it cannot prove per-pass intervals are needed")
	}

	// lastTick must be populated for every pass — R13 consumes it, and a field
	// that is only wired later is a field that gets wired wrong.
	for _, s := range r.status() {
		if s.LastTick.IsZero() {
			t.Errorf("pass %q has a zero lastTick after %d sweeps — the observability field must be live from R7a, not retrofitted", s.Name, ticks)
		}
		if s.Runs == 0 {
			t.Errorf("pass %q recorded zero runs", s.Name)
		}
	}
}

// TestReconcileRegistryFiresOnTheSameInstants strengthens the count-based proof
// to an instant-based one: it is not enough that proc-gc fires 6 times in 30
// minutes, it must fire at exactly t0+5m, t0+10m, … — the instants a dedicated
// ticker would have chosen. A registry that resampled `now` instead of anchoring
// its deadline would drift by up to one granularity per period and pass the
// count check while failing this one.
func TestReconcileRegistryFiresOnTheSameInstants(t *testing.T) {
	const granularity = time.Second
	const period = 10 * time.Second

	var fired []time.Time
	r := newReconcileRegistry(silentLogger(), nil)
	r.register("driver", granularity, authorityAny, func(context.Context, time.Time) error { return nil })
	r.register("periodic", period, authorityAny, func(_ context.Context, now time.Time) error {
		fired = append(fired, now)
		return nil
	})

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	for i := 0; i < 100; i++ {
		r.runDue(context.Background(), clk.advance(granularity))
	}

	if len(fired) != 10 {
		t.Fatalf("expected 10 firings over 100s at a 10s period, got %d", len(fired))
	}
	for i, ts := range fired {
		want := regEpoch.Add(time.Duration(i+1) * period)
		if !ts.Equal(want) {
			t.Errorf("firing %d at %v; a dedicated ticker fires at %v (drift %v) — deadlines are not anchored",
				i, ts, want, ts.Sub(want))
		}
	}
}

// TestReconcileRegistryNoCatchupBurst pins the level-triggered contract. If the
// driver stalls (a paused VM, a long GC, a blocked pass), the registry must NOT
// replay every missed slot on the next sweep. Convergence passes are
// level-triggered — re-running them N times computes the same answer N times —
// and for anything that WRITES, a burst turns a stall into a storm.
func TestReconcileRegistryNoCatchupBurst(t *testing.T) {
	var runs int
	r := newReconcileRegistry(silentLogger(), nil)
	r.register("p", time.Second, authorityAny, func(context.Context, time.Time) error { runs++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())

	// One tick 10 minutes late: 600 slots elapsed.
	r.runDue(context.Background(), clk.advance(10*time.Minute))
	if runs != 1 {
		t.Fatalf("a 10-minute stall must produce ONE catch-up run, got %d (a 600× burst)", runs)
	}
	// And the schedule must resume cleanly on the next slot, not stay behind.
	r.runDue(context.Background(), clk.advance(time.Second))
	if runs != 2 {
		t.Fatalf("after the stall the pass must resume its normal cadence: runs=%d, want 2", runs)
	}
}

// --------------------------------------------------------------------------
// The leadership gate — the tuple's `leaderOnly` field.
// --------------------------------------------------------------------------

// TestReconcileRegistryLeaderGate proves the gate is enforced by the REGISTRY,
// not by each pass remembering to check. That matters because "the pass forgot
// to check leadership" is exactly how a leader-local decision becomes N brokers
// each forwarding the same write every tick.
//
// It also pins that a gated-off pass does NOT advance lastTick (it never ran, so
// claiming it did would make the R13 status surface lie) while still advancing
// its schedule, so a follower promoted to leader picks the pass up at the next
// slot rather than immediately replaying every slot it sat out.
func TestReconcileRegistryLeaderGate(t *testing.T) {
	var leader atomic.Bool
	var leaderRuns, localRuns int

	r := newReconcileRegistry(silentLogger(), leader.Load)
	r.register("leader-only", time.Second, authorityLeader, func(context.Context, time.Time) error { leaderRuns++; return nil })
	r.register("per-broker", time.Second, authorityAny, func(context.Context, time.Time) error { localRuns++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())

	// Phase 1 — follower for 5 ticks.
	for i := 0; i < 5; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	if leaderRuns != 0 {
		t.Fatalf("a follower ran a leader-only pass %d times", leaderRuns)
	}
	if localRuns != 5 {
		t.Fatalf("a per-broker pass must run on a follower: got %d, want 5", localRuns)
	}
	for _, s := range r.status() {
		if s.Name != "leader-only" {
			continue
		}
		if !s.LastTick.IsZero() {
			t.Error("a gated-off pass must not stamp lastTick — it never ran")
		}
		if s.Skips != 5 {
			t.Errorf("gated-off sweeps must be counted as skips: got %d, want 5", s.Skips)
		}
	}

	// Phase 2 — leadership arrives. The pass must pick up on the NEXT slot,
	// with no backlog replay.
	leader.Store(true)
	r.runDue(context.Background(), clk.advance(time.Second))
	if leaderRuns != 1 {
		t.Fatalf("after promotion the leader-only pass must run exactly once on the next slot, got %d", leaderRuns)
	}
}

// TestReconcileGateMirrorsLegacyCondition pins the gate predicate itself against
// the pre-R7 inline condition `!b.clusterMode || b.cl.node.IsLeader()`. The
// single-mode arm is the one that matters: a lone broker is always "the leader",
// and getting that wrong would silently disable port revocation on every
// non-cluster deployment.
func TestReconcileGateMirrorsLegacyCondition(t *testing.T) {
	single := &Broker{}
	if !single.reconcileLeaderGate() {
		t.Fatal("single mode must always pass the leader gate — there is no one else to be leader")
	}
	clustered := &Broker{clusterMode: true}
	if clustered.reconcileLeaderGate() {
		t.Fatal("cluster mode with no raft node wired must NOT pass the leader gate (fail closed)")
	}
}

// --------------------------------------------------------------------------
// The failure half of the interface (frozen by the #31 pass, tested here).
// --------------------------------------------------------------------------

// TestReconcileRegistryBackoff proves a failing pass backs off exponentially and
// recovers its normal cadence on success. Without this, a pass that proposes
// through raft (grow-lock) would re-propose into a running election on every
// slot; with an unbounded backoff, a pass that failed overnight would never
// retry again. Both ends are pinned.
func TestReconcileRegistryBackoff(t *testing.T) {
	var attempts int
	failing := errors.New("not leader")
	var fail atomic.Bool
	fail.Store(true)

	r := newReconcileRegistry(silentLogger(), nil)
	r.register("flaky", time.Second, authorityAny, func(context.Context, time.Time) error {
		attempts++
		if fail.Load() {
			return failing
		}
		return nil
	})

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())

	// 60 one-second slots. Unbacked-off this is 60 attempts; with doubling
	// (1s, 2s, 4s, 8s, 16s, 32s …) it must be far fewer.
	for i := 0; i < 60; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	if attempts >= 10 {
		t.Fatalf("a persistently failing pass attempted %d times in 60 slots — backoff is not engaging", attempts)
	}
	if attempts < 5 {
		t.Fatalf("backoff is too aggressive: only %d attempts in 60s; convergence must not be abandoned", attempts)
	}
	st := r.status()[0]
	if st.Failures != attempts || st.LastErr != failing.Error() {
		t.Fatalf("status must expose the consecutive-failure count and the last error: %+v", st)
	}

	// Recovery: one success must clear the backoff and restore cadence.
	fail.Store(false)
	before := attempts
	for i := 0; i < 120; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	recovered := attempts - before
	if recovered < 100 {
		t.Fatalf("after recovery the pass must return to its 1s cadence: only %d runs in 120 slots", recovered)
	}
	if st := r.status()[0]; st.Failures != 0 || st.LastErr != "" {
		t.Fatalf("a successful run must reset the failure state: %+v", st)
	}
}

// TestReconcileRegistryBackoffIsCapped pins the ceiling: after many failures the
// pass must still retry within reconcileMaxBackoff, not drift toward hours.
func TestReconcileRegistryBackoffIsCapped(t *testing.T) {
	p := &reconcilePass{name: "x", interval: time.Second, failures: 40}
	p.nextDue = regEpoch
	p.backoffLocked(regEpoch)
	if d := p.nextDue.Sub(regEpoch); d != reconcileMaxBackoff {
		t.Fatalf("backoff after 40 failures = %v, want the cap %v (an uncapped shift overflows or abandons the pass)", d, reconcileMaxBackoff)
	}
}

// --------------------------------------------------------------------------
// Wiring guards.
// --------------------------------------------------------------------------

// TestReconcileRegistryRejectsBadRegistration pins that a mis-registered pass is
// a loud panic at wiring time. A silently dropped or silently duplicated
// reconciliation pass is the exact failure class R7 exists to remove, so it must
// never be expressible.
func TestReconcileRegistryRejectsBadRegistration(t *testing.T) {
	ok := func(context.Context, time.Time) error { return nil }
	cases := []struct {
		name string
		call func(r *reconcileRegistry)
	}{
		{"empty name", func(r *reconcileRegistry) { r.register("", time.Second, authorityAny, ok) }},
		{"zero interval", func(r *reconcileRegistry) { r.register("p", 0, authorityAny, ok) }},
		{"negative interval", func(r *reconcileRegistry) { r.register("p", -time.Second, authorityAny, ok) }},
		{"nil fn", func(r *reconcileRegistry) { r.register("p", time.Second, authorityAny, nil) }},
		{"duplicate name", func(r *reconcileRegistry) {
			r.register("dup", time.Second, authorityAny, ok)
			r.register("dup", time.Second, authorityAny, ok)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s must panic at wiring time", tc.name)
				}
			}()
			tc.call(newReconcileRegistry(silentLogger(), nil))
		})
	}
}

// TestReconcileRegistryLateRegistrationIsAppendOnly pins that a pass registered
// AFTER start() (R8 registers the home/roster delivery channel this way) is
// scheduled correctly instead of firing immediately or never.
func TestReconcileRegistryLateRegistrationIsAppendOnly(t *testing.T) {
	var early, late int
	r := newReconcileRegistry(silentLogger(), nil)
	r.register("early", time.Second, authorityAny, func(context.Context, time.Time) error { early++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	r.runDue(context.Background(), clk.advance(time.Second))

	r.register("late", time.Second, authorityAny, func(context.Context, time.Time) error { late++; return nil })
	for i := 0; i < 3; i++ {
		r.runDue(context.Background(), clk.advance(time.Second))
	}
	if early != 4 {
		t.Fatalf("early pass runs = %d, want 4", early)
	}
	if late != 3 {
		t.Fatalf("a pass registered after start() must begin firing on its own cadence: got %d, want 3", late)
	}
}

// TestReconcileRegistryStopsOnContextCancel pins that a cancelled loop context
// stops the sweep mid-way instead of driving the remaining passes into a
// shutting-down broker.
func TestReconcileRegistryStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var first, second int
	r := newReconcileRegistry(silentLogger(), nil)
	r.register("first", time.Second, authorityAny, func(context.Context, time.Time) error { first++; cancel(); return nil })
	r.register("second", time.Second, authorityAny, func(context.Context, time.Time) error { second++; return nil })

	clk := newFakeClock(regEpoch)
	r.start(clk.Now())
	r.runDue(ctx, clk.advance(time.Second))

	if first != 1 || second != 0 {
		t.Fatalf("a cancel inside a sweep must stop the remaining passes: first=%d second=%d", first, second)
	}
}

// TestReconcileRegistryConcurrentDriversSerialize is the registry-level half of
// the "two brokers do not double-write" requirement: even if something drives
// the same registry from several goroutines, a pass's fn is never re-entered
// concurrently. Run with -race, this is what catches a scheduler that mutates
// pass state outside the lock.
func TestReconcileRegistryConcurrentDriversSerialize(t *testing.T) {
	var inFlight atomic.Int32
	var overlaps atomic.Int32
	var runs atomic.Int32

	r := newReconcileRegistry(silentLogger(), nil)
	r.register("p", time.Millisecond, authorityAny, func(context.Context, time.Time) error {
		if inFlight.Add(1) > 1 {
			overlaps.Add(1)
		}
		runs.Add(1)
		inFlight.Add(-1)
		return nil
	})
	clk := newFakeClock(regEpoch)
	r.start(clk.Now())

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				r.runDue(context.Background(), clk.advance(time.Millisecond))
			}
		}()
	}
	wg.Wait()

	if overlaps.Load() != 0 {
		t.Fatalf("pass fn was re-entered concurrently %d times", overlaps.Load())
	}
	if runs.Load() == 0 {
		t.Fatal("no runs recorded")
	}
}

// TestCoreReconcilePassesAreRegisteredAsSpecified is the wiring contract for the
// production passes: names, cadences and — most importantly — which ones are
// leader-only. A pass that silently loses its leaderOnly bit becomes N brokers
// racing the same replicated write; a pass that silently gains one stops running
// on followers that need it locally.
func TestCoreReconcilePassesAreRegisteredAsSpecified(t *testing.T) {
	b := &Broker{}
	b.cfg.Logger = silentLogger()
	b.cfg.ReconcileInterval = time.Second
	b.cfg.ProcGCInterval = 5 * time.Minute
	b.cfg.XferReapInterval = 5 * time.Minute
	b.cfg.GrowLockReapInterval = 30 * time.Second
	b.cfg.UpgradeLockReapInterval = 30 * time.Second
	b.cfg.HomeDeliverInterval = 5 * time.Second
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.registerCoreReconcilePasses()

	const any, leader = "any", "leader"
	want := []reconcilePassStatus{
		{Name: "node-states", Interval: time.Second, Authority: any},
		{Name: "ports", Interval: time.Second, Authority: leader},
		{Name: "tunnel-sessions", Interval: time.Second, Authority: any},
		{Name: "proc-gc", Interval: 5 * time.Minute, Authority: any},
		{Name: "xfer-orphan-reap", Interval: 5 * time.Minute, Authority: any},       // #58/P10: home-authoritative, per-broker (see reaperCaughtUp+homeOwnsXferBucket)
		{Name: "xfer-inflight-finalize", Interval: 5 * time.Minute, Authority: any}, // R16 #57: finalize-on-recovery for stranded in-flight transfers (shares the reap cadence)
		{Name: "grow-lock", Interval: 30 * time.Second, Authority: leader},
		{Name: "upgrade-lock", Interval: 30 * time.Second, Authority: leader},
		// R8a P1: leader-gated — it re-delivers what the leader's homeForRegister computes
		// from replicated rows; N brokers pushing the same assignment is pure amplification.
		{Name: "home-delivery", Interval: 5 * time.Second, Authority: leader},
		// B7 deliberately did NOT add leader-maintenance / proxy-reap passes here. Both available
		// shapes are regressions and the reasoning is recorded at the top of this file's production
		// counterpart (reconcile_registry.go, "WHY THE TWO LEADER DUTIES STAYED OUT"). If a future
		// increment revisits it, this list is where the decision becomes visible again.
	}
	got := b.reconcilers.status()
	if len(got) != len(want) {
		t.Fatalf("registered %d passes, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.Name != w.Name || g.Interval != w.Interval || g.Authority != w.Authority {
			t.Errorf("pass %d: got (%s, %v, authority=%q), want (%s, %v, authority=%q)",
				i, g.Name, g.Interval, g.Authority, w.Name, w.Interval, w.Authority)
		}
	}
	// The driving ticker must be the shortest interval, or a 1s pass would run
	// at whatever coarser cadence happened to win.
	if g := b.reconcilers.granularity(); g != time.Second {
		t.Fatalf("granularity = %v, want the shortest registered interval (1s)", g)
	}
}

// TestReconcilePassesUseInjectedIntervals guards the defaults wiring: a pass that
// captured a zero interval would panic at registration, and one that captured a
// hardcoded constant would ignore broker.yaml. Drive New()'s defaulting.
func TestReconcilePassesUseInjectedIntervals(t *testing.T) {
	b := &Broker{}
	b.cfg.Logger = silentLogger()
	b.cfg.ReconcileInterval = 250 * time.Millisecond
	b.cfg.ProcGCInterval = 7 * time.Second
	b.cfg.XferReapInterval = 11 * time.Second
	b.cfg.GrowLockReapInterval = 13 * time.Second
	b.cfg.UpgradeLockReapInterval = 17 * time.Second
	b.cfg.HomeDeliverInterval = 19 * time.Second
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.registerCoreReconcilePasses()

	byName := map[string]time.Duration{}
	for _, s := range b.reconcilers.status() {
		byName[s.Name] = s.Interval
	}
	for name, want := range map[string]time.Duration{
		"node-states":      250 * time.Millisecond,
		"proc-gc":          7 * time.Second,
		"xfer-orphan-reap": 11 * time.Second,
		"grow-lock":        13 * time.Second,
		"home-delivery":    19 * time.Second,
		"upgrade-lock":     17 * time.Second,
	} {
		if byName[name] != want {
			t.Errorf("pass %q interval = %v, want the configured %v", name, byName[name], want)
		}
	}
	if g := b.reconcilers.granularity(); g != 250*time.Millisecond {
		t.Fatalf("granularity = %v, want 250ms", g)
	}
}
