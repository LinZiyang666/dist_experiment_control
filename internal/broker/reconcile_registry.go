package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// reconcile_registry.go (R7a) — the broker's periodic-reconciliation registry.
//
// WHY IT EXISTS
// -------------
// Before R7 the broker converged state from a hand-rolled `select` over two
// tickers, and every convergence duty that was NOT one of those three inline
// bodies had to invent its own cadence — or, far more often, ran EXACTLY ONCE at
// boot and never again (#58/P10: the orphan xfer-object reaper). "Runs once at
// boot, behind a gate that is structurally false at boot" is not convergence; it
// is a no-op with a comment. The registry makes "this duty runs forever, on this
// cadence, on this node role" the only way to express a reconciliation pass.
//
// THE TUPLE IS (name, interval, leaderOnly, lastTick, fn) — NOT NEGOTIABLE
// ------------------------------------------------------------------------
// The pre-R7 loop already exhibited THREE heterogeneous shapes, which a flat
// single-interval table structurally cannot express:
//
//	node-states       ReconcileInterval   per-broker-local (reads livenessDB)
//	ports             ReconcileInterval   LEADER-ONLY (a replicated decision)
//	tunnel-sessions   ReconcileInterval   per-broker (local proxy fds)
//	proc-gc           ProcGCInterval      a DIFFERENT cadence entirely
//
// so `interval` and `leaderOnly` are both per-pass. `lastTick` lands in the tuple
// NOW (not when R13 wants it) because retrofitting an observability field into a
// scheduler after four batches have registered passes against it is how interface
// freezes get broken.
//
// THE ONE-VOTE-VETO INVARIANT (read this before adding a pass)
// ------------------------------------------------------------
// A pass may ONLY:
//
//	(1) read the EXPECTED state,
//	(2) compare it against the ACTUAL state,
//	(3) call an ALREADY-EXISTING, ALREADY-IDEMPOTENT command path.
//
// A pass may NOT invent policy. The reason is arithmetic, not aesthetic: a
// one-shot action that is wrong destroys state once; the same action on a 30s
// cadence destroys state 2880 times a day. Periodizing a destructive action whose
// safety argument depended on "we only do this at boot, when nothing is in
// flight" is strictly worse than the bug it was meant to fix. Every pass
// registered here carries a comment naming the pre-existing idempotent command
// path it calls, and every pass has three hermetic tests: converged ⇒ zero
// side effects across consecutive ticks; drift ⇒ re-converges; two brokers ⇒
// exactly one writer.
//
// SCHEDULING MODEL
// ----------------
// One driving ticker at granularity() == min(interval) wakes the loop; each pass
// fires when its own anchored deadline is reached. Deadlines are ANCHORED
// (nextDue += interval), not resampled from wall-clock, so a pass whose interval
// is a multiple of the granularity fires on EXACTLY the same instants a dedicated
// time.Ticker would have — that is what makes the pre-R7 → post-R7 rewrite
// provably behavior-equivalent under a fake clock. A pass that falls far behind
// (a long stall, a paused VM) skips the missed slots instead of firing a catch-up
// burst: convergence passes are level-triggered, so replaying missed edges is
// both pointless and, for anything that writes, actively harmful.
//
// The registry starts NO goroutines and owns NO timers. It is driven entirely by
// its caller's clock, which is what lets the equivalence proof run in
// microseconds of fake time and what keeps it invisible to the repo's
// NumGoroutine/fd leak gate.

// reconcilePassFn is one convergence pass. It receives the loop's context (so a
// shutdown cancels mid-pass) and the tick instant from the broker's injectable
// clock (b.cfg.Now) — a pass must NEVER call time.Now() directly, or it becomes
// untestable and drifts from the rest of the tick.
//
// Returning a non-nil error means "this pass could not converge"; the registry
// applies exponential backoff to its next deadline and records the error for
// status(). Passes that merely LOG their internal per-item failures (the pre-R7
// bodies all did) must return nil, or the rewrite would not be behavior-equivalent.
type reconcilePassFn func(ctx context.Context, now time.Time) error

// reconcilePass is the registry tuple plus its scheduling/observability state.
// Every mutable field is guarded by reconcileRegistry.mu.
type reconcilePass struct {
	// --- the frozen tuple ---
	name       string
	interval   time.Duration
	leaderOnly bool
	lastTick   time.Time // last INVOCATION of fn; zero == never invoked
	fn         reconcilePassFn

	// --- scheduling ---
	nextDue time.Time // anchored deadline; fires when !nextDue.After(now)

	// --- observability (R13 consumes these; it does not change the mechanism) ---
	lastEval time.Time // last time the pass came due, whether or not it ran
	lastErr  string
	runs     uint64
	skips    uint64 // came due but was gated off by leaderOnly
	failures int    // consecutive failures; drives backoff, reset on success
}

// reconcilePassStatus is an immutable snapshot of one pass for status endpoints
// and tests. Copied out under the lock so callers never touch live state.
type reconcilePassStatus struct {
	Name       string
	Interval   time.Duration
	LeaderOnly bool
	LastTick   time.Time
	LastEval   time.Time
	LastErr    string
	Runs       uint64
	Skips      uint64
	Failures   int
}

const (
	// reconcileMaxBackoff caps the exponential backoff a failing pass earns. A
	// pass that has been failing for an hour must still retry every few minutes:
	// the whole point of the registry is that convergence is not abandoned.
	reconcileMaxBackoff = 5 * time.Minute

	// reconcileMaxBackoffShift bounds the shift so the doubling can never
	// overflow a time.Duration on a pass that has failed thousands of times.
	reconcileMaxBackoffShift = 20
)

// reconcileRegistry holds the registered passes and drives them from a clock the
// caller supplies. Safe for concurrent use; production drives it from a single
// goroutine, tests drive it from several.
type reconcileRegistry struct {
	// runMu serializes runDue so two concurrent drivers can never invoke the
	// same pass's fn at the same time. Held for the whole sweep, INCLUDING the
	// fn calls, so it must never be taken while holding mu.
	runMu sync.Mutex

	mu        sync.Mutex
	passes    []*reconcilePass
	byName    map[string]*reconcilePass
	started   bool
	startedAt time.Time

	// isLeader is the leadership gate for leaderOnly passes. Nil means "always
	// the leader" (single mode). Evaluated ONCE per sweep so every leaderOnly
	// pass in one tick sees a consistent view of leadership.
	isLeader func() bool

	logger *slog.Logger
}

func newReconcileRegistry(logger *slog.Logger, isLeader func() bool) *reconcileRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	return &reconcileRegistry{
		byName:   map[string]*reconcilePass{},
		isLeader: isLeader,
		logger:   logger,
	}
}

// register appends a pass. Append-only by design: later batches (R8's delivery
// channel, R9's lifecycle watchdogs) add passes without touching the scheduler.
//
// Duplicate names and non-positive intervals are PROGRAMMING errors, not runtime
// conditions — a silently dropped reconciliation pass is precisely the failure
// mode R7 exists to eliminate, so they panic at wiring time rather than degrade
// in production.
func (r *reconcileRegistry) register(name string, interval time.Duration, leaderOnly bool, fn reconcilePassFn) {
	if name == "" {
		panic("broker: reconcile pass registered with an empty name")
	}
	if interval <= 0 {
		panic(fmt.Sprintf("broker: reconcile pass %q registered with a non-positive interval %v", name, interval))
	}
	if fn == nil {
		panic(fmt.Sprintf("broker: reconcile pass %q registered with a nil fn", name))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		panic(fmt.Sprintf("broker: duplicate reconcile pass %q", name))
	}
	p := &reconcilePass{name: name, interval: interval, leaderOnly: leaderOnly, fn: fn}
	// A pass registered after start() anchors from now; one registered before
	// anchors at start(), so all boot-time passes share a single epoch.
	if r.started {
		p.nextDue = r.startedAt.Add(interval)
		for !p.nextDue.After(r.startedAt) {
			p.nextDue = p.nextDue.Add(interval)
		}
	}
	r.passes = append(r.passes, p)
	r.byName[name] = p
}

// start anchors every registered pass's first deadline at now+interval — the
// exact instant a time.NewTicker(interval) created at `now` would first fire.
// Boot-time warm-up calls (the broker runs a few passes once before the loop)
// are the caller's business; start() only sets the schedule.
func (r *reconcileRegistry) start(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = true
	r.startedAt = now
	for _, p := range r.passes {
		p.nextDue = now.Add(p.interval)
	}
}

// granularity is the driving ticker's period: the shortest registered interval.
// Every pass whose interval is an exact multiple of it fires on precisely the
// instants a dedicated ticker would have. Returns 0 when nothing is registered
// (the caller must treat that as a wiring bug).
func (r *reconcileRegistry) granularity() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var g time.Duration
	for _, p := range r.passes {
		if g == 0 || p.interval < g {
			g = p.interval
		}
	}
	return g
}

// runDue invokes every pass whose deadline has arrived and returns the names it
// actually invoked (in registration order) — the return value is for tests and
// tracing; production ignores it.
//
// Deadlines advance BEFORE fn runs, so a slow pass delays only itself and never
// produces a catch-up burst on the next sweep.
func (r *reconcileRegistry) runDue(ctx context.Context, now time.Time) []string {
	r.runMu.Lock()
	defer r.runMu.Unlock()

	leader := true
	if r.isLeader != nil {
		leader = r.isLeader()
	}

	r.mu.Lock()
	due := make([]*reconcilePass, 0, len(r.passes))
	for _, p := range r.passes {
		if !p.nextDue.After(now) {
			p.lastEval = now
			p.advanceLocked(now)
			due = append(due, p)
		}
	}
	r.mu.Unlock()

	ran := make([]string, 0, len(due))
	for _, p := range due {
		if ctx.Err() != nil {
			return ran
		}
		if p.leaderOnly && !leader {
			r.mu.Lock()
			p.skips++
			r.mu.Unlock()
			continue
		}
		err := p.fn(ctx, now)

		r.mu.Lock()
		p.lastTick = now
		p.runs++
		if err != nil {
			p.failures++
			p.lastErr = err.Error()
			p.backoffLocked(now)
			failures, next := p.failures, p.nextDue
			r.mu.Unlock()
			r.logger.Warn("broker: reconcile pass failed",
				"pass", p.name, "err", err, "consecutive_failures", failures, "next_attempt", next)
		} else {
			p.failures = 0
			p.lastErr = ""
			r.mu.Unlock()
		}
		ran = append(ran, p.name)
	}
	return ran
}

// advanceLocked moves the anchored deadline forward by exactly one interval, or —
// if the driver was stalled long enough that several slots elapsed — to the next
// slot strictly after now. Missed slots are DROPPED, never replayed: these are
// level-triggered convergence passes, so a burst would do the same work N times.
func (p *reconcilePass) advanceLocked(now time.Time) {
	p.nextDue = p.nextDue.Add(p.interval)
	if p.nextDue.After(now) {
		return
	}
	behind := now.Sub(p.nextDue)
	p.nextDue = p.nextDue.Add((behind/p.interval + 1) * p.interval)
}

// backoffLocked pushes a failing pass's deadline out exponentially (interval,
// 2×, 4×, … capped at reconcileMaxBackoff). It only ever DELAYS: a pass whose
// normal interval already exceeds the backoff keeps its normal cadence.
func (p *reconcilePass) backoffLocked(now time.Time) {
	shift := p.failures - 1
	if shift < 0 {
		shift = 0
	}
	if shift > reconcileMaxBackoffShift {
		shift = reconcileMaxBackoffShift
	}
	d := p.interval << uint(shift)
	if d > reconcileMaxBackoff || d <= 0 {
		d = reconcileMaxBackoff
	}
	if next := now.Add(d); next.After(p.nextDue) {
		p.nextDue = next
	}
}

// status snapshots every pass, in registration order.
func (r *reconcileRegistry) status() []reconcilePassStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]reconcilePassStatus, 0, len(r.passes))
	for _, p := range r.passes {
		out = append(out, reconcilePassStatus{
			Name:       p.name,
			Interval:   p.interval,
			LeaderOnly: p.leaderOnly,
			LastTick:   p.lastTick,
			LastEval:   p.lastEval,
			LastErr:    p.lastErr,
			Runs:       p.runs,
			Skips:      p.skips,
			Failures:   p.failures,
		})
	}
	return out
}
