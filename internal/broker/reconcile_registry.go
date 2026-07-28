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
// THE TUPLE IS (name, interval, authority, lastTick, fn) — NOT NEGOTIABLE
// -----------------------------------------------------------------------
// The pre-R7 loop already exhibited THREE heterogeneous shapes, which a flat
// single-interval table structurally cannot express:
//
//	node-states       ReconcileInterval   per-broker-local (reads livenessDB)
//	ports             ReconcileInterval   LEADER-ONLY (a replicated decision)
//	tunnel-sessions   ReconcileInterval   per-broker (local proxy fds)
//	proc-gc           ProcGCInterval      a DIFFERENT cadence entirely
//
// so `interval` and the authority gate are both per-pass. `lastTick` lands in the
// tuple NOW (not when R13 wants it) because retrofitting an observability field
// into a scheduler after four batches have registered passes against it is how
// interface freezes get broken.
//
// The third element was a `leaderOnly bool` until B7. It became a named
// passAuthority for one reason: a bool can only ever answer "leader or not", so
// the SECOND kind of gate — and there will be one, this file already hosts
// per-broker, leader-gated and home-authoritative duties — would have had to
// arrive as a second bool, and two bools describing one decision is how a field
// acquires two meanings. Naming the authority makes adding a third a one-line
// addition to a per-sweep table instead of a new column. NOT NEGOTIABLE still
// stands: what is frozen is the SHAPE (one name, one cadence, one authority, one
// liveness stamp, one function), not the spelling of the authority.
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
//
// WHY THE TWO LEADER DUTIES STAYED OUT (B7, a permanent decision)
// ---------------------------------------------------------------
// The refactor roadmap's B7 asked for driveProxyReconcile and driveLeaderMaintenance to move in here
// from the observe ticker, plus an "own-goroutine-with-timeout" execution mode to hold them. Neither
// half survived contact with the code, and both conclusions are permanent rather than deferred.
//
// The execution mode is refused by the paragraph above: a pass on its own goroutine breaks the
// fake-clock equivalence proof (every test here asserts side effects are visible once runDue returns)
// and makes the registry visible to the leak gate. It would also be decorative — both duties are
// zero-argument methods, so a deadline the runner holds cannot interrupt them. The calls that genuinely
// could hang forever were two JS replica observations reachable from leader maintenance; those are now
// bounded at their own seams (clusterwrite.go), where a deadline actually cancels something. What was
// missing was visibility, not interruption, so the passes gained lastDur/maxDur/overruns instead.
//
// The move itself has only two available shapes and both are regressions:
//
//	(a) move the two duties and leave observeOnce on the observe ticker. They then run on TWO
//	    goroutines: ReconcileMembershipOnLeadership (forward-completing a half-done AddNode) could
//	    interleave with driveInFlightOperations (advancing that same op), giving one node two concurrent
//	    proposers of membership phase transitions — the no-silent-fork hazard §8.1 exists to prevent.
//	    observeOnce's driveAutoRebalanceOnReturn and driveProxyReconcile would likewise both move
//	    __proxy__ homes concurrently. The idempotency argument (phase writes are baked WHERE phase IN
//	    (preds)) covers double APPLICATION, not the interleaving of two DIFFERENT transitions.
//
//	(b) move the whole observe tick so the order is preserved on one goroutine. That puts its 2s
//	    scatter-gather (observePollWindow) under runMu, and node-states / ports / tunnel-sessions run at
//	    ReconcileInterval = 1s. Every 5s those three would be held for up to 2s by an unrelated duty —
//	    a convergence-latency regression on the passes that today are unaffected because they are on a
//	    different goroutine.
//
// Today's shape has the property both alternatives lose: the observe ticker is a single goroutine on
// which fsArm.observeLeadership, the leadership edges, ReconcileMembershipOnLeadership,
// driveLeaderMaintenance, driveProxyReconcile and observeOnce run in that exact order, and nothing
// else shares their lock. That IS the serialization argument; moving them would require re-deriving it
// from scratch for no gain the observability work did not already deliver.

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

// passAuthority names who may run a pass. Adding a member is how a new kind of gate arrives: name it,
// evaluate it once in runDue's per-sweep table, and every pass that declares it is gated consistently.
// The zero value is authorityAny, so a pass that says nothing is an unconditional per-broker duty —
// the same default the old `leaderOnly: false` had.
type passAuthority int

const (
	// authorityAny: every broker runs it, leader or not. Per-broker-local convergence (node states,
	// tunnel sessions, proc GC) and anything that must keep observing while quorum is LOST.
	authorityAny passAuthority = iota
	// authorityLeader: only the raft leader. In single mode there is no raft leader to ask, and the
	// registry's nil isLeader means "always permitted" — dropping that would silently disable every
	// leader-gated pass on the entire current fleet, which runs single-mode brokers.
	authorityLeader
)

func (a passAuthority) String() string {
	if a == authorityLeader {
		return "leader"
	}
	return "any"
}

// reconcilePass is the registry tuple plus its scheduling/observability state.
// Every mutable field is guarded by reconcileRegistry.mu.
type reconcilePass struct {
	// --- the frozen tuple ---
	name string
	// interval is the pass's own cadence. It doubles as its OVERRUN BUDGET: a pass that takes longer
	// than its own interval has, by definition, stopped keeping up with itself, and because runDue holds
	// runMu for the whole sweep it is also holding every other pass up. See overruns below.
	interval time.Duration
	// authority names WHO is permitted to run this pass, replacing the old `leaderOnly bool`.
	//
	// IT IS A VALUE, NOT A CLOSURE, AND THAT IS THE POINT. The roadmap asked for
	// `leaderOnly bool -> authority func() bool`, and a closure per pass would reintroduce exactly the
	// hazard the bool version avoided by accident: runDue evaluates leadership ONCE per sweep so every
	// gated pass in one tick sees a consistent view (see the isLeader field). Per-pass closures are
	// evaluated per pass, so leadership lost mid-sweep would let the first half of a tick believe it is
	// leader and the second half not — and one of the passes moving in here drives membership
	// transitions, where a split view within one tick is the no-silent-fork hazard §8.1 exists to
	// prevent. A value lets the registry evaluate each distinct authority once and hand every pass the
	// same answer, so the property is structural instead of a rule someone has to remember.
	authority passAuthority
	lastTick  time.Time // last INVOCATION of fn; zero == never invoked
	fn        reconcilePassFn

	// --- scheduling ---
	nextDue time.Time // anchored deadline; fires when !nextDue.After(now)

	// --- observability (R13 consumes these; it does not change the mechanism) ---
	lastEval time.Time // last time the pass came due, whether or not it ran
	lastErr  string
	runs     uint64
	skips    uint64 // came due but was gated off by authority
	failures int    // consecutive failures; drives backoff, reset on success

	// lastDur / maxDur / overruns are DURATION observability, and they are deliberately NOT a timeout.
	//
	// B7 considered giving each pass a cancellable budget. It would have been decorative: the two duties
	// this batch moves in take no context at all, so a deadline the runner holds cannot interrupt them,
	// and the calls that genuinely COULD hang indefinitely (two JS replica observations reachable from
	// leader maintenance) are now bounded at their own seams instead — where a deadline actually
	// cancels something. What remained missing was not interruption but VISIBILITY: "the op driver is
	// wedged" was unobservable, and the op controller is the convergence duty most likely to wedge.
	//
	// Measured with the injected clock, so the fake-clock equivalence tests stay deterministic and the
	// registry still starts no goroutines and owns no timers.
	lastDur  time.Duration
	maxDur   time.Duration
	overruns uint64 // completions where the pass took longer than its own interval
}

// reconcilePassStatus is an immutable snapshot of one pass for status endpoints
// and tests. Copied out under the lock so callers never touch live state.
type reconcilePassStatus struct {
	Name      string
	Interval  time.Duration
	Authority string // "any" / "leader" — what gated this pass, in words a status reader can act on
	LastTick  time.Time
	LastEval  time.Time
	LastErr   string
	Runs      uint64
	Skips     uint64
	Failures  int
	LastDur   time.Duration
	MaxDur    time.Duration
	Overruns  uint64
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

	// now is the injectable clock used ONLY to measure how long a pass took. It is set by the broker
	// after construction (b.cfg.Now) and left nil by tests that do not care; see clockNow.
	now func() time.Time

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

// clockNow reads the injected clock, falling back to the sweep instant when none is wired.
//
// The fallback is what keeps the registry's "starts no goroutines, owns no timers, driven entirely by
// its caller's clock" property intact: with no clock every measured duration is exactly zero, which is
// wrong but INERT — it can never influence a scheduling decision, and the fake-clock equivalence tests
// stay deterministic. Production wires b.cfg.Now, so the observability is real there; a test that wants
// to assert on durations wires its own fake clock and gets deterministic values rather than wall time.
func (r *reconcileRegistry) clockNow(fallback time.Time) time.Time {
	if r.now == nil {
		return fallback
	}
	return r.now()
}

// register appends a pass. Append-only by design: later batches (R8's delivery
// channel, R9's lifecycle watchdogs) add passes without touching the scheduler.
//
// Duplicate names and non-positive intervals are PROGRAMMING errors, not runtime
// conditions — a silently dropped reconciliation pass is precisely the failure
// mode R7 exists to eliminate, so they panic at wiring time rather than degrade
// in production.
// authority declares who may run the pass (authorityAny / authorityLeader). See the field's comment on
// why it is a value rather than a per-pass closure.
func (r *reconcileRegistry) register(name string, interval time.Duration, authority passAuthority, fn reconcilePassFn) {
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
	p := &reconcilePass{name: name, interval: interval, authority: authority, fn: fn}
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

	// THE PER-SWEEP AUTHORITY TABLE. Every distinct authority is resolved exactly once here, so all
	// passes in this sweep are gated against ONE answer. authorityAny is unconditional by definition;
	// authorityLeader asks the registry's single leadership source, whose nil case means single mode
	// (always permitted) — the whole current fleet.
	permitted := map[passAuthority]bool{authorityAny: true, authorityLeader: true}
	if r.isLeader != nil {
		permitted[authorityLeader] = r.isLeader()
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
		if !permitted[p.authority] {
			r.mu.Lock()
			p.skips++
			r.mu.Unlock()
			continue
		}
		started := r.clockNow(now)
		err := p.fn(ctx, now)
		dur := r.clockNow(now).Sub(started)

		r.mu.Lock()
		p.lastTick = now
		p.runs++
		// Duration observability, NOT a timeout — the registry cannot interrupt a pass that does not
		// take a context, and pretending otherwise is how a decorative bound gets shipped. What this
		// makes possible is noticing that a pass is wedged at all, which for the operation controller
		// was previously invisible.
		p.lastDur = dur
		if dur > p.maxDur {
			p.maxDur = dur
		}
		overran := dur > p.interval
		if overran {
			p.overruns++
		}
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
		if overran {
			// One line per overrun, not per tick: a pass that is merely slow says so once each time it
			// happens, and runMu means the cost was paid by every other pass in this sweep too.
			r.logger.Warn("broker: reconcile pass exceeded its own interval — it is holding the sweep",
				"pass", p.name, "took", dur, "interval", p.interval)
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
			Name:      p.name,
			Interval:  p.interval,
			Authority: p.authority.String(),
			LastTick:  p.lastTick,
			LastEval:  p.lastEval,
			LastErr:   p.lastErr,
			Runs:      p.runs,
			Skips:     p.skips,
			Failures:  p.failures,
			LastDur:   p.lastDur,
			MaxDur:    p.maxDur,
			Overruns:  p.overruns,
		})
	}
	return out
}
