package agent

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// origin: gotcha #72 — the bounded-teardown ladder. The field defect was ORDER (close before
// cancel) compounded by an UNBOUNDED close: nats.go's Close takes the same mutex doReconnect holds
// across a no-deadline dial/handshake, so the cancel the redial watchdog wanted to deliver sat
// behind a block that lasted 10m58s. These tests drive the real ladder with a deliberately wedged
// connection and assert the two properties the fix promises: cancel happens FIRST, and teardown
// RETURNS within budget+grace — never by abandoning a goroutine (which the leak gate would catch,
// and which the ledger names as the forbidden fake fix).

// blockingConn is a net.Conn whose Read/Write park forever until the conn is poisoned
// (SetDeadline in the past or Close) — the deterministic stand-in for a half-dead link.
type blockingConn struct {
	mu      sync.Mutex
	dead    chan struct{}
	closed  bool
	ignores bool // adversarial: ignore Close/SetDeadline entirely (the escalate path)
	// onRelease lets a test drop a REAL socket alongside the wrapper, so the fd leak gate measures
	// the ladder rather than the fixture's own bookkeeping.
	onRelease func()
}

func newBlockingConn(ignores bool) *blockingConn {
	return &blockingConn{dead: make(chan struct{}), ignores: ignores}
}

func (c *blockingConn) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.dead)
		if c.onRelease != nil {
			c.onRelease()
		}
	}
}

func (c *blockingConn) Read(b []byte) (int, error)  { <-c.dead; return 0, net.ErrClosed }
func (c *blockingConn) Write(b []byte) (int, error) { <-c.dead; return 0, net.ErrClosed }
func (c *blockingConn) Close() error {
	if c.ignores {
		return nil // adversarial: refuses to die — forces the escalation rung
	}
	c.release()
	return nil
}
func (c *blockingConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4zero} }
func (c *blockingConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero} }
func (c *blockingConn) SetDeadline(t time.Time) error {
	if c.ignores {
		return nil
	}
	if !t.IsZero() && t.Before(time.Now()) {
		c.release()
	}
	return nil
}
func (c *blockingConn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *blockingConn) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

// teardownTestAgent also shrinks the ladder's budgets to milliseconds. The RATIOS and the ORDER are
// what these tests assert; sitting through the production 10s+10s would add a minute to `make test`
// and prove nothing extra (the production values are a documented contract, pinned in
// TestTeardownBudgetsMatchThePublishedContract).
func teardownTestAgent(t *testing.T) *Agent {
	t.Helper()
	oldClose, oldGrace := closeBudget, poisonGrace
	closeBudget, poisonGrace = 150*time.Millisecond, 150*time.Millisecond
	t.Cleanup(func() { closeBudget, poisonGrace = oldClose, oldGrace })
	return &Agent{cfg: Config{Logger: slog.New(slog.DiscardHandler), SID: "lab", NID: "n1"}}
}

// The published recovery bound (deploy-tier-gotchas #72, usage.md §9.9) is arithmetic over these
// two values plus redialAfter. Pin them so a quiet edit cannot silently change what the docs and
// drill 98's derived poll budget promise.
func TestTeardownBudgetsMatchThePublishedContract(t *testing.T) {
	if closeBudget != 10*time.Second || poisonGrace != 10*time.Second {
		t.Fatalf("teardown budgets = %s/%s, want 10s/10s — the ≤60s recovery bound in gotcha #72 and usage.md §9.9 is derived from these",
			closeBudget, poisonGrace)
	}
}

// wedgedFinalizer builds a finalizer whose close call parks until the tracked conn is poisoned.
// closeStarted/closeReturned let a test observe the ladder without sleeping on guesses.
type wedgedFinalizer struct {
	fin           *sessionFinalizer
	conn          *blockingConn
	cancelledAt   atomic.Int64
	closeStarted  chan struct{}
	closeReturned chan struct{}
}

func newWedgedFinalizer(t *testing.T, a *Agent, ignoresPoison bool) *wedgedFinalizer {
	t.Helper()
	w := &wedgedFinalizer{
		conn:          newBlockingConn(ignoresPoison),
		closeStarted:  make(chan struct{}),
		closeReturned: make(chan struct{}),
	}
	tr := newConnTracker(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return w.conn, nil
	})
	// Register the conn the way a real dial would, so poisoning has something to reach.
	if _, err := tr.Dial("tcp", "127.0.0.1:4222"); err != nil {
		t.Fatalf("tracker dial: %v", err)
	}
	w.fin = &sessionFinalizer{
		tracker: tr,
		agent:   a,
		cancel:  func() { w.cancelledAt.Store(time.Now().UnixNano()) },
	}
	return w
}

// closeStub replaces the *nats.Conn close with a park-until-poisoned stub. finalizeConn's real
// close path (nc.Close) is exercised in the p10/e2e layers; here the point is the LADDER.
func (w *wedgedFinalizer) install(a *Agent) {
	a.cfg.TeardownCloseFn = func(*nats.Conn) {
		close(w.closeStarted)
		<-w.conn.dead
		close(w.closeReturned)
	}
}

// A — ORDER: the session ctx is cancelled BEFORE the (wedged) close is even reached, and teardown
// returns inside budget+grace. MUTATION "restore close-before-cancel" makes the cancel timestamp
// land after closeStarted and this test red.
func TestTeardownCancelsBeforeClosing(t *testing.T) {
	a := teardownTestAgent(t)
	w := newWedgedFinalizer(t, a, false)
	w.install(a)

	done := make(chan struct{})
	go func() { defer close(done); w.fin.Do(teardownRebuild) }()

	select {
	case <-w.closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("close never started — the ladder did not reach S3")
	}
	if w.cancelledAt.Load() == 0 {
		t.Fatal("the session ctx was NOT cancelled before the close began — this is exactly the #72 order defect")
	}
	select {
	case <-done:
	case <-time.After(closeBudget + poisonGrace + 5*time.Second):
		t.Fatal("teardown did not return within budget+grace")
	}
	select {
	case <-w.closeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("the closer goroutine was ABANDONED, not joined (the forbidden fake fix)")
	}
}

// B — BOUNDED: poisoning is what unblocks the wedged close, and the closer is JOINED (no abandoned
// goroutine). MUTATION "delete the poison step" leaves the close parked and this red on the budget.
func TestTeardownPoisoningUnblocksAndJoins(t *testing.T) {
	a := teardownTestAgent(t)
	w := newWedgedFinalizer(t, a, false)
	w.install(a)

	start := time.Now()
	w.fin.Do(teardownRebuild)
	elapsed := time.Since(start)

	if elapsed < closeBudget {
		t.Fatalf("teardown returned in %s — before the close budget even elapsed; the wedge was not exercised", elapsed)
	}
	// Upper bound carries scheduling slack (internal review TQ-9): under `-race` on a loaded box the
	// two timer hops can drift, and a tight bound would flake without measuring anything more. The
	// LOWER bound is the load-bearing one — it proves the wedge was actually exercised.
	if elapsed > closeBudget+poisonGrace+2*time.Second {
		t.Fatalf("teardown took %s — poisoning did not unblock the close inside the grace", elapsed)
	}
	select {
	case <-w.closeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("closer goroutine not joined")
	}
}

// D — ESCALATE: a conn that ignores Close AND SetDeadline (the DNS-lookup-under-nc.mu shape, where
// there is no fd to poison) must reach the escalation rung with the caller's intent.
func TestTeardownEscalatesWhenPoisonCannotReach(t *testing.T) {
	for _, tc := range []struct {
		name   string
		intent teardownIntent
		want   string
	}{
		{"rebuild_self_execs", teardownRebuild, "rebuild"},
		{"shutdown_exits", teardownShutdown, "shutdown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := teardownTestAgent(t)
			var got atomic.Value
			a.cfg.TeardownEscalateFn = func(intent string) { got.Store(intent) }
			w := newWedgedFinalizer(t, a, true) // ignores poison
			w.install(a)

			w.fin.Do(tc.intent)

			v, _ := got.Load().(string)
			if v != tc.want {
				t.Fatalf("escalation intent = %q, want %q (the ladder must reach S5 with the CALLER's intent)", v, tc.want)
			}
			w.conn.ignores = false
			w.conn.release() // let the parked stub finish so the test leaves nothing running
		})
	}
}

// A redial callback may win sessionFinalizer.once just before systemd cancels the parent context.
// If S5 still self-execs as a rebuild, the daemon being stopped is resurrected in place. Re-check
// the parent lifecycle at the terminal rung and let shutdown win.
func TestParentCancellationOverridesRebuildEscalation(t *testing.T) {
	a := teardownTestAgent(t)
	var got atomic.Value
	a.cfg.TeardownEscalateFn = func(intent string) { got.Store(intent) }
	w := newWedgedFinalizer(t, a, true)
	w.install(a)
	parent, cancelParent := context.WithCancel(context.Background())
	w.fin.parent = parent
	cancelParent()

	w.fin.Do(teardownRebuild)

	v, _ := got.Load().(string)
	if v != "shutdown" {
		t.Fatalf("escalation intent = %q, want shutdown: parent cancellation must prevent teardown self-exec from resurrecting a stopping daemon", v)
	}
	w.conn.ignores = false
	w.conn.release()
}

// E — SINGLE-FLIGHT: two callers (the redial callback and the session's own deferred teardown) must
// run the ladder exactly once; the loser blocks until it is finished. That barrier is what lets Run
// clear `rebuilding` safely.
func TestTeardownIsSingleFlightAndBarriers(t *testing.T) {
	a := teardownTestAgent(t)
	w := newWedgedFinalizer(t, a, false)
	var closes atomic.Int32
	a.cfg.TeardownCloseFn = func(*nats.Conn) { closes.Add(1); w.conn.release() }

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); w.fin.Do(teardownRebuild) }()
	}
	wg.Wait()

	if n := closes.Load(); n != 1 {
		t.Fatalf("close ran %d times — the per-session finalizer is not single-flight", n)
	}
}

// origin: internal review CT-2 (S5) — a stale redial timer can fire inside the successor's connect
// window, before that session published a finalizer OR a cancel. Latching rebuilding/rebuildRequested
// there silently disarms the NEXT session (its onNATSReconnect early-returns, its roster refresh
// skips, its redial watchdog self-cancels) and makes a later clean shutdown read as a rebuild. Both
// entry points must roll the flags back when there is nothing to tear down.
func TestNoOpRebuildRollsBackTheRebuildFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		fire func(a *Agent)
	}{
		{"fireRedial", func(a *Agent) { a.fireRedial() }},
		{"rebuildOntoVoter", func(a *Agent) { a.rebuildOntoVoter("test") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := teardownTestAgent(t) // no finalizer, no session cancel — nothing to tear down
			tc.fire(a)
			if a.rebuilding.Load() {
				t.Error("rebuilding stayed latched after a no-op rebuild — the successor session is now disarmed")
			}
			if a.rebuildRequested.Load() {
				t.Error("rebuildRequested stayed latched — a later clean shutdown would be misread as a rebuild")
			}
		})
	}
}

// F — the tracker's sticky poison: conns dialed AFTER poisoning are killed on arrival and new dials
// are refused, so nats.go's reconnect loop cannot park on a fresh socket mid-teardown.
func TestConnTrackerPoisonIsSticky(t *testing.T) {
	late := newBlockingConn(false)
	tr := newConnTracker(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return late, nil
	})
	tr.poison()

	if _, err := tr.Dial("tcp", "127.0.0.1:4222"); err == nil {
		t.Fatal("a poisoned tracker must REFUSE new dials (else the reconnect loop parks on a fresh socket)")
	}
	select {
	case <-late.dead:
		t.Fatal("the refused dial's conn should not have been handed out at all")
	default:
	}
}

// origin: internal review CT-5 (S11) — the registry must stay bounded across a long-lived session's
// reconnect storm; an append-only slice grows one dead conn reference per redial forever.
func TestConnTrackerRegistryStaysBounded(t *testing.T) {
	tr := newConnTracker(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return newBlockingConn(false), nil
	})
	for i := 0; i < 1000; i++ {
		if _, err := tr.Dial("tcp", "127.0.0.1:4222"); err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}
	tr.mu.Lock()
	n := len(tr.conns)
	tr.mu.Unlock()
	if n > trackerConnCap {
		t.Fatalf("tracker holds %d conn references after 1000 redials (cap %d) — unbounded growth on a flapping link", n, trackerConnCap)
	}
}

// origin: internal review TQ-1 (S7) — the tracker must FORWARD to the CustomDialer it displaces, or
// a proxied agent silently dials direct (and a NAT'd fleet goes dark). Also covers S18: this is the
// use for the TeardownDialFn seam.
func TestTrackerDialOptionWrapsExistingCustomDialer(t *testing.T) {
	var innerCalls atomic.Int32
	inner := &recordingDialer{calls: &innerCalls}
	a := &Agent{cfg: Config{Logger: slog.New(slog.DiscardHandler)}}

	opt := a.trackerDialOption(context.Background(), []nats.Option{nats.SetCustomDialer(inner)})
	var o nats.Options
	if err := opt(&o); err != nil {
		t.Fatal(err)
	}
	if o.CustomDialer == nil {
		t.Fatal("trackerDialOption did not install a CustomDialer")
	}
	c, err := o.CustomDialer.Dial("tcp", "127.0.0.1:4222")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = c.Close()
	if innerCalls.Load() != 1 {
		t.Fatalf("inner (proxy) dialer called %d times, want 1 — the tracker REPLACED it instead of wrapping it", innerCalls.Load())
	}
}

type recordingDialer struct{ calls *atomic.Int32 }

func (r *recordingDialer) Dial(string, string) (net.Conn, error) {
	r.calls.Add(1)
	return newBlockingConn(false), nil
}

// The tracker forwards to the dialer it wraps (proxy support survives) and applies its own timeout,
// because nats.go skips Options.Timeout entirely when a CustomDialer is installed.
func TestConnTrackerWrapsInnerDialerAndBoundsIt(t *testing.T) {
	var saw atomic.Bool
	tr := newConnTracker(context.Background(), func(ctx context.Context, network, address string) (net.Conn, error) {
		saw.Store(true)
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("tracker did not bound the dial with its own timeout")
		}
		return newBlockingConn(false), nil
	})
	if _, err := tr.Dial("tcp", "127.0.0.1:4222"); err != nil {
		t.Fatalf("dial: %v", err)
	}
	if !saw.Load() {
		t.Fatal("the wrapped (proxy-aware) dialer was never called")
	}
}
