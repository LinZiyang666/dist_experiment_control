// Bounded NATS-connection teardown — the gotcha #72 fix.
//
// THE DEFECT. `fireRedial` (and its twin `rebuildOntoVoter`) used to call `nc.Close()` SYNCHRONOUSLY
// and only then cancel the session context. nats.go's `Conn.Close`, `Drain` and `wsClose` all take
// `nc.mu` — and `doReconnect` holds that same mutex across `createConn`/`processConnectInit`, whose
// DNS lookup (`net.LookupHost`, no ctx), TLS `Handshake()` (no deadline until processConnectInit
// sets one) and websocket `req.Write` (no write deadline) can block indefinitely on a half-dead
// link. A 2026-08-01 field incident sat there for 10m58s: systemd read `active (running)`, the node
// was OFFLINE, and the 20s redial watchdog bought nothing because the cancel it wanted to deliver
// was queued BEHIND the block. (The ledger's original suspicion — the close-frame flush — is NOT the
// primary suspect: that flush rides `FlusherTimeout` and is skipped outright while RECONNECTING.)
//
// THE SHAPE OF THE FIX. Teardown becomes a state machine that never lets an unbounded call sit on
// the recovery path:
//
//	S1 PINNED+ARMED   pin the dying conn + its tracker (never re-read from ncBox — a late close must
//	                  not kill the successor); arm the escalation timer FIRST, so the safety net does
//	                  not depend on the close returning
//	S2 CANCELLED      cancel the session ctx (pure pointer + channel work; touches no nats.go mutex)
//	S3 CLOSING        a closer goroutine runs the real Close/Drain, bounded by closeBudget
//	S4 POISONED       budget blown: STICKILY poison the tracked raw conns (SetDeadline(past)+Close),
//	                  which unblocks the reader/writer nats.go is stuck in, and refuse new dials
//	S5 ESCALATE       poison grace blown too: the process is structurally wedged (the DNS-lookup layer
//	                  holds nc.mu with no fd we can reach). Rebuild callers self-exec in place (same
//	                  PID — the setsid deployments have no supervisor to rely on); shutdown callers
//	                  exit non-zero.
//
// The closer goroutine is ALWAYS joined — poisoning is what makes that join bounded. Abandoning it
// after a timeout would trade a hang for a goroutine/fd leak, which the ledger names as the
// forbidden fake fix and which the leak gate (test/concurrency) would have to be lied to about.
// ONE HONEST EXCEPTION (internal review CT-6): on the S5 rung the process is about to be replaced
// or exit, so the escalation returns without joining — there is nothing left to leak into.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// closeBudget / poisonGrace are vars, not consts, ONLY so the ladder's tests can run in
// milliseconds instead of sitting through the production budgets; production never rewrites them
// (same precedent as upgradeWaitBudget). Their VALUES are a contract documented in
// deploy-tier-gotchas #72 and usage.md §9.9 — changing them changes the published recovery bound.
var (
	// closeBudget bounds the graceful Close/Drain. Generous next to a healthy close (microseconds)
	// and short next to the failure it exists for (minutes).
	closeBudget = 10 * time.Second
	// poisonGrace bounds the post-poison wait. Poisoning forces the blocked syscall to return
	// EBADF/ETIMEDOUT; anything still stuck after this is holding nc.mu somewhere we cannot reach.
	poisonGrace = 10 * time.Second
)

const (
	// teardownDialTimeout replaces the per-dial timeout nats.go skips when a CustomDialer is set
	// (nats.go connects via the dialer without applying Options.Timeout or its host-count split).
	teardownDialTimeout = 2 * time.Second
	// exitTeardownWedged is the escalation exit code for the shutdown path: a distinct, classified
	// value so an operator/monitor can tell "wedged teardown" from any other non-zero exit.
	exitTeardownWedged = 91
	// trackerConnCap bounds the per-session conn registry (internal review CT-5). nats.go holds at
	// most one live connection at a time; the extra slots cover a reconnect storm's in-flight
	// stragglers with room to spare, and anything older than that cannot still be blocking it.
	trackerConnCap = 8
)

// teardownIntent selects the S5 escalation. Two callers, two correct terminal moves.
type teardownIntent int

const (
	// teardownRebuild: the session is being torn down so a NEW one can take its place. Escalation
	// must keep the node ALIVE — self-exec in place (same PID, same supervisor bookkeeping), which
	// is the only move that also works on the unsupervised setsid deployment.
	teardownRebuild teardownIntent = iota
	// teardownShutdown: the process is going away anyway. Escalation exits non-zero; a wedged
	// closer must never be allowed to hold an operator's `systemctl stop` hostage.
	teardownShutdown
)

// connTracker wraps the dialer nats.go uses so the raw net.Conns behind a *nats.Conn stay reachable.
// It is BOTH the production instrument (poisoning) and the test seam (a fake dialer can hand back a
// conn whose Write blocks forever, reproducing the field hang deterministically).
//
// KNOWN LIMIT (internal review CT-7): nats.go probes a CustomDialer for the optional
// SkipTLSHandshake interface; this wrapper does not forward it, so a wrapped dialer implementing it
// would be silently ignored. Inert today — proxydial deliberately does not implement it — and
// recorded here rather than pre-solved because forwarding an interface nobody implements is
// untestable code.
//
// Sticky by design: once poisoned, conns registered LATER are poisoned on arrival and new dials are
// refused. A one-shot sweep would race nats.go's reconnect loop dialing a fresh socket and blocking
// on that one instead.
type connTracker struct {
	mu       sync.Mutex
	conns    []net.Conn
	poisoned bool
	// dialCtx bounds every dial. HONEST SCOPE (internal review CT-4): connectNATS passes the
	// AGENT-level ctx, not the per-session runCtx, so cancelling a session does NOT abort an
	// in-flight dial — poisoning and the ladder's budget are what bound teardown; this ctx only
	// carries process shutdown plus the per-dial timeout applied below. Nothing depends on the
	// stronger property, and wiring runCtx here would need the tracker built after the session ctx
	// exists (it is built inside connectNATS, before that point).
	dialCtx context.Context
	// dialFn is the underlying dialer (proxy-aware in production, a fake in tests).
	dialFn func(ctx context.Context, network, address string) (net.Conn, error)
}

func newConnTracker(ctx context.Context, dialFn func(context.Context, string, string) (net.Conn, error)) *connTracker {
	if dialFn == nil {
		d := &net.Dialer{}
		dialFn = d.DialContext
	}
	return &connTracker{dialCtx: ctx, dialFn: dialFn}
}

// Dial implements nats.CustomDialer.
func (t *connTracker) Dial(network, address string) (net.Conn, error) {
	t.mu.Lock()
	poisoned := t.poisoned
	base := t.dialCtx
	t.mu.Unlock()
	if poisoned {
		return nil, errors.New("agent: connection teardown in progress; refusing to dial")
	}
	if base == nil {
		// ctx-none: a tracker built without a ctx (defensive/test construction only — connectNATS
		// always passes the agent ctx). origin: external review F8, CLAUDE.md §5 requires every new
		// context.Background() site to name its class. This creates no uncancellable root in
		// production: the WithTimeout below always bounds the dial, and the teardown ladder's
		// poisoning is what bounds an in-flight one.
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, teardownDialTimeout)
	defer cancel()
	c, err := t.dialFn(ctx, network, address)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.poisoned {
		// Poisoned while this dial was in flight: kill it on arrival rather than handing nats.go a
		// socket it could block on after teardown started.
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("agent: connection teardown in progress; discarding late dial")
	}
	// origin: internal review CT-5 (S11) — bound the registry. A long-lived session on a flapping
	// link redials indefinitely (MaxReconnects(-1)), and an append-only slice would grow a dead
	// conn reference per reconnect for as long as the process runs. Poisoning only ever needs the
	// conns that could still be blocking nats.go, so keep the newest trackerConnCap.
	t.conns = append(t.conns, c)
	if len(t.conns) > trackerConnCap {
		drop := len(t.conns) - trackerConnCap
		t.conns = append([]net.Conn(nil), t.conns[drop:]...)
	}
	t.mu.Unlock()
	return c, nil
}

// poison makes every tracked conn fail fast and keeps that property for conns dialed later. The
// deadline in the past forces a blocked read/write to return immediately; Close covers the rest.
func (t *connTracker) poison() int {
	t.mu.Lock()
	t.poisoned = true
	conns := append([]net.Conn(nil), t.conns...)
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.SetDeadline(time.Unix(1, 0))
		_ = c.Close()
	}
	return len(conns)
}

// finalizeConn runs the whole S1–S5 ladder for one connection. Single-flight per session via
// sync.Once in the caller (sessionFinalizer) so the fireRedial path and the session's own deferred
// teardown cannot both drive it.
//
// Returns when the closer goroutine has been JOINED — never before, and never by abandoning it.
func (a *Agent) finalizeConn(parent context.Context, nc *nats.Conn, tracker *connTracker, intent teardownIntent, cancel context.CancelFunc, cleanups []func()) {
	// S2 — cancel FIRST. Pure pointer/channel work: heartbeatLoop, roster refresh and every ctx-
	// aware producer stop here, regardless of what the socket does.
	if cancel != nil {
		cancel()
	}

	// S3 — the real close, off the recovery path.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// nc.mu-touching cleanups first (subscription teardown before the close), all inside the
		// same budget — see addBoundedCleanup.
		for _, fn := range cleanups {
			fn()
		}
		if fn := a.cfg.TeardownCloseFn; fn != nil {
			fn(nc)
			return
		}
		if nc != nil {
			// Drain would be nicer, but on a RECONNECTING conn nats.go converts it to Close anyway
			// (and takes the same mutex): Close is the honest call.
			nc.Close()
		}
	}()

	select {
	case <-done:
		return
	case <-time.After(closeBudget):
	}

	// S4 — poison. This is what makes the join below bounded.
	n := 0
	if tracker != nil {
		n = tracker.poison()
	}
	a.cfg.Logger.Warn("agent: NATS close exceeded its budget; poisoning the underlying connection(s)",
		"budget", closeBudget.String(), "conns_poisoned", n, "intent", intentName(intent))

	select {
	case <-done:
		a.cfg.Logger.Warn("agent: NATS close returned after poisoning (teardown recovered within budget+grace)")
		return
	case <-time.After(poisonGrace):
	}

	// S5 — escalate. We are wedged somewhere poisoning cannot reach (the nc.mu-holding DNS lookup
	// is the known such layer: no fd exists yet to poison). The closer goroutine is still live and
	// we are deliberately NOT pretending otherwise — the process is about to be replaced or exit.
	// Parent shutdown wins over an earlier redial callback's rebuild request: otherwise a
	// systemctl stop racing this 20s ladder can self-exec and resurrect the daemon it is stopping.
	if intent == teardownRebuild && parent != nil && parent.Err() != nil {
		intent = teardownShutdown
	}
	a.cfg.Logger.Error("agent: NATS teardown WEDGED past its budget and poison grace; escalating",
		"close_budget", closeBudget.String(), "poison_grace", poisonGrace.String(),
		"intent", intentName(intent))
	a.escalateWedgedTeardown(intent)
}

func intentName(i teardownIntent) string {
	if i == teardownShutdown {
		return "shutdown"
	}
	return "rebuild"
}

// escalateWedgedTeardown is S5. Rebuild callers self-exec (keep the node alive, same PID, works on
// setsid); shutdown callers exit non-zero (never hold a stop hostage).
func (a *Agent) escalateWedgedTeardown(intent teardownIntent) {
	if a.cfg.TeardownEscalateFn != nil { // test seam: record, do not replace/kill the test binary
		a.cfg.TeardownEscalateFn(intentName(intent))
		return
	}
	if intent == teardownShutdown {
		os.Exit(exitTeardownWedged)
	}
	// origin: internal review CT-3 (S6) — MUST go through upgradeExePath, not a bare os.Executable:
	// inside an upgrade window the running inode is gone and os.Executable returns the path with a
	// " (deleted)" suffix. Exec'ing that fails ENOENT, and recoverFromFailedExec would then derive
	// the marker path from the same bad string, miss the pending marker, and os.Exit — violating the
	// F3 contract that a pending upgrade must never exit. The other three sites in this package
	// already trim; this was the one that did not.
	exePath, err := a.upgradeExePath()
	if err != nil {
		a.cfg.Logger.Error("agent: wedged-teardown escalation cannot resolve its own executable; exiting for the supervisor",
			"err", err)
		os.Exit(exitTeardownWedged)
	}
	// origin: external review F4 (MAJOR) — teardown escalation must NOT reuse the ordinary upgrade
	// path's boolean contract. `recoverFromFailedExec` is written for the OLD process: it restores
	// .prev and DELIBERATELY keeps running, because the image in memory is the good one. Here the
	// opposite may hold — this process can be the NEW staged image whose teardown wedged, so
	// "restore the disk and return" would leave the wrong code running against a rolled_back marker,
	// with the proven-wedged closer goroutine still live and Run free to start a successor inside a
	// contaminated process. Teardown escalation therefore has its OWN terminal rules:
	//   1. try the in-place exec;
	//   2. if it fails, restore the prev slot ourselves (best effort) and try ONCE MORE against the
	//      restored path — the point of the restore is to have something exec-able;
	//   3. if that also fails, this process cannot be made correct: exit non-zero. On systemd the
	//      supervisor relaunches the restored binary; on setsid the node is down and says so, which
	//      is strictly better than serving from an image we just declared unusable.
	// There is no path here that returns to Run.
	if execErr := a.upgradeExec(exePath); execErr != nil {
		a.cfg.Logger.Error("agent: wedged-teardown self-exec failed; restoring the prev slot and retrying once",
			"err", execErr, "exe", exePath)
		a.restorePrevForEscalation(exePath, execErr)
		if execErr2 := a.upgradeExec(exePath); execErr2 != nil {
			a.cfg.Logger.Error("agent: wedged-teardown self-exec failed again after restoring prev; exiting for the supervisor",
				"err", execErr2, "exe", exePath)
			os.Exit(exitTeardownWedged)
		}
	}
}

// restorePrevForEscalation puts the known-good binary back at exePath before the escalation's second
// exec attempt. Distinct from recoverFromFailedExec (external review F4): it makes NO claim about
// staying alive, and a lock it cannot take is a reason to log and continue to the retry rather than
// to invent a terminal state — the caller's next step decides that.
func (a *Agent) restorePrevForEscalation(exePath string, execErr error) {
	if lerr := withUpgradeFileLock(exePath, true, func() error {
		m, merr := readUpgradeMarker(upgradeMarkerPath(exePath))
		if merr != nil || m == nil || m.State != upgradeStatePending || !a.markerTargetsThisAgent(m) {
			//nolint:nilerr // The closure's error return carries LOCK failures only; no pending
			// marker targeted at THIS instance means there is no prev slot this escalation may restore.
			return nil
		}
		executeRollback(exePath, m, fmt.Sprintf("teardown escalation: self-exec failed (%v)", execErr),
			a.cfg.Logger, false, nil)
		return nil
	}); lerr != nil {
		a.cfg.Logger.Error("agent: wedged-teardown escalation could not take the host upgrade lock to restore prev",
			"err", lerr)
	}
}

// sessionFinalizer is the per-session single-flight teardown. Both the redial/rebuild paths and the
// session's own deferred cleanup call Do; whoever is first runs the ladder, the other blocks until
// it is finished — which is exactly the barrier Run needs before it may clear `rebuilding` and start
// a successor session (a late reconnect on the dying conn must not re-subscribe, and a late Close
// must not target the successor's connection).
type sessionFinalizer struct {
	once    sync.Once
	nc      *nats.Conn
	tracker *connTracker
	cancel  context.CancelFunc
	parent  context.Context
	agent   *Agent

	// cleanupMu/cleanups hold teardown work that TOUCHES nc.mu (subscription teardown, handler
	// drains). origin: internal review CT-1 (S4) — as plain `defer`s they ran BEFORE this
	// finalizer (LIFO) and could block forever on a wedged connection, re-creating the very hang
	// the ladder removes. Run inside the budgeted closer goroutine, poisoning bounds them too.
	cleanupMu sync.Mutex
	cleanups  []func()
}

// addBoundedCleanup registers nc.mu-touching teardown work to run inside the budgeted closer.
func (f *sessionFinalizer) addBoundedCleanup(fn func()) {
	if f == nil || fn == nil {
		return
	}
	f.cleanupMu.Lock()
	f.cleanups = append(f.cleanups, fn)
	f.cleanupMu.Unlock()
}

func (f *sessionFinalizer) takeCleanups() []func() {
	if f == nil {
		return nil
	}
	f.cleanupMu.Lock()
	defer f.cleanupMu.Unlock()
	out := f.cleanups
	f.cleanups = nil
	return out
}

func (f *sessionFinalizer) Do(intent teardownIntent) {
	if f == nil {
		return
	}
	f.once.Do(func() {
		f.agent.finalizeConn(f.parent, f.nc, f.tracker, intent, f.cancel, f.takeCleanups())
	})
}

// trackerDialOption builds this session's connTracker (wrapping whatever dialer the options
// already settled on) and returns the nats.Option that installs it. The tracker is stashed on the
// Agent so session() can hand it to its finalizer — takeSessionTracker consumes it.
//
// Wrapping, not replacing: if proxydial installed a CustomDialer we forward to it, so proxy support
// and poisoning coexist. nats.go's own Options.Timeout / host-count split are SKIPPED for a
// CustomDialer, which is why the tracker applies teardownDialTimeout itself — with one honest
// caveat (internal review CT-4): nats.CustomDialer's Dial takes no ctx, so when we forward to a
// wrapped dialer our timeout bounds only OUR frame; the proxy dialer keeps its own (10s) internal
// deadline. Both are bounded, which is all the ladder needs.
func (a *Agent) trackerDialOption(ctx context.Context, existing []nats.Option) nats.Option {
	base := a.cfg.TeardownDialFn
	if base == nil {
		// Resolve whatever CustomDialer the options installed (proxydial's, if any) by applying
		// them to a scratch Options — the only supported way to read that field back.
		var probe nats.Options
		for _, o := range existing {
			if o != nil {
				_ = o(&probe)
			}
		}
		if probe.CustomDialer != nil {
			inner := probe.CustomDialer
			base = func(_ context.Context, network, address string) (net.Conn, error) {
				return inner.Dial(network, address)
			}
		}
	}
	tr := newConnTracker(ctx, base)
	a.sessTrackerMu.Lock()
	a.sessTracker = tr
	a.sessTrackerMu.Unlock()
	return nats.SetCustomDialer(tr)
}

// takeSessionTracker hands the tracker built for the connection just dialed to that session's
// finalizer, clearing it so a later session cannot inherit a stale one.
func (a *Agent) takeSessionTracker() *connTracker {
	a.sessTrackerMu.Lock()
	defer a.sessTrackerMu.Unlock()
	tr := a.sessTracker
	a.sessTracker = nil
	return tr
}

// setSessionFinalizer / loadSessionFinalizer publish the current session's finalizer to the
// callback goroutines (fireRedial, rebuildOntoVoter) that must be able to tear down a session they
// do not own. Same mutex discipline as sessCancel.
func (a *Agent) setSessionFinalizer(f *sessionFinalizer) {
	a.sessFinMu.Lock()
	a.sessFin = f
	a.sessFinMu.Unlock()
}

func (a *Agent) loadSessionFinalizer() *sessionFinalizer {
	a.sessFinMu.Lock()
	defer a.sessFinMu.Unlock()
	return a.sessFin
}

// connectedURLSnapshot returns the URL captured while the link was healthy (external review F3).
// Deliberately does NOT consult the *nats.Conn: every observer on it takes nc.mu, the mutex a wedged
// doReconnect holds across a no-deadline dial/handshake — the exact hang this increment removes.
func (a *Agent) connectedURLSnapshot() string {
	if p := a.connURLBox.Load(); p != nil {
		return *p
	}
	return ""
}
