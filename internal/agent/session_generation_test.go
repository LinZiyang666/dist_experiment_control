package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// session_generation_test.go — callbacks belong to the session that created them.
//
// origin: prerelease audit agent-conn/AC-F1 + AC-F2.
//
// buildConnOptions runs once per NATS session and installs a reconnect handler and a
// disconnect handler. Both used to fire for ANY connection they were attached to, and
// `rebuilding` only suppresses them WHILE a rebuild is in flight — once Run clears it,
// a callback still queued on a retired connection is indistinguishable from one on the
// live connection. It then re-registers and re-subscribes on a socket nobody reads, or
// arms the fail-closed countdown for a session that no longer exists.

// redialArmed reads the watchdog timer under its own mutex.
func redialArmed(a *Agent) bool {
	a.redialMu.Lock()
	defer a.redialMu.Unlock()
	return a.redialTimer != nil
}

// readAgentSource reads a file from this package's directory for the source-shape guard.
func readAgentSource(name string) (string, error) {
	b, err := os.ReadFile(name)
	return string(b), err
}

func genTestAgent(t *testing.T) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL: testharness.StartNATS(t), SID: "lab", NID: "gpu1",
		Logger: testharness.SilentLog(), HeartbeatInterval: time.Second,
		RegisterTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestEachSessionsCallbacksOnlyActForThatSession is the AC-F2 guard. It drives the
// REAL handler closures out of buildConnOptions rather than asserting on a flag,
// because the defect was that the closures could not tell which session they belonged
// to.
func TestEachSessionsCallbacksOnlyActForThatSession(t *testing.T) {
	a := genTestAgent(t)

	// Session 1's options, then session 2's — exactly what a rebuild does.
	first := a.buildConnOptions()
	_ = a.buildConnOptions()

	var o nats.Options
	for _, opt := range first {
		if err := opt(&o); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	if o.DisconnectedErrCB == nil || o.ReconnectedCB == nil {
		t.Fatal("buildConnOptions no longer installs both handlers; this guard is testing nothing")
	}

	// The RETIRED session's disconnect handler must not arm anything. armFailClosed and
	// armRedialWatchdog are observable through the timers they set.
	a.stopRedialWatchdog()
	o.DisconnectedErrCB(nil, nil)
	if redialArmed(a) {
		t.Error("a RETIRED session's disconnect handler armed the redial watchdog.\n\n" +
			"`rebuilding` is cleared as soon as the dying session tears down, so a callback " +
			"arriving one moment later looks live. It then schedules a rebuild on behalf of a " +
			"connection the agent has already finished with.")
	}

	// And the CURRENT session's handler must still work, or the guard above is satisfied
	// by a handler that does nothing at all.
	current := a.buildConnOptions()
	var co nats.Options
	for _, opt := range current {
		if err := opt(&co); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	a.stopRedialWatchdog()
	co.DisconnectedErrCB(nil, nil)
	if !redialArmed(a) {
		t.Error("the CURRENT session's disconnect handler did not arm the redial watchdog — " +
			"the generation check is rejecting the live session too")
	}
	a.stopRedialWatchdog()

	// THE RECONNECT HANDLER TOO — origin: prerelease audit round 2, AC-4. AC-F2 is about
	// DOUBLE DISPATCH: a stale ReconnectedCB re-registers the agent and re-subscribes the
	// forwarded command subject, so two live subscriptions deliver one exec twice. That is
	// the half the finding is actually about, and only the disconnect handler was
	// exercised — the reconnect generation check could be deleted and this stayed green.
	//
	// onNATSReconnect is single-flighted through reconnectInFlight, so "did the retired
	// handler act" is observable without a live connection: a handler that DECLINES never
	// takes the flag, a handler that acts takes it and spawns the goroutine that releases
	// it. Checking it synchronously right after the call is therefore a one-way test —
	// true only if the callback ran the branch.
	a.reconnectInFlight.Store(false)
	o.ReconnectedCB(nil) // the RETIRED session's handler
	if a.reconnectInFlight.Load() {
		t.Error("a RETIRED session's reconnect handler started onNATSReconnect.\n\n" +
			"It re-registers and re-subscribes the forwarded command subject on behalf of a " +
			"connection the agent has finished with — two live subscriptions on one subject, " +
			"so every exec runs twice. That is the fan-out AC-F2 exists to prevent.")
	}

	// ...and the CURRENT session's reconnect handler must still act, or the assertion
	// above is satisfied by a handler that declines unconditionally.
	a.reconnectInFlight.Store(false)
	co.ReconnectedCB(nil)
	if !a.reconnectInFlight.Load() {
		t.Error("the CURRENT session's reconnect handler declined too — the generation check " +
			"is rejecting the live session, so a real reconnect never re-registers")
	}
}

// TestConnOptionsSuppressCallbacksAfterClose is the AC-F1 guard. nats.go delivers
// disconnect and close callbacks after Close() returns unless told not to, so a
// teardown could arm work for a connection the agent had already finished with.
func TestConnOptionsSuppressCallbacksAfterClose(t *testing.T) {
	a := genTestAgent(t)
	var o nats.Options
	for _, opt := range a.buildConnOptions() {
		if err := opt(&o); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	if !o.NoCallbacksAfterClientClose {
		t.Fatal("buildConnOptions does not set NoCallbacksAfterClientClose.\n\n" +
			"Without it nats.go still delivers disconnect and close callbacks after Close() " +
			"returns, so a session teardown arms the fail-closed countdown and the redial " +
			"watchdog for a session that is already gone.")
	}
}

// TestDialFailureIsAttributedAcrossAMultiURLPool is the AC-F3 guard.
//
// nats.Connect over a pool reports the LAST error it saw — usually ErrNoServers — even
// when one server answered with an authorization violation. Every decision downstream
// that asks "was this an auth failure" then answers no, and a leased agent re-presents
// the refused name forever.
func TestDialFailureIsAttributedAcrossAMultiURLPool(t *testing.T) {
	denying := denyingNATS(t)    // answers -ERR 'Authorization Violation'
	dead := "nats://127.0.0.1:1" // connection refused
	pool := dead + "," + denying // the shape a roster produces

	got := attributeDialFailure(pool, nats.ErrNoServers, nil, testharness.SilentLog())
	if got == nil {
		t.Fatal("a pool containing a server that REFUSED THE CREDENTIAL was reported as a plain " +
			"no-servers failure.\n\n" +
			"The lease degrade is gated on isAuthFailure, so an auth denial hidden behind " +
			"ErrNoServers leaves the agent re-presenting the name that was just refused, forever.")
	}
	if !isAuthFailure(got) {
		t.Fatalf("attribution returned %v, which isAuthFailure does not recognise", got)
	}

	// A SINGLE-URL pool has nothing to disambiguate — otherwise the healthy path
	// pays for a re-dial that can only tell it what it already knows.
	if attributeDialFailure(denying, nats.ErrNoServers, nil, testharness.SilentLog()) != nil {
		t.Error("a single-URL pool was re-dialled; there is nothing to attribute")
	}
	// An error that IS the denial is already attributed; re-dialling it would
	// spend a second auth decision to reach the same answer.
	if attributeDialFailure(pool, errAttrSentinel("nats: Authorization Violation"), nil, testharness.SilentLog()) != nil {
		t.Error("an error that already names the denial was re-dialled")
	}
}

// origin: prerelease audit round 2, AC-1.
//
// THE ORDINARY DOWN-BROKER ERROR IS `i/o timeout`, NOT ErrNoServers.
//
// nats.go rewrites the pooled error to ErrNoServers only when the last dial
// failed with "connection refused" (nats.go@v1.52.0, connect()). A host that is
// powered off, firewalled DROP or partitioned ends the dial in a timeout — and
// the first version of attributeDialFailure returned nil on its first line for
// anything that was not ErrNoServers, under a comment claiming it was "already
// attributed". So AC-F3's fix worked or did not purely by where in the pool the
// denying broker happened to sit.
func TestAttributionIsNotGatedOnErrNoServers(t *testing.T) {
	denying := denyingNATS(t)
	// A blackhole address: dialling it ends in a timeout, not a refusal. This
	// is the pooled error nats.go would hand back for a partitioned broker.
	pool := denying + ",nats://192.0.2.1:4222"

	got := attributeDialFailure(pool, errAttrSentinel("dial tcp 192.0.2.1:4222: i/o timeout"),
		nil, testharness.SilentLog())
	if got == nil {
		t.Fatal("a credential refusal went unattributed because the pooled error was a TIMEOUT " +
			"rather than ErrNoServers.\n\n" +
			"That is the ORDINARY shape for an unreachable broker, so the lease degrade stayed " +
			"gated shut on exactly the failures it was written for — and order-dependently, " +
			"since moving the denying broker to the end of the pool makes the same fleet work.")
	}
	if !isAuthFailure(got) {
		t.Fatalf("attribution returned %v, which isAuthFailure does not recognise", got)
	}
}

// origin: prerelease audit round 2, AC-5.
//
// A PROBE MUST NOT CARRY THE LIVE SESSION'S CALLBACKS. They close over the
// current generation, so the generation guard cannot filter them: a probe
// connection dropped by a restarting broker armed armFailClosed() and
// armRedialWatchdog() for a connection the agent had already discarded, and
// session() clears the watchdog BEFORE connectNATS and never after — so it
// survived into the healthy session that connect was about to establish.
func TestAttributionProbesDropTheLiveSessionCallbacks(t *testing.T) {
	a := genTestAgent(t)
	live := a.buildConnOptions()

	var lo nats.Options
	for _, opt := range live {
		if err := opt(&lo); err != nil {
			t.Fatalf("apply live option: %v", err)
		}
	}
	if lo.DisconnectedErrCB == nil || lo.ReconnectedCB == nil {
		t.Fatal("premise broken: the LIVE options no longer install the lifecycle callbacks, " +
			"so this test can no longer tell whether the probe set drops them")
	}

	var po nats.Options
	for _, opt := range probeConnOptions(live) {
		if err := opt(&po); err != nil {
			t.Fatalf("apply probe option: %v", err)
		}
	}
	if po.DisconnectedErrCB != nil {
		t.Error("an attribution probe still installs DisconnectErrHandler.\n\n" +
			"A probe dropped by a restarting broker then arms this session's fail-closed " +
			"countdown and redial watchdog on behalf of a connection that was thrown away.")
	}
	if po.ReconnectedCB != nil {
		t.Error("an attribution probe still installs ReconnectHandler, so a throwaway connection " +
			"can drive onNATSReconnect and re-register the agent")
	}
	if po.ClosedCB != nil {
		t.Error("an attribution probe still installs ClosedHandler")
	}
	if po.AllowReconnect {
		t.Error("an attribution probe still reconnects; it is closed immediately and must not " +
			"keep a connection alive in the background")
	}
	if po.RetryOnFailedConnect {
		t.Error("an attribution probe still retries a failed connect.\n\n" +
			"That turns a refusal into a *nats.Conn that reports its error later, which is " +
			"exactly the synchronous signal this probe exists to read.")
	}
	// The identity must SURVIVE: a probe that does not present the credential
	// proves nothing about whether the credential is refused.
	if po.Name != lo.Name {
		t.Errorf("the probe presents CONNECT name %q, the live session %q — an attribution probe "+
			"that presents a different identity answers a different question", po.Name, lo.Name)
	}
}

type errAttrSentinel string

func (e errAttrSentinel) Error() string { return string(e) }

// TestPtyIntakeIsSessionScopedNotPerRun is the L3-F2 guard.
//
// `.in` and `.resize` used to be subscribed on the conn captured when the child was
// spawned, so a session rebuild — an ordinary consequence of a drain or a roster change
// — left the interactive session deaf: keystrokes and resizes stopped arriving while
// `.out` kept streaming, so the user saw a live terminal that ignored them.
func TestPtyIntakeIsSessionScopedNotPerRun(t *testing.T) {
	src, err := readAgentSource("run.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(src, "nc.Subscribe(proto.SubjPtyIn(") ||
		strings.Contains(src, "nc.Subscribe(proto.SubjPtyResize(") {
		t.Fatal("the PTY intake is subscribed per run, on the conn captured at spawn.\n\n" +
			"A session rebuild closes that conn and the interactive session goes deaf while its " +
			"output keeps streaming. It must be a session-scoped wildcard, like pty.*.ka.")
	}
	if !strings.Contains(src, `".pty.*.in"`) || !strings.Contains(src, `".pty.*.resize"`) {
		t.Fatal("no session-scoped wildcard intake found for .in / .resize")
	}
}

// abandonReapBudget is a small fraction of the 30s sleep the guard below spawns — see
// the comment there for why the discriminator has to be time.
const abandonReapBudget = 3 * time.Second

// TestExecChunksRideTheCurrentConnection is the L3-F1 guard.
//
// run.go had this reasoning and applied it to exactly one publish. exec's chunks —
// started, exit, error, and every stdout and stderr chunk — all rode the conn captured
// when the child was spawned, so a session rebuild silenced a running `tether exec`
// completely: the caller saw a command that produced no output and never exited.
func TestExecChunksRideTheCurrentConnection(t *testing.T) {
	a := genTestAgent(t)

	// With nothing stored, the captured conn is used — the in-process-test fallback.
	captured := &nats.Conn{}
	if got := liveConn(a, captured); got != captured {
		t.Fatal("with no current conn stored, liveConn must fall back to the captured one; " +
			"tests that never populate ncBox would otherwise publish to nil")
	}

	// Once a session exists, that is the one every chunk goes out on.
	live := &nats.Conn{}
	a.ncBox.Store(live)
	if got := liveConn(a, captured); got != live {
		t.Fatal("liveConn returned the CAPTURED conn while a current one exists.\n\n" +
			"That conn is closed after a rebuild, so every chunk published on it vanishes and a " +
			"running exec goes silent with no error anywhere.")
	}

	// And the resolution must happen inside replyChunk, so streamPipe's per-chunk loop
	// is covered by construction rather than by remembering.
	src, err := readAgentSource("exec.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(src, "func (a *Agent) replyChunk(")
	if i < 0 {
		t.Fatal("SELF-CHECK FAILED: replyChunk not found in exec.go")
	}
	body := src[i:]
	if j := strings.Index(body, "\nfunc "); j > 0 {
		body = body[:j]
	}
	if !strings.Contains(body, "liveConn(a, nc)") {
		t.Error("replyChunk does not resolve the live conn.\n\n" +
			"Resolving at the call sites instead means every future one has to remember, and " +
			"streamPipe's per-chunk loop is the one that matters most.")
	}
}

// TestAnAbandonedExecChildIsKilledNotJustWaitedOn is the L3-F3 guard.
//
// The abandon path used to be a bare cmd.Wait(). Waiting reaps the process-table entry
// but does nothing about the process: the handler has ALREADY answered
// remote_fs_spawn_timeout and nobody will ever read its output or report its exit, yet
// it keeps running and holding the mount it wedged on. On a filesystem that recovers
// minutes later, the wait itself blocks for that whole time.
func TestAnAbandonedExecChildIsKilledNotJustWaitedOn(t *testing.T) {
	a := genTestAgent(t)

	cmd := exec.Command("sh", "-c", "sleep 30")
	setExecProcGroup(cmd) // its own group, so the negative pid reaches only its descendants
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid

	// THE DISCRIMINATOR IS TIME, not "is it dead afterwards".
	//
	// A bare cmd.Wait() also leaves the child dead — thirty seconds later, when the
	// sleep finishes on its own. That is the defect, not the fix: the handler has
	// already answered, so those thirty seconds are thirty seconds of a wedged mount
	// held and a goroutine parked. Mutation verification is the only reason this was
	// noticed; the first version of this test passed against the reverted code.
	start := time.Now()
	reapAbandonedExecChild(a, cmd, nil)
	elapsed := time.Since(start)

	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("the abandoned child (pid %d) is still alive: %v.\n\n"+
			"The spawn was given up on and the caller already told the operator it timed out. "+
			"Leaving the process running keeps the resources the abandon was supposed to reclaim, "+
			"including the mount it wedged on.", pid, err)
	}
	if elapsed > abandonReapBudget {
		t.Fatalf("the reap took %v; the child was going to exit on its own in 30s, so anything "+
			"near that means it was WAITED OUT rather than killed. Waiting reaps the process-table "+
			"entry and nothing else.", elapsed)
	}

	// A FAILED START must be a no-op — cmd.Process is nil, so there is nothing to
	// signal and Wait would be a second one.
	failed := exec.Command("definitely-not-a-real-binary-xyz")
	if err := failed.Start(); err == nil {
		t.Skip("this environment resolved a binary that should not exist")
	}
	reapAbandonedExecChild(a, failed, errors.New("start failed")) // must not panic or Wait
	reapAbandonedExecChild(a, nil, nil)                           // must not panic

	// AND A LIVE CHILD MUST BE KILLED EVEN WHEN startErr IS NON-NIL.
	//
	// origin: prerelease audit round 2, I-F1. This is the path that actually occurs:
	// onAbandon runs first and cancels the handshake, so the wedged execve returns an
	// ERROR — and the first version of this function returned early on exactly that,
	// leaving the child alive and releasing the wedge slot immediately.
	live := exec.Command("sh", "-c", "sleep 30")
	setExecProcGroup(live)
	if err := live.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	livePID := live.Process.Pid
	start2 := time.Now()
	reapAbandonedExecChild(a, live, errors.New("handshake cancelled"))
	if el := time.Since(start2); el > abandonReapBudget {
		t.Fatalf("the reap took %v with a non-nil startErr — it waited the child out instead of "+
			"killing it, which is the case the early return used to skip entirely", el)
	}
	if err := syscall.Kill(livePID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("a live child survived the abandon reap when startErr was non-nil (pid %d: %v).\n\n"+
			"That is the ONLY path this function is called on in production — onAbandon cancels the "+
			"handshake before the wedged start returns — so the early return meant the child was "+
			"never killed and the wedge ceiling never armed.", livePID, err)
	}
}

// origin: prerelease audit round 2, AC-3 / G4.
//
// ATTRIBUTION MUST BE ON THE PATH, not merely correct in isolation.
//
// Every guard above calls attributeDialFailure directly with a hand-made error. Delete
// its call site in connectNATS and all of them stay green while the lease degrade goes
// back to being gated shut behind an unattributed pool error — which is the entire
// defect AC-F3 was written to fix.
//
// Driven end to end: a pool whose FIRST entry refuses the credential and whose second is
// a blackhole, so nats.go's own pooled error is a timeout rather than the denial. If
// connectNATS attributes, the agent classifies this as auth (and, having never
// authenticated, fails fast with the operator hint). If it does not, the loop treats it
// as transient and the context deadline is what comes back.
func TestConnectNATSAttributesAPoolFailureItselfNotJustTheHelper(t *testing.T) {
	denying := denyingNATS(t)
	a := &Agent{cfg: Config{
		SID: "lab", NID: "gpu1", Logger: d6Logger(),
		// The denying broker FIRST, a blackhole second: nats.go's last error is the
		// blackhole's timeout, so only a per-URL re-dial can find the refusal.
		NATSURL:              denying + ",nats://192.0.2.1:4222",
		RegisterRetryInitial: 5 * time.Millisecond,
		RegisterRetryMax:     10 * time.Millisecond,
	}}
	// everAuthed deliberately NOT set: a first-connect denial is terminal and loud, which
	// is the observable that says attribution happened.

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := a.connectNATS(ctx)
	if err == nil {
		t.Fatal("connectNATS unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "auth_callout rejected") {
		t.Fatalf("err = %v; want the terminal auth message.\n\n"+
			"One broker in the pool answered `Authorization Violation` and nats.go reported the "+
			"OTHER one's timeout. Without connectNATS re-dialling per URL, that denial is "+
			"invisible to every decision downstream — including the lease degrade, which is "+
			"then permanently gated shut on exactly the failure it exists for.", err)
	}
}
