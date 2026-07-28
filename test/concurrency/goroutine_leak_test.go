// goroutine_leak_test.go — exercise the lifecycle of every long-
// lived component (broker / agent / tunnel.Server / adminsock.Server)
// and assert ctx-cancel actually unwinds every spawned goroutine.
//
// Pattern:
//
//  1. Snapshot runtime.NumGoroutine BEFORE component start.
//  2. Spin component, do something representative.
//  3. Cancel ctx (or invoke Close).
//  4. assertNoGoroutineLeak — polls until count returns near baseline.
//
// Why these tests matter: the v1 audit found at least one
// shutdown-race goroutine leak (tunnel C1 — handleAgent that
// inserted into s.sessions after Close drained it; cleaned-up under
// audit shard 01). Without explicit goroutine-count assertions the
// next regression would land silently.
package concurrency_test

import (
	"context"
	"github.com/LinZiyang666/tether/internal/testharness"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
)

// TestBrokerRunCancelNoGoroutineLeak — A.1 from the test plan.
// Boot broker, immediately cancel, ensure NumGoroutine returns to
// baseline. Without proper subscription unsubscribe + ticker stop
// + drain wait, the broker would leak (a) the reconcile ticker,
// (b) the NATS Drain flusher, (c) the disk monitor, (d) the
// session subscription handlers.
func TestBrokerRunCancelNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	before := runtime.NumGoroutine()

	// leakExerciseRounds, not once. A single boot+shutdown cannot see a per-boot leak of one or two
	// goroutines against the ±2 noise floor — the same arithmetic that made the active-session tunnel
	// assertion structurally blind. Internal review B9-1: this was one of six call sites still running
	// its subject once while the shared gate's doc claimed the whole repo ran N>=5.
	for i := 0; i < leakExerciseRounds; i++ {
		_, shutdown := startBrokerNoTunnel(t, url, db)
		shutdown()
	}

	assertNoGoroutineLeak(t, "broker run+cancel", before)
}

// TestBrokerImmediateCancelNoGoroutineLeak hits the rarer race
// where ctx is canceled before the broker's Run loop even reaches
// the ticker — the connection setup, subscriptions, and JS probe
// must all unwind in this case too.
func TestBrokerImmediateCancelNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	before := runtime.NumGoroutine()

	// leakExerciseRounds rounds (internal review B9-1): the immediate-cancel race is exactly the shape
	// where a partially-started subsystem leaks ONE goroutine, and one round of that is invisible at ±2.
	// Repeating also samples the race at different points in Run's startup, which a single round cannot.
	for i := 0; i < leakExerciseRounds; i++ {
		b, err := broker.New(broker.Config{
			NATSURL: url, DB: db, Logger: silentLog(),
			ReconcileInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- b.Run(ctx) }()
		cancel() // race: Run may not even have connected yet
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit on immediate cancel")
		}
	}

	assertNoGoroutineLeak(t, "broker immediate cancel", before)
}

// TestAgentRunCancelNoGoroutineLeak — A.2. Agent has its own
// dispatcher subscription, sys.events evict subscription, the
// runCtx watcher, and the heartbeat ticker; cancel must unwind
// all of them.
//
// We use a stub broker (just respond OK to register) so the test
// stays in agent.Run code paths and doesn't pull broker goroutines
// into our baseline.
func TestAgentRunCancelNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)

	// Stub broker: subscribe to register.req, ACK it OK.
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	if _, err := stub.Subscribe(
		"tether.v2.ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			_ = msg.Respond([]byte(`{"OK":true}`))
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	before := runtime.NumGoroutine()

	// leakExerciseRounds rounds (internal review B9-1): the agent's dispatcher subscription, sys.events
	// evict subscription, runCtx watcher and heartbeat ticker are all per-Run, so a leak of any ONE of
	// them is exactly +1 per round — the shape ±2 cannot see in a single round.
	for i := 0; i < leakExerciseRounds; i++ {
		a, err := agent.New(agent.Config{
			NATSURL:           url,
			SID:               "lab",
			NID:               "lab-1",
			Logger:            silentLog(),
			HeartbeatInterval: 50 * time.Millisecond,
			RegisterTimeout:   1 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- a.Run(ctx) }()

		// Wait until at least one heartbeat has gone out so we know the
		// agent is fully in steady-state (subscription + runCtx watcher
		// + heartbeat loop all live).
		time.Sleep(150 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit on cancel")
		}
	}

	assertNoGoroutineLeak(t, "agent run+cancel", before)
}

// TestTunnelServerCloseNoGoroutineLeak — A.3 part 1. Empty server
// (no clients) — Close should drain immediately and leave no
// goroutines behind.
func TestTunnelServerCloseNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	// leakExerciseRounds rounds (internal review B9-1). An empty server's Start+Close still spawns the
	// control accept loop; one round of a per-Start leak is invisible at ±2.
	for i := 0; i < leakExerciseRounds; i++ {
		controlPort := findFreePort(t)
		srv := newTunnelServer(t, controlPort)
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		srv.Close()
		cancel()
	}

	assertNoGoroutineLeak(t, "tunnel empty close", before)
}

// leakExerciseRounds is the repo-wide constant, re-exported here so this package's call sites read
// naturally. See internal/testharness.LeakExerciseRounds for the derivation, and
// test/determinism/leak_assert_shape_test.go for the gate that makes it a property rather than a habit.
//
// The arithmetic, restated because it is the whole point: the tolerance is ±2 by derivation, so a
// subject exercised ONCE can leak up to two goroutines per exercise and stay green forever. The fix is
// the sample size, not the tolerance. TestBrokerRepeatedRunNoGoroutineLeak below already ran 5 rounds and
// got this right; every other assertion in this file has now been brought up to the same standard
// (internal review B9-1 found six that had not been).
const leakExerciseRounds = testharness.LeakExerciseRounds

// TestTunnelServerCloseWithActiveSessionNoLeak — A.3 part 2. Open
// leakExerciseRounds client sessions, then Close server. Server-side handleAgent +
// publicAcceptLoop + the yamux watcher goroutine + the client's
// streamAcceptLoop all need to unwind — and the per-session ones must unwind
// leakExerciseRounds times, which is what makes a single leaked watcher visible
// against the ±2 noise floor.
//
// This is the regression test for the C1 audit fix: a Close mid-
// REGISTER used to leak the session port across broker restarts.
func TestTunnelServerCloseWithActiveSessionNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	controlPort := findFreePort(t)
	publicPorts := make([]int, leakExerciseRounds)
	for i := range publicPorts {
		publicPorts[i] = findFreePort(t)
	}

	srv := newTunnelServer(t, controlPort)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}

	cli := tunnel.NewClient(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"lab", "lab-1",
		func(int) (int, error) { return 1, nil },
		silentLog(),
	)
	cli.Start(ctx)
	for _, p := range publicPorts {
		if err := cli.Open(p, 1, "tok"); err != nil {
			t.Fatalf("open public port %d: %v", p, err)
		}
	}

	// Close server FIRST (mimics broker restart). Client.Close
	// after observes the sessions have gone away.
	srv.Close()
	for _, p := range publicPorts {
		cli.Close(p)
	}
	cancel()

	assertNoGoroutineLeak(t, "tunnel close with sessions", before)
}

// TestAdminsockServerCloseNoGoroutineLeak — A.5. Boot adminsock
// server, run a few client roundtrips, Close. Audit shard 01 F6
// caught the inner ctx-watcher goroutine leak — this test is the
// regression.
func TestAdminsockServerCloseNoGoroutineLeak(t *testing.T) {
	db := openMemDB(t)

	before := runtime.NumGoroutine()

	// The inner loop already exercised the per-CALL path five times, but the SERVER LIFECYCLE (Start +
	// accept loop + Close) ran exactly once — and the ctx-watcher leak this test was written for (audit
	// shard 01 F6) is per-Start, not per-Call. Internal review B9-1 caught the asymmetry: the half that
	// mattered was the unrepeated half. Both are now leakExerciseRounds.
	for round := 0; round < leakExerciseRounds; round++ {
		socketPath := shortSocketPath(t)
		srv := adminsock.New(socketPath, adminsock.Backend{
			DB: db, Logger: silentLog(),
		})
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}

		// Run a handful of valid Calls so the handle goroutine path
		// has actually been exercised (any leak in that path would
		// show up too).
		cli := &adminsock.Client{Path: socketPath, Timeout: time.Second}
		for i := 0; i < 5; i++ {
			if _, err := cli.Call(adminsock.Request{Op: adminsock.OpSessions}); err != nil {
				t.Fatalf("admin call: %v", err)
			}
		}

		_ = srv.Close()
		cancel()
	}

	assertNoGoroutineLeak(t, "adminsock close", before)
}

// TestBrokerRepeatedRunNoGoroutineLeak — D.15 covers the same
// component-can-restart-cleanly idea. Run the broker, cancel, do
// it again, do it 3 more times. Total goroutine count must stay
// near baseline across every iteration (not just the first), so a
// global state pollution that only kicks in on the Nth iteration
// would also surface.
func TestBrokerRepeatedRunNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	before := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		_, shutdown := startBrokerNoTunnel(t, url, db)
		shutdown()
	}

	assertNoGoroutineLeak(t, "broker repeated run", before)
}
