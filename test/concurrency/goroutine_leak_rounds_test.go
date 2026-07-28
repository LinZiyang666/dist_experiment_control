// goroutine_leak_rounds_test.go — N>=5 siblings for the leak gates whose subject is a COMPONENT
// LIFECYCLE and which currently exercise that lifecycle exactly once.
//
// WHY THIS FILE EXISTS
// --------------------
// assertNoGoroutineLeak passes when the delta is <= GoroutineLeakTolerance (2), and that floor is
// correct — internal/testharness/leakgate.go derives it from the runtime's shifting helper goroutines.
// The arithmetic consequence is the one leakExerciseRounds already states in goroutine_leak_test.go: a
// subject exercised ONCE can leak one or two goroutines per exercise and stay green forever.
//
// leakgate.go says "every leak assertion in this repo runs its subject N>=5 times". At the time this
// file was written that was true of two assertions. These three tests are the missing siblings for the
// three whose subject is a boot/shutdown cycle:
//
//	TestAdminsockServerCloseNoGoroutineLeak  — boots the server ONCE (its 5-iteration loop is around
//	                                           Call, not around Start/Close)
//	TestAgentRunCancelNoGoroutineLeak        — one agent.Run + cancel
//	TestTunnelServerCloseNoGoroutineLeak     — one Start + Close
//
// They are ADDITIVE. The incumbents keep their own value: a single-shot cancel exercises the
// immediate-teardown race that a settled loop does not. What they cannot do is see a steady per-boot
// leak, and that is what these add.
//
// NON-VACUITY (the mutation each one is built to catch)
// ----------------------------------------------------
// Inject one goroutine that never exits into the per-boot path of the component under test — e.g.
// `go func() { select {} }()` at the top of internal/adminsock.(*Server).acceptLoop, which is the shape
// of the audit-shard-01 F6 ctx-watcher leak that server.go:214-227 documents. The N=5 test here reports
// +5 and fails; the N=1 incumbent reports +1 and passes. That asymmetry IS the reason this file exists,
// and it was executed rather than assumed before the file was committed.
package concurrency_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/nats-io/nats.go"
)

// settledBaseline waits for the warm-up round's teardown to finish, then returns the goroutine count.
//
// This is not politeness, it is correctness, and it was found by mutation rather than by reasoning: with
// a leak injected into agent.Run the first draft of TestAgentRepeatedRunNoGoroutineLeak stayed GREEN,
// because nats.go's drain/flusher goroutines from the warm-up run were still alive when the baseline was
// sampled and their exit during the measured rounds cancelled the +5 out. A baseline taken while the
// previous exercise is still unwinding does not measure a leak, it measures a race between two
// teardowns — and it fails OPEN, which is the direction that matters. Same reason test/d4 and test/d5
// sleep + GC before their baselines (test/d4/forward_test.go:199-201).
func settledBaseline(t *testing.T) int {
	t.Helper()
	last := runtime.NumGoroutine()
	stable := 0
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		runtime.GC()
		n := runtime.NumGoroutine()
		if n >= last {
			stable++
			if stable >= 3 {
				return n
			}
		} else {
			stable = 0
		}
		last = n
	}
	return runtime.NumGoroutine()
}

// TestAdminsockRepeatedStartCloseNoGoroutineLeak boots and closes the admin socket server
// leakExerciseRounds times against a fresh socket path each round.
//
// The leak this is aimed at is one goroutine per acceptLoop: internal/adminsock/server.go:221 spawns a
// ctx-watcher whose exit depends on `defer close(done)` at :228, and audit shard 01 F6 was exactly that
// goroutine surviving a Close()-initiated shutdown. One boot cannot distinguish that from runtime noise.
func TestAdminsockRepeatedStartCloseNoGoroutineLeak(t *testing.T) {
	db := openMemDB(t)

	// Warm-up boot so first-time allocations (socket dir, slog handler, sql conn) are inside the
	// baseline rather than counted as the measured delta.
	warm := adminsock.New(shortSocketPath(t), adminsock.Backend{DB: db, Logger: silentLog()})
	warmCtx, warmCancel := context.WithCancel(context.Background())
	if err := warm.Start(warmCtx); err != nil {
		warmCancel()
		t.Fatal(err)
	}
	_ = warm.Close()
	warmCancel()

	before := settledBaseline(t)

	for i := 0; i < leakExerciseRounds; i++ {
		socketPath := shortSocketPath(t)
		srv := adminsock.New(socketPath, adminsock.Backend{DB: db, Logger: silentLog()})
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatalf("round %d: start: %v", i, err)
		}
		// One real round-trip per boot so the accept + handle paths are actually entered; a server
		// that never accepted anything would not exercise the goroutines this is measuring.
		cli := &adminsock.Client{Path: socketPath, Timeout: 2 * time.Second}
		if _, err := cli.Call(adminsock.Request{Op: adminsock.OpSessions}); err != nil {
			cancel()
			t.Fatalf("round %d: call: %v", i, err)
		}
		_ = srv.Close()
		cancel()
	}

	assertNoGoroutineLeak(t, "adminsock repeated start+close", before)
}

// TestAgentRepeatedRunNoGoroutineLeak runs and cancels the agent leakExerciseRounds times against one
// stub broker.
//
// TestAgentRunCancelNoGoroutineLeak names four things that must unwind (dispatcher subscription,
// sys.events evict subscription, the runCtx watcher, the heartbeat ticker). Any ONE of them surviving is
// +1 per Run, which its single Run cannot separate from the ±2 floor; leakExerciseRounds runs make it +N.
func TestAgentRepeatedRunNoGoroutineLeak(t *testing.T) {
	url := startNATS(t)

	// Stub broker: ACK every register.req so the agent reaches steady state instead of retrying.
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	if _, err := stub.Subscribe(
		"tether.v2.ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) { _ = msg.Respond([]byte(`{"OK":true}`)) },
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	runOnce := func(t *testing.T, round int) {
		t.Helper()
		a, err := agent.New(agent.Config{
			NATSURL:           url,
			SID:               "lab",
			NID:               "lab-1",
			Logger:            silentLog(),
			HeartbeatInterval: 50 * time.Millisecond,
			RegisterTimeout:   1 * time.Second,
		})
		if err != nil {
			t.Fatalf("round %d: new: %v", round, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- a.Run(ctx) }()
		// Long enough for the subscriptions + heartbeat loop to be live, so cancel is tearing down a
		// steady-state agent rather than racing its setup (the incumbent test covers that race).
		time.Sleep(150 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: agent did not exit on cancel", round)
		}
	}

	// Warm-up outside the baseline: the first Run allocates the NATS connection pool and slog state.
	runOnce(t, -1)

	before := settledBaseline(t)

	for i := 0; i < leakExerciseRounds; i++ {
		runOnce(t, i)
	}

	assertNoGoroutineLeak(t, "agent repeated run+cancel", before)
}

// TestTunnelServerRepeatedCloseNoGoroutineLeak starts and closes an empty tunnel.Server
// leakExerciseRounds times.
//
// The client-facing per-session shape is covered by TestTunnelServerCloseWithActiveSessionNoLeak. This
// covers the other axis: the server's OWN per-boot goroutines (acceptLoop and the ctx watcher started in
// Server.Start) must not accumulate across restarts, which is what a broker restart does in production.
func TestTunnelServerRepeatedCloseNoGoroutineLeak(t *testing.T) {
	// Warm-up boot, excluded from the baseline.
	{
		srv := newTunnelServer(t, findFreePort(t))
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		srv.Close()
		cancel()
	}

	before := settledBaseline(t)

	for i := 0; i < leakExerciseRounds; i++ {
		srv := newTunnelServer(t, findFreePort(t))
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatalf("round %d: start: %v", i, err)
		}
		srv.Close()
		cancel()
	}

	assertNoGoroutineLeak(t, "tunnel server repeated start+close", before)
}
