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

	_, shutdown := startBrokerNoTunnel(t, url, db)
	shutdown()

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
		"tether.v1.ctrl.s.*.node.*.register.req",
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

	assertNoGoroutineLeak(t, "agent run+cancel", before)
}

// TestTunnelServerCloseNoGoroutineLeak — A.3 part 1. Empty server
// (no clients) — Close should drain immediately and leave no
// goroutines behind.
func TestTunnelServerCloseNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	controlPort := findFreePort(t)
	srv := newTunnelServer(t, controlPort)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	srv.Close()
	cancel()

	assertNoGoroutineLeak(t, "tunnel empty close", before)
}

// TestTunnelServerCloseWithActiveSessionNoLeak — A.3 part 2. Open
// one client session, then Close server. Server-side handleAgent +
// publicAcceptLoop + the yamux watcher goroutine + the client's
// streamAcceptLoop all need to unwind.
//
// This is the regression test for the C1 audit fix: a Close mid-
// REGISTER used to leak the session port across broker restarts.
func TestTunnelServerCloseWithActiveSessionNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

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
	if err := cli.Open(publicPort, 1, "tok"); err != nil {
		t.Fatal(err)
	}

	// Close server FIRST (mimics broker restart). Client.Close
	// after observes the session has gone away.
	srv.Close()
	cli.Close(publicPort)
	cancel()

	assertNoGoroutineLeak(t, "tunnel close with session", before)
}

// TestAdminsockServerCloseNoGoroutineLeak — A.5. Boot adminsock
// server, run a few client roundtrips, Close. Audit shard 01 F6
// caught the inner ctx-watcher goroutine leak — this test is the
// regression.
func TestAdminsockServerCloseNoGoroutineLeak(t *testing.T) {
	db := openMemDB(t)

	before := runtime.NumGoroutine()

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
