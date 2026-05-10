// broker_lifecycle_test.go — D.* dimension. Boot/cancel timing,
// repeated boots, adminsock double-bind. None of these need NATS-
// auth_callout; they all run against a plain embedded NATS so the
// failure modes we surface are pure broker-side.
package concurrency_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/nats-io/nats.go"
)

// TestBrokerCancelReturnsQuickly — D.13. Run + immediate ctx
// cancel must return within 1s. A regression here would mean
// some shutdown step (NATS Drain, JS probe, disk monitor) is
// blocking on something it shouldn't.
func TestBrokerCancelReturnsQuickly(t *testing.T) {
	url := startNATS(t)
	db := openMemDB(t)

	b, err := broker.New(broker.Config{
		NATSURL: url, DB: db, Logger: silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// Wait for steady-state.
	if err := waitNATSConnect(url, 2*time.Second); err != nil {
		cancel()
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	t0 := time.Now()
	cancel()
	select {
	case err := <-done:
		dt := time.Since(t0)
		if dt > 1*time.Second {
			t.Errorf("Run took %v to return after cancel; budget is 1s", dt)
		}
		// Run should return ctx.Err() (context.Canceled).
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v; want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("broker did not return within 3s of cancel")
	}
}

// TestAgentCancelReturnsQuickly — D.14 agent variant. Same shape.
func TestAgentCancelReturnsQuickly(t *testing.T) {
	url := startNATS(t)

	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	if _, err := stub.Subscribe(
		"tether.v1.ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) { _ = msg.Respond([]byte(`{"OK":true}`)) },
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

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

	time.Sleep(150 * time.Millisecond)

	t0 := time.Now()
	cancel()
	select {
	case err := <-done:
		dt := time.Since(t0)
		if dt > 1*time.Second {
			t.Errorf("agent.Run took %v to return after cancel; budget is 1s", dt)
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v; want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not return within 3s of cancel")
	}
}

// TestAdminsockDoubleBindRejected — D.16. Two Servers pointed at
// the same socket path: the second Start MUST return an error
// instead of silently stealing the path. The socket-alive probe
// in adminsock.Start enforces this.
func TestAdminsockDoubleBindRejected(t *testing.T) {
	db := openMemDB(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "tether-run", "admin.sock")

	srv1 := adminsock.New(socketPath, adminsock.Backend{
		DB: db, Logger: silentLog(),
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	if err := srv1.Start(ctx1); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer func() { _ = srv1.Close() }()

	// Second Start on the same path: must error.
	srv2 := adminsock.New(socketPath, adminsock.Backend{
		DB: db, Logger: silentLog(),
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	err := srv2.Start(ctx2)
	if err == nil {
		_ = srv2.Close()
		t.Fatal("second Start MUST fail when socket is already bound")
	}
	// Hostile case the comment in server.go calls out: the
	// "active socket already exists" error string. We only check
	// that it's non-nil — checking the exact text would couple
	// the test to a comment.
}

// TestAdminsockReBindAfterClose — companion to D.16. After Close,
// the socket path is reusable: a fresh Server should be able to
// Start on the same path. (Tests the cleanup path in Close: it
// removes the socket file inode.)
func TestAdminsockReBindAfterClose(t *testing.T) {
	db := openMemDB(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "tether-run", "admin.sock")

	srv1 := adminsock.New(socketPath, adminsock.Backend{
		DB: db, Logger: silentLog(),
	})
	ctx1, cancel1 := context.WithCancel(context.Background())
	if err := srv1.Start(ctx1); err != nil {
		cancel1()
		t.Fatalf("first Start: %v", err)
	}
	_ = srv1.Close()
	cancel1()

	srv2 := adminsock.New(socketPath, adminsock.Backend{
		DB: db, Logger: silentLog(),
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := srv2.Start(ctx2); err != nil {
		t.Fatalf("re-Start after Close: %v", err)
	}
	_ = srv2.Close()
}
