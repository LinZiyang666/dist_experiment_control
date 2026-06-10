// Package chaos collects fault-injection / partial-failure tests that
// span the broker + agent + ctl boundary. Each file in this package
// covers one chaos dimension (NATS disruption, broker restart, agent
// crash, disk failure, partial fleet failure, timeout propagation).
//
// Conventions:
//   - every Test* sets a hard deadline via context.WithTimeout in its
//     setup so a hang shows up as a Fatal, not as a 120s test runner
//     kill.
//   - every helper that starts a long-lived goroutine returns a stop
//     closure registered to t.Cleanup so a Fatal mid-test still tears
//     down NATS / broker / agent.
//   - tests prefer testharness.WaitFor / WaitNodeOnline over bare
//     time.Sleep — the chaos suite spends most of its time waiting
//     for state convergence and bare sleeps make CI flakey.
package chaos_test

import (
	"context"
	"database/sql"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// short type aliases over testharness so the per-file tests stay terse.

func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

// startJSNATS / startNATS proxy through to the shared harness; they're
// here too so the chaos suite reads consistently with test/p* naming.
func startJSNATS(t *testing.T) string { return testharness.StartJSNATS(t) }
func startNATS(t *testing.T) string   { return testharness.StartNATS(t) }

// reserveTCPPort returns a free TCP port. Mirrors the helper from
// test/p2/agent_nats_startup_resilience_test.go — duplicated here
// because the p2 helper lives in another _test package.
func reserveTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	_, portText, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// startNATSOnPort boots an embedded nats-server bound to a fixed port
// (so a later restart can land on the same URL). t.Cleanup shuts it
// down — but tests that need to "kill mid-flight then restart" should
// call the returned stop() explicitly and pass the same port back to
// startNATSOnPort for the relaunch.
func startNATSOnPort(t *testing.T, port int) (url string, stop func()) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = port
	ns := natstest.RunServer(&opts)
	if !ns.ReadyForConnections(2 * time.Second) {
		ns.Shutdown()
		t.Fatal("embedded nats-server not ready")
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		ns.Shutdown()
		ns.WaitForShutdown()
	}
	t.Cleanup(stop)
	return ns.ClientURL(), stop
}

// seedSession creates the "lab" session with fp as the owner. Mirror
// of seed() in test/p4 / p8. Idempotent on re-call only via session
// uniqueness (caller MUST pass a fresh sid per test).
func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

// brokerHandle bundles a running broker with its stop function so tests
// can lifecycle the process explicitly (kill + restart) without
// fighting Go's defer order. cancelled tracks a one-shot stop call so
// double-stop from chained t.Cleanup is a no-op.
type brokerHandle struct {
	stop      func()
	cancelled atomic.Bool
}

func (h *brokerHandle) Stop() {
	if h.cancelled.Swap(true) {
		return
	}
	h.stop()
}

// startBrokerExplicit boots a broker on url + db. Caller MUST call
// Stop() (or rely on t.Cleanup which the helper registers). Used by
// tests that crash the broker mid-flight — the explicit Stop() is the
// "broker died" event, then a fresh startBrokerExplicit reuses the
// same db (= persistent state recovered).
func startBrokerExplicit(t *testing.T, url string, db *sql.DB) *brokerHandle {
	return startBrokerExplicitCfg(t, broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
}

// startBrokerExplicitCfg lets a test override broker.Config (e.g. for
// disk-pressure / store-dir injection scenarios).
func startBrokerExplicitCfg(t *testing.T, cfg broker.Config) *brokerHandle {
	t.Helper()
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// crude readiness probe: wait for NATS accept.
	for i := 0; i < 50; i++ {
		if nc, err := nats.Connect(cfg.NATSURL, nats.Timeout(200*time.Millisecond)); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	h := &brokerHandle{
		stop: func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Errorf("broker did not exit cleanly within 3s")
			}
		},
	}
	t.Cleanup(h.Stop)
	return h
}

// agentHandle is the agent counterpart to brokerHandle.
type agentHandle struct {
	stop      func()
	cancelled atomic.Bool
}

func (h *agentHandle) Stop() {
	if h.cancelled.Swap(true) {
		return
	}
	h.stop()
}

// startAgentExplicit boots an agent on (sid, nid). Same lifecycle
// contract as startBrokerExplicit: caller may Stop() mid-test (=
// "agent crashed") and a follow-on startAgentExplicit simulates the
// supervisor restart.
func startAgentExplicit(t *testing.T, url, sid, nid string) *agentHandle {
	return startAgentExplicitCfg(t, agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   1500 * time.Millisecond,
	})
}

func startAgentExplicitCfg(t *testing.T, cfg agent.Config) *agentHandle {
	t.Helper()
	a, err := agent.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	h := &agentHandle{
		stop: func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Errorf("agent did not exit cleanly within 3s")
			}
		},
	}
	t.Cleanup(h.Stop)
	return h
}

// withTestDeadline returns a child context that cancels at d (or earlier
// if the parent does). Tests use it as the top-level "if anything
// hangs longer than this, fail rather than wait for the test runner
// kill". Standardizing on this pattern catches the chaos-suite-
// specific failure mode where a hang masquerades as a slow test.
func withTestDeadline(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}
