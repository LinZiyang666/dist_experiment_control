// Package cli_e2e_test is the deepened CLI end-to-end suite.
//
// Existing per-phase suites (test/p4..p10) cover one verb each at a
// happy-path granularity. This package bundles **multi-command
// workflows** + boundary cases the per-phase suites skim over:
// session full lifecycle, exec / run I/O extremes, expose lifecycle
// + name/port reuse, history flag combinations, fleet upgrade, and
// cross-session isolation.
//
// Same anonymous-NATS pattern as test/p4..p7 — auth_callout coverage
// is already pinned in test/p3, and adding it here would only add
// minutes per assertion without exercising any new business rule.
package cli_e2e_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// startNATS / startJSNATS / openDB / silentLog / freshUserPub are
// thin re-exports so each *_test.go file in this package keeps the
// same naming the per-phase suites use.
func startNATS(t *testing.T) string                  { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                    { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                        { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string)     { return testharness.FreshUserPub(t) }

// seedSession inserts an ACTIVE session row owned by fp. Mirrors the
// helper used by every per-phase suite — copy-pasted here rather than
// imported because the per-phase versions live in `_test.go` files
// (Go test packages can't cross-import).
func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create(%s): %v", sid, err)
	}
}

// brokerOpts mutates a broker.Config before broker.New is called.
type brokerOpts func(*broker.Config)

// startBroker boots an in-process broker (no auth_callout, no JS).
// Use startJSBroker when JetStream is required.
func startBroker(t *testing.T, url string, db *sql.DB, opts ...brokerOpts) func() {
	t.Helper()
	cfg := broker.Config{
		NATSURL:                 url,
		DB:                      db,
		Logger:                  silentLog(),
		ReconcileInterval:       50 * time.Millisecond,
		StaleAfter:              300 * time.Millisecond,
		OfflineAfter:            900 * time.Millisecond,
		PublicHost:              "test.local",
		PortBandLow:             14000,
		PortBandHigh:            14002,
		PortRevokeAfterDur:      300 * time.Millisecond,
		ExposeForwardTimeoutDur: 1 * time.Second,
		UpgradeForwardTimeoutDur: 5 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	testharness.WaitConnect(t, url, 3*time.Second)
	// Subscriptions install synchronously inside Run() before the
	// connect loop returns, but the audit/reconciler tickers race
	// the first probe. A 50ms slack lets the goroutines settle —
	// shorter than the per-phase suites' 150ms because we don't
	// run JS by default.
	time.Sleep(50 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// startAgent boots an in-process agent for (sid, nid) and waits for
// the broker to mark its node row ONLINE. Returns a stop function
// that's idempotent (callable from both an explicit defer and t.Cleanup).
func startAgent(t *testing.T, url, sid, nid string, opts ...func(*agent.Config)) func() {
	t.Helper()
	cfg := agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	a, err := agent.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// Don't WaitNodeOnline here — some tests intentionally start an
	// agent against a tombstoned session (register fails). Caller
	// can poll nodes table when it needs a guarantee.
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// connect opens a NATS client with sane defaults; the caller closes it.
func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	return nc
}
