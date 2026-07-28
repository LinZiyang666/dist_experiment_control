// Package concurrency_test holds the goroutine-leak / fd-leak /
// stress / race tests that run against the broker / agent / tunnel /
// adminsock primitives end-to-end.
//
// The tests deliberately avoid the goleak library — we don't want
// to introduce a new dep, and a hand-rolled poll-with-tolerance is
// adequate for the leak shapes (per-goroutine mismatches of >2 in
// steady state are clear regressions; jitter from the runtime never
// exceeds that).
package concurrency_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
)

// silentLog returns a discard-only slog handler so test output is
// not polluted by broker / agent / tunnel info lines.
// B9: delegates to internal/testharness, like startNATS above. The handler allocates no goroutines in
// either branch, so this is safe for a package whose whole purpose is counting them.
func silentLog() *slog.Logger {
	return testharness.SilentLog()
}

// shortSocketPath returns a Unix socket path under a SHORT temp dir.
// t.TempDir() embeds the full test name, which on macOS pushes the
// path past the kernel's sun_path limit (104 bytes on Darwin, 108 on
// Linux) and bind fails with EINVAL. os.MkdirTemp("", ...) stays under
// the limit on both platforms. The tether-run subdir is left for
// adminsock.Server.Start to create so its mkdir(0o700) path is
// exercised.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "tether-run", "admin.sock")
}

// startNATS launches an embedded NATS server on an ephemeral port, per-test (no t.Parallel here) so it
// is torn down via t.Cleanup.
//
// B9: it now DELEGATES to testharness.StartNATS instead of cloning it. The clone's justification was
// written down and it was false — "the harness reuses a slog logger that allocates background goroutines
// we'd want to account for". testharness.StartNATS takes no logger and starts no goroutines of its own;
// the two bodies were character-for-character identical (natstest.DefaultTestOptions, Port = -1,
// RunServer, a Shutdown+WaitForShutdown cleanup, a 2s ReadyForConnections, ClientURL). So the copy was
// costing this package a maintenance obligation it was told it had a reason for.
//
// This matters more here than in an ordinary package: these tests assert goroutine counts before and
// after the server lifecycle, so a divergence between the two spellings would show up as a leak-gate
// failure whose cause is in a file nobody would think to look at. One spelling, one teardown.
func startNATS(t *testing.T) string {
	t.Helper()
	return testharness.StartNATS(t)
}

// openMemDB returns a fresh in-memory SQLite handle with all
// migrations applied. Closed via t.Cleanup. Prefer this over a
// file:// DB for goroutine/fd leak tests so the migration code path
// doesn't hold any FDs open beyond the *sql.DB conn pool.
func openMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// waitNATSConnect polls Connect against url until the server starts
// accepting connections OR ctx expires. Returns nil on success and
// an error on timeout — callers t.Fatal on err.
func waitNATSConnect(url string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		nc, err := nats.Connect(url, nats.Timeout(200*time.Millisecond))
		if err == nil {
			nc.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("nats never accepted a connection")
}

// findFreePort grabs an ephemeral port the kernel hands us and
// closes the listener so callers can re-bind it. Race-prone in
// theory, fine for single-process tests.
func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// assertNoGoroutineLeak delegates to the single implementation in internal/testharness (B9).
//
// The body used to live here, and identically in test/d4, test/d5 and test/d8 — four copies whose only
// differences were a local variable name and one comment block. The derivation of the ±2 floor and the
// ~1s poll window now lives in exactly one place, which matters because a number derived in four places
// is a number that gets tuned in one.
//
// The local NAME stays so the ~15 call sites in this package do not churn; unlike silentLog and
// startNATS below, this helper touches no logger and opens no server, so there is nothing about it that
// needs to stay package-local.
func assertNoGoroutineLeak(t *testing.T, label string, before int) {
	t.Helper()
	testharness.AssertNoGoroutineLeak(t, label, before)
}

// startBrokerNoTunnel boots a minimal broker (no tunnel, no admin,
// no JS) so the goroutine baseline is small enough to make leak
// asserts tractable. The shutdown closure cancels ctx and blocks
// until Run returns.
func startBrokerNoTunnel(t *testing.T, url string, db *sql.DB) (b *broker.Broker, shutdown func()) {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Wait for the register subscription to be live so callers can
	// safely send requests immediately. Using the same probe pattern
	// as broker_test.waitNATSReady but timing out faster.
	if err := waitNATSConnect(url, 2*time.Second); err != nil {
		cancel()
		t.Fatal(err)
	}
	// Tiny additional grace so subs are installed before tests act.
	time.Sleep(50 * time.Millisecond)
	return b, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("broker did not exit on cancel")
		}
	}
}

// newTunnelServer is a thin wrapper around tunnel.NewServer used by
// every tunnel-side concurrency test. The lookup always accepts —
// these tests stress concurrency, not auth.
func newTunnelServer(t *testing.T, controlPort int) *tunnel.Server {
	t.Helper()
	return tunnel.NewServer(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1",
		func(_, _ string, _ int, _ string, _ int64) error { return nil },
		silentLog(),
	)
}

// raceLatchClose closes ch exactly once even if invoked from
// multiple goroutines (sync.Once internally). Used by stress tests
// that need a one-shot "go" signal across N parallel workers.
type raceLatch struct {
	once sync.Once
	ch   chan struct{}
}

func newRaceLatch() *raceLatch { return &raceLatch{ch: make(chan struct{})} }
func (l *raceLatch) wait()     { <-l.ch }
func (l *raceLatch) release()  { l.once.Do(func() { close(l.ch) }) }

// fmtErr is a one-liner so tests can ".Error()" cleanly when joining
// goroutine error channels. Keeps the call-site short.
func fmtErr(format string, a ...any) error { return fmt.Errorf(format, a...) }
