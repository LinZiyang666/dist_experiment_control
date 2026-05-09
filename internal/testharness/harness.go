// Package testharness collects helpers shared by the integration test
// packages under test/p2 .. test/p7. They were copy-pasted across each
// phase's test file in the early days; pulled into one package here
// so a JetStream config tweak / nats-server option change happens in
// exactly one place.
//
// Per-phase test files still own the harness pieces that vary by
// phase (broker config, agent config, auth_callout wiring, custom
// startBroker timings) — only the truly identical primitives live
// here.
package testharness

import (
	"database/sql"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/storage"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// StartNATS spins up an embedded nats-server (no JetStream) on a
// random port and returns its client URL. The server is shut down
// via t.Cleanup so callers don't need to track it.
func StartNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// StartJSNATS is StartNATS with JetStream enabled. The store dir is
// allocated under t.TempDir() so file storage is isolated per test
// and reaped on test exit.
func StartJSNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded JS nats-server not ready")
	}
	waitJSReady(t, ns)
	return ns.ClientURL()
}

func waitJSReady(t *testing.T, ns *natsserver.Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ns.JetStreamEnabled() && ns.JetStreamIsCurrent() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("JS not ready after 2s")
}

// OpenDB returns a fresh in-memory SQLite handle with all migrations
// applied. Closed via t.Cleanup.
func OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// SilentLog returns a discard logger by default. With
// TETHER_TEST_VERBOSE set, returns a debug logger to stderr — useful
// when chasing flakes locally.
func SilentLog() *slog.Logger {
	if os.Getenv("TETHER_TEST_VERBOSE") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// WaitFor polls predicate every interval until it returns true or
// deadline elapses. Returns false on timeout. Designed to replace
// the "sleep N then assert" anti-pattern that dominates the
// pre-quality-audit test suite — quality audit shard 05 found 23
// bare-sleep barriers; this helper is the standard replacement.
//
// Tests use it like:
//
//	if !testharness.WaitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
//	    return store.Has(key)
//	}) {
//	    t.Fatalf("key %q never appeared", key)
//	}
//
// 20ms polling is fine for in-process state; bump for RPC-backed
// predicates so we don't DOS the system under test.
func WaitFor(t *testing.T, deadline, interval time.Duration, predicate func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if predicate() {
			return true
		}
		time.Sleep(interval)
	}
	return predicate()
}

// WaitNodeOnline polls SQLite for the (sid, nid) agent row to
// reach status='ONLINE'. This is the canonical "both broker AND
// agent are ready" signal: the broker has installed its register
// subscription (else the agent's register.req gets ErrNoResponders
// forever) AND the agent's register actually round-tripped.
//
// Replaces the "time.Sleep(150ms) then time.Sleep(300ms)" stack
// that dominates phase test helpers. On a fast box the row
// usually appears in <50ms; on a busy CI runner it can take
// 1-2s — either way this returns as soon as it's there.
func WaitNodeOnline(t *testing.T, db *sql.DB, sid, nid string, deadline time.Duration) {
	t.Helper()
	if !WaitFor(t, deadline, 20*time.Millisecond, func() bool {
		var status string
		err := db.QueryRow(
			`SELECT status FROM nodes WHERE sid=? AND nid=?`, sid, nid,
		).Scan(&status)
		return err == nil && status == "ONLINE"
	}) {
		t.Fatalf("agent %s/%s never reached ONLINE within %v", sid, nid, deadline)
	}
}

// WaitConnect polls nats.Connect against url until it succeeds or
// deadline elapses. Use as the broker-ready probe when the test
// doesn't also start an agent (so WaitNodeOnline isn't applicable).
func WaitConnect(t *testing.T, url string, deadline time.Duration) {
	t.Helper()
	if !WaitFor(t, deadline, 20*time.Millisecond, func() bool {
		nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
		if err != nil {
			return false
		}
		nc.Close()
		return true
	}) {
		t.Fatalf("NATS at %s never accepted a connection within %v", url, deadline)
	}
}

// FreshUserPub generates a brand-new NATS user nkey and returns its
// public key + the SHA256 fingerprint that broker-side membership /
// owner checks use.
func FreshUserPub(t *testing.T) (pub, fp string) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err = kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	kp.Wipe()
	fp, err = auth.FingerprintFromActor(pub)
	if err != nil {
		t.Fatal(err)
	}
	return pub, fp
}
