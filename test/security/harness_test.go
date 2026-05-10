// Package security_test groups red-team / attack-surface tests for the
// tether broker + agent + ctl stack. The tests here are ALL black-box
// (or grey-box) and target documented security boundaries from the
// quality audit (C2 upgrade hardening, C10 admin socket + tunnel info
// disclosure, C11 actor token validation).
//
// Per the user's "v1 security pragmatism" feedback: tests pin
// concrete, repeatable behaviors rather than chasing theoretical
// attack chains. Every test runs entirely under t.TempDir() and
// embedded NATS — none of it touches /etc, /usr, /, or any
// network-reachable URL.
package security_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// silentLog returns a discard logger unless TETHER_TEST_VERBOSE is
// set; matches the convention from test/p3/setup_test.go.
func silentLog() *slog.Logger {
	if os.Getenv("TETHER_TEST_VERBOSE") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// openDB opens a fresh in-memory SQLite with all migrations applied.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// startNATS starts an embedded nats-server (no JS).
func startNATS(t *testing.T) string { return testharness.StartNATS(t) }

// freshUserPub mints a fresh user nkey + returns its public key + fingerprint.
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

// seedSession writes a sessions row + makes ownerFP the owner. PIN
// hash is a placeholder so JoinWithPIN tests must use the matching
// PIN; tests that don't exercise PIN-join leave it as the constant
// below.
const testPINHash = "test-pin-hash"

func seedSession(t *testing.T, db *sql.DB, sid, ownerFP string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, ownerFP, testPINHash, time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

// seedSessionWithPIN seeds with a real argon2id hash so JoinWithPIN
// can be exercised. Only used by tests that need PIN flow.
func seedSessionWithPIN(t *testing.T, db *sql.DB, sid, ownerFP, pin string) {
	t.Helper()
	hash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatalf("auth.HashPIN: %v", err)
	}
	if _, err := session.Create(db, sid, sid, ownerFP, hash, time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

// seedNodeOnline inserts a `nodes` row marked ONLINE so verbs that
// pre-check status (exec/run/expose/upgrade) accept it without a
// real agent.
func seedNodeOnline(t *testing.T, db *sql.DB, sid, nid string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES (?, ?, ?, 'ONLINE', ?)`,
		nid, sid, now, now,
	); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

// startBroker spins a no-auth broker (matches test/p10 pattern). The
// auth_callout-disabled broker is fine for application-layer security
// tests because we drive the attack vector via direct subject pubs;
// where we need NATS-layer pinning we use a separate harness.
type brokerOpt func(*broker.Config)

func withUpgradeAllow(allow []string) brokerOpt {
	return func(c *broker.Config) { c.UpgradeURLAllowlist = allow }
}

func startBroker(t *testing.T, url string, db *sql.DB, opts ...brokerOpt) func() {
	t.Helper()
	cfg := broker.Config{
		NATSURL:                  url,
		DB:                       db,
		Logger:                   silentLog(),
		ReconcileInterval:        50 * time.Millisecond,
		StaleAfter:               300 * time.Millisecond,
		OfflineAfter:             900 * time.Millisecond,
		UpgradeForwardTimeoutDur: 3 * time.Second,
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
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// connectAnon opens an anonymous NATS connection. Used for direct
// subject pub attacks that bypass the application-layer broker
// path.
func connectAnon(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}


// newTestServer returns an httptest.Server that always serves body
// (with a 200 status) for any path. The caller must Close() it.
func newTestServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
}

// startAgentForUpgrade boots an in-process agent wired to a sandboxed
// upgrade-target executable so a successful upgrade doesn't trample
// the go-test binary. Returns a teardown func.
func startAgentForUpgrade(t *testing.T, url, sid, nid string, allow []string) func() {
	t.Helper()
	exePath := filepath.Join(t.TempDir(), "tether-target")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	a, err := agent.New(agent.Config{
		NATSURL:               url,
		SID:                   sid,
		NID:                   nid,
		Logger:                silentLog(),
		HeartbeatInterval:     100 * time.Millisecond,
		RegisterTimeout:       2 * time.Second,
		UpgradeURLAllowlist:   allow,
		UpgradeNoExit:         true,
		UpgradeExecutablePath: exePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	// Brief wait for register; matches test/p10 pattern.
	time.Sleep(300 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// scriptPath returns the absolute path to scripts/install.sh.
// Computed off this test file's location (matches test/p10).
func scriptPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repo, "scripts", "install.sh")
}
