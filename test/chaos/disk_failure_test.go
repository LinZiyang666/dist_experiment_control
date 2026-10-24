// Disk-failure chaos tests. Cover dimension D from the chaos
// checklist: SQLite path unwritable, JetStream store_dir unwritable,
// upgrade tarball write-fail, adminsock parent dir unwritable.
//
// These tests prefer "operator gets a clean error message" over
// "broker silently keeps running with a degraded subsystem". A
// panic / unhandled error here is a real reliability bug.
package chaos_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// runningAsRoot returns true when the test process can ignore
// filesystem permission bits (root / CAP_DAC_OVERRIDE). Read-only
// dir tests degrade to t.Skip in that case — they'd false-pass
// because the kernel never returns EACCES for the broker's open()
// either. CI runners typically aren't root, but local sudo + go test
// would silently break the assertions otherwise.
func runningAsRoot() bool {
	return os.Geteuid() == 0
}

// TestSQLiteOpenFailsCleanlyOnReadOnlyDir pins D.15: a broker
// configured with an SQLite DSN under a read-only parent must surface
// the error from broker.New rather than panic later.
func TestSQLiteOpenFailsCleanlyOnReadOnlyDir(t *testing.T) {
	if runningAsRoot() {
		t.Skip("perm tests require non-root; running as uid=0 would bypass mode bits")
	}
	parent := t.TempDir()
	roDir := filepath.Join(parent, "ro")
	if err := os.Mkdir(roDir, 0o500); err != nil { // r-x, no write
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0o700) })
	proveInjected(t, "directory is read-only", dirUnwritable(roDir))

	dbPath := filepath.Join(roDir, "state.db")
	_, err := storage.Open("file:" + dbPath)
	if err == nil {
		t.Fatal("expected storage.Open to fail under read-only parent")
	}
	// We just want the error to be non-empty + non-panicking. The exact
	// text comes from modernc, which we don't pin.
	if err.Error() == "" {
		t.Errorf("storage.Open returned empty-string error")
	}
}

// TestBrokerNewRequiresDB pins the validation contract: passing nil
// DB to broker.New surfaces a clear error rather than nil-derefing
// later inside Run. This is the operator-facing equivalent of D.15
// when the SQLite open succeeded but somehow yielded nil (config
// bug; protects against future refactors that do auto-open).
func TestBrokerNewRequiresDB(t *testing.T) {
	_, err := broker.New(broker.Config{NATSURL: "nats://127.0.0.1:4222"})
	if err == nil {
		t.Fatal("expected broker.New to require DB")
	}
	if !strings.Contains(err.Error(), "DB") {
		t.Errorf("broker.New nil-DB error should mention DB: %v", err)
	}
}

// TestBrokerNewRequiresNATSURL is the symmetric validation pin.
func TestBrokerNewRequiresNATSURL(t *testing.T) {
	db := openDB(t)
	_, err := broker.New(broker.Config{DB: db})
	if err == nil {
		t.Fatal("expected broker.New to require NATSURL")
	}
	if !strings.Contains(err.Error(), "NATSURL") {
		t.Errorf("broker.New nil-NATSURL error should mention NATSURL: %v", err)
	}
}

// TestAdminSockStartFailsOnReadOnlyParent pins D.18: adminsock.Start
// must surface a clear error when its parent dir is unwritable rather
// than panic / hang.
func TestAdminSockStartFailsOnReadOnlyParent(t *testing.T) {
	if runningAsRoot() {
		t.Skip("perm tests require non-root")
	}
	parent := t.TempDir()
	roDir := filepath.Join(parent, "noperm")
	if err := os.Mkdir(roDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0o700) })

	// adminsock.New takes a socket path; the parent (`<roDir>/admin`)
	// will be created mode 0700 by Start. The MkdirAll under a 0500
	// parent should fail.
	proveInjected(t, "directory is read-only", dirUnwritable(roDir))
	srv := adminsock.New(filepath.Join(roDir, "admin", "admin.sock"), adminsock.Backend{
		DB: openDB(t),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := srv.Start(ctx)
	if err == nil {
		_ = srv.Close()
		t.Fatal("adminsock.Start unexpectedly succeeded under read-only parent")
	}
	if err.Error() == "" {
		t.Errorf("adminsock.Start: empty-string error")
	}
}

// The D.17 lower-level non-panic invariant remains: a forwarded request
// returns a clean error without downloading or writing. S8 moves candidate
// fetching to ctl; a bare URL cannot authorize the agent's old install path.
// The actual write-failure case remains covered by agent upgrade unit tests.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §5.2 / §7.2
func TestAgentUpgradeRequiresTransactionBeforeURLFetch(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseAgentUpgradeUnfencedURL(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "unfenced agent URL", g)
	testharness.AssertNoFDLeak(t, "unfenced agent URL", fd, 0)
}

// TestDiskUsageFnInjectionTriggersPressureEvent pins H.4: with a
// Config.DiskUsageFn that reports >threshold, the broker must emit a
// sys.events{type:disk_pressure} on the very first tick.
//
// This isn't a "disk full" test per se (D.16) but it pins the same
// path: the disk-pressure subsystem responds correctly to a
// constrained-disk environment without crashing.
func TestDiskUsageFnInjectionTriggersPressureEvent(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)

	pressed := make(chan struct{}, 1)
	cfg := broker.Config{
		NATSURL:               url,
		DB:                    db,
		Logger:                silentLog(),
		ReconcileInterval:     50 * time.Millisecond,
		StaleAfter:            300 * time.Millisecond,
		OfflineAfter:          900 * time.Millisecond,
		StoreDir:              t.TempDir(),
		DiskCheckInterval:     50 * time.Millisecond,
		DiskPressureThreshold: 0.5,
		DiskUsageFn: func(path string) (uint64, uint64, error) {
			return 900, 1000, nil // 90% — above 0.5 threshold
		},
	}

	// Subscribe BEFORE starting broker so we don't miss the start-up
	// edge emission.
	probeNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer probeNC.Close()
	if _, err := probeNC.Subscribe(proto.SubjSysEvents, func(msg *nats.Msg) {
		if strings.Contains(string(msg.Data), `"type":"disk_pressure"`) {
			select {
			case pressed <- struct{}{}:
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	_ = startBrokerExplicitCfg(t, cfg)

	select {
	case <-pressed:
	case <-time.After(3 * time.Second):
		t.Fatal("disk_pressure event never emitted despite injected 90% usage")
	}
}
