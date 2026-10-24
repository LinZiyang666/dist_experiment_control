package broker

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// origin: hot-update S5 internal review rv-authority-2
func TestProxyAuthoritySQLWaitUsesLookupBudget(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseProxyAuthoritySQLBudget(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "proxy authority SQL budget", g)
	testharness.AssertNoFDLeak(t, "proxy authority SQL budget", fd, 0)
}

func exerciseProxyAuthoritySQLBudget(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authority.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	b, _ := authorityFixture(t, db)
	if _, err := db.Exec(`UPDATE nodes SET proxy_capable=1; UPDATE port_allocations SET home_broker=''`); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = writer.Exec("ROLLBACK") }()
	start := time.Now()
	a, err := proxyAuthoritySnapshot(b, "lab", "lab-1")
	if elapsed := time.Since(start); elapsed > tunnel.LookupBudget+300*time.Millisecond {
		t.Errorf("SQLite lock exceeded lookup budget: %s", elapsed)
	}
	if !errors.Is(err, errTunnelAuthorityRetry) || a.Enabled || a.Allocation != nil || len(a.Keys) != 0 {
		t.Errorf("locked snapshot must return typed retryable refusal without authority: %v", err)
	}
	if _, err := writer.Exec("ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	var busy int
	if err := db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil || busy != 5000 {
		t.Fatalf("pooled busy timeout not restored: %d/%v", busy, err)
	}
	if a, err := proxyAuthoritySnapshot(b, "lab", "lab-1"); err != nil || !a.Enabled || len(a.Keys) != 2 {
		t.Fatalf("snapshot failed after lock release: %v", err)
	}
	// Pool wait spends the same budget, and must return without a detached worker.
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	start = time.Now()
	a, err = proxyAuthoritySnapshot(b, "lab", "lab-1")
	if elapsed := time.Since(start); elapsed > tunnel.LookupBudget+300*time.Millisecond {
		t.Errorf("pool wait exceeded lookup budget: %s", elapsed)
	}
	if !errors.Is(err, errTunnelAuthorityRetry) || a.Enabled || len(a.Keys) != 0 {
		t.Errorf("pool timeout must return typed retryable refusal: %v", err)
	}
	if db.Stats().InUse != 1 {
		t.Errorf("authority retained a pooled connection")
	}
}
