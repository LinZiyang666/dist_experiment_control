package main

import (
	"io"
	"path/filepath"
	"runtime"

	"github.com/LinZiyang666/tether/internal/testharness"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// A cold single-mode start must not become stricter than storage.Open: a
// database whose ledger carries a migration from a newer release (an N-1
// binary rollback) used to start, and the image-eligibility stamp is only
// required for a hot cut. Refusing the cold start strands a downgrade.
// origin: hot-update S6 internal review rv-writer
func TestServeColdSingleStartToleratesNewerMigrationLedger(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseServeNewerMigrationLedger(t)
	}
	testharness.AssertNoGoroutineLeak(t, "newer-ledger single serve", gb)
	testharness.AssertNoFDLeak(t, "newer-ledger single serve", fb, 0)
}

func exerciseServeNewerMigrationLedger(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations(version) VALUES('9999_from_newer_release.sql')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--db", dbPath, "--nats-url=nats://127.0.0.1:1", "--admin-socket=", "--tunnel-addr="})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	err = cmd.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), "broker: NATS connect:") {
		t.Fatalf("cold single start refused before reaching Run: %v", err)
	}
	identity, err := storage.CanonicalStoreIdentity(dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	if writers, readers := storage.DefaultDriverRegistry().Connections(identity); writers != 0 || readers != 0 {
		t.Fatalf("newer-ledger serve retained actual drivers: %d/%d", writers, readers)
	}
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("newer-ledger serve retained its terminal flock: %v", err)
	}
	release()
}

// The advertised ephemeral DB flag must reach the ordinary broker Run path.
// Refusing it is an image-operation decision, not a cold CLI startup decision.
// origin: hot-update S6 plan §3.1; internal review rv-writer-12 and rv-fd-9
func TestServeColdSingleMemoryStoreReachesRun(t *testing.T) {
	gb, fb := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseServeMemoryStore(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ephemeral single serve", gb)
	testharness.AssertNoFDLeak(t, "ephemeral single serve", fb, 0)
}

func exerciseServeMemoryStore(t *testing.T) {
	t.Helper()
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--db=:memory:", "--nats-url=nats://127.0.0.1:1", "--admin-socket=", "--tunnel-addr=", "--store-dir", t.TempDir()})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "broker: NATS connect:") {
		t.Fatalf("ephemeral cold serve refused before Run: %v", err)
	}
}
