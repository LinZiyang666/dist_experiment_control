package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// TestWALConcurrency is §13.5: continuous Apply (single writer) + streaming
// online-backup (the dedicated RO handle) interleave without the WRITER ever
// seeing SQLITE_BUSY, each completed backup is integrity-clean, and no goroutine
// or fd leaks. Leak gate = the in-tree runtime.NumGoroutine + fd baseline (the
// project deliberately avoids goleak — see test/concurrency/helpers_test.go).
func TestWALConcurrency(t *testing.T) {
	dir := t.TempDir()
	n := mustNode(t, dir, "w")
	dbPath := n.fsm.dbPath

	// WAL + FK are actually in effect on the write pool.
	if jm := pragmaStr(t, n.db, "journal_mode"); jm != "wal" {
		t.Fatalf("journal_mode = %q, want wal", jm)
	}
	if fk := pragmaStr(t, n.db, "foreign_keys"); fk != "1" {
		t.Fatalf("foreign_keys = %q, want 1 (WAL must not disable FK)", fk)
	}
	// synchronous=FULL is the durability foundation of the §3.7 invariant (WAL's
	// default NORMAL can lose the last committed frame on power loss).
	if sy := pragmaStr(t, n.db, "synchronous"); sy != "2" {
		t.Fatalf("synchronous = %q, want 2 (FULL) — the §3.7 applied_index durability basis", sy)
	}
	if mc := n.db.Stats().MaxOpenConnections; mc != 1 {
		t.Fatalf("write pool MaxOpenConnections = %d, want 1 (load-bearing single writer)", mc)
	}

	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	// The RO handle must be truly read-only — a write through it FAILS (no second
	// writer can slip in).
	if _, err := ro.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:rofail','x')`); err == nil {
		t.Fatal("a write through the read-only handle must FAIL")
	}

	const N = 300
	gBaseline := runtime.NumGoroutine()
	fdBaseline := fdCount()

	var (
		wg               sync.WaitGroup
		backuping        atomic.Bool
		appliesDuringBkp atomic.Int64
		completedBackups atomic.Int64
		latencies        = make([]time.Duration, 0, N)
		latMu            sync.Mutex
		writerErr        atomic.Pointer[error]
	)
	stop := make(chan struct{})

	// Writer: tight single-writer Apply loop. A SQLITE_BUSY would surface as an
	// Apply error here — which must never happen.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < N; i++ {
			start := time.Now()
			if err := n.ApplyMetaSet(fmt.Sprintf("t:w%d", i), "v"); err != nil {
				e := fmt.Errorf("apply %d: %w", i, err)
				writerErr.Store(&e)
				return
			}
			d := time.Since(start)
			latMu.Lock()
			latencies = append(latencies, d)
			latMu.Unlock()
			if backuping.Load() {
				appliesDuringBkp.Add(1)
			}
		}
	}()

	// Backup loop: stream the RO handle into a temp file while the writer runs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tmp := filepath.Join(dir, "wal-backup.db")
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(tmp)
			backuping.Store(true)
			err := backupTo(context.Background(), ro, tmp)
			backuping.Store(false)
			if err != nil {
				// A Step SQLITE_BUSY under a concurrent WAL checkpoint is tolerated
				// and retried; only COMPLETED backups are asserted integrity-clean.
				continue
			}
			if ierr := verifyIntegrity(tmp); ierr != nil {
				t.Errorf("a completed backup is not integrity-clean: %v", ierr)
				return
			}
			completedBackups.Add(1)
		}
	}()

	wg.Wait()
	_ = os.Remove(filepath.Join(dir, "wal-backup.db"))

	if ep := writerErr.Load(); ep != nil {
		t.Fatalf("writer saw an error (SQLITE_BUSY / serialization?): %v", *ep)
	}
	if completedBackups.Load() == 0 {
		t.Fatal("no backup completed — the streaming-backup path never succeeded")
	}
	// Interleave proof: a meaningful fraction of applies committed while a backup
	// Step was outstanding, so "zero BUSY" is non-vacuous.
	if got := appliesDuringBkp.Load(); got < N/4 {
		t.Fatalf("only %d/%d applies overlapped a backup — interleave too low, zero-BUSY is vacuous", got, N)
	}
	// Concrete latency ceiling: a multi-second stall = silent serialization
	// absorbed by busy_timeout = fail.
	if p99 := percentile(latencies, 0.99); p99 > 100*time.Millisecond {
		t.Fatalf("p99 Apply latency = %v, want < 100ms (silent serialization?)", p99)
	}

	// Leak gate (in-tree convention, NOT goleak): goroutines return to baseline and
	// no fd leak (a missing NewBackup Finish would leak an fd NumGoroutine misses).
	waitGoroutineBaseline(t, gBaseline+2, 5*time.Second)
	if fdBaseline >= 0 {
		if leaked := fdCount() - fdBaseline; leaked > 4 {
			t.Fatalf("fd leak: %d fds above baseline after backups (missing NewBackup Finish?)", leaked)
		}
	}
}

func pragmaStr(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var v string
	if err := db.QueryRow("PRAGMA " + name).Scan(&v); err != nil {
		t.Fatalf("pragma %s: %v", name, err)
	}
	return v
}

// ---- small test utilities ----

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), ds...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(float64(len(cp)-1) * p)
	return cp[idx]
}

// fdCount returns the number of open fds for this process, or -1 if unavailable
// (non-Linux). Linux exposes them under /proc/self/fd.
func fdCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func waitGoroutineBaseline(t *testing.T, ceiling int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= ceiling {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutines did not return to baseline (%d): now %d", ceiling, runtime.NumGoroutine())
}
