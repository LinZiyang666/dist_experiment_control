package cluster

import (
	"context"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S10 internal review CLUSTER-WAL-FLAKE F1
// Architecture §3.7 requires a durable commit; §3.8/§13.5 require independent
// backup readers, not a 100 ms bound on Raft and the writer's own storage I/O.
// Delay the existing pre-commit seam often enough to reach the original p99.
func TestWALConcurrencyWithSlowDurableCommit(t *testing.T) {
	var slowCommits atomic.Int64
	previous := applyCommitGate
	defer func() { applyCommitGate = previous }()
	applyCommitGate = func(index uint64) {
		if index%32 == 0 {
			slowCommits.Add(1)
			// sleep-fixture: deterministic slow writer durability, not readiness.
			time.Sleep(150 * time.Millisecond)
		}
	}
	gBaseline, fdBaseline := runtime.NumGoroutine(), testharness.FDCount()
	n := mustNode(t, t.TempDir(), "w-slow")
	defer func() { _ = n.Shutdown() }()
	var latencies []time.Duration
	for round := 0; round < 5; round++ {
		latencies = append(latencies, exerciseWALConcurrency(t, n, round)...)
	}
	if got := slowCommits.Load(); got < 5 {
		t.Fatalf("slow durable commits = %d, want at least 5", got)
	}
	assertWALWriterAdmission(t, latencies)
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	testharness.AssertNoGoroutineLeak(t, "WAL with slow durable commits", gBaseline)
	testharness.AssertNoFDLeak(t, "WAL with slow durable commits", fdBaseline, 0)
}

// origin: S10 internal review CLUSTER-WAL-FLAKE F1
// A held first backup step keeps its real connection checked out until the
// writer commits. This distinguishes independent readers from accidental pool
// serialization without placing a wall-clock deadline on durable storage I/O.
func TestWALBackupKeepsWriterAvailable(t *testing.T) {
	gBaseline, fdBaseline := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWALWriterDuringBackup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "writer during held backup", gBaseline)
	testharness.AssertNoFDLeak(t, "writer during held backup", fdBaseline, 0)
}

func exerciseWALWriterDuringBackup(t *testing.T) {
	t.Helper()
	f, db := freshFSM(t, t.TempDir())
	defer func() { _ = f.ro.Close(); _ = db.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	held, release := make(chan struct{}), make(chan struct{})
	previous := backupStepGate
	defer func() { backupStepGate = previous }()
	backupStepGate = func() {
		close(held)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	dst := filepath.Join(f.tmpDir, "held-backup.db")
	backupDone := make(chan error, 1)
	go func() { backupDone <- backupTo(ctx, f.ro, dst) }()
	defer func() {
		close(release)
		if err := <-backupDone; err != nil {
			t.Errorf("held backup: %v", err)
		} else if err := verifyIntegrity(dst); err != nil {
			t.Errorf("completed held backup integrity: %v", err)
		}
	}()
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("backup did not reach its first step")
	}
	// These observations are stable while the backup is held, so a shared writer
	// pool fails deterministically before attempting any potentially slow fsync.
	if got := f.ro.Stats().InUse; got != 1 {
		t.Fatalf("backup reader connections in use = %d, want 1", got)
	}
	if got := db.Stats().InUse; got != 0 {
		t.Fatalf("held backup occupies %d writer connections, want 0", got)
	}
	admission, stopAdmission := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stopAdmission()
	conn, err := db.Conn(admission)
	if err != nil {
		t.Fatalf("writer pool blocked by held backup: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	// The read-only backup must permit the actual FULL writer commit as well as
	// pool admission. Keep the backup held until this call has returned.
	if got := f.Apply(logCmd(t, 1, 1, metaCmd(t, "t:while-backup-held", "v"))); got != (appliedOK{1}) {
		t.Fatalf("writer did not commit during held backup: %v", got)
	}
}
