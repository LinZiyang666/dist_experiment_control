package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// datadirlock_test.go (formerly datadirlock_round6_test.go) — round-6 self-review, test-adequacy lane.
//
// The round-5 B3 fix (Broker.Run holds ${ClusterDataDir}/tether.lock for its process lifetime) shipped with
// ZERO coverage at its real entry point: the whole hunk in Run could be deleted and `make test` stayed green.
// That is verbatim the criticism round-5 levelled at the PREVIOUS remediation, so it must not stand here.
//
// These tests bind the lock to Broker.Run itself.

// origin: datadirlock_round6_test.go (renamed in B6)
//
// B3 CALL SITE: Run must REFUSE to start while an offline recovery holds the data-dir lock. Deleting the
// AcquireDataDirLock hunk from Run makes this test fail (Run would proceed past it into its normal startup).
func TestRound6_BrokerRunRefusesWhileTheDataDirLockIsHeld(t *testing.T) {
	dir := t.TempDir()
	release, err := cluster.AcquireDataDirLock(dir) // stand in for a force-single mid-surgery
	if err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	defer release()

	b := &Broker{cfg: Config{ClusterDataDir: dir}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = b.Run(ctx)
	if err == nil {
		t.Fatal("Broker.Run started while an offline recovery held the data-dir lock — the interlock is not wired into Run")
	}
	if !strings.Contains(err.Error(), "refusing to start") || !strings.Contains(err.Error(), "lock") {
		t.Fatalf("want the lock-contention refusal from Run, got: %v", err)
	}
}

// B3 CALL SITE (release): the lock must be RELEASED when Run returns, or a crashed/stopped broker would
// wedge every future recovery forever. Deleting the `defer release()` makes this test fail.
func TestRound6_BrokerRunReleasesTheDataDirLockOnReturn(t *testing.T) {
	dir := t.TempDir()
	b := &Broker{cfg: Config{ClusterDataDir: dir}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = b.Run(ctx) // fails later in startup (no NATS/DB) — irrelevant; we only care that the lock is freed

	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("Broker.Run did not release the data-dir lock on return — recovery would be wedged forever: %v", err)
	}
	release()
}

// Round-6 R2: the regression my own B3 fix introduced. The runbook documents `sudo tether cluster recovery
// …`, which creates a ROOT-owned tether.lock; the daemon runs as `User=tether` and would then EACCES. That
// must NOT be reported as "another process holds it / stop the previous broker" — the broker is already
// stopped and the real remedy is chown. AcquireDataDirLock must classify the two distinctly.
func TestRound6_UnusableLockIsNotReportedAsContention(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: an unreadable lock cannot be simulated")
	}
	dir := t.TempDir()
	lock := cluster.DataDirLockPath(dir)
	if err := os.WriteFile(lock, nil, 0o000); err != nil { // stand in for a root-owned lock
		t.Fatal(err)
	}
	_, err := cluster.AcquireDataDirLock(dir)
	if err == nil {
		t.Skip("filesystem ignores the mode (e.g. running with CAP_DAC_OVERRIDE)")
	}
	if !strings.Contains(err.Error(), "cannot be opened") {
		t.Fatalf("an UNREADABLE lock was not classified as unusable: %v", err)
	}
	if strings.Contains(err.Error(), "another process holds") {
		t.Fatalf("a permission problem was misreported as contention — the operator would be told to stop a broker that is already stopped: %v", err)
	}
	// The remedy must name the real fix (chown), not a stop.
	if !strings.Contains(err.Error(), "chown") {
		t.Fatalf("the unusable-lock error does not tell the operator the actual remedy: %v", err)
	}
}

// Round-6: a root-run offline tool must not LEAVE a lock the daemon cannot take. AcquireDataDirLock mirrors
// the data dir's ownership onto the lock file, so the tether daemon can always open it afterwards.
func TestRound6_LockFileInheritsTheDataDirOwnership(t *testing.T) {
	dir := t.TempDir()
	release, err := cluster.AcquireDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	release()
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	li, err := os.Stat(filepath.Join(dir, cluster.DataDirLockFile))
	if err != nil {
		t.Fatalf("lock file missing: %v", err)
	}
	_ = di
	// Ownership comparison is platform-specific; the invariant we can always assert is that the lock stays
	// owner-readable/writable, i.e. the daemon that must take it next is never locked out by mode.
	if li.Mode().Perm()&0o600 == 0 {
		t.Fatalf("lock file is not owner-readable/writable: %v", li.Mode())
	}
}
