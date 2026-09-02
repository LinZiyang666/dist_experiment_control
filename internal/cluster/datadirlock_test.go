package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// datadirlock_test.go (formerly datadirlock_round7_test.go) — round-7 external review: AcquireDataDirLock followed a tether.lock symlink
// and then f.Chown'd the TARGET as root, letting the unprivileged tether account take ownership of any file
// via the runbook's `sudo … recovery` (local privilege escalation). These pin the boundary beyond the
// reviewer's own regression (which covers the symlink case).

// A DANGLING symlink is the nastier variant: O_CREATE would happily create the TARGET (root-owned, at an
// attacker-chosen path) and then chown it.
func TestRound7_DataDirLockRefusesADanglingSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "does-not-exist-yet")
	if err := os.Symlink(target, DataDirLockPath(dir)); err != nil {
		t.Fatal(err)
	}
	release, err := AcquireDataDirLock(dir)
	if release != nil {
		release()
	}
	if err == nil {
		t.Fatal("AcquireDataDirLock followed a DANGLING symlink — sudo recovery would CREATE and chown an attacker-chosen path")
	}
	if _, serr := os.Lstat(target); serr == nil {
		t.Fatal("the dangling symlink's target was created — the lock open followed the link")
	}
	if !strings.Contains(err.Error(), "SYMLINK") {
		t.Fatalf("want an explicit symlink refusal, got: %v", err)
	}
}

// O_NOFOLLOW rules out symlinks but not a FIFO/device planted at the path — which we would otherwise chown
// as root (and open() on a FIFO can even block).
func TestRound7_DataDirLockRefusesANonRegularFile(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(DataDirLockPath(dir), 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		release, err := AcquireDataDirLock(dir)
		if release != nil {
			release()
		}
		if err == nil {
			t.Error("AcquireDataDirLock accepted a FIFO as the lock file — it would be chowned as root")
			return
		}
		if !strings.Contains(err.Error(), "not a regular file") {
			t.Errorf("want a non-regular-file refusal, got: %v", err)
		}
	}()
	<-done
}

// The happy path must still work: a normal data dir yields a usable, re-acquirable lock.
func TestRound7_DataDirLockStillWorksOnACleanDir(t *testing.T) {
	dir := t.TempDir()
	release, err := AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("clean acquire failed — the hardening broke the normal path: %v", err)
	}
	if _, err := AcquireDataDirLock(dir); err == nil {
		t.Fatal("exclusivity lost")
	}
	release()
	r2, err := AcquireDataDirLock(dir)
	if err != nil {
		t.Fatalf("re-acquire after release failed: %v", err)
	}
	r2()
}
