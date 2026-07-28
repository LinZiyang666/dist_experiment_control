package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

// origin: s6_s8_round7_external_review_test.go (renamed in B6) — docs/reviews/s6-s8-external-review.md
//
// The production data dir is writable by the tether service account while the
// runbook invokes offline recovery through sudo. The lock acquisition must not
// follow a service-account-controlled symlink: AcquireDataDirLock mirrors the
// data-dir owner with f.Chown, so following one as root can transfer ownership
// of an arbitrary system file to tether.
func TestS6S8Round7DataDirLockRefusesSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, DataDirLockPath(dir)); err != nil {
		t.Fatal(err)
	}

	release, err := AcquireDataDirLock(dir)
	if release != nil {
		release()
	}
	if err == nil {
		t.Fatal("AcquireDataDirLock followed a symlink; sudo recovery could chown its target to the data-dir owner")
	}
}
