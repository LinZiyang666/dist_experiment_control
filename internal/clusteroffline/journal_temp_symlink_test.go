package clusteroffline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// origin: s6_s8_round6_external_review_test.go (renamed in B6) — docs/reviews/s6-s8-external-review.md
//
// TestS6S8Round6JournalTempRefusesSymlink pins the privilege boundary of the offline CLI. The broker
// data dir is intentionally writable by the unprivileged tether service account, while the recovery
// runbook also documents `sudo tether cluster recovery force-single`. A fixed, symlink-following temp
// path would therefore let a compromised tether account redirect root's journal write onto any file.
func TestS6S8Round6JournalTempRefusesSymlink(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.Mkdir(dataDir, 0o750); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(root, "root-owned-victim")
	const sentinel = "DO-NOT-TOUCH"
	if err := os.WriteFile(victim, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, journalPath(dataDir)+".tmp"); err != nil {
		t.Fatal(err)
	}

	// The PROPERTY under test is "root's journal write must never clobber an arbitrary file". The fix uses
	// an unpredictable O_EXCL temp name (os.CreateTemp), so a symlink pre-planted at the OLD fixed path is
	// simply irrelevant and the write legitimately SUCCEEDS. Asserting "writeJournal must error" would pin an
	// implementation choice, not the boundary; assert the boundary.
	err := writeJournal(dataDir, &forceSingleJournal{SelfID: "brk1", Phase: phaseStarted})
	got, readErr := os.ReadFile(victim)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != sentinel {
		t.Fatalf("journal temp FOLLOWED an attacker-controlled symlink and clobbered the victim: %q", got)
	}
	if err != nil {
		t.Fatalf("writeJournal failed despite the symlink being irrelevant to it: %v", err)
	}
	if id, _, _ := InterruptedForceSingle(dataDir); id != "brk1" {
		t.Fatalf("journal did not land: id=%q", id)
	}
}

// origin: prerelease audit round 2, CC-2 (the missing coverage) and the §3 MINOR sweep
// (the fix).
//
// THE RESTORE STAGER IS THE SAME PRIVILEGE BOUNDARY THIS FILE ALREADY GUARDS.
//
// `sudo tether cluster recovery restore` runs as ROOT, and both of copyFileSync's paths
// live in the data dir that the unprivileged tether service account can write. Without
// O_NOFOLLOW a symlink planted at either path makes root read through it, or O_TRUNC and
// rewrite whatever it points at — /etc/shadow, a unit file, another service's database.
//
// The test above sealed exactly this primitive at exactly this directory for the journal
// in round 6, and the restore stager was left open until the §3 sweep — which then
// shipped the fix with no test, so it could be reverted silently.
func TestRestoreStagerRefusesToReadOrWriteThroughASymlink(t *testing.T) {
	t.Run("source is a symlink", func(t *testing.T) {
		dir := t.TempDir()
		real := filepath.Join(dir, "secret")
		if err := os.WriteFile(real, []byte("root-only content"), 0o600); err != nil {
			t.Fatal(err)
		}
		src := filepath.Join(dir, "snapshot.db")
		if err := os.Symlink(real, src); err != nil {
			t.Fatal(err)
		}
		err := copyFileSync(src, filepath.Join(dir, "out.db"))
		if err == nil {
			t.Fatal("copyFileSync READ THROUGH a symlinked source.\n\n" +
				"Running as root, that hands the caller the contents of whatever the tether " +
				"account pointed it at.")
		}
		if !strings.Contains(err.Error(), "SYMLINK") {
			t.Errorf("the refusal (%v) does not say the path is a symlink — an ELOOP on a path "+
				"the operator did not think was a link is a puzzle, not an error message", err)
		}
	})

	t.Run("destination is a symlink", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "snapshot.db")
		if err := os.WriteFile(src, []byte("staged bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		victim := filepath.Join(dir, "victim")
		if err := os.WriteFile(victim, []byte("do not clobber me"), 0o600); err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(dir, "stage.db")
		if err := os.Symlink(victim, dst); err != nil {
			t.Fatal(err)
		}
		if err := copyFileSync(src, dst); err == nil {
			t.Fatal("copyFileSync WROTE THROUGH a symlinked destination.\n\n" +
				"Running as root that is an arbitrary-file overwrite, which is the whole reason " +
				"this primitive carries O_NOFOLLOW on BOTH ends.")
		}
		body, rerr := os.ReadFile(victim)
		if rerr != nil {
			t.Fatal(rerr)
		}
		if string(body) != "do not clobber me" {
			t.Fatalf("the symlink target was overwritten: %q", body)
		}
	})

	// POSITIVE CONTROL: an ordinary copy still works, or the two refusals above are
	// satisfied by a function that refuses everything.
	t.Run("an ordinary copy still succeeds", func(t *testing.T) {
		dir := t.TempDir()
		src := filepath.Join(dir, "snapshot.db")
		if err := os.WriteFile(src, []byte("staged bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(dir, "stage.db")
		if err := copyFileSync(src, dst); err != nil {
			t.Fatalf("an ordinary copy was refused: %v", err)
		}
		body, err := os.ReadFile(dst)
		if err != nil || string(body) != "staged bytes" {
			t.Fatalf("copy produced %q (err=%v)", body, err)
		}
	})
}
