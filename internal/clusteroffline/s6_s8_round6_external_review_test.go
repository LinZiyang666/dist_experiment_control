package clusteroffline

import (
	"os"
	"path/filepath"
	"testing"
)

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
