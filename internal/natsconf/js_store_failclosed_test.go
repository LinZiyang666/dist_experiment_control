package natsconf

import (
	"os"
	"path/filepath"
	"testing"
)

// origin: r16_g67_g69_external_review_test.go (renamed in B6) — docs/reviews/r16-g67-g69-external-review.md
//
// TestExternalReviewJSStoreRootStatErrorFailsClosed pins the function's documented
// fail-closed contract at the root itself. A returning joiner's store path can be a
// symlink; ELOOP (or EACCES/I/O failure in production) must not be interpreted as
// "absent/empty", because that silently skips the grow-time stale-store reset.
func TestExternalReviewJSStoreRootStatErrorFailsClosed(t *testing.T) {
	store := filepath.Join(t.TempDir(), "jetstream")
	if err := os.Symlink(store, store); err != nil {
		t.Fatal(err)
	}
	// Signature adapted to the (hasData, err) API this review asked for; the assertion is unchanged.
	hasData, _ := JSStoreHasData(store)
	if !hasData {
		t.Fatal("an unstatable JetStream store must fail closed as potentially data-bearing; false means the joiner reset is silently skipped")
	}
}

func TestExternalReviewJSStoreSymlinkDoesNotHideData(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real-store")
	store := filepath.Join(root, "jetstream")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "meta.db"), []byte("clustered metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, store); err != nil {
		t.Fatal(err)
	}
	hasData, err := JSStoreHasData(store)
	if err != nil {
		t.Fatalf("a resolvable symlinked store must not error: %v", err)
	}
	if !hasData {
		t.Fatal("a symlinked JetStream store hid data in its target; WalkDir must inspect the directory that os.Stat accepted")
	}
}

// TestExternalReviewStaleSentinelCannotDisarmADataBearingReset models a
// successful move whose backup was archived by the operator, followed by new
// writes into the recreated store before the operation completed. The sentinel
// proves only that an earlier move happened; it cannot prove that the current
// store is still empty. Treating it as an unconditional no-op boots stale/live
// JS state through the very cutover the reset is meant to protect.
func TestExternalReviewStaleSentinelCannotDisarmADataBearingReset(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	backup := filepath.Join(root, "jetstream.grow-bak.op")
	sentinel := filepath.Join(root, ".grow-reset.done")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "new-meta"), []byte("live clustered state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("an earlier move happened"), 0o600); err != nil {
		t.Fatal(err)
	}

	moved, err := MoveAsideJSStore(store, backup, sentinel, true)
	if err != nil {
		t.Fatal(err)
	}
	if moved == "" {
		t.Fatal("a stale sentinel suppressed reset of a currently data-bearing store")
	}
}
