package natsconf_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/natsconf"
)

// seedStore creates storeDir with `files` entries (empty dir if none).
func seedStore(t *testing.T, storeDir string, files ...string) {
	t.Helper()
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(storeDir, f), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// An EMPTY store dir IS moved by the shared helper (the former-N1 cutover still needs the fresh-store
// guarantee); the JOINER-specific "structural-only ⇒ no-op" lives one layer up in JSStoreHasData (below).
func TestMoveAsideJSStore_EmptyStoreMoves(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	seedStore(t, store) // empty
	backup := store + ".grow-bak.e1"
	sentinel := filepath.Join(root, ".reset-e1.done")

	got, err := natsconf.MoveAsideJSStore(store, backup, sentinel, false /*ack*/)
	if err != nil {
		t.Fatalf("empty store must move without an ack (no data to lose): %v", err)
	}
	if got != backup {
		t.Fatalf("empty store should have moved to %q, got %q", backup, got)
	}
	// The store dir is recreated empty; the backup holds the (empty) original.
	if fi, e := os.Stat(store); e != nil || !fi.IsDir() {
		t.Fatalf("store dir must be recreated empty after the move")
	}
}

// TestJSStoreHasData pins the R16 M3 data-bearing discriminator that keeps the grow JOINER reset a no-op on
// a truly-fresh joiner (absent / empty / structural-only) while resetting a returning node's stale store.
func TestJSStoreHasData(t *testing.T) {
	if hasData, _ := natsconf.JSStoreHasData(""); hasData {
		t.Fatal("empty path ⇒ no data")
	}
	if hasData, _ := natsconf.JSStoreHasData(filepath.Join(t.TempDir(), "nope")); hasData {
		t.Fatal("absent store ⇒ no data")
	}
	empty := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	if hasData, _ := natsconf.JSStoreHasData(empty); hasData {
		t.Fatal("empty store dir ⇒ no data")
	}
	// Structural skeleton a booted JS nats lays down: empty subdirs + a 0-byte marker → still no data.
	structural := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(filepath.Join(structural, "$G", "streams"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(structural, "$SYS"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(structural, "marker"), nil /*0 bytes*/, 0o600); err != nil {
		t.Fatal(err)
	}
	if hasData, _ := natsconf.JSStoreHasData(structural); hasData {
		t.Fatal("structural-only skeleton (empty dirs + 0-byte marker) ⇒ no data (a fresh joiner)")
	}
	// A real stream message block (>0 bytes under streams/) ⇒ data-bearing.
	dataBearing := filepath.Join(t.TempDir(), "jetstream")
	if err := os.MkdirAll(filepath.Join(dataBearing, "$G", "streams", "history-lab"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataBearing, "$G", "streams", "history-lab", "1.blk"), []byte("msg"), 0o600); err != nil {
		t.Fatal(err)
	}
	if hasData, _ := natsconf.JSStoreHasData(dataBearing); !hasData {
		t.Fatal("a stream with a non-empty message block ⇒ data-bearing (a returning node's stale store)")
	}
}

func TestMoveAsideJSStore_NonEmptyRefusesWithoutAck(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	seedStore(t, store, "history-lab", "events")
	backup := store + ".grow-bak.e1"

	got, err := natsconf.MoveAsideJSStore(store, backup, "", false /*ack*/)
	if err == nil {
		t.Fatal("a NON-EMPTY (data-bearing) store MUST refuse without ack (no silent data loss)")
	}
	if got != "" {
		t.Fatalf("a refusal must return no backup path, got %q", got)
	}
	if !strings.Contains(err.Error(), "NON-EMPTY") || !strings.Contains(err.Error(), backup) {
		t.Fatalf("refusal must name the non-empty store + the move-aside destination, got: %v", err)
	}
	// The store must be UNTOUCHED by the refusal.
	if _, e := os.Stat(filepath.Join(store, "history-lab")); e != nil {
		t.Fatalf("a refused reset must not touch the data-bearing store: %v", e)
	}
	if _, e := os.Stat(backup); e == nil {
		t.Fatal("a refused reset must not create the backup dir")
	}
}

func TestMoveAsideJSStore_NonEmptyMovesWithAck(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	seedStore(t, store, "history-lab")
	backup := store + ".grow-bak.e1"
	sentinel := filepath.Join(root, ".reset-e1.done")

	got, err := natsconf.MoveAsideJSStore(store, backup, sentinel, true /*ack*/)
	if err != nil {
		t.Fatalf("with ack the non-empty store must move: %v", err)
	}
	if got != backup {
		t.Fatalf("backup path = %q, want %q", got, backup)
	}
	if _, e := os.Stat(filepath.Join(backup, "history-lab")); e != nil {
		t.Fatalf("the data must live in the backup dir after the move: %v", e)
	}
	if _, e := os.Stat(sentinel); e != nil {
		t.Fatalf("the advisory sentinel should be written: %v", e)
	}
}

func TestMoveAsideJSStore_BackupDirFirstIdempotent(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	seedStore(t, store, "history-lab")
	backup := store + ".grow-bak.e1"
	sentinel := filepath.Join(root, ".reset-e1.done")

	if _, err := natsconf.MoveAsideJSStore(store, backup, sentinel, true); err != nil {
		t.Fatalf("first move: %v", err)
	}
	// A fresh clustered store now sits at storeDir; a SECOND call for the same epoch must be a no-op driven
	// by the DURABLE backup dir (M3) — NOT a second rename that would ENOTEMPTY over the data-bearing backup
	// or move the fresh clustered store aside. ack=false proves it does not even reach the emptiness gate.
	if err := os.WriteFile(filepath.Join(store, "fresh-clustered-meta"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := natsconf.MoveAsideJSStore(store, backup, sentinel, false)
	if err != nil {
		t.Fatalf("second call must be an idempotent no-op (backup dir present), got: %v", err)
	}
	if got != backup {
		t.Fatalf("idempotent no-op should still report the backup path %q, got %q", backup, got)
	}
	if _, e := os.Stat(filepath.Join(store, "fresh-clustered-meta")); e != nil {
		t.Fatal("the fresh clustered store must NOT be moved aside on a resume (backup-dir-first idempotency)")
	}
}

func TestMoveAsideJSStore_SentinelPresentBackupGoneIsNoOp(t *testing.T) {
	root := t.TempDir()
	store := filepath.Join(root, "jetstream")
	// EXTERNAL REVIEW F5: this fixture used to seed DATA here and assert a no-op — i.e. it pinned the
	// unsafe behaviour, where a sentinel whose backup is gone exempted a data-bearing store forever. The
	// safe contract is: the sentinel may only excuse an EMPTY store. The data-bearing half is asserted
	// below and by the reviewer's own TestExternalReviewStaleSentinelCannotDisarmADataBearingReset.
	if err := os.MkdirAll(store, 0o700); err != nil {
		t.Fatal(err)
	}
	backup := store + ".grow-bak.e1"
	sentinel := filepath.Join(root, ".reset-e1.done")
	// Post-move state where the operator has since removed the backup: sentinel present, backup GONE.
	if err := os.WriteFile(sentinel, []byte("backup="+backup+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := natsconf.MoveAsideJSStore(store, backup, sentinel, false)
	if err != nil {
		t.Fatalf("sentinel-present/backup-gone must be a no-op, got: %v", err)
	}
	if got != "" {
		t.Fatalf("no backup dir exists, so the restore hint must be empty, got %q", got)
	}
	if _, e := os.Stat(store); e != nil {
		t.Fatal("the empty store dir must be left in place when the move already happened")
	}

	// The other half of the contract (external review F5): once that same store becomes DATA-BEARING
	// again, the artifact-less sentinel must NOT excuse it — otherwise a store the operator repopulated
	// (or nats-server rewrote) is exempted from reset permanently.
	if err := os.WriteFile(filepath.Join(store, "meta.db"), []byte("live data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := natsconf.MoveAsideJSStore(store, backup, sentinel, false); err == nil {
		t.Fatal("a stale sentinel must not disarm the reset of a store that is data-bearing NOW — the " +
			"sentinel proves only that SOME earlier move happened, never that this store is still empty")
	}
}

// TestMoveAsideJSStore_EACCESLoudChownHint pins the R16 A0 NEW behavior: a rename that fails EACCES (a
// root-owned data dir after a root-run restore/install leaves the User=tether daemon unable to move its
// own store) fails LOUD with the broker-ops #6 chown remedy — never skip-and-proceed.
func TestMoveAsideJSStore_EACCESLoudChownHint(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("perm semantics are POSIX-specific")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks; cannot provoke EACCES")
	}
	root := t.TempDir()
	// The store lives under a PARENT dir we make un-writable, so the rename OUT of it is denied.
	parent := filepath.Join(root, "locked")
	store := filepath.Join(parent, "jetstream")
	seedStore(t, store) // empty (so the emptiness gate passes and we reach the rename)
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })

	backup := store + ".grow-bak.e1"
	_, err := natsconf.MoveAsideJSStore(store, backup, "", false)
	if err == nil {
		t.Skip("environment allowed the rename despite a 0500 parent (some CI overlay FS ignore dir perms) — cannot assert EACCES here")
	}
	if !strings.Contains(err.Error(), "chown") || !strings.Contains(err.Error(), "broker-ops #6") {
		t.Fatalf("an EACCES rename must fail LOUD with the chown/broker-ops #6 hint, got: %v", err)
	}
}
