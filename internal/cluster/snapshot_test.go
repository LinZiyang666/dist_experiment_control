package cluster

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// TestSnapshotRestore_RoundTripAndInvariant is §13.3: apply ops, snapshot, assert
// for an all-command tail that snapshot.Index == applied_index (the benign
// barrier gap is in TestSnapshotAfterBarrier; the real "no committed op lost"
// guarantee in TestSnapshotThenRestart), restore into a fresh FSM, and assert
// logical-content equality (per-table hash, NOT byte compare).
func TestSnapshotRestore_RoundTripAndInvariant(t *testing.T) {
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "a")
	for i := 0; i < 20; i++ {
		if err := a.ApplyMetaSet(fmt.Sprintf("t:k%02d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	srcHash := clusterMetaHash(t, a.db)
	appliedA, err := a.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}

	meta, rc := openSnapshot(t, srcDir)
	// For an all-command snapshot tail (no Barrier/config entry between the last
	// command and the snapshot) snapshot.Index EQUALS the durable apply cursor. The
	// BENIGN gap (snapshot.Index > applied_index after a Barrier) is covered by
	// TestSnapshotAfterBarrier; the real survival guarantee — no committed op lost
	// across snapshot+restart — by TestSnapshotThenRestart (the old "snapshot.Index
	// <= applied_index always" invariant was wrong; see §3.7 D1 amendment).
	if uint64(meta.Index) != appliedA {
		t.Fatalf("all-command snapshot: snapshot.Index=%d, want == applied_index=%d", meta.Index, appliedA)
	}
	snapBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(snapBytes) == 0 {
		t.Fatal("snapshot is empty")
	}

	// Restore into a brand-new empty FSM and compare logical content.
	tf, tdb := freshFSM(t, t.TempDir())
	if err := tf.Restore(io.NopCloser(bytes.NewReader(snapBytes))); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := clusterMetaHash(t, tdb); got != srcHash {
		t.Fatalf("restored content mismatch:\n src=%s\n got=%s", srcHash, got)
	}
	// Snapshot-file-size guard against the LOGICAL db size (page_count*page_size),
	// NOT the WAL-truncated 4KB main file — catches a Step-truncated backup
	// masquerading as complete.
	srcSize := logicalDBSize(t, a.db)
	if int64(len(snapBytes)) < srcSize {
		t.Fatalf("snapshot size %d < logical db size %d (truncated backup?)", len(snapBytes), srcSize)
	}
}

// TestSnapshot_TornRejected is the §13.3 load-bearing-region corruption gate with
// a positive control (uncorrupted restores) and a negative control (trailing
// garbage beyond the logical DB is NOT rejected — proving the check is not a
// paranoid always-reject stub).
func TestSnapshot_TornRejected(t *testing.T) {
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "a")
	for i := 0; i < 8; i++ {
		if err := a.ApplyMetaSet(fmt.Sprintf("t:k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Snapshot(); err != nil {
		t.Fatal(err)
	}
	goodHash := clusterMetaHash(t, a.db)
	_, rc := openSnapshot(t, srcDir)
	good, _ := io.ReadAll(rc)
	_ = rc.Close()
	if len(good) < 100 {
		t.Fatalf("snapshot suspiciously small: %d bytes", len(good))
	}

	// Positive control: the uncorrupted snapshot restores cleanly AND to the
	// correct logical content (not just err==nil).
	tf, tdb := freshFSM(t, t.TempDir())
	if err := tf.Restore(io.NopCloser(bytes.NewReader(good))); err != nil {
		t.Fatalf("positive control: uncorrupted snapshot must restore: %v", err)
	}
	if got := clusterMetaHash(t, tdb); got != goodHash {
		t.Fatalf("positive control: restored content mismatch: src=%s got=%s", goodHash, got)
	}

	// Torn variants targeting load-bearing regions — each MUST be rejected, and
	// the rejection must leave a fresh target's live DB unmodified.
	torn := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"header_magic", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			copy(c[0:16], []byte("NOT-SQLITE-FILE\x00"))
			return c
		}},
		{"page1_btree", func(b []byte) []byte {
			c := append([]byte(nil), b...)
			for i := 100; i < 130 && i < len(c); i++ {
				c[i] ^= 0xFF
			}
			return c
		}},
		{"header_pagesize", func(b []byte) []byte { c := append([]byte(nil), b...); c[16], c[17] = 0x00, 0x07; return c }}, // page-size field -> invalid (not a power of two)
		{"truncated_half", func(b []byte) []byte { return append([]byte(nil), b[:len(b)/2]...) }},
	}
	for _, tc := range torn {
		t.Run(tc.name, func(t *testing.T) {
			ttf, ttdb := freshFSM(t, t.TempDir())
			before := clusterMetaHash(t, ttdb)
			err := ttf.Restore(io.NopCloser(bytes.NewReader(tc.mutate(good))))
			if err == nil {
				t.Fatalf("torn snapshot %q must be REJECTED by integrity_check", tc.name)
			}
			if after := clusterMetaHash(t, ttdb); after != before {
				t.Fatalf("torn snapshot %q must leave the live DB unchanged: before=%s after=%s", tc.name, before, after)
			}
		})
	}

	// Negative control: garbage appended BEYOND the logical DB size is ignored by
	// integrity_check (SQLite reads page_count from the header), so it must NOT be
	// rejected — proving the gate isn't "any byte changed => reject".
	t.Run("trailing_garbage_not_rejected", func(t *testing.T) {
		ntf, ntdb := freshFSM(t, t.TempDir())
		padded := append(append([]byte(nil), good...), bytes.Repeat([]byte{0xAB}, 4096)...)
		if err := ntf.Restore(io.NopCloser(bytes.NewReader(padded))); err != nil {
			t.Fatalf("trailing-garbage control must NOT be rejected (paranoid check?): %v", err)
		}
		// ...and it must restore the SAME logical content (full discriminating loop).
		if got := clusterMetaHash(t, ntdb); got != goodHash {
			t.Fatalf("trailing-garbage control restored wrong content: src=%s got=%s", goodHash, got)
		}
	})
}

// TestSnapshot_FKViolationRejected exercises verifyIntegrity's SECOND rejection
// arm (foreign_key_check) — distinct from the integrity_check arm the torn tests
// trip. A structurally-valid DB carrying an orphaned FK row (a node referencing a
// non-existent session, inserted with FK enforcement off) passes integrity_check
// but must be rejected by foreign_key_check, leaving the live target unchanged.
func TestSnapshot_FKViolationRejected(t *testing.T) {
	dir := t.TempDir()
	orphanPath := filepath.Join(dir, "orphan.db")
	// Build a non-WAL (self-contained file) migrated DB and insert an orphan with
	// FK enforcement disabled on a pinned connection.
	odb, err := storage.Open("file:" + orphanPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := odb.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(),
		`INSERT INTO nodes(sid, nid, status) VALUES('ghost-sid','n1','ONLINE')`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	_ = odb.Close()

	good, err := os.ReadFile(orphanPath)
	if err != nil {
		t.Fatal(err)
	}

	tf, tdb := freshFSM(t, t.TempDir())
	before := clusterMetaHash(t, tdb)
	err = tf.Restore(io.NopCloser(bytes.NewReader(good)))
	if err == nil {
		t.Fatal("a snapshot with an orphaned FK row must be REJECTED (foreign_key_check arm)")
	}
	if !strings.Contains(err.Error(), "foreign-key") {
		t.Fatalf("rejection must come from the foreign_key_check arm, got: %v", err)
	}
	if after := clusterMetaHash(t, tdb); after != before {
		t.Fatal("an FK-rejected restore must leave the live DB unchanged")
	}
}

// TestApply_SynchronousCommit proves node.Apply returns only AFTER the SQLite
// commit is durable: a SEPARATE read-only connection sees the new applied_index
// immediately. A deferred-commit FSM (commit after Apply returns) would fail this
// because the independent connection could not see an uncommitted write.
func TestApply_SynchronousCommit(t *testing.T) {
	dir := t.TempDir()
	n := mustNode(t, dir, "a")
	for i := 0; i < 5; i++ {
		if err := n.ApplyMetaSet(fmt.Sprintf("t:s%d", i), "v"); err != nil {
			t.Fatal(err)
		}
		want, err := n.AppliedIndex()
		if err != nil {
			t.Fatal(err)
		}
		// Independent connection — only a committed write is visible here.
		ro, err := storage.OpenReadOnly("file:" + n.fsm.dbPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := readAppliedIndexDB(ro)
		_ = ro.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("independent conn sees applied_index=%d, node reports %d — Apply returned before commit (async gap)", got, want)
		}
	}
}

// logicalDBSize returns page_count*page_size — the true logical size of the DB
// (incl. pages still resident in the WAL), unlike the 4KB checkpointed main file.
func logicalDBSize(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var pageCount, pageSize int64
	if err := db.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		t.Fatalf("page_count: %v", err)
	}
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatalf("page_size: %v", err)
	}
	return pageCount * pageSize
}
