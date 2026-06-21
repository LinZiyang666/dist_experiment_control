package storage

import (
	"path/filepath"
	"testing"
)

// TestOpen_DefaultPathDoesNotEnableWAL is an external-review guard for the D1
// scope boundary: the existing P0-P13 storage.Open path must keep its historical
// rollback-journal behavior. WAL is opt-in through OpenWAL for the cluster FSM
// only; flipping Open would change the live broker DB's journaling semantics.
func TestOpen_DefaultPathDoesNotEnableWAL(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "default.db")
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&got); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if got == "wal" {
		t.Fatal("storage.Open unexpectedly enabled WAL; D1 must keep WAL scoped to OpenWAL")
	}
}

// TestOpenWAL_PragmasInEffect pins the cluster FSM DB's durability/concurrency
// pragmas (architecture §3.8 D1 amendment): WAL journaling, synchronous=FULL,
// foreign keys on, and the load-bearing single-writer pool.
func TestOpenWAL_PragmasInEffect(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "wal.db")
	db, err := OpenWAL(dsn)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, tc := range []struct{ pragma, want string }{
		{"journal_mode", "wal"},
		{"synchronous", "2"}, // FULL
		{"foreign_keys", "1"},
	} {
		var got string
		if err := db.QueryRow("PRAGMA " + tc.pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", tc.pragma, err)
		}
		if got != tc.want {
			t.Errorf("PRAGMA %s = %q, want %q", tc.pragma, got, tc.want)
		}
	}
	if mc := db.Stats().MaxOpenConnections; mc != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1 (single-writer pool preserved)", mc)
	}
}

// TestOpenReadOnly_RejectsWrites confirms the dedicated backup-source handle is
// OS-level read-only: reads succeed, writes fail (no second writer can slip in).
func TestOpenReadOnly_RejectsWrites(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "ro.db")
	w, err := OpenWAL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:ro','1')`); err != nil {
		t.Fatal(err)
	}

	ro, err := OpenReadOnly(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	var v string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:ro'`).Scan(&v); err != nil || v != "1" {
		t.Fatalf("read-only read = (%q,%v), want (\"1\",nil)", v, err)
	}
	if _, err := ro.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:bad','x')`); err == nil {
		t.Fatal("a write through OpenReadOnly must FAIL")
	}
}
