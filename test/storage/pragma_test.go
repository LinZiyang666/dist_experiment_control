package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// B6 — foreign_keys is ON for every connection storage.Open hands out.
//
// We exercise BOTH the in-memory DSN and an on-disk DSN: the in-memory
// path goes through the special-case "promote :memory: to file::memory:"
// branch in withForeignKeysPragma; the on-disk path takes the
// regular branch. Both must end up with foreign_keys=1.
func TestB6_ForeignKeysOnAfterOpen(t *testing.T) {
	cases := map[string]string{
		"memory": ":memory:",
		"file":   "file:" + filepath.Join(t.TempDir(), "fk.db"),
	}
	for label, dsn := range cases {
		t.Run(label, func(t *testing.T) {
			db, err := storage.Open(dsn)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			var on int
			if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&on); err != nil {
				t.Fatalf("pragma: %v", err)
			}
			if on != 1 {
				t.Errorf("foreign_keys: got %d want 1", on)
			}
		})
	}
}

// B7 — busy_timeout is the configured 5000 ms after Open.
//
// The contract is encoded in the DSN (`_pragma=busy_timeout(5000)`) so
// modernc applies it on every sqlite3_open_v2 — whichever pooled conn
// answers PRAGMA busy_timeout must agree. 5s is load-bearing for
// concurrent writers (audit shard 03 F9) — if anyone bumps the DSN
// down to e.g. 1000, this test fails.
func TestB7_BusyTimeoutIsFiveSeconds(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var ms int
	if err := db.QueryRow(`PRAGMA busy_timeout`).Scan(&ms); err != nil {
		t.Fatalf("pragma busy_timeout: %v", err)
	}
	if ms != 5000 {
		t.Errorf("busy_timeout: got %d want 5000", ms)
	}
}

// B8 — MaxOpenConnections=1 is set by storage.Open. This is the
// "load-bearing" comment in storage.go; many writers (port.Allocate,
// session.Create, agentprov.Provision) execute compound read-then-write
// sequences under default-deferred transactions and rely on the pool
// serializing them. Verify the pool config so a future SetMaxOpenConns
// removal/relaxation gets caught here.
func TestB8_SingleOpenConnection(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections: got %d want 1", got)
	}
}

// B9 — journal_mode contract. modernc's ":memory:" path defaults to
// "memory"; the file path defaults to "delete" (we don't enable WAL).
// We pin the OBSERVED defaults so a future "let's turn on WAL" change
// is forced through this test (and then through code review of the
// recovery semantics WAL implies on crash).
func TestB9_JournalModeDefault(t *testing.T) {
	cases := map[string]struct {
		dsn  string
		want []string // any of these is acceptable
	}{
		"memory": {":memory:", []string{"memory"}},
		"file":   {"file:" + filepath.Join(t.TempDir(), "jm.db"), []string{"delete"}},
	}
	for label, c := range cases {
		t.Run(label, func(t *testing.T) {
			db, err := storage.Open(c.dsn)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			var mode string
			if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
				t.Fatalf("pragma journal_mode: %v", err)
			}
			ok := false
			for _, w := range c.want {
				if mode == w {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("journal_mode for %s: got %q want one of %v",
					label, mode, c.want)
			}
		})
	}
}

// B10 — Same PRAGMAs hold for the :memory: DSN, which is the one tests
// (and a hypothetical embedded admin tool) will use most. We assert
// foreign_keys, busy_timeout, AND MaxOpenConnections together — the
// "memory" DSN goes through the file::memory: rewrite, so it's the
// most likely to silently lose a query parameter.
func TestB10_MemoryDSNHasFullPragmaSet(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var fk, bt int
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`PRAGMA busy_timeout`).Scan(&bt); err != nil {
		t.Fatal(err)
	}
	if fk != 1 || bt != 5000 {
		t.Errorf(":memory: DSN PRAGMAs lost: foreign_keys=%d busy_timeout=%d", fk, bt)
	}
	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf(":memory: DSN MaxOpenConnections: got %d want 1", got)
	}
}
