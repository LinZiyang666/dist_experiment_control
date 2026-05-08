package storage

import (
	"database/sql"
	"errors"
	"testing"
)

// openTest opens an in-memory SQLite, runs migrations, and closes via t.Cleanup.
func openTest(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestOpenAppliesMigrations(t *testing.T) {
	db := openTest(t)

	want := []string{"sessions", "members", "nodes", "processes", "port_allocations", migrationsTable}
	for _, tbl := range want {
		var got string
		err := db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl,
		).Scan(&got)
		if err != nil {
			t.Errorf("table %q missing after migrations: %v", tbl, err)
			continue
		}
		if got != tbl {
			t.Errorf("table lookup for %q returned %q", tbl, got)
		}
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	db := openTest(t)

	// Re-running applyMigrations on the same DB must be a no-op.
	if err := applyMigrations(db); err != nil {
		t.Fatalf("second applyMigrations: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + migrationsTable).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 migration recorded after re-apply, got %d", n)
	}
}

func TestForeignKeysEnabled(t *testing.T) {
	db := openTest(t)
	var on int
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&on); err != nil {
		t.Fatal(err)
	}
	if on != 1 {
		t.Fatalf("foreign_keys must be ON, got %d", on)
	}
}

// Architecture H.3: session rm must cascade to members / nodes / processes /
// port_allocations. Verify the CASCADE relationships work end-to-end.
func TestSessionRmCascades(t *testing.T) {
	db := openTest(t)

	mustExec(t, db,
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:owner", "$argon2id$...",
	)
	mustExec(t, db,
		`INSERT INTO members(sid, pubkey_fp, role, via) VALUES (?,?,?,?)`,
		"lab", "SHA256:owner", "owner", "create",
	)
	mustExec(t, db,
		`INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		"lab", "lab-1", "ONLINE",
	)
	mustExec(t, db,
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES (?,?,?,?,CURRENT_TIMESTAMP,?,?)`,
		"01hzxk", "lab", "lab-1", `["sleep","99"]`, "RUNNING", "SHA256:owner",
	)
	mustExec(t, db,
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp)
		 VALUES (?,?,?,?,?,?,?,?)`,
		14022, "lab", "lab-1", "jupyter", 8888, "deadbeef", "ALLOCATED", "SHA256:owner",
	)

	mustExec(t, db, `DELETE FROM sessions WHERE sid = ?`, "lab")

	for _, q := range []struct {
		label string
		sql   string
	}{
		{"members", `SELECT COUNT(*) FROM members          WHERE sid='lab'`},
		{"nodes", `SELECT COUNT(*) FROM nodes              WHERE sid='lab'`},
		{"processes", `SELECT COUNT(*) FROM processes      WHERE sid='lab'`},
		{"port_allocations", `SELECT COUNT(*) FROM port_allocations WHERE sid='lab'`},
	} {
		var n int
		if err := db.QueryRow(q.sql).Scan(&n); err != nil {
			t.Fatalf("%s count: %v", q.label, err)
		}
		if n != 0 {
			t.Errorf("%s: expected 0 rows after session rm, got %d", q.label, n)
		}
	}
}

func TestTransactionRollback(t *testing.T) {
	db := openTest(t)
	mustExec(t, db,
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:o", "phc",
	)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"prod", "prod", "SHA256:o", "phc",
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='prod'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("rollback should drop the prod insert, got %d rows", n)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='lab'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("pre-tx insert should survive, got %d rows", n)
	}
}

// Foreign-key violations must be rejected (proves PRAGMA foreign_keys=ON took
// effect, not just that the table CHECK constraints are present).
func TestForeignKeyViolationRejected(t *testing.T) {
	db := openTest(t)
	_, err := db.Exec(
		`INSERT INTO members(sid, pubkey_fp, role, via) VALUES (?,?,?,?)`,
		"nonexistent-session", "SHA256:x", "member", "pin",
	)
	if err == nil {
		t.Fatal("expected FK violation when inserting member with no session")
	}
	// Both modernc and mattn drivers wrap it differently; just ensure it errored.
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unexpected error type: %v", err)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// Cover the applyOne body-exec failure branch.
func TestApplyOneRejectsMalformedSQL(t *testing.T) {
	db := openTest(t)
	if err := applyOne(db, "0099_malformed.sql", "THIS IS NOT VALID SQL;"); err == nil {
		t.Fatal("expected applyOne to fail on malformed SQL")
	}
	// Failed migration must leave no row in schema_migrations.
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM `+migrationsTable+` WHERE version=?`,
		"0099_malformed.sql",
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0 recorded versions for failed migration, got %d", n)
	}
}

// Cover the Open failure branch via an unparseable DSN.
func TestOpenRejectsBadDSN(t *testing.T) {
	// modernc.org/sqlite uses URI parsing; an obviously broken file URI should
	// fail at PRAGMA / Exec time. We don't pin the exact error string.
	if _, err := Open("file:/?_pragma=foreign_keys%28banana%29"); err == nil {
		t.Fatal("expected Open to fail on garbage pragma")
	}
}
