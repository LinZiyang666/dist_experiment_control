// Package storage_test exercises the SQLite migration / DSN / FK / boundary
// behaviours of internal/storage AND the JetStream `history-<sid>` durability
// invariants of internal/jsstream from a black-box perspective. Tests use
// storage.Open (no internal helpers) and jsstream public API only.
package storage_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// migrationsRelDir is the on-disk source-of-truth path used by these tests.
// internal/storage's embed.FS is unexported, so we re-read the same files
// directly from the repo to drive single-step / append-only / idempotency
// assertions. Project layout is fixed (cmd / internal / test); the tests
// live in test/storage so two `..` jumps reach the repo root.
const migrationsRelDir = "../../internal/storage/migrations"

// listMigrations enumerates *.sql files under migrationsRelDir, sorted
// lexicographically — matching applyMigrations' own sort order so the
// names line up with what storage.Open recorded in schema_migrations.
func listMigrations(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(migrationsRelDir, "*.sql"))
	if err != nil {
		t.Fatalf("glob migrations: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no migration files found under %s", migrationsRelDir)
	}
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, filepath.Base(m))
	}
	sort.Strings(out)
	return out
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(migrationsRelDir, name))
	if err != nil {
		t.Fatalf("read migration %s: %v", name, err)
	}
	return string(body)
}

// openTestDB is the canonical "fresh DB with all migrations applied" used
// by most of these tests. Closed via t.Cleanup.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// openRawDB opens a fresh SQLite without applying migrations. Used by the
// "single migration in isolation" / "manual ordering" tests so each
// migration can be exercised independently of storage.Open's batch path.
//
// We reproduce the exact DSN flags that storage.Open uses (foreign_keys=ON,
// busy_timeout=5000) so single-step tests share the same FK / lock semantics
// as production. SetMaxOpenConns(1) too — keeps PRAGMAs sticky on the lone
// pooled conn.
func openRawDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := "file::memory:?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// schemaSnapshot returns a deterministic dump of all CREATE TABLE / INDEX
// statements registered in sqlite_master (excluding internal SQLite
// bookkeeping like sqlite_sequence). Used to compare two DBs for shape
// equivalence.
func schemaSnapshot(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(
		`SELECT type, name, COALESCE(sql, '') FROM sqlite_master
		 WHERE type IN ('table','index')
		   AND name NOT LIKE 'sqlite_%'
		 ORDER BY type, name`,
	)
	if err != nil {
		t.Fatalf("snapshot query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var typ, name, ddl string
		if err := rows.Scan(&typ, &name, &ddl); err != nil {
			t.Fatalf("snapshot scan: %v", err)
		}
		// Whitespace-collapse the DDL so cosmetic SQL formatting can't
		// flap the comparison.
		ddl = strings.Join(strings.Fields(ddl), " ")
		out = append(out, typ+":"+name+":"+ddl)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("snapshot rows: %v", err)
	}
	return out
}

// A1 — Each migration is independently executable on a fresh DB.
//
// 0003 transforms port_allocations (CREATE _new + DROP + RENAME) so it
// REQUIRES 0001 already applied. We honour that by running prior
// migrations first, but exercise just the single migration's exec call
// in isolation — proving each file is a valid SQL script (no embedded
// dependencies on schema_migrations / pragma side effects).
func TestA1_EachMigrationExecutesStandalone(t *testing.T) {
	names := listMigrations(t)
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			db := openRawDB(t)
			// Apply prerequisites raw (no schema_migrations bookkeeping)
			// so the file under test sees a realistic DB shape.
			for _, prior := range names[:i] {
				if _, err := db.Exec(readMigration(t, prior)); err != nil {
					t.Fatalf("apply prerequisite %s: %v", prior, err)
				}
			}
			body := readMigration(t, name)
			if _, err := db.Exec(body); err != nil {
				t.Fatalf("apply %s standalone: %v", name, err)
			}
		})
	}
}

// A2 — Manual sequential apply produces the same schema as storage.Open.
//
// We re-create the schema by hand-applying every migration in lexical
// order on a raw conn, then snapshot both databases and compare.
// Catches the class of bug where storage.Open silently skips a file
// (e.g. wrong embed pattern, off-by-one in sort).
func TestA2_ManualApplyEqualsOpen(t *testing.T) {
	names := listMigrations(t)

	manual := openRawDB(t)
	for _, name := range names {
		body := readMigration(t, name)
		if _, err := manual.Exec(body); err != nil {
			t.Fatalf("manual apply %s: %v", name, err)
		}
	}

	auto := openTestDB(t)

	wantTables := []string{"sessions", "members", "nodes", "processes",
		"port_allocations", "agent_provisioning"}
	for _, tbl := range wantTables {
		var n int
		if err := auto.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			tbl,
		).Scan(&n); err != nil || n != 1 {
			t.Errorf("storage.Open missing table %q (count=%d, err=%v)", tbl, n, err)
		}
		if err := manual.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			tbl,
		).Scan(&n); err != nil || n != 1 {
			t.Errorf("manual apply missing table %q (count=%d, err=%v)", tbl, n, err)
		}
	}

	manualSnap := schemaSnapshot(t, manual)
	autoSnap := schemaSnapshot(t, auto)
	// auto has the schema_migrations bookkeeping table; manual doesn't.
	// Filter that single entry out of the auto snapshot before diff.
	filtered := autoSnap[:0]
	for _, row := range autoSnap {
		if strings.HasPrefix(row, "table:schema_migrations:") {
			continue
		}
		filtered = append(filtered, row)
	}
	if len(manualSnap) != len(filtered) {
		t.Fatalf("schema row count mismatch: manual=%d auto=%d\nmanual=%v\nauto=%v",
			len(manualSnap), len(filtered), manualSnap, filtered)
	}
	for i := range manualSnap {
		if manualSnap[i] != filtered[i] {
			t.Errorf("schema row %d differs:\n  manual: %s\n  auto:   %s",
				i, manualSnap[i], filtered[i])
		}
	}
}

// A3 — Idempotency check: running storage.Open a second time on an
// existing on-disk DB must be a true no-op. The contract is recorded
// per-version via the schema_migrations table; this test pins that
// neither row count nor schema shape drifts on re-Open.
func TestA3_OpenIsIdempotentAcrossRestarts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	first, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	// Seed a row so we can also assert data survives the round-trip.
	if _, err := first.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"persist", "persist", "SHA256:o", "phc",
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	snap1 := schemaSnapshot(t, first)
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	second, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer func() { _ = second.Close() }()

	snap2 := schemaSnapshot(t, second)
	if len(snap1) != len(snap2) {
		t.Fatalf("schema row count drifted across restarts: %d -> %d", len(snap1), len(snap2))
	}
	for i := range snap1 {
		if snap1[i] != snap2[i] {
			t.Errorf("schema row %d drifted across restarts:\n  first:  %s\n  second: %s",
				i, snap1[i], snap2[i])
		}
	}
	var n int
	if err := second.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='persist'`).Scan(&n); err != nil {
		t.Fatalf("post-restart query: %v", err)
	}
	if n != 1 {
		t.Errorf("persisted row gone after restart, got count=%d", n)
	}
}

// A4 — schema_migrations bookkeeping table exists and has one entry per
// migration file shipped in the repo.
func TestA4_SchemaMigrationsRecordsAllVersions(t *testing.T) {
	db := openTestDB(t)
	names := listMigrations(t)

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if n != len(names) {
		t.Errorf("schema_migrations count: got %d want %d", n, len(names))
	}

	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != len(names) {
		t.Fatalf("recorded versions length: got %d want %d", len(got), len(names))
	}
	for i, n := range names {
		if got[i] != n {
			t.Errorf("schema_migrations[%d]: got %q want %q", i, got[i], n)
		}
	}
}

// A5 — Append-only policy. Migrations may NOT introduce destructive
// schema mutations except in the well-known SQLite "rename via temp
// table" idiom (CREATE x_new → ... → DROP x → ALTER x_new RENAME TO x).
//
// Allow-list:
//   - DROP TABLE <name>_new        (cleanup of the temp name itself)
//   - DROP TABLE <name> when the same file also has CREATE <name>_new
//     AND ALTER TABLE <name>_new RENAME TO <name>
//
// Anything else (DROP COLUMN, ALTER TABLE ... DROP, plain DROP TABLE
// without the rename pattern) is a regression of the append-only rule
// from architecture H.1.
func TestA5_MigrationsAreAppendOnly(t *testing.T) {
	names := listMigrations(t)
	dropTableRE := regexp.MustCompile(`(?i)\bDROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+([a-zA-Z0-9_]+)`)
	dropColumnRE := regexp.MustCompile(`(?i)\bDROP\s+COLUMN\b`)
	alterDropRE := regexp.MustCompile(`(?i)\bALTER\s+TABLE\b[^;]*\bDROP\b`)

	for _, name := range names {
		body := readMigration(t, name)
		if dropColumnRE.FindString(body) != "" {
			t.Errorf("%s: DROP COLUMN is forbidden (append-only policy)", name)
		}
		if alterDropRE.FindString(body) != "" {
			t.Errorf("%s: ALTER TABLE ... DROP is forbidden (append-only policy)", name)
		}
		for _, m := range dropTableRE.FindAllStringSubmatch(body, -1) {
			tbl := m[1]
			if strings.HasSuffix(tbl, "_new") {
				continue
			}
			// Allow drop iff the file also has BOTH create-new AND
			// rename-back idiom for the same table.
			createNewRE := regexp.MustCompile(`(?i)\bCREATE\s+TABLE\s+` +
				regexp.QuoteMeta(tbl) + `_new\b`)
			renameBackRE := regexp.MustCompile(`(?i)\bALTER\s+TABLE\s+` +
				regexp.QuoteMeta(tbl) + `_new\s+RENAME\s+TO\s+` +
				regexp.QuoteMeta(tbl) + `\b`)
			if !createNewRE.MatchString(body) || !renameBackRE.MatchString(body) {
				t.Errorf("%s: DROP TABLE %s outside the rename idiom is forbidden",
					name, tbl)
			}
		}
	}
}
