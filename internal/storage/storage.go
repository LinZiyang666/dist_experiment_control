// Package storage opens & migrates tetherd's authoritative SQLite database.
//
// SQLite holds the realtime authoritative state (sessions / members / nodes /
// processes / port_allocations). It does NOT hold audit history — audit lives
// only in JetStream `history-<sid>` (architecture H.2).
//
// Driver: pure-Go modernc.org/sqlite (CGO_ENABLED=0 so the single-binary
// release is fully static; see architecture I.5 / K.0).
package storage

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

// SQLite PRAGMAs are connection-local. A one-shot `PRAGMA foreign_keys = ON`
// on *sql.DB after Open only affects whichever pooled connection happened to
// run it; new connections (e.g. from another goroutine, or after the first is
// returned to the pool) start with FK off again.
//
// Three safeguards together (belt + suspenders + braces):
//   1. URI parameter `_pragma=foreign_keys(1)` baked into the DSN — modernc
//      runs it at every sqlite3_open_v2(), so every conn in the pool starts
//      with FK on regardless of how the *sql.DB pool is later configured.
//   2. URI parameter `_pragma=busy_timeout(5000)` so any future writer that
//      shares the file (sidecar admin tool, accidental SetMaxOpenConns
//      relaxation) blocks up to 5s instead of immediately failing with
//      SQLITE_BUSY (audit shard 03 F9).
//   3. SetMaxOpenConns(1) — SQLite is a single-writer engine; serializing
//      writes also closes the door on any "second conn missed the pragma"
//      class of bug. SetMaxOpenConns(1) is LOAD-BEARING: many writers
//      (port.Allocate, session.Create, agentprov.Provision) execute
//      compound read-then-write sequences under default-deferred
//      transactions and rely on the pool serializing them. Don't relax
//      without re-thinking the affected allocators (audit shard 03 F8).
//
// Plain ":memory:" isn't URI-style; promote it to "file::memory:" so the
// query parameter is honored.

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationsTable is the per-database tracker of which migration files have
// been applied. Created lazily by applyMigrations.
const migrationsTable = "schema_migrations"

// Open opens (and creates if absent) a SQLite database at dsn, enforces
// foreign keys on every pooled connection (see comment above), serializes
// writes via SetMaxOpenConns(1), applies all embedded migrations
// idempotently, and returns the *sql.DB.
//
// dsn examples:
//   - ":memory:"                          in-process testing
//   - "file:/var/lib/tether/state.db"     production
//
// On any failure the partially-opened DB is closed before returning.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", withForeignKeysPragma(dsn))
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := applyMigrations(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func withForeignKeysPragma(dsn string) string {
	base := dsn
	if dsn == ":memory:" {
		base = "file::memory:"
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	// foreign_keys + busy_timeout chained via & on the same query
	// string. Both are PRAGMAs that modernc applies at every
	// sqlite3_open_v2() — pool-safe.
	return base + sep + "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
}

// applyMigrations applies every embedded migration file (sorted by filename)
// that has not yet been recorded in schema_migrations. Each migration runs in
// its own transaction; the tracker insert is part of that same transaction so
// "applied" and "recorded" are atomic.
func applyMigrations(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`, migrationsTable)); err != nil {
		return fmt.Errorf("storage: create %s: %w", migrationsTable, err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("storage: read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		applied, err := isMigrationApplied(db, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("storage: read migration %s: %w", name, err)
		}
		if err := applyOne(db, name, string(body)); err != nil {
			return err
		}
	}
	return nil
}

func isMigrationApplied(db *sql.DB, name string) (bool, error) {
	var v string
	err := db.QueryRow(
		fmt.Sprintf(`SELECT version FROM %s WHERE version = ?`, migrationsTable),
		name,
	).Scan(&v)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("storage: check migration %s: %w", name, err)
	}
}

func applyOne(db *sql.DB, name, body string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("storage: begin %s: %w", name, err)
	}
	if _, err := tx.Exec(body); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("storage: apply %s: %w", name, err)
	}
	if _, err := tx.Exec(
		fmt.Sprintf(`INSERT INTO %s(version) VALUES (?)`, migrationsTable),
		name,
	); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("storage: record %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit %s: %w", name, err)
	}
	return nil
}
