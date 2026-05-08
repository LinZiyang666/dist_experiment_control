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

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationsTable is the per-database tracker of which migration files have
// been applied. Created lazily by applyMigrations.
const migrationsTable = "schema_migrations"

// Open opens (and creates if absent) a SQLite database at dsn, enables foreign
// keys, applies all embedded migrations idempotently, and returns the *sql.DB.
//
// dsn examples:
//   - ":memory:"                    in-process testing
//   - "file:/var/lib/tether/state.db"   production
//
// On any failure the partially-opened DB is closed before returning.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("storage: enable foreign keys: %w", err)
	}
	if err := applyMigrations(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
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
