// Package agentprov is the SQLite-backed binding from `(sid, nid)` to
// the agent's nkey fingerprint. It exists so auth_callout can answer
// "may this nkey present itself as agent for this (sid, nid)?" without
// trusting the CONNECT name alone.
//
// The table layout lives in 0002_agent_provisioning.sql; this package
// is the only writer. Two callers:
//
//   - auth_callout, on every agent CONNECT:
//       Lookup(db, sid, nid) → ErrNotProvisioned means "first time, must
//       supply --pin"; a returned fp that doesn't match the connecting
//       nkey means "this nid is bound to a different agent, deny".
//
//   - auth_callout, on first-time CONNECT (with --pin verified):
//       Provision(db, sid, nid, fp, now) — idempotent INSERT OR IGNORE
//       for the (sid, nid) row.
//
// Architecture K.1 — agent identity is per-machine, per-session.
package agentprov

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrSessionMissing / ErrSessionDeleting / ErrInvalidPIN are returned by
// ProvisionWithPIN. They map cleanly to auth_callout deny reasons and are
// distinguishable so the handler can log specifically.
var (
	ErrSessionMissing  = errors.New("agentprov: session does not exist")
	ErrSessionDeleting = errors.New("agentprov: session is being deleted")
	ErrInvalidPIN      = errors.New("agentprov: invalid PIN")
)

// ErrNotProvisioned is returned by Lookup when no row exists for the
// requested (sid, nid). auth_callout uses it to gate the PIN-bootstrap
// path (no row + no PIN → deny; no row + valid PIN → provision + allow).
var ErrNotProvisioned = errors.New("agentprov: (sid,nid) not provisioned")

// Lookup returns the agent fingerprint bound to (sid, nid), or
// ErrNotProvisioned if no row exists.
func Lookup(db *sql.DB, sid, nid string) (string, error) {
	var fp string
	switch err := db.QueryRow(
		`SELECT agent_fp FROM agent_provisioning WHERE sid = ? AND nid = ?`,
		sid, nid,
	).Scan(&fp); err {
	case nil:
		return fp, nil
	case sql.ErrNoRows:
		return "", ErrNotProvisioned
	default:
		return "", fmt.Errorf("agentprov: lookup: %w", err)
	}
}

// Provision binds (sid, nid) → agentFP. First-write-wins: a second call
// with a different fp on the same (sid, nid) leaves the existing row
// untouched and returns ErrAlreadyProvisioned so auth_callout can deny.
//
// Caller responsibility: validate sid/nid format and PIN BEFORE calling.
// Provision treats its inputs as already-trusted.
func Provision(db *sql.DB, sid, nid, agentFP string, now time.Time) error {
	res, err := db.Exec(
		`INSERT OR IGNORE INTO agent_provisioning(sid, nid, agent_fp, joined_at)
		 VALUES (?, ?, ?, ?)`,
		sid, nid, agentFP, now.UTC(),
	)
	if err != nil {
		return fmt.Errorf("agentprov: insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("agentprov: rows affected: %w", err)
	}
	if n == 0 {
		// Row already exists. Re-read to decide if it's the same fp
		// (idempotent retry) or a different one (real conflict).
		existing, err := Lookup(db, sid, nid)
		if err != nil {
			return fmt.Errorf("agentprov: re-lookup after no-op insert: %w", err)
		}
		if existing != agentFP {
			return ErrAlreadyProvisioned
		}
	}
	return nil
}

// ErrAlreadyProvisioned is returned by Provision when the (sid, nid) row
// already exists and is bound to a different fingerprint. auth_callout
// surfaces this as a clear "nid taken by another agent" deny reason.
var ErrAlreadyProvisioned = errors.New("agentprov: (sid,nid) already bound to a different agent")

// ProvisionWithPIN is the PIN-bootstrap path: read the session's pin_hash,
// verify the supplied pin via verifyPIN, then INSERT the (sid, nid, fp)
// row. Returns ErrSessionMissing / ErrSessionDeleting / ErrInvalidPIN /
// ErrAlreadyProvisioned for the four clean failure shapes.
//
// verifyPIN is injected so this package doesn't pull internal/auth (which
// pulls jwt + ed25519, both unrelated to provisioning state).
func ProvisionWithPIN(
	db *sql.DB, sid, nid, agentFP, pin string,
	verifyPIN func(pin, hash string) error,
	now time.Time,
) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("agentprov: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var pinHash, state string
	switch err := tx.QueryRow(
		`SELECT pin_hash, state FROM sessions WHERE sid = ?`, sid,
	).Scan(&pinHash, &state); err {
	case nil:
		// proceed
	case sql.ErrNoRows:
		return ErrSessionMissing
	default:
		return fmt.Errorf("agentprov: session lookup: %w", err)
	}
	if state != "ACTIVE" {
		return ErrSessionDeleting
	}
	if err := verifyPIN(pin, pinHash); err != nil {
		return ErrInvalidPIN
	}

	res, err := tx.Exec(
		`INSERT OR IGNORE INTO agent_provisioning(sid, nid, agent_fp, joined_at)
		 VALUES (?, ?, ?, ?)`,
		sid, nid, agentFP, now.UTC(),
	)
	if err != nil {
		return fmt.Errorf("agentprov: insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("agentprov: rows affected: %w", err)
	}
	if n == 0 {
		var existing string
		if err := tx.QueryRow(
			`SELECT agent_fp FROM agent_provisioning WHERE sid = ? AND nid = ?`,
			sid, nid,
		).Scan(&existing); err != nil {
			return fmt.Errorf("agentprov: re-lookup after no-op insert: %w", err)
		}
		if existing != agentFP {
			return ErrAlreadyProvisioned
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agentprov: commit: %w", err)
	}
	return nil
}
