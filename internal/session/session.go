// Package session manages session lifecycle, membership, and the
// ACTIVE → DELETING tombstone transition (architecture H.3 stage 1).
//
// The full three-stage delete (墓碑 → DELETE stream → DELETE rows) lives in
// P7; this package only implements stage 1 + the C.1 §6 precheck so callers
// can already reject commands targeting a DELETING session.
package session

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

type State string

const (
	StateActive   State = "ACTIVE"
	StateDeleting State = "DELETING"
)

type Role string

const (
	RoleOwner  Role = "owner"
	RoleMember Role = "member"
)

type Via string

const (
	ViaCreate Via = "create"
	ViaPin    Via = "pin"
)

// Session is the read-side projection of the sessions row (no pin_hash —
// that never leaves storage).
type Session struct {
	SID           string
	Name          string
	OwnerPubkeyFP string
	State         State
	CreatedAt     time.Time
	DeletingAt    *time.Time
}

type Member struct {
	SID      string
	PubkeyFP string
	Role     Role
	Via      Via
	JoinedAt time.Time
}

// Sentinel errors so callers can branch (and protocol replies can have
// stable codes).
var (
	ErrAlreadyExists = errors.New("session: already exists")
	ErrNotFound      = errors.New("session: not found")
	ErrDeleting      = errors.New("session: state DELETING (cannot mutate)")
	ErrInvalidPIN    = errors.New("session: pin verification failed")
)

// Create writes a new session row + auto-adds the creator as owner. Returns
// ErrAlreadyExists on sid clash.
func Create(db *sql.DB, sid, name, ownerFP, pinHash string, now time.Time) (*Session, error) {
	if err := proto.ValidateSID(sid); err != nil {
		return nil, fmt.Errorf("session: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("session: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash, state, created_at)
		 VALUES (?,?,?,?,?,?)`,
		sid, name, ownerFP, pinHash, string(StateActive), now,
	); err != nil {
		if isUniqueViolation(err) {
			return nil, ErrAlreadyExists
		}
		return nil, fmt.Errorf("session: insert: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT INTO members(sid, pubkey_fp, role, via, joined_at) VALUES (?,?,?,?,?)`,
		sid, ownerFP, string(RoleOwner), string(ViaCreate), now,
	); err != nil {
		return nil, fmt.Errorf("session: insert owner: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("session: commit: %w", err)
	}
	return &Session{
		SID: sid, Name: name, OwnerPubkeyFP: ownerFP,
		State: StateActive, CreatedAt: now,
	}, nil
}

// Get returns the session row by sid. ErrNotFound when absent.
func Get(db *sql.DB, sid string) (*Session, error) {
	var s Session
	var state string
	var deletingAt sql.NullTime
	err := db.QueryRow(
		`SELECT sid, name, owner_pubkey_fp, state, created_at, deleting_at
		 FROM sessions WHERE sid=?`,
		sid,
	).Scan(&s.SID, &s.Name, &s.OwnerPubkeyFP, &state, &s.CreatedAt, &deletingAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("session: get: %w", err)
	}
	s.State = State(state)
	if deletingAt.Valid {
		s.DeletingAt = &deletingAt.Time
	}
	return &s, nil
}

// IsActive returns true iff the session exists and is in ACTIVE state. Used
// by the C.1 §6 precheck on every incoming command.
func IsActive(db *sql.DB, sid string) (bool, error) {
	var state string
	err := db.QueryRow(`SELECT state FROM sessions WHERE sid=?`, sid).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return state == string(StateActive), nil
}

// ListVisible returns sessions where pubkeyFP appears in members.
func ListVisible(db *sql.DB, pubkeyFP string) ([]Session, error) {
	rows, err := db.Query(`
		SELECT s.sid, s.name, s.owner_pubkey_fp, s.state, s.created_at, s.deleting_at
		FROM sessions s
		JOIN members m ON m.sid = s.sid
		WHERE m.pubkey_fp = ?
		ORDER BY s.sid
	`, pubkeyFP)
	if err != nil {
		return nil, fmt.Errorf("session: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Session
	for rows.Next() {
		var s Session
		var state string
		var deletingAt sql.NullTime
		if err := rows.Scan(&s.SID, &s.Name, &s.OwnerPubkeyFP, &state, &s.CreatedAt, &deletingAt); err != nil {
			return nil, err
		}
		s.State = State(state)
		if deletingAt.Valid {
			s.DeletingAt = &deletingAt.Time
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Tombstone transitions ACTIVE → DELETING. Returns ErrNotFound if absent or
// ErrDeleting if already DELETING. Caller must have already verified owner.
//
// Stage 2 (DELETE history-<sid> stream) and stage 3 (DELETE related rows)
// (architecture H.3 — full three-phase lives in broker/audit.go).
func Tombstone(db *sql.DB, sid string, now time.Time) error {
	res, err := db.Exec(
		`UPDATE sessions SET state='DELETING', deleting_at=?
		 WHERE sid=? AND state='ACTIVE'`,
		now, sid,
	)
	if err != nil {
		return fmt.Errorf("session: tombstone: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var state string
		err := db.QueryRow(`SELECT state FROM sessions WHERE sid=?`, sid).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("session: tombstone lookup: %w", err)
		}
		return ErrDeleting
	}
	return nil
}

// IsOwner returns true iff pubkeyFP is the owner of sid (per members.role).
func IsOwner(db *sql.DB, sid, pubkeyFP string) (bool, error) {
	var role string
	err := db.QueryRow(
		`SELECT role FROM members WHERE sid=? AND pubkey_fp=?`,
		sid, pubkeyFP,
	).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return role == string(RoleOwner), nil
}

// IsMember returns true iff pubkeyFP appears in members for sid.
func IsMember(db *sql.DB, sid, pubkeyFP string) (bool, error) {
	var n int
	err := db.QueryRow(
		`SELECT 1 FROM members WHERE sid=? AND pubkey_fp=? LIMIT 1`,
		sid, pubkeyFP,
	).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// AddMember idempotently adds a row to members.
func AddMember(db *sql.DB, sid, pubkeyFP string, role Role, via Via, now time.Time) error {
	_, err := db.Exec(
		`INSERT OR IGNORE INTO members(sid, pubkey_fp, role, via, joined_at)
		 VALUES (?,?,?,?,?)`,
		sid, pubkeyFP, string(role), string(via), now,
	)
	if err != nil {
		return fmt.Errorf("session: add member: %w", err)
	}
	return nil
}

// JoinWithPIN verifies pin against the session's stored hash via verifyPIN,
// then adds the caller as a member (via=pin). On bad PIN returns
// ErrInvalidPIN; on missing/deleting session returns ErrNotFound /
// ErrDeleting respectively.
//
// verifyPIN is injected so this package doesn't import internal/auth (which
// imports proto, which would create a cycle if auth ever needed session).
func JoinWithPIN(
	db *sql.DB, sid, pubkeyFP, pin string,
	verifyPIN func(pin, hash string) error,
	now time.Time,
) error {
	var pinHash, state string
	err := db.QueryRow(`SELECT pin_hash, state FROM sessions WHERE sid=?`, sid).Scan(&pinHash, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("session: lookup: %w", err)
	}
	if state != string(StateActive) {
		return ErrDeleting
	}
	if err := verifyPIN(pin, pinHash); err != nil {
		return ErrInvalidPIN
	}
	return AddMember(db, sid, pubkeyFP, RoleMember, ViaPin, now)
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "constraint failed: UNIQUE")
}
