// Package proxysub is the storage layer for P13 per-subscriber proxy
// credentials. Each subscriber is one ACTIVE Shadowsocks key plus a bearer
// subscription token; revoking one subscriber leaves the others untouched.
//
// Idioms mirror internal/port: crypto/rand secrets, SHA256-hashed bearer
// token (raw returned exactly once), partial-unique-index single-winner, and
// typed sentinel errors. The DB stores the SS psk RECOVERABLE (the agent must
// decrypt with it and the /sub renderer must vend it) but the subscription
// token hash-only.
package proxysub

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
)

var (
	// ErrNotFound is the lookup/revoke negative.
	ErrNotFound = errors.New("proxysub: subscriber not found")
	// ErrNameTaken means an ACTIVE subscriber with this (sid, name) exists.
	ErrNameTaken = errors.New("proxysub: an active subscriber already uses this name")
	// ErrAlreadyRevoked means the named subscriber is already REVOKED.
	ErrAlreadyRevoked = errors.New("proxysub: subscriber already revoked")
)

// Subscriber is one proxy subscriber row.
type Subscriber struct {
	SID         string
	SubID       string
	Name        string
	TokenHash   string
	PSK         string // base64 SS password
	Cipher      string
	State       string // ACTIVE | REVOKED
	CreatedByFP string
	CreatedAt   time.Time
	RevokedAt   *time.Time

	// Token is the raw subscription token, populated ONLY by Create's return
	// value. Callers build the /sub URL from it and discard it; only the hash
	// is persisted.
	Token string
}

// Create inserts an ACTIVE subscriber with a fresh token + SS psk and returns
// it (with the raw Token populated once). ErrNameTaken if an ACTIVE row
// already uses (sid, name).
// execQuerier is satisfied by both *sql.DB and *sql.Tx, so Create/Revoke can run
// either standalone or inside a broker transaction that also bumps the epoch
// (round-6 F5: the credential mutation and version bump must commit together).
type execQuerier interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

func Create(db execQuerier, sid, name, createdByFP string, now time.Time) (*Subscriber, error) {
	token, err := genSecret(32)
	if err != nil {
		return nil, fmt.Errorf("proxysub: token gen: %w", err)
	}
	psk, err := genSecret(16)
	if err != nil {
		return nil, fmt.Errorf("proxysub: psk gen: %w", err)
	}
	subID := newSubID()
	tokenHash := HashToken(token)

	if _, err := db.Exec(
		`INSERT INTO proxy_subscribers
		   (sid, sub_id, name, token_hash, psk, cipher, state, created_by_fp, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)`,
		sid, subID, name, tokenHash, psk, ssproxy.Cipher, createdByFP, now.UTC(),
	); err != nil {
		if isUniqueViolation(err) {
			return nil, ErrNameTaken
		}
		return nil, fmt.Errorf("proxysub: insert: %w", err)
	}
	return &Subscriber{
		SID: sid, SubID: subID, Name: name, TokenHash: tokenHash, PSK: psk,
		Cipher: ssproxy.Cipher, State: "ACTIVE", CreatedByFP: createdByFP,
		CreatedAt: now.UTC(), Token: token,
	}, nil
}

// Revoke transitions the named subscriber ACTIVE -> REVOKED. ErrNotFound if no
// such name in this session ever existed; ErrAlreadyRevoked if it is already
// revoked.
func Revoke(db execQuerier, sid, name string, now time.Time) error {
	res, err := db.Exec(
		`UPDATE proxy_subscribers SET state='REVOKED', revoked_at=?
		 WHERE sid=? AND name=? AND state='ACTIVE'`,
		now.UTC(), sid, name,
	)
	if err != nil {
		return fmt.Errorf("proxysub: revoke: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("proxysub: rows affected: %w", err)
	}
	if n == 0 {
		var any int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM proxy_subscribers WHERE sid=? AND name=?`, sid, name,
		).Scan(&any); err != nil {
			return err
		}
		if any == 0 {
			return ErrNotFound
		}
		return ErrAlreadyRevoked
	}
	return nil
}

// ListBySession returns every subscriber row (any state) for sid, newest
// first. Never populates Token. Used by status/ls (redacted upstream).
func ListBySession(db *sql.DB, sid string) ([]Subscriber, error) {
	rows, err := db.Query(
		`SELECT sid, sub_id, name, token_hash, psk, cipher, state, created_by_fp, created_at, revoked_at
		 FROM proxy_subscribers WHERE sid=? ORDER BY created_at DESC`, sid,
	)
	if err != nil {
		return nil, fmt.Errorf("proxysub: list: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRows(rows)
}

// ListActive returns the ACTIVE subscribers for sid (with PSK), oldest first
// for a stable keyset order. Used to build the ssproxy keyset pushed to agents.
func ListActive(db *sql.DB, sid string) ([]Subscriber, error) {
	rows, err := db.Query(
		`SELECT sid, sub_id, name, token_hash, psk, cipher, state, created_by_fp, created_at, revoked_at
		 FROM proxy_subscribers WHERE sid=? AND state='ACTIVE' ORDER BY created_at ASC`, sid,
	)
	if err != nil {
		return nil, fmt.Errorf("proxysub: list active: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRows(rows)
}

// LookupByTokenHash returns the ACTIVE subscriber whose token hashes to
// tokenHash, or ErrNotFound. Used by GET /sub/<token>.
func LookupByTokenHash(db *sql.DB, sid, tokenHash string) (*Subscriber, error) {
	row := db.QueryRow(
		`SELECT sid, sub_id, name, token_hash, psk, cipher, state, created_by_fp, created_at, revoked_at
		 FROM proxy_subscribers WHERE sid=? AND token_hash=? AND state='ACTIVE'`, sid, tokenHash,
	)
	s, err := scanOne(row)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// LookupSIDByTokenHash resolves the owning sid for an ACTIVE token hash
// without binding to a session up front (the /sub URL carries only the token).
func LookupSIDByTokenHash(db *sql.DB, tokenHash string) (string, error) {
	var sid string
	err := db.QueryRow(
		`SELECT sid FROM proxy_subscribers WHERE token_hash=? AND state='ACTIVE'`, tokenHash,
	).Scan(&sid)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("proxysub: lookup sid: %w", err)
	}
	return sid, nil
}

// HashToken returns SHA256(rawToken) hex-encoded (same scheme as port tokens).
func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func genSecret(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func newSubID() string {
	return strings.ToLower(ulid.Make().String())
}

func isUniqueViolation(err error) bool {
	// modernc.org/sqlite surfaces a UNIQUE constraint failure in the message.
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique")
}

func scanRows(rows *sql.Rows) ([]Subscriber, error) {
	var out []Subscriber
	for rows.Next() {
		var s Subscriber
		var revoked sql.NullTime
		if err := rows.Scan(&s.SID, &s.SubID, &s.Name, &s.TokenHash, &s.PSK, &s.Cipher,
			&s.State, &s.CreatedByFP, &s.CreatedAt, &revoked); err != nil {
			return nil, err
		}
		if revoked.Valid {
			t := revoked.Time
			s.RevokedAt = &t
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func scanOne(row *sql.Row) (*Subscriber, error) {
	var s Subscriber
	var revoked sql.NullTime
	err := row.Scan(&s.SID, &s.SubID, &s.Name, &s.TokenHash, &s.PSK, &s.Cipher,
		&s.State, &s.CreatedByFP, &s.CreatedAt, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if revoked.Valid {
		t := revoked.Time
		s.RevokedAt = &t
	}
	return &s, nil
}
