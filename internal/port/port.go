// Package port owns the SQLite `port_allocations` table — the
// authoritative state for `tether expose`.
//
// Architecture D.4 / F.3 / F.4 (read first; this package implements the
// state machine they describe):
//
//   - Public ports come from a fixed band [PortBandLow, PortBandHigh]
//     (default 14000-14999, 1000 ports). Allocate finds the first free
//     port in the band, generates a 32-byte URL-safe random token, and
//     writes (port, token_hash=SHA256(token), state=ALLOCATED) in one
//     SQLite transaction.
//
//   - The raw token is returned to the caller and NEVER stored. Broker
//     forwards it to the agent inside the expose.req.forwarded message;
//     agent persists it to ~/.tether/agent/<sid>/state.json so frpc can
//     re-present it on restart. broker side has only the SHA256.
//
//   - State machine: ALLOCATED → REVOKED (broker single-side, e.g. node
//     OFFLINE ≥ 15min) or ALLOCATED → FREED (operator `expose rm` /
//     session rm). Both transitions IMMEDIATELY make the port number
//     re-usable; the row is kept for audit. Port reuse is the common
//     case (1000 ports across many short-lived exposes).
//
//   - Lookup-by-name is the operator-facing query (`expose rm
//     --name jupyter` finds the port by (sid, name)).
//
//   - Lookup-by-token-hash is the frps plugin-hook query (when frpc
//     dials in claiming token T, broker computes SHA256(T) and looks
//     up the row to authorize the requested remote_port). Only
//     ALLOCATED rows count.
package port

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// PortBandLow / PortBandHigh bound the public port range. Defaults
// match architecture A.3 (`broker.yaml.frp.port_range = "14000-14999"`).
// Tests typically pass a smaller band to avoid noisy parallel test
// allocation; production uses the defaults.
const (
	DefaultPortBandLow  = 14000
	DefaultPortBandHigh = 14999
)

// State labels mirror the CHECK constraint on port_allocations.state.
type State string

const (
	StateAllocated State = "ALLOCATED"
	StateRevoked   State = "REVOKED"
	StateFreed     State = "FREED"
)

// Allocation is the read-side projection of one port_allocations row.
// Token is populated by Allocate (the only entry point that ever sees
// the raw token); subsequent lookups never return it (only the hash).
type Allocation struct {
	Port        int
	SID         string
	NID         string
	Name        string
	LocalPort   int
	TokenHash   string // hex SHA256(rawToken)
	State       State
	CreatedByFP string
	CreatedAt   time.Time
	RevokedAt   *time.Time

	// Token is the raw 32-byte URL-safe base64 token, ONLY populated
	// in the Allocate return value. Callers MUST forward it to the
	// agent and discard their own copy; subsequent lookups will not
	// re-derive it (only the hash is persisted).
	Token string
}

// Errors callers may need to distinguish. ErrNotFound is the lookup
// negative; ErrPortExhausted means every slot in the band is in
// ALLOCATED state right now (REVOKED/FREED rows count as free).
// ErrNameTaken is "another ALLOCATED row already has this (sid, name)".
var (
	ErrNotFound      = errors.New("port: row not found")
	ErrPortExhausted = errors.New("port: no free port in band")
	ErrNameTaken     = errors.New("port: another ALLOCATED row already uses this (sid, name)")
)

// Config customizes the band; pass nil for defaults.
type Config struct {
	BandLow  int
	BandHigh int
	Now      func() time.Time
}

// Allocate finds the first free port in the band, generates a fresh
// random token, and INSERTs the ALLOCATED row in a single SQLite
// transaction. Returns the assigned port + raw token. The raw token
// is returned exactly once and never stored — caller MUST forward it
// to the agent and forget their own copy.
//
// "Free" means: no row exists with state=ALLOCATED for that port. Rows
// in REVOKED/FREED state DO NOT block reuse (architecture D.4: revoked
// port number is immediately back in pool).
func Allocate(db *sql.DB, sid, nid, name string, localPort int, createdByFP string, cfg *Config) (*Allocation, error) {
	low, high, now := cfgWithDefaults(cfg)

	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("port: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Architecture D.4 / F.3 invariant: at most one ALLOCATED row per
	// (sid, name) at a time. Without this, `tether expose --name jupyter`
	// twice would silently allocate two public ports for the same logical
	// service and the second `expose rm --name jupyter` would only free
	// one of them.
	var existing int
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE sid=? AND name=? AND state='ALLOCATED'`,
		sid, name,
	).Scan(&existing); err != nil {
		return nil, fmt.Errorf("port: name uniqueness check: %w", err)
	}
	if existing > 0 {
		return nil, ErrNameTaken
	}

	port, err := findFreePort(tx, low, high)
	if err != nil {
		return nil, err
	}

	token, err := genToken()
	if err != nil {
		return nil, fmt.Errorf("port: token gen: %w", err)
	}
	tokenHash := hashToken(token)

	if _, err := tx.Exec(
		`INSERT INTO port_allocations
		   (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, 'ALLOCATED', ?, ?)`,
		port, sid, nid, name, localPort, tokenHash, createdByFP, now.UTC(),
	); err != nil {
		return nil, fmt.Errorf("port: insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("port: commit: %w", err)
	}

	return &Allocation{
		Port:        port,
		SID:         sid,
		NID:         nid,
		Name:        name,
		LocalPort:   localPort,
		TokenHash:   tokenHash,
		State:       StateAllocated,
		CreatedByFP: createdByFP,
		CreatedAt:   now.UTC(),
		Token:       token,
	}, nil
}

// findFreePort scans (low..high) and returns the first int with no
// ALLOCATED row. O(N) in the band size; fine for v1's 1000-port range.
// A future P-future could keep an in-memory free-list seeded from DB on
// boot, but the SQL is plenty fast for v1's expected volumes.
func findFreePort(tx *sql.Tx, low, high int) (int, error) {
	rows, err := tx.Query(
		`SELECT port FROM port_allocations WHERE state='ALLOCATED' AND port BETWEEN ? AND ? ORDER BY port`,
		low, high,
	)
	if err != nil {
		return 0, fmt.Errorf("port: scan band: %w", err)
	}
	defer func() { _ = rows.Close() }()

	taken := map[int]bool{}
	for rows.Next() {
		var p int
		if err := rows.Scan(&p); err != nil {
			return 0, err
		}
		taken[p] = true
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for p := low; p <= high; p++ {
		if !taken[p] {
			return p, nil
		}
	}
	return 0, ErrPortExhausted
}

// LookupByName returns the ALLOCATED row matching (sid, name), or
// ErrNotFound. Used by `expose rm --name <X>` — the operator never
// types the public port number, only the logical name they assigned at
// expose time.
func LookupByName(db *sql.DB, sid, name string) (*Allocation, error) {
	return scanOne(db.QueryRow(
		`SELECT port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at
		 FROM port_allocations
		 WHERE sid=? AND name=? AND state='ALLOCATED'`,
		sid, name,
	))
}

// LookupByTokenHash returns the ALLOCATED row whose SHA256(rawToken)
// matches tokenHash. Used by the frps plugin-hook (architecture F.4):
// when frpc dials in claiming token T, the hook computes SHA256(T),
// looks up here, and authorizes only if the row is ALLOCATED AND the
// claimed remote_port matches the row's port.
func LookupByTokenHash(db *sql.DB, tokenHash string) (*Allocation, error) {
	return scanOne(db.QueryRow(
		`SELECT port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at
		 FROM port_allocations
		 WHERE token_hash=? AND state='ALLOCATED'`,
		tokenHash,
	))
}

// ListBySession returns every row (any state) for sid, sorted by port
// ASC. Used by `tether ps` to render the PORTS section.
func ListBySession(db *sql.DB, sid string) ([]Allocation, error) {
	rows, err := db.Query(
		`SELECT port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at
		 FROM port_allocations
		 WHERE sid=?
		 ORDER BY port`,
		sid,
	)
	if err != nil {
		return nil, fmt.Errorf("port: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Allocation
	for rows.Next() {
		a, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// Free transitions ALLOCATED → FREED (or no-op on REVOKED/FREED). Used
// by `expose rm` and `session rm` cleanup. Returns ErrNotFound if no
// row matches; nil on idempotent re-call against an already-non-ALLOCATED
// row (the row exists, just nothing to do).
func Free(db *sql.DB, port int, now time.Time) error {
	res, err := db.Exec(
		`UPDATE port_allocations
		   SET state='FREED', revoked_at=?
		 WHERE port=? AND state='ALLOCATED'`,
		now.UTC(), port,
	)
	if err != nil {
		return fmt.Errorf("port: free: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("port: rows affected: %w", err)
	}
	if n == 0 {
		// Either no such port, or it's already non-ALLOCATED. Distinguish.
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE port=?`, port).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return ErrNotFound
		}
	}
	return nil
}

// Revoke transitions ALLOCATED → REVOKED. Same shape as Free but used
// by the broker reconciler when the owning node has been OFFLINE long
// enough (architecture D.4 / F.3, default 15min). Idempotent.
func Revoke(db *sql.DB, port int, now time.Time) error {
	res, err := db.Exec(
		`UPDATE port_allocations
		   SET state='REVOKED', revoked_at=?
		 WHERE port=? AND state='ALLOCATED'`,
		now.UTC(), port,
	)
	if err != nil {
		return fmt.Errorf("port: revoke: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("port: rows affected: %w", err)
	}
	if n == 0 {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE port=?`, port).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return ErrNotFound
		}
	}
	return nil
}

// ListAllocatedForOfflineNodes is the read side of the broker
// reconciler: returns every ALLOCATED port whose owning node has been
// OFFLINE for at least the supplied duration. Caller revokes each.
//
// "OFFLINE ≥ T" is computed as nodes.status='OFFLINE' AND
// last_heartbeat_at < (now - T). If the node has no last_heartbeat_at
// yet (just registered, never beat), it isn't OFFLINE so isn't matched.
func ListAllocatedForOfflineNodes(db *sql.DB, now time.Time, threshold time.Duration) ([]Allocation, error) {
	cutoff := now.UTC().Add(-threshold)
	rows, err := db.Query(`
		SELECT pa.port, pa.sid, pa.nid, pa.name, pa.local_port, pa.token_hash,
		       pa.state, pa.created_by_fp, pa.created_at, pa.revoked_at
		FROM port_allocations pa
		JOIN nodes n ON n.sid = pa.sid AND n.nid = pa.nid
		WHERE pa.state='ALLOCATED'
		  AND n.status='OFFLINE'
		  AND n.last_heartbeat_at < ?`,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("port: scan offline: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Allocation
	for rows.Next() {
		a, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// HashToken returns SHA256(rawToken) hex-encoded. Public so callers
// outside this package (frps plugin hook) can compute the lookup key
// from a frpc-supplied token without re-implementing the hash choice.
func HashToken(rawToken string) string { return hashToken(rawToken) }

func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func genToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func cfgWithDefaults(cfg *Config) (low, high int, now time.Time) {
	low, high = DefaultPortBandLow, DefaultPortBandHigh
	now = time.Now()
	if cfg != nil {
		if cfg.BandLow > 0 {
			low = cfg.BandLow
		}
		if cfg.BandHigh > 0 {
			high = cfg.BandHigh
		}
		if cfg.Now != nil {
			now = cfg.Now()
		}
	}
	return low, high, now
}

func scanOne(row *sql.Row) (*Allocation, error) {
	var a Allocation
	var revokedAt sql.NullTime
	var stateStr string
	err := row.Scan(
		&a.Port, &a.SID, &a.NID, &a.Name, &a.LocalPort,
		&a.TokenHash, &stateStr, &a.CreatedByFP, &a.CreatedAt, &revokedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	a.State = State(stateStr)
	if revokedAt.Valid {
		t := revokedAt.Time
		a.RevokedAt = &t
	}
	return &a, nil
}

func scanRow(rows *sql.Rows) (*Allocation, error) {
	var a Allocation
	var revokedAt sql.NullTime
	var stateStr string
	if err := rows.Scan(
		&a.Port, &a.SID, &a.NID, &a.Name, &a.LocalPort,
		&a.TokenHash, &stateStr, &a.CreatedByFP, &a.CreatedAt, &revokedAt,
	); err != nil {
		return nil, err
	}
	a.State = State(stateStr)
	if revokedAt.Valid {
		t := revokedAt.Time
		a.RevokedAt = &t
	}
	return &a, nil
}
