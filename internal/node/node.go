// Package node owns the node lifecycle: register, heartbeat, state-machine
// reconciliation, and read-side snapshots. tetherd writes; admin tools read.
//
// State machine (architecture D.2):
//
//	ONLINE   — heartbeat within StaleAfter (default 5s)
//	STALE    — StaleAfter ≤ age < OfflineAfter
//	OFFLINE  — age ≥ OfflineAfter (default 60s)
//
// STALE / OFFLINE are visibility states only; per the architecture they
// don't trigger any cleanup of resources owned by the node.
package node

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// State labels written to the SQLite `nodes.status` column. Mirrors the CHECK
// constraint in 0001_init.sql.
type State string

const (
	StateOnline  State = "ONLINE"
	StateStale   State = "STALE"
	StateOffline State = "OFFLINE"
)

// Default thresholds (architecture D.2). Broker.Config can override for tests.
const (
	DefaultStaleAfter   = 5 * time.Second
	DefaultOfflineAfter = 60 * time.Second
)

// stateForAge maps a heartbeat age to the canonical state.
func stateForAge(age time.Duration, staleAfter, offlineAfter time.Duration) State {
	switch {
	case age < staleAfter:
		return StateOnline
	case age < offlineAfter:
		return StateStale
	default:
		return StateOffline
	}
}

// RegisterInput is the payload an agent supplies on register.req. The broker
// fills in its `now` separately to keep all clock authority on tetherd.
type RegisterInput struct {
	SID            string
	NID            string
	ProtoVersion   int
	ReleaseVersion string
	OS             string
	Arch           string
	BootID         string
	// ProxyCapable (P13) records whether the agent advertised the proxy-v1
	// capability; the broker uses it to gate proxy allocation/push.
	ProxyCapable bool
}

// ErrSessionMissing is returned by Register when the agent's target sid
// has no row in the sessions table. The caller (broker) replies with
// `code=session_not_found` so the agent surfaces a clear error and the
// operator knows to create the session first.
var ErrSessionMissing = errors.New("node: target session does not exist")

// Register inserts (or updates) the node row and forces status to ONLINE.
//
// The target session MUST already exist (caller responsibility — owner
// creates it via `tether session create`). Foreign-key cascade in
// 0001_init.sql guarantees the row will be cleaned up with the session.
func Register(db *sql.DB, in RegisterInput, now time.Time) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("node: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existsSid string
	switch err := tx.QueryRow(
		`SELECT sid FROM sessions WHERE sid = ?`, in.SID,
	).Scan(&existsSid); err {
	case nil:
		// row found
	case sql.ErrNoRows:
		return ErrSessionMissing
	default:
		return fmt.Errorf("node: lookup session: %w", err)
	}

	capable := 0
	if in.ProxyCapable {
		capable = 1
	}
	if _, err := tx.Exec(`
		INSERT INTO nodes(nid, sid, last_heartbeat_at, status, boot_id,
		                  release_version, proto_version, registered_at, proxy_capable)
		VALUES (?,?,?,?,?,?,?,?,?)
		ON CONFLICT(sid, nid) DO UPDATE SET
		    last_heartbeat_at = excluded.last_heartbeat_at,
		    status            = 'ONLINE',
		    boot_id           = excluded.boot_id,
		    release_version   = excluded.release_version,
		    proto_version     = excluded.proto_version,
		    proxy_capable     = excluded.proxy_capable,
		    -- round-6 F8: a (re)register means a NEW agent process that has NOT yet
		    -- re-established its SS server + tunnel. Clear readiness so a restarted
		    -- or downgraded agent is never rendered into /sub until its post-bind
		    -- ACK sets proxy_ready=1 again.
		    proxy_ready       = 0
	`,
		in.NID, in.SID, now, string(StateOnline),
		in.BootID, in.ReleaseVersion, in.ProtoVersion, now, capable,
	); err != nil {
		return fmt.Errorf("node: upsert: %w", err)
	}

	return tx.Commit()
}

// Heartbeat updates last_heartbeat_at and forces status back to ONLINE
// (covers STALE → ONLINE recovery on the very next beat without waiting for
// the reconcile ticker). Returns an error when the node is unknown.
func Heartbeat(db *sql.DB, sid, nid string, now time.Time) error {
	res, err := db.Exec(
		`UPDATE nodes SET last_heartbeat_at = ?, status = 'ONLINE' WHERE sid = ? AND nid = ?`,
		now, sid, nid,
	)
	if err != nil {
		return fmt.Errorf("node: heartbeat: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("node: heartbeat for unknown (%s, %s)", sid, nid)
	}
	return nil
}

// ReconcileStates walks every node row and aligns its status with the age of
// last_heartbeat_at. Returns the number of rows whose status was changed.
//
// Pure read-then-write — safe to call from a single ticker goroutine. With
// SetMaxOpenConns(1) on storage.Open, no other goroutine can interleave.
func ReconcileStates(db *sql.DB, now time.Time, staleAfter, offlineAfter time.Duration) (int, error) {
	rows, err := db.Query(`SELECT sid, nid, status, last_heartbeat_at FROM nodes`)
	if err != nil {
		return 0, fmt.Errorf("node: scan: %w", err)
	}

	type pending struct {
		sid, nid string
		newState State
	}
	var pendingUpdates []pending
	for rows.Next() {
		var sid, nid, status string
		var lastHb sql.NullTime
		if err := rows.Scan(&sid, &nid, &status, &lastHb); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("node: scan row: %w", err)
		}
		if !lastHb.Valid {
			continue // node row exists but never beat; leave alone.
		}
		want := stateForAge(now.Sub(lastHb.Time), staleAfter, offlineAfter)
		if string(want) != status {
			pendingUpdates = append(pendingUpdates, pending{sid, nid, want})
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, u := range pendingUpdates {
		// P13 M6: a node going OFFLINE also loses its proxy_ready flag, so a
		// later ONLINE-via-heartbeat flap can't render a not-yet-rebound node
		// in /sub (it must re-ACK first), and `proxy status` doesn't show a
		// dead node as Ready.
		q := `UPDATE nodes SET status = ? WHERE sid = ? AND nid = ?`
		if u.newState == StateOffline {
			q = `UPDATE nodes SET status = ?, proxy_ready = 0 WHERE sid = ? AND nid = ?`
		}
		if _, err := db.Exec(q, string(u.newState), u.sid, u.nid); err != nil {
			return 0, fmt.Errorf("node: update %s/%s: %w", u.sid, u.nid, err)
		}
	}
	return len(pendingUpdates), nil
}

// Snapshot is the read-side row used by `tether admin nodes`.
type Snapshot struct {
	SID             string
	NID             string
	Status          string
	LastHeartbeatAt time.Time
	BootID          string
	ReleaseVersion  string
	ProtoVersion    int
}

// ErrNotFound is returned by LookupStatus when no `nodes(sid,nid)` row
// exists. Lets callers (broker.handleExecReq) distinguish "node never
// registered / typoed nid" from a generic store error and return a
// clean `node_not_found` rejection instead of letting the request hang
// on a NATS timeout.
var ErrNotFound = errors.New("node: row not found")

// LookupStatus returns the current `nodes.status` for (sid,nid) without
// touching the heartbeat. Used as a precheck before forwarding cmds to
// a node — if the node row is missing or not ONLINE, the broker
// rejects immediately and writes audit.call{ok=false} with the
// appropriate code.
//
// Reads the column verbatim; reconciliation runs on a ticker
// (architecture D.2) so the value is at most one ReconcileInterval out
// of date. That's acceptable for "is the agent there right now?"
// precheck — agents that just dropped surface within ReconcileInterval,
// and a forwarded msg to a freshly-dropped agent that hasn't been
// reconciled yet falls back to the existing failure mode (NATS no
// responders → ctl timeout). The cheap precheck eliminates the common
// "typoed nid" / "long-OFFLINE node" path.
func LookupStatus(db *sql.DB, sid, nid string) (State, error) {
	var status string
	switch err := db.QueryRow(
		`SELECT status FROM nodes WHERE sid = ? AND nid = ?`, sid, nid,
	).Scan(&status); err {
	case nil:
		return State(status), nil
	case sql.ErrNoRows:
		return "", ErrNotFound
	default:
		return "", fmt.Errorf("node: lookup status: %w", err)
	}
}

// SetProxyReady sets the P13 nodes.proxy_ready flag (the agent's SS-bind ACK).
// A node is rendered into a /sub response only when ready=1. Idempotent.
func SetProxyReady(db *sql.DB, sid, nid string, ready bool) error {
	v := 0
	if ready {
		v = 1
	}
	if _, err := db.Exec(
		`UPDATE nodes SET proxy_ready=? WHERE sid=? AND nid=?`, v, sid, nid,
	); err != nil {
		return fmt.Errorf("node: set proxy_ready: %w", err)
	}
	return nil
}

// List returns every node row, sorted by (sid, nid) for stable display.
func List(db *sql.DB) ([]Snapshot, error) {
	rows, err := db.Query(`
		SELECT sid, nid, status, last_heartbeat_at, boot_id, release_version, proto_version
		FROM nodes
		ORDER BY sid, nid
	`)
	if err != nil {
		return nil, fmt.Errorf("node: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Snapshot
	for rows.Next() {
		var s Snapshot
		var lastHb sql.NullTime
		var boot, rel sql.NullString
		var pv sql.NullInt64
		if err := rows.Scan(&s.SID, &s.NID, &s.Status, &lastHb, &boot, &rel, &pv); err != nil {
			return nil, err
		}
		if lastHb.Valid {
			s.LastHeartbeatAt = lastHb.Time
		}
		s.BootID = boot.String
		s.ReleaseVersion = rel.String
		s.ProtoVersion = int(pv.Int64)
		out = append(out, s)
	}
	return out, rows.Err()
}
