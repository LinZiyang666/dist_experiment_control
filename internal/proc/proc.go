// Package proc tracks managed processes (architecture D.3).
//
// Lifecycle: RUNNING → EXITED. The LOST state lives at the node level
// (when the owning node enters OFFLINE) and is computed at read time
// from process row + node status — not stored explicitly here.
//
// agent owns the runtime knowledge ("process is running / has exited
// with rc=N"); tetherd owns the SQLite row. The path:
//
//   agent fork+exec → pub `ev.proc.started` → broker.Insert
//   agent reads child wait → pub `ev.proc.exit` → broker.MarkExited
package proc

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

type State string

const (
	StateRunning State = "RUNNING"
	StateExited  State = "EXITED"
	StateLost    State = "LOST"
)

// Process is the read-side projection of one row in the `processes` table.
type Process struct {
	PID            string
	SID            string
	NID            string
	Argv           []string
	Cwd            string
	StartedAt      time.Time
	EndedAt        *time.Time
	Status         State
	ExitCode       *int
	StartedByFP    string
	BootID         string
	StartTimeTicks int64
}

var (
	// ErrNotFound is returned by Get / MarkExited when the pid doesn't exist.
	ErrNotFound = errors.New("proc: not found")
	// ErrNodeMissing is returned by Insert when the (sid, nid) FK target
	// doesn't exist (i.e. the agent isn't registered yet).
	ErrNodeMissing = errors.New("proc: target node does not exist")
)

// NewPID generates a fresh ULID (lowercase) suitable for processes.pid.
// Architecture B.5 specifies ULID for pid; lowercase keeps it subject-safe.
func NewPID() string {
	return ulidLower(ulid.Make())
}

func ulidLower(u ulid.ULID) string {
	const enc = "0123456789abcdefghjkmnpqrstvwxyz"
	const t = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	s := u.String()
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		// ulid.String uses Crockford uppercase; lowercase the alpha part.
		if idx := bytePos(t, c); idx >= 0 {
			out[i] = enc[idx]
		} else {
			out[i] = c
		}
	}
	return string(out)
}

func bytePos(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Insert records a new RUNNING process. The (sid, nid) tuple must already
// exist in nodes (FK contract); if not, returns ErrNodeMissing.
func Insert(db *sql.DB, p Process) error {
	if p.PID == "" {
		return fmt.Errorf("proc: pid required")
	}
	argvJSON, err := json.Marshal(p.Argv)
	if err != nil {
		return fmt.Errorf("proc: marshal argv: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("proc: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var found int
	switch err := tx.QueryRow(
		`SELECT 1 FROM nodes WHERE sid=? AND nid=?`, p.SID, p.NID,
	).Scan(&found); err {
	case nil:
		// node exists
	case sql.ErrNoRows:
		return ErrNodeMissing
	default:
		return fmt.Errorf("proc: lookup node: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT INTO processes(pid, sid, nid, argv, cwd, started_at, status, started_by_fp, boot_id, start_time_ticks)
		VALUES (?,?,?,?,?,?,?,?,?,?)
	`,
		p.PID, p.SID, p.NID, string(argvJSON), p.Cwd,
		p.StartedAt, string(StateRunning), p.StartedByFP, p.BootID,
		nullableInt64(p.StartTimeTicks),
	); err != nil {
		return fmt.Errorf("proc: insert: %w", err)
	}
	return tx.Commit()
}

// nullableInt64 returns sql.NullInt64{Valid:false} for zero so the
// processes.start_time_ticks column stays NULL when the agent didn't
// report it (e.g. exec children). Without this the column gets
// literal 0, which would compare-equal to a real "boot tick 0"
// reading on a freshly-booted box and pass the G.1 triple check
// spuriously.
func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// MarkExited transitions pid to EXITED with the given exit code. Returns
// ErrNotFound if pid doesn't exist.
func MarkExited(db *sql.DB, pid string, exitCode int, when time.Time) error {
	res, err := db.Exec(`
		UPDATE processes
		SET status='EXITED', exit_code=?, ended_at=?
		WHERE pid=? AND status='RUNNING'
	`, exitCode, when, pid)
	if err != nil {
		return fmt.Errorf("proc: mark exited: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either unknown pid or already non-RUNNING. Distinguish.
		var any int
		err := db.QueryRow(`SELECT 1 FROM processes WHERE pid=?`, pid).Scan(&any)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		// Already EXITED (or LOST). Idempotent — treat as no-op.
	}
	return nil
}

// Get returns the row for pid. ErrNotFound when absent.
func Get(db *sql.DB, pid string) (*Process, error) {
	row := db.QueryRow(`
		SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
		       started_by_fp, boot_id, start_time_ticks
		FROM processes
		WHERE pid = ?
	`, pid)
	return scanProcess(row)
}

// ListBySession returns all processes for sid in (sid, started_at desc) order.
func ListBySession(db *sql.DB, sid string) ([]Process, error) {
	rows, err := db.Query(`
		SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
		       started_by_fp, boot_id, start_time_ticks
		FROM processes
		WHERE sid = ?
		ORDER BY started_at DESC
	`, sid)
	if err != nil {
		return nil, fmt.Errorf("proc: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Process
	for rows.Next() {
		p, err := scanProcessRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanProcess(s scanner) (*Process, error) {
	p, err := scanProcessRow(s)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

func scanProcessRow(s scanner) (*Process, error) {
	var (
		p              Process
		argvJSON       string
		cwd            sql.NullString
		endedAt        sql.NullTime
		exitCode       sql.NullInt64
		startedBy      sql.NullString
		bootID         sql.NullString
		startTimeTicks sql.NullInt64
		statusStr      string
	)
	if err := s.Scan(&p.PID, &p.SID, &p.NID, &argvJSON, &cwd, &p.StartedAt,
		&endedAt, &statusStr, &exitCode, &startedBy, &bootID, &startTimeTicks); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(argvJSON), &p.Argv); err != nil {
		return nil, fmt.Errorf("proc: parse argv: %w", err)
	}
	p.Cwd = cwd.String
	if endedAt.Valid {
		t := endedAt.Time
		p.EndedAt = &t
	}
	p.Status = State(statusStr)
	if exitCode.Valid {
		ec := int(exitCode.Int64)
		p.ExitCode = &ec
	}
	p.StartedByFP = startedBy.String
	p.BootID = bootID.String
	if startTimeTicks.Valid {
		p.StartTimeTicks = startTimeTicks.Int64
	}
	return &p, nil
}
