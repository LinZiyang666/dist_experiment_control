// Package proc tracks managed processes (architecture D.3).
//
// Lifecycle: RUNNING → EXITED. The LOST state lives at the node level
// (when the owning node enters OFFLINE) and is computed at read time
// from process row + node status — not stored explicitly here.
//
// agent owns the runtime knowledge ("process is running / has exited
// with rc=N"); tetherd owns the SQLite row. The path:
//
//	agent fork+exec → pub `ev.proc.started` → broker.Insert
//	agent reads child wait → pub `ev.proc.exit` → broker.MarkExited
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

	// ErrUnscopedExit is the refusal of an exit report that carries no session.
	//
	// origin: prerelease audit external review B-2. The forwarded path used to render
	// the pre-fence `WHERE pid=?` predicate for an empty sid, justified by "only a peer
	// broker inside the mTLS/system-account boundary can present one". That reasoned
	// about the TRANSPORT and not about where the DATA came from: an N-1 broker's own
	// input is an agent-published exit whose PID the agent chose, and the old binary
	// drops the subject's sid when it forwards. The old broker is a legitimate peer, so
	// the new leader cannot tell that confused-deputy request from a trustworthy
	// unscoped write — and one session's agent could retire another session's process.
	//
	// There is no fix that keeps the write: the old payload carries no session evidence
	// at all, and the old binary cannot be made to add any. So the exit is REFUSED. The
	// cost is bounded and self-healing — reconcileOnRegister compares the broker's
	// RUNNING rows against the agent's live pid list on every register, so a refused
	// exit is corrected at the agent's next reconnect rather than lost.
	ErrUnscopedExit = errors.New("proc: exit report carries no session")
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
// ErrNotFound if pid doesn't exist IN sid.
//
// THE sid PREDICATE IS A SECURITY FENCE, not a convenience filter.
//
// origin: prerelease audit broker-core #27, raised to BLOCKER by the main process.
// The predicate used to be `WHERE pid=? AND status='RUNNING'` — no sid, no nid — while
// the only thing that reaches it from the network is an agent publishing
// ev.<sid>.proc.exit. PIDs are ULIDs and appear in `tether ps` output, so a member of
// session A could publish, on its OWN legal subject, an exit for session B's pid and
// choose its exit code. That is a cross-session write escape, and it does not stop at
// a wrong row: the victim's next register finds no RUNNING row for a process that IS
// running, so G.1 reconcile classifies it as an orphan and the agent SIGTERMs then
// SIGKILLs the operator's live job.
//
// WHY sid AND NOT (sid, nid). A lease rename moves a node's identity while its
// processes keep running; the refile UPDATE (PlanRefileStatements) is itself keyed
// `WHERE sid=? AND pid=? AND nid=?`, so nid legitimately changes underneath a live
// process and an exit arriving mid-rename would be rejected by a (sid,nid) fence.
// sid never changes for a row, so it fences without breaking anything.
//
// The check costs NOTHING: it is a narrowing of an UPDATE predicate the statement
// already had, on a column already in the row.
func MarkExited(db *sql.DB, pid, sid string, exitCode int, when time.Time) error {
	// FAIL CLOSED ON A MISSING sid, loudly, rather than degrading to the predicate
	// this fence replaced.
	//
	// PlanMarkExited has an N-1 escape hatch for an empty sid because a broker on the
	// previous release forwards without the field. THIS writer has no such caller: it
	// runs only in single mode, where nothing is forwarded and every call site
	// (exec.go, reconcileOnRegister) has the sid in hand. So an empty sid here can
	// only be a future call site that forgot to pass one — and the failure mode of
	// silently accepting it is that the fence disappears for that path while every
	// test in exit_session_fence_test.go still passes, because they all pass a sid.
	if sid == "" {
		return fmt.Errorf("proc: mark exited: empty sid — this writer is single-mode only and all of " +
			"its call sites have the session; the empty-sid N-1 hatch exists ONLY on the forwarded " +
			"cluster path (PlanMarkExited)")
	}
	res, err := db.Exec(`
		UPDATE processes
		SET status='EXITED', exit_code=?, ended_at=?
		WHERE pid=? AND sid=? AND status='RUNNING'
	`, exitCode, when, pid, sid)
	if err != nil {
		return fmt.Errorf("proc: mark exited: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either unknown pid or already non-RUNNING. Distinguish.
		// Audit shard 03 F6: previously the SELECT err was discarded
		// — a transient DB error would silently report success while
		// SQLite still says RUNNING but audit.proc{exit} got
		// published. Surface non-ErrNoRows errors so the caller
		// knows the row is in an unknown state.
		//
		// The existence probe is sid-scoped TOO, and that is load-bearing rather than
		// symmetry: an unscoped probe would find the victim's row, conclude "already
		// EXITED, idempotent no-op" and return nil — reporting SUCCESS to the very
		// caller the UPDATE predicate just refused, which in exec.go publishes an
		// audit.proc{exit} for a process that is still running. Scoped, a foreign pid
		// is indistinguishable from an unknown one: ErrNotFound, no audit, and the
		// caller's terminal ack tells the sender nothing about whether the pid exists.
		//
		// THAT LAST SENTENCE IS ABOUT THIS FUNCTION, and round 2 (E3) caught it being
		// read as a claim about the SYSTEM. It was not true of the system: the only
		// caller, handleProcEvent, answered its ack from an UNSCOPED pre-read that ran
		// before this code was ever reached, so the indistinguishability this comment
		// establishes was handed straight back one frame up. Both pre-reads are scoped
		// now (GetInSession). What is NOT closed, and cannot be here, is that
		// processes.pid is a GLOBAL primary key: a forged `started` for a pid that
		// exists in another session fails its INSERT where an unused pid succeeds, so
		// pid-existence remains observable to a caller willing to write. Closing that
		// means re-keying the table on (sid, pid) — a migration, not a predicate.
		var any int
		switch err := db.QueryRow(`SELECT 1 FROM processes WHERE pid=? AND sid=?`, pid, sid).Scan(&any); {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNotFound
		case err != nil:
			return fmt.Errorf("proc: existence check after no-op update: %w", err)
		}
		// Already EXITED (or LOST). Idempotent — treat as no-op.
	}
	return nil
}

// GetInSession returns the row for pid WITHIN sid, or ErrNotFound.
//
// origin: prerelease audit round 2, E3. MarkExited's existence probe above is
// sid-scoped so that "somebody else's pid" and "no such pid" are indistinguishable
// to a caller, and the comment there says exactly that. It was only true of the
// WRITE. Both of the broker's audit-dedup pre-reads (internal/broker/exec.go)
// called the UNSCOPED Get first and answered from it, so a member of session
// `other` could publish on its own legal `ev.other.other-1.<pid>.started` subject
// and read the ack: `already_recorded` for a pid that exists in ANY session,
// `recorded` / `node_missing` for one that exists nowhere. That is a complete
// cross-session pid-existence oracle over precisely the pids the fence was added
// to hide, and it needed no write at all.
//
// A separate constructor rather than a parameter on Get: Get's remaining callers
// are storage-level tests that legitimately want "the row, whatever session it is
// in", and folding the two would make every one of them assert something weaker
// than it does today.
func GetInSession(db *sql.DB, sid, pid string) (*Process, error) {
	row := db.QueryRow(`
		SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
		       started_by_fp, boot_id, start_time_ticks
		FROM processes
		WHERE pid = ? AND sid = ?
	`, pid, sid)
	return scanProcess(row)
}

// Get returns the row for pid, IGNORING which session owns it.
//
// Not for request handling: answering a request from this makes the reply an
// existence oracle across every session on the broker (round 2, E3). A handler
// that has a sid — and every one of them does, it is in the subject — wants
// GetInSession.
func Get(db *sql.DB, pid string) (*Process, error) {
	row := db.QueryRow(`
		SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
		       started_by_fp, boot_id, start_time_ticks
		FROM processes
		WHERE pid = ?
	`, pid)
	return scanProcess(row)
}

// ListBySessionOpts narrows the result of the per-session process list.
//
//   - IncludeExited=false (default) returns only rows whose storage
//     `status` column equals 'RUNNING'. LOST is read-derived in the
//     handler from a RUNNING row + an OFFLINE node — no SQLite row
//     is ever stored with status='LOST' (Insert writes 'RUNNING',
//     MarkExited writes 'EXITED'; the schema CHECK constraint
//     allows 'LOST' for forward compatibility but nothing in the
//     tree writes it). The storage-level equality filter
//     `status='RUNNING'` therefore captures every row that could
//     become RUNNING-or-LOST in the response, without missing an
//     active process.
//   - Limit > 0 caps the returned slice. Rows are ordered by
//     started_at DESC, so the newest are kept; the rest are
//     silently dropped. Limit=0 returns the whole filtered set.
//
// Used by `tether ps` (default RUNNING-only) and by G.1 reconcile.
// ListBySession remains for out-of-tree callers that need the full
// RUNNING+EXITED view.
type ListBySessionOpts struct {
	IncludeExited bool
	Limit         int
}

// ListBySessionFiltered runs a narrowed SELECT against the session.
//
// Default (IncludeExited=false) uses the (sid, status, started_at)
// composite from migration 0005 — equality on the first two
// columns, trailing started_at DESC matches the ORDER BY, so
// EXPLAIN QUERY PLAN is "SEARCH ... USING INDEX ..." with no
// "USE TEMP B-TREE FOR ORDER BY".
//
// IncludeExited=true uses the (sid, started_at) composite from
// migration 0005 with the LIMIT short-circuiting the index walk.
func ListBySessionFiltered(db *sql.DB, sid string, opts ListBySessionOpts) ([]Process, error) {
	q := `SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
	             started_by_fp, boot_id, start_time_ticks
	      FROM processes WHERE sid = ?`
	args := []any{sid}
	if !opts.IncludeExited {
		// Equality, not inequality — see ListBySessionOpts doc.
		// The (sid, status, started_at DESC) index covers this fully.
		q += ` AND status = 'RUNNING'`
	}
	q += ` ORDER BY started_at DESC`
	if opts.Limit > 0 {
		q += ` LIMIT ?`
		args = append(args, opts.Limit)
	}
	rows, err := db.Query(q, args...)
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

// ListBySession returns every row for sid in (sid, started_at desc)
// order. Pre-existing API; kept for out-of-tree callers that need
// the full RUNNING+EXITED+LOST view. In-tree callers have been
// migrated to ListBySessionFiltered.
func ListBySession(db *sql.DB, sid string) ([]Process, error) {
	return ListBySessionFiltered(db, sid, ListBySessionOpts{IncludeExited: true})
}

// CountBySession reports how many rows ListBySessionFiltered would
// match WITHOUT its Limit — the `ProcsTotal` the ps reply surfaces so
// a truncated view can say so (h1 A1; symmetric with
// port.CountBySession). Second, non-transactional read by design; the
// ±1 skew against the list is cosmetic and documented on proto.PsResp.
func CountBySession(db *sql.DB, sid string, includeExited bool) (int, error) {
	q := `SELECT COUNT(*) FROM processes WHERE sid = ?`
	if !includeExited {
		// Equality, not inequality — same rationale as ListBySessionOpts.
		q += ` AND status = 'RUNNING'`
	}
	var n int
	if err := db.QueryRow(q, sid).Scan(&n); err != nil {
		return 0, fmt.Errorf("proc: count: %w", err)
	}
	return n, nil
}

// GCExited deletes EXITED rows whose ended_at is older than cutoff.
// Returns the number of rows removed (for log lines / metrics).
//
// Long-term audit lives in JetStream `history-<sid>`, which is
// byte-bounded (MaxBytes=1 GiB per session, MaxAge=0, DiscardNew —
// see internal/jsstream/jsstream.go). The SQLite processes table
// only needs to keep what the operational surface (`tether ps -a`,
// G.1 reconcile, agent crash recovery) reads in the near term.
//
// Uses migration 0005's idx_processes_status_endedat index: status
// equality on the leading column, then range scan on ended_at over
// a tight contiguous prefix.
func GCExited(db *sql.DB, cutoff time.Time) (int64, error) {
	// COALESCE + UTC keep this symmetric with its three siblings
	// (PlanGCExited, port.GCTerminated, port.PlanGCTerminated): an EXITED row
	// with a NULL ended_at (crash-truncated / hand-repaired) must not be
	// immortal, and the stored column is heterogeneous TEXT compared
	// lexically — see PlanGCExited's timestamp contract. Before this the
	// single-mode path diverged from the cluster path on exactly that row
	// class (internal review, raft lens).
	res, err := db.Exec(
		`DELETE FROM processes
		  WHERE status = 'EXITED' AND COALESCE(ended_at, started_at) < ?`,
		cutoff.UTC(),
	)
	if err != nil {
		return 0, fmt.Errorf("proc: gc exited: %w", err)
	}
	return res.RowsAffected()
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

// Refile moves one RUNNING process row to a different node name.
//
// It exists for lease adoption, which renames a LIVE agent: the processes it
// manages keep running, but their rows are filed under the name it had before.
// Carrying that history forward with a "the old name is also mine" predicate is
// not enough and not safe — the old name may already belong to ANOTHER live
// instance, and a predicate cannot tell whose row it is looking at. So the
// agent re-presents its own pids on its first register under the new name, and
// each one that it genuinely re-presents is MOVED here.
//
// Moving, rather than remembering, is what bounds this to a single hop: after
// the move the rows are ordinary rows of the new name, so every later register
// reconciles them without any memory of the rename at all.
//
// Scoped to (sid, pid) and to rows still filed under `from`, so it cannot touch
// a row another instance has since re-filed.
// It reports whether a row actually MOVED. A nil error alone does not mean it did:
// the predicate is four-way (sid, pid, from-nid, RUNNING) and any of them can fail to
// match — the row was already refiled by a concurrent register, it exited between the
// read and this write, or the caller's `from` is stale. SQLite calls all of those a
// perfectly successful zero-row UPDATE.
//
// origin: prerelease audit broker-proc/L3-F1. The caller counted every nil return as an
// adopted row, and that count is what satisfies reconcileOnRegister's `sawAnyRow` gate —
// the fail-closed check that refuses to orphan-kill on a node name with no process
// history. A phantom adoption defeats the one gate standing between a stale rename and
// SIGKILL on the operator's running work, so "did it move" has to be an ANSWER, not an
// inference from the absence of an error.
func Refile(db *sql.DB, sid, pid, from, to string) (bool, error) {
	res, err := db.Exec(
		`UPDATE processes SET nid = ? WHERE sid = ? AND pid = ? AND nid = ? AND status = 'RUNNING'`,
		to, sid, pid, from)
	if err != nil {
		return false, err
	}
	n, aerr := res.RowsAffected()
	if aerr != nil {
		// Cannot tell. Report NOT moved: the caller's only use of a true is to
		// unlock a kill, so an unknown must never read as evidence.
		return false, fmt.Errorf("proc: refile rows affected: %w", aerr)
	}
	return n > 0, nil
}
