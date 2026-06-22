package proc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// Plan* are the D2 leader-side renderers for the process mutators (architecture
// §3.3 + docs/reviews/d2-plan.md §3). proc binds its timestamps RAW (Insert binds
// p.StartedAt, MarkExited binds `when` — neither calls .UTC()), so LitTime must
// receive the same raw time.Time. The ULID pid is minted agent-side (proc.NewPID
// at agent/exec.go) and arrives pre-formed; the leader bakes nothing for it, and
// oklog/ulid is never reachable from Apply.

// PlanInsert renders OpProcCreate. It performs Insert's node-FK check on the
// leader DB (returning ErrNodeMissing before proposing) and bakes the INSERT.
// start_time_ticks is baked as NULL when zero (matching nullableInt64) so the
// PID-reuse defense's exact-int64 compare is preserved.
func PlanInsert(db *sql.DB, p Process) (*cluster.Command, error) {
	if p.PID == "" {
		return nil, fmt.Errorf("proc: pid required")
	}
	argvJSON, err := json.Marshal(p.Argv)
	if err != nil {
		return nil, fmt.Errorf("proc: marshal argv: %w", err)
	}
	var found int
	switch err := db.QueryRow(`SELECT 1 FROM nodes WHERE sid=? AND nid=?`, p.SID, p.NID).Scan(&found); err {
	case nil:
	case sql.ErrNoRows:
		return nil, ErrNodeMissing
	default:
		return nil, fmt.Errorf("proc: plan lookup node: %w", err)
	}

	lits, err := cluster.LitTextAll(p.PID, p.SID, p.NID, string(argvJSON), p.Cwd, string(StateRunning), p.StartedByFP, p.BootID)
	if err != nil {
		return nil, fmt.Errorf("proc: plan insert literal: %w", err)
	}
	ticks := cluster.LitNull()
	if p.StartTimeTicks != 0 {
		ticks = cluster.LitInt(p.StartTimeTicks)
	}
	sql := `INSERT INTO processes(pid, sid, nid, argv, cwd, started_at, status, started_by_fp, boot_id, start_time_ticks)` +
		` VALUES (` + lits[0] + `, ` + lits[1] + `, ` + lits[2] + `, ` + lits[3] + `, ` + lits[4] + `, ` +
		cluster.LitTime(p.StartedAt) + `, ` + lits[5] + `, ` + lits[6] + `, ` + lits[7] + `, ` + ticks + `)`
	return cluster.NewCommand(cluster.OpProcCreate, cluster.Stmt(sql)), nil
}

// PlanMarkExited renders OpProcMarkExited. It returns ErrNotFound when no row
// carries the pid (a leader-side decision before proposing); an existing
// non-RUNNING row yields the idempotent UPDATE (WHERE status='RUNNING' makes Apply
// a no-op), matching MarkExited's RowsAffected==0 path. `when` is baked RAW.
func PlanMarkExited(db *sql.DB, pid string, exitCode int, when time.Time) (*cluster.Command, error) {
	var any int
	switch err := db.QueryRow(`SELECT 1 FROM processes WHERE pid=?`, pid).Scan(&any); err {
	case nil:
	case sql.ErrNoRows:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("proc: plan mark exited lookup: %w", err)
	}
	sql, err := markExitedSQL(pid, exitCode, when)
	if err != nil {
		return nil, err
	}
	return cluster.NewCommand(cluster.OpProcMarkExited, cluster.Stmt(sql)), nil
}

// markExitedSQL is the ONE canonical renderer for a MarkExited UPDATE, shared by
// PlanMarkExited (single exit) and PlanReconcileBatch (G.1 reconcile) so the two
// paths cannot drift into byte-different SQL (d2-plan §2).
func markExitedSQL(pid string, exitCode int, when time.Time) (string, error) {
	pidLit, err := cluster.LitText(pid)
	if err != nil {
		return "", fmt.Errorf("proc: mark exited literal: %w", err)
	}
	return `UPDATE processes SET status='EXITED', exit_code=` + cluster.LitInt(int64(exitCode)) +
		`, ended_at=` + cluster.LitTime(when) +
		` WHERE pid=` + pidLit + ` AND status='RUNNING'`, nil
}

// ExitMark is one resolved G.1 reconcile decision: a process to transition to
// EXITED. The resolver (the leader-side classifier extracted from
// reconcileOnRegister) produces the full set; PlanReconcileBatch bakes them.
type ExitMark struct {
	PID      string
	ExitCode int
	When     time.Time
}

// PlanReconcileBatch renders OpReconcileBatch — the whole G.1 reconcile result as
// ONE op. It sorts the marks by PID (ULID) ASC so the entry is byte-stable
// regardless of the Go-map iteration order the caller resolved them in
// (architecture §3.4/§4.1: "leader sorts by pid ASC into one ReconcileBatch"), and
// bakes each through the shared markExitedSQL renderer. Returns a nil command for
// an empty mark set (no-op: nothing to propose). The caller (resolver) owns
// de-duplication and the not-found decisions; the UPDATEs are idempotent
// (WHERE status='RUNNING').
func PlanReconcileBatch(marks []ExitMark) (*cluster.Command, error) {
	if len(marks) == 0 {
		return nil, nil
	}
	sorted := append([]ExitMark(nil), marks...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PID < sorted[j].PID })
	stmts := make([]cluster.Statement, len(sorted))
	for i, m := range sorted {
		sql, err := markExitedSQL(m.PID, m.ExitCode, m.When)
		if err != nil {
			return nil, err
		}
		stmts[i] = cluster.Stmt(sql)
	}
	return cluster.NewCommand(cluster.OpReconcileBatch, stmts...), nil
}
