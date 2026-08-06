package proc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
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
//
// IDEMPOTENT BY CONSTRUCTION (audit M8 / port-plan F1): the baked statement is
// INSERT OR IGNORE on the pid PRIMARY KEY, so a committed-but-ack-lost forwarder
// retry that re-proposes the SAME pid at a NEW raft index is a deterministic
// RowsAffected==0 no-op on EVERY replica — NOT a PK-violation that genericExecApplier
// would surface as a plain error, tripping fsm.Apply's retry→panic and bricking the
// whole cluster on log replay. The DB-state equivalence with the single-mode direct
// mutator holds (a duplicate insert leaves the original row unchanged either way); only
// the error return differs (live errors, op no-ops) — intentional, since the error path
// is the brick. VerbProcInsert forwards with reqID="" (the 0011 ledger does NOT dedup it),
// so this OR IGNORE is the SOLE idempotency anchor.
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
	sql := `INSERT OR IGNORE INTO processes(pid, sid, nid, argv, cwd, started_at, status, started_by_fp, boot_id, start_time_ticks)` +
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

// ReconProcAudit / ReconPortAudit are the apply-inert audit-replay tuples baked into
// OpReconcileBatch's Command.Aux (§4.1 D4). They capture the fields that live in NO
// persistent row — proc rc (from the agent's LocalProcesses) and port name/local_port
// (from LocalPorts) — so any new leader replays byte-identical audit reading ONLY the
// committed entry, never the live register request or a leader-local map. killed_orphan
// carries NO rc (RC=nil), matching pubAuditProc's kind-gating (rc only on
// exit/reconciled_closed). Ts is monotonic-stripped at bake time.
type ReconProcAudit struct {
	NID  string    `json:"nid"`
	PID  string    `json:"pid"`
	Kind string    `json:"kind"` // "reconciled_closed" | "killed_orphan"
	RC   *int      `json:"rc,omitempty"`
	Ts   time.Time `json:"ts"`
}

// ReconPortAudit is the port half of the reconcile audit (kind="reconciled").
type ReconPortAudit struct {
	NID       string    `json:"nid"`
	Port      int       `json:"port"`
	Name      string    `json:"name,omitempty"`
	LocalPort int       `json:"local_port,omitempty"`
	Kind      string    `json:"kind"` // "reconciled"
	Ts        time.Time `json:"ts"`
}

// ReconcileBatchInput is the FULL leader-resolved G.1 result for one (sid, nid)
// register: the proc MarkExited decisions (Body) + the audit-replay tuples (Aux).
// The broker-side resolver produces it; PlanReconcileBatch bakes it into ONE op.
type ReconcileBatchInput struct {
	SID       string
	Marks     []ExitMark
	ProcAudit []ReconProcAudit
	PortAudit []ReconPortAudit
}

// reconAuxV1 is the apply-inert Command.Aux payload for OpReconcileBatch: the
// total-ordered audit-replay tuples. genericExecApplier ignores Aux; only
// ReplayReconcileAudit (D5 publisher / D4 replay test) reads it.
type reconAuxV1 struct {
	SID       string           `json:"sid"`
	ProcAudit []ReconProcAudit `json:"proc,omitempty"`
	PortAudit []ReconPortAudit `json:"port,omitempty"`
}

// PlanReconcileBatch renders OpReconcileBatch — the whole G.1 reconcile result as
// ONE self-sufficient op (§4.1 D4). Body = the MarkExited UPDATEs, total-ordered by
// PID (ULID) ASC so the entry is byte-stable regardless of the Go-map iteration
// order the resolver produced them in, each baked through the shared markExitedSQL
// renderer (idempotent: WHERE status='RUNNING'). Aux = the ordered audit-replay
// tuples (proc by (PID,Kind) ASC — a PID-reuse PID carries BOTH reconciled_closed
// and killed_orphan, so Kind breaks the tie deterministically; port by Port ASC),
// monotonic-stripped. Returns a nil command only when there is NOTHING to do
// (no marks AND no audit) — an audit-only reconcile (orphans / port revokes, which
// have no DB mutation) still commits an entry so D5 can replay+publish its audit.
func PlanReconcileBatch(in ReconcileBatchInput) (*cluster.Command, error) {
	if len(in.Marks) == 0 && len(in.ProcAudit) == 0 && len(in.PortAudit) == 0 {
		return nil, nil
	}
	marks := append([]ExitMark(nil), in.Marks...)
	sort.Slice(marks, func(i, j int) bool { return marks[i].PID < marks[j].PID })
	stmts := make([]cluster.Statement, len(marks))
	for i, m := range marks {
		sql, err := markExitedSQL(m.PID, m.ExitCode, m.When)
		if err != nil {
			return nil, err
		}
		stmts[i] = cluster.Stmt(sql)
	}

	aux := reconAuxV1{SID: in.SID}
	aux.ProcAudit = append(aux.ProcAudit, in.ProcAudit...)
	sort.Slice(aux.ProcAudit, func(i, j int) bool {
		if aux.ProcAudit[i].PID != aux.ProcAudit[j].PID {
			return aux.ProcAudit[i].PID < aux.ProcAudit[j].PID
		}
		return aux.ProcAudit[i].Kind < aux.ProcAudit[j].Kind
	})
	aux.PortAudit = append(aux.PortAudit, in.PortAudit...)
	sort.Slice(aux.PortAudit, func(i, j int) bool {
		if aux.PortAudit[i].Port != aux.PortAudit[j].Port {
			return aux.PortAudit[i].Port < aux.PortAudit[j].Port
		}
		if aux.PortAudit[i].LocalPort != aux.PortAudit[j].LocalPort {
			return aux.PortAudit[i].LocalPort < aux.PortAudit[j].LocalPort
		}
		return aux.PortAudit[i].Name < aux.PortAudit[j].Name
	})
	// Strip the monotonic reading (review m6): time.Time.MarshalJSON already emits
	// only the wall RFC3339Nano (never the monotonic part), so this is NOT required
	// for the replayed-JSON byte-identity — it is defensive so any non-JSON time.Time
	// EQUALITY check (tests, future callers) sees the same value the wire carries.
	for i := range aux.ProcAudit {
		aux.ProcAudit[i].Ts = aux.ProcAudit[i].Ts.Round(0)
	}
	for i := range aux.PortAudit {
		aux.PortAudit[i].Ts = aux.PortAudit[i].Ts.Round(0)
	}

	cmd := cluster.NewCommand(cluster.OpReconcileBatch, stmts...)
	auxBytes, err := json.Marshal(aux)
	if err != nil {
		return nil, fmt.Errorf("proc: marshal reconcile aux: %w", err)
	}
	cmd.Aux = auxBytes
	return cmd, nil
}

// ReplayReconcileAudit deterministically reconstructs the audit records a
// ReconcileBatch entry stands for, reading ONLY the committed Command (its Aux) —
// no live request, no leader-local map, no DB (§4.1 D4). It reproduces
// pubAuditProc/pubAuditPort's record shapes exactly (incl killed_orphan's nil rc),
// in the baked total order, so two leaders replaying the SAME committed entry
// produce byte-identical audit. D4 only PROVES this (the byte-identical-replay
// test); the post-commit publish is D5. Threads no raftIndex and computes no dedup
// key (that is D5's raft_index:kind:seq). A v1-shaped entry never reaches here
// (decodeCommand rejects it as poison); an empty Aux yields empty records.
func ReplayReconcileAudit(cmd *cluster.Command) ([]schema.AuditProc, []schema.AuditPort, error) {
	if cmd == nil || cmd.Op != cluster.OpReconcileBatch {
		return nil, nil, fmt.Errorf("proc: ReplayReconcileAudit: not a ReconcileBatch command")
	}
	if len(cmd.Aux) == 0 {
		return nil, nil, nil
	}
	var aux reconAuxV1
	if err := json.Unmarshal(cmd.Aux, &aux); err != nil {
		return nil, nil, fmt.Errorf("proc: ReplayReconcileAudit decode aux: %w", err)
	}
	procs := make([]schema.AuditProc, 0, len(aux.ProcAudit))
	for _, t := range aux.ProcAudit {
		procs = append(procs, schema.AuditProc{
			V: schema.AuditSchemaVersion, Kind: t.Kind, Ts: t.Ts,
			Session: aux.SID, Node: t.NID, PID: t.PID, RC: t.RC,
		})
	}
	ports := make([]schema.AuditPort, 0, len(aux.PortAudit))
	for _, t := range aux.PortAudit {
		ports = append(ports, schema.AuditPort{
			V: schema.AuditSchemaVersion, Kind: t.Kind, Ts: t.Ts,
			Session: aux.SID, Node: t.NID, Port: t.Port, Name: t.Name, LocalPort: t.LocalPort,
		})
	}
	return procs, ports, nil
}

// PlanGCExited renders one OpProcGC chunk (h1 B1): up to `limit` EXITED rows
// whose end-of-life timestamp is older than `cutoff`, deleted by EXPLICIT pid
// keyset with the terminal-state guard re-asserted. Returns (nil, 0, nil)
// when nothing is due — Propose treats a nil planned command as a no-op, so
// an idle tick writes zero raft entries.
//
// TIMESTAMP CONTRACT (h1 plan critique-1): processes.ended_at is TEXT holding
// heterogeneous encodings — agent exits store modernc's bound-time form
// ("… +0000 UTC"), while pre-h1 G.1 reconcile baked LitTime of a raw local
// b.cfg.Now() (zone name + monotonic suffix). SQLite compares TEXT lexically,
// so `< ?` against those legacy rows is best-effort, not exact. That is why
// the retention COMPARISON stays HERE, on the leader, and the replicated
// DELETE keys off pids only: a replica never re-evaluates a timestamp, so the
// heterogeneity can skew WHEN a legacy row is collected (bounded by the
// generous retention), never WHETHER replicas agree. cutoff MUST be UTC
// (callers pass now.Add(-retention).UTC()); G.1 now bakes UTC going forward
// (reconcile.go), so new rows are homogeneous.
//
// COALESCE(ended_at, started_at): an EXITED row with NULL ended_at (a
// hand-repaired or crash-truncated row) must not be immortal.
//
// ROWS-CLOSE CONTRACT (h1 plan critique-2): fsm.db and n.db share ONE
// SetMaxOpenConns(1) pool. The keyset is materialized into a slice and
// rows.Close()+rows.Err() are checked BEFORE the Command is built — a plan
// closure that returned while holding *sql.Rows would deadlock fsm.Apply's
// Begin() forever.
func PlanGCExited(db *sql.DB, cutoff time.Time, limit int) (*cluster.Command, int, error) {
	rows, err := db.Query(
		`SELECT pid FROM processes
		  WHERE status='EXITED' AND COALESCE(ended_at, started_at) < ?
		  ORDER BY rowid LIMIT ?`,
		cutoff.UTC(), limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("proc: plan gc select: %w", err)
	}
	var pids []string
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			_ = rows.Close()
			return nil, 0, fmt.Errorf("proc: plan gc scan: %w", err)
		}
		pids = append(pids, pid)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("proc: plan gc rows: %w", err)
	}
	if len(pids) == 0 {
		return nil, 0, nil
	}
	lits, err := cluster.LitTextAll(pids...)
	if err != nil {
		return nil, 0, fmt.Errorf("proc: plan gc literal: %w", err)
	}
	sql := `DELETE FROM processes WHERE pid IN (` + strings.Join(lits, `, `) +
		`) AND status='EXITED'`
	return cluster.NewCommand(cluster.OpProcGC, cluster.Stmt(sql)), len(pids), nil
}
