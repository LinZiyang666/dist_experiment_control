// Implementation of architecture G.1 — agent reconnect reconciliation.
//
// One register.req → one reconcileOnRegister call. The broker treats
// SQLite as authoritative for "what was supposed to be running" and
// the agent's LocalProcesses / LocalPorts arrays as authoritative for
// "what is actually running on the box right now". Disagreements are
// resolved per the table in architecture G.1; the resolution lands as
//
//   - SQLite UPDATEs (proc.MarkExited for missed-exit / agent-reported exit),
//   - audit.proc / audit.port records (kind=reconciled_closed / reconciled),
//   - directives in the register reply (drop_processes / revoke_ports)
//     for the agent to act on.
//
// PID-reuse defense:
//   - When the agent supplies (StartedAt, StartTimeTicks) for a
//     "running" entry, the broker compares against the persisted
//     processes.boot_id + processes.start_time_ticks: a mismatch
//     triggers the architecture G.1 PID-reuse handling — original
//     row → EXITED(rc=-1, reconciled_closed); the new pid is then
//     treated as an orphan and pushed into drop_processes so the
//     agent kills it (SIGTERM+5s+SIGKILL).
//   - When either side is missing the triple data (zero
//     start_time_ticks; empty boot_id) the broker falls back to the
//     pre-triple "same ULID = same process" accept path. Exec-style
//     children fall into this bucket — they have a sync lifecycle
//     and no agent-side persistence path that would let agent claim
//     a stale ULID, so the verification has nothing to compare.
//
// v1 caveat:
//   - Per-port LocalPort.TokenHash is matched against the row's
//     token_hash; sid/nid mismatch is treated as orphan (not a
//     missed-revoke), since v1 doesn't migrate port rows across
//     nodes.
package broker

import (
	"database/sql"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"slices"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
)

// intp returns a pointer to i (for the audit-tuple RC, which is *int so
// killed_orphan can carry no rc).
func intp(i int) *int { return &i }

// resolveReconcile is the PURE, full G.1 classifier (architecture §4.1 D4 +
// docs/reviews/d4-plan.md §0bis-A): given the broker's RUNNING/LOST procs, the
// agent's reported LocalProcesses/LocalPorts, and the session's port rows, it
// returns the COMPLETE leader-resolved reconcile result — the proc.ExitMark
// decisions (Body) AND the audit-replay tuples (Aux) — with ZERO side effects (no
// DB write, no audit emission). It is a faithful, side-effect-free copy of the
// EXACT transitions + audit reconcileOnRegister produces inline today:
//   - proc: PID-reuse -> MarkExited(-1) + reconciled_closed(rc=-1) AND the reused
//     pid is re-treated as an orphan (killed_orphan); agent exit -> MarkExited(rc) +
//     reconciled_closed(rc); unknown/missed -> MarkExited(-1) + reconciled_closed(-1);
//     accepted running -> NO mark, NO audit; orphan (agent-only running pid) ->
//     killed_orphan (NO rc, no mark).
//   - port: token unknown / port-mismatch / state!=ALLOCATED -> reconciled audit
//     (name/local_port from the AGENT report); ALLOCATED -> keep, NO audit.
//
// The D4 op path feeds this into proc.PlanReconcileBatch (Body+Aux). The live
// reconcileOnRegister keeps its inline application unchanged (zero risk to the
// e2e-covered G.1 path) and adopts this shared classifier at the D9 cutover.
// Equivalence (marks AND audit, compared as a set) is proven in
// reconcile_marks_test.go.
func resolveReconcile(sid, nid string, req proto.NodeRegisterReq, procs []proc.Process, portRows []port.Allocation, now time.Time) proc.ReconcileBatchInput {
	// h1 B1: normalize HERE, at the classifier boundary, so BOTH callers
	// (single-mode reconcileOnRegister and the cluster VerbReconcile forward
	// arm) bake homogeneous UTC ended_at text — see reconcileOnRegister for
	// the lexical-compare rationale.
	now = now.UTC()
	out := proc.ReconcileBatchInput{SID: sid}

	agentByPID := map[string]proto.LocalProcess{}
	for _, lp := range req.LocalProcesses {
		agentByPID[lp.PID] = lp
	}
	for _, p := range procs {
		if p.NID != nid {
			continue
		}
		if p.Status != proc.StateRunning && p.Status != proc.StateLost {
			continue
		}
		// Re-treat the reused pid as an orphan (mirrors the live re-add).
		// accepted: no mark, no audit
		lp, hasIt := agentByPID[p.PID]
		if hasIt {
			delete(agentByPID, p.PID)
		}
		mark, reused, closeRow := reconcileExitDecision(p, lp, hasIt, req.BootID, now)
		if closeRow {
			out.Marks = append(out.Marks, mark)
			out.ProcAudit = append(out.ProcAudit, proc.ReconProcAudit{NID: nid, PID: p.PID, Kind: "reconciled_closed", RC: intp(mark.ExitCode), Ts: mark.When})
		}
		if reused {
			agentByPID[p.PID] = lp
		}

	}
	// Orphans: agent claims a running pid the broker has no record of (or a PID-reuse
	// pid re-added above) -> killed_orphan (NO rc, no mark).
	for pid, lp := range agentByPID {
		if lp.State != "running" {
			continue
		}
		if lp.StartedUnacked {
			// C-5 (hot-update S1): the agent adopted this child but its courier
			// still owes us the proc.started — the row simply may not exist yet.
			// Killing it here is the G.1 false-orphan window drainStarted only
			// narrows. Neither accepted nor dropped; the courier keeps the
			// started pending. origin: hot-update reliable-exec plan §2.5.
			continue
		}
		out.ProcAudit = append(out.ProcAudit, proc.ReconProcAudit{NID: nid, PID: pid, Kind: "killed_orphan", RC: nil, Ts: now})
	}

	// Ports: classify the agent's re-presented tunnels against the port rows.
	portByHash := map[string]*port.Allocation{}
	for i := range portRows {
		if portRows[i].NID != nid {
			continue
		}
		portByHash[portRows[i].TokenHash] = &portRows[i]
	}
	for _, lp := range req.LocalPorts {
		alloc, ok := portByHash[lp.TokenHash]
		switch {
		case !ok, ok && alloc.Port != lp.Port, ok && alloc.State != port.StateAllocated:
			// Orphan / mismatch / dropped-while-offline -> reconciled audit + revoke
			// directive (the directive itself is live-path-only; the op bakes the audit).
			out.PortAudit = append(out.PortAudit, proc.ReconPortAudit{NID: nid, Port: lp.Port, Name: lp.Name, LocalPort: lp.LocalPort, Kind: "reconciled", Ts: now})
		default:
			// ALLOCATED + matching: keep, NO audit.
		}
	}
	return out
}

// reconcileOnRegister runs G.1 over (sid, nid) using req.LocalProcesses /
// req.LocalPorts as the agent's truth. Side effects:
//   - SQLite UPDATEs for processes that need to leave RUNNING/LOST,
//   - audit.proc / audit.port emissions for every change,
//
// Returns the directive arrays the broker writes into NodeRegisterResp.
func (b *Broker) reconcileOnRegister(sid, nid string, req proto.NodeRegisterReq) (
	accepted []string,
	reconciled []proto.ReconciledProc,
	keepPorts []int,
	revokePorts []int,
	dropProcesses []string,
) {
	// h1 B1 (plan critique-1): .UTC() so every G.1 missed-exit stamp — direct
	// markProcExited in single mode, LitTime-baked in the cluster batch — is
	// homogeneous with agent-published exit stamps ("+0000 UTC" text). The
	// pre-h1 raw local now baked zone-name + monotonic-suffix text into
	// ended_at, which SQLite's lexical TEXT compare (retention GC's leader
	// SELECT) cannot order against UTC rows.
	now := b.cfg.Now().UTC()
	if req.Incarnation != "" {
		if _, err := upgradeauthority.ReadCreation(b.read().SQL(), upgradeauthority.RequestGuard(sid, nid, req)); err != nil {
			return retainPendingExits(req.LocalProcesses), nil, nil, nil, nil
		}
	}

	// Reconcile only consumes RUNNING/LOST rows (see the explicit
	// filter below at `if p.Status != proc.StateRunning && p.Status
	// != proc.StateLost`). Reading EXITED rows from disk used to be
	// dead weight that grew with session history; the bounded helper
	// keeps reconnect O(active processes) regardless of session age.
	procs, err := proc.ListBySessionFiltered(b.cfg.DB, sid, proc.ListBySessionOpts{IncludeExited: false})
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcileOnRegister list procs", "err", err, "sid", sid)
		return retainPendingExits(req.LocalProcesses), nil, nil, nil, nil
	}

	agentByPID := map[string]proto.LocalProcess{}
	for _, lp := range req.LocalProcesses {
		agentByPID[lp.PID] = lp
	}

	adopted := adoptRowsCarriedAcrossARename(b, sid, nid, req, procs, agentByPID)
	// The cases still share their original failure/ACK rules below; one writer
	// now carries the same captured authority for every destructive G.1 mark.
	// origin: hot-update S8 plan §6.2 / P16
	// G.1 PID-reuse: the SQLite row and the agent
	// describe two different OS processes that just
	// happen to share the tether ULID. Original row
	// → EXITED(-1, reconciled_closed). The new
	// process is then treated as an orphan and
	// scheduled for kill below (we re-add it to
	// agentByPID so the orphan loop picks it up).
	// D9 round-1 BLOCKER: route through raft in cluster mode (was a direct
	// b.cfg.DB==RODB write that silently failed). Audit only on a committed
	// transition (don't emit reconciled_closed for a write that didn't land).
	// round-2 MINOR: on a close error STILL re-add the reused PID as an orphan
	// (the kill is independent of the old row's close) — preserving the pre-D9
	// swallow-and-schedule behavior; only the audit is gated on success.
	// Deliberately NOT added to `accepted` (contrast the two
	// F1 sites below): this pid is being re-treated as an
	// orphan on the next line, and one pid may never appear
	// in accepted AND dropProcesses at once. It also cannot
	// strand a courier exit: a tether PID is a ULID minted
	// once per run, so a pid the agent reports as RUNNING has
	// not been unregistered and therefore has no pending exit
	// event. (The "reuse" here is OS-pid recycling detected
	// via the boot/start-ticks triple, not tether-PID reuse.)
	// origin: h1 external review F1 (Blocker). A bare `continue`
	// left this pid in NEITHER response set, and the agent's
	// courier reads "absent from AcceptedProcesses" as SETTLED —
	// so it deleted the only copy of the real exit code while the
	// broker row stayed RUNNING. A transient SQLite/raft fault
	// thus manufactured exactly the zombie row this increment
	// exists to abolish.
	//
	// The write failed, so the row IS still RUNNING, and
	// `accepted` means precisely "the broker still believes this
	// pid is running". Saying so is the truth AND makes the
	// courier keep its pending exit for the next attempt.
	// Unknown state from agent — treat as missed-exit so we
	// don't leave the row stuck RUNNING forever.
	// Same F1 reasoning as the "exited" branch above: the write
	// failed, the row is still RUNNING, so report it as accepted
	// rather than letting silence read as "settled".
	// Broker thinks RUNNING/LOST but agent didn't list it →
	// missed-exit. Architecture G.1: rc=-1, audit.proc{kind:
	// reconciled_closed}.
	// Anything left in agentByPID is a pid the agent claims to be
	// running but the broker has no record of (or that we just
	// flagged as PID-reuse above) → orphan. Architecture G.1: v1
	// directs the agent to kill (SIGTERM+5s+SIGKILL).
	// The fail-closed gate counts rows THIS name owns, plus the ones just
	// carried across the rename. It must not be satisfied by a row that merely
	// sits under the previous name: that name's rows may belong to another
	// instance entirely, and "somebody has history here" is then true for a nid
	// that has none — which is precisely the evidence the gate exists to demand
	// before ordering a kill.
	//
	// gotcha #87: "history" means ANY row ever filed under this name, EXITED ones
	// included. The first version read it off `procs`, which is the RUNNING/LOST
	// list (the bounded read above, sized for the reconcile loop), so a node whose
	// jobs had all finished before a broker rollback had no history in the gate's
	// eyes and its real orphan — a process started after the restored bundle was
	// taken — was never ordered killed (drill 94 arm B, red on every sweep since
	// the gate landed on 2026-08-19, misfiled as "stale table"). A read failure is
	// fail-closed: no evidence, no kill.
	// C-5 (hot-update S1): same rule as the pure classifier above —
	// a pid whose proc.started is still in flight is not an orphan.
	batch := resolveReconcile(sid, nid, req, procs, nil, now)
	for _, mark := range batch.Marks {
		if err := markProcExitAuthority(b, ProcMarkExitedPayload(upgradeauthority.ExitRequest(sid, nid, req, mark.PID, mark.ExitCode, mark.When))); err != nil {
			b.cfg.Logger.Warn("broker: reconcile mark exited", "err", err, "pid", mark.PID)
			if lp, hasIt := agentByPID[mark.PID]; hasIt && lp.State != "running" {
				accepted = append(accepted, mark.PID)
			}
			continue
		}
		reconciled = append(reconciled, proto.ReconciledProc{PID: mark.PID, NewState: "EXITED", RC: mark.ExitCode})
		b.pubAuditProc(sid, "reconciled_closed", nid, mark.PID, nil, mark.ExitCode, mark.When)
	}
	for _, p := range procs {
		lp, hasIt := agentByPID[p.PID]
		if p.NID == nid && (p.Status == proc.StateRunning || p.Status == proc.StateLost) && hasIt && lp.State == "running" && !pidReused(req.BootID, p.BootID, lp.StartTimeTicks, p.StartTimeTicks) {
			accepted = append(accepted, p.PID)
		}
	}
	history, herr := proc.NodeHasHistory(b.read(), sid, nid)
	if herr != nil {
		b.cfg.Logger.Warn("broker: reconcile node history read failed — declining orphan kills this register", "sid", sid, "nid", nid, "err", herr)
	}
	sawAnyRow := adopted > 0 || anyRowMatches(procs, nid) || (herr == nil && history)
	knownPID := livePIDsByRow(procs, agentByPID, req.BootID)
	for _, audit := range batch.ProcAudit {
		if audit.Kind == "killed_orphan" && !knownPID[audit.PID] && sawAnyRow {
			dropProcesses = append(dropProcesses, audit.PID)
			b.pubAuditProc(sid, "killed_orphan", nid, audit.PID, nil, 0, now)
		}
	}

	// ---- ports -----------------------------------------------------
	keep, revoke, ok := reconcilePortsOnRegister(b, sid, nid, req, now)
	if !ok {
		// Port half of the same fail-closed rule — see reconcileReadFailure.
		return accepted, reconciled, nil, nil, dropProcesses
	}
	keepPorts, revokePorts = keep, revoke

	return
}

// reconcileReadFailure — WHY AN UNREADABLE AUTHORITY ISSUES NO DIRECTIVES.
//
// reconcileOnRegister answers two questions from committed state: which
// processes the broker still believes are running, and which tunnels it still
// backs. Both reads can fail transiently (SQLite busy, a raft read on a node
// that just lost leadership). Neither failure may be treated as an empty
// answer, because "empty" is itself a strong, destructive claim:
//
//   - empty process set ⇒ every agent-reported exit is ABSENT from
//     AcceptedProcesses, which the courier reads as SETTLED and deletes (the
//     only copy of the real exit code is gone), AND every live process is an
//     orphan to KILL. origin: h1 external review round 2, R1.
//   - empty port set ⇒ every tunnel the agent re-presents falls into the
//     "broker has no record of this" arm and is REVOKED, tearing down every
//     public expose in the session. origin: same round, extending R1 — whose
//     fix returned early only on the PROCESS read, while its regression test
//     closed the whole DB so both reads failed at once and the port path was
//     never exercised.
//
// The rule is therefore: issue no directive the read did not prove. Empty
// directives are inert on the agent — RevokePorts drives all teardown,
// KeepPorts has no consumer, and an unacknowledged started/exit event simply
// stays pending in the courier — so the next register reconciles against real
// state. Each half is scoped: a failed port read does NOT discard the process
// conclusions, which their own successful read did prove.
//
// retainPendingExits is the one directive that IS safe to emit blind: naming an
// exited pid in AcceptedProcesses says "the broker still believes this pid is
// running", which is exactly true when we could not observe otherwise, and it
// keeps the courier's pending exit alive for the retry.
func retainPendingExits(lps []proto.LocalProcess) []string {
	var accepted []string
	for _, lp := range lps {
		if lp.State == "exited" {
			accepted = append(accepted, lp.PID)
		}
	}
	return accepted
}

// pidReused implements the architecture G.1 (boot_id, pid,
// start_time_ticks) PID-reuse defense. Returns true ONLY when both
// sides supplied enough information to compare AND the comparison
// fails. Missing data on either side → returns false (preserve the
// pre-triple accept path: no false-positive kills when the agent or
// the SQLite row predates the triple capture, e.g. exec children).
// anyRowMatches reports whether ANY process row is filed under this name.
//
// It gates the orphan kill, and the reason is fail-closed: with no rows at all
// the broker cannot tell "these pids are orphans" from "I have simply never
// seen this name before" — and the second reading is exactly what a
// freshly-leased instance looks like. Ordering a kill on that evidence destroys
// live work irreversibly, while declining to kill merely leaves rows the next
// register reconciles. It is also the belt for PreviousNID: an agent too old to
// send that field still does not get its processes killed.
func anyRowMatches(procs []proc.Process, nid string) bool {
	return slices.ContainsFunc(procs, func(p proc.Process) bool { return p.NID == nid })
}

func pidReused(agentBootID, dbBootID string, agentTicks, dbTicks int64) bool {
	if agentBootID == "" || dbBootID == "" {
		return false
	}
	if agentTicks == 0 || dbTicks == 0 {
		return false
	}
	return agentBootID != dbBootID || agentTicks != dbTicks
}

// livePIDsByRow reports which of the agent's re-presented pids the broker
// already has a row for.

// An ORPHAN is a pid with NO ROW ANYWHERE — not a pid whose row is filed
// under another name.
//
// The two are easy to conflate and the difference is destructive. After a
// lease adoption an agent's own long-running processes may still be filed
// under the name it left, and the carry-across only rides the first register
// (PreviousNID is consumed once). A NATS blip later, that agent re-presents
// the same live pids with no PreviousNID: under a name-scoped test they are
// strangers, and because a leased instance is a full citizen there is
// usually some row under its lease name, so the fail-closed gate is open and
// the broker orders the operator's work killed.
//
// A row existing elsewhere means the broker HAS seen this process; what it
// has is a bookkeeping question about which name owns it, and the answer to
// a bookkeeping question is never SIGKILL. Genuinely unknown pids — the case
// the orphan arm exists for — still have no row at all and are still killed.
//
// PID-REUSE IS THE ONE EXCEPTION, and it is not an exception to the rule so
// much as a case where the row is about a DIFFERENT process. G.1 detects
// that the stored row and the agent's report describe two OS processes
// sharing one tether ULID; the old row is closed and the NEW process is
// deliberately re-added here to be killed. Its pid has a row, but not a row
// about it — so it is deliberately left OUT of the result.
func livePIDsByRow(procs []proc.Process, agentByPID map[string]proto.LocalProcess, bootID string) map[string]bool {
	known := make(map[string]bool, len(procs))
	for _, p := range procs {
		if p.Status != proc.StateRunning && p.Status != proc.StateLost {
			continue
		}
		if lp, reported := agentByPID[p.PID]; reported &&
			pidReused(bootID, p.BootID, lp.StartTimeTicks, p.StartTimeTicks) {
			continue
		}
		known[p.PID] = true
	}
	return known
}

// adoptRowsCarriedAcrossARename moves the process rows an agent carries with it
// through a lease adoption, and reports how many it took.
//
// OWNERSHIP IS THE CURRENT NAME. The previous name is only a place to LOOK FOR
// rows this agent re-presents — never a claim on rows it does not.
//
// Treating previousNID as ownership is a remote kill: after a rename the old
// name typically belongs to ANOTHER live instance (that is why this one was
// renamed), and every RUNNING row of that instance which this agent does not
// list would read as "gone" and be closed with ExitMark{-1}. One rename then
// marks a healthy sibling's processes EXITED while they are still running, and
// the sibling's own next register sees them as orphans and SIGKILLs the
// operator's work.
//
// MOVING, NOT REMEMBERING, is what makes this work for every later register
// too. A predicate only ever rescues rows on the FIRST register after adoption
// (PreviousNID is consumed once); the rows themselves never change name, so the
// second register would see the agent's own live processes as pids with no row
// — orphans — and order it to kill them.
func adoptRowsCarriedAcrossARename(b *Broker, sid, nid string, req proto.NodeRegisterReq,
	procs []proc.Process, agentByPID map[string]proto.LocalProcess) int {
	if req.PreviousNID == "" {
		return 0
	}
	adopted := 0
	// BY INDEX, because a successful move has to be reflected in the slice the rest of
	// reconcileOnRegister reads. The row now belongs to `nid`, and the ordinary
	// same-name arm below is exactly the treatment it should get — including the
	// PID-reuse check, which an adopted row needs as much as any other. Leaving the
	// slice saying PreviousNID made the row invisible for a whole register cycle and
	// forced this function to compensate by deleting the pid from agentByPID, i.e. by
	// hiding it from the orphan pass rather than by accounting for it.
	for i := range procs {
		p := &procs[i]
		if p.NID != req.PreviousNID {
			continue
		}
		if _, rePresented := agentByPID[p.PID]; !rePresented {
			continue
		}
		moved, err := refileReconcileAuthority(b, sid, nid, req, p.PID)
		if err != nil {
			b.cfg.Logger.Warn("broker: could not re-file an adopted process row",
				"err", err, "sid", sid, "pid", p.PID, "from", req.PreviousNID, "to", nid)
			continue
		}
		if !moved {
			// A zero-row UPDATE, reported as success by SQLite. Counting it would
			// hand `sawAnyRow` evidence of history this name does not have. The pid
			// stays in agentByPID on purpose: its row is still in `procs` under the
			// old name, so livePIDsByRow keeps it out of the orphan pass by the
			// honest route — a row that exists — instead of by deletion.
			b.cfg.Logger.Warn("broker: adopted process row did not move (already refiled, exited, or stale previous name)",
				"sid", sid, "pid", p.PID, "from", req.PreviousNID, "to", nid)
			continue
		}
		p.NID = nid
		adopted++
	}
	return adopted
}

// reconcilePortsOnRegister classifies the tunnels an agent re-presents against
// the port rows the broker still backs, returning the keep/revoke directives.
//
// ok=false means the authority could not be READ, which is never the same as
// "the agent holds nothing" — see reconcileReadFailure for why an empty answer
// is itself a destructive claim.
// A package-level function taking *Broker, not a method: the structural-budget
// ratchet pins this type's method count exactly. Same shape, same reason, as
// adjudicateLease and refileProc.
func reconcilePortsOnRegister(b *Broker, sid, nid string, req proto.NodeRegisterReq, now time.Time) (
	keepPorts []int, revokePorts []int, ok bool,
) {
	portRows, err := port.ListBySession(b.cfg.DB, sid)
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcileOnRegister list ports", "err", err, "sid", sid)
		return nil, nil, false
	}
	// Agent claims a tunnel the broker has no record of → orphan.
	// Token matches but port reassigned (shouldn't happen in
	// v1 — token rotates with port — defensive). Drop.
	// State is REVOKED or FREED — broker decided to drop while
	// agent was offline. Tell agent to tear it down now.
	portByHash := map[string]port.Allocation{}
	for _, a := range portRows {
		if a.NID == nid {
			portByHash[a.TokenHash] = a
		}
	}
	for _, lp := range req.LocalPorts {
		alloc, ok := portByHash[lp.TokenHash]
		if !ok || alloc.Port != lp.Port || alloc.State != port.StateAllocated {
			revokePorts = append(revokePorts, lp.Port)
			b.pubAuditPort(sid, "reconciled", nid, lp.Port, lp.Name, lp.LocalPort, "", now)
		} else {
			keepPorts = append(keepPorts, alloc.Port)
		}
	}

	// Ports the broker has ALLOCATED for this node but the agent didn't
	// re-present (state.json lost / never wrote): leave them ALLOCATED.
	// The standard 15min OFFLINE→REVOKED reconciler will catch them on
	// the normal timeline. v1 doesn't proactively REVOKE on register
	// because the more common case is "agent restarted before
	// state.json was first written" — punishing that with revocation
	// would force operators to re-expose by hand on every restart.
	return keepPorts, revokePorts, true
}

// The mode guard must select a writer before any authoritative mutation.
// origin: S7 internal review GATES-PROD F5
func refileReconcileAuthority(b *Broker, sid, nid string, req proto.NodeRegisterReq, pid string) (bool, error) {
	if db, ok := b.singleWriter(); ok && req.Incarnation != "" {
		moved, _, err := proc.RefileGuarded(db, upgradeauthority.RequestGuard(sid, nid, req), pid, req.PreviousNID, nid)
		return moved, err
	}
	return refileProc(b, sid, pid, req.PreviousNID, nid)
}

func planReconcileAuthority(db *sql.DB, p ReconcilePayload, procs []proc.Process, ports []port.Allocation, now time.Time) (*cluster.Command, error) {
	batch := resolveReconcile(p.SID, p.NID, p.Req, procs, ports, now)
	if p.Req.Incarnation == "" {
		return proc.PlanReconcileBatch(batch)
	}
	_, cmd, err := proc.PlanReconcileBatchGuarded(db, upgradeauthority.RequestGuard(p.SID, p.NID, p.Req), batch)
	return cmd, err
}

// A single decision owns PID reuse, old-wire defaults, and the exit timestamp;
// the callers still own their respective committed writes and courier ACKs.
// origin: hot-update S8 plan §6.2 / P16
func reconcileExitDecision(p proc.Process, lp proto.LocalProcess, hasIt bool, boot string, now time.Time) (proc.ExitMark, bool, bool) {
	reused := hasIt && lp.State == "running" && pidReused(boot, p.BootID, lp.StartTimeTicks, p.StartTimeTicks)
	mark := proc.ExitMark{PID: p.PID, ExitCode: -1, When: now}
	if hasIt && lp.State == "exited" {
		if lp.RC != nil {
			mark.ExitCode = *lp.RC
		}
		mark.When = exitStamp(lp, now)
	}
	return mark, reused, !hasIt || lp.State != "running" || reused
}

// A cold resumer captures before settlement, then retains that exact creation
// throughout the final cleanup. Re-reading SID after a barrier is too late.
// origin: hot-update S8 plan §6.2 / P16
func resumeSessionAuthority(b *Broker, d controlbudget.Deadline, stop <-chan struct{}, sid string) error {
	guard, err := upgradeauthority.Capture(b.read().SQL(), upgradeauthority.Guard{SID: sid})
	if err != nil {
		return err
	}
	return xferauth.ResumeDeletingSession(d, stop, b.read().SQL(), sid,
		func(d controlbudget.Deadline, stop <-chan struct{}) error {
			return settleSessionDeadline(b, d, stop, sid, false)
		},
		func(d controlbudget.Deadline, stop <-chan struct{}) error {
			return cascadeSessionAuthority(b, d, stop, guard)
		})
}

// A write reservation holds the original tombstone during external cleanup;
// the session owner retains the final guarded SQL transaction after it.
// origin: hot-update S8 plan §6.2 / P16
// origin: S7 internal review GATES-PROD F5
func cascadeSessionSingleAuthority(b *Broker, d controlbudget.Deadline, stop <-chan struct{}, guard upgradeauthority.Guard) error {
	db, ok := b.singleWriter()
	if !ok {
		return singleWriteRefusal()
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	creation, err := upgradeauthority.ReadCreation(tx, guard)
	if err != nil {
		return err
	}
	predicate, args, err := upgradeauthority.CreationPredicate(creation)
	if err != nil {
		return err
	}
	var sid string
	if err := tx.QueryRow(`UPDATE sessions SET state=state WHERE state='DELETING' AND `+predicate+` RETURNING sid`, args...).Scan(&sid); err != nil {
		return err
	}

	return finishSessionAuthority(b, d, stop, guard, func() error {
		if err := tx.Commit(); err != nil {
			return err
		}
		_, err := session.HardDeleteGuarded(db, guard)
		return err
	})
}

// The post-commit liveness write must not refresh a replacement that appeared
// after the identity proposal. Its actual UPDATE retains the original creation.
// origin: hot-update S8 plan §6.2 / P16
func heartbeatNodeAuthority(b *Broker, in node.RegisterInput, now time.Time) error {
	if in.Authority == nil {
		return node.Heartbeat(b.livenessDB(), in.SID, in.NID, now)
	}
	if in.Authority.ConfiguredNID != in.NID && in.Authority.ConfiguredNID != proto.BasenameOf(in.NID) {
		return upgradeauthority.Match("", "")
	}
	predicate, err := upgradeauthority.MutationPredicate(b.livenessDB(), upgradeauthority.SessionMutation{SID: in.SID, Authority: in.Authority}, cluster.LitText)
	if err != nil {
		return err
	}
	return b.livenessDB().QueryRow(`UPDATE nodes SET last_heartbeat_at=?,status='ONLINE' WHERE sid=? AND nid=? AND `+predicate+` RETURNING nid`, now.UTC(), in.SID, in.NID).Scan(new(string))
}
