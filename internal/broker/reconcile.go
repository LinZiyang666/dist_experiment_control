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
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
)

// resolveReconcileMarks is the PURE G.1 classifier (architecture §4.1 +
// docs/reviews/d2-plan.md §4 / 裁定 R2): given the broker's RUNNING/LOST procs and
// the agent's reported LocalProcesses, it returns the ordered set of proc.ExitMark
// decisions — the EXACT MarkExited transitions reconcileOnRegister applies inline
// today (PID-reuse -> -1, agent-reported exit -> rc, unknown -> -1, missed-exit ->
// -1; an accepted running proc yields no mark). It has ZERO side effects (no DB
// write, no audit). The D2 ReconcileBatch op path feeds these marks into
// proc.PlanReconcileBatch; under ops-only the live reconcileOnRegister keeps its
// inline application unchanged (zero risk to the e2e-covered G.1 path), and adopts
// this shared classifier at the D9 cutover. Equivalence is proven in
// reconcile_marks_test.go.
func resolveReconcileMarks(nid string, req proto.NodeRegisterReq, procs []proc.Process, now time.Time) []proc.ExitMark {
	agentByPID := map[string]proto.LocalProcess{}
	for _, lp := range req.LocalProcesses {
		agentByPID[lp.PID] = lp
	}
	var marks []proc.ExitMark
	for _, p := range procs {
		if p.NID != nid {
			continue
		}
		if p.Status != proc.StateRunning && p.Status != proc.StateLost {
			continue
		}
		lp, hasIt := agentByPID[p.PID]
		if !hasIt {
			// Broker thinks RUNNING/LOST but agent didn't list it -> missed-exit.
			marks = append(marks, proc.ExitMark{PID: p.PID, ExitCode: -1, When: now})
			continue
		}
		switch lp.State {
		case "running":
			// PID-reuse: same ULID, different OS process -> close the old row.
			// (A genuinely-accepted running proc yields NO mark.)
			if pidReused(req.BootID, p.BootID, lp.StartTimeTicks, p.StartTimeTicks) {
				marks = append(marks, proc.ExitMark{PID: p.PID, ExitCode: -1, When: now})
			}
		case "exited":
			rc := -1
			if lp.RC != nil {
				rc = *lp.RC
			}
			marks = append(marks, proc.ExitMark{PID: p.PID, ExitCode: rc, When: now})
		default:
			// Unknown agent state -> treat as missed-exit.
			marks = append(marks, proc.ExitMark{PID: p.PID, ExitCode: -1, When: now})
		}
	}
	return marks
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
	now := b.cfg.Now()

	// Reconcile only consumes RUNNING/LOST rows (see the explicit
	// filter below at `if p.Status != proc.StateRunning && p.Status
	// != proc.StateLost`). Reading EXITED rows from disk used to be
	// dead weight that grew with session history; the bounded helper
	// keeps reconnect O(active processes) regardless of session age.
	procs, err := proc.ListBySessionFiltered(b.cfg.DB, sid,
		proc.ListBySessionOpts{IncludeExited: false})
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcileOnRegister list procs", "err", err, "sid", sid)
	}

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
		lp, hasIt := agentByPID[p.PID]
		if hasIt {
			delete(agentByPID, p.PID)
			switch lp.State {
			case "running":
				if pidReused(req.BootID, p.BootID, lp.StartTimeTicks, p.StartTimeTicks) {
					// G.1 PID-reuse: the SQLite row and the agent
					// describe two different OS processes that just
					// happen to share the tether ULID. Original row
					// → EXITED(-1, reconciled_closed). The new
					// process is then treated as an orphan and
					// scheduled for kill below (we re-add it to
					// agentByPID so the orphan loop picks it up).
					_ = proc.MarkExited(b.cfg.DB, p.PID, -1, now)
					reconciled = append(reconciled, proto.ReconciledProc{
						PID: p.PID, NewState: "EXITED", RC: -1,
					})
					b.pubAuditProc(sid, "reconciled_closed", nid, p.PID, nil, -1, now)
					agentByPID[p.PID] = lp
					continue
				}
				accepted = append(accepted, p.PID)
				continue
			case "exited":
				rc := -1
				if lp.RC != nil {
					rc = *lp.RC
				}
				if err := proc.MarkExited(b.cfg.DB, p.PID, rc, now); err != nil {
					b.cfg.Logger.Warn("broker: reconcile mark exited", "err", err, "pid", p.PID)
					continue
				}
				reconciled = append(reconciled, proto.ReconciledProc{
					PID: p.PID, NewState: "EXITED", RC: rc,
				})
				b.pubAuditProc(sid, "reconciled_closed", nid, p.PID, nil, rc, now)
			default:
				// Unknown state from agent — treat as missed-exit so we
				// don't leave the row stuck RUNNING forever.
				_ = proc.MarkExited(b.cfg.DB, p.PID, -1, now)
				reconciled = append(reconciled, proto.ReconciledProc{
					PID: p.PID, NewState: "EXITED", RC: -1,
				})
				b.pubAuditProc(sid, "reconciled_closed", nid, p.PID, nil, -1, now)
			}
		} else {
			// Broker thinks RUNNING/LOST but agent didn't list it →
			// missed-exit. Architecture G.1: rc=-1, audit.proc{kind:
			// reconciled_closed}.
			if err := proc.MarkExited(b.cfg.DB, p.PID, -1, now); err != nil {
				b.cfg.Logger.Warn("broker: reconcile missed-exit", "err", err, "pid", p.PID)
				continue
			}
			reconciled = append(reconciled, proto.ReconciledProc{
				PID: p.PID, NewState: "EXITED", RC: -1,
			})
			b.pubAuditProc(sid, "reconciled_closed", nid, p.PID, nil, -1, now)
		}
	}

	// Anything left in agentByPID is a pid the agent claims to be
	// running but the broker has no record of (or that we just
	// flagged as PID-reuse above) → orphan. Architecture G.1: v1
	// directs the agent to kill (SIGTERM+5s+SIGKILL).
	for pid, lp := range agentByPID {
		if lp.State != "running" {
			continue
		}
		dropProcesses = append(dropProcesses, pid)
		b.pubAuditProc(sid, "killed_orphan", nid, pid, nil, 0, now)
	}

	// ---- ports -----------------------------------------------------
	portRows, err := port.ListBySession(b.cfg.DB, sid)
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcileOnRegister list ports", "err", err, "sid", sid)
	}
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
		case !ok:
			// Agent claims a tunnel the broker has no record of → orphan.
			revokePorts = append(revokePorts, lp.Port)
			b.pubAuditPort(sid, "reconciled", nid, lp.Port, lp.Name, lp.LocalPort, "", now)
		case alloc.Port != lp.Port:
			// Token matches but port reassigned (shouldn't happen in
			// v1 — token rotates with port — defensive). Drop.
			revokePorts = append(revokePorts, lp.Port)
			b.pubAuditPort(sid, "reconciled", nid, lp.Port, lp.Name, lp.LocalPort, "", now)
		case alloc.State == port.StateAllocated:
			keepPorts = append(keepPorts, alloc.Port)
		default:
			// State is REVOKED or FREED — broker decided to drop while
			// agent was offline. Tell agent to tear it down now.
			revokePorts = append(revokePorts, lp.Port)
			b.pubAuditPort(sid, "reconciled", nid, lp.Port, lp.Name, lp.LocalPort, "", now)
		}
	}

	// Ports the broker has ALLOCATED for this node but the agent didn't
	// re-present (state.json lost / never wrote): leave them ALLOCATED.
	// The standard 15min OFFLINE→REVOKED reconciler will catch them on
	// the normal timeline. v1 doesn't proactively REVOKE on register
	// because the more common case is "agent restarted before
	// state.json was first written" — punishing that with revocation
	// would force operators to re-expose by hand on every restart.

	return
}

// pidReused implements the architecture G.1 (boot_id, pid,
// start_time_ticks) PID-reuse defense. Returns true ONLY when both
// sides supplied enough information to compare AND the comparison
// fails. Missing data on either side → returns false (preserve the
// pre-triple accept path: no false-positive kills when the agent or
// the SQLite row predates the triple capture, e.g. exec children).
func pidReused(agentBootID, dbBootID string, agentTicks, dbTicks int64) bool {
	if agentBootID == "" || dbBootID == "" {
		return false
	}
	if agentTicks == 0 || dbTicks == 0 {
		return false
	}
	return agentBootID != dbBootID || agentTicks != dbTicks
}
