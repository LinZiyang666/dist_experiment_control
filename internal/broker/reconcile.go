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
// v1 simplifications vs. the full G.1 spec:
//   - PID-reuse detection (boot_id + start_time_ticks triple) is NOT
//     implemented here. Process rows already carry boot_id but the
//     agent doesn't currently report start_time_ticks per-pid in
//     ProcStartedEvent, so we can't disambiguate "same pid, different
//     process" yet. The chaos cases the architecture tests for (kill
//     agent → restart → all RUNNING become EXITED-1) are covered by
//     the simpler rules below; PID-reuse paranoia is a P-future item.
//   - Per-port LocalPort.TokenHash is matched against the row's
//     token_hash; sid/nid mismatch is treated as orphan (not a
//     missed-revoke), since v1 doesn't migrate port rows across nodes.
package broker

import (
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
)

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

	procs, err := proc.ListBySession(b.cfg.DB, sid)
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
	// running but the broker has no record of → orphan. Architecture
	// G.1: v1 directs the agent to kill (SIGTERM+5s+SIGKILL).
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
