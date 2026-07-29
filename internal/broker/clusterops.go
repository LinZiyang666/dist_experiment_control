package broker

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// clusterops.go — B7 DOC#2: the READ-ONLY membership-operations view. It DERIVES each op from the
// replicated cluster_nodes columns (phase / added_at / phase_changed_at / voter_add_error) rather
// than tracking a separate state — so it can NEVER diverge from the phase SSOT across a leader
// loss (there is no second written state). Leader-agnostic: any broker reads its replica via RODB().
//
// (Main-process adjudication of b7-plan §2 DOC#2: shipped as a derived read view — no migration,
// no fold into the membership Commands, zero divergence risk — instead of a persistent per-op
// ledger table. The persistent retire-then-re-add history is an additive post-v2 follow-up.)

func (b *clusterAdminBackend) handleClusterOps(req adminsock.Request) adminsock.Response {
	ro := b.admin.node.RODB()
	// C4: the REAL operation log is authoritative; the phase-derived view is a FALLBACK for legacy
	// rows (a `cluster add` one-shot / a pre-C4 membership state) with no op record.
	realOps, err := cluster.RecentOperations(ro, 200)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}
	covered := map[string]bool{}
	var out []adminsock.ClusterOpEntry
	for i := range realOps {
		op := realOps[i]
		covered[op.TargetNode] = true
		if req.OpsNode != "" && req.OpsNode != op.OpID && req.OpsNode != op.TargetNode {
			continue
		}
		out = append(out, opEntryFromOperation(op))
	}
	// Phase-derived fallback for nodes WITHOUT a real op (so `ops ls` still lists every membership state).
	derived, err := deriveClusterOps(ro, req.OpsNode)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}
	for _, d := range derived {
		if !covered[d.NodeID] {
			out = append(out, d)
		}
	}
	return adminsock.Response{Op: req.Op, OK: true, Ops: out}
}

// opEntryFromOperation maps a real cluster_operations row to the wire entry, mapping the rich
// op_state down to the STABLE v1 State vocab (so a monitor that negotiated schema_version 1 keeps
// working) while exposing the rich OpState + the durable timeline additively.
func opEntryFromOperation(op cluster.Operation) adminsock.ClusterOpEntry {
	e := adminsock.ClusterOpEntry{
		NodeID: op.TargetNode, Kind: op.Kind, OpID: op.OpID, TargetEnd: op.TargetNode, OpState: op.OpState,
		Terminal: op.Terminal, CreatedAt: op.CreatedAt, StartedAt: op.CreatedAt, UpdatedAt: op.UpdatedAt,
		LastError: op.LastError,
	}
	switch op.OpState {
	case cluster.OpStateServing, cluster.OpStateRetired:
		e.State = "done"
	case cluster.OpStateRetireFailed, cluster.OpStateAborted:
		e.State = "failed"
		e.Resume = "operation ended: " + op.OpState + " — see last_error; `cluster ops show " + op.OpID + "`"
	case cluster.OpStateFSFinalized:
		e.State = "done"
	case cluster.OpStateFSGhostLeft:
		// batch C: NOT the generic failed text. The remedy is specific and the consequence of skipping
		// it is a survivor whose JetStream never comes back, so name both.
		e.State = "failed"
		e.Resume = "force-single finalize could not remove every abandoned roster row — run " +
			// origin: batch-c internal review sweep-F1 — the flag is NOT optional. `remove` without
			// --manual returns a usage error that points the operator at `cluster retire <id>`, which is
			// the one command that must never be aimed at a ghost (it has no raft role and is absent
			// from the configuration).
			"`tether cluster recovery node remove <id> --manual` for each id in last_error. Until they are gone, " +
			"`tether cluster reconcile nats --to-standalone` refuses and this broker's JetStream stays 503."
	case cluster.OpStateFSPrunePending:
		e.State = "in_progress"
		e.Resume = "retrying the roster prune left over from an online force-single; `cluster ops show " + op.OpID + "`"
	case cluster.OpStateBlocked:
		e.State = "stalled"
		e.Resume = "blocked awaiting operator — `cluster ops confirm " + op.OpID + "` (or `cluster ops abort " + op.OpID + "`)"
	case cluster.OpStateDrainRequested, cluster.OpStateNoNewHome, cluster.OpStateRehomeExposes,
		cluster.OpStateStreamsAtTarget, cluster.OpStateSeedWithdrawn, cluster.OpStateLeaderTransferred:
		e.State = "draining"
	case cluster.OpStateRaftRemoved:
		e.State = "retiring"
	default:
		e.State = "in_progress"
	}
	if e.Resume == "" && !op.Terminal {
		e.Resume = "in flight — `cluster ops show " + op.OpID + "` for the timeline; `cluster ops abort " + op.OpID + "` to cancel"
	}
	var tl []adminsock.ClusterOpEvent
	if err := json.Unmarshal([]byte(op.Timeline), &tl); err == nil {
		// the op timeline uses compact keys {s,t,e}; re-map to the wire {state,at,note}.
		var compact []struct {
			S, T, E string
		}
		if json.Unmarshal([]byte(op.Timeline), &compact) == nil {
			e.Timeline = nil
			for _, c := range compact {
				e.Timeline = append(e.Timeline, adminsock.ClusterOpEvent{State: c.S, At: c.T, Note: c.E})
			}
		}
	}
	return e
}

// deriveClusterOps reads cluster_nodes and maps each row to a membership operation. If node != ""
// it scopes to that one node (for `cluster ops show <node>`).
func deriveClusterOps(ro *sql.DB, node string) ([]adminsock.ClusterOpEntry, error) {
	q := `SELECT node_id, phase, COALESCE(added_at,''), COALESCE(phase_changed_at,''), COALESCE(voter_add_error,'')
	      FROM cluster_nodes`
	args := []any{}
	if node != "" {
		q += ` WHERE node_id = ?`
		args = append(args, node)
	}
	q += ` ORDER BY node_id`
	rows, err := ro.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("cluster ops: read roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []adminsock.ClusterOpEntry
	for rows.Next() {
		var id, phase, addedAt, changedAt, addErr string
		if err := rows.Scan(&id, &phase, &addedAt, &changedAt, &addErr); err != nil {
			return nil, fmt.Errorf("cluster ops: scan: %w", err)
		}
		out = append(out, opFromPhase(id, phase, addedAt, changedAt, addErr))
	}
	return out, rows.Err()
}

// opFromPhase maps a cluster_nodes row to a derived ClusterOpEntry. For an add the op began at
// added_at; for a drain/retire it began at phase_changed_at (Stage-C m11: an add-time StartedAt
// would misreport the timeline the view exists to provide).
func opFromPhase(id, phase, addedAt, changedAt, addErr string) adminsock.ClusterOpEntry {
	started := addedAt
	if phase == "DRAINING" || phase == "RETIRING" {
		if changedAt != "" {
			started = changedAt
		}
	}
	e := adminsock.ClusterOpEntry{NodeID: id, Phase: phase, StartedAt: started, UpdatedAt: changedAt, LastError: addErr}
	switch phase {
	case "VOTER":
		e.Kind, e.State = "add", "done"
	case "JOIN_VERIFIED_PENDING_VOTER":
		e.Kind, e.State = "add", "in_progress"
		e.Resume = "AddVoter pending — the leader-startup reconciliation completes it; re-run `cluster join approve <bundle>` if it stalls"
	case "CATCHING_UP":
		e.Kind = "add"
		if addErr == "catch_up_stalled" {
			e.State = "stalled"
			e.Resume = "catch-up stalled — check the joiner's connectivity to the leader; `cluster status` shows the lag"
		} else {
			e.State = "in_progress"
		}
	case "VOTER_ADD_FAILED":
		e.Kind, e.State = "add", "failed"
		e.Resume = "AddVoter failed — see last_error; `cluster recovery node remove " + id + " --manual --force` to clear, then re-join with `cluster join prepare`/`approve`"
	case "DRAINING":
		e.Kind, e.State = "drain", "draining"
		e.Resume = "drain in progress — `cluster drain " + id + " --abort` to cancel, or `cluster retire " + id + "` to complete removal"
	case "RETIRING":
		e.Kind, e.State = "retire", "retiring"
		e.Resume = "retire in progress — completes when its exposes finish migrating + streams reach target"
	default:
		e.Kind, e.State = "add", phase
	}
	return e
}
