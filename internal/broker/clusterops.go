package broker

import (
	"database/sql"
	"fmt"

	"github.com/LinZiyang666/tether/internal/adminsock"
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
	ops, err := deriveClusterOps(b.admin.node.RODB(), req.OpsNode)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}
	return adminsock.Response{Op: req.Op, OK: true, Ops: ops}
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
		e.Resume = "AddVoter pending — the leader-startup reconciliation completes it; re-run `cluster add` with the same token if it stalls"
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
		e.Resume = "AddVoter failed — see last_error; `cluster remove " + id + " --force` to clear, then re-add"
	case "DRAINING":
		e.Kind, e.State = "drain", "draining"
		e.Resume = "drain in progress — `cluster drain " + id + " --abort` to cancel, or `--retire` to complete removal"
	case "RETIRING":
		e.Kind, e.State = "retire", "retiring"
		e.Resume = "retire in progress — completes when its exposes finish migrating + streams reach target"
	default:
		e.Kind, e.State = "add", phase
	}
	return e
}
