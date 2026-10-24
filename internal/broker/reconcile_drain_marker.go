package broker

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// reconcile_drain_marker.go (batch C / C1 companion) — the healer AbortOp's comment has been
// promising since C4.
//
// AbortOp says the membership "stays whatever the gates left it; reconcile/doctor heals". No such
// healer existed. This is it — and it is deliberately much narrower than the refactor roadmap
// suggested, because the roadmap's literal predicates would each break something.
//
// WHAT THE ROADMAP ASKED FOR, AND WHY IT IS WRONG AS WRITTEN
// ----------------------------------------------------------
// (1) "marker raised + no non-terminal retire op + phase VOTER ⇒ clear the marker".
//
//	That shape is ALSO the shape of a raw `cluster drain` that is halfway done. DrainNode raises the
//	marker (step 1) long before it flips the phase to DRAINING (step 3), creates NO operation row at
//	any point, and its documented failure exit is ErrDataPlaneNotConverged — whose message tells the
//	operator to "re-run to keep waiting". So between two operator attempts the state sits at exactly
//	(marker, VOTER, no op) indefinitely. Clearing there would be worse than doing nothing twice over:
//	the marker and its broker_draining alert are the ONLY status-visible evidence of that
//	half-finished drain (the phase still reads VOTER), and pickProxyRehomeTarget orders candidates by
//	FEWEST proxy homes — so the node whose exposes were just migrated away is the one it picks first.
//	The healer would silently re-fill the node being drained.
//
// (2) "phase DRAINING but no active op ⇒ record INCONSISTENT".
//
//	That is the NORMAL, indefinitely-stable end state of a successful `cluster drain <node>`:
//	DrainNode's success path is setPhase(DRAINING) and return, with no op row ever created. Marking
//	it INCONSISTENT would make `cluster doctor` exit 64 on every correctly drained node, because
//	roster_consistency is a FATAL check.
//
// WHAT THIS DOES INSTEAD
// ----------------------
// CLEAR only the case that is provably an orphan: a `draining:<node>` marker whose node has NO
// cluster_nodes row at all. Nothing can be draining a node that does not exist. That state is
// reachable and not hypothetical — driveRetire clears the marker with `_ = a.node.Propose(...)`, the
// error discarded, and then removes the roster row and goes terminal, so one failed clear leaves a
// permanent broker_draining alert for a node nobody can even name any more.
//
// REPORT (never clear) the corrected version of (2): phase DRAINING with the marker MISSING and no
// non-terminal op. That combination is not a legal transient — all three producers write the marker
// before the phase and clear it after restoring the phase — so it means a drain died between its own
// steps. Reporting is handled at render time (see the Inconsistent predicate in clusterstatus.go);
// this pass writes nothing for it.
//
// LEAVE ALONE the half-finished raw drain (marker + VOTER + no op), as a permanent decision.

// reconcileDrainMarkers clears broker_draining markers for nodes with no roster row.
//
// authorityLeader: it Proposes, and it needs a leader-consistent view of both cluster_meta and
// cluster_nodes. It calls only the EXISTING idempotent command path PlanClusterDrainSet(node, nil) —
// the same one AbortDrain and the retire ladder use — so it invents no policy, which is the registry's
// one-vote-veto requirement.
func (b *Broker) reconcileDrainMarkers(_ context.Context, _ time.Time) error {
	if b.cl == nil || b.cl.node == nil || b.cl.admin == nil {
		return nil
	}
	// Same caught-up gate the grow-lock reaper uses: a leader still replaying its committed tail can
	// read "no roster row" for a row it simply has not applied yet. Deleting a marker on that basis
	// would be acting on a view it knows is incomplete.
	if !b.reaperCaughtUp() {
		return nil
	}
	orphans, err := b.orphanDrainMarkers()
	if err != nil {
		return err
	}
	if len(orphans) == 0 {
		return nil // converged: ZERO writes, which is what the registry requires of an idle pass
	}
	sort.Strings(orphans)
	var failed []string
	for _, node := range orphans {
		if perr := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterDrainSet(node, nil)
		}); perr != nil {
			failed = append(failed, node+": "+perr.Error())
			continue
		}
		b.cfg.Logger.Info("broker: cleared an orphaned broker_draining marker (no roster row for this node — "+
			"the retire that raised it discarded the error from its own clear)",
			"node", node)
	}
	if len(failed) > 0 {
		return fmt.Errorf("clear orphaned drain markers: %s", strings.Join(failed, "; "))
	}
	return nil
}

// orphanDrainMarkers returns the nodes that have a draining marker but NO cluster_nodes row.
//
// Both reads happen in ONE bounded-stale snapshot, and the roster is materialized before the marker
// set is filtered: a node whose row is being written concurrently must not read as an orphan because
// the two reads landed either side of the write.
func (b *Broker) orphanDrainMarkers() ([]string, error) {
	var marked []string
	roster := map[string]bool{}
	if err := b.cl.node.BoundedStaleRead(func(db *sql.DB) error {
		var derr error
		marked, derr = cluster.DrainingNodes(db)
		if derr != nil {
			return derr
		}
		if len(marked) == 0 {
			return nil
		}
		rows, qerr := db.Query(`SELECT node_id FROM cluster_nodes`)
		if qerr != nil {
			return qerr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if serr := rows.Scan(&id); serr != nil {
				return serr
			}
			roster[id] = true
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	var orphans []string
	for _, node := range marked {
		if !roster[node] {
			orphans = append(orphans, node)
		}
	}
	return orphans, nil
}

func drainingWithoutMarker(phase string, markerRaised, hasActiveOp bool) bool {
	return brokercontrol.DrainingWithoutMarker(phase, markerRaised, hasActiveOp)
}
