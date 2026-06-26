package broker

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
)

// clusterdrain.go — D7 §8.3 drain / retire orchestration (build-and-prove; part of
// the guard-excluded ClusterAdmin mechanism). Reuses D6 rehome (port.PlanReassignHome)
// to migrate exposes off the draining node and D5 AllAtTarget (via the injected
// streamsReady probe) to gate retire.

// QuorumProjection is the §8.3 "after this op" fault-tolerance summary shown before
// a drain/retire. FaultTolerance==0 means the cluster goes read-only on the next
// single failure — the TTY+typed-confirm gate (and `--yes` is refused there).
type QuorumProjection struct {
	Voters         int // raft voters remaining after the op
	Quorum         int // majority needed to commit
	FaultTolerance int // how many further node failures keep quorum (Voters - Quorum)
}

// ProjectQuorum computes the post-op projection. A plain drain keeps the node a raft
// voter (OQ-6: it sheds serving load only), so currentVoters is unchanged; a retire
// removes it. Either way FaultTolerance is the raft headroom on the resulting voter
// set — so a plain drain at N=2 still projects F==0 (matches §8.3 "含已处于 N=2 时再
// drain").
func ProjectQuorum(currentVoters int, retire bool) QuorumProjection {
	remaining := currentVoters
	if retire && remaining > 0 {
		remaining--
	}
	quorum := remaining/2 + 1
	f := remaining - quorum
	if f < 0 {
		f = 0
	}
	return QuorumProjection{Voters: remaining, Quorum: quorum, FaultTolerance: f}
}

// ErrQuorumConfirmRequired is returned by DrainNode when the projection is F==0 and
// the operator has not passed the typed confirmation. The CLI renders Proj, requires
// a typed node_id (never --yes), then re-calls with confirmed=true.
type ErrQuorumConfirmRequired struct{ Proj QuorumProjection }

func (e *ErrQuorumConfirmRequired) Error() string {
	return fmt.Sprintf("quorum projection F==0 (after op: %d voters, quorum=%d, tolerate 0 failures) — typed confirmation required",
		e.Proj.Voters, e.Proj.Quorum)
}

// ErrStreamsNotAtTarget refuses a retire while the node still holds stream replicas
// below target (data not yet redundant elsewhere; §8.3 AllAtTarget guard).
var ErrStreamsNotAtTarget = errors.New("cluster retire: this node's streams are not yet at target replicas (data not redundant) — refusing retire")

// ErrNoMigrationTarget is returned when exposes are homed on the draining node but no
// other eligible VOTER exists to receive them.
var ErrNoMigrationTarget = errors.New("cluster drain: exposes are homed here but no other eligible VOTER exists to migrate them to")

// ErrLeadershipTransferred is returned when DrainNode was asked to drain the CURRENT
// leader: it transfers leadership off it FIRST (so this broker, now a follower,
// never half-drains via failed Proposes — review B5) and bails. The operator
// re-runs the drain on the new leader, which drains the old leader as a follower.
type ErrLeadershipTransferred struct{ NodeID string }

func (e *ErrLeadershipTransferred) Error() string {
	return fmt.Sprintf("cluster drain %s: it was the leader — leadership transferred off it; re-run `cluster drain %s` on the new leader", e.NodeID, e.NodeID)
}

// DrainNode drains (and optionally retires) nodeID (§8.3). streamsReady is the D5
// AllAtTarget probe (retire only; nil => treated as ready, for the no-JS unit path).
// confirmed is the operator's typed F==0 confirmation.
//
// Order: quorum-projection guard -> AllAtTarget (retire) -> raise broker_draining ->
// migrate exposes (D6 rehome) -> transfer-leader if self is leader -> phase
// VOTER->DRAINING -> (retire) RETIRING -> RemoveServer -> ClusterNodeRemove -> clear
// the drain marker. Each step's failure leaves a status-visible stuck phase.
func (a *ClusterAdmin) DrainNode(nodeID string, retire, confirmed bool, deadline time.Time, streamsReady func() (bool, error)) error {
	// C4: refuse a raw drain/retire when an operation already owns this node's membership.
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	voters, err := a.node.NumVoters()
	if err != nil {
		return fmt.Errorf("cluster drain %s: count voters: %w", nodeID, err)
	}
	proj := ProjectQuorum(voters, retire)
	if retire && proj.Voters < 1 {
		// Retiring the last/only voter destroys the cluster (unrecoverable except via
		// force-single). HARD-refuse — no typed confirm bypasses this (review m4).
		return fmt.Errorf("cluster retire %s: cannot retire the last voter (the cluster would have 0 voters); this is the force-single/recover territory, not retire", nodeID)
	}
	if proj.FaultTolerance == 0 && !confirmed {
		return &ErrQuorumConfirmRequired{Proj: proj}
	}
	// If draining the CURRENT leader, transfer leadership off it FIRST and bail
	// (review B5): this broker is the leader running the orchestration; once it sheds
	// leadership it can no longer Propose, so it must NOT proceed to raise the marker
	// / migrate / phase-bump (that half-drains). The operator re-runs on the new
	// leader, which drains the old leader as a follower.
	//
	// This MUST precede requireClusterNode (the roster-membership check): the leader is
	// identified by the raft config, NOT the cluster_nodes roster, so the transfer-bail must
	// fire even for a leader that has no roster row yet (e.g. a freshly-bootstrapped node before
	// its self row is seeded). The roster check then validates the node on the re-run, where it
	// is a follower. (Regression fix: requireClusterNode had been inserted ahead of the bail,
	// making `drain <leader>` fail "not in cluster_nodes" instead of transferring + bailing.)
	if _, leaderID := a.node.LeaderWithID(); leaderID == nodeID {
		if err := a.transferLeadershipOff(nodeID); err != nil {
			return fmt.Errorf("cluster drain %s: transfer leadership off the leader: %w", nodeID, err)
		}
		return &ErrLeadershipTransferred{NodeID: nodeID}
	}
	if err := a.requireClusterNode(nodeID); err != nil {
		return fmt.Errorf("cluster drain %s: %w", nodeID, err)
	}
	if retire && streamsReady != nil {
		ready, err := streamsReady()
		if err != nil {
			return fmt.Errorf("cluster retire %s: stream readiness: %w", nodeID, err)
		}
		if !ready {
			return ErrStreamsNotAtTarget
		}
	}

	// 1. raise broker_draining with the deadline (nodeID is a follower; we are leader).
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterDrainSet(nodeID, &deadline)
	}); err != nil {
		return fmt.Errorf("cluster drain %s: raise broker_draining: %w", nodeID, err)
	}

	// 2. migrate exposes homed here to another eligible VOTER (D6 rehome).
	if err := a.migrateExposes(nodeID); err != nil {
		return fmt.Errorf("cluster drain %s: migrate exposes: %w", nodeID, err)
	}

	// 3. phase VOTER -> DRAINING (the node still votes; it sheds serving load).
	if err := a.setPhase(nodeID, phaseDraining, []string{phaseVoter}, ""); err != nil {
		return fmt.Errorf("cluster drain %s: phase->DRAINING: %w", nodeID, err)
	}
	if !retire {
		return nil
	}

	// 5. retire: §8.1 removal ORDER — roster RETIRING -> raft RemoveServer -> roster delete.
	if err := a.setPhase(nodeID, phaseRetiring, []string{phaseDraining}, ""); err != nil {
		return fmt.Errorf("cluster retire %s: phase->RETIRING: %w", nodeID, err)
	}
	if err := a.node.RemoveServer(nodeID); err != nil {
		return fmt.Errorf("cluster retire %s: raft RemoveServer (roster stuck at RETIRING, status shows next step): %w", nodeID, err)
	}
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeRemove(nodeID, a.now())
	}); err != nil {
		return fmt.Errorf("cluster retire %s: roster delete: %w", nodeID, err)
	}
	// Clear the drain marker (the node is gone).
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterDrainSet(nodeID, nil)
	}); err != nil {
		a.logger.Warn("cluster retire: clear broker_draining marker failed", "node_id", nodeID, "err", err)
	}
	return nil
}

func (a *ClusterAdmin) requireClusterNode(nodeID string) error {
	return a.node.VerifyLeaderRead(func(db *sql.DB) error {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&exists); err != nil {
			return err
		}
		if exists != 1 {
			return fmt.Errorf("node_id %q is not in cluster_nodes", nodeID)
		}
		return nil
	})
}

// RemoveNode finishes the removal of a node that has ALREADY walked through a
// removal phase (RETIRING or VOTER_ADD_FAILED). It REFUSES a live VOTER/CATCHING_UP/
// PENDING node (review B1): a bare RemoveServer on a healthy voter, paired with the
// phase-guarded roster DELETE, leaves the roster row stuck at VOTER while raft drops
// the voter — a permanent silent fork the reconciliation pass cannot heal. Use
// `drain --retire` to remove a live node.
func (a *ClusterAdmin) RemoveNode(nodeID string, force bool) error {
	// C4: refuse a raw remove when an operation already owns this node's membership.
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	phase, ok := a.nodePhase(nodeID)
	if !ok {
		return fmt.Errorf("recovery node remove %s: no such roster node", nodeID)
	}
	if phase != phaseRetiring && phase != phaseAddFailed {
		return fmt.Errorf("recovery node remove %s: node is %s; raw remove only finishes a RETIRING or VOTER_ADD_FAILED node — use `cluster retire %s` to remove a live node (refusing to avoid a roster/raft silent fork)", nodeID, phase, nodeID)
	}
	// B3 item 7: a RETIRING node already had its exposes migrated by DrainNode→migrateExposes, but
	// a VOTER_ADD_FAILED node never drained — it can still HOME allocated exposes that a bare
	// remove would orphan. Refuse by default; --force bypasses ONLY this ownership probe (the
	// phase-gate above is independent and unweakened: a live VOTER still refuses regardless).
	if phase == phaseAddFailed && !force {
		n, err := a.countOwnedExposes(nodeID)
		if err != nil {
			return fmt.Errorf("recovery node remove %s: count owned exposes: %w", nodeID, err)
		}
		if n > 0 {
			return &ErrRemoveOwnsResources{NodeID: nodeID, Exposes: n}
		}
	}
	if err := a.node.RemoveServer(nodeID); err != nil {
		return fmt.Errorf("recovery node remove %s: raft RemoveServer: %w", nodeID, err)
	}
	return a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeRemove(nodeID, a.now())
	})
}

// ErrRemoveOwnsResources is returned when `cluster remove` (no --force) targets a
// VOTER_ADD_FAILED node that still HOMES allocated exposes (which a bare remove would orphan).
// The "still HOMES" substring is the SINGLE authored literal (B3 item 7) — clusterCodeFor and the
// D7 pin key on it; keep them in lockstep.
type ErrRemoveOwnsResources struct {
	NodeID  string
	Exposes int
}

func (e *ErrRemoveOwnsResources) Error() string {
	// NOTE (B3 review M1): do NOT suggest `drain --retire` here — DrainNode only accepts a live
	// VOTER (phaseVoter), so it is a dead end for a VOTER_ADD_FAILED node. The real remedies are
	// (a) free/re-home the exposes, or (b) --force (orphan them).
	return fmt.Sprintf("recovery node remove %s: REFUSED — this VOTER_ADD_FAILED node still HOMES %d expose(s) that would be orphaned. Free/re-home those exposes first (re-expose them on a healthy node), or pass `recovery node remove %s --manual --force` to remove anyway (orphans them). (`cluster retire` does NOT apply — it retires a live VOTER, not a VOTER_ADD_FAILED node.)", e.NodeID, e.Exposes, e.NodeID)
}

// countOwnedExposes counts ALLOCATED exposes homed on nodeID (the same single-COUNT read pattern
// migrateExposes uses; no nested query under the single-conn pool).
func (a *ClusterAdmin) countOwnedExposes(nodeID string) (int, error) {
	var n int
	err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE home_broker=? AND state=?`,
			nodeID, string(port.StateAllocated)).Scan(&n)
	})
	return n, err
}

// nodePhase reads a roster row's phase (materialized read; no nested Propose).
func (a *ClusterAdmin) nodePhase(nodeID string) (string, bool) {
	var phase string
	var found bool
	_ = a.node.BoundedStaleRead(func(db *sql.DB) error {
		err := db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&phase)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return phase, found
}

// AbortDrain clears broker_draining and returns the node to VOTER (the --abort path).
func (a *ClusterAdmin) AbortDrain(nodeID string) error {
	// Mega-audit MAJ-1: a node DRAINING as part of a recoverable retire operation must NOT be flipped
	// back to VOTER by a raw `drain --abort` (that desyncs the op's phase machine from raft — a later
	// RAFT_REMOVED tick then RemoveServer's the voter while PlanClusterNodeRemove leaves the roster row
	// VOTER → a silent roster/raft fork). Refuse like the other raw drain-family mutators; the operator
	// aborts the operation via `cluster ops abort <id>` instead.
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	if err := a.setPhase(nodeID, phaseVoter, []string{phaseDraining}, ""); err != nil {
		return fmt.Errorf("cluster drain --abort %s: phase->VOTER: %w", nodeID, err)
	}
	return a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterDrainSet(nodeID, nil)
	})
}

// RotateTunnelCert rotates a node's stable tunnel cert pin (external review F2 /
// §15 RF3): the new fp becomes cert_fp, the old becomes cert_fp_prev, and
// cert_fp_valid_until = now + window so the D6 cert-pin VerifyConnection accepts the
// previous pin during the cutover. The post-window clear is enforced by that
// VerifyConnection check (it rejects the previous pin after valid_until) regardless
// of the column value, so no separate clear op is needed.
func (a *ClusterAdmin) RotateTunnelCert(nodeID, newFP string, window time.Duration) error {
	if newFP == "" {
		return fmt.Errorf("rotate-tunnel-cert %s: --cert-fp is required", nodeID)
	}
	if nodeID != a.node.SelfID() {
		return fmt.Errorf("rotate-tunnel-cert %s: rotate must run on the target broker while it is leader; transfer leadership to %s first so it can hot-swap its live tunnel certificate", nodeID, nodeID)
	}
	if phase, ok := a.nodePhase(nodeID); !ok {
		return fmt.Errorf("rotate-tunnel-cert %s: no such roster node", nodeID)
	} else if phase != phaseVoter && phase != phaseCatchingUp && phase != phasePending {
		return fmt.Errorf("rotate-tunnel-cert %s: node is %s (rotate a live node)", nodeID, phase)
	}
	var commitCert func()
	if a.prepareTunnelCertRotate != nil {
		var err error
		commitCert, err = a.prepareTunnelCertRotate(newFP)
		if err != nil {
			return fmt.Errorf("rotate-tunnel-cert %s: %w", nodeID, err)
		}
	}
	validUntil := a.now().Add(window)
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterCertRotate(nodeID, newFP, validUntil, a.now())
	}); err != nil {
		return err
	}
	if commitCert != nil {
		commitCert()
	}
	return nil
}

// transferLeadershipOff hands raft leadership to a specific voter that is NOT
// excludeID, using the TARGETED LeadershipTransferToServer (review m1: the targeted
// wrapper was dead code; drain used the untargeted variant). It picks any raft voter
// != excludeID from the live configuration.
func (a *ClusterAdmin) transferLeadershipOff(excludeID string) error {
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return err
	}
	for _, s := range cfg {
		if s.Voter && s.NodeID != excludeID {
			return a.node.LeadershipTransferToServer(s.NodeID, s.Addr)
		}
	}
	return fmt.Errorf("no other voter to transfer leadership to (cannot drain the only voter)")
}

// ErrRebuildOffExposes is returned when a drained node homes rebuild-OFF exposes:
// the operator's explicit "do not rebuild this on failure" choice must NOT be turned
// into a silent auto-rehome (external review F3). The error enumerates the exact
// ports so the operator handles them deliberately (free them, or re-decide) before
// re-running drain.
type ErrRebuildOffExposes struct {
	NodeID string
	Ports  []int
}

func (e *ErrRebuildOffExposes) Error() string {
	return fmt.Sprintf("cluster drain %s: %d rebuild-OFF expose(s) are homed here (ports %v) — they will NOT be auto-migrated (the operator chose no-rebuild). Free/re-decide them, then re-run drain.",
		e.NodeID, len(e.Ports), e.Ports)
}

// migrateExposes re-homes ALLOCATED rebuild-ON exposes homed on nodeID to another
// eligible VOTER via D6 rehome, and REFUSES (enumerating them) on any rebuild-OFF
// expose — silently rehoming a rebuild-OFF expose would override the operator's
// explicit choice (external review F3). It materializes the port lists + the target
// FIRST (close the rows) THEN Proposes — the FSM and the reader share the one
// SetMaxOpenConns(1) pool, so a Propose nested inside an open *sql.Rows deadlocks
// (the D6 lesson).
func (a *ClusterAdmin) migrateExposes(nodeID string) error {
	var rebuildOn, rebuildOff []int
	names := map[int]string{} // B7 DOC#5: port → expose name, for the expose_rehomed event
	sids := map[int]string{}  // Stage-C m1: port → sid, so the event can correlate to a session
	var target string
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(
			`SELECT port, rebuild_on_failure, name, sid FROM port_allocations WHERE home_broker=? AND state=?`,
			nodeID, string(port.StateAllocated))
		if err != nil {
			return err
		}
		for rows.Next() {
			var p, rebuild int
			var name, sid string
			if err := rows.Scan(&p, &rebuild, &name, &sid); err != nil {
				_ = rows.Close()
				return err
			}
			names[p] = name
			sids[p] = sid
			if rebuild == 1 {
				rebuildOn = append(rebuildOn, p)
			} else {
				rebuildOff = append(rebuildOff, p)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		// Pick an eligible VOTER that is not the draining node AND has a resolved
		// nats_server_id (review m5: a home the D6 server-id bridge cannot resolve is
		// not a valid rehome target — match D6's eligibility, not just phase=VOTER).
		return db.QueryRow(
			`SELECT node_id FROM cluster_nodes WHERE phase='VOTER' AND node_id != ? AND nats_server_id != '' ORDER BY node_id LIMIT 1`,
			nodeID).Scan(&target)
	}); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Rebuild-OFF exposes are NEVER silently rehomed (F3): refuse + enumerate.
	if len(rebuildOff) > 0 {
		return &ErrRebuildOffExposes{NodeID: nodeID, Ports: rebuildOff}
	}
	if len(rebuildOn) == 0 {
		return nil // nothing to migrate
	}
	if target == "" {
		// C6: rehome_stalled{expose,no_eligible_target} per port BEFORE returning the error (so the
		// operator sees WHICH exposes are stuck), then still return ErrNoMigrationTarget.
		for _, p := range rebuildOn {
			a.emitDrainEvent(evRehomeStalled, map[string]any{
				"kind": "expose", "name": names[p], "sid": sids[p], "port": p,
				"home_broker": nodeID, "reason": reasonNoEligibleTarget,
			})
		}
		return ErrNoMigrationTarget
	}
	for _, p := range rebuildOn {
		p := p
		skipped := false
		var newEpoch int64
		if err := a.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			ne, cmd, err := port.PlanReassignHome(db, p, target, a.now())
			if errors.Is(err, port.ErrNotFound) {
				skipped = true // BD8: raced free/revoke — emit NO started / succeeded / expose_rehomed
				return nil, nil
			}
			newEpoch = ne
			return cmd, err
		}); err != nil {
			// C6-EVT-5: started fires only on a real attempt, so a failed (but not raced-away) rehome
			// gets a matched started→failed pair; a raced-away (skipped) expose emits nothing at all.
			a.emitDrainEvent(evHomeReassignStarted, map[string]any{
				"kind": "expose", "name": names[p], "sid": sids[p], "port": p, "from_broker": nodeID, "to_broker": target,
			})
			a.emitDrainEvent(evHomeReassignFailed, map[string]any{
				"kind": "expose", "name": names[p], "sid": sids[p], "port": p,
				"from_broker": nodeID, "to_broker": target, "reason": classifyRehomeErr(err),
			})
			return fmt.Errorf("rehome port %d -> %s: %w", p, target, err)
		}
		if skipped {
			continue // BD8/C6-EVT-5: no started/succeeded/expose_rehomed for a raced-away row
		}
		// started→succeeded as a matched pair (HR2: carry the authoritative post-Apply epoch) + the
		// legacy expose_rehomed (back-compat).
		a.emitDrainEvent(evHomeReassignStarted, map[string]any{
			"kind": "expose", "name": names[p], "sid": sids[p], "port": p, "from_broker": nodeID, "to_broker": target,
		})
		a.emitDrainEvent(evHomeReassignSucceeded, map[string]any{
			"kind": "expose", "name": names[p], "sid": sids[p], "port": p, "from_broker": nodeID, "to_broker": target, "epoch": newEpoch,
		})
		a.emitDrainEvent("expose_rehomed", map[string]any{
			"port": p, "name": names[p], "sid": sids[p], "from_broker": nodeID, "to_broker": target,
		})
	}
	a.logger.Info("cluster drain: migrated rebuild-ON exposes", "node_id", nodeID, "count", len(rebuildOn), "target", target)
	return nil
}

// emitDrainEvent fires a leader-side drain observability event (nil emitter ⇒ no-op).
func (a *ClusterAdmin) emitDrainEvent(kind string, fields map[string]any) {
	if a.emitEvent != nil {
		a.emitEvent(kind, fields)
	}
}
