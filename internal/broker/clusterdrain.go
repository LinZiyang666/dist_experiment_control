package broker

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusterdrain.go — D7 §8.3 drain / retire orchestration (build-and-prove; part of
// the guard-excluded ClusterAdmin mechanism). Reuses D6 rehome (port.PlanReassignHome)
// to migrate exposes off the draining node and D5 AllAtTarget (via the injected
// streamsReady probe) to gate retire — that half moved to StartRetireOperation
// in batch-A A13; what remains here is drain only.

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

// DrainNode drains nodeID (§8.3).
//
// Batch-A A13 removed the retire half: it did RemoveServer + a roster delete
// with none of StartRetireOperation's protections. The `retire` parameter is
// kept so the 8 call sites and the adminsock shape are unchanged, but passing
// true is now an error naming `tether cluster retire`. streamsReady is likewise
// vestigial — the D5 AllAtTarget gate it fed lives on the operation path.
// confirmed is the operator's typed F==0 confirmation.
//
// Order: quorum-projection guard -> raise broker_draining -> migrate exposes
// (D6 rehome) -> transfer-leader if self is leader -> phase VOTER->DRAINING.
// Each step's failure leaves a status-visible stuck phase. The steps that
// followed (RETIRING -> RemoveServer -> ClusterNodeRemove -> clear marker) are
// now driven by the retire operation, not from here.
func (a *ClusterAdmin) DrainNode(nodeID string, retire, confirmed bool, deadline time.Time, streamsReady func() (bool, error)) error {
	// Batch-A A13: the synchronous retire is gone.
	//
	// It performed RemoveServer plus a roster row delete — an IRREVERSIBLE raft
	// membership change — with none of the protections StartRetireOperation
	// has: no opStillLive TOCTOU recheck, no replicable deadline (it carried the
	// caller's wall clock, so a broker restart lost it), no BLOCKED escape
	// hatch, no resume. cmd/tether hardcoded Retire:false, so no released CLI
	// ever reached it, and handleDrain now refuses the field on the socket too —
	// which left this body reachable only from tests, while still being the most
	// destructive code path in the package.
	//
	// The parameter is kept so the 8 call sites and the adminsock shape stay
	// unchanged; passing true is now an error that names the supported verb.
	if retire {
		return fmt.Errorf("cluster drain %s: --retire is no longer supported here; use `tether cluster retire %s` "+
			"(the recoverable operation: crash-resumable deadline, BLOCKED escape hatch, TOCTOU-safe membership change)",
			nodeID, nodeID)
	}
	_ = streamsReady // retained for signature compatibility; only the retire path consumed it
	// C4: refuse a raw drain/retire when an operation already owns this node's membership.
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	voters, err := a.node.NumVoters()
	if err != nil {
		return fmt.Errorf("cluster drain %s: count voters: %w", nodeID, err)
	}
	proj := ProjectQuorum(voters, false) // retire is refused above; this is the drain-only projection
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
	// 1. raise broker_draining with the deadline (nodeID is a follower; we are leader).
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterDrainSet(nodeID, &deadline)
	}); err != nil {
		return fmt.Errorf("cluster drain %s: raise broker_draining: %w", nodeID, err)
	}

	// 2. migrate exposes homed here to another eligible VOTER (D6 rehome). The returned
	// list is discarded — the durable gate below re-derives convergence (CRIT-1); what
	// matters here is the raft move side effect.
	if err := a.migrateExposes(nodeID); err != nil {
		return fmt.Errorf("cluster drain %s: migrate exposes: %w", nodeID, err)
	}

	// 2b. R8a P1 — the rc-semantics gate. The raft write above changed only the
	// CONTROL plane. Until each affected agent has re-dialed the new home, `drain`
	// has NOT done what the operator asked, and reporting rc=0 here is precisely the
	// lie this batch exists to remove. Block (bounded by the drain deadline) on the
	// agents' APPLIED acks; on timeout return ErrDataPlaneNotConverged, which the
	// adminsock layer maps to a nonzero, retry-classified exit.
	//
	// CRIT-1: gate on the DURABLE pendingRetireConvergence(nodeID), not migrated. On a
	// re-run (the "re-run to keep waiting" hint) migrateExposes returns an EMPTY list
	// because the rows already moved, and awaiting an empty list is an instant false
	// rc=0. The durable set re-derives the still-un-confirmed exposes from current state,
	// so a re-run keeps waiting until the data plane genuinely follows.
	//
	// Ordered BEFORE the phase bump and (for retire) before RemoveServer: removing a
	// node while exposes still point at it is the very outage this gate prevents.
	// Fleet-wide (zero time) for the SYNCHRONOUS drain: fail-CLOSED, since a recency window would fail
	// OPEN on a slow re-run (rows aging out ⇒ false rc=0). Drain fences no membership; its over-wait is
	// bounded by the drain deadline (F3 scoping applies to the retire OP, which does fence).
	pending, err := a.pendingRetireConvergence(nodeID, time.Time{})
	if err != nil {
		return fmt.Errorf("cluster drain %s: check data-plane convergence: %w", nodeID, err)
	}
	if err := a.awaitHomeConvergence("drain", nodeID, pending, deadline); err != nil {
		return err
	}

	// 3. phase VOTER -> DRAINING (the node still votes; it sheds serving load).
	if err := a.setPhase(nodeID, phaseDraining, []string{phaseVoter}, ""); err != nil {
		return fmt.Errorf("cluster drain %s: phase->DRAINING: %w", nodeID, err)
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
	// G2 #12: ghost passthrough. A roster row in a non-removable phase (a stale VOTER after force-single)
	// that is ABSENT from the committed raft config is a force-single ghost — otherwise undeletable online
	// (the phase-gate refuses its VOTER phase AND PlanClusterNodeRemove's phase-scoped deletes never match
	// it: the live racknerd pc732 deadlock). Deleting a not-in-config row cannot fork quorum. The
	// not-in-config proof MUST be a LEADER-committed config read (a stale follower could misjudge a live
	// voter as absent → B1 fork), so require leadership before treating absence as proof.
	if phase != phaseRetiring && phase != phaseAddFailed && phase != phaseCatchingUp {
		_, leaderID := a.node.LeaderWithID()
		if leaderID != a.node.SelfID() {
			return fmt.Errorf("recovery node remove %s: node is %s and this broker is not the raft leader — run on the leader (only a leader-committed config read can prove a removable ghost vs a live voter)", nodeID, phase)
		}
		inCfg, err := a.nodeInCommittedConfig(nodeID)
		if err != nil {
			return fmt.Errorf("recovery node remove %s: read committed raft config: %w", nodeID, err)
		}
		if inCfg {
			return fmt.Errorf("recovery node remove %s: node is %s; raw remove only finishes a RETIRING, VOTER_ADD_FAILED, or aborted staged CATCHING_UP node — use `cluster retire %s` to remove a live voter (refusing to avoid a roster/raft silent fork)", nodeID, phase, nodeID)
		}
		return a.removeGhost(nodeID, force)
	}
	// review F1: a CATCHING_UP node is a STAGED NONVOTER left behind by a failed/aborted grow — it is
	// online-removable (a nonvoter is not a committed voter, so RemoveServer cannot fork quorum), which
	// is the whole point of nonvoter staging. Defensively confirm it is NOT already a committed raft
	// VOTER (a divergent voter would be the exact B1 silent-fork hazard → refuse, point to retire).
	if phase == phaseCatchingUp {
		sub, serr := a.readSubstrate(nodeID)
		if serr != nil {
			return fmt.Errorf("recovery node remove %s: read raft membership: %w", nodeID, serr)
		}
		if sub.isVoter {
			return fmt.Errorf("recovery node remove %s: node is CATCHING_UP yet already a committed raft VOTER — use `cluster retire %s` (refusing a bare remove to avoid a roster/raft fork)", nodeID, nodeID)
		}
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
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeRemove(nodeID, a.now())
	}); err != nil {
		return err
	}
	a.convergeSeedsAfterRemoval(nodeID) // G3 #1 M1
	return nil
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

// nodeInCommittedConfig reports whether nodeID is in THIS broker's raft configuration. RaftConfiguration()
// returns the LATEST configuration (on the leader that includes any appended-but-uncommitted membership
// change); it is only trustworthy on the LEADER (a follower may hold a stale config), so every caller MUST
// leader-gate before treating a false result as proof of a removable ghost. On the leader hashicorp raft
// permits only one in-flight config change, so an in-flight add of the SAME id reads present → safely
// refuses (G2 #12).
func (a *ClusterAdmin) nodeInCommittedConfig(nodeID string) (bool, error) {
	servers, err := a.node.RaftConfiguration()
	if err != nil {
		return false, err
	}
	for _, s := range servers {
		if s.NodeID == nodeID {
			return true, nil
		}
	}
	return false, nil
}

// removeGhost deletes a force-single ghost roster row — a node ABSENT from the committed raft config
// (proven by the LEADER-gated caller in RemoveNode). It reuses the ownership guard (a ghost that still
// HOMES exposes would orphan them on a bare delete; --force bypasses only this probe), calls RemoveServer
// as a defensive idempotent no-op (harmless on an already-absent node; catches a racing re-add), then
// prunes the row via PlanClusterNodePrune — a plain by-node_id DELETE, because PlanClusterNodeRemove's
// phase-scoped deletes would not match a VOTER-phase ghost. Not-in-config ⇒ the delete cannot fork quorum.
func (a *ClusterAdmin) removeGhost(nodeID string, force bool) error {
	if !force {
		n, err := a.countOwnedExposes(nodeID)
		if err != nil {
			return fmt.Errorf("recovery node remove %s: count owned exposes: %w", nodeID, err)
		}
		if n > 0 {
			return &ErrRemoveOwnsResources{NodeID: nodeID, Exposes: n}
		}
	}
	if err := a.node.RemoveServer(nodeID); err != nil {
		return fmt.Errorf("recovery node remove %s (ghost): raft RemoveServer: %w", nodeID, err)
	}
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodePrune([]string{nodeID}, a.now())
	}); err != nil {
		return err
	}
	a.convergeSeedsAfterRemoval(nodeID) // G3 #1 M1: converge seeds so the cleared ghost's endpoint leaves the bundle
	return nil
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
	// R11 P11/#56: this is a SELF-ONLY verb (a broker hot-swaps its OWN live tunnel cert), and it needs
	// to be leader to commit the pin update through raft. The two failure modes get DISTINCT, executable
	// guidance — never the generic "re-run on the leader host", which is wrong for a self-only verb:
	//   (a) wrong host        — you ran it somewhere other than the target; run it ON the target.
	//   (b) right host, not leader — the target IS this broker but it is a follower; make it leader.
	self := a.node.SelfID()
	if nodeID != self {
		return fmt.Errorf("rotate-tunnel-cert %s: must run ON the target broker %s (it hot-swaps its OWN live "+
			"tunnel cert); you ran it on %s. Re-run it on %s — and %s must be the leader when you do (transfer "+
			"leadership to it first if it is not: `tether cluster transfer %s`)", nodeID, nodeID, self, nodeID, nodeID, nodeID)
	}
	if !a.node.IsLeader() {
		return fmt.Errorf("rotate-tunnel-cert %s: this IS the target broker, but it is a FOLLOWER — a broker can "+
			"only hot-swap its live tunnel cert while it is the leader. Transfer leadership to it (`tether cluster "+
			"transfer %s`), then re-run this on %s", nodeID, nodeID, nodeID)
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

// assertAllVotersSupportPhaseFluidityOps refuses the v0.4.2 phase-fluidity ops (OpClusterNodeReaddr/
// Route) when any OTHER raft voter cannot apply them (review F5). Those ops are REPLICATED to every
// voter; an older binary whose knownOps lacks them decodeCommand-poisons the entry — advancing
// applied_index but skipping the SQL — so the leader updates its SQLite while the old follower does
// not: a deterministic, silent replica fork. The cluster permits release-skew rolling upgrade, so
// "operators upgrade first" is NOT a safety property; we gate instead. N=1 (only self applies the op)
// is always safe. For N>=2 every NON-self voter must advertise PhaseFluidityOps in its live health
// report; an unreachable or older voter fails CLOSED.
func (a *ClusterAdmin) assertAllVotersSupportPhaseFluidityOps(opName string) error {
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("%s: read raft config: %w", opName, err)
	}
	var health map[string]proto.ClusterHealthResp
	if a.healthPoll != nil {
		health = a.healthPoll()
	}
	return checkVotersSupportPhaseFluidityOps(opName, cfg, a.node.SelfID(), health)
}

// checkVotersSupportPhaseFluidityOps is the pure F5 gate (testable without a multi-voter raft
// harness): every NON-self voter must advertise PhaseFluidityOps in the live health map; an
// unreachable (absent) or older (false) voter fails CLOSED. N=1 (<=1 voter) is always allowed.
func checkVotersSupportPhaseFluidityOps(opName string, cfg []cluster.ServerInfo, self string, health map[string]proto.ClusterHealthResp) error {
	var voters []string
	for _, s := range cfg {
		if s.Voter {
			voters = append(voters, s.NodeID)
		}
	}
	if len(voters) <= 1 {
		return nil // N=1: only self applies the op — no mixed-version fork is possible
	}
	for _, v := range voters {
		if v == self {
			continue // this binary is proposing the op, so it self-evidently supports it
		}
		hr, ok := health[v]
		if !ok {
			return fmt.Errorf("%s: cannot confirm voter %q supports the v0.4.2 phase-fluidity ops (unreachable) — refusing to avoid a mixed-version replica fork; upgrade ALL voters to v0.4.2 and ensure they are reachable", opName, v)
		}
		if !hr.PhaseFluidityOps {
			return fmt.Errorf("%s: voter %q is an older binary without the v0.4.2 phase-fluidity ops — upgrade ALL voters to v0.4.2 first (an old voter would poison-skip the op and fork its replica)", opName, v)
		}
	}
	return nil
}

// SetRaftAddr rebinds a node's raft ADVERTISE address ONLINE — the routine replacement for
// force-single (v0.4.2 phase-fluidity). It updates BOTH replicated authorities consistently: the
// cluster_nodes.raft_addr roster column (Propose, committed first) and the raft Configuration
// server address (node.AddVoter on the existing voter — an in-place address update, NOT a wipe /
// snapshot reset / log replay). ORDER is column-first so a crash between the two heals on a safe
// idempotent re-run; both steps no-op when the target already matches. Self-rebind at N=1 is
// unconditionally safe (the lone leader rewrites its own address at quorum=1, retaining
// leadership); a peer-rebind at N>=2 changes where the leader DIALS that peer, so it must run only
// AFTER that peer is already listening on newAddr. Loopback/unspecified is refused unless
// allowLoopback (single-host dev). Refused while another membership op is in flight.
func (a *ClusterAdmin) SetRaftAddr(nodeID, newAddr string, allowLoopback bool) error {
	if nodeID == "" {
		nodeID = a.node.SelfID() // default: rebind self (the common N=1 grow-prep case)
	}
	// v0.4.2 review B1 — set-raft-addr is SELF-ONLY. Rebinding a PEER's advertised address from the
	// leader re-points the leader's replication target to the new addr immediately; if that peer is
	// not already serving it the config entry never reaches the new quorum and the cluster WEDGES
	// (the exact failure this feature removes). The safe way to readdress a FOLLOWER is to transfer
	// leadership to it, then run set-raft-addr there (self-rebind). Self-rebind is always safe — the
	// node serves the address it just bound, so the followers' re-dial succeeds.
	if nodeID != a.node.SelfID() {
		return fmt.Errorf("set-raft-addr: only the leader's OWN address can be rebound (asked for %q, self is %q) — rebinding a peer would wedge the cluster; `cluster transfer-leader %s` first, then run set-raft-addr there", nodeID, a.node.SelfID(), nodeID)
	}
	if err := validateAdvertiseHostPort(newAddr, allowLoopback); err != nil {
		return fmt.Errorf("set-raft-addr %s: %w", nodeID, err)
	}
	if err := a.requireClusterNode(nodeID); err != nil {
		return err
	}
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	if err := a.assertAllVotersSupportPhaseFluidityOps("set-raft-addr " + nodeID); err != nil {
		return err
	}
	// Read the committed config once: REJECT a newAddr that collides with another voter's address
	// (raft FATALs the config change on a duplicate), and capture self's current addr so the raft
	// write is SKIPPED when it already matches — appendConfigurationEntry always logs a config entry
	// even for an unchanged re-affirm, so guarding here keeps a redundant set-raft-addr write-free
	// (review m2; this also retires the unused SelfConfiguredAddr helper).
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("set-raft-addr %s: read raft config: %w", nodeID, err)
	}
	curAddr := ""
	for _, s := range cfg {
		if s.NodeID == nodeID {
			curAddr = s.Addr
		} else if s.Addr == newAddr {
			return fmt.Errorf("set-raft-addr %s: %s already advertises %s (duplicate raft address)", nodeID, s.NodeID, newAddr)
		}
	}
	// (1) Roster column first, committed through raft. Change-gated, so a re-run after the
	// addr already matches is a RowsAffected==0 no-op.
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeReaddr(nodeID, newAddr)
	}); err != nil {
		return fmt.Errorf("set-raft-addr %s: roster update: %w", nodeID, err)
	}
	// (2) Then the raft Configuration in place — only when it actually differs. raft.AddVoter on an
	// EXISTING voter just rewrites that server's Address (no wipe), an online replicated
	// config-change. At N=1 it commits at quorum=1; a crash between (1) and (2) heals on re-run.
	if curAddr != newAddr {
		if err := a.node.AddVoter(nodeID, newAddr); err != nil {
			return fmt.Errorf("set-raft-addr %s: raft config update: %w", nodeID, err)
		}
	}
	return nil
}

// SetNatsRoute rebinds a node's NATS route-mesh ADVERTISE (the twin of raft_addr for the :6222
// cluster{} routes; v0.4.2 phase-fluidity). It Proposes the cluster_nodes.nats_route UPDATE, which
// bumps topology_generation (nats_route IS rendered into nats.conf) so the topology reconcile
// re-renders + reloads the mesh. No raft Configuration change (nats_route is not a raft address).
// Loopback/unspecified is refused unless allowLoopback. Refused while a membership op is in flight.
func (a *ClusterAdmin) SetNatsRoute(nodeID, newRoute string, allowLoopback bool) error {
	if nodeID == "" {
		nodeID = a.node.SelfID()
	}
	// Self-only, consistent with set-raft-addr (B1) and the combined `set-raft-addr --route` command:
	// a node advertises its OWN route; readdress a follower by transferring leadership to it first.
	if nodeID != a.node.SelfID() {
		return fmt.Errorf("set-route: only the leader's OWN nats route can be rebound (asked for %q, self is %q); `cluster transfer-leader %s` first", nodeID, a.node.SelfID(), nodeID)
	}
	if err := validateNatsRoute(newRoute, allowLoopback); err != nil {
		return fmt.Errorf("set-route %s: %w", nodeID, err)
	}
	if err := a.requireClusterNode(nodeID); err != nil {
		return err
	}
	if err := a.assertNoActiveOp(nodeID); err != nil {
		return err
	}
	if err := a.assertAllVotersSupportPhaseFluidityOps("set-route " + nodeID); err != nil {
		return err
	}
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeRoute(nodeID, newRoute, a.now())
	}); err != nil {
		return fmt.Errorf("set-route %s: %w", nodeID, err)
	}
	return nil
}

// validateAdvertiseHostPort checks addr is a structural host:port and (unless allowLoopback)
// rejects a loopback/unspecified host — the whole point of a rebind is a peer-reachable address;
// a loopback advertise is exactly the pc732 defect this feature exists to fix. Note 0.0.0.0 is a
// legitimate BIND but never a valid ADVERTISE (peers cannot dial the unspecified address).
func validateAdvertiseHostPort(addr string, allowLoopback bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid host:port %q: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("invalid host:port %q: empty port", addr)
	}
	// review suggestion: reject port 0 / out-of-range (SplitHostPort splits but does not range-check).
	if p, perr := strconv.Atoi(port); perr != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid host:port %q: port must be 1-65535", addr)
	}
	if allowLoopback {
		return nil
	}
	if host == "" {
		return fmt.Errorf("advertise host is empty (0.0.0.0 is a BIND, not an advertise)")
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return fmt.Errorf("advertise host %q is loopback/unspecified — peers cannot reach it; use the node's reachable address (or --allow-loopback for single-host dev)", host)
	}
	return nil
}

// validateNatsRoute checks route is a nats:// URL with a peer-reachable host (unless
// allowLoopback). The NATS route-mesh advertise is the twin of raft_addr; a loopback route splits
// the data plane cross-host even after the raft rebind succeeds.
func validateNatsRoute(route string, allowLoopback bool) error {
	// SHARED grammar (review F6/R4): the structural bare-authority check lives in ONE function
	// (cluster.ValidateBareNatsRoute) that BOTH this CLI/admin validator and the replicated planner
	// call, so the two trust boundaries cannot drift. This wrapper adds only the CLI-side loopback gate.
	if err := cluster.ValidateBareNatsRoute(route); err != nil {
		return err
	}
	if allowLoopback {
		return nil
	}
	u, _ := url.Parse(route) // already validated parseable above
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return fmt.Errorf("nats route host %q is loopback/unspecified — peers cannot reach it (or --allow-loopback for single-host dev)", host)
	}
	return nil
}

// isLoopbackAdvertiseHostPort reports whether addr parses as host:port with a loopback/unspecified
// host — the grow-blocking condition the status report + doctor surface (a peer cannot dial it). A
// non-IP host (a DNS name) is NOT flagged: it may resolve to a routable address.
func isLoopbackAdvertiseHostPort(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

// isLoopbackNatsRoute reports whether a nats:// route URL has a loopback/unspecified host — the
// twin of isLoopbackAdvertiseHostPort for the :6222 route mesh.
func isLoopbackNatsRoute(route string) bool {
	u, err := url.Parse(route)
	if err != nil || u.Scheme != "nats" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
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
// It returns the exposes it actually re-pointed, with the post-Apply epoch each one
// must now converge to. R8a: that list is the INPUT to the data-plane convergence
// gate — "we wrote home_broker through raft" is a control-plane fact, and the whole
// point of this batch is that a control-plane fact is not a data-plane fact.
// The []rehomedExpose it used to return was read by nobody (both call sites spelled `_, err :=`);
// the rehome events it emits internally are the actual output.
func (a *ClusterAdmin) migrateExposes(nodeID string) error {
	var rebuildOn, rebuildOff []int
	names := map[int]string{} // B7 DOC#5: port → expose name, for the expose_rehomed event
	sids := map[int]string{}  // Stage-C m1: port → sid, so the event can correlate to a session
	nids := map[int]string{}  // R8a: port → nid, so the convergence gate can address the OWNING agent
	var target string
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(
			`SELECT port, rebuild_on_failure, name, sid, nid FROM port_allocations WHERE home_broker=? AND state=?`,
			nodeID, string(port.StateAllocated))
		if err != nil {
			return err
		}
		for rows.Next() {
			var p, rebuild int
			var name, sid, nid string
			if err := rows.Scan(&p, &rebuild, &name, &sid, &nid); err != nil {
				_ = rows.Close()
				return err
			}
			names[p] = name
			sids[p] = sid
			nids[p] = nid
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

// pendingRetireConvergence is the DURABLE form of the drain/retire data-plane gate
// (external review CRIT-1).
//
// migrateExposes' RETURNED list self-destructs: its query finds rows by
// home_broker=nodeID and the migrate re-points exactly those rows, so a SECOND call
// (a retire's next controller tick, or a drain the operator re-runs after the
// "re-run to keep waiting" hint) returns an EMPTY list. Gating on that empty list
// waves the op straight past the convergence check to the IRREVERSIBLE RemoveServer —
// with the silent agent's tunnel still pinned to the node being removed — and lets a
// re-run drain print a false rc=0. That is exactly the P1 outage this batch exists to
// kill, reintroduced one layer down.
//
// This reads CURRENT replicated state each call: ALLOCATED homed exposes that have
// already been moved OFF nodeID (home_broker != nodeID) whose owning agent has NOT
// confirmed the row epoch (homeAppliedFn(port) < epoch). It is idempotent across ticks
// and re-runs.
//
// SCOPING (external re-review F3 + round-2 self-review): rehomedSince RESTRICTS the set to
// exposes rehomed at or after that instant (last_rehome_at, which migrateExposes stamps in
// the same CAS that bumps the epoch). Without it the gate is FLEET-WIDE, so ONE unrelated
// permanently-stranded expose (a prior op of another node whose agent died) would freeze
// EVERY drain/retire. The RETIRE op passes a FIXED origin — its own op.CreatedAt — NOT a
// sliding wall-clock window. That distinction is load-bearing: this op's migrate always runs
// AFTER its creation, so its rows are ALWAYS >= op.CreatedAt and never age out, even across a
// `cluster ops confirm` retry that restarts the convergence deadline; a sliding window
// (now − Δ) drifts away from the fixed last_rehome_at and would let a still-stranded row age
// out BEFORE the reset deadline fires, silently completing RemoveServer (a fail-open the
// round-2 self-review caught). Unrelated old stranded rows (rehomed before this op started)
// fall below op.CreatedAt and drop out — a permanently-stranded tenant no longer wedges
// membership. The synchronous DRAIN passes the zero time (fleet-wide, FAIL-CLOSED): a recency
// window would fail OPEN on a slow drain re-run (rows aging out ⇒ false rc=0, the CRIT-1
// relapse), and drain fences no membership — its over-wait is bounded by the drain deadline.
// Empty/unparseable last_rehome_at is always INCLUDED (fail-closed). nil homeAppliedFn / node
// (the bare unit admin) ⇒ nothing pending, preserving the unit shape.
//
// RESIDUAL-1 (external review round-3 §1 — RECORDED, deliberately unclosed): a purely theoretical
// fail-open survives the fixed origin. If a NEW leader's wall clock is rolled BACK by more than the
// create→migrate elapsed time (an NTP-level >10-15s cross-leader step, inside a window that is
// normally seconds wide), this op's row can stamp last_rehome_at < op.CreatedAt and age out of the
// window → a still-stranded expose is dropped from the gate and RemoveServer completes (fail-open).
// The followups' own optional "tidy-it-up" idea — subtract a clockSkewTolerance from op.CreatedAt — is DECLINED,
// not merely deferred: a lower origin re-admits EVERY unconfirmed row rehomed within
// (CreatedAt−tolerance, CreatedAt), which is exactly the unrelated-stranded-row membership wedge the
// fixed origin exists to eliminate. Back-to-back retires within one tolerance window (a realistic
// fleet reshuffle) would then spuriously hold the second retire on the first's corpse row and route
// it to BLOCKED — real operator friction traded for an incomplete fix (a rollback > tolerance still
// fails open) of a theoretical bug. Per security-pragmatism (docs/architecture safety stance) and the
// reviewer's ruling, the fixed origin — strictly better than the discarded sliding wall-clock window —
// stays as-is and the residual is recorded here. TestPendingRetireConvergenceRecencyWindow pins the
// fixed-origin-vs-sliding-window distinction that makes this the safe choice.
func (a *ClusterAdmin) pendingRetireConvergence(nodeID string, rehomedSince time.Time) ([]rehomedExpose, error) {
	if a.homeAppliedFn == nil || a.node == nil {
		return nil, nil
	}
	type row struct {
		r          rehomedExpose
		lastRehome string
	}
	var moved []row
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(
			`SELECT sid, nid, name, port, epoch, home_broker, last_rehome_at FROM port_allocations
			  WHERE state=? AND home_broker != '' AND home_broker != ? ORDER BY port`,
			string(port.StateAllocated), nodeID)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var m row
			if err := rows.Scan(&m.r.sid, &m.r.nid, &m.r.name, &m.r.port, &m.r.epoch, &m.r.toBroker, &m.lastRehome); err != nil {
				return err
			}
			moved = append(moved, m)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	// Filter to the UNCONFIRMED (and, when a window is given, RECENTLY-rehomed) subset AFTER
	// the rows are closed (the D6 SetMaxOpenConns(1) discipline).
	var pending []rehomedExpose
	for _, m := range moved {
		if a.homeAppliedFn(m.r.port) >= m.r.epoch {
			continue
		}
		if !rehomedSince.IsZero() && !rehomedRecently(m.lastRehome, rehomedSince) {
			continue // an old stranded row unrelated to THIS operation
		}
		pending = append(pending, m.r)
	}
	return pending, nil
}

// clusterTimeLayout is the layout of a cluster.LitTime value (= time.Time.String()),
// which is how both port_allocations.last_rehome_at and cluster_operations.created_at
// are stored. Parsing it round-trips exactly (verified: fractional + non-fractional + UTC).
const clusterTimeLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

// parseClusterTime parses a cluster.LitTime string; ok=false on empty/malformed.
func parseClusterTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(clusterTimeLayout, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// rehomedRecently reports whether a port_allocations.last_rehome_at is at or after
// `since`. An empty or unparseable stamp is treated as RECENT (fail-closed: never drop a
// possibly-relevant row from the convergence gate on a formatting quirk).
func rehomedRecently(lastRehomeAt string, since time.Time) bool {
	t, ok := parseClusterTime(lastRehomeAt)
	if !ok {
		return true
	}
	return !t.Before(since)
}

// emitDrainEvent fires a leader-side drain observability event (nil emitter ⇒ no-op).
func (a *ClusterAdmin) emitDrainEvent(kind string, fields map[string]any) {
	if a.emitEvent != nil {
		a.emitEvent(kind, fields)
	}
}
