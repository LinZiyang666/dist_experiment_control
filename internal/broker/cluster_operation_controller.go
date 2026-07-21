package broker

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// mintOpID derives a deterministic, stable operation id from (kind, target, discriminator). For a
// join the discriminator is the joiner's nonce (stable across resume); for a retire it is a leader-
// baked timestamp (a fresh retire after a terminal one gets a new id; the single-active guard prevents
// two concurrent).
func mintOpID(kind, target, discriminator string) string {
	sum := sha256.Sum256([]byte(kind + "|" + target + "|" + discriminator))
	return "op-" + hex.EncodeToString(sum[:8])
}

// assertNoActiveOp refuses a raw membership mutation (AddNode/DrainNode/RemoveNode) when an operation
// is already in flight for the target (no two-writer hole — the controller owns the node's membership
// while an op is active). Leader-agnostic RODB read.
func (a *ClusterAdmin) assertNoActiveOp(target string) error {
	op, err := cluster.ActiveOperationForTarget(a.node.RODB(), target)
	if err != nil {
		return err
	}
	if op != nil {
		return fmt.Errorf("an operation (%s, %s) is already in flight for %q — drive it via `cluster ops`/`apply` or `cluster ops abort %s`", op.OpID, op.OpState, target, op.OpID)
	}
	return nil
}

// StartJoinOperation admits a join from a joiner-local bundle (the `approve`/`apply` leader path): it
// pre-verifies the PoP, refuses a replayed nonce, creates the operation row (single-active-guarded),
// and commits the roster admission (re-verified on every replica). The controller then drives it to
// SERVING. Returns the op_id (= plan-id) for `--wait`.
func (a *ClusterAdmin) StartJoinOperation(bundleStr string) (string, error) {
	if upgradeActive(a.node.RODB()) {
		return "", fmt.Errorf("cluster join: a `cluster upgrade` roll is in progress — retry after it completes (External-review B2)")
	}
	b, err := cluster.DecodeJoinBundle(bundleStr)
	if err != nil {
		return "", err
	}
	if err := cluster.VerifyBundlePoP(b); err != nil {
		return "", err
	}
	// m1 (review): symmetric with StartRetireOperation — refuse a join of a DIFFERENT node while a `cluster
	// add` grow holds the marker (strict serialize). The marker is joiner-id-valued, so the grow's OWN join
	// (b.NodeID == the active joiner) is NOT blocked (the self-op carve-out — else the grow would deadlock its
	// own op). This closes the asymmetry the orchestrator's "join/retire stay BLOCKED" warning promised.
	if j := growActiveJoiner(a.node.RODB()); j != "" && j != b.NodeID {
		return "", fmt.Errorf("cluster join %s: a `cluster add` grow of %q is in progress — retry after it completes (G4 §B)", b.NodeID, j)
	}
	consumed, err := cluster.NonceConsumed(a.node.RODB(), b.NodeID, b.JoinNonce)
	if err != nil {
		return "", err
	}
	if consumed {
		return "", fmt.Errorf("cluster join: nonce already consumed for %q (replay) — `cluster join prepare` mints a fresh one", b.NodeID)
	}
	opID := mintOpID(cluster.OpKindJoin, b.NodeID, b.JoinNonce)
	// C4-M2: if an operation already owns this node, ATTACH idempotently iff it is THIS op_id (a plain
	// re-approve of the same bundle self-heals); a DIFFERENT active op (e.g. a fresh-bundle re-approve
	// while one drives) is refused — never clobber the in-flight identity (two-writer hole).
	if active, err := cluster.ActiveOperationForTarget(a.node.RODB(), b.NodeID); err != nil {
		return "", err
	} else if active != nil && active.OpID != opID {
		return "", fmt.Errorf("cluster join: another operation (%s, %s) is already in flight for %q — `cluster ops abort %s` first", active.OpID, active.OpState, b.NodeID, active.OpID)
	}
	// F1 (external review): PREFLIGHT the deterministic roster-admission constraint leader-side BEFORE
	// consuming operator intent into an op row. The clusterNodeUpsertApplier converts a SQL constraint
	// failure on Apply into a poison-skip no-op (errAppliedRejected → Node.Propose returns nil; §3.7
	// never-wedge), so a downstream caller cannot tell a REJECTED admission from a committed one. The
	// dominant cause is UNIQUE(name): a NEW node joining under a name another node already holds. Reject
	// it up front with an explainable error and NO op row (a re-approve of the SAME node keeps its own
	// name, so owner==b.NodeID is allowed through for idempotent self-heal).
	if owner, err := a.rosterNameOwner(b.Name); err != nil {
		return "", err
	} else if owner != "" && owner != b.NodeID {
		return "", fmt.Errorf("cluster join: name %q is already used by node %q — choose a different --name (nothing was admitted)", b.Name, owner)
	}
	params, _ := json.Marshal(map[string]string{"raft_addr": b.RaftAddr, "public_host": b.PublicHost})
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindJoin, TargetNode: b.NodeID, InitState: cluster.OpStateJoinProofVerified,
			JoinNonce: b.JoinNonce, Params: string(params),
			Timeline: a.initTimeline(cluster.OpStateJoinProofVerified),
		}, a.now())
	}); err != nil {
		return "", fmt.Errorf("cluster join: create operation: %w", err)
	}
	// Verify the active op IS ours (the single-active guard may have no-op'd a racing create) before
	// committing the roster — never return a phantom op_id (M2) or admit under another op.
	active, err := cluster.ActiveOperationForTarget(a.node.RODB(), b.NodeID)
	if err != nil {
		return "", err
	}
	if active == nil || active.OpID != opID {
		return "", fmt.Errorf("cluster join: lost the create race for %q — retry", b.NodeID)
	}
	// Commit the roster row (PoP re-verified on every replica by clusterNodeUpsertApplier). Idempotent:
	// a re-approve re-commits the same identity (self-heals a crash between op-create and roster admit).
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(b.ToUpsertInput(a.now()))
	}); err != nil {
		// A HARD propose error (leadership loss / store error) — not a poison-skip. Tear the op down so a
		// failed admission never leaves a non-terminal op, then surface the error.
		a.abortRejectedJoinOp(active, "roster admission propose failed: "+err.Error())
		return "", fmt.Errorf("cluster join: roster admission: %w", err)
	}
	// F1 BACKSTOP: Node.Propose returns nil even when the FSM POISON-SKIPPED a deterministic constraint
	// failure (errAppliedRejected → committed-but-no-op). VERIFY the roster row actually reflects this
	// admission (exists + carries the bundle's identity) before reporting success. If it was rejected,
	// drive the op to a terminal ABORTED (no lingering active op the controller would spin on) and return
	// an explainable synchronous error — never a usable op_id for an admission that did not happen.
	if admitted, err := a.rosterAdmitted(b.NodeID, b.NodeIdentPub); err != nil {
		return "", err
	} else if !admitted {
		a.abortRejectedJoinOp(active, "roster admission rejected by the FSM (duplicate name/identity or constraint)")
		return "", fmt.Errorf("cluster join: roster admission for %q was rejected (name %q likely already in use, or an identity/constraint conflict) — nothing was admitted; the operation was aborted", b.NodeID, b.Name)
	}
	return opID, nil
}

// rosterNameOwner returns the node_id that already holds `name` in the replicated roster ("" if none) —
// a leader-side preflight of the UNIQUE(name) constraint the baked upsert would otherwise hit as a
// deterministic poison-skip (F1). Leader RODB read.
func (a *ClusterAdmin) rosterNameOwner(name string) (string, error) {
	var owner string
	err := a.node.RODB().QueryRow(`SELECT node_id FROM cluster_nodes WHERE name = ?`, name).Scan(&owner)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("cluster join: check name uniqueness: %w", err)
	}
	return owner, nil
}

// rosterAdmitted reports whether the roster row for nodeID exists AND carries the expected node-identity
// pubkey — the post-propose proof that the admission COMMITTED rather than being poison-skipped (F1).
func (a *ClusterAdmin) rosterAdmitted(nodeID, wantIdentPub string) (bool, error) {
	var gotPub string
	err := a.node.RODB().QueryRow(`SELECT node_ident_pub FROM cluster_nodes WHERE node_id = ?`, nodeID).Scan(&gotPub)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cluster join: verify roster admission: %w", err)
	}
	return gotPub == wantIdentPub, nil
}

// abortRejectedJoinOp drives a just-created join op to terminal ABORTED so a rejected admission never
// leaves a non-terminal operation behind (F1). Best-effort: a transition failure (e.g. concurrent
// leadership loss) is logged, not fatal — the operator can `cluster ops abort` any residual.
func (a *ClusterAdmin) abortRejectedJoinOp(op *cluster.Operation, reason string) {
	if op == nil {
		return
	}
	if err := a.transition(op, cluster.OpStateAborted, true, reason, nil); err != nil {
		a.logger.Warn("cluster join: abort rejected-admission op", "op", op.OpID, "err", err)
	}
}

// StartRetireOperation creates a recoverable retire operation. The F==0 typed-confirm + refuse-last-
// voter gates are checked SYNCHRONOUSLY here (so the CLI prompts immediately), AND re-run on every
// drive by the controller. Returns the op_id for `--wait`.
func (a *ClusterAdmin) StartRetireOperation(target string, confirmed bool) (string, error) {
	if upgradeActive(a.node.RODB()) {
		return "", fmt.Errorf("cluster retire %s: a `cluster upgrade` roll is in progress — retry after it completes (External-review B2)", target)
	}
	if j := growActiveJoiner(a.node.RODB()); j != "" {
		return "", fmt.Errorf("cluster retire %s: a `cluster add` grow of %q is in progress — retry after it completes (G4 §B)", target, j)
	}
	if err := a.assertNoActiveOp(target); err != nil {
		return "", err
	}
	if err := a.requireClusterNode(target); err != nil {
		return "", fmt.Errorf("cluster retire %s: %w", target, err)
	}
	// C4-m3: only a VOTER or an already-DRAINING node may be retired (the legacy DrainNode hard-fails a
	// CATCHING_UP / VOTER_ADD_FAILED node; the op machine's conditional phase bumps would silently skip).
	if phase, ok := a.nodePhase(target); !ok {
		return "", fmt.Errorf("cluster retire %s: no such roster node", target)
	} else if phase != phaseVoter && phase != phaseDraining {
		return "", fmt.Errorf("cluster retire %s: node is %s; only a VOTER or DRAINING node may be retired (use `cluster recovery node remove --manual` for a failed join)", target, phase)
	}
	voters, err := a.node.NumVoters()
	if err != nil {
		return "", fmt.Errorf("cluster retire %s: count voters: %w", target, err)
	}
	proj := ProjectQuorum(voters, true)
	if proj.Voters < 1 {
		return "", fmt.Errorf("cluster retire %s: cannot retire the last voter (force-single/recover territory, not retire)", target)
	}
	if proj.FaultTolerance == 0 && !confirmed {
		return "", &ErrQuorumConfirmRequired{Proj: proj}
	}
	opID := mintOpID(cluster.OpKindRetire, target, a.now().UTC().Format(time.RFC3339Nano))
	if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindRetire, TargetNode: target, InitState: cluster.OpStateDrainRequested,
			Confirmed: confirmed, ConfirmedFT: int64(proj.FaultTolerance),
			Timeline: a.initTimeline(cluster.OpStateDrainRequested),
		}, a.now())
	}); err != nil {
		return "", fmt.Errorf("cluster retire %s: create operation: %w", target, err)
	}
	// m6: read back the active op — never return a phantom op_id from a racing no-op insert.
	active, err := cluster.ActiveOperationForTarget(a.node.RODB(), target)
	if err != nil {
		return "", err
	}
	if active == nil {
		return "", fmt.Errorf("cluster retire %s: operation did not persist — retry", target)
	}
	return active.OpID, nil
}

// ConfirmOp re-confirms / retries a BLOCKED op (C4-M6: works for BOTH kinds). A retire re-acknowledges
// the worsened fault tolerance and re-enters the ladder; a BLOCKED join (catch-up deadline / AddVoter
// retries exhausted) re-enters CATCHING_UP with a FRESH barrier + deadline so a slow-but-healthy
// joiner is recoverable, not a permanent dead-end.
func (a *ClusterAdmin) ConfirmOp(opID string) error {
	op, err := cluster.OperationByID(a.node.RODB(), opID)
	if err != nil {
		return err
	}
	if op == nil || op.Terminal {
		return fmt.Errorf("operation %q is not an in-flight op", opID)
	}
	a.clearOpAttempts(opID)
	if op.Kind == cluster.OpKindRetire {
		voters, err := a.node.NumVoters()
		if err != nil {
			return fmt.Errorf("count voters: %w", err)
		}
		ft := int64(ProjectQuorum(voters, true).FaultTolerance)
		if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterOpConfirm(opID, ft, a.now())
		}); err != nil {
			return err
		}
		if op.OpState == cluster.OpStateBlocked {
			// F2 (external re-review): a confirmed retry must get a FRESH convergence window. Reset
			// catchup_deadline to 0 so boundRehomeConvergence re-stamps a new deadline on the next
			// REHOME_EXPOSES hold — otherwise the op re-enters carrying its OLD, already-expired deadline
			// and boundRehomeConvergence re-BLOCKs it on the very next tick, making `cluster ops confirm` a
			// no-op. (catchup_deadline only persists alongside SetBarrier — see operation_ops.go.)
			return a.transition(op, cluster.OpStateDrainRequested, false, "", func(in *cluster.OpTransitionInput) {
				in.SetBarrier = true
				in.Barrier = op.Barrier
				in.CatchupDeadline = 0
				in.TopoTargetGen = op.TopoTargetGen
			})
		}
		return nil
	}
	// Join: re-enter at ROSTER_COMMITTED (NOT CATCHING_UP) so the ladder re-captures the barrier AND
	// re-issues the idempotent AddVoter at RAFT_ADDING — recovering a join BLOCKED at EITHER the
	// catch-up deadline OR exhausted AddVoter attempts (the closure-review M6 residual).
	if op.Kind == cluster.OpKindJoin && op.OpState == cluster.OpStateBlocked {
		return a.transition(op, cluster.OpStateRosterCommitted, false, "", nil)
	}
	return fmt.Errorf("operation %q (%s/%s) is not awaiting a confirm", opID, op.Kind, op.OpState)
}

// AbortOp transitions a non-terminal op to ABORTED (predecessor-CAS), freeing the per-node active slot
// WITHOUT touching the substrate (the membership stays whatever the gates left it; reconcile/doctor
// heals). The stuck-op escape hatch.
func (a *ClusterAdmin) AbortOp(opID string) error {
	op, err := cluster.OperationByID(a.node.RODB(), opID)
	if err != nil {
		return err
	}
	if op == nil {
		return fmt.Errorf("operation %q not found", opID)
	}
	if op.Terminal {
		return nil // already terminal — idempotent
	}
	return a.transition(op, cluster.OpStateAborted, true, "aborted by operator", nil)
}

// initTimeline bakes the single-entry initial timeline for a new op.
func (a *ClusterAdmin) initTimeline(state string) string {
	return appendTimeline("[]", opTimelineEntry{S: state, T: a.now().UTC().Format(time.RFC3339)})
}

// cluster_operation_controller.go (C4) — the leader-gated driver of the replicated operation log. It
// reads each non-terminal cluster_operations row, derives the live substrate (cluster_nodes.phase +
// raft config + topology_generation), computes the NEXT idempotent transition, performs its
// side-effect (reusing the EXISTING D7/C3 primitives), and commits the cursor advance AFTER the
// substrate fact lands (advance-after-observe). A kill-9 / leadership change resumes by re-deriving
// from the substrate — no double-apply, no orphaned half-state. Every retire safety gate is re-run on
// EVERY drive (never trusted from a stale plan).

const (
	opTimelineCap    = 32              // cap the durable in-row timeline
	opCatchupTimeout = 2 * time.Minute // join catch-up base deadline (small DBs / fresh joiners)
	opMaxAttempts    = 5               // bounded retry of a failing side-effect before BLOCKED (C4-M8)
	// opTopoConvergeTimeout (#45) bounds the NATS_ROLLED_OUT topology-convergence gate. Derivation: the
	// slowest LEGITIMATE way a voter observes a new topology generation is a full nats.conf re-render plus a
	// broker restart, and the product's own bound on that is upgradeConvergeTimeout (3 min — past it the
	// rolling upgrade HALTs the host). 5 min gives that a comfortable margin while still turning a wedge that
	// used to be permanent into a BLOCKED op an operator can confirm or abort. It does NOT apply to the N=1
	// de-cluster boundary, which is carved out by design — see topoAdvance.
	opTopoConvergeTimeout = 5 * time.Minute
	// opRehomeConvergeTimeout (self-review high) bounds the REHOME_EXPOSES data-plane-convergence hold so a
	// migrated expose whose agent never reconnects routes a retire to BLOCKED instead of fencing the whole
	// membership plane forever. Generous (10 min): a NATted agent may take a while to notice + re-dial the new
	// home, and BLOCKED is only a loud, `cluster ops confirm/abort`-actionable pause, not a failure — so err
	// toward waiting. A genuinely dead agent still hits it and surfaces.
	opRehomeConvergeTimeout = 10 * time.Minute
	// #7: the fixed 2-min deadline false-BLOCKs a large/slow-but-healthy joiner whose InstallSnapshot takes
	// longer over a WAN. Scale the persisted catch-up deadline by the command-domain DB size at a conservative
	// transfer floor, clamped to a max window, so a big cluster gets proportionally more time without ever
	// waiting unboundedly on a genuinely dead joiner.
	opCatchupMaxWindow   = 30 * time.Minute // upper clamp on the size-scaled deadline
	opCatchupBytesPerSec = 512 * 1024       // conservative WAN InstallSnapshot floor (512 KiB/s)
)

// opTimelineEntry is one durable transition record (compact keys — the row is capped).
type opTimelineEntry struct {
	S string `json:"s"`           // op_state
	T string `json:"t"`           // RFC3339 timestamp
	E string `json:"e,omitempty"` // error/note
}

// substrate is the live membership ground truth for one target node (the membership SSOT).
type substrate struct {
	phase          string // cluster_nodes.phase ("" = absent)
	inRaft         bool   // present in the raft configuration
	isVoter        bool
	numVoters      int
	isLeaderTarget bool // the target node IS the current raft leader
}

// driveLeaderMaintenance is the per-tick leader-gated cluster maintenance the observe loop runs: resume
// in-flight operations AND re-converge client discovery seeds. Grouping them keeps the wiring explicit
// and unit-testable (a half-wired converge would silently leave seeds stale).
//
// #46 / SB-91: the per-grow seed converge (OpStateNatsRolledOut below) fires at most ONCE and is gated
// behind topoAdvance; the ONLY other re-trigger was the leadership-ACQUIRED edge, which never re-fires on
// a stable single-leader cluster. So the LAST-grown voter (e.g. the 3rd) could reach VOTER yet never enter
// `seeds show` — an earlier voter is silently rescued by the NEXT grow re-deriving the full set, but the
// final one has no successor. Re-deriving every leader tick closes the hole for grow/retire/force-single
// uniformly: it is a cheap, idempotent no-op once converged (the seedSetEqual change-gate skips the Propose
// so seed_generation never churns) and adds NO goroutine (it runs inline on the existing observe ticker).
func (a *ClusterAdmin) driveLeaderMaintenance() {
	if a == nil {
		return
	}
	a.driveInFlightOperations()
	if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil {
		a.logger.Debug("cluster: periodic seed converge", "err", serr)
	}
}

// driveInFlightOperations is the leader-gated tick: it advances every non-terminal operation by one
// idempotent step. Called from the observe loop (leader-edge + each tick). A non-leader is a no-op.
func (a *ClusterAdmin) driveInFlightOperations() {
	if a == nil || a.node == nil || !a.node.IsLeader() {
		return
	}
	// External-review round2 B2: a cluster-scoped `cluster upgrade` roll lock is a real MUTEX, not just a
	// start-gate. While it is held, FREEZE every non-terminal membership op — do NOT drive phase
	// transitions / AddVoter / drain / RemoveServer — so an already-in-flight join/retire cannot cross the
	// rolling restart. The op state persists in raft; driving resumes automatically once the lock releases.
	if upgradeActive(a.node.RODB()) {
		return
	}
	// G4 §B / review M6: do NOT add a growActive() freeze here. Unlike upgrade, a `cluster add` grow DRIVES its
	// OWN OpKindJoin op through this loop (AddNonvoter → catch-up → AddVoter); freezing on growActive would
	// deadlock the grow's own op forever (it never reaches SERVING, the marker never releases). Grow serializes
	// against OTHER membership ops at the entry points (StartJoinOperation/StartRetireOperation refuse a
	// different-node op while the marker is held), not by freezing the driver — that is the deliberate Q7 carve-out.
	ops, err := cluster.NonTerminalOperations(a.node.RODB())
	if err != nil {
		a.logger.Warn("cluster op controller: list non-terminal", "err", err)
		return
	}
	for i := range ops {
		a.driveOne(&ops[i])
	}
}

// driveOne advances ONE operation by a single transition (best-effort; errors are recorded in
// last_error + surfaced via `ops show`, never fatal to the loop).
func (a *ClusterAdmin) driveOne(op *cluster.Operation) {
	// R1: the controller SNAPSHOTS non-terminal operations (driveInFlightOperations) before executing
	// any side effect; an AbortOp — or any transition — may COMMIT between that snapshot and now. Re-read
	// the LATEST replicated op state and bail if it became terminal/absent, so an irreversible raft side
	// effect (AddNonvoter/AddVoter) never runs for an operation that is no longer in flight. driveJoin
	// re-confirms again immediately before each AddNonvoter/AddVoter to close the residual TOCTOU.
	fresh, err := cluster.OperationByID(a.node.RODB(), op.OpID)
	if err != nil || fresh == nil || fresh.Terminal {
		return
	}
	op = fresh
	sub, err := a.readSubstrate(op.TargetNode)
	if err != nil {
		a.logger.Warn("cluster op controller: read substrate", "op", op.OpID, "err", err)
		return
	}
	switch op.Kind {
	case cluster.OpKindJoin:
		a.driveJoin(op, sub)
	case cluster.OpKindRetire:
		a.driveRetire(op, sub)
	}
}

// opStillLive re-reads an operation's LATEST replicated terminal state (review R1) so an irreversible
// side effect is skipped when an AbortOp/transition committed since the controller's snapshot.
// Fail-closed: any read error or absent op counts as "not live" (do not run the side effect).
func (a *ClusterAdmin) opStillLive(opID string) bool {
	cur, err := cluster.OperationByID(a.node.RODB(), opID)
	return err == nil && cur != nil && !cur.Terminal
}

// readSubstrate reads the membership ground truth for a target (bounded-stale leader read).
func (a *ClusterAdmin) readSubstrate(target string) (substrate, error) {
	var s substrate
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		var phase string
		err := db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id = ?`, target).Scan(&phase)
		if err == nil {
			s.phase = phase
		} else if err != sql.ErrNoRows {
			return err
		}
		return nil
	}); err != nil {
		return s, err
	}
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return s, err
	}
	for _, srv := range cfg {
		if srv.NodeID == target {
			s.inRaft = true
			s.isVoter = srv.Voter
		}
	}
	if nv, err := a.node.NumVoters(); err == nil {
		s.numVoters = nv
	}
	if _, leaderID := a.node.LeaderWithID(); leaderID == target {
		s.isLeaderTarget = true
	}
	return s, nil
}

// transition commits one predecessor-CAS cursor advance with a fresh timeline entry. The new timeline
// is computed leader-side (current + entry, capped) and baked as a literal — deterministic on every
// replica. opt mutators (SetBarrier) are passed through.
func (a *ClusterAdmin) transition(op *cluster.Operation, to string, terminal bool, lastErr string, mut func(*cluster.OpTransitionInput)) error {
	tl := appendTimeline(op.Timeline, opTimelineEntry{S: to, T: a.now().UTC().Format(time.RFC3339), E: lastErr})
	in := cluster.OpTransitionInput{
		OpID: op.OpID, FromState: op.OpState, ToState: to, Terminal: terminal, LastError: lastErr, Timeline: tl,
	}
	if mut != nil {
		mut(&in)
	}
	return a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpTransition(in, a.now())
	})
}

// recordOpError records last_error WITHOUT a state change (stays in the current state for the next
// drive). C4-M4: it is CHANGE-GATED — if last_error is already this message it does NOT re-write (a
// self-CAS would commit a raft entry every 5s tick while merely waiting, violating idle-zero-writes).
func (a *ClusterAdmin) recordOpError(op *cluster.Operation, stepErr error) {
	if stepErr == nil || op.LastError == stepErr.Error() {
		return
	}
	_ = a.transition(op, op.OpState, false, stepErr.Error(), nil)
}

func appendTimeline(cur string, e opTimelineEntry) string {
	var entries []opTimelineEntry
	_ = json.Unmarshal([]byte(cur), &entries)
	entries = append(entries, e)
	if len(entries) > opTimelineCap {
		entries = entries[len(entries)-opTimelineCap:]
	}
	b, err := json.Marshal(entries)
	if err != nil {
		return cur
	}
	return string(b)
}

// ---- JOIN state machine ------------------------------------------------------------------------

func (a *ClusterAdmin) driveJoin(op *cluster.Operation, sub substrate) {
	switch op.OpState {
	case cluster.OpStateBlocked:
		// awaiting an explicit `approve`/`apply` (the bundle's PoP) — the controller does not auto-drive.
		return
	case cluster.OpStateJoinProofVerified:
		// Admission: the PoP was pre-verified at approve; commit the roster row (re-verified on every
		// replica by clusterNodeUpsertApplier). If the row already exists (resume), advance.
		if sub.phase != "" {
			_ = a.transition(op, cluster.OpStateRosterCommitted, false, "", nil)
			return
		}
		// The roster admit itself is performed by the approve/apply caller (it holds the bundle/PoP);
		// the controller only advances once it OBSERVES the row. If it never appears, the op stalls
		// loud (last_error set by the caller). Nothing to do here without the PoP.
		return
	case cluster.OpStateRosterCommitted:
		// C4-BLOCKER-2: capture + PERSIST the catch-up barrier BEFORE any AddVoter, so a kill-9 after
		// AddVoter's config change commits resumes against a REAL goalpost (never promotes a node that
		// has not actually caught up). No side-effect here — only the barrier persist.
		barrier, err := a.captureBarrier()
		if err != nil {
			a.recordOpError(op, fmt.Errorf("capture barrier: %w", err))
			return
		}
		deadline := a.adaptiveCatchupDeadline()
		_ = a.transition(op, cluster.OpStateRaftAdding, false, "", func(in *cluster.OpTransitionInput) {
			in.SetBarrier = true
			in.Barrier = barrier
			in.CatchupDeadline = deadline
			in.TopoTargetGen = op.TopoTargetGen
		})
	case cluster.OpStateRaftAdding:
		// The barrier is persisted now. v0.4.2 wedge-prevention (the keystone): stage the joiner as
		// a NON-VOTER first, not a voter. A nonvoter does not count toward quorum, so adding an
		// UNREACHABLE peer can never wedge the cluster — the config change commits at the OLD quorum
		// and the peer simply never catches up (online-RemoveServer-able). Promotion to VOTER happens
		// in CATCHING_UP, only once the peer is actually caught up (hence reachable). AddNonvoter is
		// idempotent; guard on inRaft (present as voter OR nonvoter) so a staged nonvoter — for which
		// isVoter stays false — progresses past this state instead of looping.
		if !sub.inRaft {
			raftAddr, err := a.nodeRaftAddr(op.TargetNode)
			if err != nil {
				a.recordOpError(op, err)
				return
			}
			if !a.opStillLive(op.OpID) {
				return // R1: aborted between the controller tick and this irreversible AddNonvoter
			}
			// Refresh the leader snapshot before staging the joiner so the joiner installs the LATEST
			// state with the least log replay. The PRIMARY grow-onto-migrated-leader fix is at INIT
			// (clusteroffline.InitFromExisting → cluster.GrowReadySnapshot compacts the log so
			// FirstIndex>1, which is what makes raft choose SEND_SNAP over log replay — a leader migrated
			// by `cluster init --from-existing` direct-seeds DB rows no log entry created, so a joiner
			// that replays from index 1 FK-fail-stops). This call is complementary, NOT the fix: on a
			// short un-compacted log raft.Snapshot writes a snapshot FILE but compacts nothing (the gate
			// is TrailingLogs, not SnapshotThreshold), so it cannot by itself trigger InstallSnapshot —
			// the init/resnapshot compaction does. Runs once per join (guarded by !sub.inRaft); a
			// persistent error blocks (visible); SnapshotForJoin treats ErrNothingNewToSnapshot as success.
			if err := a.node.SnapshotForJoin(); err != nil {
				a.recordOpError(op, fmt.Errorf("pre-join leader snapshot: %w", err))
				return
			}
			if err := a.node.AddNonvoter(op.TargetNode, raftAddr); err != nil {
				a.blockAfterAttempts(op, "AddNonvoter", err)
				return
			}
			return // wait a tick for the config change to reflect in sub.inRaft
		}
		// staged (nonvoter, or already a voter on a resume); phase → CATCHING_UP (mesh enter; bumps
		// topology_generation). Always runs from a pre-mesh phase, so a resume where AddNonvoter
		// committed but this setPhase did not still heals (no stuck PENDING).
		if sub.phase != phaseVoter && sub.phase != phaseCatchingUp {
			if err := a.setPhase(op.TargetNode, phaseCatchingUp, []string{"JOIN_VERIFIED_PENDING_VOTER", "VOTER_ADD_FAILED"}, ""); err != nil {
				a.recordOpError(op, fmt.Errorf("phase->CATCHING_UP: %w", err))
				return
			}
		}
		_ = a.transition(op, cluster.OpStateCatchingUp, false, "", nil)
	case cluster.OpStateCatchingUp:
		// #47 (R7b): EVERY non-advancing outcome of this state must funnel through the deadline check
		// below — including the ones that used to `return` first. A joiner that is perfectly REACHABLE
		// could sit in CATCHING_UP forever through two holes:
		//
		//   (1) a catch-up probe that ERRORS every tick returned before the deadline was ever consulted,
		//       so the persisted deadline simply never fired;
		//   (2) an op whose catchup_deadline is 0 (a row written before #7 persisted one, or any future
		//       path that reaches CATCHING_UP without the ROSTER_COMMITTED capture) had no deadline at all.
		//
		// Either way the op stayed non-terminal, which fences the whole membership plane via
		// assertNoActiveOp — the next `cluster add` is refused "already in flight" and the operator has no
		// state to act on, because BLOCKED is the only state `cluster ops confirm/abort` accepts.
		//
		// The exit criterion is therefore absolute: leave CATCHING_UP, or terminate with an explicit error
		// in bounded time. "It will probably converge eventually" is not an acceptable third option.
		if a.driveCatchingUp(op, sub) {
			return // the op actually changed state this tick
		}
		a.boundCatchingUp(op)
		return
	case cluster.OpStateNatsRolledOut:
		if !a.topoAdvance(op, sub, true) {
			return
		}
		// C4-m1: clear force_single_active BEFORE the terminal transition — a kill-9 between a terminal
		// SERVING and a later clear would strand a stale FORCE_SINGLE marker (the op is never revisited).
		if nv, err := a.node.NumVoters(); err == nil && nv > 1 {
			_ = a.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClearForceSingle() })
		}
		// G4 §B (P7): converge discovery seeds on the ASYNC join path before terminal SERVING. This is
		// retryable state-machine work, not best-effort decoration: once the op is terminal the controller
		// never revisits it, so swallowing a transient proposal failure leaves the endpoint stale forever.
		// Round-5 S5-15: escalate like every other BLOCKING step in this file. recordOpError only rewrites
		// last_error — it never changes state — so a persistent failure here pins the op at NATS_ROLLED_OUT
		// forever (never SERVING, never BLOCKED), which is verbatim gotcha #45 and fences the whole
		// membership plane via assertNoActiveOp. blockAfterAttempts caps at opMaxAttempts and routes to
		// OpStateBlocked, the state `cluster ops confirm/abort` can actually act on.
		if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil {
			a.blockAfterAttempts(op, "converge client seeds after join", serr)
			return
		}
		// A cluster-add lock is safety-critical while the join is in flight, but leaving its release
		// solely to the remote orchestrator creates a permanent serialized-fence when the final NATS
		// reply is lost. Clear the joiner-bound marker through Raft BEFORE making the operation terminal;
		// a crash between these steps is resumed, while a different joiner's marker is protected by
		// PlanClearGrowActive's value predicate. Raw join approve never sets the marker, so this is an
		// idempotent no-op for that path.
		if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClearGrowActive(op.TargetNode)
		}); err != nil {
			// Round-5 S5-15: a blocking terminal gate must escalate to BLOCKED, not retry forever.
			a.blockAfterAttempts(op, "release cluster-add grow lock", err)
			return
		}
		a.clearOpAttempts(op.OpID)
		_ = a.transition(op, cluster.OpStateServing, true, "", nil)
	}
}

// driveCatchingUp performs ONE catch-up/promotion step. It returns true ONLY when the op's STATE actually
// changed this tick; issuing an idempotent AddVoter is deliberately NOT counted as progress, so a promotion
// that never reflects in the raft configuration still runs down the CATCHING_UP deadline instead of looping
// forever (#47).
func (a *ClusterAdmin) driveCatchingUp(op *cluster.Operation, sub substrate) bool {
	// op.Barrier is guaranteed non-zero (persisted at ROSTER_COMMITTED) — the catch-up gate is real.
	caught, err := a.caughtUpFn(op.TargetNode, op.Barrier)
	if err != nil {
		// #47: this used to `return` straight out of driveJoin, so a probe that fails EVERY tick — a
		// permanently unreachable health endpoint on an otherwise-live joiner — skipped the deadline
		// check forever. Now it merely declines to advance, and the caller bounds it.
		a.recordOpError(op, fmt.Errorf("catch-up probe: %w", err))
		return false
	}
	if !caught {
		return false
	}
	// v0.4.2: PROMOTE the caught-up nonvoter to VOTER. AddVoter on an existing server flips
	// its suffrage to Voter — an online config change that NOW commits because the peer is
	// reachable AND caught up (it could not have wedged the cluster while it was a nonvoter).
	// Idempotent; guard on isVoter so an already-promoted resume falls through. Bound retries
	// like the staging step.
	if !sub.isVoter {
		raftAddr, err := a.nodeRaftAddr(op.TargetNode)
		if err != nil {
			a.recordOpError(op, err)
			return false
		}
		if !a.opStillLive(op.OpID) {
			return true // R1: aborted between the controller tick and this irreversible promotion
		}
		if err := a.node.AddVoter(op.TargetNode, raftAddr); err != nil {
			a.blockAfterAttempts(op, "AddVoter (promote)", err)
			return true // blockAfterAttempts owns this failure's bound; do not double-count it
		}
		// The promotion was ISSUED, not observed. Report "no state change" so the deadline keeps
		// running: if the config change never reflects in sub.isVoter we would otherwise re-issue an
		// idempotent AddVoter every tick, forever, with nothing counting it (#47).
		return false
	}
	// Promote to VOTER (from CATCHING_UP; tolerate an already-VOTER resume). Heals any phase.
	if sub.phase == phaseCatchingUp {
		if err := a.setPhase(op.TargetNode, phaseVoter, []string{phaseCatchingUp}, ""); err != nil {
			a.recordOpError(op, fmt.Errorf("phase->VOTER: %w", err))
			return false
		}
	}
	a.toNatsRolledOut(op)
	return true
}

// boundCatchingUp is #47's exit criterion: a join that has not left CATCHING_UP by its persisted deadline
// goes to BLOCKED — a LOUD, operator-actionable state that `cluster ops confirm/abort` can act on — rather
// than staying non-terminal forever and fencing every subsequent membership operation.
//
// An op carrying NO deadline (catchup_deadline == 0) is not blocked: it is given one. A missing deadline is
// a gap in the ladder, not evidence the joiner is dead, and blocking on it would false-fail a healthy join.
// The lazy stamp costs one raft write once per such op and restores the invariant "CATCHING_UP is always
// bounded" for rows that predate #7's capture step.
func (a *ClusterAdmin) boundCatchingUp(op *cluster.Operation) {
	if op.CatchupDeadline == 0 {
		deadline := a.adaptiveCatchupDeadline()
		_ = a.transition(op, cluster.OpStateCatchingUp, false, op.LastError, func(in *cluster.OpTransitionInput) {
			in.SetBarrier = true
			in.Barrier = op.Barrier
			in.CatchupDeadline = deadline
			in.TopoTargetGen = op.TopoTargetGen
		})
		return
	}
	if a.now().UnixNano() > op.CatchupDeadline {
		_ = a.transition(op, cluster.OpStateBlocked, false,
			"catch-up exceeded the deadline — check the joining broker, then `cluster ops confirm "+op.OpID+"` to retry (or `cluster ops abort "+op.OpID+"`)", nil)
	}
}

// boundRehomeConvergence bounds the REHOME_EXPOSES data-plane wait (self-review high). The hold via
// recordOpError is non-terminal FOREVER and never touches opAttempts, so a migrated expose whose agent
// never reconnects would keep a retire in flight indefinitely — and assertNoActiveOp then fences every
// subsequent join/retire, a permanent membership wedge (the #45/#47 anti-pattern the sibling gates already
// bound). Mirror them: on FIRST entry stamp a fresh, crash-safe, replicated convergence deadline by
// re-purposing catchup_deadline (a retire never used it for CATCHING_UP; toNatsRolledOut re-stamps it for
// the later topology phase, so the two windows never overlap). Once the deadline passes, route to BLOCKED —
// a loud, `cluster ops confirm/abort`-actionable state, NONZERO for `cluster retire --wait`.
//
// Returns (blocked, stamped): blocked ⇒ routed to BLOCKED this tick; stamped ⇒ the deadline was just written
// (that transition already recorded the hold, so the caller must not also recordOpError). Both ⇒ the caller
// returns without further work.
func (a *ClusterAdmin) boundRehomeConvergence(op *cluster.Operation, holdErr error) (blocked, stamped bool) {
	if op.CatchupDeadline == 0 {
		deadline := a.now().Add(opRehomeConvergeTimeout).UnixNano()
		// catchup_deadline is only persisted alongside the SetBarrier capture step (operation_ops.go),
		// so set it and PRESERVE the current barrier + topo target (mirrors boundCatchingUp).
		_ = a.transition(op, cluster.OpStateRehomeExposes, false, holdErr.Error(), func(in *cluster.OpTransitionInput) {
			in.SetBarrier = true
			in.Barrier = op.Barrier
			in.CatchupDeadline = deadline
			in.TopoTargetGen = op.TopoTargetGen
		})
		return false, true
	}
	if a.now().UnixNano() > op.CatchupDeadline {
		_ = a.transition(op, cluster.OpStateBlocked, false,
			"data plane did not converge onto the new home by the deadline — the migrated expose(s) never re-dialed the "+
				"new broker; check the agent, then `cluster ops confirm "+op.OpID+"` to retry (or `cluster ops abort "+op.OpID+"`)", nil)
		return true, false
	}
	return false, false
}

// adaptiveCatchupDeadline (#7) returns the persisted catch-up deadline scaled by the command-domain DB size:
// base + dbBytes/rate, clamped to [base, maxWindow]. A fresh/small joiner keeps the 2-min base; a large
// cluster whose InstallSnapshot legitimately takes longer over a WAN gets proportionally more time, so a
// slow-but-healthy joiner is never false-BLOCKED — while a genuinely dead joiner still hits the max clamp.
// On any DB-size read error it falls back to the fixed base (never a shorter deadline).
func (a *ClusterAdmin) adaptiveCatchupDeadline() int64 {
	d := opCatchupTimeout
	if bytes, err := a.commandDomainDBBytes(); err == nil && bytes > 0 {
		d = opCatchupTimeout + time.Duration(bytes/opCatchupBytesPerSec)*time.Second
		if d > opCatchupMaxWindow {
			d = opCatchupMaxWindow
		}
	}
	return a.now().Add(d).UnixNano()
}

// commandDomainDBBytes estimates the command-domain SQLite size (page_count × page_size) — the InstallSnapshot
// a joiner must transfer + apply. Read-only PRAGMAs on the committed RODB.
func (a *ClusterAdmin) commandDomainDBBytes() (int64, error) {
	db := a.node.RODB()
	var pageCount, pageSize int64
	if err := db.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		return 0, err
	}
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		return 0, err
	}
	return pageCount * pageSize, nil
}

// blockAfterAttempts bounds a retried side-effect: it records the error and, after opMaxAttempts
// consecutive failures, transitions the op to BLOCKED (loud, operator-recoverable) instead of
// re-issuing the side-effect every tick forever (C4-M8). The counter is leader-local (reset on a
// leadership change is fine — the new leader re-counts from 0).
func (a *ClusterAdmin) blockAfterAttempts(op *cluster.Operation, what string, stepErr error) {
	a.opAttemptsMu.Lock()
	if a.opAttempts == nil {
		a.opAttempts = map[string]int{}
	}
	a.opAttempts[op.OpID]++
	n := a.opAttempts[op.OpID]
	a.opAttemptsMu.Unlock()
	if n >= opMaxAttempts {
		_ = a.transition(op, cluster.OpStateBlocked, false,
			fmt.Sprintf("%s failed %d times (%v) — fix the target, then `cluster ops confirm %s` to retry or `cluster ops abort %s`", what, n, stepErr, op.OpID, op.OpID), nil)
		return
	}
	a.recordOpError(op, fmt.Errorf("%s (attempt %d/%d): %w", what, n, opMaxAttempts, stepErr))
}

func (a *ClusterAdmin) clearOpAttempts(opID string) {
	a.opAttemptsMu.Lock()
	delete(a.opAttempts, opID)
	a.opAttemptsMu.Unlock()
}

// ---- RETIRE state machine ----------------------------------------------------------------------

func (a *ClusterAdmin) driveRetire(op *cluster.Operation, sub substrate) {
	switch op.OpState {
	case cluster.OpStateBlocked:
		return // awaiting `cluster ops confirm`
	case cluster.OpStateDrainRequested:
		// Re-run the safety gates EVERY drive from CURRENT ground truth (never trust stale consent).
		if !a.retireGatePasses(op, sub) {
			return // gate routed to RETIRE_FAILED / BLOCKED
		}
		_ = a.transition(op, cluster.OpStateNoNewHome, false, "", nil)
	case cluster.OpStateNoNewHome:
		if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			d := a.now().Add(opCatchupTimeout)
			return cluster.PlanClusterDrainSet(op.TargetNode, &d)
		}); err != nil {
			a.recordOpError(op, fmt.Errorf("raise broker_draining: %w", err))
			return
		}
		_ = a.transition(op, cluster.OpStateRehomeExposes, false, "", nil)
	case cluster.OpStateRehomeExposes:
		// Move any exposes still homed here (idempotent: empty on a re-drive once they are
		// already re-pointed). The RETURN value is deliberately discarded — the gate below
		// reads the DURABLE convergence set, because migrateExposes' list self-destructs on
		// the second tick (see pendingRetireConvergence / CRIT-1).
		if _, err := a.migrateExposes(op.TargetNode); err != nil {
			a.recordOpError(op, fmt.Errorf("migrate exposes: %w", err)) // rebuild-OFF refusal surfaces here, loud
			return
		}
		// R8a P1 — the retire verb's rc-semantics gate. `cluster retire` is the
		// RESUMABLE form of drain, so instead of blocking it HOLDS in this state: the
		// op records a last_error, the controller re-drives on the next tick, and the
		// home-delivery pass keeps re-delivering meanwhile. It only leaves this state
		// once every migrated expose's agent has CONFIRMED the new home — so a retire
		// can never walk on to RemoveServer (irreversible) while the data plane is
		// still pointing at the node being removed.
		//
		// CRIT-1: the gate reads the DURABLE pendingRetireConvergence(nodeID), NOT the
		// migrateExposes return value. The latter self-destructs — its query finds rows
		// by home_broker=nodeID, which the migrate just re-pointed, so on the SECOND tick
		// it is empty and the old gate waved the op straight on to RemoveServer with the
		// agent still silent. pendingRetireConvergence re-derives the un-confirmed set
		// from current state every tick, so the hold survives across ticks.
		//
		// The hold is BOUNDED (self-review high): recordOpError alone is non-terminal forever and does
		// NOT increment opAttempts, so a migrated agent that never reconnects would fence the whole
		// membership plane indefinitely. boundRehomeConvergence stamps a crash-safe replicated deadline
		// (re-purposing catchup_deadline, exactly as toNatsRolledOut does — retire never used CATCHING_UP)
		// and routes a run-down retire to BLOCKED, a NONZERO terminal that `cluster retire --wait` sees and
		// `cluster ops confirm/abort` can act on. That is the rc assertion for this verb.
		// F3 (+ round-2 self-review): scope to exposes rehomed at or after THIS op's creation, so an
		// UNRELATED permanently-stranded expose elsewhere (rehomed by an earlier op) cannot freeze this
		// retire. The origin is op.CreatedAt — a FIXED point, NOT a sliding wall-clock window: this op's
		// migrate always runs AFTER creation, so its rows never age out (even across a `cluster ops confirm`
		// retry that restarts the convergence deadline), while a sliding window would let a still-stranded
		// row age out BEFORE the reset deadline fires and silently complete RemoveServer (fail-open). An
		// unparseable created_at falls back to the fleet-wide (zero) window — fail-CLOSED, never fail-open.
		since, _ := parseClusterTime(op.CreatedAt)
		pending, perr := a.pendingRetireConvergence(op.TargetNode, since)
		if perr != nil {
			a.recordOpError(op, fmt.Errorf("check data-plane convergence: %w", perr))
			return
		}
		if len(pending) > 0 {
			a.kickHomeDelivery(pending)
			holdErr := &ErrDataPlaneNotConverged{Verb: "retire", NodeID: op.TargetNode, Pending: pending}
			if blocked, stamped := a.boundRehomeConvergence(op, holdErr); blocked || stamped {
				return // routed to BLOCKED, or just stamped the deadline (the stamp records the hold)
			}
			a.recordOpError(op, holdErr)
			return
		}
		// R6: expose migration can BLOCK; re-confirm the op is live before the phase->DRAINING write.
		if !a.opStillLive(op.OpID) {
			return
		}
		if sub.phase == phaseVoter {
			if err := a.setPhase(op.TargetNode, phaseDraining, []string{phaseVoter}, ""); err != nil {
				a.recordOpError(op, fmt.Errorf("phase->DRAINING: %w", err))
				return
			}
		}
		_ = a.transition(op, cluster.OpStateStreamsAtTarget, false, "", nil)
	case cluster.OpStateStreamsAtTarget:
		ready, err := a.streamsReadyFn(op.TargetNode)
		if err != nil {
			a.recordOpError(op, fmt.Errorf("stream readiness: %w", err))
			return
		}
		if !ready {
			a.recordOpError(op, errors.New("streams not at target replica count yet"))
			return
		}
		_ = a.transition(op, cluster.OpStateSeedWithdrawn, false, "", nil)
	case cluster.OpStateSeedWithdrawn:
		// Advisory only (C2 seeds are free-form, not roster-keyed) — never blocks, and (C4-m5) must NOT
		// set last_error so a healthy retire shows no error. The advisory lives in the runbook, not the op.
		_ = a.transition(op, cluster.OpStateLeaderTransferred, false, "", nil)
	case cluster.OpStateLeaderTransferred:
		if sub.isLeaderTarget {
			// R6: never shed leadership for an op aborted before this tick reached the transfer.
			if !a.opStillLive(op.OpID) {
				return
			}
			if err := a.transferLeadershipOff(op.TargetNode); err != nil {
				a.recordOpError(op, fmt.Errorf("transfer leadership off: %w", err))
				return
			}
			// We just shed leadership; the NEW leader's controller resumes from here.
			return
		}
		_ = a.transition(op, cluster.OpStateRaftRemoved, false, "", nil)
	case cluster.OpStateRaftRemoved:
		// C4-BLOCKER-1 + M3: FINAL re-check of the IRREVERSIBLE-removal gates from current ground truth,
		// using the SAME isVoter-aware decision as DRAIN_REQUESTED. Once the target is no longer a voter
		// (resume after the removal committed), the gate passes (already removed) → fall through to
		// RETIRED instead of false-failing. While it IS still a voter, a worsened F==0 (e.g. a concurrent
		// retire eroded fault tolerance) routes to BLOCKED — never barrels through the irreversible step.
		if sub.isVoter && !a.retireGatePasses(op, sub) {
			return
		}
		if ready, err := a.streamsReadyFn(op.TargetNode); err != nil || !ready {
			a.recordOpError(op, errors.New("streams regressed below target before removal — holding"))
			return
		}
		// R6: the readiness probe can BLOCK, and an operator may abort DURING it (the driveOne-entry
		// re-read cannot see an abort that commits mid-tick). Re-confirm the op is still in flight before
		// ANY irreversible removal side effect (phase->RETIRING / RemoveServer / roster delete) — an
		// ABORTED op must never mutate raft membership or the roster.
		if !a.opStillLive(op.OpID) {
			return
		}
		if sub.phase == phaseDraining {
			if err := a.setPhase(op.TargetNode, phaseRetiring, []string{phaseDraining}, ""); err != nil {
				a.recordOpError(op, fmt.Errorf("phase->RETIRING: %w", err))
				return
			}
		}
		if sub.inRaft {
			if err := a.node.RemoveServer(op.TargetNode); err != nil {
				a.recordOpError(op, fmt.Errorf("raft RemoveServer: %w", err))
				return
			}
		}
		if sub.phase != "" {
			if err := a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
				return cluster.PlanClusterNodeRemove(op.TargetNode, a.now())
			}); err != nil {
				a.recordOpError(op, fmt.Errorf("roster delete: %w", err))
				return
			}
		}
		// Capture the topology generation the removal (mesh-leave) bumped — the NATS_ROLLED_OUT target.
		a.toNatsRolledOut(op)
	case cluster.OpStateNatsRolledOut:
		if !a.topoAdvance(op, sub, false) {
			return
		}
		// clear the drain marker; terminal RETIRED (the op row is the only record of RETIRED).
		_ = a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterDrainSet(op.TargetNode, nil)
		})
		// The roster row is gone and mesh convergence is proven. Make seed withdrawal part of the
		// terminal contract; a best-effort call after/before RETIRED can fail once and is never retried,
		// which leaves signed client discovery advertising the retired host indefinitely (#91 A3).
		// Round-5 S5-15: escalate — an un-escalating retry here pins the retire at NATS_ROLLED_OUT forever
		// (#45), and a wedged retire has no clean escape (the node is already RemoveServer'd and its roster
		// row deleted, so `cluster ops abort` would mark ABORTED a retire that physically completed).
		if err := a.deriveAndConvergeSeedsFromRoster(); err != nil {
			a.blockAfterAttempts(op, "converge client seeds after retire", err)
			return
		}
		a.clearOpAttempts(op.OpID)
		_ = a.transition(op, cluster.OpStateRetired, true, "", nil)
	}
}

// ---- shared helpers ----------------------------------------------------------------------------

// retireGatePasses re-runs the retire safety gates from CURRENT ground truth (C4-BLOCKER-1/M3). It
// returns true only if the op may proceed; otherwise it routes the op to RETIRE_FAILED (last voter)
// or BLOCKED (F==0 needs a fresh confirm) and returns false. isVoter-aware: once the target is no
// longer a voter (resume after removal), the gate passes (the removal already happened).
func (a *ClusterAdmin) retireGatePasses(op *cluster.Operation, sub substrate) bool {
	if !sub.isVoter {
		return true // already removed from the raft config — nothing left to gate
	}
	proj := ProjectQuorum(sub.numVoters, true)
	if proj.Voters < 1 {
		// Clear any drain marker raised earlier (the node stays serving) so it isn't orphaned on a node
		// that did NOT get removed (closure-review minor).
		_ = a.node.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterDrainSet(op.TargetNode, nil) })
		_ = a.transition(op, cluster.OpStateRetireFailed, true, "cannot retire the last voter — force-single/recover territory", nil)
		return false
	}
	if proj.FaultTolerance == 0 && !op.Confirmed {
		_ = a.transition(op, cluster.OpStateBlocked, false,
			"fault tolerance would drop to 0 — `cluster ops confirm "+op.OpID+"` to proceed", nil)
		return false
	}
	return true
}

// topoConvergedForOp reports whether EVERY current raft voter (for retire: every REMAINING voter) has
// observed this op's target topology generation. FAIL-CLOSED: an unreachable/UNKNOWN voter counts as
// NOT converged (never false-greens SERVING/RETIRED). Deliberately differs from computeHealth's
// inlined predicate, which excludes unreachable voters.
func (a *ClusterAdmin) topoConvergedForOp(op *cluster.Operation, joining bool) (bool, string) {
	if op.TopoTargetGen == 0 {
		return true, "" // nothing to converge (no topology managed)
	}
	rep, err := a.StatusReport("ctl-nats")
	if err != nil || rep == nil {
		return false, "topology convergence: status unavailable"
	}
	for _, n := range rep.Nodes {
		if n.Role != "leader" && n.Role != "voter" {
			continue // only voters carry topology
		}
		if !joining && n.NodeID == op.TargetNode {
			continue // retire: skip the leaving node
		}
		if !n.TopoReported || !n.Reachable {
			return false, fmt.Sprintf("topology convergence: voter %q not yet reporting/reachable", n.NodeID)
		}
		if n.TopoObserved < op.TopoTargetGen {
			return false, fmt.Sprintf("topology convergence: voter %q at gen %d < %d", n.NodeID, n.TopoObserved, op.TopoTargetGen)
		}
	}
	return true, ""
}

// topoAdvance is #45's bounded gate. It returns true only when the op may proceed past NATS_ROLLED_OUT.
//
// THE DEFECT (as re-diagnosed in R6 — the earlier ledger entry blamed the wrong state)
// -------------------------------------------------------------------------------------
// topoConvergedForOp is fail-closed on ANY unreachable or non-reporting voter, and it had no counter, no
// deadline and no watchdog behind it. A single voter that never reports its topology generation therefore
// pinned the op at NATS_ROLLED_OUT indefinitely; because the op stays non-terminal, assertNoActiveOp then
// refused the NEXT membership operation with "already in flight", and the shrink/grow spine wedged with no
// state an operator could act on. The stall is at THIS gate — not at rehome/migrate, which is five states
// earlier and had already committed.
//
// WHAT IS DELIBERATELY *NOT* FIXED (and must not be)
// ---------------------------------------------------
// The final retire of an N=2 cluster is an INTENTIONAL two-phase boundary, not a bug. When the leaving
// voter is removed the survivor is alone, yet its on-disk nats.conf is still clustered — and a lone-self
// clustered render is invalid, so the survivor cannot observe the new topology generation until the
// operator explicitly de-clusters (`cluster reconcile --to-standalone --confirm-single`) and the live NATS
// process proves it loaded that conf. The op MUST remain non-terminal across that boundary: going terminal
// would declare a retire complete while the cluster is still mid-de-cluster, and BLOCKING it would fabricate
// a failure out of a correct, documented, operator-gated wait. deploy-tier drill 41 asserts exactly this
// shape (op_state == NATS_ROLLED_OUT && terminal != true), and after `--to-standalone` the same op reaches
// terminal RETIRED on its own.
//
// So the deadline is applied to the UNBOUNDED gate only, and the N→1 de-cluster boundary is carved out by
// its own predicate: a RETIRE whose removal left exactly one voter. A JOIN can never match it (a join at
// NATS_ROLLED_OUT has just promoted its joiner, so there are at least two voters).
func (a *ClusterAdmin) topoAdvance(op *cluster.Operation, sub substrate, joining bool) bool {
	ok, reason := a.topoConvergedForOp(op, joining)
	if ok {
		return true
	}
	if !joining && sub.numVoters <= 1 {
		// The de-cluster boundary. Record WHY we are waiting — an operator reading `cluster ops show`
		// must see a next step, not a bare convergence complaint — and wait indefinitely, on purpose.
		a.recordOpError(op, errors.New(reason+
			" — this is the N=1 de-cluster boundary: the last remaining voter still has a CLUSTERED nats.conf and cannot"+
			" observe the new topology until you run `cluster reconcile --to-standalone --confirm-single` on it."+
			" The retire stays in flight (deliberately) until then"))
		return false
	}
	if op.CatchupDeadline != 0 && a.now().UnixNano() > op.CatchupDeadline {
		_ = a.transition(op, cluster.OpStateBlocked, false,
			"topology convergence exceeded the deadline ("+reason+") — fix the voter, then `cluster ops confirm "+op.OpID+
				"` to retry (or `cluster ops abort "+op.OpID+"`)", nil)
		return false
	}
	a.recordOpError(op, errors.New(reason))
	return false
}

// toNatsRolledOut transitions into NATS_ROLLED_OUT, capturing the CURRENT topology_generation as the
// op's convergence target (the gen this op's membership change bumped).
//
// R7b (#45): it also (re-)stamps catchup_deadline with a FRESH topology-convergence window. The column is
// the op's ACTIVE-PHASE deadline, not a per-state one: CATCHING_UP uses it as the catch-up window, and from
// here on it is the topology window. Re-purposing it is what lets #45 gain a crash-safe, replicated deadline
// with no schema migration — and nothing reads the catch-up meaning past CATCHING_UP. Deriving the window
// from the timeline instead was rejected: recordOpError appends timeline entries carrying the SAME state
// string, so "when did this op enter NATS_ROLLED_OUT" is not recoverable from it.
func (a *ClusterAdmin) toNatsRolledOut(op *cluster.Operation) {
	gen, err := cluster.TopologyGeneration(a.node.RODB())
	if err != nil {
		// C4-M5: do NOT advance with topo_target_gen=0 — that would short-circuit topoConvergedForOp to
		// "converged" (fail-OPEN). Record + retry next tick.
		a.recordOpError(op, fmt.Errorf("read topology generation: %w", err))
		return
	}
	a.clearOpAttempts(op.OpID)
	_ = a.transition(op, cluster.OpStateNatsRolledOut, false, "", func(in *cluster.OpTransitionInput) {
		in.SetBarrier = true
		in.Barrier = op.Barrier
		in.CatchupDeadline = a.now().Add(opTopoConvergeTimeout).UnixNano()
		in.TopoTargetGen = gen
	})
}

// captureBarrier reads the leader's command-domain AppliedIndex under a VerifyLeaderRead barrier (a
// genuinely-writable leader), the catch-up goalpost the joiner must reach.
func (a *ClusterAdmin) captureBarrier() (uint64, error) {
	var barrier uint64
	err := a.node.VerifyLeaderRead(func(*sql.DB) error {
		b, e := a.node.AppliedIndex()
		if e != nil {
			return e
		}
		barrier = b
		return nil
	})
	return barrier, err
}

// nodeRaftAddr reads the target's raft_addr from the SUBSTRATE (cluster_nodes), not from op params.
func (a *ClusterAdmin) nodeRaftAddr(target string) (string, error) {
	var addr string
	err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT raft_addr FROM cluster_nodes WHERE node_id = ?`, target).Scan(&addr)
	})
	if err != nil {
		return "", fmt.Errorf("read raft_addr for %q: %w", target, err)
	}
	if addr == "" {
		return "", fmt.Errorf("node %q has no raft_addr", target)
	}
	return addr, nil
}
