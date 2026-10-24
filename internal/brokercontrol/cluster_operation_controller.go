package brokercontrol

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// mintOpID derives a deterministic, stable operation id from (kind, target, discriminator). For a
// join the discriminator is the joiner's nonce (stable across resume); for a retire it is a leader-
// baked timestamp (a fresh retire after a terminal one gets a new id; the single-active guard prevents
// two concurrent).
func MintOpID(kind, target, discriminator string) string {
	sum := sha256.Sum256([]byte(kind + "|" + target + "|" + discriminator))
	return "op-" + hex.EncodeToString(sum[:8])
}

// assertNoActiveOp refuses a raw membership mutation (AddNode/DrainNode/RemoveNode) when an operation
// is already in flight for the target (no two-writer hole — the controller owns the node's membership
// while an op is active). Leader-agnostic RODB read.
func (a *ClusterAdmin) AssertNoActiveOp(target string) error {
	op, err := cluster.ActiveOperationForTarget(a.Node.RODB(), target)
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
	if UpgradeActive(a.Node.RODB()) {
		return "", fmt.Errorf("cluster join: a `cluster upgrade` roll is in progress — retry after it completes (External-review B2)")
	}
	b, err := cluster.DecodeJoinBundle(bundleStr)
	if err != nil {
		return "", err
	}
	if err := cluster.VerifyBundlePoP(b); err != nil {
		return "", err
	}
	// B4: the version gate. Position is load-bearing and follows the discipline this function
	// already states below for the roster-name preflight — refuse BEFORE consuming operator
	// intent into an op row, so a rejected joiner leaves no op, no nonvoter and no raft write.
	//
	// Until now this check existed but was wired to `handleAdd`, whose adminsock op
	// (OpClusterAdd) is deliberately unrouted since v0.4.2 — so the gate was product-
	// unreachable while b6_skew_test.go called it directly and stayed permanently green.
	// This is the live path: `cluster join approve` -> StartJoinOperation -> driveJoin.
	if err := VersionSkewRefusal(b.ProtoVer, b.ReleaseVersion, b.NodeID, a.Logger); err != nil {
		return "", err
	}
	// m1 (review): symmetric with StartRetireOperation — refuse a join of a DIFFERENT node while a `cluster
	// add` grow holds the marker (strict serialize). The marker is joiner-id-valued, so the grow's OWN join
	// (b.NodeID == the active joiner) is NOT blocked (the self-op carve-out — else the grow would deadlock its
	// own op). This closes the asymmetry the orchestrator's "join/retire stay BLOCKED" warning promised.
	if j := GrowActiveJoiner(a.Node.RODB()); j != "" && j != b.NodeID {
		return "", fmt.Errorf("cluster join %s: a `cluster add` grow of %q is in progress — retry after it completes (G4 §B)", b.NodeID, j)
	}
	consumed, err := cluster.NonceConsumed(a.Node.RODB(), b.NodeID, b.JoinNonce)
	if err != nil {
		return "", err
	}
	if consumed {
		return "", fmt.Errorf("cluster join: nonce already consumed for %q (replay) — `cluster join prepare` mints a fresh one", b.NodeID)
	}
	opID := MintOpID(cluster.OpKindJoin, b.NodeID, b.JoinNonce)
	// C4-M2: if an operation already owns this node, ATTACH idempotently iff it is THIS op_id (a plain
	// re-approve of the same bundle self-heals); a DIFFERENT active op (e.g. a fresh-bundle re-approve
	// while one drives) is refused — never clobber the in-flight identity (two-writer hole).
	if active, err := cluster.ActiveOperationForTarget(a.Node.RODB(), b.NodeID); err != nil {
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
	if owner, err := a.RosterNameOwner(b.Name); err != nil {
		return "", err
	} else if owner != "" && owner != b.NodeID {
		return "", fmt.Errorf("cluster join: name %q is already used by node %q — choose a different --name (nothing was admitted)", b.Name, owner)
	}
	params, _ := json.Marshal(map[string]string{"raft_addr": b.RaftAddr, "public_host": b.PublicHost})
	if err := a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindJoin, TargetNode: b.NodeID, InitState: cluster.OpStateJoinProofVerified,
			JoinNonce: b.JoinNonce, Params: string(params),
			Timeline: a.InitTimeline(cluster.OpStateJoinProofVerified),
		}, a.Now())
	}); err != nil {
		return "", fmt.Errorf("cluster join: create operation: %w", err)
	}
	// Verify the active op IS ours (the single-active guard may have no-op'd a racing create) before
	// committing the roster — never return a phantom op_id (M2) or admit under another op.
	active, err := cluster.ActiveOperationForTarget(a.Node.RODB(), b.NodeID)
	if err != nil {
		return "", err
	}
	if active == nil || active.OpID != opID {
		return "", fmt.Errorf("cluster join: lost the create race for %q — retry", b.NodeID)
	}
	// Commit the roster row (PoP re-verified on every replica by clusterNodeUpsertApplier). Idempotent:
	// a re-approve re-commits the same identity (self-heals a crash between op-create and roster admit).
	if err := a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(b.ToUpsertInput(a.Now()))
	}); err != nil {
		// A HARD propose error (leadership loss / store error) — not a poison-skip. Tear the op down so a
		// failed admission never leaves a non-terminal op, then surface the error.
		a.AbortRejectedJoinOp(active, "roster admission propose failed: "+err.Error())
		return "", fmt.Errorf("cluster join: roster admission: %w", err)
	}
	// F1 BACKSTOP: Node.Propose returns nil even when the FSM POISON-SKIPPED a deterministic constraint
	// failure (errAppliedRejected → committed-but-no-op). VERIFY the roster row actually reflects this
	// admission (exists + carries the bundle's identity) before reporting success. If it was rejected,
	// drive the op to a terminal ABORTED (no lingering active op the controller would spin on) and return
	// an explainable synchronous error — never a usable op_id for an admission that did not happen.
	if admitted, err := a.RosterAdmitted(b.NodeID, b.NodeIdentPub); err != nil {
		return "", err
	} else if !admitted {
		a.AbortRejectedJoinOp(active, "roster admission rejected by the FSM (duplicate name/identity or constraint)")
		return "", fmt.Errorf("cluster join: roster admission for %q was rejected (name %q likely already in use, or an identity/constraint conflict) — nothing was admitted; the operation was aborted", b.NodeID, b.Name)
	}
	return opID, nil
}

// rosterNameOwner returns the node_id that already holds `name` in the replicated roster ("" if none) —
// a leader-side preflight of the UNIQUE(name) constraint the baked upsert would otherwise hit as a
// deterministic poison-skip (F1). Leader RODB read.
func (a *ClusterAdmin) RosterNameOwner(name string) (string, error) {
	var owner string
	err := a.Node.RODB().QueryRow(`SELECT node_id FROM cluster_nodes WHERE name = ?`, name).Scan(&owner)
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
func (a *ClusterAdmin) RosterAdmitted(nodeID, wantIdentPub string) (bool, error) {
	var gotPub string
	err := a.Node.RODB().QueryRow(`SELECT node_ident_pub FROM cluster_nodes WHERE node_id = ?`, nodeID).Scan(&gotPub)
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
func (a *ClusterAdmin) AbortRejectedJoinOp(op *cluster.Operation, reason string) {
	if op == nil {
		return
	}
	if err := a.Transition(op, cluster.OpStateAborted, true, reason, nil); err != nil {
		a.Logger.Warn("cluster join: abort rejected-admission op", "op", op.OpID, "err", err)
	}
}

// StartRetireOperation creates a recoverable retire operation. The F==0 typed-confirm + refuse-last-
// voter gates are checked SYNCHRONOUSLY here (so the CLI prompts immediately), AND re-run on every
// drive by the controller. Returns the op_id for `--wait`.
func (a *ClusterAdmin) StartRetireOperation(target string, confirmed bool) (string, error) {
	if UpgradeActive(a.Node.RODB()) {
		return "", fmt.Errorf("cluster retire %s: a `cluster upgrade` roll is in progress — retry after it completes (External-review B2)", target)
	}
	if j := GrowActiveJoiner(a.Node.RODB()); j != "" {
		return "", fmt.Errorf("cluster retire %s: a `cluster add` grow of %q is in progress — retry after it completes (G4 §B)", target, j)
	}
	if err := a.AssertNoActiveOp(target); err != nil {
		return "", err
	}
	if err := a.RequireClusterNode(target); err != nil {
		return "", fmt.Errorf("cluster retire %s: %w", target, err)
	}
	// C4-m3: only a VOTER or an already-DRAINING node may be retired (the legacy DrainNode hard-fails a
	// CATCHING_UP / VOTER_ADD_FAILED node; the op machine's conditional phase bumps would silently skip).
	if phase, ok := a.NodePhase(target); !ok {
		return "", fmt.Errorf("cluster retire %s: no such roster node", target)
	} else if phase != PhaseVoter && phase != PhaseDraining {
		return "", fmt.Errorf("cluster retire %s: node is %s; only a VOTER or DRAINING node may be retired (use `cluster recovery node remove --manual` for a failed join)", target, phase)
	}
	voters, err := a.Node.NumVoters()
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
	opID := MintOpID(cluster.OpKindRetire, target, a.Now().UTC().Format(time.RFC3339Nano))
	if err := a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindRetire, TargetNode: target, InitState: cluster.OpStateDrainRequested,
			Confirmed: confirmed, ConfirmedFT: int64(proj.FaultTolerance),
			Timeline: a.InitTimeline(cluster.OpStateDrainRequested),
		}, a.Now())
	}); err != nil {
		return "", fmt.Errorf("cluster retire %s: create operation: %w", target, err)
	}
	// m6: read back the active op — never return a phantom op_id from a racing no-op insert.
	active, err := cluster.ActiveOperationForTarget(a.Node.RODB(), target)
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
	op, err := cluster.OperationByID(a.Node.RODB(), opID)
	if err != nil {
		return err
	}
	if op == nil || op.Terminal {
		return fmt.Errorf("operation %q is not an in-flight op", opID)
	}
	if err := a.ConfirmOpKindGuard(op); err != nil {
		return err
	}
	a.ClearOpAttempts(opID)
	if op.Kind == cluster.OpKindRetire {
		voters, err := a.Node.NumVoters()
		if err != nil {
			return fmt.Errorf("count voters: %w", err)
		}
		ft := int64(ProjectQuorum(voters, true).FaultTolerance)
		if err := a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterOpConfirm(opID, ft, a.Now())
		}); err != nil {
			return err
		}
		if op.OpState == cluster.OpStateBlocked {
			// F2 (external re-review): a confirmed retry must get a FRESH convergence window. Reset
			// catchup_deadline to 0 so boundRehomeConvergence re-stamps a new deadline on the next
			// REHOME_EXPOSES hold — otherwise the op re-enters carrying its OLD, already-expired deadline
			// and boundRehomeConvergence re-BLOCKs it on the very next tick, making `cluster ops confirm` a
			// no-op. The reset is OpVal(0), NOT nil: nil preserves the column, which is exactly the
			// already-expired deadline this branch exists to discard (operation_ops.go).
			return a.Transition(op, cluster.OpStateDrainRequested, false, "", func(in *cluster.OpTransitionInput) {
				in.CatchupDeadline = cluster.OpVal(int64(0))
			})
		}
		return nil
	}
	// Join: re-enter at ROSTER_COMMITTED (NOT CATCHING_UP) so the ladder re-captures the barrier AND
	// re-issues the idempotent AddVoter at RAFT_ADDING — recovering a join BLOCKED at EITHER the
	// catch-up deadline OR exhausted AddVoter attempts (the closure-review M6 residual).
	if op.Kind == cluster.OpKindJoin && op.OpState == cluster.OpStateBlocked {
		return a.Transition(op, cluster.OpStateRosterCommitted, false, "", nil)
	}
	return fmt.Errorf("operation %q (%s/%s) is not awaiting a confirm", opID, op.Kind, op.OpState)
}

// AbortOp transitions a non-terminal op to ABORTED, freeing the per-node active slot WITHOUT touching
// the substrate (the membership stays whatever the gates left it). The stuck-op escape hatch.
//
// origin: batch-c internal review F3. This comment previously said "(predecessor-CAS)" and
// "reconcile/doctor heals", and batch C made both false:
//
//   - It is NOT a predecessor-CAS any more. It goes through cluster.PlanClusterOpAbort, whose only
//     guard is `terminal = 0` — deliberately, so the escape hatch works on an op whose state this
//     binary has never heard of. See that function for why.
//   - "reconcile/doctor heals" promised an automatic healer that did not exist. What batch C actually
//     added is narrow: the `drain-marker` reconcile pass clears a broker_draining marker ONLY when the
//     node has no cluster_nodes row at all (see reconcile_drain_marker.go, which explains why the two
//     wider predicates were rejected). EVERYTHING ELSE this abort leaves behind is the operator's:
//     `cluster drain --abort` to return a DRAINING node to VOTER, `cluster recovery node remove <id>
//     --manual` for a roster row with no raft membership. `cluster doctor` reports both — it does not
//     fix them.
func (a *ClusterAdmin) AbortOp(opID string) error {
	op, err := cluster.OperationByID(a.Node.RODB(), opID)
	if err != nil {
		return err
	}
	if op == nil {
		return fmt.Errorf("operation %q not found", opID)
	}
	if op.Terminal {
		return nil // already terminal — idempotent
	}
	// batch C: PlanClusterOpAbort, NOT transition(). transition() routes through
	// PlanClusterOpTransition, which validates the FROM state against THIS binary's compiled-in
	// validOpStates — so an operator running a binary that predates a state could not abort an op
	// sitting in it, and `driveOne` would not advance it either (no case for its kind). That
	// combination is a permanently non-terminal op, which assertNoActiveOp turns into a permanent
	// fence on that node's membership plane. An escape hatch that only works for states the binary
	// already knows is not an escape hatch.
	tl := AppendTimeline(op.Timeline, OpTimelineEntry{
		S: cluster.OpStateAborted, T: a.Now().UTC().Format(time.RFC3339), E: "aborted by operator"})
	return a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpAbort(op.OpID, "aborted by operator", tl, a.Now())
	})
}

// initTimeline bakes the single-entry initial timeline for a new op.
func (a *ClusterAdmin) InitTimeline(state string) string {
	return AppendTimeline("[]", OpTimelineEntry{S: state, T: a.Now().UTC().Format(time.RFC3339)})
}
