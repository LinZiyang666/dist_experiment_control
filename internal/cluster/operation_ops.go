package cluster

import (
	"fmt"
	"time"
)

// operation_ops.go (C4) — the replicated operation-log ops: start / transition / confirm. They ride
// the shared genericExecApplier, so EVERY value is a leader-baked literal (§3.4 determinism, no
// Apply-side reads/clocks). Single-active-op + predecessor-CAS are enforced by baked WHERE guards
// (RowsAffected==0 no-op), NEVER an Apply-time CHECK/UNIQUE constraint — a constraint failure under
// genericExecApplier is a plain error → fsm retry → panic on every replica (the poison-skip path is
// custom-applier-only). Enum validity is validated HERE on the leader bake surface.

// Operation kinds.
const (
	OpKindJoin   = "join"
	OpKindRetire = "retire"
	// OpKindForceSingleFinalize (batch C / C1) drives the RETRY of an online force-single's roster
	// prune after the synchronous attempt failed. The synchronous path is unchanged and still does
	// everything on the happy path; this op exists only because a failed prune used to be a PERMANENT
	// dead end — the code said so itself: "a re-run to retry a failed prune is REFUSED by the dwell
	// gate (CodeQuorumNotLost)". A lingering ghost roster row is not cosmetic: `cluster status` gives
	// it Role "" (it is absent from the raft config), and `reconcile nats --to-standalone` refuses on
	// an unrecognized role — so the survivor's JetStream stays 503 with no way forward.
	OpKindForceSingleFinalize = "force_single_finalize"
)

// Operation workflow states (the named cursor). Join + retire ladders + the shared terminals/blocked.
const (
	// join ladder (PREPARED/PREFLIGHT_OK live in the joiner-local bundle, not replicated). The decoupled
	// `cluster plan add` / `apply <plan-id>` front-end (a PLAN_DRAFTED predecessor) is DESCOPED to a
	// post-C4 follow-up — the ergonomic `join prepare`/`approve` already meets success metric #1
	// ("one prepare + one approve"); see docs/reviews/c4-review.md.
	OpStateJoinProofVerified = "JOIN_PROOF_VERIFIED"
	OpStateRosterCommitted   = "ROSTER_COMMITTED"
	OpStateRaftAdding        = "RAFT_ADDING"
	OpStateCatchingUp        = "CATCHING_UP"
	OpStateNatsRolledOut     = "NATS_ROLLED_OUT" // shared by join + retire (C3 convergence)
	OpStateServing           = "SERVING"         // join terminal

	// retire ladder.
	OpStateDrainRequested    = "DRAIN_REQUESTED"
	OpStateNoNewHome         = "NO_NEW_HOME"
	OpStateRehomeExposes     = "REHOME_EXPOSES"
	OpStateStreamsAtTarget   = "STREAMS_AT_TARGET"
	OpStateSeedWithdrawn     = "SEED_WITHDRAWN"
	OpStateLeaderTransferred = "LEADER_TRANSFERRED"
	OpStateRaftRemoved       = "RAFT_REMOVED"
	OpStateRetired           = "RETIRED" // retire terminal (NO substrate — the op row is its only record)

	// force-single finalize ladder (batch C). Entered only when the SYNCHRONOUS prune failed.
	//
	// It deliberately has NO blocked state. Today a failed prune leaves ghosts and a loud log line,
	// and the operator still has `cluster recovery node remove <ghost>` as a deterministic finalizer.
	// A non-terminal op would be WORSE than that: assertNoActiveOp fences the target's membership
	// plane, and an operator standing over a freshly force-singled survivor has neither a peer to
	// confirm against nor a reason to look at `cluster ops`. Every exit here is terminal.
	OpStateFSPrunePending = "FS_PRUNE_PENDING" // retrying PlanClusterNodePrune for params.abandoned
	OpStateFSFinalized    = "FS_FINALIZED"     // terminal success: the ghosts are gone and seeds re-derived
	OpStateFSGhostLeft    = "FS_GHOST_LEFT"    // terminal: budget spent, ghosts remain; last_error names the manual finalizer

	// shared non-progress states.
	OpStateRetireFailed = "RETIRE_FAILED" // terminal failure
	OpStateAborted      = "ABORTED"       // operator-aborted terminal (frees the active slot, substrate untouched)
	OpStateBlocked      = "BLOCKED"       // non-terminal stall awaiting operator (e.g. mid-flight re-confirm)
)

var validOpStates = map[string]bool{
	OpStateJoinProofVerified: true, OpStateRosterCommitted: true,
	OpStateRaftAdding: true, OpStateCatchingUp: true, OpStateNatsRolledOut: true, OpStateServing: true,
	OpStateDrainRequested: true, OpStateNoNewHome: true, OpStateRehomeExposes: true,
	OpStateStreamsAtTarget: true, OpStateSeedWithdrawn: true, OpStateLeaderTransferred: true,
	OpStateRaftRemoved: true, OpStateRetired: true,
	OpStateFSPrunePending: true, OpStateFSFinalized: true, OpStateFSGhostLeft: true,
	OpStateRetireFailed: true, OpStateAborted: true, OpStateBlocked: true,
}

// ValidOpKind / ValidOpState are the leader-side bake-surface validators (no sql CHECK).
func ValidOpKind(k string) bool {
	return k == OpKindJoin || k == OpKindRetire || k == OpKindForceSingleFinalize
}
func ValidOpState(s string) bool { return validOpStates[s] }

// OpStartInput is the fully-formed initial operation row the leader inserts.
type OpStartInput struct {
	OpID            string
	Kind            string
	TargetNode      string
	InitState       string
	Confirmed       bool
	ConfirmedFT     int64
	Barrier         uint64
	CatchupDeadline int64 // UnixNano; 0 = none
	TopoTargetGen   uint64
	JoinNonce       string
	Params          string // JSON; NO secrets
	Timeline        string // JSON array; the leader bakes the initial single-entry timeline
}

// PlanClusterOpStart inserts a new operation IFF no non-terminal op already exists for the target
// (single-active guard) and the op_id is fresh. A 2nd concurrent start is a deterministic
// RowsAffected==0 no-op — the caller then ATTACHES to the existing op.
func PlanClusterOpStart(in OpStartInput, now time.Time) (*Command, error) {
	if !ValidOpKind(in.Kind) {
		return nil, fmt.Errorf("cluster: op-start: invalid kind %q", in.Kind)
	}
	if !ValidOpState(in.InitState) {
		return nil, fmt.Errorf("cluster: op-start: invalid state %q", in.InitState)
	}
	if in.OpID == "" || in.TargetNode == "" {
		return nil, fmt.Errorf("cluster: op-start: op_id and target_node required")
	}
	lits, err := LitTextAll(in.OpID, in.Kind, in.TargetNode, in.InitState, in.JoinNonce, in.Params, in.Timeline)
	if err != nil {
		return nil, fmt.Errorf("cluster: op-start: literal: %w", err)
	}
	opID, kind, target, state, nonce, params, timeline := lits[0], lits[1], lits[2], lits[3], lits[4], lits[5], lits[6]
	confirmed := "0"
	if in.Confirmed {
		confirmed = "1"
	}
	nowLit := LitTime(now.UTC())
	sql := `INSERT INTO cluster_operations (op_id, kind, target_node, op_state, confirmed, confirmed_ft, ` +
		`barrier, catchup_deadline, topo_target_gen, join_nonce, params, last_error, timeline, terminal, created_at, updated_at) ` +
		`SELECT ` + opID + `, ` + kind + `, ` + target + `, ` + state + `, ` + confirmed + `, ` + LitInt(in.ConfirmedFT) + `, ` +
		LitInt(int64(in.Barrier)) + `, ` + LitInt(in.CatchupDeadline) + `, ` + LitInt(int64(in.TopoTargetGen)) + `, ` +
		nonce + `, ` + params + `, '', ` + timeline + `, 0, ` + nowLit + `, ` + nowLit + ` ` +
		`WHERE NOT EXISTS (SELECT 1 FROM cluster_operations WHERE target_node = ` + target + ` AND terminal = 0) ` +
		`AND NOT EXISTS (SELECT 1 FROM cluster_operations WHERE op_id = ` + opID + `)`
	return NewCommand(OpClusterOpStart, Stmt(sql)), nil
}

// OpVal returns a pointer to a copy of v so a call site can express "write this value" inline. Go has
// no address-of-literal, and the three columns below must distinguish "write 0" from "leave alone".
func OpVal[T any](v T) *T { return &v }

// OpTransitionInput is one predecessor-CAS state advance.
//
// THE THREE CAPTURE COLUMNS ARE POINTERS, AND THE THIRD STATE IS THE POINT
// -----------------------------------------------------------------------
// Barrier / CatchupDeadline / TopoTargetGen are three-state:
//
//	nil        the column is ABSENT from the SET clause — its stored value is preserved
//	OpVal(v)   the column is written as v
//	OpVal(0)   the column is written as 0 — an EXPLICIT reset
//
// The previous shape was a single `SetBarrier bool` gating all three columns, which the SET clause
// then wrote UNCONDITIONALLY. Its own doc admitted the gap — "0 = leave unchanged is NOT
// expressible — the controller always passes the current values" — and that convention was held
// together by five hand-copied write-back sites in the broker's op controller (eight individual
// field copies). A missed copy did not merely fail to update a column: it wrote ZERO over a live
// value.
//
// For topo_target_gen zero is not a neutral value. topoConvergedForOp opens with
// `if op.TopoTargetGen == 0 { return true }` — "nothing to converge" — while every other branch of
// that function fails CLOSED. So the one value a forgotten copy manufactures is the one value that
// means "converged", and the op announces SERVING/RETIRED with the topology unconverged. Under
// pointers a forgotten field is a column that is not written, i.e. the stored value survives: the
// failure mode drops from "silently destroys state and opens a gate" to "does not update".
//
// Pinned by TestBarrierOnlyCaptureMustNotZeroTopoTargetGen (operation_barrier_columns_test.go),
// which is RED against the bool-gated shape.
//
// A RESET MUST BE WRITTEN AS OpVal(0), NEVER AS nil. `cluster ops confirm` needs catchup_deadline
// back at zero so the next hold re-stamps a fresh convergence window; passing nil there would
// preserve the ALREADY-EXPIRED deadline and the confirm would be a no-op that re-blocks on the very
// next tick. Every call site therefore has to say which of the three it means, which is the whole
// improvement — the bool could not ask the question.
type OpTransitionInput struct {
	OpID      string
	FromState string // predecessor-CAS guard: the UPDATE matches only if op_state == FromState
	ToState   string
	Terminal  bool
	LastError string
	Timeline  string // the controller-computed capped JSON array (current + the new entry)

	Barrier         *uint64 // nil = leave alone; OpVal(v) = write v; OpVal(0) = explicit reset
	CatchupDeadline *int64  // UnixNano. nil = leave alone; OpVal(0) = explicit reset (a fresh window)
	TopoTargetGen   *uint64 // nil = leave alone. NEVER write 0 to "clear" it — see above.
}

// PlanClusterOpTransition advances op_state via predecessor-CAS (UPDATE … WHERE op_state=FromState),
// so a stale/duplicate drive is a RowsAffected==0 no-op (idempotent resume). It rewrites the durable
// timeline + last_error + updated_at, plus whichever of barrier / catchup_deadline / topo_target_gen
// the input actually supplies (see OpTransitionInput: nil means "do not touch that column").
func PlanClusterOpTransition(in OpTransitionInput, now time.Time) (*Command, error) {
	if !ValidOpState(in.FromState) || !ValidOpState(in.ToState) {
		return nil, fmt.Errorf("cluster: op-transition: invalid state %q→%q", in.FromState, in.ToState)
	}
	if in.OpID == "" {
		return nil, fmt.Errorf("cluster: op-transition: op_id required")
	}
	lits, err := LitTextAll(in.OpID, in.FromState, in.ToState, in.LastError, in.Timeline)
	if err != nil {
		return nil, fmt.Errorf("cluster: op-transition: literal: %w", err)
	}
	opID, from, to, lastErr, timeline := lits[0], lits[1], lits[2], lits[3], lits[4]
	terminal := "0"
	if in.Terminal {
		terminal = "1"
	}
	set := `op_state = ` + to + `, terminal = ` + terminal + `, last_error = ` + lastErr +
		`, timeline = ` + timeline + `, updated_at = ` + LitTime(now.UTC())
	// Each capture column is emitted ONLY when the caller supplied one. A column that is absent from
	// the SET clause keeps its stored value, so a forgotten field can no longer zero a live barrier,
	// deadline or topology target. The emitted text stays a plain literal statement, so a shorter SET
	// clause is still a statement every version of the FSM applies identically — no wire concern.
	if in.Barrier != nil {
		set += `, barrier = ` + LitInt(int64(*in.Barrier))
	}
	if in.CatchupDeadline != nil {
		set += `, catchup_deadline = ` + LitInt(*in.CatchupDeadline)
	}
	if in.TopoTargetGen != nil {
		set += `, topo_target_gen = ` + LitInt(int64(*in.TopoTargetGen))
	}
	sql := `UPDATE cluster_operations SET ` + set +
		` WHERE op_id = ` + opID + ` AND op_state = ` + from
	return NewCommand(OpClusterOpTransition, Stmt(sql)), nil
}

// PlanClusterOpAbort is the operator's escape hatch: force ANY non-terminal operation to ABORTED,
// guarded ONLY on `terminal = 0`.
//
// It exists because PlanClusterOpTransition validates the FROM state against this binary's compiled-in
// validOpStates, which quietly made `cluster ops abort` version-fragile: a binary that does not know
// the state an op is sitting in cannot abort it, so a ROLLBACK past the release that introduced a
// state would leave every op in it un-abortable AND un-drivable, with assertNoActiveOp fencing that
// node's membership plane permanently. The escape hatch has to work on states this binary has never
// heard of — that is the entire point of an escape hatch — so it does not consult the enum at all.
// PlanClusterOpConfirm next to it already uses exactly this shape.
func PlanClusterOpAbort(opID, lastErr, timeline string, now time.Time) (*Command, error) {
	if opID == "" {
		return nil, fmt.Errorf("cluster: op-abort: op_id required")
	}
	lits, err := LitTextAll(opID, OpStateAborted, lastErr, timeline)
	if err != nil {
		return nil, fmt.Errorf("cluster: op-abort: literal: %w", err)
	}
	id, to, le, tl := lits[0], lits[1], lits[2], lits[3]
	sql := `UPDATE cluster_operations SET op_state = ` + to + `, terminal = 1, last_error = ` + le +
		`, timeline = ` + tl + `, updated_at = ` + LitTime(now.UTC()) +
		` WHERE op_id = ` + id + ` AND terminal = 0`
	return NewCommand(OpClusterOpTransition, Stmt(sql)), nil
}

// PlanClusterOpConfirm records a (re-)confirm: sets confirmed=1 + the FaultTolerance the operator was
// shown (for the resume worsen-check). Guarded on a non-terminal op (a confirm on a finished op no-ops).
func PlanClusterOpConfirm(opID string, confirmedFT int64, now time.Time) (*Command, error) {
	idLit, err := LitText(opID)
	if err != nil {
		return nil, fmt.Errorf("cluster: op-confirm: literal: %w", err)
	}
	sql := `UPDATE cluster_operations SET confirmed = 1, confirmed_ft = ` + LitInt(confirmedFT) +
		`, updated_at = ` + LitTime(now.UTC()) +
		` WHERE op_id = ` + idLit + ` AND terminal = 0`
	return NewCommand(OpClusterOpConfirm, Stmt(sql)), nil
}
