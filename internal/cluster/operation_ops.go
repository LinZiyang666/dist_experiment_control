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
	OpStateRetireFailed: true, OpStateAborted: true, OpStateBlocked: true,
}

// ValidOpKind / ValidOpState are the leader-side bake-surface validators (no sql CHECK).
func ValidOpKind(k string) bool  { return k == OpKindJoin || k == OpKindRetire }
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

// OpTransitionInput is one predecessor-CAS state advance. Barrier/CatchupDeadline/TopoTargetGen are
// optionally re-baked (0 = leave unchanged is NOT expressible — the controller always passes the
// current values; see SetBarrier to write a freshly-captured barrier).
type OpTransitionInput struct {
	OpID            string
	FromState       string // predecessor-CAS guard: the UPDATE matches only if op_state == FromState
	ToState         string
	Terminal        bool
	LastError       string
	Timeline        string // the controller-computed capped JSON array (current + the new entry)
	SetBarrier      bool   // when true, also write Barrier + CatchupDeadline + TopoTargetGen (capture step)
	Barrier         uint64
	CatchupDeadline int64
	TopoTargetGen   uint64
}

// PlanClusterOpTransition advances op_state via predecessor-CAS (UPDATE … WHERE op_state=FromState),
// so a stale/duplicate drive is a RowsAffected==0 no-op (idempotent resume). It rewrites the durable
// timeline + last_error + updated_at, and on a capture step also persists the barrier/deadline/target.
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
	if in.SetBarrier {
		set += `, barrier = ` + LitInt(int64(in.Barrier)) +
			`, catchup_deadline = ` + LitInt(in.CatchupDeadline) +
			`, topo_target_gen = ` + LitInt(int64(in.TopoTargetGen))
	}
	sql := `UPDATE cluster_operations SET ` + set +
		` WHERE op_id = ` + opID + ` AND op_state = ` + from
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
