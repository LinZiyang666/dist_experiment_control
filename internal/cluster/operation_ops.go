package cluster

import (
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const OpKindJoin = state.OpKindJoin

const OpKindRetire = state.OpKindRetire

const OpKindForceSingleFinalize = state.OpKindForceSingleFinalize

const OpStateJoinProofVerified = state.OpStateJoinProofVerified

const OpStateRosterCommitted = state.OpStateRosterCommitted

const OpStateRaftAdding = state.OpStateRaftAdding

const OpStateCatchingUp = state.OpStateCatchingUp

const OpStateNatsRolledOut = state.OpStateNatsRolledOut

const OpStateServing = state.OpStateServing

const OpStateDrainRequested = state.OpStateDrainRequested

const OpStateNoNewHome = state.OpStateNoNewHome

const OpStateRehomeExposes = state.OpStateRehomeExposes

const OpStateStreamsAtTarget = state.OpStateStreamsAtTarget

const OpStateSeedWithdrawn = state.OpStateSeedWithdrawn

const OpStateLeaderTransferred = state.OpStateLeaderTransferred

const OpStateRaftRemoved = state.OpStateRaftRemoved

const OpStateRetired = state.OpStateRetired

const OpStateFSPrunePending = state.OpStateFSPrunePending

const OpStateFSFinalized = state.OpStateFSFinalized

const OpStateFSGhostLeft = state.OpStateFSGhostLeft

const OpStateRetireFailed = state.OpStateRetireFailed

const OpStateAborted = state.OpStateAborted

const OpStateBlocked = state.OpStateBlocked

func ValidOpKind(k string) bool {
	return state.ValidOpKind(k)
}

func ValidOpState(s string) bool {
	return state.ValidOpState(s)
}

type OpStartInput = state.OpStartInput

func PlanClusterOpStart(in OpStartInput, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterOpStart(in, now)
	return (*Command)(cmd), err
}

func OpVal[T any](v T) *T {
	return state.OpVal(v)
}

type OpTransitionInput = state.OpTransitionInput

func PlanClusterOpTransition(in OpTransitionInput, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterOpTransition(in, now)
	return (*Command)(cmd), err
}

func PlanClusterOpAbort(opID, lastErr, timeline string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterOpAbort(opID, lastErr, timeline, now)
	return (*Command)(cmd), err
}

func PlanClusterOpConfirm(opID string, confirmedFT int64, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterOpConfirm(opID, confirmedFT, now)
	return (*Command)(cmd), err
}
