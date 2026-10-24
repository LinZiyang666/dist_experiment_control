package cluster

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

type Operation = state.Operation

func NonTerminalOperations(ro *sql.DB) ([]Operation, error) {
	return state.NonTerminalOperations(ro)
}

func ActiveOperationForTarget(ro *sql.DB, target string) (*Operation, error) {
	return state.ActiveOperationForTarget(ro, target)
}

func LatestOperationForTarget(ro *sql.DB, target string) (*Operation, error) {
	return state.LatestOperationForTarget(ro, target)
}

func OperationByID(ro *sql.DB, opID string) (*Operation, error) {
	return state.OperationByID(ro, opID)
}

func RecentOperations(ro *sql.DB, limit int) ([]Operation, error) {
	return state.RecentOperations(ro, limit)
}

func NonceConsumed(ro *sql.DB, target, nonce string) (bool, error) {
	return state.NonceConsumed(ro, target, nonce)
}
