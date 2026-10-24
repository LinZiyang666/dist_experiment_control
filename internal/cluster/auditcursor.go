package cluster

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const metaKeyAuditPublishedIndex = state.MetaKeyAuditPublishedIndex

func planAuditCheckpointSet(to uint64) (*Command, error) {
	cmd, err := state.PlanAuditCheckpointSet(to)
	return (*Command)(cmd), err
}

func readMetaUintDB(db *sql.DB, key string) (uint64, error) {
	return state.ReadMetaUintDB(db, key)
}
