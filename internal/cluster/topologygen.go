package cluster

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const MetaKeyTopologyGeneration = state.MetaKeyTopologyGeneration

func TopologyGeneration(ro *sql.DB) (uint64, error) {
	return state.TopologyGeneration(ro)
}
