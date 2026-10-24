package cluster

import (
	"database/sql"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const MetaKeySeedEndpoints = state.MetaKeySeedEndpoints

const MetaKeySeedGeneration = state.MetaKeySeedGeneration

const MaxSeedEndpoints = state.MaxSeedEndpoints

func Seeds(ro *sql.DB) (endpoints []string, bootstrap string, gen uint64, err error) {
	return state.Seeds(ro)
}

func PlanClusterSeedsPublish(endpoints []string, bootstrap string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClusterSeedsPublish(endpoints, bootstrap, now)
	return (*Command)(cmd), err
}

func SeedEndpointsDropHosts(endpoints, departedHosts []string) []string {
	return state.SeedEndpointsDropHosts(endpoints, departedHosts)
}
