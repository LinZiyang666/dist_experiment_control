package cluster

import "github.com/LinZiyang666/tether/internal/cluster/state"

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

func formatUint(n uint64) string {
	return state.FormatUint(n)
}

func parseUint(s string) (uint64, error) {
	return state.ParseUint(s)
}
