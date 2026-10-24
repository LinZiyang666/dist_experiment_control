package cluster

import (
	"log/slog"

	"github.com/LinZiyang666/tether/internal/cluster/logbridge"
	"github.com/hashicorp/go-hclog"
)

// The leaf owns this responsibility; the facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

func NewRaftLogger(logger *slog.Logger, debug bool) hclog.Logger {
	return logbridge.NewRaftLogger(logger, debug)
}
