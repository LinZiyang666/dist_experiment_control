package broker

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
const phasePending = brokercontrol.PhasePending
const phaseCatchingUp = brokercontrol.PhaseCatchingUp
const phaseVoter = brokercontrol.PhaseVoter
const phaseAddFailed = brokercontrol.PhaseAddFailed
const phaseDraining = brokercontrol.PhaseDraining
const phaseRetiring = brokercontrol.PhaseRetiring

type ClusterAdmin = brokercontrol.ClusterAdmin

func NewClusterAdmin(node *cluster.Node, logger *slog.Logger) *ClusterAdmin {
	var admin *ClusterAdmin
	admin = brokercontrol.NewClusterAdmin(node, logger, func(db *sql.DB, expected *port.Allocation, target string, now func() time.Time, log *slog.Logger) (int64, *cluster.Command, error) {
		cold := &Broker{cfg: Config{DB: db, Now: now, Logger: log}, cl: &clusterRuntime{node: admin.Node}}
		return planHomeReassign(cold, db, expected, target)
	})
	return admin
}
