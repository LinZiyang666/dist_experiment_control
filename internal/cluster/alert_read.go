package cluster

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

type ActiveAlert = state.ActiveAlert

func ActiveAlerts(db *sql.DB) ([]ActiveAlert, error) {
	return state.ActiveAlerts(db)
}

func ActiveAlertKeys(db *sql.DB) (map[string]bool, error) {
	return state.ActiveAlertKeys(db)
}

func IsAlertActive(db *sql.DB, dedupKey string) (bool, error) {
	return state.IsAlertActive(db, dedupKey)
}

func DrainingNodes(db *sql.DB) ([]string, error) {
	return state.DrainingNodes(db)
}
