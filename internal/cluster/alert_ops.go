package cluster

import (
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const AlertKindManual = state.AlertKindManual

const AlertKindBelowQuorum = state.AlertKindBelowQuorum

const AlertKindBrokerDraining = state.AlertKindBrokerDraining

const AlertKindBrokerDown = state.AlertKindBrokerDown

const AlertKindReplicationDegraded = state.AlertKindReplicationDegraded

const AlertKindDiskPressure = state.AlertKindDiskPressure

const AlertKindRaftLag = state.AlertKindRaftLag

const AlertKindProxyBindStalled = state.AlertKindProxyBindStalled

const AlertSeveritySevere = state.AlertSeveritySevere

const AlertSeverityInfo = state.AlertSeverityInfo

func ValidAlertKind(kind string) bool {
	return state.ValidAlertKind(kind)
}

func ValidAlertSeverity(sev string) bool {
	return state.ValidAlertSeverity(sev)
}

func DedupKeyGlobal(kind string) string {
	return state.DedupKeyGlobal(kind)
}

func DedupKeyNode(kind, node string) string {
	return state.DedupKeyNode(kind, node)
}

func PlanAlertRaise(id, kind, severity, dedupKey, message string, raisedAt time.Time) (*Command, error) {
	cmd, err := state.PlanAlertRaise(id, kind, severity, dedupKey, message, raisedAt)
	return (*Command)(cmd), err
}

func PlanAlertClear(dedupKey string, clearedAt time.Time) (*Command, error) {
	cmd, err := state.PlanAlertClear(dedupKey, clearedAt)
	return (*Command)(cmd), err
}

func PlanAlertAck(dedupKey, ackedBy string, ackedAt time.Time) (*Command, error) {
	cmd, err := state.PlanAlertAck(dedupKey, ackedBy, ackedAt)
	return (*Command)(cmd), err
}
