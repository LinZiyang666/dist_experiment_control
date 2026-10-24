package brokerimage

import (
	"database/sql"

	"github.com/LinZiyang666/tether/internal/brokercontrol/maintenance"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// Maintenance state belongs below control and image composition. These value
// aliases preserve sealed JSON and the public cut signatures without requiring
// the control owner to import its image consumer.
// origin: hot-update S7 plan §11 / GATE-CONVERGE-layering
const MaxMaintenanceHomes = maintenance.MaxMaintenanceHomes

type MaintenanceBaseline = maintenance.MaintenanceBaseline
type MaintenanceObservation = maintenance.MaintenanceObservation
type MaintenanceSnapshot = maintenance.MaintenanceSnapshot
type MaintenanceCache = maintenance.MaintenanceCache
type MaintenanceObserver = maintenance.MaintenanceObserver
type MaintenanceAck = maintenance.MaintenanceAck

var ErrMaintenanceObserverUnsupported = maintenance.ErrMaintenanceObserverUnsupported

func MaintenanceBaselineDigest(b MaintenanceBaseline) (string, error) {
	return maintenance.MaintenanceBaselineDigest(b)
}
func NewMaintenanceCache(boot string) (*MaintenanceCache, error) {
	return maintenance.NewMaintenanceCache(boot)
}
func RestoreMaintenanceCache(s MaintenanceSnapshot, boot string, verify brokermaint.VerifySignature) (*MaintenanceCache, error) {
	return maintenance.RestoreMaintenanceCache(s, boot, verify)
}
func PlanMaintenanceRecord(db *sql.DB, expected *brokermaint.Record, next brokermaint.Record, baseline MaintenanceBaseline) (*cluster.Command, error) {
	return maintenance.PlanMaintenanceRecord(db, expected, next, baseline)
}
func ReadMaintenanceBaseline(db *sql.DB, record brokermaint.Record) (MaintenanceBaseline, error) {
	return maintenance.ReadMaintenanceBaseline(db, record)
}
func MaintenanceCutDeadline(r brokermaint.Record, boot string, now int64, required []MaintenanceObserver, acks []MaintenanceAck, verify func(MaintenanceObserver, []byte, string) error) (int64, error) {
	return maintenance.MaintenanceCutDeadline(r, boot, now, required, acks, verify)
}
