package brokercontrol

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokercontrol/maintenance"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

func SameHomeAllocation(a, expected *port.Allocation) bool {
	return a != nil && expected != nil && a.Port == expected.Port && a.SID == expected.SID &&
		a.NID == expected.NID && a.Name == expected.Name && a.TokenHash == expected.TokenHash &&
		a.HomeBroker == expected.HomeBroker && a.Epoch == expected.Epoch && a.State == expected.State && a.State == port.StateAllocated
}

// HomeMaintenanceDeferredError marks a move that has not executed. It contains
// only public identities, never a token, proof signature or private state dump.
type HomeMaintenanceDeferredError struct{ Home, CutID string }

func (e *HomeMaintenanceDeferredError) Error() string {
	return "broker: home mutation deferred during maintenance"
}

func ReadHomeMaintenance(db *sql.DB, home string) (string, brokermaint.Record, error) {
	var raw string
	var record brokermaint.Record
	if home == "" {
		return raw, record, nil
	}
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, HomeMaintenancePrefix+home).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", record, nil
	}
	if err != nil {
		return "", record, fmt.Errorf("broker: read home maintenance: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), &record); err != nil || record.Home != home || record.CutID == "" ||
		(record.Phase != brokermaint.MaintPrepared && record.Phase != brokermaint.MaintActive && record.Phase != brokermaint.MaintEnded) {
		return "", brokermaint.Record{}, errors.New("broker: invalid home maintenance record")
	}
	return raw, record, nil
}

// Only P15's already-validated local proof can protect a home. Issuer BOOTTIME
// values are never compared to this machine's clock. An ended record retains
// its original expiry for planned-return classification, but no longer fences writes.
// origin: hot-update S6 plan §7.1 / T43
func MaintainedHome(record brokermaint.Record, lease brokermaint.LocalLease, now int64) bool {
	return record.Phase != brokermaint.MaintEnded && MatchingHomeLease(record, lease) && now < lease.ExpiresLocalBootNS
}

func MatchingHomeLease(record brokermaint.Record, lease brokermaint.LocalLease) bool {
	cached := lease.Record
	// P16 may advance the replicated phase after the original proof was cached.
	// Every other record field (especially fixed expiry, holder and baseline) stays bound.
	cached.Phase = record.Phase
	return reflect.DeepEqual(cached, record) && brokermaint.MatchesLease(lease, record)
}

// guardHomeMutation must run inside Node.Propose. The outside target selection
// is just a hint: both homes' CURRENT records and the entire allocation identity
// are read again under applyMu, then carried to P14's baked SQL guard.
// origin: hot-update S6 plan §7.2 / T41
func GuardHomeMutation(snapshot func() (HomeMutationPlanners, map[string]brokermaint.LocalLease, int64, error), db *sql.DB, expected *port.Allocation, from, to string, absent bool) (port.MutationGuard, HomeMutationPlanners, error) {
	plans, proofs, now, err := snapshot()
	guard := port.MutationGuard{Expected: expected, ExpectAbsent: absent, FromHome: from, ToHome: to,
		ExpectedMaintenance: map[string]string{}, Proofs: proofs, NowBootNS: now}
	if err != nil {
		return guard, plans, err
	}
	if expected != nil {
		current, err := port.LookupByTokenHash(db, expected.TokenHash)
		if err != nil {
			return guard, plans, err
		}
		if !SameHomeAllocation(current, expected) {
			return guard, plans, port.ErrNotFound
		}
	}
	for _, home := range []string{from, to} {
		if home == "" {
			continue
		}
		raw, record, err := ReadHomeMaintenance(db, home)
		if err != nil {
			return guard, plans, err
		}
		guard.ExpectedMaintenance[home] = raw // Empty is explicit absence, also fenced by P14.
		if raw != "" && MaintainedHome(record, proofs[home], now) {
			return guard, plans, &HomeMaintenanceDeferredError{Home: home, CutID: record.CutID}
		}
	}
	return guard, plans, nil
}

// HomeMutationPlanners wires the P14 frozen signatures without importing the isolated
// compile adapters. Until the real planners are supplied, writes fail explicitly;
// falling back to an epoch-only planner would silently bypass the maintenance CAS.
// origin: hot-update S6 plan §7.2 / T41
type HomeMutationPlanners struct {
	Reassign port.PlanReassignHomeGuardedFunc
	Allocate port.PlanAllocateGuardedFunc
	Rotate   port.PlanRotateAllocationGuardedFunc
	Free     port.PlanFreeAllocationGuardedFunc
}

const HomeMaintenancePrefix = "broker_image_maintenance."

// Admission is a control responsibility below both the broker and its image.
// Preserve existing callers while preventing a dependency back into composition.
// origin: hot-update S7 plan §11 / GATE-CONVERGE-layering
type MaintenanceAgent = maintenance.MaintenanceAgent
type MaintenancePeer = maintenance.MaintenancePeer
type LocalMaintenanceConfig = maintenance.LocalMaintenanceConfig

func MaintenanceAgents(db *sql.DB, home string, local bool) ([]MaintenancePeer, error) {
	return maintenance.MaintenanceAgents(db, home, local)
}
func CollectMaintenanceObserver(record brokermaint.Record, peer MaintenancePeer, send func(proto.BrokerMaintenanceReq, func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error), applied func() (uint64, error), sign brokermaint.SignProof, verify brokermaint.VerifySignature) (maintenance.MaintenanceObserver, maintenance.MaintenanceAck, error) {
	return maintenance.CollectMaintenanceObserver(record, peer, send, applied, sign, verify)
}
func EstablishLocalMaintenance(record brokermaint.Record, peers []MaintenancePeer, commit func(brokermaint.Record) error, collect func(brokermaint.Record, MaintenancePeer) (maintenance.MaintenanceObserver, maintenance.MaintenanceAck, error), sign brokermaint.SignProof, verify func(MaintenancePeer) brokermaint.VerifySignature) (brokermaint.LocalLease, error) {
	return maintenance.EstablishLocalMaintenance(record, peers, commit, collect, sign, verify)
}
func MaintenanceWrite(db *sql.DB, node *cluster.Node, plan func(*sql.DB) (*cluster.Command, error)) error {
	return maintenance.MaintenanceWrite(db, node, plan)
}
func AdmitLocalMaintenance(c LocalMaintenanceConfig, d controlbudget.Deadline) (brokermaint.LocalLease, error) {
	return maintenance.AdmitLocalMaintenance(c, d)
}
func MaintenanceNonce() (string, error) { return maintenance.MaintenanceNonce() }
func AdmitRemoteMaintenance(node *cluster.Node, self string, seed []byte, generation uint64, req adminsock.Request, d controlbudget.Deadline, send func(*proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error), snapshot func() (map[string]brokermaint.LocalLease, int64, error), route func(string, storage.RoleReceipt) string, verify func(string) brokermaint.VerifySignature) (brokermaint.Record, int64, error) {
	return maintenance.AdmitRemoteMaintenance(node, self, seed, generation, req, d, send, snapshot, route, verify)
}
func FinishLocalMaintenance(db *sql.DB, node *cluster.Node, lease brokermaint.LocalLease, now time.Time) error {
	return maintenance.FinishLocalMaintenance(db, node, lease, now)
}
func ReadLocalMaintenance(db *sql.DB, home string) (brokermaint.Record, error) {
	return maintenance.ReadLocalMaintenance(db, home)
}
func BindLocalMaintenance(db *sql.DB, lease brokermaint.LocalLease) (brokermaint.LocalLease, error) {
	return maintenance.BindLocalMaintenance(db, lease)
}
