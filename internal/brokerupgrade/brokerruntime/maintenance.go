package brokerruntime

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
)

// MaintenanceRecord: the signed outer request grants account authority; the replicated planner
// binds the record to the current holder and its immutable cut identity.
// Observer proof delivery is separate from this durable write and must precede cut.
// The caller supplies the live Node; a broker without one refuses before calling.
// origin: hot-update S6 plan §7.1 / §9.1; S8 §12.1 (P15 trigger net-zero move)
func MaintenanceRecord(node *cluster.Node, req *proto.ClusterUpgradeReq, now time.Time) *proto.ClusterUpgradeResp {
	if req.Maintenance == nil || req.Holder == "" || req.Maintenance.Holder != req.Holder || req.Maintenance.CutID != req.CutID {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "maintenance holder/cut binding invalid"}
	}
	record := *req.Maintenance
	err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		state, e := cluster.ReadUpgradeLock(db)
		if e != nil {
			return nil, e
		}
		marker, e := cluster.UpgradeHolderMarker(req.Holder)
		if e != nil || state.Marker != marker {
			return nil, cluster.ErrUpgradeHolder
		}
		expiry, e := time.Parse(time.RFC3339Nano, state.Lease)
		if e != nil || !now.Before(expiry) {
			return nil, cluster.ErrUpgradeHolder
		}
		if err := cluster.ValidateBrokerMaintenanceTerm(node, record); err != nil {
			return nil, err
		}
		return cluster.PlanBrokerMaintenance(db, record, state)
	})
	if err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "maintenance record refused"}
	}
	actual, err := cluster.ReadBrokerMaintenance(node.RODB(), record.Home)
	if err != nil || actual != record {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeStoreError, Error: "maintenance readback did not match"}
	}
	return &proto.ClusterUpgradeResp{OK: true}
}

// DeliverMaintenanceChallenge delivers already signed challenges to exact session-scoped observers.
// The caller supplies P15's signature/proof verifier; missing verification fails
// closed. A request received is not an installed route hold.
// origin: hot-update S6 plan §7.1 / §7.3
func DeliverMaintenanceChallenge(nc *nats.Conn, sid string, req proto.BrokerMaintenanceReq, timeout time.Duration, verify func(proto.BrokerMaintenanceAck) error) (proto.BrokerMaintenanceAck, error) {
	var ack proto.BrokerMaintenanceAck
	if nc == nil || sid == "" || req.NID == "" || timeout <= 0 || timeout > 8*time.Second || verify == nil || req.Sig == "" || req.Proof.Nonce == "" || req.Record.RequiredCapability != brokermaint.Capability {
		return ack, errors.New("maintenance observer challenge invalid")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return ack, err
	}
	msg, err := nc.Request(proto.SubjCmdForwarded(sid, req.NID, proto.BrokerMaintenanceVerb), body, timeout)
	if err != nil {
		return ack, err
	}
	if json.Unmarshal(msg.Data, &ack) != nil || !ack.Accepted || ack.CutID != req.Record.CutID || ack.Nonce != req.Proof.Nonce || ack.NID != req.NID || ack.ConnectedBrokerID != req.ConnectedBrokerID || ack.IngressServerID != req.IngressServerID || ack.InstanceID != req.Record.InstanceID || ack.RouteDigest != req.RouteDigest || ack.ProofDigest == "" || ack.RemainingNS <= 0 || ack.RemainingNS > req.Record.ExpiresBootNS-req.Proof.ResponseBootNS || ack.Sig == "" {
		return ack, errors.New("maintenance observer ACK does not prove installed hold")
	}
	if err := verify(ack); err != nil {
		return ack, err
	}
	return ack, nil
}

// AwaitMaintenanceRecord: both PREPARED challenges and ACTIVE reloads can overtake their committed
// Raft record on the NATS transport. Wait for those exact bytes within the same RPC;
// a corrupt store or an ended matching cut is terminal, never fresh authority.
// origin: hot-update S6 plan §7.1 / internal review rv-cluster-3
func AwaitMaintenanceRecord(node *cluster.Node, expected brokermaint.Record) error {
	applyUntil := time.Now().Add(500 * time.Millisecond)
	for {
		current, err := cluster.ReadBrokerMaintenance(node.RODB(), expected.Home)
		if err == nil && current == expected {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) || current.CutID == expected.CutID && current.Phase == brokermaint.MaintEnded || !time.Now().Before(applyUntil) {
			return brokermaint.ErrConflict
		}
		time.Sleep(time.Millisecond)
	}
}

// AwaitMaintenanceCursor waits, within the same RPC, for the applied SQL cursor a proof names.
func AwaitMaintenanceCursor(node *cluster.Node, minimum uint64) error {
	applyUntil := time.Now().Add(500 * time.Millisecond)
	for {
		applied, err := node.AppliedIndex()
		if err != nil {
			return err
		}
		if applied >= minimum {
			return nil
		}
		if !time.Now().Before(applyUntil) {
			return brokermaint.ErrIdentity
		}
		time.Sleep(time.Millisecond)
	}
}

// MaintenancePeers snapshots the required cohort before issuing the fixed lifetime. A missing
// capability, stale route, unhealthy proxy or missing voter refuses the cut;
// the cohort is never selected from whichever ACKs happened to arrive.
// origin: hot-update S6 plan §7.1 / rv-cluster-3; S8 §12.1 (P15 net-zero move)
func MaintenancePeers(node *cluster.Node, db *sql.DB, home, accountPub string) ([]brokercontrol.MaintenancePeer, error) {
	config, err := node.RaftConfiguration()
	if err != nil {
		return nil, err
	}
	var peers []brokercontrol.MaintenancePeer
	for _, server := range config {
		if server.Voter {
			peers = append(peers, brokercontrol.MaintenancePeer{ID: "broker-" + server.NodeID, NID: server.NodeID, Subject: proto.SubjCtrlClusterUpgrade(server.NodeID), PublicKey: accountPub})
		}
	}
	agents, err := brokercontrol.MaintenanceAgents(db, home, false)
	if err != nil {
		return nil, err
	}
	peers = append(peers, agents...)
	if len(peers) == 0 || len(peers) > brokerimage.MaxMaintenanceHomes {
		return nil, brokermaint.ErrInvalid
	}
	return peers, nil
}

// CommitMaintenanceRecord replicates the record and its baseline under the held, unexpired roll lock
// and the current term, then requires an exact readback. now is read inside the writer's plan.
func CommitMaintenanceRecord(node *cluster.Node, record brokermaint.Record, baseline brokerimage.MaintenanceBaseline, now func() time.Time) error {
	err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		lock, err := cluster.ReadUpgradeLock(db)
		if err != nil {
			return nil, err
		}
		expiry, err := time.Parse(time.RFC3339Nano, lock.Lease)
		if err != nil || !now().Before(expiry) {
			return nil, cluster.ErrUpgradeHolder
		}
		if err := cluster.ValidateBrokerMaintenanceTerm(node, record); err != nil {
			return nil, err
		}
		previous, err := cluster.ReadBrokerMaintenance(db, record.Home)
		var expected *brokermaint.Record
		if err == nil {
			expected = &previous
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		baselinePlan, err := brokerimage.PlanMaintenanceRecord(db, expected, record, baseline)
		if err != nil {
			return nil, err
		}
		command, err := cluster.PlanBrokerMaintenance(db, record, lock)
		if err != nil {
			return nil, err
		}
		command.Body = append(command.Body, baselinePlan.Body[1:]...)
		return command, nil
	})
	if err != nil {
		return err
	}
	actual, err := cluster.ReadBrokerMaintenance(node.RODB(), record.Home)
	if err != nil || actual != record {
		return brokermaint.ErrConflict
	}
	return nil
}

// MaintenanceRouteDigest binds a hold to this broker's node, boot, role receipt and generation.
func MaintenanceRouteDigest(nodeID, boot string, role storage.RoleReceipt, generation uint64) string {
	raw, _ := json.Marshal(struct {
		Node, Boot string
		Role       storage.RoleReceipt
		Generation uint64
	}{nodeID, boot, role, generation})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// LocalReloadLease checks the same fixed owner before freeze, close and exec.
// Single mode requires local admission even though it has no Raft role. Once
// the store has closed, only the retained same-image lease may be consulted.
// origin: S6 internal review SINGLEACK F2 / lead decision D-C / plan §7.1
func LocalReloadLease(lease brokermaint.LocalLease, single, stopped bool, read func() *sql.DB, holder func(string) error) error {
	if lease.Record.CutID == "" {
		if single {
			return brokerimage.ErrMaintenanceObserverUnsupported
		}
		return nil
	}
	now, err := procrequest.BootNow()
	if err != nil || now >= lease.ExpiresLocalBootNS {
		return brokermaint.ErrExpired
	}
	if lease.Record.Phase != brokermaint.MaintActive {
		return brokermaint.ErrIdentity
	}
	if !stopped {
		if !single {
			if err := holder(lease.Record.Holder); err != nil {
				return err
			}
		}
		current, err := brokercontrol.ReadLocalMaintenance(read(), lease.Record.Home)
		if err != nil || current != lease.Record {
			return brokermaint.ErrIdentity
		}
	}
	return nil
}

// Business readiness precedes the remaining fallible activation work. Keep the
// original local lease until activation commits so an ACTIVE rollback can use
// its signed hold and fixed deadline. A returned-exec thaw ends it immediately;
// neither path renews the lease or silently creates another owner.
// origin: S6 internal review SINGLEACK F4 / lead decision D-C / plan §4.2 / §7.1 / T49
func FinishLocalActivation(lease *brokermaint.LocalLease, pending bool, store brokerimage.StoreOwner, finish func(*cluster.Node, brokermaint.LocalLease) error) error {
	if pending || lease.Record.CutID == "" {
		return nil
	}
	node, _ := brokerimage.ClusterStoreNode(store)
	if err := finish(node, *lease); err != nil {
		return err
	}
	*lease = brokermaint.LocalLease{}
	return nil
}
