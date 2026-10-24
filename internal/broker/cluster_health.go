// cluster_health.go (D8b §10) is the broker-side responder for the member-reachable
// cluster-health + alert RPCs. Post-D9 cutover the subscriptions are wired in CLUSTER mode
// (wireClusterLate / SubscribeClusterHealth); in SINGLE mode they are not wired, so a ctl probe
// gets ErrNoResponders and the gate/banner stay silent (byte-identical). (The per-phase
// TestD8ProductionWiresNoCluster guard was a build-and-prove scaffold removed at the D9 cutover.)
package broker

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// SubscribeClusterHealth wires the BROADCAST cluster-health responder (§10.4): every broker
// answers (no queue group) so ctl can corroborate all reachable views into a destructive gate
// WITHOUT a Raft write. writable_leader_confirmed is set ONLY after a VerifyLeaderRead barrier
// — a partitioned ex-leader still reporting State()==Leader within its lease FAILS VerifyLeader
// and answers false, so the gate fires precisely in the data-loss window.
func SubscribeClusterHealth(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport, jsUnavail func() bool, colocatedAgentNID string, accountPub func() string, logger *slog.Logger) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterHealthWildcard, clusterHealthResponder(nc, node, db, now, topoSelf, jsUnavail, colocatedAgentNID, accountPub, logger))
}

// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func SubscribeClusterRosterPull(nc *nats.Conn, manifestFn func() ([]byte, bool), logger *slog.Logger) (*nats.Subscription, error) {
	return brokerruntime.SubscribeClusterRosterPull(nc, manifestFn, func(msg *nats.Msg, body []byte) { respondLogged(logger, nc, msg, body) })
}

// SubscribeClusterCursor (D9 §17 round-1 BLOCKER fix) wires the same broadcast health
// responder on the BROKER-ONLY tether.v2.cluster.cursor.req subject — the one the leader's
// observability poll scatters to (its broker nkey can pub+sub cluster.> but not ctrl.by.*).
// Every broker answers (no queue group) so the leader collects each voter's AppliedIndex.
func SubscribeClusterCursor(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport, jsUnavail func() bool, colocatedAgentNID string, accountPub func() string, logger *slog.Logger) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjClusterCursor, clusterHealthResponder(nc, node, db, now, topoSelf, jsUnavail, colocatedAgentNID, accountPub, logger))
}

// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func clusterHealthResponder(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time, topoSelf func() *topoSelfReport, jsUnavail func() bool, colocatedAgentNID string, accountPub func() string, logger *slog.Logger) func(*nats.Msg) {
	return brokerruntime.ClusterHealthResponder(nc, node, db, now, topoSelf, jsUnavail, colocatedAgentNID, accountPub, func(resp *proto.ClusterHealthResp) {
		if owner, ok := brokerImageHealthOwners.Load(node); ok {
			resp.BrokerImage = brokerImageSnapshot(owner)
			// S8: public projection of the Current journal record; never a running proof.
			resp.Upgrade = brokerupgrade.HealthSummary(owner.cfg.Upgrade)
			infraattach.SetClusterHealth(owner.infra, resp)
		}
	}, func(msg *nats.Msg, body []byte) { respondLogged(logger, nc, msg, body) })
}

// healthContactStale is deliberately stateless so a systemd restart during quorum loss cannot reset a
// grace timer and leave the off-box verdict "electing" forever. A real writable leader is fresh. A
// follower is fresh only when it has bounded-fresh evidence of a DIFFERENT current leader; no leader,
// or a demoted ex-leader still naming itself, is already read-only and therefore stale.
func healthContactStale(fenced, stateLeader, writable bool, leaderID, selfID string) bool {
	return infraattach.HealthContactStale(fenced, stateLeader, writable, leaderID, selfID)
}

// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func SubscribeAlertLs(nc *nats.Conn, db *sql.DB, logger *slog.Logger) (*nats.Subscription, error) {
	return brokerruntime.SubscribeAlertLs(nc, db, func(msg *nats.Msg, body []byte) { respondLogged(logger, nc, msg, body) })
}

func SubscribeAlertAck(nc *nats.Conn, fwd *Forwarder, logger *slog.Logger) (*nats.Subscription, error) {
	return brokerruntime.SubscribeAlertAck(nc, func(payload []byte) error { return fwd.Forward(VerbAlertAck, "", payload) },
		func(key, actor string) ([]byte, error) {
			return json.Marshal(AlertAckPayload{DedupKey: key, AckedBy: actor})
		},
		func(key string) error { return validAlertText("dedup_key", key, maxAlertDedupKeyLen) },
		func(msg *nats.Msg, body []byte) { respondLogged(logger, nc, msg, body) })
}

// Existing responder signatures remain source compatible. Bind only live nodes,
// and remove the previous node before publishing a recovery replacement; the
// registry must not keep a stopped Broker (and all of its resource graph) alive.
// origin: hot-update S6 plan §5 / §9.1
var brokerImageHealthOwners brokerimage.CurrentStoreOwners[*cluster.Node, *Broker]

func bindBrokerImageHealth(b *Broker) {
	if b.cl != nil && b.cl.node != nil {
		brokerImageHealthOwners.Bind(b.cl.node, b)
	}
}
func unbindBrokerImageHealth(b *Broker) { brokerImageHealthOwners.Remove(b) }

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
func forceSingleActive(db *sql.DB) bool              { return brokercontrol.ForceSingleActive(db) }
func upgradeActive(db *sql.DB) bool                  { return brokercontrol.UpgradeActive(db) }
func upgradeLockHeld(db *sql.DB, now time.Time) bool { return brokercontrol.UpgradeLockHeld(db, now) }
func growActiveJoiner(db *sql.DB) string             { return brokercontrol.GrowActiveJoiner(db) }
