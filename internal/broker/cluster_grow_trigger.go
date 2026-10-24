package broker

import (
	"database/sql"
	"encoding/json"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"time"
)

// SubscribeClusterGrowTrigger wires the broadcast grow responder. Every broker subscribes; each request
// names exactly one TargetNode, so a wrong target stays SILENT and only the intended broker replies.
func (b *Broker) SubscribeClusterGrowTrigger(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterGrowWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		resp := b.handleGrowTrigger(msg.Data, b.cfg.Now())
		if resp == nil {
			return // not for this broker → stay silent
		}
		body, _ := json.Marshal(resp)
		b.respondBytes(msg, body)
	})
}

// Bind the signed grow protocol to the current runtime; its policy lives with the cluster adapters.
// origin: hot-update S7 plan §11.4 / S7 BROKER-SLIM-2
func (b *Broker) handleGrowTrigger(data []byte, now time.Time) *proto.ClusterGrowResp {
	return brokerruntime.HandleGrowTrigger(brokerruntime.GrowTrigger{
		SelfID: b.selfID, AccountSeed: func() []byte {
			if b.cfg.AuthCallout == nil {
				return nil
			}
			return b.cfg.AuthCallout.AccountSeed
		}, Node: func() *cluster.Node {
			if b.cl == nil {
				return nil
			}
			return b.cl.node
		}, Now: b.cfg.Now, AdminHandle: b.clusterAdminHandleFn, Cutover: b.performGrowCutover,
	}, data, now)
}

func meshPeerTriples(db *sql.DB) ([]string, error) { return brokerruntime.MeshPeerTriples(db) }

const (
	growTriggerCodeUnauthorized = "unauthorized"
	growTriggerCodeLockNotHeld  = "lock_not_held"
)
