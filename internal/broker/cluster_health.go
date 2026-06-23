// cluster_health.go is the D8b (§10) BUILD-AND-PROVE broker-side responder file for the
// member-reachable cluster-health + alert RPCs. The subscriptions are constructed ONLY by the
// test/d8 harness (production builds no cluster.Node, cutover=D9); serve.go never wires them,
// so at N=1 a ctl probe gets ErrNoResponders and the gate/banner stay silent (byte-identical).
//
// EXCLUDED from the TestD8ProductionWiresNoCluster guard scan.
package broker

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// SubscribeClusterHealth wires the BROADCAST cluster-health responder (§10.4): every broker
// answers (no queue group) so ctl can corroborate all reachable views into a destructive gate
// WITHOUT a Raft write. writable_leader_confirmed is set ONLY after a VerifyLeaderRead barrier
// — a partitioned ex-leader still reporting State()==Leader within its lease FAILS VerifyLeader
// and answers false, so the gate fires precisely in the data-loss window.
func SubscribeClusterHealth(nc *nats.Conn, node *cluster.Node, db *sql.DB, now func() time.Time) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjCtrlClusterHealthWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		resp := proto.ClusterHealthResp{
			SchemaVersion:      proto.ClusterHealthSchemaVersion,
			LeaderContactStale: node.LeaderContactStale(now()),
			ForceSingleActive:  forceSingleActive(db),
		}
		if verr := node.VerifyLeaderRead(func(*sql.DB) error { return nil }); verr == nil {
			resp.WritableLeaderConfirmed = true
		}
		if _, leaderID := node.LeaderWithID(); leaderID != "" {
			resp.LeaderID = leaderID // best-effort, banner text only
		}
		b, err := json.Marshal(resp)
		if err != nil {
			return
		}
		_ = msg.Respond(b)
	})
}

// SubscribeAlertLs wires the QUEUE-GROUP alert-ls responder (§10.1/§10.3): any ONE broker
// serves the bounded-stale replicated read (alerts are Raft-replicated, so any broker is
// authoritative-enough for the banner). One round-trip, best-effort.
func SubscribeAlertLs(nc *nats.Conn, db *sql.DB) (*nats.Subscription, error) {
	return nc.QueueSubscribe(proto.SubjCtrlAlertLsWildcard, "alert-ls", func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		alerts, err := cluster.ActiveAlerts(db)
		if err != nil {
			return
		}
		views := make([]proto.AlertView, 0, len(alerts))
		for _, a := range alerts {
			views = append(views, proto.AlertView{
				ID: a.ID, Kind: a.Kind, Severity: a.Severity, DedupKey: a.DedupKey,
				Message: a.Message, RaisedAt: a.RaisedAt, AckedBy: a.AckedBy, AckedAt: a.AckedAt,
			})
		}
		b, err := json.Marshal(proto.AlertLsResp{Alerts: views})
		if err != nil {
			return
		}
		_ = msg.Respond(b)
	})
}

// SubscribeAlertAck wires the QUEUE-GROUP ack responder (§10.1): ONE broker forwards the ack
// to the leader (VerbAlertAck). acked_by is the NATS-authenticated actor from the subject
// (display-only) — the ctl never asserts it. Replies "ok" or a short error.
func SubscribeAlertAck(nc *nats.Conn, fwd *Forwarder) (*nats.Subscription, error) {
	return nc.QueueSubscribe(proto.SubjCtrlAlertAckWildcard, "alert-ack", func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		actor, _, ok := proto.ParseCtrlBy(msg.Subject)
		if !ok {
			_ = msg.Respond([]byte("error: bad subject"))
			return
		}
		var req proto.AlertAckReq
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.DedupKey == "" {
			_ = msg.Respond([]byte("error: bad request"))
			return
		}
		payload, err := json.Marshal(AlertAckPayload{DedupKey: req.DedupKey, AckedBy: actor})
		if err != nil {
			_ = msg.Respond([]byte("error: marshal"))
			return
		}
		if ferr := fwd.Forward(VerbAlertAck, "", payload); ferr != nil {
			_ = msg.Respond([]byte("error: " + ferr.Error()))
			return
		}
		_ = msg.Respond([]byte("ok"))
	})
}

// forceSingleActive reports whether the persisted D7 force_single_active marker is set in the
// replicated cluster_meta (read-only; any broker's local committed copy).
func forceSingleActive(db *sql.DB) bool {
	var one int
	err := db.QueryRow(`SELECT 1 FROM cluster_meta WHERE key=? LIMIT 1`, cluster.MetaKeyForceSingle).Scan(&one)
	return err == nil
}
