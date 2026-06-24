package broker

// observability.go — D9 §17 step 10b: the leader-gated observability poll that closes the
// `broker_down` + `raft_lag` rows D8b left writerless. hashicorp/raft does not cleanly
// expose per-peer liveness or command-domain cursors, so the leader SCATTER-GATHERs the
// cluster-health broadcast (each broker self-reports its NodeID + AppliedIndex), then:
//   - broker_down.<id>  iff a known VOTER did NOT answer within the window.
//   - raft_lag.<id>     iff a VOTER answered but its AppliedIndex trails the leader's
//                       CommitIndex by more than the lag threshold.
// Raise/clear go through the same transition-gated planAlertSignal as disk_pressure, so the
// loop is idle-zero-writes (only a genuine transition Proposes a raft entry).

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// observeDecision is one §17 alert the poll decides to raise (Active) or clear (!Active).
type observeDecision struct {
	Kind     string
	NodeID   string
	Active   bool
	DedupKey string
	Message  string
}

// decideObservabilityAlerts is the PURE §17 step-10b decision (unit-tested without a
// cluster): for each known voter it emits a broker_down decision (active iff the voter is
// ABSENT from responses) and a raft_lag decision (active iff the voter responded but its
// AppliedIndex trails leaderCommit by > lagThreshold). A voter that answered with a fresh
// cursor yields both inactive (clear). The leader itself (selfID) is skipped — it cannot be
// down/lagging relative to its own commit index.
func decideObservabilityAlerts(selfID string, leaderCommit uint64, voters []string,
	responses map[string]proto.ClusterHealthResp, lagThreshold uint64) []observeDecision {
	var out []observeDecision
	for _, v := range voters {
		if v == selfID {
			continue
		}
		r, ok := responses[v]
		out = append(out, observeDecision{
			Kind: cluster.AlertKindBrokerDown, NodeID: v, Active: !ok,
			DedupKey: cluster.AlertKindBrokerDown + ":" + v,
			Message:  "broker " + v + " is not answering cluster-health probes",
		})
		lag := ok && leaderCommit > r.AppliedIndex && leaderCommit-r.AppliedIndex > lagThreshold
		out = append(out, observeDecision{
			Kind: cluster.AlertKindRaftLag, NodeID: v, Active: lag,
			DedupKey: cluster.AlertKindRaftLag + ":" + v,
			Message:  "broker " + v + " raft replication is lagging the leader",
		})
	}
	return out
}

// pollClusterHealth scatter-gathers the cluster-health broadcast: it publishes one request
// with a private reply inbox and collects replies until the window closes, keyed by the
// responder's NodeID. Used by the leader's observability poll AND by the multi-broker ctl
// `cluster status` aggregation. A nil nc / subscribe error yields an empty map (no panic).
// subject is the request subject to scatter on: the leader's §17 observe poll passes the
// broker-only proto.SubjClusterCursor (its broker nkey can pub there); a future member-side
// ctl status aggregation would pass proto.SubjCtrlClusterHealth(actor).
func pollClusterHealth(nc *nats.Conn, subject string, window time.Duration) map[string]proto.ClusterHealthResp {
	out := map[string]proto.ClusterHealthResp{}
	if nc == nil {
		return out
	}
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return out
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest(subject, inbox, nil); err != nil {
		return out
	}
	deadline := time.Now().Add(window)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			break
		}
		var r proto.ClusterHealthResp
		if json.Unmarshal(msg.Data, &r) == nil && r.NodeID != "" {
			out[r.NodeID] = r
		}
	}
	return out
}

// observeOnce is the leader-gated §17 poll: scatter-gather the health, decide the
// broker_down/raft_lag alerts for the current voter set, and Propose each transition via
// planAlertSignal (transition-gated ⇒ idle-zero-writes). No-op on a non-leader (the caller
// gates) or when the cluster is N=1 (no peers to observe).
func (b *Broker) observeOnce(ctx context.Context, voters []string, lagThreshold uint64) {
	if b.cl == nil || !b.cl.node.IsLeader() {
		return
	}
	responses := pollClusterHealth(b.nc.Load(), proto.SubjClusterCursor, observePollWindow)
	for _, d := range decideObservabilityAlerts(b.selfID, b.cl.node.CommitIndex(), voters, responses, lagThreshold) {
		_ = ctx // reserved for cancellation if planAlertSignal grows a ctx
		payload := AlertSignalPayload{
			Kind:     d.Kind,
			Node:     d.NodeID,
			Active:   d.Active,
			Severity: cluster.AlertSeverityInfo,
			Message:  d.Message,
		}
		_ = b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return planAlertSignal(db, payload, b.cfg.Now())
		})
	}
}

// observePollWindow bounds the health scatter-gather (a peer that does not answer within it
// is treated as broker_down). observeTickInterval is how often the leader polls;
// observeLagThreshold is the command-domain AppliedIndex gap that counts as raft_lag.
const (
	observePollWindow   = 2 * time.Second
	observeTickInterval = 5 * time.Second
	observeLagThreshold = 64
)

// runObserveLoop is the leader-gated §17 observability ticker (the 3rd cluster loop). It
// polls health + raises/clears broker_down/raft_lag each tick; observeOnce no-ops on a
// follower, so the goroutine count is constant across leadership flaps (like the publisher
// + reconciler). Bounded by ctx (the ordered shutdown cancels + joins it).
func (b *Broker) runObserveLoop(ctx context.Context) {
	t := time.NewTicker(observeTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			voters, err := b.clusterVoters()
			if err != nil {
				b.cfg.Logger.Debug("broker: observe loop voter read", "err", err)
				continue
			}
			b.observeOnce(ctx, voters, observeLagThreshold)
		}
	}
}

// clusterVoters returns the node_ids of the current VOTER roster (the §17 observe target).
func (b *Broker) clusterVoters() ([]string, error) {
	rows, err := b.cfg.DB.Query(`SELECT node_id FROM cluster_nodes WHERE phase = 'VOTER'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
