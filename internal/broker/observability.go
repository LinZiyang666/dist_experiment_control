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
	"strings"
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
// AppliedIndex trails leaderApplied by > lagThreshold). A voter that answered with a fresh
// cursor yields both inactive (clear). The leader itself (selfID) is skipped — it cannot be
// down/lagging relative to its own cursor.
//
// audit xx-concurrency F2: leaderApplied MUST be the leader's COMMAND-DOMAIN AppliedIndex,
// NOT its raft CommitIndex. The responders self-report their command-domain AppliedIndex, so
// comparing against CommitIndex (which also counts config/barrier entries) over-reports the
// gap and fires a FALSE raft_lag across election history. Like-for-like: applied vs applied.
func decideObservabilityAlerts(selfID string, leaderApplied uint64, voters []string,
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
		lag := ok && leaderApplied > r.AppliedIndex && leaderApplied-r.AppliedIndex > lagThreshold
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
	_ = ctx // reserved for cancellation if planAlertSignal grows a ctx
	// audit xx-concurrency F2: measure lag against the leader's COMMAND-DOMAIN AppliedIndex
	// (not raft CommitIndex), like-for-like with the responders' self-reported AppliedIndex.
	leaderApplied, err := b.cl.node.AppliedIndex()
	if err != nil {
		return // cannot measure lag without the leader's own command cursor
	}
	responses := pollClusterHealth(b.nc.Load(), proto.SubjClusterCursor, observePollWindow)
	// B5 OPS#1: cache the per-peer health for the metrics scrape (so /metrics never re-runs this
	// blocking poll). Lag is the leader's command-domain AppliedIndex minus the peer's self-report;
	// a peer that did not answer the window is unreachable (lag unknown → 0).
	peers := make([]peerObserve, 0, len(voters))
	for _, v := range voters {
		if v == b.selfID {
			continue
		}
		po := peerObserve{NodeID: v}
		if r, ok := responses[v]; ok {
			po.Reachable = true
			if leaderApplied > r.AppliedIndex {
				po.AppliedLag = leaderApplied - r.AppliedIndex
			}
		}
		peers = append(peers, po)
	}
	b.lastObserveMu.Lock()
	b.lastObserve = peers
	b.lastObserveMu.Unlock()

	// BD7: read ActiveAlerts ONCE — reused for the broker_down false→true edge detection (the
	// broker_down_rehome_summary fires on a NEW down) AND the orphan-clear pass below (no double scan).
	active, aerr := cluster.ActiveAlerts(b.cfg.DB)
	prevDown := map[string]bool{}
	if aerr == nil {
		for _, a := range active {
			if a.Kind == cluster.AlertKindBrokerDown {
				if node := strings.TrimPrefix(a.DedupKey, a.Kind+":"); node != "" {
					prevDown[node] = true
				}
			}
		}
	}
	// G7a #18: collect this tick's broker_down edges. downNow = voters reporting down (a fire-gate);
	// returned = voters whose broker_down CLEARED this tick (down→true→false edge) = a broker RETURNED,
	// the auto-rebalance trigger. Both are only trustworthy when the ActiveAlerts read succeeded.
	var returned, downNow []string
	for _, d := range decideObservabilityAlerts(b.selfID, leaderApplied, voters, responses, lagThreshold) {
		b.proposeAlertSignal(d.Kind, d.NodeID, d.Active, d.Message)
		if aerr == nil && d.Kind == cluster.AlertKindBrokerDown {
			if d.Active {
				downNow = append(downNow, d.NodeID)
				// BD7: a brand-new broker_down (false→true) → one rehome-impact summary (proxy_homes the
				// reaper will rehome vs exposes_stranded). Honest, secret-free, once per transition.
				if !prevDown[d.NodeID] {
					b.emitBrokerDownRehomeSummary(d.NodeID)
				}
			} else if prevDown[d.NodeID] {
				returned = append(returned, d.NodeID)
			}
		}
	}

	// audit-publish F1: clear ORPHANED per-node observability alerts for nodes that LEFT the
	// voter set — the decide loop only iterates CURRENT voters, so a removed/retired node's
	// broker_down/raft_lag alert would otherwise stay ACTIVE forever. The clear is
	// transition-gated (planAlertSignal), so a node still down/lagging keeps its alert.
	if aerr != nil {
		return
	}
	voterSet := make(map[string]bool, len(voters))
	for _, v := range voters {
		voterSet[v] = true
	}
	for _, a := range active {
		if a.Kind != cluster.AlertKindBrokerDown && a.Kind != cluster.AlertKindRaftLag {
			continue
		}
		node := strings.TrimPrefix(a.DedupKey, a.Kind+":")
		if node == "" || node == b.selfID || voterSet[node] {
			continue
		}
		b.proposeAlertSignal(a.Kind, node, false, "")
	}

	// G7a #18: after all alert bookkeeping (aerr==nil is guaranteed here — the orphan-clear pass above
	// returns early on a read error), evaluate the auto-rebalance-on-return trigger with this tick's edges.
	b.driveAutoRebalanceOnReturn(returned, downNow)
}

// proposeAlertSignal proposes one transition-gated per-node observability raise/clear via
// planAlertSignal (a nil command on no transition → no raft write, so idle-zero-writes holds).
func (b *Broker) proposeAlertSignal(kind, node string, active bool, message string) {
	payload := AlertSignalPayload{
		Kind: kind, Node: node, Active: active, Severity: cluster.AlertSeverityInfo, Message: message,
	}
	_ = b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		return planAlertSignal(db, payload, b.cfg.Now())
	})
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
	wasLeader := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// audit M1 / membership F1: on the leader-ACQUIRED edge run the §8.1 no-silent-fork
			// reconciliation pass ONCE — it is the REAL guarantee that a half-completed AddNode
			// (leader crashed between raft.AddVoter and the CATCHING_UP phase bump) gets
			// forward-completed, and nothing ELSE in production calls it (it had only test
			// callers). Leader-only + idempotent (phase transitions are baked WHERE phase IN
			// (preds) no-ops), so re-running on a leadership flap is safe. Tick-sampled rather
			// than a raft LeaderCh observer to avoid adding raft plumbing outside internal/cluster.
			// Online force-single dwell: record leader-contact state EVERY tick, NOT leader-gated — a
			// quorum-lost survivor is never leader, so this is the only loop that can observe the
			// sustained loss the force-single gate requires.
			if b.cl != nil && b.cl.node != nil && b.cl.fsArm != nil {
				now := time.Now()
				b.cl.fsArm.observeLeadership(b.cl.node.LeaderContactStale(now), now)
			}
			isLeader := b.cl != nil && b.cl.node != nil && b.cl.node.IsLeader()
			// Batch-A A15: the two leadership edges are the anchor of any
			// post-incident raft read — without them the raft library's own
			// lines have nothing to be correlated against, and "the write timed
			// out right after leadership moved" is unreconstructable. Two lines
			// per election is not a volume concern.
			if isLeader != wasLeader {
				what := "lost"
				if isLeader {
					what = "acquired"
				}
				b.cfg.Logger.Info("broker: raft leadership "+what, "component", "cluster", "is_leader", isLeader)
			}
			if isLeader && !wasLeader && b.cl.admin != nil {
				if err := b.cl.admin.ReconcileMembershipOnLeadership(); err != nil {
					b.cfg.Logger.Warn("broker: membership reconciliation on leadership", "err", err)
				}
			}
			if wasLeader && !isLeader {
				b.autoRebalanceArm.reset() // G7a #18: a demoted leader drops its debounce state
			}
			wasLeader = isLeader
			// C4: drive in-flight operations one idempotent step per tick (leader-gated inside). This is
			// the resume mechanism — a kill-9/leadership-change leaves a non-terminal op that the new
			// leader picks up + drives to terminal by re-deriving from the substrate. It also re-converges
			// client discovery seeds each tick (#46) — see driveLeaderMaintenance.
			if isLeader && b.cl.admin != nil {
				b.cl.admin.driveLeaderMaintenance()
			}
			// C5: the leader-gated proxy data-plane reaper (allocation + home-down rehome).
			if isLeader {
				b.driveProxyReconcile()
			}

			voters, err := b.clusterVoters()
			if err != nil {
				b.cfg.Logger.Debug("broker: observe loop voter read", "err", err)
				// B7: beat before continuing. The iteration DID complete — a voter read that fails every
				// tick is a live loop with a problem, and conflating it with a dead loop is what the beat
				// exists to prevent.
				b.cl.loops.Beat("observe")
				continue
			}
			b.observeOnce(ctx, voters, observeLagThreshold)
			b.cl.loops.Beat("observe")
		}
	}
}

// clusterVoters returns the node_ids of the current VOTER roster (the §17 observe target).
func (b *Broker) clusterVoters() ([]string, error) {
	rows, err := b.read().Query(`SELECT node_id FROM cluster_nodes WHERE phase = 'VOTER'`)
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
