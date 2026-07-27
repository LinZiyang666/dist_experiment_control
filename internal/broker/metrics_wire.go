package broker

// metrics_wire.go (B5 OPS#1) — the broker-side adapters feeding internal/brokermetrics. Both are
// request-time closures: CHEAP, in-process raft-accessor reads + the cached observe tick, NEVER
// the 2s-blocking StatusReport scatter-gather. Single mode (b.cl==nil) returns an honest
// degenerate snapshot and a ready=200 — it never fabricates HA.

import (
	"github.com/LinZiyang666/tether/internal/brokermetrics"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// metricsSnapshot builds the metrics view from cheap accessors. alerts_active works in both modes
// (the single-broker alerts table is empty → 0); the raft/peer gauges are populated only in
// cluster mode.
func (b *Broker) metricsSnapshot() brokermetrics.Snapshot {
	s := brokermetrics.Snapshot{ClusterMode: b.clusterMode}
	if keys, err := cluster.ActiveAlertKeys(b.cfg.DB); err == nil {
		s.AlertsActive = len(keys)
	}
	// Batch-A A15: surface the audit pipeline's loss counters.
	//
	// Review F-04 corrected the original comment here, which claimed this
	// placement let single-mode brokers benefit too. It does not: b.cl is nil
	// outside cluster mode, so this block is unreachable there. The publisher
	// itself only exists in cluster mode. Placing the read above the
	// cluster-mode early return buys nothing today and is kept only because it
	// costs nothing and stays correct if the publisher is ever wired for single
	// mode.
	if b.cl != nil {
		if ap := b.cl.auditPub.Load(); ap != nil {
			s.AuditTruncationLoss = int64(ap.TruncationLossCount())
			s.AuditLagExceeded = int64(ap.LagExceededCount())
			s.AuditDeletedStreamLoss = int64(ap.DeletedStreamLossCount())
		}
	}
	// batch B / B2: forward outcomes. Read BEFORE the cluster-mode early return so a broker that
	// was clustered and has since been force-singled still exposes the tallies it accumulated —
	// omitting them would make the series vanish exactly when an operator is investigating why.
	s.ForwardOutcomes = b.forwards.snapshot()

	if !b.clusterMode || b.cl == nil || b.cl.node == nil {
		return s
	}
	n := b.cl.node
	s.IsLeader = n.IsLeader()
	if v, err := n.NumVoters(); err == nil {
		s.Voters = v
		// Audit OBS-MAJOR-2: report LIVE resilience, not static config headroom. The static
		// FaultTolerance over-reports danger in a partial outage (stays 1 on a 3-voter cluster with a
		// voter already down). Subtract the unreachable voters the leader observed (floored at 0).
		margin := ProjectQuorum(v, false).FaultTolerance
		if n.IsLeader() {
			down := 0
			b.lastObserveMu.Lock()
			for _, p := range b.lastObserve {
				if !p.Reachable {
					down++
				}
			}
			b.lastObserveMu.Unlock()
			if margin -= down; margin < 0 {
				margin = 0
			}
		}
		s.QuorumMargin = margin
	}
	if ai, err := n.AppliedIndex(); err == nil {
		s.AppliedIndex = ai
	}
	s.CommitIndex = n.CommitIndex()
	s.ForceSingle = forceSingleActive(b.cfg.DB)
	s.XferUnreapableBuckets = int(b.xferUnreapableBuckets.Load()) // N-6: latest reap-pass gauge (any broker)
	// m2 (Stage-C): the cached per-peer observe is leader-era data — a DEMOTED leader would serve
	// frozen peer gauges next to is_leader 0. Emit peer series ONLY while leader (a follower honestly
	// shows no peer observation, matching the package's omit-don't-fabricate stance).
	if n.IsLeader() {
		b.lastObserveMu.Lock()
		for _, p := range b.lastObserve {
			s.Peers = append(s.Peers, brokermetrics.PeerSnap{NodeID: p.NodeID, AppliedLag: p.AppliedLag, Reachable: p.Reachable})
		}
		b.lastObserveMu.Unlock()
		// B6 OPS#4: the cached stream replica posture (leader-era data, like the peer gauges).
		b.lastReplicaMu.Lock()
		s.StreamsActual, s.StreamsTarget = b.lastReplicaActual, b.lastReplicaTarget
		b.lastReplicaMu.Unlock()
	}
	return s
}

// metricsReady is the /readyz predicate (B5 OPS#1, plan §1.1/§0.6). Single mode → always ready (it
// serves). Cluster → ready iff there IS a raft leader AND this node is itself a healthy serving
// VOTER (phase==VOTER in the roster AND a voter in the committed raft config). A DEGRADED-but-
// serving cluster (a healthy 2-voter cluster, a draining PEER) stays READY so a load balancer does
// not pull a serving broker during benign maintenance; but a node that is itself CATCHING_UP /
// RETIRING / VOTER_ADD_FAILED / roster-vs-raft INCONSISTENT answers 503 so the LB drains IT.
func (b *Broker) metricsReady() (bool, string) {
	if !b.clusterMode || b.cl == nil || b.cl.node == nil {
		return true, "single"
	}
	n := b.cl.node
	_, leaderID := n.LeaderWithID()
	if leaderID == "" {
		return false, "no raft leader (quorum lost or election in progress)"
	}
	selfID := n.SelfID()
	var phase string
	if err := b.read().QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, selfID).Scan(&phase); err != nil {
		return false, "self phase unknown: " + err.Error()
	}
	if phase != phaseVoter {
		return false, "self not a serving voter (phase=" + phase + ")"
	}
	// Inconsistency guard: a roster VOTER that is NOT a voter in the committed raft config is the
	// no-silent-fork red flag — pull it from the LB.
	if cfg, err := n.RaftConfiguration(); err == nil {
		voter := false
		for _, srv := range cfg {
			if srv.NodeID == selfID && srv.Voter {
				voter = true
				break
			}
		}
		if !voter {
			return false, "self is a roster VOTER but not a raft-config voter (INCONSISTENT)"
		}
	}
	return true, "leader=" + leaderID + " self=VOTER"
}
