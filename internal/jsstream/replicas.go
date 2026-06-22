package jsstream

import (
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ReplicasSingle is the non-HA replica factor (N=1 / build-and-prove production:
// b has no cluster.Node, so live callers pass this and behavior is byte-equivalent
// to today's hardcoded 1). D5 §6.4.
const ReplicasSingle = 1

// AuditDedupWindow is the JetStream Duplicates window on the audit-bearing
// history-<sid> streams (§6.3 D5 / d5-plan R-8/MP-3): the leader audit publisher
// stamps a Nats-Msg-Id (raft_index:kind:seq, or reqID-keyed for forwarded reconcile),
// and JetStream collapses any re-publish of the SAME id within this window. It MUST
// exceed the worst-case (election + post-election tail-drain) latency so a re-publish
// from a NEW leader still lands inside the window — with MultinodeElectionTimeout=1s
// and a bounded sweep this is ~100x margin. It is the SSOT shared by the stream config
// and Mechanism A's window assertion. INERT in build-and-prove production (the live
// publishAudit sets no Nats-Msg-Id, so no dedup happens; this pre-stages D9).
const AuditDedupWindow = 2 * time.Minute

// ErrMetaGroupNotReady is returned by a replica-RAISE (UpdateStream toward a higher
// target) when the JetStream meta-group cannot yet host the target — typically because
// a newly-added nats-server has joined the raft voter set but not yet the JS
// meta-group (§6.4 D5 / R-17). It is RETRIABLE: the caller skips this stream for the
// tick and retries next pass. The raise GATE is this rejection itself, because an
// R1 stream's StreamInfo.Cluster cannot reveal the meta-group size (it lists only this
// stream's own placement) — so a pre-check on Cluster.Replicas would falsely block the
// very expansion it gates (main-process correction to the synthesis's pre-gate).
var ErrMetaGroupNotReady = errors.New("jsstream: JS meta-group cannot yet host target replicas (retriable)")

// ReplicasFor maps the raft voter count to a JetStream replica factor (§6.4 D5 /
// R-16). Capped at 3 (architecture pins R<=3; the N>=3 HA target is R3). N=2 -> R2 is
// the only choice that is monotone non-decreasing (1->2->3, which D7's retire gate
// consumes) AND never requests more replicas than servers AND lets the
// kill-during-expand no-loss property hold at the 2-voter waypoint (the data already
// lives on the 2nd node). Honesty (§17): RF2 = read-survivable, zero write
// fault-tolerance (a 2-replica JS raft group needs both for a write quorum).
func ReplicasFor(nVoters int) int {
	switch {
	case nVoters <= 1:
		return 1
	case nVoters == 2:
		return 2
	default:
		return 3
	}
}

// StreamReplicaState is one stream's replica health for the §6.4 AllAtTarget predicate
// (R-19/R-20). Target = the desired replica factor (= ReplicasFor(nVoters)); Actual =
// the count of caught-up replicas observed from the live StreamInfo.Cluster; Ready =
// Actual >= Target.
type StreamReplicaState struct {
	Name   string
	Target int
	Actual int
	Ready  bool
}

// ActualReplicas computes the number of CAUGHT-UP replicas of a stream from its live
// StreamInfo (§6.4 D5 / R-20): 1 (this/leader replica) + each peer that is Current and
// not Offline. A present-but-lagging peer (!Current or Offline) counts as NOT actual —
// conservative for the retire gate (a node a stream still needs must never be retired).
// A non-clustered stream (Cluster == nil, single-server JS) has Actual = 1.
func ActualReplicas(info *jetstream.StreamInfo) int {
	if info == nil {
		return 0
	}
	actual := 1
	if info.Cluster != nil {
		for _, p := range info.Cluster.Replicas {
			if p != nil && p.Current && !p.Offline {
				actual++
			}
		}
	}
	return actual
}

// IsMetaGroupNotReady classifies a JetStream UpdateStream/UpdateObjectStore error as a
// TRANSIENT "the meta-group cannot host this replica count YET" condition (R-17) — i.e.
// retry next tick. It must NOT match PERMANENT misconfigurations that merely mention
// peers/replicas: JS code 10074 "replicas > 1 not supported in non-clustered mode" means
// the node's JetStream never formed a cluster, so retrying spins the reconcile loop
// forever masked by Degraded() (internal review M1). A bare "peers" substring is likewise
// too broad (e.g. "lost connection to peers" is a real failure). Any non-matching error
// propagates as a hard failure (E-B3 pins the live classification). Exported so the
// broker object-store reconcile (transfer.go) shares the one classifier.
func IsMetaGroupNotReady(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Permanent dead-ends that mention replicas/peers but are NOT retriable.
	for _, perm := range []string{"non-clustered", "not supported"} {
		if strings.Contains(msg, perm) {
			return false
		}
	}
	// Genuinely transient placement-capacity signals (a newly-joined voter not yet in the
	// JS meta-group, or resources momentarily unavailable).
	for _, s := range []string{
		"no suitable peer",
		"no peers",
		"insufficient",
		"not enough",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
