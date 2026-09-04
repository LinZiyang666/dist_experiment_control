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
	// Assigned / Configured (G69, #67 sub-face 4) are ADDITIVE and do not participate in Ready. They
	// answer a different question from Actual: "did the META manage to place this many peers", not
	// "have this many peers caught up". The join terminal gate needs the former — a peer that has been
	// assigned but is still copying bytes has ALREADY proven the meta could place an R=N group, which
	// is all a NEW (empty) asset needs.
	Assigned   int // peers the JS meta has assigned to the raft group (Current NOT required)
	Configured int // the stream's configured Replicas (the raise may not have landed yet)

	// CaughtUpServers names the NATS servers whose replica of this stream is CURRENT and
	// not offline — the same set Actual counts, but identified rather than tallied.
	//
	// origin: prerelease audit round 2, G-1. The retire readiness gate asked "is Actual
	// at least the post-removal target" without knowing WHERE those replicas live, so a
	// caught-up replica sitting on the node about to be removed counted toward the floor
	// it was supposed to survive. A tally cannot answer "how many of these survive if I
	// remove that one"; a set can. Additive and does not participate in Ready.
	CaughtUpServers []string
}

// AssignedReplicas computes how many peers the JS META has ASSIGNED to a stream's raft group and are
// still PRESENT (G69, #67 sub-face 4): 1 (this replica) + every non-nil, non-Offline peer in
// Cluster.Replicas.
//
// What is deliberately NOT filtered is `Current`. A peer that has been assigned and is still copying
// bytes is real placement evidence, and requiring catch-up would make every grow wait for a full byte
// copy (the events stream alone is capped at 1 GiB) — a false-block of a healthy grow over a slow route.
//
// What IS filtered is `Offline`, and that was external review F3: tether never issues a JS peer-remove,
// so a member retired in a 3->2 shrink stays listed forever. Counting it let a later 2->3 regrow satisfy
// "Assigned >= target" on the FIRST tick from a corpse, and the join gate then declared the meta able to
// place a new R=N asset while the new joiner had not joined the meta group at all.
//
// It still does NOT prove anything about a SPECIFIC peer (e.g. the joiner), so this is only a cheap
// PRE-GATE: the join gate's authoritative evidence is the DIRECT measurement, ProbeMetaCanPlace, which
// actually creates an empty R=N canary stream and deletes it again (round-4 R4-F4: that canary has
// LANDED — this comment used to describe it as an outstanding suggestion, which read as a still-open gap
// long after it was closed). What remains genuinely open is registered in docs/reviews/g69-plan.md §7:
// the `3->2 (no JS peer-remove) ->3` real-machine differential, and the fact that a MemoryStorage canary
// measures meta placement but not whether a new File/ObjectStore asset has the disk budget to be created.
func AssignedReplicas(info *jetstream.StreamInfo) int {
	if info == nil {
		return 0
	}
	if info.Cluster == nil {
		return 1 // non-clustered: this replica is the whole group
	}
	n := 1 // self
	for _, p := range info.Cluster.Replicas {
		// EXTERNAL REVIEW F3: nil and OFFLINE peers must not count. tether never issues a JS
		// peer-remove, so a member retired in a 3->2 shrink stays listed forever; counting it let a
		// later 2->3 regrow satisfy the target on the FIRST tick from a corpse, and the join gate
		// declared "the meta can place a new R=N asset" while the new joiner had not joined the meta
		// group at all — the exact post-grow window the gate exists to measure.
		//
		// Note what is deliberately NOT excluded: !Current. A peer that has been ASSIGNED and is still
		// copying bytes is real placement evidence, and requiring catch-up would make every grow wait
		// for a full byte copy (events alone is capped at 1 GiB). Assigned AND PRESENT is the contract.
		if p == nil || p.Offline {
			continue
		}
		n++
	}
	return n
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
	// Permanent dead-ends that mention replicas/peers but are NOT retriable. Checked BEFORE the
	// transient signals so an overlapping substring resolves to permanent. "insufficient storage"
	// (a disk-full peer; JS "insufficient storage resources available") is PERMANENT — retrying
	// the same placement spins the reconcile loop forever, masked by Degraded() (audit jsstream
	// F3); it must propagate as a hard failure so the operator sees it, distinct from the
	// transient "insufficient resources"/"no suitable peer" placement-capacity signals below.
	for _, perm := range []string{"non-clustered", "not supported", "insufficient storage"} {
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

// CaughtUpPeersExcluding counts the CAUGHT-UP replicas of a stream that do NOT live on
// the named NATS server — i.e. how many would survive that server being removed.
//
// origin: prerelease audit round 2, G-1. The retire readiness gate asked "is Actual >=
// the post-removal target" while discarding the node it was being asked about, so a
// caught-up replica living ON the node about to be removed counted toward the floor it
// was supposed to survive. With three voters and one lagging peer, a stream at Actual=2
// passed a target of 2 and dropped to 1 the moment the retire completed.
//
// leaderServer is info.Cluster.Leader — the replica ActualReplicas counts as the
// implicit "1". Excluding by NATS server NAME (not raft node id) because that is what
// StreamInfo carries; the caller maps its node id through cluster_nodes.nats_server_id.
//
// An empty excludeServer counts everything, i.e. it degrades to ActualReplicas.
func CaughtUpServersOf(info *jetstream.StreamInfo) []string {
	if info == nil || info.Cluster == nil {
		return nil
	}
	var out []string
	if info.Cluster.Leader != "" {
		out = append(out, info.Cluster.Leader)
	}
	for _, p := range info.Cluster.Replicas {
		if p == nil || !p.Current || p.Offline {
			continue
		}
		out = append(out, p.Name)
	}
	return out
}

// SurvivorsExcluding counts how many of a stream's caught-up replicas would remain if
// `server` were removed. See CaughtUpServers for why a tally alone cannot answer this.
//
// An UNIDENTIFIED placement (no cluster info, or a leader the server did not name) is
// counted as NOT surviving: a retire gate must err toward refusing.
func SurvivorsExcluding(s StreamReplicaState, server string) int {
	if server == "" {
		return len(s.CaughtUpServers)
	}
	n := 0
	for _, name := range s.CaughtUpServers {
		if name != server {
			n++
		}
	}
	return n
}
