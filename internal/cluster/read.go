package cluster

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// TFence is the unified self-observation fail-closed window (§3.2 / §6.2 / §8.4):
// a node that has not had timely successful leader contact within TFence stops
// authorizing already-provisioned auth_callout reads. Pinned at 10s = k_fence(10)
// × the multinode ElectionTimeout (1000ms) — see TestFenceExceedsWorstCaseElection.
// It is ONE monotonic-clock predicate with NO raft.VerifyLeader round-trip, so a
// quorum-lost node keeps serving local reads instead of bricking (§6.2).
const TFence = 10 * time.Second

// LeaderContactStale reports whether THIS node should fail closed — stop
// authorizing already-provisioned auth_callout reads — because it has lost timely
// contact with the leader (§3.2 / §6.2). It is the single shared predicate and
// NEVER calls raft.VerifyLeader (a quorum-lost node must keep serving local reads,
// not block). now is injected for tests; production passes time.Now() (monotonic,
// NTP-step immune).
//
// Asymmetry (raft v1.7.3 — see leaderContactStaleAt):
//   - Leader => fresh. raft's leader-lease loop demotes a leader to Follower the
//     instant it cannot reach a quorum within LeaderLeaseTimeout
//     (checkLeaderLease), so State()==Leader IS proof of recent quorum contact.
//     Stateless: no timestamp, no LeaderCh goroutine (LeaderCh fires only on
//     leadership transitions, so a clock derived from it would false-fence a
//     healthy long-lived leader).
//   - Follower/Candidate => stale iff now - LastContact() > TFence. LastContact()
//     is refreshed on a successful AppendEntries (and on a granted vote /
//     InstallSnapshot); its zero value (never heard a leader) is far past TFence
//     => stale (fail-closed).
//
// Bounded residual fail-open: a partitioned ex-leader resets its follower clock at
// step-down (raft setLastContact), so the worst-case authorize-while-isolated
// window is LeaderLeaseTimeout + TFence — bounded and accepted (§8.4(b)).
func (n *Node) LeaderContactStale(now time.Time) bool {
	return leaderContactStaleAt(n.raft.State(), n.raft.LastContact(), now)
}

// leaderContactStaleAt is the pure decision behind LeaderContactStale, split out so
// the boundary + vacuity controls are deterministic without a live raft.
func leaderContactStaleAt(state raft.RaftState, lastContact, now time.Time) bool {
	if state == raft.Leader {
		return false // stateless: leader-lease guarantees recent quorum contact
	}
	return now.Sub(lastContact) > TFence
}

// AppliedIndex returns the FSM's durable apply cursor (architecture §3.7: the DB
// is the authority). This is a bounded-stale read off the local DB.
func (n *Node) AppliedIndex() (uint64, error) {
	return readAppliedIndexDB(n.db)
}

// VerifyLeaderRead runs fn ONLY after raft confirms this node is STILL the leader
// at read time (architecture §3.2): a VerifyLeader/ReadIndex barrier for
// correctness-sensitive reads (force-single peer health, revocation). At N=1 it
// trivially succeeds; the seam exists so D2+ consumers (auth_callout, catch-up
// barrier, force-single) attach to the correct path. D1 wires NO real consumer.
func (n *Node) VerifyLeaderRead(fn func(*sql.DB) error) error {
	if err := n.raft.VerifyLeader().Error(); err != nil {
		return fmt.Errorf("cluster: verify leader: %w", err)
	}
	return fn(n.db)
}

// BoundedStaleRead runs fn against the local DB WITHOUT a leader barrier
// (architecture §3.2): explicitly bounded-stale, for ps/history/status that may
// reflect a not-yet-stepped-down leader. D1 names the seam; no real consumer yet.
func (n *Node) BoundedStaleRead(fn func(*sql.DB) error) error {
	return fn(n.db)
}
