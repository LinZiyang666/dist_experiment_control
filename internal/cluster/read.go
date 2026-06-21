package cluster

import (
	"database/sql"
	"fmt"
)

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
