package cluster

import (
	"database/sql"
	"fmt"
)

// RebuildLiveness resets the leader-local liveness columns to their just-restored
// baseline (architecture §3.5 D1 amendment): a freshly restored DB has no live
// heartbeats, so every node is OFFLINE with proxy_ready cleared and no heartbeat
// timestamp until a live Register/Heartbeat says otherwise. The online-backup
// page-copy carries the old liveness values along (it cannot projection-filter
// columns), so this reset is UNCONDITIONAL on restore.
//
// This is invoked from the restore path only — it is NOT Apply-reachable, so it
// does not violate the §3.5 "Apply never writes liveness columns" invariant (the
// full Apply-reachability guard is D2). The complete heartbeat-driven G.2
// reconcile lands in a later phase; D1 only pins the restore baseline.
func RebuildLiveness(db *sql.DB) error {
	if _, err := db.Exec(
		`UPDATE nodes SET status = 'OFFLINE', proxy_ready = 0, last_heartbeat_at = NULL`,
	); err != nil {
		return fmt.Errorf("cluster: rebuild liveness baseline: %w", err)
	}
	return nil
}
