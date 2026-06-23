package cluster

import (
	"database/sql"
	"fmt"
)

// ActiveAlert is one ACTIVE row of the replicated alert store joined to its (optional)
// cluster-level ack (§10.1). All times are the UTC RFC3339Nano strings the leader baked
// (display-only). AckedBy/AckedAt are empty when the alert has not been acked.
type ActiveAlert struct {
	ID       string
	Kind     string
	Severity string
	DedupKey string
	Message  string
	RaisedAt string
	AckedBy  string
	AckedAt  string
}

// ActiveAlerts returns every ACTIVE alert with its ack info (LEFT JOIN alert_acks on
// dedup_key), ordered by raised_at then id for a stable render. Read-only and bounded-stale:
// the alert store is Raft-replicated, so ANY broker can serve this for the banner / `alert ls`
// (the JOIN filters state='ACTIVE', so a CLEARED row's stale ack never surfaces — pinned by a
// regression test).
func ActiveAlerts(db *sql.DB) ([]ActiveAlert, error) {
	rows, err := db.Query(`
		SELECT a.id, a.kind, a.severity, a.dedup_key, a.message, a.raised_at,
		       COALESCE(ack.acked_by, ''), COALESCE(ack.acked_at, '')
		  FROM alerts a
		  LEFT JOIN alert_acks ack ON ack.dedup_key = a.dedup_key
		 WHERE a.state = 'ACTIVE'
		 ORDER BY a.raised_at, a.id`)
	if err != nil {
		return nil, fmt.Errorf("cluster: active alerts query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ActiveAlert
	for rows.Next() {
		var a ActiveAlert
		if err := rows.Scan(&a.ID, &a.Kind, &a.Severity, &a.DedupKey, &a.Message, &a.RaisedAt, &a.AckedBy, &a.AckedAt); err != nil {
			return nil, fmt.Errorf("cluster: active alerts scan: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cluster: active alerts rows: %w", err)
	}
	return out, nil
}

// ActiveAlertKeys returns the set of dedup_keys with an ACTIVE alert. The leader-gated
// reconcile loop diffs this against its desired set so it proposes a raise/clear ONLY on a
// genuine transition (preserving D5's idle-zero-writes — a WHERE NOT EXISTS no-op is still a
// committed raft entry, so the loop must not re-propose every tick).
func ActiveAlertKeys(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`SELECT dedup_key FROM alerts WHERE state='ACTIVE'`)
	if err != nil {
		return nil, fmt.Errorf("cluster: active alert keys query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]bool)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("cluster: active alert keys scan: %w", err)
		}
		out[k] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cluster: active alert keys rows: %w", err)
	}
	return out, nil
}

// IsAlertActive reports whether an ACTIVE alert holds dedup_key. The leader-side
// VerbAlertSignal handler reads it (under applyMu, in Plan) so a forwarded disk-pressure
// signal commits a raise/clear ONLY on a genuine transition — a re-asserted unchanged state
// (the level-triggered forward) yields a nil command and zero raft writes (idle-zero-writes).
func IsAlertActive(db *sql.DB, dedupKey string) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM alerts WHERE dedup_key=? AND state='ACTIVE' LIMIT 1`, dedupKey).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cluster: is-alert-active: %w", err)
	}
	return true, nil
}

// DrainingNodes returns the node ids with an active broker_draining marker (a cluster_meta
// 'draining:<node>' key written by OpClusterDrainSet). The reconcile loop turns each into a
// broker_draining alert; a removed key clears it.
func DrainingNodes(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT key FROM cluster_meta WHERE key LIKE 'draining:%'`)
	if err != nil {
		return nil, fmt.Errorf("cluster: draining nodes query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("cluster: draining nodes scan: %w", err)
		}
		out = append(out, k[len("draining:"):])
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cluster: draining nodes rows: %w", err)
	}
	return out, nil
}
