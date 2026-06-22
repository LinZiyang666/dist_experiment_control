package cluster

import (
	"database/sql"
	"errors"
	"fmt"
)

// metaKeyAuditPublishedIndex is the cluster_meta KV key holding the D5 audit-publish
// cursor (§6.3): the highest raft index whose re-derivable audit (OpReconcileBatch →
// proc.ReplayReconcileAudit) the leader has durably published to JetStream. It is
// REPLICATED (advanced via OpAuditCheckpointSet through raft) so any NEW leader reads
// a recent floor and bounds its post-election sweep, instead of re-publishing from 0.
// Reads-as-0 when absent (D0 ships cluster_meta empty), mirroring applied_index.
const metaKeyAuditPublishedIndex = "audit_published_index"

// planAuditCheckpointSet renders OpAuditCheckpointSet advancing the audit-publish
// cursor to `to` (§6.3 D5 / d5-plan R-2/R-3). The MONOTONIC GUARD lives in the baked
// SQL — `ON CONFLICT … WHERE CAST(value AS INTEGER) < CAST(excluded.value AS INTEGER)`
// — so a stale ex-leader (still IsLeader() within its lease) proposing a LOWER value
// is a deterministic no-op on EVERY replica: the FSM, not a leader-side max() against a
// possibly-stale local view, enforces monotonicity. The value is stored as a decimal
// TEXT literal byte-identically to how applied_index is stored (formatUint), so the
// CAST comparison and AuditPublishedIndex's parseUint both round-trip for indices < 2^63
// (the practical raft range; SQLite CAST saturates a decimal >= 2^63 to max int64, which a
// real cluster never reaches — internal review m4). All-literal (no Statement.Args) so it
// rides genericExecApplier like every D2 op.
func planAuditCheckpointSet(to uint64) (*Command, error) {
	sql := `INSERT INTO cluster_meta(key, value) VALUES(` +
		MustLitText(metaKeyAuditPublishedIndex) + `, ` + MustLitText(formatUint(to)) + `) ` +
		`ON CONFLICT(key) DO UPDATE SET value = excluded.value ` +
		`WHERE CAST(cluster_meta.value AS INTEGER) < CAST(excluded.value AS INTEGER)`
	return NewCommand(OpAuditCheckpointSet, Stmt(sql)), nil
}

// readMetaUintDB reads a uint-valued cluster_meta KV outside any txn (bounded-stale).
// A missing row reads as 0 — the D0-empty cursor convention shared with applied_index.
func readMetaUintDB(db *sql.DB, key string) (uint64, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("cluster: read meta %q: %w", key, err)
	}
	n, perr := parseUint(v)
	if perr != nil {
		return 0, fmt.Errorf("cluster: corrupt meta %q value %q: %w", key, v, perr)
	}
	return n, nil
}
