package cluster

import (
	"database/sql"
	"time"
)

// topologygen.go (C3 §D-E) — the monotone `topology_generation` counter the per-broker NATS topology
// reconciler watches. It advances ONLY on changes that alter the rendered cluster nats.conf: a broker
// ENTERS the route mesh (phase →CATCHING_UP), LEAVES it (PlanClusterNodeRemove), or fills its bus
// nkey (OpClusterBusNkeySet). It is DEDICATED (NOT roster_generation) because rostergen also bumps on
// PlanClusterCertRotate, and a tunnel-cert rotation changes NOTHING in the rendered conf (the conf
// emits cert FILE PATHS, never fingerprints) — reusing it would flap the cluster to DEGRADED on every
// routine cert rotation. Mirrors rostergen.go (same monotone + change-gated + all-literal pattern).
const metaKeyTopologyGeneration = "topology_generation"

// TopologyGeneration reads the monotone topology generation counter (bounded-stale, no txn). Missing
// row reads as 0 (the D0-empty cluster_meta convention, shared with applied_index / roster_generation).
// The broker reconciler reads this as the DESIRED generation; each broker tracks its own applied/observed.
func TopologyGeneration(ro *sql.DB) (uint64, error) {
	return readMetaUintDB(ro, metaKeyTopologyGeneration)
}

// topologyGenBumpStmt is the shared all-literal statement appended to a topology-affecting command so
// the counter advances IN THE SAME Apply txn. Identical shape to rosterGenBumpStmt:
//   - STRICTLY MONOTONE (MAX(existing+1, now) → never regresses on prune/recover);
//   - ROLLING-UPGRADE SAFE (seed/floor ≈ now.UnixNano(), same magnitude as any prior value);
//   - CHANGE-GATED (WHERE changes() > 0 ties the bump to the PRECEDING statement's RowsAffected, so a
//     stale-leader no-op transition does NOT inflate it). When appended AFTER rosterGenBumpStmt, this
//     bump's changes() reads rosterGenBumpStmt's RowsAffected, which is 1 iff it fired (i.e. iff the
//     original phase/remove UPDATE matched) and 0 otherwise — so the chain is truth-preserving: both
//     gen bumps fire exactly when the originating membership UPDATE matched, neither on a no-op.
//   - DETERMINISTIC (leader bakes now.UnixNano() as a literal).
func topologyGenBumpStmt(now time.Time) Statement {
	nowLit := MustLitText(formatUint(uint64(now.UTC().UnixNano())))
	sql := `INSERT INTO cluster_meta(key, value) ` +
		`SELECT ` + MustLitText(metaKeyTopologyGeneration) + `, ` + nowLit + ` WHERE changes() > 0 ` +
		`ON CONFLICT(key) DO UPDATE SET ` +
		`value = MAX(CAST(cluster_meta.value AS INTEGER)+1, CAST(excluded.value AS INTEGER))`
	return Stmt(sql)
}
