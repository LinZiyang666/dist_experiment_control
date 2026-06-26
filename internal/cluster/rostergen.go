package cluster

import (
	"database/sql"
	"time"
)

// rostergen.go (C1 §D-2) — the truly-monotone `roster_generation` counter that the
// signed broker roster (internal/broker/cluster_roster.go) stamps and the agent
// consumer (internal/agent) uses for anti-rollback. It replaces the SHIPPED derived
// generation `max(added_at, phase_changed_at)`, which REGRESSES on retire-of-newest /
// recover / force-single (cluster_roster.go) and would make a strict-monotone agent
// reject the corrected post-recovery roster (a wedge) — see c1-plan.md §D-2.
//
// SHAPE: a single cluster_meta KV bumped INSIDE the same Apply txn as each broker
// membership/phase mutation (PlanClusterNodePhase / PlanClusterNodeRemove /
// PlanClusterCertRotate), so it advances atomically with the change and never on a
// bare agent register (which is not a topology change → `agentGen < counter` cleanly
// measures topology lag for the §6 stale event). It is NOT bumped on
// PlanClusterNodeUpsert: that op rides the ONE custom applier (clusterNodeUpsertApplier)
// whose `len(cmd.Body)==1` PoP cross-check forbids a second baked statement, and a join
// always immediately walks PENDING_VOTER → CATCHING_UP via PlanClusterNodePhase (which
// DOES bump) before the new broker is a dialable VOTER — so the PENDING-only window is
// agent-irrelevant (the selector de-prefers non-VOTER anyway).
const metaKeyRosterGeneration = "roster_generation"

// RosterGeneration reads the monotone roster generation counter (bounded-stale, no txn).
// A missing row reads as 0 (the D0-empty cluster_meta convention, shared with
// applied_index / audit_published_index). The broker's buildSignedRoster stamps this
// onto ClusterRoster.Generation instead of the regression-prone derived max.
func RosterGeneration(ro *sql.DB) (uint64, error) {
	return readMetaUintDB(ro, metaKeyRosterGeneration)
}

// rosterGenBumpStmt is the shared all-literal statement appended to a membership/phase
// command so the counter advances IN THE SAME Apply txn. It is:
//
//   - STRICTLY MONOTONE: `MAX(existing+1, now)` always yields ≥ existing+1, so it never
//     regresses on prune/recover/force-single (a standalone counter restored from
//     snapshot + replayed from log), making the agent's strict reject-lower sound.
//   - ROLLING-UPGRADE SAFE: the seed/floor is `now.UnixNano()` — the SAME magnitude as
//     the shipped derived-max generation — so an agent that cached a v0.4.0 derived
//     value never sees a LOWER value from an upgraded broker (no mixed-version wedge),
//     and no migration is needed (the key is absent → reads 0 → first bump seeds it).
//   - CHANGE-GATED: `WHERE changes() > 0` ties the bump to the PRECEDING statement's
//     RowsAffected (SQLite changes() is connection-scoped and the FSM execs Body[0]
//     then Body[1] on the same *sql.Tx connection — genericExecApplier). So a stale
//     ex-leader's no-op transition (phase-predecessor guard / cert_fp-unchanged guard /
//     already-removed) does NOT inflate the counter — it advances ONLY on a real
//     topology change (preserves D5 idle-zero-writes spirit; no NEW raft writes — it
//     rides a command that was already being proposed).
//   - DETERMINISTIC: the leader bakes now.UnixNano() as a literal (like LitTime), so
//     every replica applies the identical MAX() and converges bit-for-bit.
//
// The value is stored as a decimal TEXT literal byte-identically to applied_index /
// audit_published_index (MustLitText(formatUint(...))), so readMetaUintDB's parseUint
// round-trips. Callers append it via the variadic NewCommand(op, Stmt(...), bump).
func rosterGenBumpStmt(now time.Time) Statement {
	nowLit := MustLitText(formatUint(uint64(now.UTC().UnixNano())))
	sql := `INSERT INTO cluster_meta(key, value) ` +
		`SELECT ` + MustLitText(metaKeyRosterGeneration) + `, ` + nowLit + ` WHERE changes() > 0 ` +
		`ON CONFLICT(key) DO UPDATE SET ` +
		`value = MAX(CAST(cluster_meta.value AS INTEGER)+1, CAST(excluded.value AS INTEGER))`
	return Stmt(sql)
}
