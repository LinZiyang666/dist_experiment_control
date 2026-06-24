package cluster

import (
	"fmt"
	"time"
)

// Alert kinds (the store-backed subset; §10.2). The 0009 CHECK enumerates exactly these —
// the two client-synthesized severe kinds (quorum_lost / force_single_active) are NOT here
// (they cannot be Raft-written when quorum is lost). raft_lag / broker_down are in the 0009
// catalog but writerless until D9 (leader cannot read per-peer cursor/liveness pre-D9).
const (
	AlertKindManual              = "manual"
	AlertKindBelowQuorum         = "below_quorum"
	AlertKindBrokerDraining      = "broker_draining"
	AlertKindBrokerDown          = "broker_down"
	AlertKindReplicationDegraded = "replication_degraded"
	AlertKindDiskPressure        = "disk_pressure"
	AlertKindRaftLag             = "raft_lag"
)

// Alert severities (0009 CHECK).
const (
	AlertSeveritySevere = "severe"
	AlertSeverityInfo   = "info"
)

// ValidAlertKind reports whether kind is in the 0009 CHECK enum. A forwarded VerbAlertSignal
// must be validated against this BEFORE PlanAlertRaise — an out-of-enum kind would fail the
// CHECK on EVERY replica (a fail-stop poison entry, since alert ops ride genericExecApplier,
// not the poison-skip applier), bricking the cluster on replay (defense-in-depth, review m1).
func ValidAlertKind(kind string) bool {
	switch kind {
	case AlertKindManual, AlertKindBelowQuorum, AlertKindBrokerDraining, AlertKindBrokerDown,
		AlertKindReplicationDegraded, AlertKindDiskPressure, AlertKindRaftLag:
		return true
	}
	return false
}

// ValidAlertSeverity reports whether sev is in the 0009 CHECK enum.
func ValidAlertSeverity(sev string) bool {
	return sev == AlertSeveritySevere || sev == AlertSeverityInfo
}

// DedupKeyGlobal is the dedup key for a cluster-wide singleton alert (one ACTIVE row for the
// whole cluster, e.g. replication_degraded / below_quorum).
func DedupKeyGlobal(kind string) string { return kind }

// DedupKeyNode is the dedup key for a per-node alert (one ACTIVE row per node, e.g.
// broker_draining:<node> / disk_pressure:<node>).
func DedupKeyNode(kind, node string) string { return kind + ":" + node }

// alertLitTime bakes a timestamp as UTC RFC3339Nano TEXT. UTC-normalizing makes the literal
// timezone-deterministic (two leaders in different local zones bake the SAME string for the
// same instant — the DIFF-1 property), and a plain string column reads back without modernc
// time-parsing ambiguity (ActiveAlerts scans these as strings, display-only).
func alertLitTime(t time.Time) (string, error) {
	return LitText(t.UTC().Format(time.RFC3339Nano))
}

// PlanAlertRaise renders OpAlertRaise: INSERT one ACTIVE alert iff no ACTIVE row already
// holds dedup_key (no-op-on-conflict — avoids the idx_alerts_dedup_active unique-index Apply
// error that would otherwise fork the FSM). id + raisedAt are leader-baked. The leader-gated
// reconcile loop calls this ONLY when dedup_key is not already ACTIVE, so in steady state it
// issues zero raft writes; the WHERE NOT EXISTS is the second line that keeps a racing
// double-raise a deterministic no-op.
func PlanAlertRaise(id, kind, severity, dedupKey, message string, raisedAt time.Time) (*Command, error) {
	// audit S1: self-validate the enum at the single bake surface. alert ops ride the plain
	// genericExecApplier, so an out-of-enum kind/severity would fail the 0009 CHECK on EVERY
	// replica → a fail-stop poison entry that bricks the cluster on log replay. Callers
	// (planAlertSignal) already validate, but enforcing it HERE means a future raise caller that
	// forgets cannot mint a CHECK-violating row.
	if !ValidAlertKind(kind) {
		return nil, fmt.Errorf("cluster: plan alert-raise: invalid kind %q", kind)
	}
	if !ValidAlertSeverity(severity) {
		return nil, fmt.Errorf("cluster: plan alert-raise: invalid severity %q", severity)
	}
	lits, err := LitTextAll(id, kind, severity, dedupKey, message)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-raise: %w", err)
	}
	tsLit, err := alertLitTime(raisedAt)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-raise: ts literal: %w", err)
	}
	idL, kindL, sevL, keyL, msgL := lits[0], lits[1], lits[2], lits[3], lits[4]
	sql := `INSERT INTO alerts(id, kind, severity, dedup_key, state, message, raised_at) ` +
		`SELECT ` + idL + `, ` + kindL + `, ` + sevL + `, ` + keyL + `, 'ACTIVE', ` + msgL + `, ` + tsLit + ` ` +
		`WHERE NOT EXISTS (SELECT 1 FROM alerts WHERE dedup_key=` + keyL + ` AND state='ACTIVE')`
	return NewCommand(OpAlertRaise, Stmt(sql)), nil
}

// PlanAlertClear renders OpAlertClear: mark the (single) ACTIVE row for dedup_key CLEARED.
// Idempotent (a second clear matches no ACTIVE row → 0 rows). clearedAt is leader-baked.
func PlanAlertClear(dedupKey string, clearedAt time.Time) (*Command, error) {
	keyL, err := LitText(dedupKey)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-clear: key literal: %w", err)
	}
	tsLit, err := alertLitTime(clearedAt)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-clear: ts literal: %w", err)
	}
	sql := `UPDATE alerts SET state='CLEARED', cleared_at=` + tsLit +
		` WHERE dedup_key=` + keyL + ` AND state='ACTIVE'`
	return NewCommand(OpAlertClear, Stmt(sql)), nil
}

// PlanAlertAck renders OpAlertAck: UPSERT the SINGLE cluster-level ack for dedup_key (§18.3 —
// one ack per alert, not per-identity; acked_by is display-only). Idempotent (ON CONFLICT DO
// UPDATE → last-writer-wins). ackedAt is leader-baked.
func PlanAlertAck(dedupKey, ackedBy string, ackedAt time.Time) (*Command, error) {
	lits, err := LitTextAll(dedupKey, ackedBy)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-ack: %w", err)
	}
	tsLit, err := alertLitTime(ackedAt)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan alert-ack: ts literal: %w", err)
	}
	keyL, byL := lits[0], lits[1]
	sql := `INSERT INTO alert_acks(dedup_key, acked_by, acked_at) VALUES(` + keyL + `, ` + byL + `, ` + tsLit + `) ` +
		`ON CONFLICT(dedup_key) DO UPDATE SET acked_by=excluded.acked_by, acked_at=excluded.acked_at`
	return NewCommand(OpAlertAck, Stmt(sql)), nil
}
