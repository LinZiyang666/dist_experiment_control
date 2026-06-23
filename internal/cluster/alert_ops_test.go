package cluster

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

func openMigrated(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// execAlert execs a planned command (caller checks the Plan error first via mustPlan).
func execAlert(t *testing.T, db *sql.DB, cmd *Command, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := ExecCommand(db, cmd); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

// mustPlanExec plans (via fn) and execs, failing on either error. Wrapping the Plan call in a
// thunk avoids Go's "multi-value in single-value context" at the call site.
func mustPlanExec(t *testing.T, db *sql.DB, fn func() (*Command, error)) {
	t.Helper()
	cmd, err := fn()
	execAlert(t, db, cmd, err)
}

func countAlerts(t *testing.T, db *sql.DB, where string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM alerts ` + where).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestAlertRaiseIdempotentAndReRaise: a second raise of an ACTIVE dedup_key is a no-op
// (WHERE NOT EXISTS), but a raise AFTER a clear inserts a NEW row — so the history retains
// every episode while only one is ever ACTIVE.
func TestAlertRaiseIdempotentAndReRaise(t *testing.T) {
	db := openMigrated(t)
	ts := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	key := DedupKeyGlobal(AlertKindReplicationDegraded)

	cmd, err := PlanAlertRaise("a1", AlertKindReplicationDegraded, AlertSeveritySevere, key, "degraded", ts)
	execAlert(t, db, cmd, err)
	if got := countAlerts(t, db, `WHERE state='ACTIVE'`); got != 1 {
		t.Fatalf("after first raise: ACTIVE=%d, want 1", got)
	}
	// Second raise of the SAME active key → no-op.
	cmd2, err := PlanAlertRaise("a2", AlertKindReplicationDegraded, AlertSeveritySevere, key, "degraded again", ts.Add(time.Second))
	execAlert(t, db, cmd2, err)
	if got := countAlerts(t, db, `WHERE state='ACTIVE'`); got != 1 {
		t.Fatalf("after duplicate raise: ACTIVE=%d, want 1 (WHERE NOT EXISTS no-op)", got)
	}
	if got := countAlerts(t, db, ``); got != 1 {
		t.Fatalf("duplicate raise inserted a row: total=%d, want 1", got)
	}
	// Clear, then re-raise → a NEW active row; history keeps the cleared one.
	cmdC, err := PlanAlertClear(key, ts.Add(2*time.Second))
	execAlert(t, db, cmdC, err)
	if got := countAlerts(t, db, `WHERE state='ACTIVE'`); got != 0 {
		t.Fatalf("after clear: ACTIVE=%d, want 0", got)
	}
	cmd3, err := PlanAlertRaise("a3", AlertKindReplicationDegraded, AlertSeveritySevere, key, "degraded epoch 2", ts.Add(3*time.Second))
	execAlert(t, db, cmd3, err)
	if got := countAlerts(t, db, `WHERE state='ACTIVE'`); got != 1 {
		t.Fatalf("after re-raise: ACTIVE=%d, want 1", got)
	}
	if got := countAlerts(t, db, ``); got != 2 {
		t.Fatalf("re-raise history: total=%d, want 2 (one CLEARED + one ACTIVE)", got)
	}
}

// TestAlertAckLastWriterWins: the cluster-level ack is a single row per dedup_key; a second
// ack overwrites acked_by/acked_at (ON CONFLICT DO UPDATE). ActiveAlerts joins it.
func TestAlertAckLastWriterWins(t *testing.T) {
	db := openMigrated(t)
	ts := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	key := DedupKeyGlobal(AlertKindBelowQuorum)
	mustPlanExec(t, db, func() (*Command, error) {
		return PlanAlertRaise("b1", AlertKindBelowQuorum, AlertSeverityInfo, key, "below quorum", ts)
	})
	mustPlanExec(t, db, func() (*Command, error) { return PlanAlertAck(key, "UALICE", ts.Add(time.Minute)) })
	mustPlanExec(t, db, func() (*Command, error) { return PlanAlertAck(key, "UBOB", ts.Add(2*time.Minute)) })

	alerts, err := ActiveAlerts(db)
	if err != nil {
		t.Fatalf("active alerts: %v", err)
	}
	if len(alerts) != 1 {
		t.Fatalf("active alerts = %d, want 1", len(alerts))
	}
	if alerts[0].AckedBy != "UBOB" {
		t.Fatalf("acked_by = %q, want UBOB (last writer wins)", alerts[0].AckedBy)
	}
	var ackRows int
	_ = db.QueryRow(`SELECT COUNT(*) FROM alert_acks WHERE dedup_key=?`, key).Scan(&ackRows)
	if ackRows != 1 {
		t.Fatalf("alert_acks rows = %d, want 1 (single cluster-level ack)", ackRows)
	}
}

// TestActiveAlertsExcludesCleared: ActiveAlerts (and its ack JOIN) must filter state='ACTIVE'
// — a cleared alert's stale ack must never surface (the banner only renders live alerts).
func TestActiveAlertsExcludesCleared(t *testing.T) {
	db := openMigrated(t)
	ts := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	key := DedupKeyNode(AlertKindBrokerDraining, "node-x")
	mustPlanExec(t, db, func() (*Command, error) {
		return PlanAlertRaise("d1", AlertKindBrokerDraining, AlertSeverityInfo, key, "draining", ts)
	})
	mustPlanExec(t, db, func() (*Command, error) { return PlanAlertAck(key, "UOP", ts.Add(time.Minute)) })
	mustPlanExec(t, db, func() (*Command, error) { return PlanAlertClear(key, ts.Add(2*time.Minute)) })
	alerts, err := ActiveAlerts(db)
	if err != nil {
		t.Fatalf("active alerts: %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("cleared alert still ACTIVE: %d rows", len(alerts))
	}
}

// TestIsAlertActive backs the leader-side VerbAlertSignal transition gate: true while an
// ACTIVE row holds the key, false after it is cleared (so a re-asserted unchanged disk state
// proposes nothing).
func TestIsAlertActive(t *testing.T) {
	db := openMigrated(t)
	ts := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	key := DedupKeyNode(AlertKindDiskPressure, "node-1")
	if a, _ := IsAlertActive(db, key); a {
		t.Fatal("no alert yet, want inactive")
	}
	mustPlanExec(t, db, func() (*Command, error) {
		return PlanAlertRaise("p1", AlertKindDiskPressure, AlertSeverityInfo, key, "disk", ts)
	})
	if a, err := IsAlertActive(db, key); err != nil || !a {
		t.Fatalf("after raise: active=%v err=%v, want true/nil", a, err)
	}
	mustPlanExec(t, db, func() (*Command, error) { return PlanAlertClear(key, ts.Add(time.Minute)) })
	if a, _ := IsAlertActive(db, key); a {
		t.Fatal("after clear, want inactive")
	}
}

// TestAlertRaiseTimezoneDeterministic (DIFF-1): the SAME instant expressed in two different
// local zones bakes the BYTE-IDENTICAL raised_at literal (UTC-normalized), so two leaders in
// different zones produce identical committed entries for the same logical raise.
func TestAlertRaiseTimezoneDeterministic(t *testing.T) {
	utc := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	zone := time.FixedZone("UTC+8", 8*3600)
	same := utc.In(zone) // same instant, different zone
	key := DedupKeyGlobal(AlertKindReplicationDegraded)
	c1, err1 := PlanAlertRaise("x", AlertKindReplicationDegraded, AlertSeveritySevere, key, "m", utc)
	c2, err2 := PlanAlertRaise("x", AlertKindReplicationDegraded, AlertSeveritySevere, key, "m", same)
	if err1 != nil || err2 != nil {
		t.Fatalf("plan errors: %v %v", err1, err2)
	}
	if c1.Body[0].SQL != c2.Body[0].SQL {
		t.Fatalf("timezone-dependent baked SQL:\n utc =%s\n +08 =%s", c1.Body[0].SQL, c2.Body[0].SQL)
	}
}
