package proc

// plan_test.go — leader-side Plan renderer tests. Currently covers the h1 B1
// OpProcGC plan (PlanGCExited); the older Plan* renderers are exercised by the
// broker-side differential/equivalence harnesses.
// origin: docs/reviews/h1-plan.md workstream B (2026-08-04 incident: cluster
// mode had NO processes retention at all — 8.5k EXITED rows on the fleet).

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func gcTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('s1','s1','SHA256:o','h')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status) VALUES ('s1','n1','ONLINE')`,
	); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedGCProc(t *testing.T, db *sql.DB, pid, status string, startedAt time.Time, endedAt any) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, started_by_fp)
		 VALUES (?,?,?,?,?,?,?,'SHA256:u')`,
		pid, "s1", "n1", `["x"]`, startedAt, endedAt, status,
	); err != nil {
		t.Fatal(err)
	}
}

func TestPlanGCExitedSelectsExitedOnly(t *testing.T) {
	db := gcTestDB(t)
	old := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

	seedGCProc(t, db, "running", "RUNNING", old, nil)            // NEVER collected
	seedGCProc(t, db, "exited-old", "EXITED", old, old)          // due
	seedGCProc(t, db, "exited-recent", "EXITED", recent, recent) // inside retention
	// NULL ended_at EXITED row: COALESCE falls back to started_at.
	seedGCProc(t, db, "exited-null", "EXITED", old, nil) // due via started_at
	// Adversarial pid: an embedded quote must round-trip through LitText.
	seedGCProc(t, db, "exited-'quote", "EXITED", old, old) // due

	cmd, n, err := PlanGCExited(db, cutoff, 500)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("planned %d rows, want 3 (exited-old, exited-null, exited-'quote)", n)
	}
	baked := cmd.Body[0].SQL
	if len(cmd.Body[0].Args) != 0 || strings.Contains(baked, "?") {
		t.Fatalf("GC Apply SQL must be all-literal: %q args=%v", baked, cmd.Body[0].Args)
	}
	if strings.Contains(baked, "2026") || strings.Contains(strings.ToLower(baked), "now") {
		t.Fatalf("GC Apply SQL leaked a timestamp/now to the replica: %q", baked)
	}
	if !strings.Contains(baked, "AND status='EXITED'") {
		t.Fatalf("GC Apply SQL dropped the terminal-state guard: %q", baked)
	}

	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("replaying the baked GC command must be a no-op, got %v", err)
	}
	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM processes`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 2 {
		t.Fatalf("after GC: %d rows left, want 2 (running + exited-recent)", left)
	}
	var runStatus string
	if err := db.QueryRow(`SELECT status FROM processes WHERE pid='running'`).Scan(&runStatus); err != nil {
		t.Fatalf("RUNNING row was deleted: %v", err)
	}

	cmd2, n2, err := PlanGCExited(db, cutoff, 500)
	if err != nil {
		t.Fatal(err)
	}
	if cmd2 != nil || n2 != 0 {
		t.Fatalf("re-plan after convergence: cmd=%v n=%d, want nil/0", cmd2, n2)
	}
}

func TestPlanRefileStatementsMoveOnlyRunningRowsDeterministically(t *testing.T) {
	db := gcTestDB(t)
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('s1','n2','ONLINE')`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seedGCProc(t, db, "p-b", "RUNNING", now, nil)
	seedGCProc(t, db, "p-a", "RUNNING", now, nil)
	seedGCProc(t, db, "p-exited", "EXITED", now, now)

	stmts, err := PlanRefileStatements("s1", "n1", "n2", []string{"p-b", "p-a", "p-b", "p-exited"})
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 3 || !strings.Contains(stmts[0].SQL, "'p-a'") ||
		!strings.Contains(stmts[1].SQL, "'p-b'") {
		t.Fatalf("refile statements are not deduplicated and pid-sorted: %+v", stmts)
	}
	if err := cluster.ExecCommand(db, cluster.NewCommand(cluster.OpNodeRegister, stmts...)); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []string{"p-a", "p-b"} {
		var nid string
		if err := db.QueryRow(`SELECT nid FROM processes WHERE pid=?`, pid).Scan(&nid); err != nil {
			t.Fatal(err)
		}
		if nid != "n2" {
			t.Fatalf("running row %s stayed under %s", pid, nid)
		}
	}
	var exitedNID string
	if err := db.QueryRow(`SELECT nid FROM processes WHERE pid='p-exited'`).Scan(&exitedNID); err != nil {
		t.Fatal(err)
	}
	if exitedNID != "n1" {
		t.Fatalf("terminal history was refiled from n1 to %s", exitedNID)
	}
}

// TestPlanGCExitedLegacyLocalZoneText documents the BOUNDED best-effort
// contract for pre-h1 heterogeneous ended_at text (plan critique-1): a legacy
// G.1 row baked with a raw local-zone String() ("… +0800 CST m=+…") compares
// LEXICALLY, so it is collected by its WALL-CLOCK prefix, not its true UTC
// instant — i.e. it may survive up to ~zone-offset longer than an exact
// comparison would allow, and never crashes the plan. The retention (1h proc /
// 24h port) dwarfs any zone offset, so the skew is bounded and accepted; G.1
// bakes UTC going forward (reconcile.go), which is what this test would catch
// regressing.
func TestPlanGCExitedLegacyLocalZoneText(t *testing.T) {
	db := gcTestDB(t)
	old := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	// Legacy text: wall-clock 22:00 on Aug 2 in +0800 (= 14:00 UTC), with the
	// monotonic suffix a raw time.Time.String() carries.
	legacy := "2026-08-02 22:00:00.000000001 +0800 CST m=+8123.456"
	seedGCProc(t, db, "legacy", "EXITED", old, legacy)

	// Cutoff Aug 2 18:00 UTC: the row's TRUE instant (14:00 UTC) is older —
	// an exact comparison would collect it — but its lexical prefix (22:00)
	// is newer, so the documented behavior is: NOT collected yet.
	_, n, err := PlanGCExited(db, time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC), 500)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("legacy row collected by true instant (%d) — the documented contract is lexical/bounded, update the doc if the comparison got smarter", n)
	}
	// Once the cutoff's lexical prefix passes the row's wall-clock text, it IS
	// collected — the row is not immortal.
	cmd, n, err := PlanGCExited(db, time.Date(2026, 8, 2, 23, 0, 0, 0, time.UTC), 500)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("legacy row must be collected once lexically past cutoff, got %d", n)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
}

func TestPlanGCExitedClosesRows(t *testing.T) {
	db := gcTestDB(t)
	db.SetMaxOpenConns(1)
	old := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedGCProc(t, db, "e1", "EXITED", old, old)

	if _, _, err := PlanGCExited(db, old.AddDate(0, 0, 1), 500); err != nil {
		t.Fatal(err)
	}
	if in := db.Stats().InUse; in != 0 {
		t.Fatalf("plan returned with %d connection(s) in use — an open *sql.Rows here deadlocks the FSM pool", in)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin after plan on a 1-conn pool: %v", err)
	}
	_ = tx.Rollback()
}
