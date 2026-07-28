package proc

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// openDB returns a fresh in-memory SQLite seeded with one ACTIVE session
// and one node — the (sid, nid) FK target proc.Insert needs.
// openDB opens the shared in-memory fixture and then SEEDS this package's session + node rows — see the
// note in internal/node/node_test.go for why the seed stays local.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:owner", "phc",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		"lab", "lab-1", "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
	return db
}

func sample() Process {
	return Process{
		PID:         "01abcdefghij",
		SID:         "lab",
		NID:         "lab-1",
		Argv:        []string{"echo", "hello"},
		Cwd:         "/tmp",
		StartedAt:   time.Now().UTC().Truncate(time.Second),
		StartedByFP: "SHA256:user",
		BootID:      "deadbeef",
	}
}

func TestNewPIDIsLowercase(t *testing.T) {
	pid := NewPID()
	if len(pid) != 26 {
		t.Errorf("ULID length should be 26, got %d", len(pid))
	}
	for _, c := range pid {
		if c >= 'A' && c <= 'Z' {
			t.Errorf("PID should be lowercase, got %q", pid)
			break
		}
	}
	// Two calls should produce different ids (overwhelmingly likely).
	if NewPID() == pid {
		t.Error("NewPID returned the same value twice")
	}
}

func TestInsertHappy(t *testing.T) {
	db := openDB(t)
	if err := Insert(db, sample()); err != nil {
		t.Fatal(err)
	}
	got, err := Get(db, "01abcdefghij")
	if err != nil {
		t.Fatal(err)
	}
	if got.SID != "lab" || got.NID != "lab-1" || got.Status != StateRunning {
		t.Errorf("unexpected row: %+v", got)
	}
	if len(got.Argv) != 2 || got.Argv[0] != "echo" || got.Argv[1] != "hello" {
		t.Errorf("argv: %v", got.Argv)
	}
}

func TestInsertRejectsMissingNode(t *testing.T) {
	db := openDB(t)
	in := sample()
	in.NID = "no-such-node"
	err := Insert(db, in)
	if !errors.Is(err, ErrNodeMissing) {
		t.Fatalf("expected ErrNodeMissing, got %v", err)
	}
}

func TestMarkExitedTransitions(t *testing.T) {
	db := openDB(t)
	if err := Insert(db, sample()); err != nil {
		t.Fatal(err)
	}
	end := time.Now().UTC().Truncate(time.Second)
	if err := MarkExited(db, "01abcdefghij", 0, end); err != nil {
		t.Fatal(err)
	}
	p, _ := Get(db, "01abcdefghij")
	if p.Status != StateExited {
		t.Errorf("status: got %s want EXITED", p.Status)
	}
	if p.ExitCode == nil || *p.ExitCode != 0 {
		t.Errorf("exit_code: %+v", p.ExitCode)
	}
	if p.EndedAt == nil || !p.EndedAt.Equal(end) {
		t.Errorf("ended_at: got %v want %v", p.EndedAt, end)
	}
	// Idempotent: marking again is a no-op (no error).
	if err := MarkExited(db, "01abcdefghij", 1, end.Add(time.Second)); err != nil {
		t.Errorf("second MarkExited: %v", err)
	}
}

func TestMarkExitedRejectsUnknown(t *testing.T) {
	db := openDB(t)
	if err := MarkExited(db, "ghost", 0, time.Now()); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListBySessionOrder(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	for i, args := range [][]string{{"a"}, {"b"}, {"c"}} {
		p := sample()
		p.PID = NewPID()
		p.Argv = args
		p.StartedAt = now.Add(time.Duration(i) * time.Second)
		if err := Insert(db, p); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
	// DESC by started_at: most recent first → ["c"], ["b"], ["a"].
	if got[0].Argv[0] != "c" || got[1].Argv[0] != "b" || got[2].Argv[0] != "a" {
		t.Errorf("order: %+v", got)
	}
}

// ----------------------------------------------------------------------
// ps-retention-plan §A — ListBySessionFiltered + GCExited + query plan.
// ----------------------------------------------------------------------

// insertRow inserts a processes row with the given fields, bypassing
// the FK-checking proc.Insert path so the test can set EXITED/ended_at
// directly. Caller supplies a pid (must be unique).
func insertRow(t *testing.T, db *sql.DB, pid, sid, status string, startedAt time.Time, endedAt *time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(sid, nid, status) VALUES (?,?,?)`,
		sid, "lab-1", "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
	var endedArg any
	if endedAt != nil {
		endedArg = *endedAt
	}
	_, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, started_by_fp)
		 VALUES (?,?,?,?,?,?,?,?)`,
		pid, sid, "lab-1", `["x"]`, startedAt, endedArg, status, "SHA256:u",
	)
	if err != nil {
		t.Fatalf("insert pid=%s status=%s: %v", pid, status, err)
	}
}

// A1 — IncludeExited=false returns only RUNNING rows.
func TestListBySessionFiltered_RunningOnly(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ended := base.Add(-time.Hour)
	for i := 0; i < 3; i++ {
		insertRow(t, db, "r"+string(rune('0'+i)), "lab", "RUNNING",
			base.Add(time.Duration(i)*time.Millisecond), nil)
	}
	for i := 0; i < 5; i++ {
		insertRow(t, db, "e"+string(rune('0'+i)), "lab", "EXITED",
			base.Add(time.Duration(i+3)*time.Millisecond), &ended)
	}

	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 RUNNING, got %d (rows=%+v)", len(got), got)
	}
	for _, p := range got {
		if p.Status != StateRunning {
			t.Errorf("leaked non-RUNNING row: %+v", p)
		}
	}
	// Newest first (DESC).
	if !got[0].StartedAt.After(got[1].StartedAt) || !got[1].StartedAt.After(got[2].StartedAt) {
		t.Errorf("order not started_at DESC: %v %v %v",
			got[0].StartedAt, got[1].StartedAt, got[2].StartedAt)
	}
}

// A2 — IncludeExited=true returns RUNNING + EXITED.
func TestListBySessionFiltered_IncludeExited(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ended := base.Add(-time.Hour)
	for i := 0; i < 3; i++ {
		insertRow(t, db, "r"+string(rune('0'+i)), "lab", "RUNNING",
			base.Add(time.Duration(i)*time.Millisecond), nil)
	}
	for i := 0; i < 5; i++ {
		insertRow(t, db, "e"+string(rune('0'+i)), "lab", "EXITED",
			base.Add(time.Duration(i+3)*time.Millisecond), &ended)
	}

	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 8 {
		t.Fatalf("want 8 rows, got %d", len(got))
	}
}

// A3 — Limit caps to the N newest rows.
func TestListBySessionFiltered_Limit(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 10; i++ {
		insertRow(t, db, "p"+string(rune('0'+i)), "lab", "RUNNING",
			base.Add(time.Duration(i)*time.Microsecond), nil)
	}
	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: true, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	// Newest should be p9, p8, p7 (started_at DESC).
	wantPIDs := []string{"p9", "p8", "p7"}
	for i, p := range got {
		if p.PID != wantPIDs[i] {
			t.Errorf("got[%d].PID=%q want=%q", i, p.PID, wantPIDs[i])
		}
	}
}

// A4 — LIMIT applied AFTER the status filter.
func TestListBySessionFiltered_LimitWithFilter(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ended := base.Add(-time.Hour)
	// Interleave RUNNING and EXITED by started_at.
	pids := []struct {
		name   string
		status string
	}{
		{"r0", "RUNNING"}, {"e0", "EXITED"}, {"r1", "RUNNING"},
		{"e1", "EXITED"}, {"r2", "RUNNING"}, {"e2", "EXITED"},
		{"r3", "RUNNING"}, {"e3", "EXITED"}, {"r4", "RUNNING"}, {"e4", "EXITED"},
	}
	for i, p := range pids {
		var ea *time.Time
		if p.status == "EXITED" {
			ea = &ended
		}
		insertRow(t, db, p.name, "lab", p.status,
			base.Add(time.Duration(i)*time.Microsecond), ea)
	}
	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: false, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 RUNNING, got %d", len(got))
	}
	for _, p := range got {
		if p.Status != StateRunning {
			t.Errorf("leaked non-RUNNING: %+v", p)
		}
	}
	// Newest 2 RUNNING are r4 (i=8) and r3 (i=6).
	wantPIDs := []string{"r4", "r3"}
	for i, p := range got {
		if p.PID != wantPIDs[i] {
			t.Errorf("got[%d].PID=%q want=%q", i, p.PID, wantPIDs[i])
		}
	}
}

// A5 — Limit=0 returns the whole filtered set.
func TestListBySessionFiltered_LimitZeroMeansUnlimited(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 7; i++ {
		insertRow(t, db, "p"+string(rune('0'+i)), "lab", "RUNNING",
			base.Add(time.Duration(i)*time.Microsecond), nil)
	}
	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: true, Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 7 {
		t.Fatalf("want 7, got %d", len(got))
	}
}

// A6 — empty session.
func TestListBySessionFiltered_EmptySession(t *testing.T) {
	db := openDB(t)
	got, err := ListBySessionFiltered(db, "lab",
		ListBySessionOpts{IncludeExited: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0, got %d", len(got))
	}
}

// A7 — GCExited deletes only EXITED rows past cutoff.
func TestGCExited_DeletesOldExited(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	twoHoursAgo := now.Add(-2 * time.Hour)
	oneMinAgo := now.Add(-time.Minute)
	insertRow(t, db, "old", "lab", "EXITED", now.Add(-3*time.Hour), &twoHoursAgo)
	insertRow(t, db, "new", "lab", "EXITED", now.Add(-time.Hour), &oneMinAgo)
	insertRow(t, db, "alive", "lab", "RUNNING", now, nil)

	n, err := GCExited(db, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("want 1 deleted, got %d", n)
	}

	var (
		exitedCount, runningCount int
	)
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='EXITED'`).Scan(&exitedCount)
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='RUNNING'`).Scan(&runningCount)
	if exitedCount != 1 || runningCount != 1 {
		t.Errorf("after GC: EXITED=%d RUNNING=%d (want 1, 1)", exitedCount, runningCount)
	}
}

// A8 — RUNNING rows never deleted by GCExited.
func TestGCExited_RunningNeverDeleted(t *testing.T) {
	db := openDB(t)
	tenHoursAgo := time.Now().UTC().Add(-10 * time.Hour)
	for i := 0; i < 5; i++ {
		insertRow(t, db, "r"+string(rune('0'+i)), "lab", "RUNNING", tenHoursAgo, nil)
	}
	n, err := GCExited(db, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("want 0 deleted, got %d", n)
	}
	var c int
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='RUNNING'`).Scan(&c)
	if c != 5 {
		t.Errorf("want 5 RUNNING surviving, got %d", c)
	}
}

// A9 — GCExited on empty table.
func TestGCExited_EmptyTable(t *testing.T) {
	db := openDB(t)
	n, err := GCExited(db, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("want 0 deleted, got %d", n)
	}
}

// A10 — all EXITED younger than cutoff → 0 deleted.
func TestGCExited_AllExitedYounger(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	oneMinAgo := now.Add(-time.Minute)
	for i := 0; i < 3; i++ {
		insertRow(t, db, "e"+string(rune('0'+i)), "lab", "EXITED",
			oneMinAgo.Add(-time.Second*time.Duration(i)), &oneMinAgo)
	}
	n, err := GCExited(db, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("want 0 deleted (all younger than cutoff), got %d", n)
	}
}

// A11 — ListBySession (wrapper) backward-compat with IncludeExited=true.
func TestListBySessionFiltered_WrapperBackwardCompat(t *testing.T) {
	db := openDB(t)
	base := time.Now().UTC()
	ended := base.Add(-time.Hour)
	insertRow(t, db, "r1", "lab", "RUNNING", base, nil)
	insertRow(t, db, "e1", "lab", "EXITED", base.Add(time.Millisecond), &ended)

	got, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows from wrapper, got %d", len(got))
	}
}

// A12 — EXPLAIN QUERY PLAN for the three queries does not contain
// "USE TEMP B-TREE FOR ORDER BY", and names the expected indexes.
func TestPsQueryPlan_NoTempBTree(t *testing.T) {
	db := openDB(t)

	cases := []struct {
		name      string
		query     string
		args      []any
		wantIdx   string
		forbidden string
	}{
		{
			name: "default ps (IncludeExited=false, Limit=500)",
			query: `EXPLAIN QUERY PLAN
				SELECT pid FROM processes
				WHERE sid = ? AND status = 'RUNNING'
				ORDER BY started_at DESC LIMIT 500`,
			args:      []any{"lab"},
			wantIdx:   "idx_processes_sid_status_started",
			forbidden: "USE TEMP B-TREE FOR ORDER BY",
		},
		{
			name: "ps -a (IncludeExited=true, Limit=500)",
			query: `EXPLAIN QUERY PLAN
				SELECT pid FROM processes
				WHERE sid = ?
				ORDER BY started_at DESC LIMIT 500`,
			args:      []any{"lab"},
			wantIdx:   "idx_processes_sid_started",
			forbidden: "USE TEMP B-TREE FOR ORDER BY",
		},
		{
			name: "GC sweep",
			query: `EXPLAIN QUERY PLAN
				DELETE FROM processes
				WHERE status = 'EXITED' AND ended_at < ?`,
			args:    []any{time.Now()},
			wantIdx: "idx_processes_status_endedat",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(tc.query, tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			plan := []string{}
			for rows.Next() {
				var id, parent, notused int
				var detail string
				if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
					t.Fatal(err)
				}
				plan = append(plan, detail)
			}
			_ = rows.Close()
			joined := ""
			for _, l := range plan {
				joined += l + "\n"
			}
			if tc.wantIdx != "" && !contains(joined, tc.wantIdx) {
				t.Errorf("plan missing index %q\n--- plan ---\n%s", tc.wantIdx, joined)
			}
			if tc.forbidden != "" && contains(joined, tc.forbidden) {
				t.Errorf("plan contains forbidden %q\n--- plan ---\n%s", tc.forbidden, joined)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
