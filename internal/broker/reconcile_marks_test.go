package broker

import (
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// TestResolveReconcileMarks_G1Cases proves the pure G.1 classifier produces the
// exact MarkExited decisions reconcileOnRegister applies inline (PID-reuse -> -1,
// agent exit -> rc, unknown -> -1, missed-exit -> -1; an accepted running proc
// gets NO mark), then feeds them through the D2 ReconcileBatch op
// (proc.PlanReconcileBatch -> ExecCommand) and asserts the resulting processes
// table — closing the ReconcileBatch differential.
func TestResolveReconcileMarks_G1Cases(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at) VALUES('lab-1','lab','ONLINE',?)`, now); err != nil {
		t.Fatal(err)
	}

	mk := func(pid, boot string, ticks int64) {
		p := proc.Process{PID: pid, SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: boot, StartTimeTicks: ticks}
		if err := proc.Insert(db, p); err != nil {
			t.Fatalf("insert %s: %v", pid, err)
		}
	}
	mk("01a", "boot2", 100) // accepted: same boot + ticks as agent report
	mk("01b", "boot1", 200) // PID-reuse: boot mismatch vs agent's req.BootID
	mk("01c", "boot2", 300) // agent-reported exit rc=5
	mk("01d", "boot2", 400) // missed-exit: not in agent report
	mk("01e", "boot2", 500) // unknown agent state -> missed-exit

	rc5 := 5
	req := proto.NodeRegisterReq{
		BootID: "boot2",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01a", State: "running", StartTimeTicks: 100},
			{PID: "01b", State: "running", StartTimeTicks: 200},
			{PID: "01c", State: "exited", RC: &rc5},
			{PID: "01e", State: "weird"},
		},
	}
	procs, err := proc.ListBySessionFiltered(db, "lab", proc.ListBySessionOpts{IncludeExited: false})
	if err != nil {
		t.Fatal(err)
	}

	res := resolveReconcile("lab", "lab-1", req, procs, nil, now)
	marks := res.Marks
	got := map[string]int{}
	for _, m := range marks {
		got[m.PID] = m.ExitCode
	}
	want := map[string]int{"01b": -1, "01c": 5, "01d": -1, "01e": -1}
	if len(got) != len(want) {
		t.Fatalf("marks=%v want=%v", got, want)
	}
	for pid, rc := range want {
		if got[pid] != rc {
			t.Fatalf("mark %s rc=%d, want %d (all=%v)", pid, got[pid], rc, got)
		}
	}
	if _, ok := got["01a"]; ok {
		t.Fatal("01a (accepted running proc) must have NO mark")
	}

	// apply via the ReconcileBatch op and verify the processes table
	cmd, err := proc.PlanReconcileBatch(res)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	check := func(pid, wantStatus string, wantRC int, rcValid bool) {
		var st string
		var ec sql.NullInt64
		if err := db.QueryRow(`SELECT status, exit_code FROM processes WHERE pid=?`, pid).Scan(&st, &ec); err != nil {
			t.Fatalf("read %s: %v", pid, err)
		}
		if st != wantStatus {
			t.Fatalf("%s status=%s, want %s", pid, st, wantStatus)
		}
		if rcValid && (!ec.Valid || int(ec.Int64) != wantRC) {
			t.Fatalf("%s exit_code=%v, want %d", pid, ec, wantRC)
		}
		if !rcValid && ec.Valid {
			t.Fatalf("%s exit_code should be NULL (still RUNNING), got %d", pid, ec.Int64)
		}
	}
	check("01a", "RUNNING", 0, false) // accepted: untouched
	check("01b", "EXITED", -1, true)
	check("01c", "EXITED", 5, true)
	check("01d", "EXITED", -1, true)
	check("01e", "EXITED", -1, true)
}

// TestResolveReconcileMarks_VsLiveDifferential is the Stage C T6 fix: the classifier
// is a hand-copy of reconcileOnRegister's inline classification, so a drift could
// pass the hardcoded-want test. This runs the LIVE (*Broker).reconcileOnRegister and
// the classifier->ReconcileBatch op on identically-seeded DBs under the same input
// and asserts the resulting processes tables are identical — catching any drift.
func TestResolveReconcileMarks_VsLiveDifferential(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	silent := slogDiscard()

	seed := func() *sql.DB {
		db, err := storage.Open(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, now); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at) VALUES('lab-1','lab','ONLINE',?)`, now); err != nil {
			t.Fatal(err)
		}
		for _, p := range []proc.Process{
			{PID: "01a", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 100},
			{PID: "01b", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot1", StartTimeTicks: 200},
			{PID: "01c", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 300},
			{PID: "01d", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 400},
			{PID: "01e", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 500},
		} {
			if err := proc.Insert(db, p); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}
	rc5 := 5
	req := proto.NodeRegisterReq{
		BootID: "boot2",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01a", State: "running", StartTimeTicks: 100},
			{PID: "01b", State: "running", StartTimeTicks: 200},
			{PID: "01c", State: "exited", RC: &rc5},
			{PID: "01e", State: "weird"},
		},
	}

	// LIVE arm: reconcileOnRegister marks rows EXITED inline on dbLive.
	dbLive := seed()
	bLive := &Broker{cfg: Config{DB: dbLive, Logger: silent, Now: func() time.Time { return now }}}
	bLive.reconcileOnRegister("lab", "lab-1", req)

	// OP arm: classifier -> ReconcileBatch op on dbOp.
	dbOp := seed()
	procs, err := proc.ListBySessionFiltered(dbOp, "lab", proc.ListBySessionOpts{IncludeExited: false})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := proc.PlanReconcileBatch(resolveReconcile("lab", "lab-1", req, procs, nil, now))
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(dbOp, cmd); err != nil {
		t.Fatal(err)
	}

	if a, b := procSnapshot(t, dbLive), procSnapshot(t, dbOp); a != b {
		t.Fatalf("classifier drifted from live reconcileOnRegister:\n live=%s\n op  =%s", a, b)
	}
}

func procSnapshot(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT pid, status, CAST(COALESCE(exit_code,-9999) AS TEXT) FROM processes ORDER BY pid`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out string
	for rows.Next() {
		var pid, st, ec string
		if err := rows.Scan(&pid, &st, &ec); err != nil {
			t.Fatal(err)
		}
		out += pid + ":" + st + ":" + ec + "|"
	}
	return out
}

func slogDiscard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestD4ReconcileEquivalence_AuditSet (D4 §13.7 #3 live-vs-op) proves the op-path
// audit (resolveReconcile -> PlanReconcileBatch -> ReplayReconcileAudit) reproduces
// EXACTLY the audit the LIVE reconcileOnRegister emits — captured byte-for-byte via
// the auditTapForTest seam (NATS-free) and compared as a MULTISET (R-7: the live
// path emits killed_orphan in Go-map order, so sequence is not comparable without
// reordering the live path, which a zero-regression phase must not do). It covers
// reconciled_closed (rc set), killed_orphan (NO rc), PID-reuse (BOTH), and port
// reconciled (name/local_port from the agent report) — the exact drift surface.
func TestD4ReconcileEquivalence_AuditSet(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at) VALUES('lab-1','lab','ONLINE',?)`, now); err != nil {
		t.Fatal(err)
	}
	for _, p := range []proc.Process{
		{PID: "01a", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 100},
		{PID: "01b", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot1", StartTimeTicks: 200},
		{PID: "01c", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 300},
		{PID: "01d", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 400},
		{PID: "01e", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot2", StartTimeTicks: 500},
	} {
		if err := proc.Insert(db, p); err != nil {
			t.Fatal(err)
		}
	}
	// One ALLOCATED port the agent re-presents (keep, NO audit) + the agent will also
	// claim an unknown-token tunnel (orphan -> reconciled audit).
	if _, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp)
		VALUES (14022,'lab','lab-1','keepme',8888,'hashA','ALLOCATED','o')`); err != nil {
		t.Fatal(err)
	}

	rc5 := 5
	req := proto.NodeRegisterReq{
		BootID: "boot2",
		LocalProcesses: []proto.LocalProcess{
			{PID: "01a", State: "running", StartTimeTicks: 100}, // accepted -> no audit
			{PID: "01b", State: "running", StartTimeTicks: 200}, // PID-reuse -> reconciled_closed(-1) + killed_orphan
			{PID: "01c", State: "exited", RC: &rc5},             // reconciled_closed(5)
			{PID: "01e", State: "weird"},                        // unknown -> reconciled_closed(-1)
			{PID: "01forphan", State: "running"},                // orphan -> killed_orphan (no rc)
			// 01d absent -> missed-exit -> reconciled_closed(-1)
		},
		LocalPorts: []proto.LocalPort{
			{Port: 14022, Name: "keepme", LocalPort: 8888, TokenHash: "hashA"},      // keep -> no audit
			{Port: 15000, Name: "orphanport", LocalPort: 9999, TokenHash: "hashZZZ"}, // reconciled
		},
	}

	// OP arm (read state BEFORE the live arm mutates it): resolve -> bake -> replay.
	procs, err := proc.ListBySessionFiltered(db, "lab", proc.ListBySessionOpts{IncludeExited: false})
	if err != nil {
		t.Fatal(err)
	}
	ports, err := port.ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := proc.PlanReconcileBatch(resolveReconcile("lab", "lab-1", req, procs, ports, now))
	if err != nil {
		t.Fatal(err)
	}
	aps, pps, err := proc.ReplayReconcileAudit(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var opProc, opPort []string
	for _, r := range aps {
		b, _ := json.Marshal(r)
		opProc = append(opProc, string(b))
	}
	for _, r := range pps {
		b, _ := json.Marshal(r)
		opPort = append(opPort, string(b))
	}

	// LIVE arm: capture every audit publish byte-for-byte via the seam.
	var liveProc, livePort []string
	auditTapForTest = func(subject string, payload []byte) {
		switch subject {
		case proto.SubjAuditProc("lab"):
			liveProc = append(liveProc, string(payload))
		case proto.SubjAuditPort("lab"):
			livePort = append(livePort, string(payload))
		}
	}
	defer func() { auditTapForTest = nil }()
	bLive := &Broker{cfg: Config{DB: db, Logger: slogDiscard(), Now: func() time.Time { return now }}}
	bLive.reconcileOnRegister("lab", "lab-1", req)

	if !sameMultiset(liveProc, opProc) {
		t.Fatalf("proc-audit drift (op replay must reproduce live emission as a set):\n live=%v\n op  =%v", liveProc, opProc)
	}
	if !sameMultiset(livePort, opPort) {
		t.Fatalf("port-audit drift:\n live=%v\n op  =%v", livePort, opPort)
	}
	if len(opProc) != 6 {
		t.Fatalf("expected 6 proc audits (4 reconciled_closed + 2 killed_orphan), got %d: %v", len(opProc), opProc)
	}
	if len(opPort) != 1 {
		t.Fatalf("expected 1 port audit (reconciled), got %d: %v", len(opPort), opPort)
	}
}

// sameMultiset reports whether a and b contain the same strings with the same
// multiplicities (order-independent — R-7 set comparison).
func sameMultiset(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}
