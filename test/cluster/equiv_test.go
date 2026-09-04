package cluster_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
)

// acceptPIN is a verifyPIN stub that accepts (the differential feeds identical
// inputs to both arms, so PIN verification must behave identically; success is
// the interesting path for the row-writing ops).
func acceptPIN(pin, hash string) error { return nil }

// freshDB returns a fresh fully-migrated in-memory DB (FK enforced). Both the
// direct-mutator arm and the FSM-rendered arm of the D2 equivalence/differential
// tests run on an independent freshDB so they are genuinely different code paths,
// not a self-comparison.
func freshDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSessionNode inserts the session + node parent rows so a port_allocations FK
// (sid,nid)->nodes is satisfiable. This is test fixture (not the op under test);
// both arms are seeded identically.
func seedSessionNode(t *testing.T, db *sql.DB, sid, nid string, now time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES(?,?,?,?, 'ACTIVE', ?)`,
		sid, sid, "SHA256:owner", "pinhash", now,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(nid,sid,status,registered_at) VALUES(?,?, 'ONLINE', ?)`,
		nid, sid, now,
	); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

// logicalHash hashes a table's logical content: every column except those in
// exclude, every value CAST to TEXT, rows ordered by rowid (so AUTOINCREMENT
// divergence is visible — a row that got a different rowid sorts to a different
// position and changes the hash). This is the §13.2 / DIFF-1 comparator primitive.
func logicalHash(t *testing.T, db *sql.DB, table string, exclude ...string) string {
	t.Helper()
	ex := map[string]bool{}
	for _, c := range exclude {
		ex[c] = true
	}
	// columns in schema order
	tiRows, err := db.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
	if err != nil {
		t.Fatalf("table_info %s: %v", table, err)
	}
	var cols []string
	for tiRows.Next() {
		var name string
		if err := tiRows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if !ex[name] {
			cols = append(cols, name)
		}
	}
	_ = tiRows.Close()
	if len(cols) == 0 {
		t.Fatalf("table %s: no comparable columns", table)
	}
	sel := []string{`CAST(rowid AS TEXT)`}
	for _, c := range cols {
		sel = append(sel, `CAST("`+c+`" AS TEXT)`)
	}
	q := `SELECT ` + strings.Join(sel, ",") + ` FROM "` + table + `" ORDER BY rowid`
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("scan %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	h := sha256.New()
	for rows.Next() {
		vals := make([]sql.NullString, len(cols)+1)
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for i, v := range vals {
			tag := "rowid"
			if i > 0 {
				tag = cols[i-1]
			}
			if v.Valid {
				_, _ = fmt.Fprintf(h, "%s=%q|", tag, v.String)
			} else {
				_, _ = fmt.Fprintf(h, "%s=NULL|", tag)
			}
		}
		h.Write([]byte("\n"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// fixedClock returns a port.Config whose Now is pinned, so both arms bake the
// same created_at and the comparison is not defeated by wall-clock skew.
func fixedClock(now time.Time) *port.Config {
	return &port.Config{Now: func() time.Time { return now }}
}

// TestPortAllocate_RenderingEquivalence is the core §13.2 proof for PortAllocate:
// the FSM-rendered op (PlanAllocate -> ExecCommand) produces a port_allocations
// row LOGICALLY identical to today's direct port.Allocate, on independent DBs.
// token_hash is excluded (the raw token is random per call, so the two arms mint
// different tokens — token-hash determinism is proven separately by §13.2
// same-command replay; here we additionally assert the raw token never leaks into
// the replicated Command and that the stored hash matches the returned token).
func TestPortAllocate_RenderingEquivalence(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 123456789, time.UTC)
	cfg := fixedClock(now)

	da := freshDB(t)
	fa := freshDB(t)
	seedSessionNode(t, da, "lab", "lab-1", now)
	seedSessionNode(t, fa, "lab", "lab-1", now)

	// direct arm
	if _, err := port.Allocate(da, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", false, cfg); err != nil {
		t.Fatalf("direct Allocate: %v", err)
	}
	// fsm-rendered arm
	alloc, cmd, err := port.PlanAllocate(fa, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", "", false, cfg)
	if err != nil {
		t.Fatalf("PlanAllocate: %v", err)
	}
	if err := cluster.ExecCommand(fa, cmd); err != nil {
		t.Fatalf("ExecCommand: %v", err)
	}

	// secret-leak guard: the RAW token must never appear in the replicated Command
	// (only its hash). Assert on the ACTUAL wire bytes (json.Marshal, the exact
	// cmd.encode() path), not %+v — a future Args-carrying op could leak via a field
	// %+v renders differently. (Stage C minor.)
	encB, mErr := json.Marshal(cmd)
	if mErr != nil {
		t.Fatal(mErr)
	}
	enc := string(encB)
	if alloc.Token == "" {
		t.Fatal("PlanAllocate returned an empty raw token")
	}
	if strings.Contains(enc, alloc.Token) {
		t.Fatalf("raw token LEAKED into the replicated Command: %s", enc)
	}
	if !strings.Contains(enc, alloc.TokenHash) {
		t.Fatal("token_hash not present in the Command")
	}
	var storedHash string
	if err := fa.QueryRow(`SELECT token_hash FROM port_allocations WHERE port=?`, alloc.Port).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash != alloc.TokenHash {
		t.Fatalf("stored token_hash %q != returned %q", storedHash, alloc.TokenHash)
	}

	// logical equivalence (excluding the random token_hash)
	hd := logicalHash(t, da, "port_allocations", "token_hash")
	hf := logicalHash(t, fa, "port_allocations", "token_hash")
	if hd != hf {
		t.Fatalf("PortAllocate divergence:\n direct=%s\n fsm   =%s", hd, hf)
	}

	// NEGATIVE CONTROL: a corrupted baked value (wrong port) MUST diverge — proves
	// the comparator actually bites.
	fb := freshDB(t)
	seedSessionNode(t, fb, "lab", "lab-1", now)
	badAlloc, badCmd, err := port.PlanAllocate(fb, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", "", false, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = badAlloc
	badCmd.Body[0].SQL = strings.Replace(badCmd.Body[0].SQL, "8888", "9999", 1)
	if err := cluster.ExecCommand(fb, badCmd); err != nil {
		t.Fatal(err)
	}
	if logicalHash(t, fb, "port_allocations", "token_hash") == hd {
		t.Fatal("negative control did not diverge — comparator is vacuous")
	}
}

// TestDifferential_MultiOp is the D2 exit-gate shape (DIFF-1): a realistic op
// sequence (session create -> proc start/exit -> port allocate/free -> session
// tombstone) driven through BOTH today's direct mutators AND the FSM Plan*->
// ExecCommand path on independent DBs, asserting every affected table is
// logically identical. token_hash is excluded (random per call); everything else
// — including raw-bound RAW timestamps (session) vs UTC (port/proc bind their own)
// and AUTOINCREMENT rowids — must match byte-for-byte.
// buildMultiOp runs the realistic D2 op sequence through either today's direct
// mutators (useFSM=false) or the FSM Plan*->ExecCommand path (useFSM=true) under a
// caller-supplied clock, returning the resulting DB. Parameterizing `now` lets the
// differential run under BOTH a UTC and a NON-UTC clock: session.* bind the raw
// now while port/proc/node/agentprov bind now.UTC(), so a correct pair converges on
// any zone but a swapped .UTC() choice in any Plan func diverges only off-UTC
// (Stage C blocker T1).
func buildMultiOp(t *testing.T, useFSM bool, now time.Time, cfg *port.Config) *sql.DB {
	t.Helper()
	const pid = "01abcpidulid000000000000ab"
	db := freshDB(t)
	apply := func(c *cluster.Command, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if err := cluster.ExecCommand(db, c); err != nil {
			t.Fatalf("exec: %v", err)
		}
	}
	if useFSM {
		apply(session.PlanCreate(db, "lab", "lab", "SHA256:owner", "pinhash", now))
	} else if _, err := session.Create(db, "lab", "lab", "SHA256:owner", "pinhash", now); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
	regIn := node.RegisterInput{SID: "lab", NID: "lab-1", BootID: "boot1",
		ReleaseVersion: "v1", ProtoVersion: 2, ProxyCapable: true}
	if useFSM {
		apply(node.PlanRegister(db, regIn, now))
	} else if err := node.Register(db, regIn, now); err != nil {
		t.Fatalf("node.Register: %v", err)
	}
	p := proc.Process{PID: pid, SID: "lab", NID: "lab-1", Argv: []string{"sleep", "1"}, Cwd: "/tmp",
		StartedAt: now, StartedByFP: "SHA256:caller", BootID: "boot1", StartTimeTicks: 12345}
	if useFSM {
		apply(proc.PlanInsert(db, p))
	} else if err := proc.Insert(db, p); err != nil {
		t.Fatalf("proc.Insert: %v", err)
	}
	if useFSM {
		apply(proc.PlanMarkExited(db, pid, p.SID, 0, now))
	} else if err := proc.MarkExited(db, pid, p.SID, 0, now); err != nil {
		t.Fatalf("proc.MarkExited: %v", err)
	}
	if useFSM {
		apply(agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", "SHA256:agent", "1234", acceptPIN, now))
	} else if err := agentprov.ProvisionWithPIN(db, "lab", "lab-1", "SHA256:agent", "1234", acceptPIN, now); err != nil {
		t.Fatalf("agentprov.ProvisionWithPIN: %v", err)
	}
	if useFSM {
		apply(session.PlanJoinWithPIN(db, "lab", "SHA256:member", "1234", acceptPIN, now))
	} else if err := session.JoinWithPIN(db, "lab", "SHA256:member", "1234", acceptPIN, now); err != nil {
		t.Fatalf("session.JoinWithPIN: %v", err)
	}
	if useFSM {
		_, cmd, err := port.PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", "", false, cfg)
		apply(cmd, err)
	} else if _, err := port.Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", false, cfg); err != nil {
		t.Fatalf("port.Allocate: %v", err)
	}
	var pport int
	if err := db.QueryRow(`SELECT port FROM port_allocations WHERE name='jupyter'`).Scan(&pport); err != nil {
		t.Fatal(err)
	}
	if useFSM {
		apply(port.PlanFree(db, pport, now))
	} else if err := port.Free(db, pport, now); err != nil {
		t.Fatalf("port.Free: %v", err)
	}
	if useFSM {
		apply(session.PlanTombstone(db, "lab", now))
	} else if err := session.Tombstone(db, "lab", now); err != nil {
		t.Fatalf("session.Tombstone: %v", err)
	}
	return db
}

// multiOpTables is the comparator's table set + per-table exclusions. nodes
// excludes the leader-local liveness columns (§3.5); port_allocations excludes the
// random token_hash (token-hash determinism is proven by TestMultiFSM_SameStreamConverges).
var multiOpTables = []struct {
	table string
	excl  []string
}{
	{"sessions", nil},
	{"members", nil},
	{"processes", nil},
	{"port_allocations", []string{"token_hash"}},
	{"agent_provisioning", nil},
	{"nodes", []string{"status", "last_heartbeat_at", "proxy_ready"}},
}

func compareMultiOp(t *testing.T, d, f *sql.DB, clockLabel string) {
	t.Helper()
	for _, tc := range multiOpTables {
		hd := logicalHash(t, d, tc.table, tc.excl...)
		hf := logicalHash(t, f, tc.table, tc.excl...)
		if hd != hf {
			t.Fatalf("[%s clock] table %q diverged direct-vs-FSM:\n direct=%s\n fsm   =%s", clockLabel, tc.table, hd, hf)
		}
	}
}

func TestDifferential_MultiOp(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 123456789, time.UTC)
	cfg := fixedClock(now)
	compareMultiOp(t, buildMultiOp(t, false, now, cfg), buildMultiOp(t, true, now, cfg), "UTC")
}

// TestDifferential_MultiOp_NonUTC is the Stage C T1 fix: the UTC-only differential
// is BLIND to a wrong UTC-vs-raw choice (under a UTC clock now==now.UTC() byte-for-
// byte, so a Plan func that wrongly applies/omits .UTC() bakes an identical literal).
// Under a +0800 clock, session.* (raw) store +0800 while port/proc/node/agentprov
// (now.UTC()) store UTC — a correct pair still converges, but any swapped .UTC()
// diverges. (Proven: injecting .UTC() into session.PlanCreate makes THIS test fail.)
func TestDifferential_MultiOp_NonUTC(t *testing.T) {
	cst := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 6, 21, 21, 14, 15, 123456789, cst)
	cfg := fixedClock(now)
	compareMultiOp(t, buildMultiOp(t, false, now, cfg), buildMultiOp(t, true, now, cfg), "+0800")
}

// TestNodeRegister_IdentityOnly_PreservesLivenessOnConflict is the Stage C T7
// column-level liveness lint (§3.5 / d2-plan §6(c)). OpNodeRegister may set the
// INSERT baseline status='OFFLINE' so a replicated identity row is not live before
// a heartbeat, but its ON CONFLICT identity update must never touch status,
// last_heartbeat_at, or proxy_ready.
func TestNodeRegister_IdentityOnly_PreservesLivenessOnConflict(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	db := freshDB(t)
	if _, err := session.Create(db, "lab", "lab", "o", "p", now); err != nil {
		t.Fatal(err)
	}
	cmd, err := node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "lab-1", BootID: "b", ReleaseVersion: "v1", ProtoVersion: 2, ProxyCapable: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(cmd.Body[0].SQL)
	for _, liveness := range []string{"last_heartbeat_at", "proxy_ready"} {
		if strings.Contains(sqlText, liveness) {
			t.Fatalf("OpNodeRegister SQL names leader-local liveness column %q — Apply must never write it (§3.5):\n%s", liveness, cmd.Body[0].SQL)
		}
	}
	parts := strings.Split(sqlText, "do update set")
	if len(parts) != 2 {
		t.Fatalf("OpNodeRegister SQL missing ON CONFLICT DO UPDATE clause:\n%s", cmd.Body[0].SQL)
	}
	if !strings.Contains(parts[0], "status") || !strings.Contains(parts[0], "'offline'") {
		t.Fatalf("OpNodeRegister insert must create an OFFLINE baseline status:\n%s", cmd.Body[0].SQL)
	}
	if strings.Contains(parts[1], "status") {
		t.Fatalf("OpNodeRegister conflict update must preserve status:\n%s", cmd.Body[0].SQL)
	}
	// non-vacuity: it MUST write identity columns + carry the ON CONFLICT re-register clause.
	for _, identity := range []string{"registered_at", "boot_id", "on conflict"} {
		if !strings.Contains(sqlText, identity) {
			t.Fatalf("OpNodeRegister SQL missing expected %q — test may be vacuous:\n%s", identity, cmd.Body[0].SQL)
		}
	}
	// the ON CONFLICT path applies (re-register with changed identity) without error.
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	cmd2, err := node.PlanRegister(db, node.RegisterInput{SID: "lab", NID: "lab-1", BootID: "b2", ReleaseVersion: "v2", ProtoVersion: 2, ProxyCapable: false}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd2); err != nil {
		t.Fatalf("re-register (ON CONFLICT) failed: %v", err)
	}
	var boot string
	_ = db.QueryRow(`SELECT boot_id FROM nodes WHERE sid='lab' AND nid='lab-1'`).Scan(&boot)
	if boot != "b2" {
		t.Fatalf("re-register did not update boot_id: %q", boot)
	}
}

// TestPortRevoke_Equivalence (Stage C T2): PlanRevoke (ALLOCATED->REVOKED) is a
// near-clone of PlanFree differing only by the baked state literal — a copy-paste
// 'FREED'-for-'REVOKED' bug would otherwise ship green. Both arms allocate directly
// (to isolate the Revoke op), then Revoke direct vs PlanRevoke->ExecCommand.
func TestPortRevoke_Equivalence(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)
	run := func(useFSM bool) *sql.DB {
		db := freshDB(t)
		seedSessionNode(t, db, "lab", "lab-1", now)
		a, err := port.Allocate(db, "lab", "lab-1", "j", 1000, 0, "SHA256:fp", false, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if useFSM {
			cmd, err := port.PlanRevoke(db, a.Port, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.ExecCommand(db, cmd); err != nil {
				t.Fatal(err)
			}
		} else if err := port.Revoke(db, a.Port, now); err != nil {
			t.Fatal(err)
		}
		return db
	}
	d, f := run(false), run(true)
	if logicalHash(t, d, "port_allocations", "token_hash") != logicalHash(t, f, "port_allocations", "token_hash") {
		t.Fatal("PlanRevoke diverged from port.Revoke (state='REVOKED'/revoked_at?)")
	}
	var st string
	_ = f.QueryRow(`SELECT state FROM port_allocations`).Scan(&st)
	if st != "REVOKED" {
		t.Fatalf("PlanRevoke produced state=%q, want REVOKED", st)
	}
	// not-found parity: both return ErrNotFound for an unknown port.
	db := freshDB(t)
	if _, err := port.PlanRevoke(db, 65000, now); err != port.ErrNotFound {
		t.Fatalf("PlanRevoke on missing port: got %v, want ErrNotFound", err)
	}
}

// TestNodeEvict_Equivalence (Stage C T2): PlanEvict bakes the two DELETEs of
// adminsock handleEvict (DELETE agent_provisioning + DELETE nodes, the latter
// cascading processes/port_allocations). Both arms run the same two statements;
// the test confirms PlanEvict bakes them correctly (order + columns) and the whole
// (sid,nid) subtree is gone identically.
func TestNodeEvict_Equivalence(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)
	seed := func(db *sql.DB) {
		seedSessionNode(t, db, "lab", "lab-1", now)
		if err := proc.Insert(db, proc.Process{PID: "01p", SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "b"}); err != nil {
			t.Fatal(err)
		}
		if _, err := port.Allocate(db, "lab", "lab-1", "j", 1000, 0, "fp", false, cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES('lab','lab-1','SHA256:a',?)`, now); err != nil {
			t.Fatal(err)
		}
	}
	run := func(useFSM bool) *sql.DB {
		db := freshDB(t)
		seed(db)
		if useFSM {
			cmd, err := node.PlanEvict("lab", "lab-1")
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.ExecCommand(db, cmd); err != nil {
				t.Fatal(err)
			}
		} else {
			// adminsock handleEvict order: agent_provisioning then nodes.
			if _, err := db.Exec(`DELETE FROM agent_provisioning WHERE sid='lab' AND nid='lab-1'`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM nodes WHERE sid='lab' AND nid='lab-1'`); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}
	d, f := run(false), run(true)
	for _, tbl := range []string{"nodes", "agent_provisioning", "processes", "port_allocations"} {
		if logicalHash(t, d, tbl) != logicalHash(t, f, tbl) {
			t.Fatalf("PlanEvict diverged on %s", tbl)
		}
		var n int
		_ = f.QueryRow(`SELECT COUNT(*) FROM ` + tbl + ` WHERE sid='lab'`).Scan(&n)
		if n != 0 {
			t.Fatalf("%s not empty for evicted (sid,nid): %d rows", tbl, n)
		}
	}
	// evict of a nonexistent (sid,nid) is a clean no-op.
	empty := freshDB(t)
	cmd, _ := node.PlanEvict("ghost", "ghost-1")
	if err := cluster.ExecCommand(empty, cmd); err != nil {
		t.Fatalf("evict of nonexistent node must be a clean no-op: %v", err)
	}
}

// TestPlanAllocate_DesiredPortCollision is the Stage C B1 regression: a desired
// port already ALLOCATED must return ErrPortTaken from Plan (BEFORE proposing), not
// bake a doomed INSERT that fail-stop PANICS the FSM at Apply.
func TestPlanAllocate_DesiredPortCollision(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)
	db := freshDB(t)
	seedSessionNode(t, db, "lab", "lab-1", now)
	a, err := port.Allocate(db, "lab", "lab-1", "first", 1000, 0, "fp", false, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// request the SAME public port with a different name -> must be ErrPortTaken.
	_, _, err = port.PlanAllocate(db, "lab", "lab-1", "second", 1001, a.Port, "fp", "", false, cfg)
	if err != port.ErrPortTaken {
		t.Fatalf("desired-port collision: got %v, want ErrPortTaken (a bare INSERT would panic the FSM at Apply)", err)
	}
}

// TestPlanError_Parity (Stage C T3): each Plan func that translates a live
// INSERT/UPDATE diagnosis into a pre-propose read must return the SAME typed error
// as the live mutator on the same error-inducing input.
func TestPlanError_Parity(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)

	t.Run("session_already_exists", func(t *testing.T) {
		db := freshDB(t)
		if _, err := session.Create(db, "lab", "lab", "o", "p", now); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Create(db, "lab", "lab", "o", "p", now); err != session.ErrAlreadyExists {
			t.Fatalf("live re-create: %v", err)
		}
		if _, err := session.PlanCreate(db, "lab", "lab", "o", "p", now); err != session.ErrAlreadyExists {
			t.Fatalf("Plan re-create: got %v, want ErrAlreadyExists", err)
		}
	})
	t.Run("tombstone_not_found", func(t *testing.T) {
		db := freshDB(t)
		if err := session.Tombstone(db, "ghost", now); err != session.ErrNotFound {
			t.Fatalf("live: %v", err)
		}
		if _, err := session.PlanTombstone(db, "ghost", now); err != session.ErrNotFound {
			t.Fatalf("Plan: got %v, want ErrNotFound", err)
		}
	})
	t.Run("proc_node_missing", func(t *testing.T) {
		db := freshDB(t)
		if _, err := session.Create(db, "lab", "lab", "o", "p", now); err != nil {
			t.Fatal(err)
		}
		p := proc.Process{PID: "01x", SID: "lab", NID: "ghost", Argv: []string{"x"}, StartedAt: now, BootID: "b"}
		if err := proc.Insert(db, p); err != proc.ErrNodeMissing {
			t.Fatalf("live: %v", err)
		}
		if _, err := proc.PlanInsert(db, p); err != proc.ErrNodeMissing {
			t.Fatalf("Plan: got %v, want ErrNodeMissing", err)
		}
	})
	t.Run("port_out_of_band_and_exhausted", func(t *testing.T) {
		db := freshDB(t)
		seedSessionNode(t, db, "lab", "lab-1", now)
		if _, _, err := port.PlanAllocate(db, "lab", "lab-1", "n", 1, 70000, "fp", "", false, cfg); err != port.ErrPortOutOfBand {
			t.Fatalf("Plan out-of-band: got %v, want ErrPortOutOfBand", err)
		}
	})
}

// TestReconcileBatch_TotalOrderDeterminism is the §13.2 non-vacuous proof of the
// ReconcileBatch total order: the SAME set of exit marks fed in 4 different input
// orders MUST render a byte-identical Command body (sorted by PID), eliminating
// the Go-map iteration nondeterminism of reconcileOnRegister. It then applies the
// batch and asserts every RUNNING proc transitioned to EXITED.
func TestReconcileBatch_TotalOrderDeterminism(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	a := proc.ExitMark{PID: "01pidaaa0000000000000000aa", ExitCode: 0, When: now}
	b := proc.ExitMark{PID: "01pidbbb0000000000000000bb", ExitCode: 1, When: now}
	c := proc.ExitMark{PID: "01pidccc0000000000000000cc", ExitCode: -1, When: now}
	orders := [][]proc.ExitMark{{a, b, c}, {c, b, a}, {b, a, c}, {c, a, b}}

	var firstSQL []string
	for i, ord := range orders {
		// SID is REQUIRED since external review B-2 (see command_shape_test.go). The
		// determinism property under test is unaffected: the same SID rides every order.
		cmd, err := proc.PlanReconcileBatch(proc.ReconcileBatchInput{SID: "lab", Marks: ord})
		if err != nil {
			t.Fatalf("order %d: %v", i, err)
		}
		sqls := make([]string, len(cmd.Body))
		for j, st := range cmd.Body {
			sqls[j] = st.SQL
		}
		if i == 0 {
			firstSQL = sqls
			continue
		}
		if !reflect.DeepEqual(sqls, firstSQL) {
			t.Fatalf("input order %d produced a DIFFERENT Command body (map nondeterminism leaked):\n got=%v\n want=%v", i, sqls, firstSQL)
		}
	}
	// PID-sorted: a < b < c
	if !strings.Contains(firstSQL[0], "pidaaa") || !strings.Contains(firstSQL[1], "pidbbb") || !strings.Contains(firstSQL[2], "pidccc") {
		t.Fatalf("ReconcileBatch not pid-sorted: %v", firstSQL)
	}

	// apply to a real DB: 3 RUNNING procs -> all EXITED.
	db := freshDB(t)
	seedSessionNode(t, db, "lab", "lab-1", now)
	for _, m := range []proc.ExitMark{a, b, c} {
		p := proc.Process{PID: m.PID, SID: "lab", NID: "lab-1", Argv: []string{"x"}, StartedAt: now, BootID: "boot1"}
		if err := proc.Insert(db, p); err != nil {
			t.Fatal(err)
		}
	}
	// shuffled input. The error is CHECKED: discarding it turned the B-2 refusal into a
	// nil Command and a segfault three lines down, which is a worse way to learn that the
	// plan was rejected than being told so.
	cmd, perr := proc.PlanReconcileBatch(proc.ReconcileBatchInput{SID: "lab", Marks: []proc.ExitMark{c, a, b}})
	if perr != nil {
		t.Fatal(perr)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	var running int
	if err := db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='RUNNING'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if running != 0 {
		t.Fatalf("after ReconcileBatch, %d procs still RUNNING (want 0)", running)
	}
}

// TestSessionHardDelete_Equivalence drives the 7-table cascade delete through both
// paths and asserts the whole session subtree is gone identically.
func TestSessionHardDelete_Equivalence(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	build := func(useFSM bool) *sql.DB {
		db := freshDB(t)
		if _, err := session.Create(db, "lab", "lab", "SHA256:o", "ph", now); err != nil {
			t.Fatal(err)
		}
		if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "lab-1", ProtoVersion: 2}, now); err != nil {
			t.Fatal(err)
		}
		if err := session.Tombstone(db, "lab", now); err != nil {
			t.Fatal(err)
		}
		if useFSM {
			cmd, err := session.PlanHardDelete(db, "lab")
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.ExecCommand(db, cmd); err != nil {
				t.Fatal(err)
			}
		} else {
			// today's dropSessionRows order (internal/broker/audit.go)
			for _, tbl := range []string{"port_allocations", "processes", "proxy_subscribers", "agent_provisioning", "nodes", "members", "sessions"} {
				if _, err := db.Exec(`DELETE FROM `+tbl+` WHERE sid=?`, "lab"); err != nil {
					t.Fatal(err)
				}
			}
		}
		return db
	}
	d, f := build(false), build(true)
	for _, tbl := range []string{"sessions", "members", "nodes"} {
		if logicalHash(t, d, tbl) != logicalHash(t, f, tbl) {
			t.Fatalf("hard-delete diverged on %s", tbl)
		}
		var n int
		_ = f.QueryRow(`SELECT COUNT(*) FROM ` + tbl).Scan(&n)
		if n != 0 {
			t.Fatalf("%s not empty after hard delete: %d rows", tbl, n)
		}
	}
}

// TestPortAllocateFreeReallocate_AutoincrementEquivalence drives the §13.2
// adversarial scenario "allocate -> free -> re-allocate" through both paths and
// asserts logical equivalence INCLUDING row_id ordering — proving AUTOINCREMENT
// assigns the same rowids (no reuse) under the FSM path as under the direct path
// (the leader omits row_id; SQLite assigns it deterministically under serial Apply).
func TestPortAllocateFreeReallocate_AutoincrementEquivalence(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 0, time.UTC)
	cfg := fixedClock(now)

	run := func(direct bool) *sql.DB {
		db := freshDB(t)
		seedSessionNode(t, db, "lab", "lab-1", now)
		apply := func(p func(*sql.DB) (*cluster.Command, error)) {
			cmd, err := p(db)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if err := cluster.ExecCommand(db, cmd); err != nil {
				t.Fatalf("exec: %v", err)
			}
		}
		// alloc A (auto), alloc B (auto), free A, re-alloc C (auto)
		seq := []struct {
			name string
		}{{"a"}, {"b"}}
		for _, s := range seq {
			if direct {
				if _, err := port.Allocate(db, "lab", "lab-1", s.name, 1000, 0, "SHA256:c", false, cfg); err != nil {
					t.Fatalf("direct alloc %s: %v", s.name, err)
				}
			} else {
				apply(func(d *sql.DB) (*cluster.Command, error) {
					_, cmd, err := port.PlanAllocate(d, "lab", "lab-1", s.name, 1000, 0, "SHA256:c", "", false, cfg)
					return cmd, err
				})
			}
		}
		// free "a" (look up its port first)
		var aPort int
		if err := db.QueryRow(`SELECT port FROM port_allocations WHERE name='a' AND state='ALLOCATED'`).Scan(&aPort); err != nil {
			t.Fatal(err)
		}
		if direct {
			if err := port.Free(db, aPort, now); err != nil {
				t.Fatal(err)
			}
			if _, err := port.Allocate(db, "lab", "lab-1", "c", 1000, 0, "SHA256:c", false, cfg); err != nil {
				t.Fatal(err)
			}
		} else {
			apply(func(d *sql.DB) (*cluster.Command, error) { return port.PlanFree(d, aPort, now) })
			apply(func(d *sql.DB) (*cluster.Command, error) {
				_, cmd, err := port.PlanAllocate(d, "lab", "lab-1", "c", 1000, 0, "SHA256:c", "", false, cfg)
				return cmd, err
			})
		}
		return db
	}

	da := run(true)
	fa := run(false)
	// include row_id (default, not excluded) + everything except the random token_hash
	hd := logicalHash(t, da, "port_allocations", "token_hash")
	hf := logicalHash(t, fa, "port_allocations", "token_hash")
	if hd != hf {
		t.Fatalf("allocate/free/re-allocate divergence (AUTOINCREMENT?):\n direct=%s\n fsm   =%s", hd, hf)
	}
	// sanity: 3 rows, rowids 1,2,3 (no reuse), 'a' FREED.
	var ids []int
	rows, _ := da.Query(`SELECT row_id FROM port_allocations ORDER BY row_id`)
	for rows.Next() {
		var id int
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	_ = rows.Close()
	sort.Ints(ids)
	if fmt.Sprint(ids) != "[1 2 3]" {
		t.Fatalf("expected rowids [1 2 3] (no reuse), got %v", ids)
	}
}
