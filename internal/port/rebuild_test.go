package port

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"
	"time"
)

// B4 port-layer tests: --no-rebuild threads rebuild_on_failure through Allocate (live) and
// PlanAllocate (baked), the default path stays byte-identical to pre-B4, and the shared
// home-scanners map home/epoch/rebuild to the right struct fields.

// b4Cfg returns a tiny-band Config with an injectable Now (for the DIFF-1 timezone test).
func b4Cfg(now time.Time) *Config {
	return &Config{BandLow: 14000, BandHigh: 14009, Now: func() time.Time { return now }}
}

func queryRebuild(t *testing.T, db *sql.DB, port int) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`SELECT rebuild_on_failure FROM port_allocations WHERE port=?`, port).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// origin: b4_rebuild_test.go (renamed in B6) — docs/reviews/b4-plan.md, docs/reviews/b4-review.md
//
// TestB4AllocateRebuildColumn (plan §F.6): Allocate(rebuildOff=false) writes rebuild_on_failure=1
// (the migration-0010 DEFAULT — byte-identical to a pre-B4 allocate); rebuildOff=true writes 0 and
// sets Allocation.RebuildOff.
func TestB4AllocateRebuildColumn(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	on, err := Allocate(db, "lab", "lab-1", "on", 8888, 0, "SHA256:fp", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if on.RebuildOff {
		t.Error("default Allocate must leave RebuildOff=false (rebuild ON)")
	}
	if got := queryRebuild(t, db, on.Port); got != 1 {
		t.Errorf("default rebuild_on_failure = %d, want 1", got)
	}

	off, err := Allocate(db, "lab", "lab-1", "off", 8889, 0, "SHA256:fp", true, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if !off.RebuildOff {
		t.Error("Allocate(rebuildOff=true) must set RebuildOff=true")
	}
	if got := queryRebuild(t, db, off.Port); got != 0 {
		t.Errorf("--no-rebuild rebuild_on_failure = %d, want 0", got)
	}
}

// TestB4PlanAllocateConditionalBake (plan §F.7): the baked INSERT names rebuild_on_failure ONLY
// when rebuildOff is set, across both home-states (the 2×2). The default path's column list is
// byte-identical to pre-B4.
func TestB4PlanAllocateConditionalBake(t *testing.T) {
	noRebuildCols := "(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)"
	rebuildCols := "(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, rebuild_on_failure)"
	homeCols := "(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch)"
	homeRebuildCols := "(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)"

	cases := []struct {
		name           string
		home           string
		rebuildOff     bool
		wantCols       string
		wantRebuildLit bool
	}{
		{"default", "", false, noRebuildCols, false},
		{"norebuild", "", true, rebuildCols, true},
		{"home", "brk-b", false, homeCols, false},
		{"home+norebuild", "brk-b", true, homeRebuildCols, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openDB(t)
			seedSessionAndNode(t, db, "lab", "lab-1")
			_, cmd, err := PlanAllocate(db, "lab", "lab-1", "j", 8888, 0, "SHA256:fp", c.home, c.rebuildOff, tinyBand())
			if err != nil {
				t.Fatal(err)
			}
			sql := cmd.Body[0].SQL
			if !strings.Contains(sql, c.wantCols) {
				t.Fatalf("baked column list wrong\n got: %s\nwant substring: %s", sql, c.wantCols)
			}
			hasRebuildCol := strings.Contains(sql, "rebuild_on_failure")
			if hasRebuildCol != c.wantRebuildLit {
				t.Fatalf("rebuild_on_failure present=%v, want %v: %s", hasRebuildCol, c.wantRebuildLit, sql)
			}
			if c.wantRebuildLit && !strings.HasSuffix(sql, ", 0)") {
				t.Fatalf("--no-rebuild bake must end with the rebuild_on_failure=0 literal: %s", sql)
			}
		})
	}
}

// TestB4PlanAllocateDIFF1 (plan §F.7): --on-broker + --no-rebuild bakes byte-identical SQL across
// a UTC and a non-UTC Now of the SAME instant (the DIFF-1 property; the rebuild_on_failure literal
// is a constant 0 and created_at is UTC-normalized). The only varying part is the random token,
// which is masked.
func TestB4PlanAllocateDIFF1(t *testing.T) {
	instant := time.Date(2026, 6, 24, 17, 0, 0, 0, time.UTC)
	cdt := time.FixedZone("CDT", -5*3600)

	db1 := openDB(t)
	seedSessionAndNode(t, db1, "lab", "lab-1")
	_, cmd1, err := PlanAllocate(db1, "lab", "lab-1", "j", 8888, 0, "SHA256:fp", "brk-b", true, b4Cfg(instant.In(time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	db2 := openDB(t)
	seedSessionAndNode(t, db2, "lab", "lab-1")
	_, cmd2, err := PlanAllocate(db2, "lab", "lab-1", "j", 8888, 0, "SHA256:fp", "brk-b", true, b4Cfg(instant.In(cdt)))
	if err != nil {
		t.Fatal(err)
	}
	tok := regexp.MustCompile(`'[0-9a-f]{64}'`)
	got1 := tok.ReplaceAllString(cmd1.Body[0].SQL, "'TOKEN'")
	got2 := tok.ReplaceAllString(cmd2.Body[0].SQL, "'TOKEN'")
	if got1 != got2 {
		t.Fatalf("DIFF-1 violated: UTC vs non-UTC bake differ\n utc: %s\n cdt: %s", got1, got2)
	}
}

// TestB4ScannerColumnOrderPin (plan §F.8): the #1 mechanical landmine. Insert a row with DISTINCT
// home_broker / epoch / rebuild_on_failure values, then read it back through EVERY home-scanner
// caller and assert each value lands in the right field — a SELECT/Scan misalignment would swap
// them.
func TestB4ScannerColumnOrderPin(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	// Distinct, non-default, non-confusable values.
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)
		 VALUES (14000,'lab','lab-1','svc',8888,'th','ALLOCATED','SHA256:fp',?, 'brk-XYZ', 7, 0)`,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	check := func(label string, a *Allocation) {
		if a.HomeBroker != "brk-XYZ" {
			t.Errorf("%s: HomeBroker = %q, want brk-XYZ (column misalignment?)", label, a.HomeBroker)
		}
		if a.Epoch != 7 {
			t.Errorf("%s: Epoch = %d, want 7", label, a.Epoch)
		}
		if !a.RebuildOff {
			t.Errorf("%s: RebuildOff = false, want true (rebuild_on_failure=0)", label)
		}
	}
	byName, err := LookupByName(db, "lab", "svc")
	if err != nil {
		t.Fatal(err)
	}
	check("LookupByName", byName)

	byTok, err := LookupByTokenHash(db, "th")
	if err != nil {
		t.Fatal(err)
	}
	check("LookupByTokenHash", byTok)

	list, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("ListBySession len = %d, want 1", len(list))
	}
	check("ListBySession", &list[0])
}

// TestB4OfflineReconcilerScannerColumnOrder (test-adversary gap, complements plan §F.8): the
// §F.8 column-order pin covers LookupByName/LookupByTokenHash/ListBySession but NOT
// ListAllocatedForOfflineNodes — the offline-reconciler path uses a DIFFERENT, table-qualified
// SELECT (pa.* + JOIN nodes) feeding the same scanRowWithHome. A SELECT/Scan misalignment there
// (e.g. pa.epoch and pa.rebuild_on_failure swapped) would silently revoke ports with the wrong
// home identity. Assert each distinct value lands in the right field through THAT caller.
func TestB4OfflineReconcilerScannerColumnOrder(t *testing.T) {
	db := openDB(t)
	// Seed an OFFLINE node whose last heartbeat is well past any threshold.
	if _, err := db.Exec(`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:owner','h')`); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-24 * time.Hour)
	if _, err := db.Exec(`INSERT INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('lab','lab-1',?, 'OFFLINE')`, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)
		 VALUES (14000,'lab','lab-1','svc',8888,'th','ALLOCATED','SHA256:fp',?, 'brk-XYZ', 7, 0)`,
		old,
	); err != nil {
		t.Fatal(err)
	}
	got, err := ListAllocatedForOfflineNodes(db, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("ListAllocatedForOfflineNodes len = %d, want 1", len(got))
	}
	a := got[0]
	if a.HomeBroker != "brk-XYZ" || a.Epoch != 7 || !a.RebuildOff {
		t.Errorf("offline-scanner column misalignment: HomeBroker=%q Epoch=%d RebuildOff=%v, want brk-XYZ/7/true",
			a.HomeBroker, a.Epoch, a.RebuildOff)
	}
}

// TestB4AllocateDefaultInsertByteIdentical (test-adversary, complements plan §F.6/§F.7): the
// PlanAllocate golden proves the BAKED default INSERT omits rebuild_on_failure, but the live
// direct mutator Allocate uses a SEPARATE hand-written INSERT (the if/else in port.go). Prove the
// default (rebuildOff=false) live row reads back IDENTICALLY to a row inserted by a pre-B4-shaped
// INSERT that never names rebuild_on_failure — i.e. both take the migration-0010 DEFAULT 1.
func TestB4AllocateDefaultInsertByteIdentical(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	// Live default allocate.
	a, err := Allocate(db, "lab", "lab-1", "live", 8888, 0, "SHA256:fp", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	// A pre-B4-shaped INSERT (column list without rebuild_on_failure).
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (14001,'lab','lab-1','legacy',8889,'th2','ALLOCATED','SHA256:fp',?)`,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	var liveRebuild, legacyRebuild, liveHome, legacyHome int
	if err := db.QueryRow(`SELECT rebuild_on_failure FROM port_allocations WHERE port=?`, a.Port).Scan(&liveRebuild); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT rebuild_on_failure FROM port_allocations WHERE port=14001`).Scan(&legacyRebuild); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COALESCE(LENGTH(home_broker),0) FROM port_allocations WHERE port=?`, a.Port).Scan(&liveHome); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COALESCE(LENGTH(home_broker),0) FROM port_allocations WHERE port=14001`).Scan(&legacyHome); err != nil {
		t.Fatal(err)
	}
	if liveRebuild != 1 || legacyRebuild != 1 {
		t.Errorf("default live=%d legacy=%d, both want 1 (the migration-0010 DEFAULT — byte-identical)", liveRebuild, legacyRebuild)
	}
	if liveHome != 0 || legacyHome != 0 {
		t.Errorf("default rows must leave home_broker empty: live=%d legacy=%d", liveHome, legacyHome)
	}
}

// TestB4ListBySessionPopulatesHomeFields (plan §F.9): ListBySession (the ps read path) carries
// home/epoch/rebuild — homed AND single-mode rows.
func TestB4ListBySessionPopulatesHomeFields(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	// single-mode row (defaults) + a homed --no-rebuild row.
	if _, err := Allocate(db, "lab", "lab-1", "plain", 8888, 0, "SHA256:fp", false, tinyBand()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)
		 VALUES (14001,'lab','lab-1','homed',9000,'th2','ALLOCATED','SHA256:fp',?, 'brk-b', 2, 0)`,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	list, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Allocation{}
	for _, a := range list {
		got[a.Name] = a
	}
	if a := got["plain"]; a.HomeBroker != "" || a.Epoch != 0 || a.RebuildOff {
		t.Errorf("single-mode row should be un-homed/rebuild-ON: %+v", a)
	}
	if a := got["homed"]; a.HomeBroker != "brk-b" || a.Epoch != 2 || !a.RebuildOff {
		t.Errorf("homed --no-rebuild row wrong: %+v", a)
	}
}
