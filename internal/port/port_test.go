package port

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// openDB delegates to internal/testharness (B9) — see the note in internal/session/session_test.go.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return testharness.OpenDB(t)
}

func seedSessionAndNode(t *testing.T, db *sql.DB, sid, nid string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?, ?, ?, ?)`,
		sid, sid, "SHA256:owner", "test-hash",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO nodes(sid, nid, last_heartbeat_at, status) VALUES (?, ?, ?, ?)`,
		sid, nid, time.Now().UTC(), "ONLINE",
	); err != nil {
		t.Fatal(err)
	}
}

func tinyBand() *Config {
	return &Config{BandLow: 14000, BandHigh: 14002}
}

func TestAllocateAssignsLowestFreePort(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:alice", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if a.Port != 14000 {
		t.Errorf("first allocation: got port %d want 14000", a.Port)
	}
	if a.Token == "" {
		t.Error("raw token must be returned exactly once on Allocate")
	}
	if a.TokenHash != HashToken(a.Token) {
		t.Errorf("token hash inconsistent: got %s want %s", a.TokenHash, HashToken(a.Token))
	}
	if a.State != StateAllocated {
		t.Errorf("state: got %s want ALLOCATED", a.State)
	}
}

func TestAllocateRejectsDuplicateName(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	if _, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:alice", false, tinyBand()); err != nil {
		t.Fatal(err)
	}
	_, err := Allocate(db, "lab", "lab-1", "jupyter", 9999, 0, "SHA256:alice", false, tinyBand())
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("want ErrNameTaken, got %v", err)
	}
}

func TestAllocateExhaustsBand(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	for i := 0; i < 3; i++ {
		if _, err := Allocate(db, "lab", "lab-1",
			"name-"+string(rune('a'+i)), 8000+i, 0, "SHA256:alice", false, tinyBand()); err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
	}
	_, err := Allocate(db, "lab", "lab-1", "extra", 9000, 0, "SHA256:alice", false, tinyBand())
	if !errors.Is(err, ErrPortExhausted) {
		t.Fatalf("want ErrPortExhausted, got %v", err)
	}
}

func TestFreeReturnsPortToPool(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, _ := Allocate(db, "lab", "lab-1", "first", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Free(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	// Re-allocate with a different name; must immediately reuse the same port.
	b, err := Allocate(db, "lab", "lab-1", "second", 9999, 0, "SHA256:alice", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if b.Port != a.Port {
		t.Errorf("re-allocation should reuse freed port: got %d want %d", b.Port, a.Port)
	}
}

func TestFreeAllocationFencesReusedPort(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	first, _ := Allocate(db, "lab", "lab-1", "first", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Free(db, first.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := Allocate(db, "lab", "lab-1", "second", 9999, 0, "SHA256:alice", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if second.Port != first.Port {
		t.Fatalf("test setup expected port reuse, got first=%d second=%d", first.Port, second.Port)
	}
	if err := FreeAllocation(db, *first, time.Now().UTC()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale allocation free err = %v, want ErrNotFound", err)
	}
	got, err := LookupByName(db, "lab", "second")
	if err != nil {
		t.Fatalf("current allocation was freed by stale request: %v", err)
	}
	if got.TokenHash != second.TokenHash {
		t.Fatalf("current allocation changed: got hash %q want %q", got.TokenHash, second.TokenHash)
	}
	if err := FreeAllocation(db, *second, time.Now().UTC()); err != nil {
		t.Fatalf("current allocation free: %v", err)
	}
}

func TestRevokeAllocationFencesReusedPort(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	first, _ := Allocate(db, "lab", "lab-1", "first", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Revoke(db, first.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := Allocate(db, "lab", "lab-1", "second", 9999, 0, "SHA256:alice", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if second.Port != first.Port {
		t.Fatalf("test setup expected port reuse, got first=%d second=%d", first.Port, second.Port)
	}
	if err := RevokeAllocation(db, *first, time.Now().UTC()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale allocation revoke err = %v, want ErrNotFound", err)
	}
	got, err := LookupByName(db, "lab", "second")
	if err != nil {
		t.Fatalf("current allocation was revoked by stale request: %v", err)
	}
	if got.TokenHash != second.TokenHash {
		t.Fatalf("current allocation changed: got hash %q want %q", got.TokenHash, second.TokenHash)
	}
	if err := RevokeAllocation(db, *second, time.Now().UTC()); err != nil {
		t.Fatalf("current allocation revoke: %v", err)
	}
}

func TestRevokeReturnsPortToPool(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, _ := Allocate(db, "lab", "lab-1", "first", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Revoke(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	b, err := Allocate(db, "lab", "lab-1", "second", 9999, 0, "SHA256:alice", false, tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if b.Port != a.Port {
		t.Errorf("after revoke port should be reusable: got %d want %d", b.Port, a.Port)
	}

	// Original row must still exist with state=REVOKED for audit.
	got, err := db.Query(`SELECT state FROM port_allocations WHERE port=? AND name='first'`, a.Port)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = got.Close() }()
	if !got.Next() {
		t.Fatal("revoked row missing")
	}
	var st string
	_ = got.Scan(&st)
	if st != "REVOKED" {
		t.Errorf("revoked row state: got %s want REVOKED", st)
	}
}

func TestLookupByNameSelectsAllocatedOnly(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, _ := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Free(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Same name re-allocated; LookupByName must return the new ALLOCATED row,
	// not the FREED one.
	b, _ := Allocate(db, "lab", "lab-1", "jupyter", 9999, 0, "SHA256:alice", false, tinyBand())

	got, err := LookupByName(db, "lab", "jupyter")
	if err != nil {
		t.Fatal(err)
	}
	if got.LocalPort != b.LocalPort {
		t.Errorf("want new ALLOCATED row local_port=%d, got %d", b.LocalPort, got.LocalPort)
	}
}

func TestLookupByTokenHashRejectsNonAllocated(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	a, _ := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:alice", false, tinyBand())
	if err := Revoke(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_, err := LookupByTokenHash(db, a.TokenHash)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("revoked token must not lookup: got %v", err)
	}
}

func TestListAllocatedForOfflineNodes(t *testing.T) {
	db := openDB(t)
	// Two nodes, one OFFLINE for 20min, one OFFLINE for 1min, one ONLINE.
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:owner','h')`,
	); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	insertNode := func(nid, status string, hbAge time.Duration) {
		t.Helper()
		_, err := db.Exec(
			`INSERT INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('lab', ?, ?, ?)`,
			nid, now.Add(-hbAge), status,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	insertNode("old", "OFFLINE", 20*time.Minute)
	insertNode("recent", "OFFLINE", 1*time.Minute)
	insertNode("online", "ONLINE", 0)

	cfg := tinyBand()
	cfg.Now = func() time.Time { return now }
	for _, nid := range []string{"old", "recent", "online"} {
		if _, err := Allocate(db, "lab", nid, "p-"+nid, 8000, 0, "SHA256:alice", false, cfg); err != nil {
			t.Fatal(err)
		}
	}

	got, err := ListAllocatedForOfflineNodes(db, now, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 row for old offline node, got %d: %+v", len(got), got)
	}
	if got[0].NID != "old" {
		t.Errorf("wrong node returned: %+v", got[0])
	}
}

// TestAllocateDesiredPort exercises the P12 desired-port (--remote-port)
// path: out-of-band reject, taken hard-fail, REVOKED/FREED-don't-block,
// name-uniqueness-first, and the desired==0 auto fallback. Band is the
// tiny 14000-14002 so out-of-band boundaries are 13999 / 14003 and a
// non-lowest in-band request (14002 while 14000 is free) proves the
// port was honored rather than auto-picked.
func TestAllocateDesiredPort(t *testing.T) {
	const (
		sid = "lab"
		nid = "lab-1"
		fp  = "SHA256:alice"
	)
	mustAlloc := func(t *testing.T, db *sql.DB, name string, local, desired int) *Allocation {
		t.Helper()
		a, err := Allocate(db, sid, nid, name, local, desired, fp, false, tinyBand())
		if err != nil {
			t.Fatalf("setup Allocate(%q, false, desired=%d): %v", name, desired, err)
		}
		return a
	}

	cases := []struct {
		name     string
		setup    func(t *testing.T, db *sql.DB)
		reqName  string
		desired  int
		wantPort int   // asserted when wantErr == nil
		wantErr  error // asserted via errors.Is when non-nil
	}{
		{
			name:     "free_port_granted_not_lowest",
			reqName:  "req",
			desired:  14002,
			wantPort: 14002,
		},
		{
			name:    "allocated_port_taken",
			setup:   func(t *testing.T, db *sql.DB) { mustAlloc(t, db, "occupant", 8000, 0) }, // grabs 14000
			reqName: "req",
			desired: 14000,
			wantErr: ErrPortTaken,
		},
		{
			name:    "below_band_out_of_band",
			reqName: "req",
			desired: 13999,
			wantErr: ErrPortOutOfBand,
		},
		{
			name:    "above_band_out_of_band",
			reqName: "req",
			desired: 14003,
			wantErr: ErrPortOutOfBand,
		},
		{
			name: "revoked_only_granted",
			setup: func(t *testing.T, db *sql.DB) {
				a := mustAlloc(t, db, "old", 8000, 14001)
				if err := Revoke(db, a.Port, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			},
			reqName:  "req",
			desired:  14001,
			wantPort: 14001,
		},
		{
			name: "freed_only_granted",
			setup: func(t *testing.T, db *sql.DB) {
				a := mustAlloc(t, db, "old", 8000, 14001)
				if err := Free(db, a.Port, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			},
			reqName:  "req",
			desired:  14001,
			wantPort: 14001,
		},
		{
			name: "revoked_and_freed_mixed_granted",
			setup: func(t *testing.T, db *sql.DB) {
				a1 := mustAlloc(t, db, "h1", 8000, 14001)
				if err := Free(db, a1.Port, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				a2 := mustAlloc(t, db, "h2", 8001, 14001)
				if err := Revoke(db, a2.Port, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			},
			reqName:  "req",
			desired:  14001,
			wantPort: 14001,
		},
		{
			name:     "zero_is_auto_lowest_free",
			setup:    func(t *testing.T, db *sql.DB) { mustAlloc(t, db, "occupant", 8000, 0) }, // grabs 14000
			reqName:  "req",
			desired:  0,
			wantPort: 14001,
		},
		{
			// dup name + a desired port that is itself TAKEN: name check
			// must still win, proving name_taken short-circuits ahead of
			// the ErrPortTaken branch (D-6 ordering).
			name:    "name_uniqueness_beats_port_taken",
			setup:   func(t *testing.T, db *sql.DB) { mustAlloc(t, db, "dup", 8000, 0) }, // grabs 14000 under "dup"
			reqName: "dup",
			desired: 14000, // 14000 is itself ALLOCATED, yet name check wins
			wantErr: ErrNameTaken,
		},
		{
			// dup name + an OUT-OF-BAND desired port: name check must still
			// win, proving name_taken short-circuits ahead of the
			// ErrPortOutOfBand branch (D-6 ordering).
			name:    "name_uniqueness_beats_port_out_of_band",
			setup:   func(t *testing.T, db *sql.DB) { mustAlloc(t, db, "dup", 8000, 0) },
			reqName: "dup",
			desired: 13999, // out of band, yet name check wins
			wantErr: ErrNameTaken,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openDB(t)
			seedSessionAndNode(t, db, sid, nid)
			if tc.setup != nil {
				tc.setup(t, db)
			}
			got, err := Allocate(db, sid, nid, tc.reqName, 9000, tc.desired, fp, false, tinyBand())
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want err %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got.Port != tc.wantPort {
				t.Errorf("port: got %d want %d", got.Port, tc.wantPort)
			}
		})
	}
}

// TestAllocateDesiredPortTakenIsHardFailNoFallback pins the locked
// decision that a taken --remote-port is a HARD failure: the second
// request must NOT silently fall back to a different free port.
func TestAllocateDesiredPortTakenIsHardFailNoFallback(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	// Occupy 14000 (auto). The band still has 14001/14002 free, so a
	// fallback (if it wrongly existed) would have somewhere to go.
	if _, err := Allocate(db, "lab", "lab-1", "occupant", 8000, 0, "SHA256:alice", false, tinyBand()); err != nil {
		t.Fatal(err)
	}
	a, err := Allocate(db, "lab", "lab-1", "req", 9000, 14000, "SHA256:alice", false, tinyBand())
	if !errors.Is(err, ErrPortTaken) {
		t.Fatalf("want ErrPortTaken, got alloc=%+v err=%v", a, err)
	}
	if a != nil {
		t.Errorf("rejected Allocate must return nil allocation, got %+v", a)
	}
	// Exactly one ALLOCATED row total — no fallback row was created.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE state='ALLOCATED'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("ALLOCATED rows after taken-reject: got %d want 1 (no fallback)", n)
	}
}

// TestIsUniqueViolation pins the constraint-detection contract the
// desired-port taken path relies on: a real SQLite UNIQUE violation
// (here on the idx_port_alloc_unique_active partial index) must be
// recognized, while nil / unrelated errors must not be. If a driver
// upgrade changes the message text, this fails loudly rather than
// silently turning port_taken into a generic alloc_failed.
func TestIsUniqueViolation(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	if _, err := Allocate(db, "lab", "lab-1", "first", 8000, 14000, "SHA256:alice", false, tinyBand()); err != nil {
		t.Fatal(err)
	}
	// Raw INSERT of a second ALLOCATED row on the same port trips the
	// partial unique index — the exact failure Allocate translates.
	_, err := db.Exec(
		`INSERT INTO port_allocations
		   (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (14000, 'lab', 'lab-1', 'collide', 9000, 'deadbeef', 'ALLOCATED', 'SHA256:bob', ?)`,
		time.Now().UTC(),
	)
	if err == nil {
		t.Fatal("expected a UNIQUE constraint violation on duplicate ALLOCATED port")
	}
	if !isUniqueViolation(err) {
		t.Errorf("isUniqueViolation should recognize %q", err.Error())
	}
	if isUniqueViolation(nil) {
		t.Error("isUniqueViolation(nil) must be false")
	}
	if isUniqueViolation(errors.New("some other error")) {
		t.Error("isUniqueViolation must not match unrelated errors")
	}
}

// TestTranslateInsertErr pins the D-2 gate: the UNIQUE->ErrPortTaken
// translation fires ONLY on the desired-port path. On the auto path
// (desiredPort==0) a UNIQUE violation is impossible-by-construction and
// MUST surface loud as a wrapped "port: insert" error, never as
// ErrPortTaken — so a future edit that drops the `desiredPort != 0`
// gate fails here. (A real auto-path UNIQUE collision can't be staged
// deterministically through Allocate, since findFreePort proves the
// port free in the same tx; testing the pure mapping is the guard.)
func TestTranslateInsertErr(t *testing.T) {
	uniqueErr := errors.New("constraint failed: UNIQUE constraint failed: port_allocations.port")
	otherErr := errors.New("disk full")

	// Desired-port path + UNIQUE -> ErrPortTaken.
	if got := translateInsertErr(uniqueErr, 14001); !errors.Is(got, ErrPortTaken) {
		t.Errorf("desired-port + UNIQUE: want ErrPortTaken, got %v", got)
	}

	// Auto path + UNIQUE -> must NOT be ErrPortTaken; must wrap loud.
	got := translateInsertErr(uniqueErr, 0)
	if errors.Is(got, ErrPortTaken) {
		t.Error("auto path + UNIQUE must NOT be relabeled ErrPortTaken (D-2 guard)")
	}
	if !strings.Contains(got.Error(), "port: insert") {
		t.Errorf("auto path: want wrapped 'port: insert', got %v", got)
	}

	// Non-UNIQUE error -> always wrapped, regardless of desiredPort.
	for _, dp := range []int{0, 14001} {
		g := translateInsertErr(otherErr, dp)
		if errors.Is(g, ErrPortTaken) {
			t.Errorf("non-UNIQUE err (desired=%d) must not be ErrPortTaken", dp)
		}
		if !strings.Contains(g.Error(), "port: insert") {
			t.Errorf("non-UNIQUE err (desired=%d) must wrap 'port: insert', got %v", dp, g)
		}
	}
}

func TestSessionDeleteCascadesPortRows(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	if _, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:alice", false, tinyBand()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	rows, err := ListBySession(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("session delete should cascade to port_allocations: got %d rows", len(rows))
	}
}

// ---- h1 A1: bounded, live-first ps listing ----------------------------------
// origin: docs/reviews/h1-plan.md workstream A1 (2026-08-04 `tether ps`
// max_payload incident — 24k FREED rows in one session).

// seedRawAllocation inserts a port_allocations row directly so tests can build
// histories that Allocate's live-port uniqueness would refuse (thousands of
// FREED rows on recycled ports, controlled created_at ordering).
func seedRawAllocation(t *testing.T, db *sql.DB, sid, nid string, portN int, name, state string, createdAt time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		 VALUES (?, ?, ?, ?, 0, ?, ?, 'SHA256:test', ?)`,
		portN, sid, nid, name, name+"-hash", state, createdAt.UTC(),
	); err != nil {
		t.Fatal(err)
	}
}

func TestListBySessionFilteredLiveOnlyExcludesHistory(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "s1", "n1")
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	seedRawAllocation(t, db, "s1", "n1", 14000, "live-a", "ALLOCATED", base)
	seedRawAllocation(t, db, "s1", "n1", 14001, "live-b", "ALLOCATED", base.Add(time.Hour))
	seedRawAllocation(t, db, "s1", "n1", 14002, "dead-1", "FREED", base.Add(2*time.Hour))
	seedRawAllocation(t, db, "s1", "n1", 14003, "dead-2", "FREED", base.Add(3*time.Hour))
	seedRawAllocation(t, db, "s1", "n1", 14004, "dead-3", "REVOKED", base.Add(4*time.Hour))

	got, err := ListBySessionFiltered(db, "s1", ListBySessionOpts{IncludeFreed: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("live-only view returned %d rows, want 2", len(got))
	}
	if got[0].Port != 14000 || got[1].Port != 14001 {
		t.Fatalf("live-only view must keep port-ASC order, got %d,%d", got[0].Port, got[1].Port)
	}
	n, err := CountBySession(db, "s1", false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CountBySession(live)=%d, want 2", n)
	}
}

// TestListBySessionFilteredLiveRowsSurviveLimit is the critique-4 adversarial
// fixture: live ALLOCATED rows are OLDER than a flood of newer FREED and
// REVOKED history that alone exceeds the limit. A recency-only sort would
// evict every live row from a truncated `-a` view; the live-first sort key
// `(state='ALLOCATED') DESC` must keep them all, in first positions.
func TestListBySessionFilteredLiveRowsSurviveLimit(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "s1", "n1")
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	// 3 live rows, OLDEST timestamps in the whole table.
	for i, name := range []string{"live-a", "live-b", "live-c"} {
		seedRawAllocation(t, db, "s1", "n1", 14000+i, name, "ALLOCATED", base.Add(time.Duration(i)*time.Minute))
	}
	// 600 FREED newer + 550 REVOKED newest — the incident shape (reaper churn).
	for i := 0; i < 600; i++ {
		seedRawAllocation(t, db, "s1", "n1", 14005, "freed", "FREED", base.AddDate(0, 0, 1).Add(time.Duration(i)*time.Second))
	}
	for i := 0; i < 550; i++ {
		seedRawAllocation(t, db, "s1", "n1", 14006, "revoked", "REVOKED", base.AddDate(0, 0, 2).Add(time.Duration(i)*time.Second))
	}

	const limit = 500
	got, err := ListBySessionFiltered(db, "s1", ListBySessionOpts{IncludeFreed: true, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != limit {
		t.Fatalf("limited view returned %d rows, want %d", len(got), limit)
	}
	liveSeen := map[string]bool{}
	for i, a := range got[:3] {
		if a.State != StateAllocated {
			t.Fatalf("row %d is %s %q — live rows must sort strictly first", i, a.State, a.Name)
		}
		liveSeen[a.Name] = true
	}
	if len(liveSeen) != 3 {
		t.Fatalf("truncated view lost a live allocation: %v (1150 newer dead rows evicted it)", liveSeen)
	}
	// History fills the remainder newest-first.
	if got[3].State == StateAllocated {
		t.Fatalf("only 3 live rows were seeded; row 3 must be history, got ALLOCATED")
	}
	total, err := CountBySession(db, "s1", true)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3+600+550 {
		t.Fatalf("CountBySession(all)=%d, want %d", total, 3+600+550)
	}
}

// ---------------------------------------------------------------------------------------------
// Property test: the allocator under seeded random operation sequences.
//
// Every table test above pins one path. The allocator's invariants are about SEQUENCES — a port
// freed and re-taken, a name reused after revoke, GC racing a fresh allocation — and the tables cover
// the sequences somebody thought of. A seeded generator writes the rest; the invariants say what must
// hold after every step. The band is eight ports wide so exhaustion, reuse and GC all happen
// constantly instead of never. origin: docs/reviews/test-system-overhaul-plan.md B5 (distributed D6).
//
// Invariants (checked after every operation against the model the test keeps alongside the DB):
//   I1  every ALLOCATED row has a distinct port inside the band;
//   I2  (sid, name) is unique among ALLOCATED rows;
//   I3  a successful auto allocation returns the LOWEST port in the band not ALLOCATED beforehand,
//       and fails with ErrPortExhausted exactly when none is free;
//   I4  after GCTerminated(cutoff): no FREED/REVOKED row whose end of life is before cutoff remains,
//       every one at or after cutoff remains, and ALLOCATED rows are untouched.

// portModelRow is one allocation as the model tracks it — keyed by TOKEN HASH, the identity that
// survives port reuse. port_allocations keeps history rows (migration 0003): one port may hold a
// FREED row, a REVOKED row and one ALLOCATED row at the same time, and ListBySession returns them
// all. A model keyed by port — the first version — saw only the newest row per port and could not
// notice a GC that spared, or wrongly deleted, a shadowed one; nor could its one-way "model ⊆ DB"
// check see a Free that returned nil and changed nothing (internal review L2-F2 / L2-F10). The
// comparison is now an exact two-way multiset over token hashes.
type portModelRow struct {
	port  int
	name  string
	state State
	eol   time.Time // revoked_at for FREED/REVOKED
}

func TestAllocationInvariantsUnderRandomOperations(t *testing.T) {
	const sequences, steps = 120, 40 // plan wrote 200×40; 120 keeps the package under 6s and reaches every path (see hits)
	const bandLow, bandHigh = 14000, 14007
	// More names than ports (10 vs 8), or the band can never fill and I3's exhaustion arm is never
	// exercised — the generator self-check caught exactly that on its first run.
	names := []string{"jupyter", "tb", "ssh", "vnc", "api", "grafana", "mlflow", "ray", "code", "db"}
	// hits is the G2 self-check, counted by the REAL walk — not by a shadow replay of the RNG that
	// would silently fall out of step the day someone adds an Intn (internal review L2-F7). Every
	// path the invariants are about must be reached, or they were checked against sequences that
	// never exercised them. Subtests run sequentially, so the map needs no lock.
	hits := map[string]int{}
	for seed := int64(1); seed <= sequences; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			db := openDB(t)
			seedSessionAndNode(t, db, "lab", "lab-1")
			r := rand.New(rand.NewSource(seed))
			clock := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			cfg := &Config{BandLow: bandLow, BandHigh: bandHigh, Now: func() time.Time { return clock }}
			model := map[string]portModelRow{} // token hash -> row
			liveOnPort := func(p int) (string, bool) {
				for th, row := range model {
					if row.state == StateAllocated && row.port == p {
						return th, true
					}
				}
				return "", false
			}
			anyRowOnPort := func(p int) bool {
				for _, row := range model {
					if row.port == p {
						return true
					}
				}
				return false
			}
			allocatedName := func(name string) bool {
				for _, row := range model {
					if row.state == StateAllocated && row.name == name {
						return true
					}
				}
				return false
			}
			lowestFree := func() int {
				for p := bandLow; p <= bandHigh; p++ {
					if _, live := liveOnPort(p); !live {
						return p
					}
				}
				return 0
			}
			// compare is the exact two-way check: every model row is in the DB with the same port and
			// state, every DB row is known to the model, and the DB's ALLOCATED rows satisfy I1/I2.
			compare := func(step int, what string) {
				t.Helper()
				rows, err := ListBySession(db, "lab")
				if err != nil {
					t.Fatal(err)
				}
				got := map[string]Allocation{}
				seenPort := map[int]bool{}
				seenName := map[string]bool{}
				for _, a := range rows {
					if _, dup := got[a.TokenHash]; dup {
						t.Fatalf("step %d %s: token hash %s appears twice", step, what, a.TokenHash)
					}
					got[a.TokenHash] = a
					if a.State != StateAllocated {
						continue
					}
					if a.Port < bandLow || a.Port > bandHigh || seenPort[a.Port] {
						t.Fatalf("step %d %s: I1 violated: ALLOCATED port %d (band %d-%d, dup=%v)", step, what, a.Port, bandLow, bandHigh, seenPort[a.Port])
					}
					seenPort[a.Port] = true
					if seenName[a.Name] {
						t.Fatalf("step %d %s: I2 violated: name %q ALLOCATED twice", step, what, a.Name)
					}
					seenName[a.Name] = true
				}
				for th, row := range model {
					a, ok := got[th]
					if !ok || a.State != row.state || a.Port != row.port || a.Name != row.name {
						t.Fatalf("step %d %s: model row %s (port %d %s %q) vs DB (present=%v port=%d state=%q name=%q)", step, what, th, row.port, row.state, row.name, ok, a.Port, a.State, a.Name)
					}
					// End of life is part of "exact": a terminated row carries the clock at which it
					// was freed/revoked, and GC's cutoff is judged against it — a wrong revoked_at
					// would only show up indirectly through a later GC (external review suggestion 5).
					switch {
					case row.state == StateAllocated && a.RevokedAt != nil:
						t.Fatalf("step %d %s: ALLOCATED row %s carries revoked_at %v", step, what, th, *a.RevokedAt)
					case row.state != StateAllocated && (a.RevokedAt == nil || !a.RevokedAt.Equal(row.eol)):
						t.Fatalf("step %d %s: %s row %s revoked_at=%v, model end of life %v", step, what, row.state, th, a.RevokedAt, row.eol)
					}
				}
				for th, a := range got {
					if _, ok := model[th]; !ok {
						t.Fatalf("step %d %s: DB row %s (port %d %s) unknown to the model — a shadowed history row survived GC, or a state moved behind the model", step, what, th, a.Port, a.State)
					}
				}
			}
			for step := 0; step < steps; step++ {
				clock = clock.Add(time.Duration(1+r.Intn(60)) * time.Second)
				switch op := r.Intn(100); {
				case op < 40: // auto allocate
					name := names[r.Intn(len(names))]
					want := lowestFree()
					a, err := Allocate(db, "lab", "lab-1", name, 9000+r.Intn(100), 0, "SHA256:owner", false, cfg)
					switch {
					case allocatedName(name):
						hits["name-collision"]++
						if !errors.Is(err, ErrNameTaken) {
							t.Fatalf("step %d: allocate %q while ALLOCATED: err=%v want ErrNameTaken", step, name, err)
						}
					case want == 0:
						hits["exhaustion"]++
						if !errors.Is(err, ErrPortExhausted) {
							t.Fatalf("step %d: I3 violated: band full but Allocate returned %v (port %v)", step, err, a)
						}
					default:
						hits["allocate"]++
						if err != nil {
							t.Fatalf("step %d: auto allocate: %v", step, err)
						}
						if a.Port != want {
							t.Fatalf("step %d: I3 violated: got port %d, lowest free was %d", step, a.Port, want)
						}
						if anyRowOnPort(a.Port) {
							hits["reuse-after-terminate"]++ // a history row is now shadowed by a live one
						}
						if _, dup := model[a.TokenHash]; dup || a.TokenHash == "" {
							t.Fatalf("step %d: Allocate returned token hash %q, which the model already holds (or is empty)", step, a.TokenHash)
						}
						model[a.TokenHash] = portModelRow{port: a.Port, name: name, state: StateAllocated}
					}
					compare(step, "allocate")
				case op < 55: // free a random band port
					p := bandLow + r.Intn(bandHigh-bandLow+1)
					err := Free(db, p, clock)
					th, live := liveOnPort(p)
					switch {
					case !anyRowOnPort(p):
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("step %d: free of never-allocated %d: %v", step, p, err)
						}
					case live:
						hits["terminate"]++
						if err != nil {
							t.Fatalf("step %d: free %d: %v", step, p, err)
						}
						row := model[th]
						model[th] = portModelRow{port: row.port, name: row.name, state: StateFreed, eol: clock}
					default:
						if err != nil { // freeing a terminated row is a no-op, not an error
							t.Fatalf("step %d: free of terminated %d: %v", step, p, err)
						}
					}
					compare(step, "free")
				case op < 65: // revoke
					p := bandLow + r.Intn(bandHigh-bandLow+1)
					err := Revoke(db, p, clock)
					th, live := liveOnPort(p)
					switch {
					case !anyRowOnPort(p):
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("step %d: revoke of never-allocated %d: %v", step, p, err)
						}
					case live:
						hits["terminate"]++
						if err != nil {
							t.Fatalf("step %d: revoke %d: %v", step, p, err)
						}
						row := model[th]
						model[th] = portModelRow{port: row.port, name: row.name, state: StateRevoked, eol: clock}
					default:
						if err != nil {
							t.Fatalf("step %d: revoke of terminated %d: %v", step, p, err)
						}
					}
					compare(step, "revoke")
				default: // GC with a cutoff somewhere in the recent past
					cutoff := clock.Add(-time.Duration(r.Intn(180)) * time.Second)
					if _, err := GCTerminated(db, cutoff); err != nil {
						t.Fatal(err)
					}
					for th, row := range model {
						if row.state != StateAllocated && row.eol.Before(cutoff) {
							delete(model, th) // I4: end of life before cutoff ⇒ gone; at or after ⇒ kept (compare)
							hits["gc-deleted"]++
						}
					}
					compare(step, "gc")
				}
			}
		})
	}
	for _, k := range []string{"allocate", "name-collision", "exhaustion", "reuse-after-terminate", "terminate", "gc-deleted"} {
		if hits[k] < 10 {
			t.Fatalf("the walk reached %q only %d times across %dx%d steps — the invariants were checked against sequences that do not exercise it: %v",
				k, hits[k], sequences, steps, hits)
		}
	}
}

// The generator's G2 self-check lives INSIDE TestAllocationInvariantsUnderRandomOperations (the
// `hits` floor at its end). A separate "reaches every path" test used to replay the RNG in a shadow
// model and count what it WOULD have done; that is a second walk kept in lock-step by hand, and it
// counts emissions, not effects (internal review L2-F7). It was deleted on 2026-09-01 and its line
// hand-removed from test/determinism/testdata/test_function_inventory.txt.
