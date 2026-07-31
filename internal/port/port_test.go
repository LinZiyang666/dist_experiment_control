package port

import (
	"database/sql"
	"errors"
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
