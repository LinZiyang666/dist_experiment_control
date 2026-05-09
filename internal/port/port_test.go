package port

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, "SHA256:alice", tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if a.Port != 14000 {
		t.Errorf("first allocation: got port %d want 14000", a.Port)
	}
	if a.Token == "" {
		t.Error("raw token must be returned exactly once on Allocate")
	}
	if a.TokenHash == HashToken(a.Token) {
		// good
	} else {
		t.Errorf("token hash inconsistent: got %s want %s", a.TokenHash, HashToken(a.Token))
	}
	if a.State != StateAllocated {
		t.Errorf("state: got %s want ALLOCATED", a.State)
	}
}

func TestAllocateRejectsDuplicateName(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	if _, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, "SHA256:alice", tinyBand()); err != nil {
		t.Fatal(err)
	}
	_, err := Allocate(db, "lab", "lab-1", "jupyter", 9999, "SHA256:alice", tinyBand())
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("want ErrNameTaken, got %v", err)
	}
}

func TestAllocateExhaustsBand(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	for i := 0; i < 3; i++ {
		if _, err := Allocate(db, "lab", "lab-1",
			"name-"+string(rune('a'+i)), 8000+i, "SHA256:alice", tinyBand()); err != nil {
			t.Fatalf("alloc %d: %v", i, err)
		}
	}
	_, err := Allocate(db, "lab", "lab-1", "extra", 9000, "SHA256:alice", tinyBand())
	if !errors.Is(err, ErrPortExhausted) {
		t.Fatalf("want ErrPortExhausted, got %v", err)
	}
}

func TestFreeReturnsPortToPool(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, _ := Allocate(db, "lab", "lab-1", "first", 8888, "SHA256:alice", tinyBand())
	if err := Free(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	// Re-allocate with a different name; must immediately reuse the same port.
	b, err := Allocate(db, "lab", "lab-1", "second", 9999, "SHA256:alice", tinyBand())
	if err != nil {
		t.Fatal(err)
	}
	if b.Port != a.Port {
		t.Errorf("re-allocation should reuse freed port: got %d want %d", b.Port, a.Port)
	}
}

func TestRevokeReturnsPortToPool(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	a, _ := Allocate(db, "lab", "lab-1", "first", 8888, "SHA256:alice", tinyBand())
	if err := Revoke(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	b, err := Allocate(db, "lab", "lab-1", "second", 9999, "SHA256:alice", tinyBand())
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

	a, _ := Allocate(db, "lab", "lab-1", "jupyter", 8888, "SHA256:alice", tinyBand())
	if err := Free(db, a.Port, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Same name re-allocated; LookupByName must return the new ALLOCATED row,
	// not the FREED one.
	b, _ := Allocate(db, "lab", "lab-1", "jupyter", 9999, "SHA256:alice", tinyBand())

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
	a, _ := Allocate(db, "lab", "lab-1", "jupyter", 8888, "SHA256:alice", tinyBand())
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
		if _, err := Allocate(db, "lab", nid, "p-"+nid, 8000, "SHA256:alice", cfg); err != nil {
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

func TestSessionDeleteCascadesPortRows(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	if _, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, "SHA256:alice", tinyBand()); err != nil {
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
