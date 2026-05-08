package node

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// openDB returns a fresh in-memory SQLite seeded with a single ACTIVE
// session "lab" so node.Register can target it (Register requires the
// session row to exist; FK enforced).
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:test-owner", "test-hash",
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	return db
}

func sampleInput() RegisterInput {
	return RegisterInput{
		SID: "lab", NID: "lab-1",
		ProtoVersion: 1, ReleaseVersion: "v0.0.0-dev",
		OS: "linux", Arch: "amd64",
		BootID: "deadbeef",
	}
}

func TestStateForAge(t *testing.T) {
	cases := []struct {
		age  time.Duration
		want State
	}{
		{0, StateOnline},
		{4 * time.Second, StateOnline},
		{5 * time.Second, StateStale}, // boundary: >= staleAfter → STALE
		{59 * time.Second, StateStale},
		{60 * time.Second, StateOffline},
		{2 * time.Hour, StateOffline},
	}
	for _, c := range cases {
		got := stateForAge(c.age, DefaultStaleAfter, DefaultOfflineAfter)
		if got != c.want {
			t.Errorf("stateForAge(%v) = %s, want %s", c.age, got, c.want)
		}
	}
}

func TestRegisterHappyPath(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()

	if err := Register(db, sampleInput(), now); err != nil {
		t.Fatalf("Register: %v", err)
	}

	snaps, err := List(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].SID != "lab" || snaps[0].NID != "lab-1" || snaps[0].Status != string(StateOnline) {
		t.Fatalf("List() = %+v, want one ONLINE node lab/lab-1", snaps)
	}
}

// Register MUST refuse to insert a node into a session that doesn't exist.
// The owner is responsible for creating the session first.
func TestRegisterRejectsMissingSession(t *testing.T) {
	db := openDB(t)
	in := sampleInput()
	in.SID = "ghost-session"
	err := Register(db, in, time.Now())
	if err == nil {
		t.Fatal("Register must error when session row is missing")
	}
	if !errorsIsSessionMissing(err) {
		t.Errorf("expected ErrSessionMissing, got %v", err)
	}
}

func errorsIsSessionMissing(err error) bool {
	return err != nil && err == ErrSessionMissing
}

func TestRegisterIsIdempotentUpsert(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	if err := Register(db, sampleInput(), now); err != nil {
		t.Fatal(err)
	}
	// Second register with newer release_version should update, not duplicate.
	in := sampleInput()
	in.ReleaseVersion = "v0.1.0"
	in.BootID = "newboot"
	if err := Register(db, in, now.Add(time.Second)); err != nil {
		t.Fatalf("Register again: %v", err)
	}

	snaps, _ := List(db)
	if len(snaps) != 1 {
		t.Fatalf("expected 1 row after re-register, got %d", len(snaps))
	}
	if snaps[0].ReleaseVersion != "v0.1.0" || snaps[0].BootID != "newboot" {
		t.Errorf("re-register did not refresh fields: %+v", snaps[0])
	}
}

func TestHeartbeatBringsBackOnline(t *testing.T) {
	db := openDB(t)
	t0 := time.Now().UTC()
	if err := Register(db, sampleInput(), t0); err != nil {
		t.Fatal(err)
	}
	// Force status STALE by reconcile after 10s.
	if _, err := ReconcileStates(db, t0.Add(10*time.Second), DefaultStaleAfter, DefaultOfflineAfter); err != nil {
		t.Fatal(err)
	}
	snaps, _ := List(db)
	if snaps[0].Status != string(StateStale) {
		t.Fatalf("expected STALE after 10s, got %s", snaps[0].Status)
	}

	if err := Heartbeat(db, "lab", "lab-1", t0.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	snaps, _ = List(db)
	if snaps[0].Status != string(StateOnline) {
		t.Fatalf("expected ONLINE after heartbeat, got %s", snaps[0].Status)
	}
}

func TestHeartbeatUnknownNodeRejected(t *testing.T) {
	db := openDB(t)
	if err := Heartbeat(db, "no-sid", "no-nid", time.Now()); err == nil {
		t.Fatal("Heartbeat for unknown node must error")
	}
}

func TestReconcileTransitions(t *testing.T) {
	db := openDB(t)
	t0 := time.Now().UTC()
	if err := Register(db, sampleInput(), t0); err != nil {
		t.Fatal(err)
	}

	// 0s: still ONLINE — no transition.
	n, err := ReconcileStates(db, t0, DefaultStaleAfter, DefaultOfflineAfter)
	if err != nil || n != 0 {
		t.Fatalf("at 0s: n=%d err=%v", n, err)
	}

	// 6s: → STALE.
	n, _ = ReconcileStates(db, t0.Add(6*time.Second), DefaultStaleAfter, DefaultOfflineAfter)
	if n != 1 {
		t.Fatalf("at 6s: expected 1 transition, got %d", n)
	}

	// 6s again: stable, no further transition.
	n, _ = ReconcileStates(db, t0.Add(6*time.Second), DefaultStaleAfter, DefaultOfflineAfter)
	if n != 0 {
		t.Fatalf("at 6s repeat: expected 0 transitions, got %d", n)
	}

	// 65s: → OFFLINE.
	n, _ = ReconcileStates(db, t0.Add(65*time.Second), DefaultStaleAfter, DefaultOfflineAfter)
	if n != 1 {
		t.Fatalf("at 65s: expected 1 transition, got %d", n)
	}
	snaps, _ := List(db)
	if snaps[0].Status != string(StateOffline) {
		t.Fatalf("expected OFFLINE at 65s, got %s", snaps[0].Status)
	}
}

func TestReconcileSkipsNoHeartbeat(t *testing.T) {
	db := openDB(t)
	// Insert a row directly with NULL last_heartbeat_at — Reconcile must not crash.
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('s','s','x','x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid, nid, status) VALUES ('s','n','ONLINE')`); err != nil {
		t.Fatal(err)
	}
	if _, err := ReconcileStates(db, time.Now(), DefaultStaleAfter, DefaultOfflineAfter); err != nil {
		t.Fatalf("Reconcile with NULL heartbeat: %v", err)
	}
}
