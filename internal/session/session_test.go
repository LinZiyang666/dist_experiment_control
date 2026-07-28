package session

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// openDB delegates to internal/testharness (B9). Seven packages carried a byte-identical copy of this
// eight-line body; the local name is kept so this package's call sites do not churn, which is the same
// one-line-shim shape the e2e suites already use.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return testharness.OpenDB(t)
}

const (
	ownerFP   = "SHA256:owner-fp"
	memberFP  = "SHA256:member-fp"
	pinHashed = "$argon2id$v=19$m=65536,t=3,p=2$AAAA$BBBB" // syntactic only; tests inject verifyPIN
)

func TestCreateAddsOwner(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()

	s, err := Create(db, "lab", "lab", ownerFP, pinHashed, now)
	if err != nil {
		t.Fatal(err)
	}
	if s.State != StateActive {
		t.Errorf("expected ACTIVE, got %s", s.State)
	}

	owner, err := IsOwner(db, "lab", ownerFP)
	if err != nil || !owner {
		t.Errorf("creator should be owner, got owner=%v err=%v", owner, err)
	}
	mem, _ := IsMember(db, "lab", ownerFP)
	if !mem {
		t.Error("creator should also be member")
	}
}

func TestCreateRejectsBadSID(t *testing.T) {
	db := openDB(t)
	if _, err := Create(db, "Default", "x", ownerFP, pinHashed, time.Now()); err == nil {
		t.Fatal("expected sid validation error")
	}
}

func TestCreateRejectsDuplicate(t *testing.T) {
	db := openDB(t)
	if _, err := Create(db, "lab", "lab", ownerFP, pinHashed, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err := Create(db, "lab", "lab", ownerFP, pinHashed, time.Now())
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestPlanHardDeleteRejectsActiveSession(t *testing.T) {
	db := openDB(t)
	if _, err := Create(db, "lab", "lab", ownerFP, pinHashed, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanHardDelete(db, "lab"); !errors.Is(err, ErrDeleting) {
		t.Fatalf("PlanHardDelete active session err = %v, want ErrDeleting", err)
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(StateActive) {
		t.Fatalf("active session state changed to %q", state)
	}
}

func TestPlanHardDeleteDeletesFullSubtree(t *testing.T) {
	db := openDB(t)
	now := time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC)
	if _, err := Create(db, "lab", "lab", ownerFP, pinHashed, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,last_heartbeat_at,status) VALUES('lab','lab-1',?, 'ONLINE')`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp) VALUES('p1','lab','lab-1','[]',?,'RUNNING',?)`, now, ownerFP); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at) VALUES(14000,'lab','lab-1','web',8080,'hash','ALLOCATED',?,?)`, ownerFP, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES('lab','lab-1','SHA256:agent',?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp,created_at) VALUES('lab','sub1','alice','subhash','psk','2022-blake3-aes-256-gcm',?,?)`, ownerFP, now); err != nil {
		t.Fatal(err)
	}
	if err := Tombstone(db, "lab", now); err != nil {
		t.Fatal(err)
	}
	cmd, err := PlanHardDelete(db, "lab")
	if err != nil {
		t.Fatalf("PlanHardDelete: %v", err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatalf("ExecCommand: %v", err)
	}
	for _, tbl := range []string{"sessions", "members", "nodes", "processes", "port_allocations", "agent_provisioning", "proxy_subscribers"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + tbl + ` WHERE sid='lab'`).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if n != 0 {
			t.Fatalf("%s rows after hard delete = %d, want 0", tbl, n)
		}
	}
}

func TestGetAndIsActive(t *testing.T) {
	db := openDB(t)
	if _, err := Create(db, "lab", "lab", ownerFP, pinHashed, time.Now()); err != nil {
		t.Fatal(err)
	}
	s, err := Get(db, "lab")
	if err != nil || s.SID != "lab" {
		t.Fatalf("Get: %+v %v", s, err)
	}
	active, _ := IsActive(db, "lab")
	if !active {
		t.Error("session must be active immediately after Create")
	}
	active, _ = IsActive(db, "ghost")
	if active {
		t.Error("missing session must not be active")
	}
	if _, err := Get(db, "ghost"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing session, got %v", err)
	}
}

func TestListVisibleReturnsOnlyMemberSessions(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	mustCreate(t, db, "alpha", ownerFP, now)
	mustCreate(t, db, "beta", "SHA256:other-owner", now)

	got, err := ListVisible(db, ownerFP)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SID != "alpha" {
		t.Errorf("expected alpha only, got %+v", got)
	}

	// Add memberFP into beta — now both should appear for memberFP.
	if err := AddMember(db, "beta", memberFP, RoleMember, ViaPin, now); err != nil {
		t.Fatal(err)
	}
	got, _ = ListVisible(db, memberFP)
	if len(got) != 1 || got[0].SID != "beta" {
		t.Errorf("expected beta for memberFP, got %+v", got)
	}
}

func TestTombstoneTransitions(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	mustCreate(t, db, "lab", ownerFP, now)

	if err := Tombstone(db, "lab", now); err != nil {
		t.Fatal(err)
	}
	active, _ := IsActive(db, "lab")
	if active {
		t.Error("Tombstone should leave session inactive")
	}
	s, _ := Get(db, "lab")
	if s.State != StateDeleting {
		t.Errorf("expected DELETING, got %s", s.State)
	}
	// Idempotency: second tombstone returns ErrDeleting.
	if err := Tombstone(db, "lab", now); !errors.Is(err, ErrDeleting) {
		t.Errorf("second tombstone: expected ErrDeleting, got %v", err)
	}
	// Missing session: ErrNotFound.
	if err := Tombstone(db, "ghost", now); !errors.Is(err, ErrNotFound) {
		t.Errorf("ghost tombstone: expected ErrNotFound, got %v", err)
	}
}

func TestJoinWithPINHappyPath(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	mustCreate(t, db, "lab", ownerFP, now)

	verifyOK := func(pin, hash string) error { return nil }
	if err := JoinWithPIN(db, "lab", memberFP, "any-pin", verifyOK, now); err != nil {
		t.Fatal(err)
	}
	mem, _ := IsMember(db, "lab", memberFP)
	if !mem {
		t.Error("memberFP should be a member after JoinWithPIN")
	}
	owner, _ := IsOwner(db, "lab", memberFP)
	if owner {
		t.Error("PIN-joined member should NOT be owner")
	}
}

func TestJoinWithPINRejectsBadPIN(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	mustCreate(t, db, "lab", ownerFP, now)

	verifyFail := func(pin, hash string) error { return errors.New("nope") }
	err := JoinWithPIN(db, "lab", memberFP, "wrong", verifyFail, now)
	if !errors.Is(err, ErrInvalidPIN) {
		t.Errorf("expected ErrInvalidPIN, got %v", err)
	}
	mem, _ := IsMember(db, "lab", memberFP)
	if mem {
		t.Error("memberFP must NOT be added on PIN failure")
	}
}

func TestJoinWithPINRejectsMissingAndDeleting(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	if err := JoinWithPIN(db, "ghost", memberFP, "x", func(p, h string) error { return nil }, now); !errors.Is(err, ErrNotFound) {
		t.Errorf("ghost: expected ErrNotFound, got %v", err)
	}
	mustCreate(t, db, "lab", ownerFP, now)
	if err := Tombstone(db, "lab", now); err != nil {
		t.Fatal(err)
	}
	if err := JoinWithPIN(db, "lab", memberFP, "x", func(p, h string) error { return nil }, now); !errors.Is(err, ErrDeleting) {
		t.Errorf("DELETING: expected ErrDeleting, got %v", err)
	}
}

func TestAddMemberIdempotent(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC()
	mustCreate(t, db, "lab", ownerFP, now)

	for i := 0; i < 3; i++ {
		if err := AddMember(db, "lab", memberFP, RoleMember, ViaPin, now); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM members WHERE sid='lab' AND pubkey_fp=?`, memberFP).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("AddMember should be idempotent, got %d rows", n)
	}
}

func mustCreate(t *testing.T, db *sql.DB, sid, fp string, now time.Time) {
	t.Helper()
	if _, err := Create(db, sid, sid, fp, pinHashed, now); err != nil {
		t.Fatalf("Create(%s): %v", sid, err)
	}
}
