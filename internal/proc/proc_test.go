package proc

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
)

// openDB returns a fresh in-memory SQLite seeded with one ACTIVE session
// and one node — the (sid, nid) FK target proc.Insert needs.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
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
