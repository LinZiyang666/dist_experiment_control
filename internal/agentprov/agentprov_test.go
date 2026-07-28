package agentprov

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// openDB delegates to internal/testharness (B9) — see the note in internal/session/session_test.go.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return testharness.OpenDB(t)
}

// seedSession is a minimum sessions-row insert so the FK on
// agent_provisioning.sid is satisfied. We don't pull internal/session
// here to avoid a test-side cycle (session_test already exercises that
// package).
func seedSession(t *testing.T, db *sql.DB, sid string) {
	t.Helper()
	_, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?, ?, ?, ?)`,
		sid, sid, "SHA256:owner", "test-hash",
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLookupReturnsErrNotProvisionedWhenAbsent(t *testing.T) {
	db := openDB(t)
	seedSession(t, db, "lab")
	_, err := Lookup(db, "lab", "lab-1")
	if !errors.Is(err, ErrNotProvisioned) {
		t.Fatalf("want ErrNotProvisioned, got %v", err)
	}
}

func TestProvisionThenLookupRoundtrip(t *testing.T) {
	db := openDB(t)
	seedSession(t, db, "lab")
	if err := Provision(db, "lab", "lab-1", "SHA256:agent-1", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	got, err := Lookup(db, "lab", "lab-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "SHA256:agent-1" {
		t.Errorf("got fp=%q want SHA256:agent-1", got)
	}
}

func TestProvisionIsIdempotentForSameFP(t *testing.T) {
	db := openDB(t)
	seedSession(t, db, "lab")
	now := time.Now().UTC()
	if err := Provision(db, "lab", "lab-1", "SHA256:agent-1", now); err != nil {
		t.Fatal(err)
	}
	if err := Provision(db, "lab", "lab-1", "SHA256:agent-1", now.Add(time.Second)); err != nil {
		t.Errorf("re-provisioning the same fp should be a no-op, got %v", err)
	}
}

func TestProvisionRejectsDifferentFP(t *testing.T) {
	db := openDB(t)
	seedSession(t, db, "lab")
	now := time.Now().UTC()
	if err := Provision(db, "lab", "lab-1", "SHA256:agent-1", now); err != nil {
		t.Fatal(err)
	}
	err := Provision(db, "lab", "lab-1", "SHA256:agent-2", now.Add(time.Second))
	if !errors.Is(err, ErrAlreadyProvisioned) {
		t.Fatalf("want ErrAlreadyProvisioned, got %v", err)
	}
}

func TestSessionDeleteCascadesProvisioning(t *testing.T) {
	db := openDB(t)
	seedSession(t, db, "lab")
	if err := Provision(db, "lab", "lab-1", "SHA256:agent-1", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid = ?`, "lab"); err != nil {
		t.Fatal(err)
	}
	_, err := Lookup(db, "lab", "lab-1")
	if !errors.Is(err, ErrNotProvisioned) {
		t.Errorf("after session delete, lookup should be ErrNotProvisioned, got %v", err)
	}
}
