package clusteroffline

import (
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// hashFiles returns a digest over the db file + its -wal/-shm sidecars (absent ones contribute
// nothing) — used to prove Doctor() is byte-for-byte non-mutating.
func hashFiles(t *testing.T, base string) [32]byte {
	t.Helper()
	h := sha256.New()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if b, err := os.ReadFile(base + suffix); err == nil {
			_, _ = h.Write([]byte(suffix))
			_, _ = h.Write(b)
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// TestDoctorDBReadOnlyNoMutation (B3 review M4): Doctor() opens the DB read-only and mutates
// NOTHING — the central migration-safety claim. Hash db+-wal+-shm before/after.
func TestDoctorDBReadOnlyNoMutation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	before := hashFiles(t, dbPath)
	// Other checks FATAL (empty secrets dir / no conf) — irrelevant; we only assert no DB write.
	_ = Doctor(DoctorOptions{SecretsDir: t.TempDir(), DBPath: dbPath, ConfPath: filepath.Join(t.TempDir(), "absent.conf")})
	if hashFiles(t, dbPath) != before {
		t.Fatal("Doctor() mutated the DB — it must be strictly read-only")
	}
}

// TestDoctorNoEarlyExitOnFatal (B3 review M4): a FATAL check does NOT abort the run — every check
// is still evaluated + reported (so the operator sees all problems at once).
func TestDoctorNoEarlyExitOnFatal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	// Empty secrets dir → the "secrets" check is FATAL; the "db" check must STILL appear after it.
	checks := Doctor(DoctorOptions{SecretsDir: t.TempDir(), DBPath: dbPath, ConfPath: filepath.Join(t.TempDir(), "absent.conf")})
	var sawSecretsFatal, sawDB bool
	for _, c := range checks {
		if c.Name == "secrets" && c.Status == DoctorFatal {
			sawSecretsFatal = true
		}
		if c.Name == "db" {
			sawDB = true
		}
	}
	if !sawSecretsFatal {
		t.Error("empty secrets dir should yield a secrets FATAL")
	}
	if !sawDB {
		t.Error("the db check must still run after an earlier FATAL (no early-exit)")
	}
}

// TestDoctorPortClassification (B3 item 2): an in-use raft-addr is a real conflict (FATAL); an
// in-use nats-route is the running daemon (ADVISORY); a malformed addr is always FATAL.
func TestDoctorPortClassification(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	if got := portBindCheck("raft-addr", addr, true); got.Status != DoctorFatal {
		t.Errorf("in-use raft-addr = %v, want FATAL", got.Status)
	}
	if got := portBindCheck("nats-route", addr, false); got.Status != DoctorAdvisory {
		t.Errorf("in-use nats-route = %v, want ADVISORY", got.Status)
	}
	if got := portBindCheck("raft-addr", "not-an-addr", true); got.Status != DoctorFatal {
		t.Errorf("malformed addr = %v, want FATAL", got.Status)
	}
}

func TestDoctorSummary(t *testing.T) {
	checks := []DoctorCheck{
		{"a", DoctorPass, ""}, {"b", DoctorAdvisory, ""}, {"c", DoctorFatal, ""}, {"d", DoctorPass, ""},
	}
	if p, a, f := DoctorSummary(checks); p != 2 || a != 1 || f != 1 {
		t.Errorf("summary = %d/%d/%d, want 2/1/1", p, a, f)
	}
}

func TestDoctorStripScheme(t *testing.T) {
	if stripScheme("nats://host:6222") != "host:6222" {
		t.Error("stripScheme should strip the scheme")
	}
	if stripScheme("host:6222") != "host:6222" {
		t.Error("stripScheme should pass through a no-scheme addr")
	}
}
