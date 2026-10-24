package binaryupgrade

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

type rollbackFixture struct {
	canonical, marker string
	previous          []byte
	m                 *Marker
}

// A candidate at the canonical path, the previous image in its prev slot and
// a pending marker that names both. Every path is private to this test.
func newRollbackFixture(t *testing.T) rollbackFixture {
	t.Helper()
	root := t.TempDir()
	f := rollbackFixture{canonical: filepath.Join(root, "tether"), previous: []byte("previous image")}
	f.marker = MarkerPath(f.canonical)
	if err := os.WriteFile(f.canonical, []byte("candidate image"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(PrevPath(f.canonical), f.previous, 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(f.previous)
	f.m = &Marker{State: StatePending, PrevSHA: hex.EncodeToString(sum[:]), NewSHA: "candidate", UpgradeID: "rollback-exec",
		Deadline: time.Now().Add(time.Hour), BootBudget: upgradebudget.BootBudget}
	if err := (Store{}).WriteMarker(f.marker, f.m); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f rollbackFixture) read(t *testing.T) *Marker {
	t.Helper()
	m, err := ReadMarker(f.marker)
	if err != nil || m == nil {
		t.Fatalf("marker unreadable: %v", err)
	}
	return m
}

// A failed exec cannot prove that the running candidate stopped. Rolled_back is
// recorded only by the restored image's boot convergence (or by an injected
// exec that stands for it); before the exec and after its failure the disk must
// never claim the reverse completed, and the failure must not stay pending
// either, or the still-running candidate could commit itself over the restore.
// origin: hot-update S8 plan §1 / §11.2 P17 mutant "preexec rolled_back"; S8 review rv-fault F7
func TestFaultFailedReverseExecCannotReportRolledBack(t *testing.T) {
	for _, errno := range []error{syscall.EACCES, syscall.ENOEXEC, syscall.ETXTBSY} {
		t.Run(errno.Error(), func(t *testing.T) {
			f := newRollbackFixture(t)
			var before *Marker
			calls := 0
			(Store{}).ExecuteRollback(f.canonical, f.m, "watchdog deadline", slog.New(slog.NewTextHandler(io.Discard, nil)), true, func(path string) error {
				calls++
				before = f.read(t)
				if path != f.canonical {
					t.Errorf("exec targeted %q, want the restored canonical path", path)
				}
				return errno
			})
			if calls != 1 {
				t.Fatalf("exec boundary reached %d times", calls)
			}
			// The exec boundary is the last moment the candidate is provably
			// running: it may hold recovery intent, never a completed reverse.
			if before.State != StatePending || before.Detail != "watchdog deadline" {
				t.Fatalf("pre-exec record claims more than recovery intent: %+v", before)
			}
			after := f.read(t)
			if after.State != StateRollbackFailed || after.Detail != "watchdog deadline" {
				t.Fatalf("disk reports %q (%q) despite exec %v and a surviving candidate", after.State, after.Detail, errno)
			}
			// The restore itself is kept: the next restart boots the previous image.
			if got, err := os.ReadFile(f.canonical); err != nil || !bytes.Equal(got, f.previous) {
				t.Fatalf("exec failure unwound the restored image: %q %v", got, err)
			}
			if _, err := os.Stat(PrevPath(f.canonical)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("prev slot not consumed by the restore: %v", err)
			}
			// A later register by the surviving candidate cannot treat the
			// failure as a still-open upgrade: decideBoot/commit see no pending.
			if DecideBoot(after, f.m.PrevSHA, time.Now()) != BootProceed {
				t.Fatal("failed rollback exec left a decidable pending transaction")
			}
		})
	}
}

// The restored image converges the pending recovery intent itself, and the
// two callers whose running image already IS the previous code record now.
// origin: hot-update S8 plan §1 / §11.2 P17
func TestRollbackRecordsRolledBackOnlyForTheRunningPreviousImage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Run("injected exec stands for the restored boot", func(t *testing.T) {
		f := newRollbackFixture(t)
		(Store{}).ExecuteRollback(f.canonical, f.m, "boot budget", logger, true, func(string) error { return nil })
		if m := f.read(t); m.State != StateRolledBack || m.Detail != "boot budget" {
			t.Fatalf("restored boot did not record the reverse: %+v", m)
		}
	})
	t.Run("restored binary converges a crash between restore and exec", func(t *testing.T) {
		f := newRollbackFixture(t)
		(Store{}).ExecuteRollback(f.canonical, f.m, "watchdog deadline", logger, true, func(string) error {
			// Simulate the process dying at the exec boundary: the previous
			// image boots from disk and runs the same pure decision.
			m := f.read(t)
			if DecideBoot(m, m.PrevSHA, time.Now()) != BootConvergeRolledBack {
				t.Error("restored image cannot converge the recovery intent")
			}
			return syscall.EINTR
		})
	})
	t.Run("old image is the restored code", func(t *testing.T) {
		f := newRollbackFixture(t)
		(Store{}).ExecuteRollback(f.canonical, f.m, "syscall.Exec of the new binary failed", logger, false, func(string) error {
			t.Error("non-reexec rollback executed an image")
			return nil
		})
		if m := f.read(t); m.State != StateRolledBack {
			t.Fatalf("running previous image did not record its rollback: %+v", m)
		}
	})
}

// A v2 journal at the marker path is read-only for every v1 writer: boot
// rollback/converge, bootstrap redispatch and install compensation all read
// through ReadMarker and must refuse rather than act on its v1 projection.
// origin: hot-update S8 plan §3.3 / §11.2 P17 / M11
func TestLegacyMarkerReaderRefusesTransactionJournal(t *testing.T) {
	for name, extra := range map[string]string{
		"version":        `"version":2`,
		"schema_version": `"schema_version":1`,
		"entries":        `"entries":[]`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newRollbackFixture(t)
			// Every v1 projection field says "roll me back": deadline passed.
			raw := `{"state":"pending","prev_sha":"` + f.m.PrevSHA + `","new_sha":"` + f.m.NewSHA + `","deadline":"2000-01-01T00:00:00Z","boot_budget":3,` + extra + `}`
			if err := os.WriteFile(f.marker, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			if m, err := ReadMarker(f.marker); m != nil || !errors.Is(err, ErrTransactionJournal) {
				t.Fatalf("v2 journal read as a v1 marker: %+v %v", m, err)
			}
			execs := 0
			got := (Store{}).CheckBoot(f.canonical, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Now(), nil, nil, func(string) error { execs++; return nil })
			raw2, err := os.ReadFile(f.marker)
			if got != "" || execs != 0 || err != nil || string(raw2) != raw {
				t.Fatalf("v1 boot writer acted on a v2 journal: proof=%q execs=%d marker=%s", got, execs, raw2)
			}
			if data, err := os.ReadFile(PrevPath(f.canonical)); err != nil || !bytes.Equal(data, f.previous) {
				t.Fatal("v1 boot writer consumed the prev slot of a v2 journal")
			}
		})
	}
	// A plain legacy marker that happens to carry version 1 stays readable.
	f := newRollbackFixture(t)
	if err := os.WriteFile(f.marker, []byte(`{"state":"committed","version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if m, err := ReadMarker(f.marker); err != nil || m == nil || m.State != StateCommitted {
		t.Fatalf("v1 marker became unreadable: %+v %v", m, err)
	}
}
