package brokerupgrade

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// An armed Start replies before its exec and leaves the activation intent as
// journaled. If the S6 reload then returns instead of exec'ing, the source
// still serves: the returned cut journals current-state recovery for exactly
// that handle, waiting briefly for a busy sequencer, and never touches a
// record that has already moved on.
// origin: hot-update S8 plan §3.2 / §4.1 / §8.1; S8 BROKER-EXPORTER R-11
func TestReturnedBrokerCutJournalsRecoveryForItsHandle(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stale    bool
		busy     time.Duration
		recovery bool
	}{{"idle sequencer", false, 0, true}, {"briefly busy sequencer", false, 50 * time.Millisecond, true}, {"record moved on", true, 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEngineFixture()
			f.runtime.activateErr = &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationDispatched, Accepted: true}
			view, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			a := active(view.Historical)
			if !activationDispatched(err) || a.Phase != upgradetx.PhaseActivating || a.CutStage != upgradetx.CutPartialActive || a.Outcome != upgradetx.OutcomePending {
				t.Fatalf("armed Start: phase=%s stage=%s err=%v", a.Phase, a.CutStage, err)
			}
			expected := handle(view.Historical)
			if tc.stale {
				expected.Revision--
			}
			e := f.endpoint.(*engine)
			if tc.busy > 0 {
				e.mu.Lock()
				time.AfterFunc(tc.busy, e.mu.Unlock)
			}
			settleReturned(f.endpoint, expected, errors.New("exec returned"))
			got := active(f.store.record)
			if recovered := got.Phase == upgradetx.PhaseRecoveryRequired && got.Outcome == upgradetx.OutcomeUnknown; recovered != tc.recovery {
				t.Fatalf("returned cut: phase=%s outcome=%s, want recovery=%t", got.Phase, got.Outcome, tc.recovery)
			}
			if got.CutStage != upgradetx.CutPartialActive {
				t.Fatalf("returned cut rewrote its cut stage: %s", got.CutStage)
			}
		})
	}
}

// heldFile is an inherited reservation over a real descriptor.
type heldFile struct {
	file   *os.File
	closed bool
}

func (l *heldFile) FD() uintptr      { return l.file.Fd() }
func (l *heldFile) DomainID() string { return strings.Repeat("d", 32) }
func (l *heldFile) Close() error     { l.closed = true; return l.file.Close() }

// The importer compares the carried authority map with the store it opened
// before any completion: a session recreated across the cut leaves the cut
// for recovery (it is never resumed) and keeps the inherited reservation; a
// matching map proceeds to Resume.
// origin: hot-update S8 plan §6.1 / §6.2 / §8.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
func TestImportedCutWithAChangedAuthorityMapIsLeftForRecovery(t *testing.T) {
	hash, err := auth.HashPIN("2468")
	if err != nil {
		t.Fatal(err)
	}
	for _, recreated := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreated=%t", recreated), func(t *testing.T) {
			f := newEngineFixture()
			db := testharness.OpenDB(t)
			if _, err := session.Create(db, "lab", "lab", "owner-fp", hash, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			carried, err := brokerupgradeport.SessionAuthorities(db)
			if err != nil {
				t.Fatal(err)
			}
			if recreated {
				if _, err := db.Exec(`DELETE FROM sessions WHERE sid = 'lab'`); err != nil {
					t.Fatal(err)
				}
				if _, err := session.Create(db, "lab", "lab", "owner-fp", hash, time.Now().UTC().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			state := upgradecut.State{Attempt: f.artifact.Grant.Handle, Authorities: carried}
			runtime := NewRuntime(RuntimeHost{Key: f.artifact.Grant.Handle.Key, Journal: f.store, Inherited: &state,
				Owner: func() (upgradetx.Owner, error) { return f.artifact.Grant.Issuer, nil }, RunningSHA: func() (string, error) { return f.artifact.Grant.TargetSHA, nil }})
			file, err := os.CreateTemp(t.TempDir(), "inherited-lease-*")
			if err != nil {
				t.Fatal(err)
			}
			lease := &heldFile{file: file}
			deps := Attach(&Dependencies{Key: f.artifact.Grant.Handle.Key, Store: f.store, Runtime: runtime, Registry: UnwiredRegistry(),
				Imported: state.Attempt, Lease: func() upgradelock.Lease { return lease }}, BrokerPort{DB: func() *sql.DB { return db }})
			var logs bytes.Buffer
			ResumeImported(t.Context(), New(f.store, runtime, UnwiredRegistry()), deps, slog.New(slog.NewTextHandler(&logs, nil)))
			mismatch := strings.Contains(logs.String(), "authority differs")
			resumed := strings.Contains(logs.String(), "retained unresolved evidence")
			if mismatch != recreated || resumed == recreated || lease.closed {
				t.Fatalf("recreated=%t: mismatch=%t resumed=%t lease released=%t\n%s", recreated, mismatch, resumed, lease.closed, logs.String())
			}
			_ = lease.Close()
		})
	}
}

// origin: S8 internal review cluster-roll F8
func TestParentReservationOutlivesItsBrokerChild(t *testing.T) {
	f := newEngineFixture()
	f.store.grant.ParentTxID = strings.Repeat("a", 32)
	f.store.grant.ParentSlot = "original-host-slot"
	f.artifact.Grant = f.store.grant
	view := f.start(t)
	file, err := os.CreateTemp(t.TempDir(), "parent-lease-*")
	if err != nil {
		t.Fatal(err)
	}
	lease := &heldFile{file: file}
	t.Cleanup(func() {
		if !lease.closed {
			_ = lease.Close()
		}
	})
	deps := &Dependencies{Store: f.store, Runtime: f.runtime, Imported: view.Historical.Forward.Handle, Lease: func() upgradelock.Lease { return lease }}
	ResumeImported(t.Context(), f.endpoint, deps, nil)
	if lease.closed {
		t.Fatal("component completion released its parent's host reservation")
	}
	if err := ReleaseParentLease(deps); err != nil || !lease.closed {
		t.Fatalf("host barrier did not release the adopted reservation: %v", err)
	}
	if err := ReleaseParentLease(deps); err != nil {
		t.Fatal("host release was not idempotent:", err)
	}
}
