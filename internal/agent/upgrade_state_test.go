package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

func testMarker(state string, bootCount int, deadline time.Time) *upgradeMarker {
	return &upgradeMarker{
		State: state, PrevSHA: "prevsha", NewSHA: "newsha",
		PrevVersion: "v0.1.0", NewVersion: "v0.2.0",
		Deadline: deadline, BootCount: bootCount, BootBudget: upgradeBootBudget,
		UpgradeID: "test-upgrade-id",
	}
}

// bootFixture lays out a sandbox binary dir: dst with dstContent, optional
// prev slot, optional marker. Returns (exePath, markerPath).
func bootFixture(t *testing.T, dstContent, prevContent string, m *upgradeMarker) (string, string) {
	t.Helper()
	dir := t.TempDir()
	exePath := filepath.Join(dir, "tether")
	if err := os.WriteFile(exePath, []byte(dstContent), 0o755); err != nil {
		t.Fatal(err)
	}
	if prevContent != "" {
		if err := os.WriteFile(upgradePrevPath(exePath), []byte(prevContent), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	markerPath := upgradeMarkerPath(exePath)
	if m != nil {
		requireUpgradeMarker(t, markerPath, m)
	}
	return exePath, markerPath
}

func mustSHA(t *testing.T, path string) string {
	t.Helper()
	s, err := sha256OfFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func readMarkerState(t *testing.T, markerPath string) *upgradeMarker {
	t.Helper()
	m, err := readUpgradeMarker(markerPath)
	if err != nil {
		t.Fatalf("marker unreadable: %v", err)
	}
	return m
}

func TestBootUpgradeCheckPendingIncrementsBootCount(t *testing.T) {
	now := time.Now()
	exePath, markerPath, m := stagedMarkerFixture(t, 1, now.Add(time.Minute))

	execed := ""
	proof := bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })

	if execed != "" {
		t.Fatalf("continue-pending must not exec anything; exec'd %q", execed)
	}
	if proof != m.UpgradeID {
		t.Fatalf("boot proof = %q, want transaction id %q", proof, m.UpgradeID)
	}
	got := readMarkerState(t, markerPath)
	if got.State != upgradeStatePending || got.BootCount != 2 {
		t.Errorf("marker after boot: state=%q boot_count=%d, want pending/2", got.State, got.BootCount)
	}
}

func TestBootProofFlowsIntoNewAgent(t *testing.T) {
	oldProof := currentUpgradeBootProof()
	t.Cleanup(func() { rememberUpgradeBootProof(oldProof) })
	rememberUpgradeBootProof("booted-transaction")

	a, err := New(Config{
		NATSURL: "nats://127.0.0.1:1",
		SID:     "lab",
		NID:     "n1",
		Logger:  slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.upgradeBootProofID != "booted-transaction" {
		t.Fatalf("Agent boot proof = %q, want proof recorded before config parsing", a.upgradeBootProofID)
	}
}

func TestBootUpgradeCheckBudgetExhaustedRollsBackAndExecs(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, upgradeBootBudget, now.Add(time.Minute))

	execed := ""
	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })

	if execed != exePath {
		t.Fatalf("rollback must exec the restored dst path; exec'd %q", execed)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("dst after rollback: %q, want the prev content", string(got))
	}
	if _, err := os.Stat(upgradePrevPath(exePath)); !os.IsNotExist(err) {
		t.Errorf("prev slot must be consumed by the restore (err=%v)", err)
	}
	got := readMarkerState(t, markerPath)
	if got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back", got.State)
	}
}

func TestBootUpgradeCheckDeadlinePassedRollsBack(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, 0, now.Add(-time.Second))

	execed := ""
	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })

	if execed != exePath {
		t.Fatalf("deadline rollback must exec the restored path; exec'd %q", execed)
	}
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back", got.State)
	}
}

// origin: upgrade-safety plan §3.2 F7 — the rollback path's own failure is
// terminal, loud, and non-looping: no exec, keep running what we are.
// A prev slot that exists but is NOT the recorded binary (tampered / clobbered)
// must also refuse the restore — blindly renaming it over dst would launch
// unverified bytes as the agent.
func TestBootUpgradeCheckRollbackWithoutPrevIsTerminal(t *testing.T) {
	exerciseBootUnusablePrevious(t, "")
}
func TestBootUpgradeCheckRollbackWithWrongPrevShaIsTerminal(t *testing.T) {
	exerciseBootUnusablePrevious(t, "TAMPERED")
}
func exerciseBootUnusablePrevious(t *testing.T, previous string) {
	t.Helper()
	now := time.Now()
	exe, path := bootFixture(t, "NEW", previous, nil)
	m := testMarker(upgradeStatePending, upgradeBootBudget, now.Add(time.Minute))
	m.NewSHA, m.PrevSHA = mustSHA(t, exe), "sha-of-the-binary-that-was-actually-saved"
	requireUpgradeMarker(t, path, m)
	execed := ""
	bootUpgradeCheck(exe, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })
	if execed != "" {
		t.Fatalf("unusable prev exec'd %q", execed)
	}
	if got, _ := os.ReadFile(exe); string(got) != "NEW" {
		t.Errorf("dst must be untouched; got %q", got)
	}
	if got := readMarkerState(t, path); got.State != upgradeStateRollbackFailed {
		t.Errorf("marker state = %q, want rollback_failed", got.State)
	}
}

func TestBootUpgradeCheckConvergesInterruptedRollback(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "OLD", "", nil) // we ARE the restored prev
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.PrevSHA = mustSHA(t, exePath)
	m.NewSHA = "sha-of-the-new-binary-that-was-rolled-away"
	requireUpgradeMarker(t, markerPath, m)

	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(string) error {
		t.Fatal("converge must not exec")
		return nil
	})
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back (converged)", got.State)
	}
}

func TestBootUpgradeCheckOrphanedMarkerIsRecorded(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "HAND-SWAPPED", "OLD", testMarker(upgradeStatePending, 0, now.Add(time.Minute)))

	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(string) error {
		t.Fatal("orphan handling must not exec")
		return nil
	})
	got := readMarkerState(t, markerPath)
	if got.State != upgradeStateRollbackFailed || !strings.Contains(got.Detail, "neither") {
		t.Errorf("marker = %q/%q, want rollback_failed with the neither-sha detail", got.State, got.Detail)
	}
}

// A corrupt marker is treated as idle — loudly, but with NO writes and no
// panic: a truncated JSON must never brick the boot path.
func TestBootUpgradeCheckCorruptMarkerIsIdle(t *testing.T) {
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	if err := os.WriteFile(markerPath, []byte(`{"state":"pend`), 0o644); err != nil {
		t.Fatal(err)
	}
	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), time.Now(), func(string) error {
		t.Fatal("corrupt marker must not exec")
		return nil
	})
	raw, err := os.ReadFile(markerPath)
	if err != nil || string(raw) != `{"state":"pend` {
		t.Errorf("corrupt marker must be left byte-identical; got %q err=%v", string(raw), err)
	}
}

// ---- agent-level transitions (watchdog / commit race) ----------------------

func testAgentFor(t *testing.T, exePath string) *Agent {
	t.Helper()
	return &Agent{cfg: Config{
		Logger:                slog.New(slog.DiscardHandler),
		UpgradeExecutablePath: exePath,
	}}
}

// asStagedImage marks the test agent as ACTUALLY RUNNING the staged bytes at
// exePath (external review F1): the commit proof hashes the process's running
// image, which in tests defaults to the go-test binary — i.e., "not the
// staged image". Legitimate-commit fixtures opt in explicitly; sibling
// fixtures deliberately do not.
func asStagedImage(a *Agent, exePath string) *Agent {
	a.cfg.UpgradeRunningImagePath = exePath
	a.upgradeBootProofID = "test-upgrade-id"
	return a
}

// origin: upgrade-safety plan §3.2 F5 — the register-deadline watchdog
// restores prev and execs it.
func TestWatchdogRollbackRestoresPrevAndExecs(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, 1, now.Add(-time.Second))

	a := testAgentFor(t, exePath)
	execed := ""
	a.cfg.UpgradeExecFn = func(p string) error { execed = p; return nil }
	a.watchdogRollback(exePath)

	if execed != exePath {
		t.Fatalf("watchdog must exec the restored dst; exec'd %q", execed)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("dst after watchdog rollback: %q, want prev content", string(got))
	}
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back", got.State)
	}
}

// origin: upgrade-safety plan §3.1 (定稿修订 R3) — commit and rollback are
// mutually exclusive; whichever transitions the marker first wins and the
// loser must no-op.
func TestCommitThenWatchdogIsNoOp(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, 1, now.Add(time.Minute))

	a := asStagedImage(testAgentFor(t, exePath), exePath)
	a.commitUpgradeAfterRegister(upgradeStateCommitted)
	if got := readMarkerState(t, markerPath); got.State != upgradeStateCommitted {
		t.Fatalf("commit did not land: marker state = %q", got.State)
	}

	a.cfg.UpgradeExecFn = func(string) error {
		t.Fatal("watchdog after a commit must not exec")
		return nil
	}
	a.watchdogRollback(exePath)
	if got, _ := os.ReadFile(exePath); string(got) != "NEW" {
		t.Errorf("committed binary must stay in place; got %q", string(got))
	}
	if got := readMarkerState(t, markerPath); got.State != upgradeStateCommitted {
		t.Errorf("marker state = %q, want committed", got.State)
	}
}

func TestWatchdogThenCommitIsNoOp(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, 1, now.Add(-time.Second))

	a := testAgentFor(t, exePath)
	a.cfg.UpgradeExecFn = func(string) error { return nil }
	a.watchdogRollback(exePath)
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Fatalf("watchdog rollback did not land: %q", got.State)
	}

	// The doomed register that raced the watchdog reports "committed" — it
	// must NOT resurrect the rolled-back marker, and it must NOT clear it
	// either (the rolled_back record still has to reach the broker).
	a.commitUpgradeAfterRegister(upgradeStateCommitted)
	got := readMarkerState(t, markerPath)
	if got == nil || got.State != upgradeStateRolledBack {
		t.Errorf("marker after racing commit: %+v, want intact rolled_back", got)
	}
}

// An ORDINARY register (nothing attached) must never clear a terminal marker
// that a later register still needs to deliver.
func TestOrdinaryRegisterDoesNotClearTerminalMarker(t *testing.T) {
	exePath, markerPath := bootFixture(t, "OLD", "", testMarker(upgradeStateRolledBack, 1, time.Now()))
	a := testAgentFor(t, exePath)
	a.commitUpgradeAfterRegister("")
	if got := readMarkerState(t, markerPath); got == nil || got.State != upgradeStateRolledBack {
		t.Errorf("terminal marker must survive an ordinary register; got %+v", got)
	}
}

// A register that DID deliver the terminal outcome clears it.
func TestDeliveredTerminalMarkerIsCleared(t *testing.T) {
	exePath, markerPath := bootFixture(t, "OLD", "", testMarker(upgradeStateRolledBack, 1, time.Now()))
	a := testAgentFor(t, exePath)
	a.commitUpgradeAfterRegister(upgradeStateRolledBack)
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Errorf("delivered terminal marker must be removed (err=%v)", err)
	}
}

// origin: upgrade-safety plan §3.2 F3 — a failed syscall.Exec is recovered IN
// PLACE by the still-running old process: prev restored over dst, marker
// rolled_back, NO exec and NO exit (the old image keeps serving).
func TestRecoverFromFailedExecRestoresPrevInPlace(t *testing.T) {
	now := time.Now()
	exePath, markerPath, _ := stagedMarkerFixture(t, 0, now.Add(time.Minute))

	a := testAgentFor(t, exePath)
	if !a.recoverFromFailedExec(exePath, os.ErrInvalid) {
		t.Fatal("recover must report handled=true for a pending marker")
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("dst after exec-failure recovery: %q, want prev content", string(got))
	}
	got := readMarkerState(t, markerPath)
	if got.State != upgradeStateRolledBack || !strings.Contains(got.Detail, "syscall.Exec") {
		t.Errorf("marker = %q/%q, want rolled_back with the exec-failure reason", got.State, got.Detail)
	}
}

// Without a pending marker (the ReExecOnly / cluster-upgrade leg stages no
// marker) disk recovery declines; reExecInPlace thaws and stays alive (S3 Q3).
func TestRecoverFromFailedExecDeclinesWithoutPendingMarker(t *testing.T) {
	exePath, _ := bootFixture(t, "STAGED", "", nil)
	a := testAgentFor(t, exePath)
	if a.recoverFromFailedExec(exePath, os.ErrInvalid) {
		t.Fatal("recover must decline when no pending marker exists (ReExecOnly leg stays alive on the thawed image)")
	}
	if got, _ := os.ReadFile(exePath); string(got) != "STAGED" {
		t.Errorf("dst must be untouched; got %q", string(got))
	}
}

// origin: upgrade-safety external re-review F12 — a host-lock open/flock
// failure is not evidence that no pending transaction exists. Returning false
// makes reExecInPlace call os.Exit(1), discarding the only still-running old
// process image precisely on the rollback path. Keep that process alive when
// serialization infrastructure itself is unavailable.
func TestRecoverFromFailedExecKeepsOldProcessOnLockFailure(t *testing.T) {
	exePath, _, _ := stagedMarkerFixture(t, 0, time.Now().Add(time.Minute))
	// OpenFile(O_RDWR) on a directory fails, deterministically injecting the
	// lock acquisition error without a production-only hook.
	if err := os.Mkdir(upgradeLockPath(exePath), 0o755); err != nil {
		t.Fatal(err)
	}
	a := testAgentFor(t, exePath)
	if keptAlive := a.recoverFromFailedExec(exePath, errors.New("exec format")); !keptAlive {
		t.Fatal("lock failure was treated as no pending upgrade; caller would exit the only live old process")
	}
}

// origin: internal review S8 — the F5 ARMING CHAIN end to end, which the
// direct watchdogRollback tests bypass: Run itself must read the pending
// marker, arm the deadline watchdog, and — with registration impossible
// (unreachable NATS) — fire it, restore prev, and exec the restored dst.
// Deleting the armUpgradeWatchdog call in Run turns exactly this test red.
// origin: S7 internal review GATE-CONVERGE-contracts F3
func TestRunArmsWatchdogAndRollsBackAtDeadline(t *testing.T) {
	// Capture baselines before deferring checks so the loop is visibly exercised
	// before the shared assertions run.
	// origin: S7 internal review GATE-CONVERGE-contracts F3
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "Run watchdog x5", beforeG)
	defer testharness.AssertNoFDLeak(t, "Run watchdog x5", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseRunUpgradeWatchdog(t)
	}
}
func exerciseRunUpgradeWatchdog(t *testing.T) {
	now := time.Now()
	exePath, markerPath, m := stagedMarkerFixture(t, 1, now.Add(300*time.Millisecond))

	// F1: the watchdog only arms for the instance the marker targets.
	m.TargetSID, m.TargetNID = "lab", "n1"
	requireUpgradeMarker(t, markerPath, m)
	execed := make(chan [2]string, 1)
	a, err := New(Config{
		NATSURL:               "nats://127.0.0.1:1", // reserved port: register can never succeed
		SID:                   "lab",
		NID:                   "n1",
		Logger:                slog.New(slog.DiscardHandler),
		UpgradeExecutablePath: exePath,
		// The marker is sampled exactly at the exec boundary, where the
		// candidate is still the running image.
		UpgradeExecFn: func(p string) error {
			state := ""
			if m, err := readUpgradeMarker(upgradeMarkerPath(p)); err == nil && m != nil {
				state = m.State
			}
			execed <- [2]string{p, state}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case got := <-execed:
		if got[0] != exePath {
			t.Errorf("watchdog exec'd %q, want the restored dst %q", got[0], exePath)
		}
		// origin: hot-update S8 plan §1 / §11.2 P17 mutant "preexec rolled_back";
		// S8 review rv-fault F7. Before the exec only recovery intent is durable.
		if got[1] != upgradeStatePending {
			t.Errorf("marker at the exec boundary = %q, want pending recovery intent", got[1])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the Run-armed watchdog never fired at the deadline")
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("dst after deadline rollback: %q, want prev content", string(got))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}
	// Run joined its watchdog. The injected exec returned, standing for the
	// restored image's boot convergence, which is what records rolled_back.
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back", got.State)
	}
}

// origin: internal review S21 — the commit/watchdog mutex under REAL
// contention (-race is the leak detector here; the serial two-order tests
// cannot see a dropped lock). Invariant: whatever interleaving wins, the
// final state is exactly one of committed(dst=NEW) or rolled_back(dst=OLD).
func TestCommitAndWatchdogRaceUnderContention(t *testing.T) {
	for i := 0; i < 20; i++ {
		now := time.Now()
		exePath, markerPath, _ := stagedMarkerFixture(t, 1, now.Add(-time.Second))

		a := asStagedImage(testAgentFor(t, exePath), exePath)
		a.cfg.UpgradeExecFn = func(string) error { return nil }
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a.commitUpgradeAfterRegister(upgradeStateCommitted) }()
		go func() { defer wg.Done(); a.watchdogRollback(exePath) }()
		wg.Wait()

		final := readMarkerState(t, markerPath)
		dst, _ := os.ReadFile(exePath)
		switch final.State {
		case upgradeStateCommitted:
			if string(dst) != "NEW" {
				t.Fatalf("round %d: committed but dst=%q", i, string(dst))
			}
		case upgradeStateRolledBack:
			if string(dst) != "OLD" {
				t.Fatalf("round %d: rolled_back but dst=%q", i, string(dst))
			}
		default:
			t.Fatalf("round %d: final marker state %q — the mutex let both (or neither) win", i, final.State)
		}
	}
}

// upgradeRegisterReport: the pending report only fires from the staged binary
// itself (self sha == new sha), and terminal states report verbatim.
func TestUpgradeRegisterReportTable(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name, state, dst, want, detail string
		boots                          int
		running                        bool
	}{
		{"pending_from_new_binary_reports_committed", upgradeStatePending, "NEW", upgradeStateCommitted, "", 1, true},
		{"pending_from_old_binary_reports_nothing", upgradeStatePending, "OLD", "", "", 1, false},
		{"pending_bootcount_zero_reports_nothing", upgradeStatePending, "NEW", "", "", 0, true},
		{"pending_without_process_boot_proof", upgradeStatePending, "NEW", "", "", 1, true},
		{"pending_from_different_running_inode", upgradeStatePending, "NEW", "", "", 1, true},
		{"rollback_failed_reports_with_detail", upgradeStateRollbackFailed, "NEW", upgradeStateRollbackFailed, "prev slot unusable", 1, false},
		{"rolled_back_reports_with_detail", upgradeStateRolledBack, "OLD", upgradeStateRolledBack, "register deadline exceeded", 1, false},
		{"no_marker_reports_nothing", "", "OLD", "", "", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exe, path := bootFixture(t, tc.dst, "OLD", nil)
			m := testMarker(tc.state, tc.boots, now.Add(time.Minute))
			m.NewSHA, m.Detail = "not-us", tc.detail
			if tc.running {
				m.NewSHA = mustSHA(t, exe)
			}
			if tc.state != "" {
				requireUpgradeMarker(t, path, m)
			}
			a := testAgentFor(t, exe)
			if tc.running {
				a = asStagedImage(a, exe)
			}
			if tc.name == "pending_without_process_boot_proof" {
				a.upgradeBootProofID = ""
			}
			if tc.name == "pending_from_different_running_inode" {
				a.cfg.UpgradeRunningImagePath = upgradePrevPath(exe)
			}
			state, detail := a.upgradeRegisterReport()
			if state != tc.want {
				t.Fatalf("report state = %q, want %q", state, tc.want)
			}
			if tc.want == upgradeStateCommitted && !strings.Contains(detail, "v0.1.0 -> v0.2.0") {
				t.Fatalf("report lost version transition: %q", detail)
			}
			if tc.detail != "" && detail != tc.detail {
				t.Fatalf("report detail = %q, want %q", detail, tc.detail)
			}
			// Isolate the BootCount guard from the other three commit proofs.
			// origin: internal review S1.
			if tc.running && tc.boots == 0 {
				a.commitUpgradeAfterRegister(upgradeStateCommitted)
				if got := readMarkerState(t, path); got.State != upgradeStatePending {
					t.Fatalf("never-booted marker committed as %q", got.State)
				}
			}
		})
	}
}

// origin: upgrade-safety external review F1 — requirements §3 explicitly
// permits multiple agent processes on one machine. They normally share the
// same installed tether path, marker and prev slot. BootCount is therefore a
// binary-wide fact, not proof that THIS Agent instance booted the staged image:
// after the targeted process increments it, an unrelated still-running old
// process hashes the replaced path (NEW) and can otherwise commit the marker.
func TestUpgradeCommitRequiresPerInstanceBootProofOnSharedBinary(t *testing.T) {
	exePath, markerPath, _ := stagedMarkerFixture(t, 1, time.Now().Add(time.Minute))

	// The boot_count=1 was produced by the targeted sibling process. This
	// second Agent represents another already-running session whose process
	// image never traversed BootUpgradeCheck/re-exec. Both instances resolve
	// the same on-disk executable path in production.
	unbootedSibling := testAgentFor(t, exePath)
	state, _ := unbootedSibling.upgradeRegisterReport()
	if state != "" {
		t.Fatalf("unbooted sibling reported %q from another process's boot_count; disk-path SHA is not process-image proof", state)
	}
	unbootedSibling.commitUpgradeAfterRegister(upgradeStateCommitted)
	if got := readMarkerState(t, markerPath); got.State != upgradeStatePending {
		t.Fatalf("unbooted sibling committed shared marker as %q; the real staged process's watchdog is now disarmed", got.State)
	}
}

// origin: upgrade-safety external re-review F9 — the production fallback on
// non-Linux systems hashes os.Executable(), i.e. the REPLACED pathname rather
// than this process's running inode. Model the flip→exec target process after
// a co-hosted boot has made BootCount non-zero: target identity and BootCount
// both match, but this old process has not re-execed. A path-based fallback is
// therefore still a borrowable proof and must not report committed.
func TestUpgradeCommitProofCannotUseReplacedPathFallback(t *testing.T) {
	exePath, markerPath, m := stagedMarkerFixture(t, 1, time.Now().Add(time.Minute))
	m.TargetSID, m.TargetNID = "lab", "n1"
	requireUpgradeMarker(t, markerPath, m)

	oldTarget := testAgentFor(t, exePath)
	oldTarget.cfg.SID, oldTarget.cfg.NID = "lab", "n1"
	oldTarget.cfg.UpgradeRunningImagePath = exePath // the current non-Linux fallback semantics
	if state, _ := oldTarget.upgradeRegisterReport(); state != "" {
		t.Fatalf("old target process borrowed replaced-path proof and reported %q", state)
	}
}

// origin: hot-update S8 plan §3.3 / §4.2 / M05 / M11.
// Real NATS requests exercise the register writer; the subscriber only supplies
// register acceptance, never control, migration or retirement evidence.
// origin: S7 internal review GATE-CONVERGE-contracts F3
func TestAgentRegisterKeepsUpgradePendingUntilControlReady(t *testing.T) {
	// Capture baselines before deferring checks so the loop is visibly exercised
	// before the shared assertions run.
	// origin: S7 internal review GATE-CONVERGE-contracts F3
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "agent register writer x5", beforeG)
	defer testharness.AssertNoFDLeak(t, "agent register writer x5", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseAgentRegisterUpgradeWriter(t, false)
	}
}

// origin: hot-update S8 plan §3.3 / M11.
// origin: S7 internal review GATE-CONVERGE-contracts F3
func TestAgentLateRegisterCannotUnlinkNextUpgrade(t *testing.T) {
	// Capture baselines before deferring checks so the loop is visibly exercised
	// before the shared assertions run.
	// origin: S7 internal review GATE-CONVERGE-contracts F3
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "agent register writer x5", beforeG)
	defer testharness.AssertNoFDLeak(t, "agent register writer x5", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseAgentRegisterUpgradeWriter(t, true)
	}
}

func exerciseAgentRegisterUpgradeWriter(t *testing.T, late bool) {
	t.Helper()
	t.Run("register", func(t *testing.T) {
		exe, path := bootFixture(t, "NEW", "OLD", nil)
		m := testMarker(upgradeStatePending, 1, time.Now().Add(time.Minute))
		m.NewSHA, m.PrevSHA = mustSHA(t, exe), mustSHA(t, upgradePrevPath(exe))
		m.TargetSID, m.TargetNID = "lab", "n1"
		if late {
			m.State = upgradeStateRolledBack
		}
		requireUpgradeMarker(t, path, m)
		nc, err := nats.Connect(startNATS(t))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		a, err := New(Config{NATSURL: nc.ConnectedUrl(), SID: "lab", NID: "n1", Logger: slog.New(slog.DiscardHandler), UpgradeExecutablePath: exe, UpgradeRunningImagePath: exe, UpgradeBootProofID: m.UpgradeID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nc.Subscribe(proto.SubjNodeRegister("lab", "n1"), func(msg *nats.Msg) {
			if late {
				next := *m
				next.UpgradeID = "next-upgrade"
				if err := writeUpgradeMarker(path, &next); err != nil {
					t.Error(err)
				}
			}
			_ = msg.Respond([]byte(`{"ok":true}`))
		}); err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		var delivery *upgradeMarker
		if _, err := a.register(t.Context(), nc, &delivery); err != nil {
			t.Fatal(err)
		}
		if late {
			a.ncBox.Store(nc)
			if _, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "n1", proto.ClaimProbeVerb), func(msg *nats.Msg) { replyClaimProbe(a, nc, msg) }); err != nil {
				t.Fatal(err)
			}
			if err := finishAgentControlReady(t.Context(), a, nc, delivery); err != nil {
				t.Fatal(err)
			}
			if current := readMarkerState(t, path); current == nil || current.UpgradeID != "next-upgrade" {
				t.Fatal("late report unlinked the next transaction")
			}
			next := *delivery
			next.UpgradeID, next.State = "next-upgrade", upgradeStatePending
			requireUpgradeMarker(t, path, &next)
			a.cfg.UpgradeExecFn = func(string) error { t.Error("late watchdog executed the next upgrade"); return errors.New("refused") }
			watchdogCapturedUpgrade(a, exe, delivery)
		}

		got := readMarkerState(t, path)
		if got == nil {
			t.Fatal("late register removed the next upgrade marker")
		}
		if late {
			if got.UpgradeID != "next-upgrade" || got.State != upgradeStatePending {
				t.Fatalf("next upgrade identity = %q", got.UpgradeID)
			}
		} else if got.State != upgradeStatePending {
			t.Fatalf("register without subscriptions committed %q", got.State)
		}
	})
}

// origin: hot-update S8 plan §3.3 / M11.
func TestAgentLegacyWritersPreserveV2Journal(t *testing.T) {
	for _, writer := range []string{"boot", "report", "commit", "watchdog", "recovery", "write", "install", "reexec"} {
		t.Run(writer, func(t *testing.T) {
			exe, path := bootFixture(t, "NEW", "OLD", nil)
			m := testMarker(upgradeStateRolledBack, 1, time.Now())
			if writer == "boot" || writer == "watchdog" || writer == "recovery" {
				m.State = upgradeStatePending
				m.NewSHA, m.PrevSHA = mustSHA(t, exe), mustSHA(t, upgradePrevPath(exe))
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			raw = append([]byte(`{"version":2,"schema_version":1,"entries":[],`), raw[1:]...)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			a := asStagedImage(testAgentFor(t, exe), exe)
			a.cfg.UpgradeExecFn = func(string) error { t.Error("legacy writer executed a v2 journal"); return errors.New("refused") }
			switch writer {
			case "boot":
				bootUpgradeCheck(exe, a.cfg.Logger, time.Now(), func(string) error { t.Fatal("v2 boot entered legacy exec"); return nil })
			case "report":
				if state, _ := a.upgradeRegisterReport(); state != "" {
					t.Fatalf("v2 journal reported as v1 %q", state)
				}
			case "commit":
				a.commitUpgradeAfterRegister(upgradeStateRolledBack)
			case "watchdog":
				a.watchdogRollback(exe)
			case "recovery":
				if a.recoverFromFailedExec(exe, errors.New("exec refused")) {
					t.Fatal("v2 journal claimed by legacy recovery")
				}
			case "install":
				if _, err := a.installNewBinary([]byte("invalid"), exe); err == nil {
					t.Fatal("legacy install accepted a v2 journal")
				}
			case "reexec":
				a.cfg.UpgradeExecFn = func(string) error { t.Error("legacy re-exec consumed a v2 journal"); return nil }
				a.reExecInPlace(exe, nil, true)
			case "write":
				if err := writeUpgradeMarker(path, m); err == nil {
					t.Fatal("legacy writer replaced v2 journal")
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(raw) {
				t.Fatalf("v2 evidence changed: read error=%v", err)
			}
		})
	}
}

func stagedMarkerFixture(t *testing.T, bootCount int, deadline time.Time) (string, string, *upgradeMarker) {
	t.Helper()
	exe, path := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, bootCount, deadline)
	m.NewSHA, m.PrevSHA = mustSHA(t, exe), mustSHA(t, upgradePrevPath(exe))
	requireUpgradeMarker(t, path, m)
	return exe, path, m
}

// origin: hot-update S8 plan §6.2 / M17.
// origin: S7 internal review GATE-CONVERGE-contracts F3
func TestAgentRegisterRejectsChangedIncarnation(t *testing.T) {
	// Capture baselines before deferring checks so the loop is visibly exercised
	// before the shared assertions run.
	// origin: S7 internal review GATE-CONVERGE-contracts F3
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "incarnation register x5", beforeG)
	defer testharness.AssertNoFDLeak(t, "incarnation register x5", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseAgentRegisterIncarnation(t)
	}
}
func exerciseAgentRegisterIncarnation(t *testing.T) {
	t.Helper()
	t.Run("inherited", func(t *testing.T) {
		nc, err := nats.Connect(startNATS(t))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		expected := strings.Repeat("a", 64)
		a, err := New(Config{NATSURL: nc.ConnectedUrl(), SID: "lab", NID: "n1", Logger: slog.New(slog.DiscardHandler)})
		if err != nil {
			t.Fatal(err)
		}
		a.incarnation.Store(&expected)
		if _, err := nc.Subscribe(proto.SubjNodeRegister("lab", "n1"), func(msg *nats.Msg) {
			var req struct {
				Incarnation string `json:"incarnation"`
			}
			if json.Unmarshal(msg.Data, &req) != nil || req.Incarnation != expected {
				t.Error("register lost inherited incarnation")
			}
			_ = msg.Respond([]byte(`{"ok":true,"incarnation":"` + strings.Repeat("b", 64) + `"}`))
		}); err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		_, err = a.register(t.Context(), nc)
		var refusal *upgradetx.Error
		if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeSessionIncarnationMismatch || agentIncarnation(a) != expected {
			t.Fatalf("recreated authority accepted: error=%v", err)
		}
	})
}

// origin: hot-update S8 plan §4.2 / M05 / M15.
// origin: S7 internal review GATE-CONVERGE-contracts F3
func TestAgentControlReadyRejectsStaleNonceAndOwner(t *testing.T) {
	// Capture baselines before deferring checks so the loop is visibly exercised
	// before the shared assertions run.
	// origin: S7 internal review GATE-CONVERGE-contracts F3
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "control probe x5", beforeG)
	defer testharness.AssertNoFDLeak(t, "control probe x5", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseAgentControlProbe(t)
	}
}
func exerciseAgentControlProbe(t *testing.T) {
	t.Helper()
	t.Run("nonce", func(t *testing.T) {
		nc, err := nats.Connect(startNATS(t))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		a, err := New(Config{NATSURL: nc.ConnectedUrl(), SID: "lab", NID: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		a.ncBox.Store(nc)
		a.setRunCtx(t.Context())
		for _, defect := range []string{"missing", "nonce", "instance", "generation", "current"} {
			a.cfg.Upgrade = &UpgradeDependencies{ControlProbe: func(_ context.Context, nonce string) (*nats.Msg, error) {
				if defect == "missing" {
					return nil, nil
				}
				instance, generation := a.instanceID, a.sessionGen.Load()
				if defect == "nonce" {
					nonce = upgradeNonce()
				}
				if defect == "instance" {
					instance = "wrong-owner"
				}
				if defect == "generation" {
					generation++
				}
				raw, err := json.Marshal(struct {
					Instance   string `json:"instance_id"`
					Nonce      string `json:"query_nonce"`
					Generation uint64 `json:"session_generation"`
				}{instance, nonce, generation})
				return &nats.Msg{Data: raw}, err
			}}
			if err := probeAgentControl(t.Context(), a, nc, upgradeNonce()); (err == nil) != (defect == "current") {
				t.Fatalf("probe %s error=%v", defect, err)
			}
		}
	})
}

func requireUpgradeMarker(t *testing.T, path string, marker *upgradeMarker) {
	t.Helper()
	if err := writeUpgradeMarker(path, marker); err != nil {
		t.Fatal(err)
	}
}
