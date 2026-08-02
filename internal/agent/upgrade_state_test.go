package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// origin: upgrade-safety plan §7 — table-driven coverage of the pure boot
// decision (plan §3.1), including the adversarial corners the review lenses
// asked for: deadline exactly reached, zero deadline, degenerate prev==new,
// budget at/over the line, and a hand-swapped binary.

func testMarker(state string, bootCount int, deadline time.Time) *upgradeMarker {
	return &upgradeMarker{
		State: state, PrevSHA: "prevsha", NewSHA: "newsha",
		PrevVersion: "v0.1.0", NewVersion: "v0.2.0",
		Deadline: deadline, BootCount: bootCount, BootBudget: upgradeBootBudget,
		UpgradeID: "test-upgrade-id",
	}
}

func TestDecideBootTable(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)

	cases := []struct {
		name string
		m    *upgradeMarker
		self string
		want bootAction
	}{
		{"no_marker", nil, "anything", bootProceed},
		{"committed_marker", testMarker(upgradeStateCommitted, 1, future), "newsha", bootProceed},
		{"rolled_back_marker", testMarker(upgradeStateRolledBack, 1, future), "prevsha", bootProceed},
		{"rollback_failed_marker", testMarker(upgradeStateRollbackFailed, 1, future), "x", bootProceed},

		{"pending_new_inside_budget_and_deadline", testMarker(upgradeStatePending, 0, future), "newsha", bootContinuePending},
		{"pending_new_last_budget_slot", testMarker(upgradeStatePending, upgradeBootBudget-1, future), "newsha", bootContinuePending},
		{"pending_new_budget_exhausted", testMarker(upgradeStatePending, upgradeBootBudget, future), "newsha", bootRollback},
		{"pending_new_budget_overshot", testMarker(upgradeStatePending, upgradeBootBudget+5, future), "newsha", bootRollback},
		{"pending_new_deadline_passed", testMarker(upgradeStatePending, 0, past), "newsha", bootRollback},
		// !now.Before(deadline): AT the deadline is already too late.
		{"pending_new_deadline_exactly_now", testMarker(upgradeStatePending, 0, now), "newsha", bootRollback},
		// A zero deadline (corrupt or hand-written marker) must fail toward
		// rollback, not toward an unbounded pending.
		{"pending_new_zero_deadline", testMarker(upgradeStatePending, 0, time.Time{}), "newsha", bootRollback},

		{"pending_self_is_prev", testMarker(upgradeStatePending, 0, future), "prevsha", bootConvergeRolledBack},
		// Converge wins even with the budget burned: we ARE the restored
		// binary; rolling back again would be a self-loop.
		{"pending_self_is_prev_budget_burned", testMarker(upgradeStatePending, 99, past), "prevsha", bootConvergeRolledBack},

		{"pending_self_matches_neither", testMarker(upgradeStatePending, 0, future), "handswapped", bootMarkOrphan},
	}
	// Degenerate re-upgrade to the identical binary: prev==new==self. The
	// new-sha branch must win (continue pending) — treating it as an
	// interrupted rollback would converge a perfectly healthy staging.
	degenerate := testMarker(upgradeStatePending, 0, future)
	degenerate.PrevSHA = "newsha"
	cases = append(cases, struct {
		name string
		m    *upgradeMarker
		self string
		want bootAction
	}{"pending_prev_equals_new_equals_self", degenerate, "newsha", bootContinuePending})

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decideBoot(c.m, c.self, now); got != c.want {
				t.Errorf("decideBoot = %q, want %q", got, c.want)
			}
		})
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
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, upgradeBootBudget, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, now.Add(-time.Second))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
func TestBootUpgradeCheckRollbackWithoutPrevIsTerminal(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "", nil) // prev deliberately absent
	m := testMarker(upgradeStatePending, upgradeBootBudget, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	execed := ""
	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })

	if execed != "" {
		t.Fatalf("a failed rollback must NOT exec (that would loop); exec'd %q", execed)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "NEW" {
		t.Errorf("dst must be left as-is on rollback failure; got %q", string(got))
	}
	got := readMarkerState(t, markerPath)
	if got.State != upgradeStateRollbackFailed {
		t.Errorf("marker state = %q, want rollback_failed", got.State)
	}
}

// A prev slot that exists but is NOT the recorded binary (tampered / clobbered)
// must also refuse the restore — blindly renaming it over dst would launch
// unverified bytes as the agent.
func TestBootUpgradeCheckRollbackWithWrongPrevShaIsTerminal(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "TAMPERED", nil)
	m := testMarker(upgradeStatePending, upgradeBootBudget, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = "sha-of-the-binary-that-was-actually-saved"
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	execed := ""
	bootUpgradeCheck(exePath, slog.New(slog.DiscardHandler), now, func(p string) error { execed = p; return nil })

	if execed != "" {
		t.Fatalf("tampered prev must not be exec'd; exec'd %q", execed)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "NEW" {
		t.Errorf("dst must be untouched; got %q", string(got))
	}
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRollbackFailed {
		t.Errorf("marker state = %q, want rollback_failed", got.State)
	}
}

func TestBootUpgradeCheckConvergesInterruptedRollback(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "OLD", "", nil) // we ARE the restored prev
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.PrevSHA = mustSHA(t, exePath)
	m.NewSHA = "sha-of-the-new-binary-that-was-rolled-away"
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "HAND-SWAPPED", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, now.Add(time.Minute))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(-time.Second))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(-time.Second))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	exePath, markerPath := bootFixture(t, "OLD", "", nil)
	m := testMarker(upgradeStateRolledBack, 1, time.Now())
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}
	a := testAgentFor(t, exePath)
	a.commitUpgradeAfterRegister("")
	if got := readMarkerState(t, markerPath); got == nil || got.State != upgradeStateRolledBack {
		t.Errorf("terminal marker must survive an ordinary register; got %+v", got)
	}
}

// A register that DID deliver the terminal outcome clears it.
func TestDeliveredTerminalMarkerIsCleared(t *testing.T) {
	exePath, markerPath := bootFixture(t, "OLD", "", nil)
	m := testMarker(upgradeStateRolledBack, 1, time.Now())
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}
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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
// marker) the recovery declines, preserving the historical exit-non-zero
// contract for that path.
func TestRecoverFromFailedExecDeclinesWithoutPendingMarker(t *testing.T) {
	exePath, _ := bootFixture(t, "STAGED", "", nil)
	a := testAgentFor(t, exePath)
	if a.recoverFromFailedExec(exePath, os.ErrInvalid) {
		t.Fatal("recover must decline when no pending marker exists (ReExecOnly leg keeps exit-non-zero)")
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
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, time.Now().Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}
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
func TestRunArmsWatchdogAndRollsBackAtDeadline(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(300*time.Millisecond))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	// F1: the watchdog only arms for the instance the marker targets.
	m.TargetSID, m.TargetNID = "lab", "n1"
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}
	execed := make(chan string, 1)
	a, err := New(Config{
		NATSURL:               "nats://127.0.0.1:1", // reserved port: register can never succeed
		SID:                   "lab",
		NID:                   "n1",
		Logger:                slog.New(slog.DiscardHandler),
		UpgradeExecutablePath: exePath,
		UpgradeExecFn:         func(p string) error { execed <- p; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case p := <-execed:
		if p != exePath {
			t.Errorf("watchdog exec'd %q, want the restored dst %q", p, exePath)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the Run-armed watchdog never fired at the deadline")
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("dst after deadline rollback: %q, want prev content", string(got))
	}
	if got := readMarkerState(t, markerPath); got.State != upgradeStateRolledBack {
		t.Errorf("marker state = %q, want rolled_back", got.State)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}
}

// origin: internal review S21 — the commit/watchdog mutex under REAL
// contention (-race is the leak detector here; the serial two-order tests
// cannot see a dropped lock). Invariant: whatever interleaving wins, the
// final state is exactly one of committed(dst=NEW) or rolled_back(dst=OLD).
func TestCommitAndWatchdogRaceUnderContention(t *testing.T) {
	for i := 0; i < 20; i++ {
		now := time.Now()
		exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
		m := testMarker(upgradeStatePending, 1, now.Add(-time.Second))
		m.NewSHA = mustSHA(t, exePath)
		m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}

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

	t.Run("pending_from_new_binary_reports_committed", func(t *testing.T) {
		exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
		m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
		m.NewSHA = mustSHA(t, exePath)
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
		state, detail := asStagedImage(testAgentFor(t, exePath), exePath).upgradeRegisterReport()
		if state != upgradeStateCommitted || !strings.Contains(detail, "v0.1.0 -> v0.2.0") {
			t.Errorf("report = %q/%q, want committed with the version transition", state, detail)
		}
	})

	t.Run("pending_from_old_binary_reports_nothing", func(t *testing.T) {
		exePath, markerPath := bootFixture(t, "OLD", "", nil)
		m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
		m.NewSHA = "not-us"
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
		if state, _ := testAgentFor(t, exePath).upgradeRegisterReport(); state != "" {
			t.Errorf("report = %q, want empty (we are not the staged binary)", state)
		}
	})

	// origin: internal review S1 — an INSTALL-time marker (BootCount 0: the
	// staged binary has never gone through decideBoot) must report nothing:
	// in the flip→exec window dst's sha already equals NewSHA, so without
	// the boot-count guard the OLD process's racing re-register would
	// fake-commit a binary that never booted.
	t.Run("pending_bootcount_zero_reports_nothing", func(t *testing.T) {
		exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
		m := testMarker(upgradeStatePending, 0, now.Add(time.Minute))
		m.NewSHA = mustSHA(t, exePath)
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
		// asStagedImage: isolate the BootCount guard — without it the
		// running-image check would also refuse and the S1 mutation would
		// have no dedicated red.
		a := asStagedImage(testAgentFor(t, exePath), exePath)
		if state, _ := a.upgradeRegisterReport(); state != "" {
			t.Errorf("report = %q, want empty for an install-time (never-booted) marker", state)
		}
		a.commitUpgradeAfterRegister(upgradeStateCommitted)
		if got := readMarkerState(t, markerPath); got.State != upgradeStatePending {
			t.Errorf("marker = %q, want still pending — a never-booted staging must not be committed", got.State)
		}
	})

	// origin: internal review S29 — the rollback_failed report leg.
	t.Run("rollback_failed_reports_with_detail", func(t *testing.T) {
		exePath, markerPath := bootFixture(t, "NEW", "", nil)
		m := testMarker(upgradeStateRollbackFailed, 1, now)
		m.Detail = "prev slot unusable"
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
		state, detail := testAgentFor(t, exePath).upgradeRegisterReport()
		if state != upgradeStateRollbackFailed || detail != "prev slot unusable" {
			t.Errorf("report = %q/%q", state, detail)
		}
	})

	t.Run("rolled_back_reports_with_detail", func(t *testing.T) {
		exePath, markerPath := bootFixture(t, "OLD", "", nil)
		m := testMarker(upgradeStateRolledBack, 1, now)
		m.Detail = "register deadline exceeded"
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			t.Fatal(err)
		}
		state, detail := testAgentFor(t, exePath).upgradeRegisterReport()
		if state != upgradeStateRolledBack || detail != "register deadline exceeded" {
			t.Errorf("report = %q/%q", state, detail)
		}
	})

	t.Run("no_marker_reports_nothing", func(t *testing.T) {
		exePath, _ := bootFixture(t, "OLD", "", nil)
		if state, _ := testAgentFor(t, exePath).upgradeRegisterReport(); state != "" {
			t.Errorf("report = %q, want empty", state)
		}
	})
}

// origin: upgrade-safety external review F1 — requirements §3 explicitly
// permits multiple agent processes on one machine. They normally share the
// same installed tether path, marker and prev slot. BootCount is therefore a
// binary-wide fact, not proof that THIS Agent instance booted the staged image:
// after the targeted process increments it, an unrelated still-running old
// process hashes the replaced path (NEW) and can otherwise commit the marker.
func TestUpgradeCommitRequiresPerInstanceBootProofOnSharedBinary(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

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
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 1, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.TargetSID, m.TargetNID = "lab", "n1"
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	oldTarget := testAgentFor(t, exePath)
	oldTarget.cfg.SID, oldTarget.cfg.NID = "lab", "n1"
	oldTarget.cfg.UpgradeRunningImagePath = exePath // the current non-Linux fallback semantics
	if state, _ := oldTarget.upgradeRegisterReport(); state != "" {
		t.Fatalf("old target process borrowed replaced-path proof and reported %q", state)
	}
}
