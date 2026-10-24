//go:build linux

package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 internal review rv-fd rv-fd-3
func TestActivationCutBoundsOwnershipGuards(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			for _, guard := range []string{"manager", "cut"} {
				t.Run(guard, func(t *testing.T) {
					// origin: hot-update S5 PROC-LEAK / final process census.
					testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

					exerciseCutOwnershipDeadline(t, guard)
				})
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "activation cut ownership", beforeG)
	testharness.AssertNoFDLeak(t, "activation cut ownership", beforeFD, 0)
}

func exerciseCutOwnershipDeadline(t *testing.T, guard string) {
	a := cutTestAgent(t)
	domainStartPTY(t, a, []string{"/bin/sh", "-c", "stty raw -echo; printf 'before\n'; exec sleep 30"})
	entered, release, controlDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	if guard == "manager" {
		pid := a.workloads.Snapshot()[0].ID
		go func() {
			controlDone <- a.workloads.WithControl(pid, func(workload.Control) error {
				close(entered)
				<-release
				return nil
			})
		}()
		<-entered
	} else {
		a.cutMu.Lock()
	}
	done := make(chan error, 1)
	go func() { _, err := freezeForUpgrade(a, time.Now().Add(30*time.Millisecond)); done <- err }()
	late := false
	var err error
	select {
	case err = <-done:
	case <-time.After(300 * time.Millisecond):
		late = true
	}
	if guard == "manager" {
		close(release)
		if cerr := <-controlDone; cerr != nil {
			t.Fatal(cerr)
		}
	} else {
		a.cutMu.Unlock()
	}
	if late {
		err = <-done
	}
	if err == nil || late {
		t.Fatalf("activation cut ignored %s guard deadline: late=%t err=%v", guard, late, err)
	}
	if !a.cutMu.TryLock() {
		t.Fatal("failed ownership precheck retained cut guard")
	}
	a.cutMu.Unlock()
}

// origin: hot-update S4 internal review rv-fd rv-fd-4 (independent verifier)
func TestRunActivationFailureKeepsCurrentStateRecoveryReachable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseRunActivationFailureKeepsCurrentStateRecoveryReachable(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestRunActivationFailureKeepsCurrentStateRecoveryReachable", beforeG)
	testharness.AssertNoFDLeak(t, "TestRunActivationFailureKeepsCurrentStateRecoveryReachable", beforeFD, 0)
}

func exerciseRunActivationFailureKeepsCurrentStateRecoveryReachable(t *testing.T) {
	a := cutTestAgent(t)
	domainStartPTY(t, a, []string{"/bin/sh", "-c", "stty raw -echo; printf 'before\\n'; exec sleep 30"})
	// The helper uses one fixed request ID, so make the second creation directly.
	scope := cutTaskScope(a)
	probe, err := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "probe"})
	if err != nil || probe.Lease == nil {
		t.Fatal(err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "recovery-second"}
	if _, err := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "create", Handle: &h, AttachEpoch: 1, Run: &proto.RunReq{Argv: []string{"/bin/sh", "-c", "stty raw -echo; exec sleep 30"}, Cols: 80, Rows: 24}}); err != nil {
		t.Fatal(err)
	}
	m := cutPending(t, a)
	m.Deadline = time.Now().Add(time.Hour) // only activation recovery may cut this fixture
	if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
		t.Fatal(err)
	}
	oldProof := currentUpgradeBootProof()
	t.Cleanup(func() { rememberUpgradeBootProof(oldProof) })
	rememberUpgradeBootProof(m.UpgradeID)
	frozen := freezeBootAgent(t, a)
	img, src := cutBootImage(t, a, frozen)
	defer src.Close()
	if !ownsPendingBootMarker(m, img) {
		t.Fatal("fixture marker is not owned by this boot")
	}
	cfg := processRuntimeConfig(a)
	fault := errors.New("transient second actor activation")
	calls := 0
	var failedActor *ptysession.Actor
	cfg.PTYActivate = func(_ string, actor *ptysession.Actor) error {
		calls++
		if calls == 2 {
			failedActor = actor
		}
		if actor == failedActor {
			return fault
		}
		return actor.Activate()
	}
	imp, err := img.Restore(agentimage.Target{Runtime: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	a.processRuntime, a.image, a.imageGeneration = imp.Runtime, img, img.Generation()
	before, err := os.ReadFile(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var currentCut *agentimage.Bundle
	a.cfg.UpgradeCutObserver = func(b agentimage.Bundle) { currentCut = &b; cancel() }
	runErr := a.Run(ctx)
	if calls < 3 || img.Phase() != agentboot.PhaseActive || !imp.Active() || imp.Manager.Frozen() {
		t.Fatalf("partial-active ownership/retry: calls=%d phase=%s err=%v", calls, img.Phase(), runErr)
	}
	if currentCut == nil || currentCut.Lineage.Generation != img.Generation()+1 || len(currentCut.Runtime.Tasks) != 2 {
		t.Fatalf("partial activation bypassed current-state recovery: calls=%d cut=%v err=%v", calls, currentCut != nil, runErr)
	}
	redispatches := 0
	finishBoot(img, a.cfg.UpgradeExecutablePath, testharness.SilentLog(), runErr, func(string) error { redispatches++; return errors.New("test exec refused") })
	after, err := os.ReadFile(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Run returned partial-active error; FinishBoot redispatches=%d markerChanged=%t", redispatches, string(before) != string(after))
	if redispatches != 0 {
		t.Fatal("FinishBoot redispatched a stale active image")
	}
	if redispatches == 0 && string(before) == string(after) {
		t.Error("partial activation exits Run without current-state recovery or rollback_blocked marker")
	}
}
