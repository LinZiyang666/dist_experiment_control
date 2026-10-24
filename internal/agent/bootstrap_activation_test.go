//go:build linux

package agent

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 internal review rv-fd rv-fd-4
// origin: S8 internal review FIX-AGENTRACE-B F2
func TestRunRetainsRecoveryAfterPartialPTYActivation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedAgentTest(t, "TestRunRetainsRecoveryAfterPartialPTYActivation") {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseRunRetainsRecoveryAfterPartialPTYActivation(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestRunRetainsRecoveryAfterPartialPTYActivation", beforeG)
	testharness.AssertNoFDLeak(t, "TestRunRetainsRecoveryAfterPartialPTYActivation", beforeFD, 0)
}

func exerciseRunRetainsRecoveryAfterPartialPTYActivation(t *testing.T) {
	a := newBootAgent(t, "nats://127.0.0.1:1", "node")
	scope := ptyTestScope(a)
	for _, request := range []string{"first", "second"} {
		h := ptyProbe(t, a, scope)
		h.RequestID = request
		q := ptyTaskRequest(scope, h, "create")
		q.Run = &proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "stty raw -echo; printf ready; exec cat"}}
		if r, err := a.processRuntime.Handle(scope, ownProcessRun(scope, q), "_TINBOX.pty"); err != nil || r.PID == "" {
			t.Fatalf("create: %v", err)
		}
		waitFor(t, bootTestBudget, func() bool {
			r, err := a.processRuntime.Handle(scope, ptyTaskRequest(scope, h, "resume"), "_TINBOX.pty")
			return err == nil && r.Phase == "running" && r.Stdout != nil && strings.Contains(string(r.Stdout.Data), "ready")
		})
	}
	f := freezeBootAgent(t, a)
	img, _ := cutBootImage(t, a, f)
	b := newBootAgent(t, "nats://127.0.0.1:1", "node")
	if err := b.processRuntime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("transient second activation failure")
	calls := 0
	cfg := processRuntimeConfig(b)
	cfg.PTYActivate = func(_ string, actor *ptysession.Actor) error {
		calls++
		if calls == 2 {
			return failure
		}
		return actor.Activate()
	}
	imp, err := img.Restore(agentimage.Target{Runtime: cfg, Spawn: b.spawnPolicy, Courier: NewCourierImage(b)})
	if err != nil {
		t.Fatal(err)
	}
	b.processRuntime, b.image, b.imageGeneration = imp.Runtime, img, img.Generation()
	adoptImageLineage(b, imp.Bundle.Lineage)
	exe, markerPath, marker := pendingBootMarker(t, b.instanceID, 1)
	b.cfg.UpgradeExecutablePath, b.cfg.UpgradeRunningImagePath = exe, exe
	b.upgradeBootProofID = marker.UpgradeID
	b.cfg.UpgradeExecFn = func(string) error { return errors.New("fixture rollback exec refused") }
	if err := f.Discard(); err != nil {
		t.Fatal(err)
	}
	// A recoverable activation fault must be handled before Run hands control
	// back to main. No network is reached in the buggy path.
	runCtx, cancel := context.WithTimeout(context.Background(), time.Second) // ctx-root: Run lifetime owned by this fixture
	defer cancel()
	err = b.Run(runCtx)
	if calls < 2 {
		t.Fatalf("activation fault was never reached: calls=%d err=%v", calls, err)
	}
	if calls != 3 || !img.Activated() || img.Phase() != agentboot.PhaseActive {
		t.Fatalf("transient activation was not completed before Run returned: calls=%d phase=%v activated=%t err=%v", calls, img.Phase(), img.Activated(), err)
	}
	if !imp.Active() || !imp.Runtime.Active() || imp.Manager.Frozen() {
		t.Fatal("successful retry lost active responsibility")
	}
	if got := readMarkerState(t, markerPath); got.ResourcePhase != markerPhaseActive {
		t.Fatalf("completed activation has no marker receipt: %+v", got)
	}
}
