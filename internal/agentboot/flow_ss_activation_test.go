//go:build linux

package agentboot

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §5.4 / §8 T34
// The boot responsibility boundary is shared by every domain. The existing
// S4 fixture supplies a real first actor that applies queued bytes and commits
// controls before a second actor fails. The live arm also advances an original
// flow and both SS nonces, then dispatches a current cut through another boot.
func TestFlowSSPartialActivationUsesCurrentCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFlowSSPartialActivation(t, round)
		})
		t.Run(fmt.Sprintf("live/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseLiveFlowSSRedispatch(t, round)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "flow-ss-boot-activation", beforeG)
	testharness.AssertNoFDLeak(t, "flow-ss-boot-activation", beforeFD, 0)
}

func exerciseFlowSSPartialActivation(t *testing.T, round int) {
	t.Helper()
	exercisePTYPartialActivation(t, round)
}

// origin: hot-update S5 plan §5.4 / T34
// Model only the boot phase at the preactivation scheduling boundary. Import
// and Detach themselves are real: without the guards, Repack succeeds and
// Abandon closes the frozen import while its activation owner still uses it.
func TestImageActivationOwnerExcludesPristineCleanup(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseActivationOwnerCleanup(t, round)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "boot-activation-owner", beforeG)
	testharness.AssertNoFDLeak(t, "boot-activation-owner", beforeFD, 0)
}

func exerciseActivationOwnerCleanup(t *testing.T, round int) {
	t.Helper()
	f := newCutFixture(t, fmt.Sprintf("activation-owner-%d", round))
	img, original, src := f.cut(t, 1)
	t.Cleanup(src.Close)
	target := f.target()
	target.Runtime.Manager = f.manager
	imp, err := img.Restore(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Discard(); err != nil {
		t.Fatal(err)
	}
	img.mu.Lock()
	img.activating = true
	img.mu.Unlock()
	prepared, err := img.Repack()
	if prepared != nil {
		_ = prepared.Close()
	}
	if err == nil || prepared != nil || img.Phase() != PhaseImported || img.Imported() != imp {
		t.Fatal("preactivation owner permitted pristine repack")
	}
	img.Abandon(nil, CrashWindowNoRollbackTarget, "preactivation in flight")
	if img.Phase() != PhaseImported || img.Imported() != imp {
		t.Fatal("preactivation owner permitted pristine abandon")
	}
	img.mu.Lock()
	img.activating = false
	img.mu.Unlock()
	// Once the driver relinquishes the attempt, the frozen import is still
	// pristine and must be usable for the ordinary salvage path.
	prepared, err = img.Repack()
	if err != nil {
		t.Fatalf("pristine repack after activation relinquished: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	img.Abandon(nil, CrashWindowNoRollbackTarget, "test finished")
}
