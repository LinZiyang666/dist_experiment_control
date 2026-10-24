//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Preserve the transfer/fault overlay in release and race arms alike. It owns
// original boundary custody and the adversarial cut, independently of build mode.
// origin: hot-update S10 lead decision D-A / T35 / T36 / T46
func buildTransferCandidate(t *testing.T, cmd *exec.Cmd, binary string) {
	t.Helper()
	if testharness.ReleaseBudgetRun(t) {
		flags := ""
		for _, item := range cmd.Env {
			if value, ok := strings.CutPrefix(item, "GOFLAGS="); ok {
				flags = value
			}
		}
		testharness.BuildReleaseCandidate(t, cmd.Dir, binary, proto.ReleaseVersion, flags)
		return
	}
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build transfer product: %v\n%s", err, output)
	}
}

// The real composed observer build must also be the release build in both
// executable generations; appending candidate identity must retain its metadata.
// origin: hot-update S10 lead decision D-A / T35 / T36 / T46
func TestTransferBudgetCandidateUsesReleaseBuild(t *testing.T) {
	t.Run("release-budget", func(t *testing.T) {
		a := buildAdvertisingArtifacts(t)
		g, fd := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseTransferBudgetBuild(t, a)
		}
		testharness.AssertNoGoroutineLeak(t, "transfer release build identity", g)
		testharness.AssertNoFDLeak(t, "transfer release build identity", fd, 0)
	})
}
func exerciseTransferBudgetBuild(t *testing.T, a transferArtifacts) {
	t.Helper()
	for _, path := range []string{a.a, a.b} {
		if _, err := testharness.CheckReleaseBudget(path, 0, time.Second); err != nil {
			t.Fatal(err)
		}
	}
}
