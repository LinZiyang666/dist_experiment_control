//go:build linux

package cli_e2e_test

import (
	"debug/buildinfo"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S10 internal review RELEASE-MODE F1 / lead decision D-A
func TestUpgradeBudgetCandidateUsesReleaseBuild(t *testing.T) {
	buildTether(t)
	t.Run("release-budget", func(t *testing.T) {
		g, fd := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseUpgradeBudgetCandidateBuild(t)
		}
		testharness.AssertNoGoroutineLeak(t, "upgrade release build provenance", g)
		testharness.AssertNoFDLeak(t, "upgrade release build provenance", fd, 0)
	})
}

func exerciseUpgradeBudgetCandidateBuild(t *testing.T) {
	t.Helper()
	path, _ := buildUpgradeTransactionCandidate(t)
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["-race"] == "true" || settings["CGO_ENABLED"] != "0" || settings["-trimpath"] != "true" {
		t.Fatalf("budget candidate is not a release build: race=%q cgo=%q trimpath=%q", settings["-race"], settings["CGO_ENABLED"], settings["-trimpath"])
	}
}
