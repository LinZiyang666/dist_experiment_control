//go:build linux

package clusterharness

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real: the observer fixture's compiler adapter and executable build metadata.
// Simulated: a small main package; this does not certify broker behavior.
// origin: hot-update S10 lead decision D-A / sole-voter release build
func TestBrokerBudgetCandidateUsesReleaseBuild(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":             "module github.com/LinZiyang666/tether\n\ngo 1.26.0\n",
		"go.sum":             "",
		"cmd/tether/main.go": "package main\nfunc main(){}\n",
		"overlay.json":       "{\"Replace\":{}}",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "internal"), 0700); err != nil {
		t.Fatal(err)
	}
	// Initialize the process supervisor before taking its permanent baseline.
	if _, err := testharness.Output(exec.CommandContext(t.Context(), "go", "version")); err != nil {
		t.Fatal(err)
	}
	t.Run("release-budget", func(t *testing.T) {
		g, fd := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseBrokerBudgetBuild(t, root)
		}
		testharness.AssertNoGoroutineLeak(t, "broker release build adapter", g)
		testharness.AssertNoFDLeak(t, "broker release build adapter", fd, 0)
	})
}

func exerciseBrokerBudgetBuild(t *testing.T, root string) {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "candidate")
	if err := productCandidateBuild(t, t.Context(), root, binary, "source", filepath.Join(root, "overlay.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := testharness.CheckReleaseBudget(binary, 0, time.Second); err != nil {
		t.Fatal(err)
	}
}
