//go:build linux

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
)

// Execute the complete dispatcher with protocol fixtures. Every entry requires
// the actual selected launcher binding, so a missing alias cannot hide behind
// a passing script that happens not to consume it.
// origin: S10 internal review FIX-SUPERVISE F2
func TestRunAllBindsFullstackSupervisor(t *testing.T) {
	launcher := filepath.Join(t.TempDir(), "supervise")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", launcher, ".")
	if out, err := fullstackharness.CombinedOutput(build); err != nil {
		t.Fatalf("launcher build: %v\n%s", err, out)
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "simcluster", "tests", "run-all.sh"))
	if err != nil {
		t.Fatal(err)
	}
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRunAllLauncherBinding(t, launcher, string(source))
	}
	testharness.AssertNoGoroutineLeak(t, "dispatcher launcher binding x5", g)
	testharness.AssertNoFDLeak(t, "dispatcher launcher binding x5", f, 0)
}

func exerciseRunAllLauncherBinding(t *testing.T, launcher, source string) {
	t.Helper()
	root := t.TempDir()
	dispatcher := filepath.Join(root, "run-all.sh")
	if err := os.WriteFile(dispatcher, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	loop := regexp.MustCompile(`(?m)^for t in (.+); do$`).FindStringSubmatch(source)
	if len(loop) != 2 {
		t.Fatal("dispatcher entry set missing")
	}
	names := strings.Fields(loop[1])
	for _, match := range regexp.MustCompile(`"\$HERE/([A-Za-z0-9_.-]+)\.sh"`).FindAllStringSubmatch(source, -1) {
		names = append(names, match[1])
	}
	for _, name := range names {
		fixture := "#!/bin/sh\n" +
			"[ -n \"${FULLSTACK_SUPERVISE:-}\" ] && [ \"$FULLSTACK_SUPERVISE\" = \"$SIM_UPGRADE_SUPERVISOR\" ] || {\n" +
			"    printf '%s\\n' 'selected launcher binding missing' >&2; exit 2;\n}\n"
		if err := os.WriteFile(filepath.Join(root, name+".sh"), []byte(fixture), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", dispatcher)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "SIM_UPGRADE_SUPERVISOR=") && !strings.HasPrefix(entry, "FULLSTACK_SUPERVISE=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "SIM_UPGRADE_SUPERVISOR="+launcher)
	out, err := fullstackharness.CombinedOutput(cmd)
	if err != nil || !strings.Contains(string(out), "simcluster hermetic gates: ALL PASS") {
		t.Fatalf("selected launcher was not bound throughout dispatch: %v\n%s", err, out)
	}
	for _, name := range names {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `(?: --check)?\s+PASS$`).Match(out) {
			t.Errorf("dispatcher did not verify launcher binding for %s: %s", name, out)
		}
	}
}
