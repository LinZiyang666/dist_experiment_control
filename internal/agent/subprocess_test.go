package agent

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func TestMain(m *testing.M) {
	flag.Parse()
	parallelExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.parallel" {
			parallelExplicit = true
		}
	})
	// These parallel fixtures wait on independent I/O and real five-second
	// admission/probe budgets. Testing's GOMAXPROCS default serializes their
	// waits on a two-CPU worker, although they need almost no CPU. Keep a
	// bounded pool without changing GOMAXPROCS or an explicit caller limit.
	// origin: hot-update S4 plan §8.4; AGENT-LATENCY two-CPU measurements.
	if !parallelExplicit {
		if err := flag.Set("test.parallel", "16"); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// origin: hot-update S4 plan §8.4; AGENT-LATENCY measurements.
	// The race runtime adds a one-second exit sleep to every isolated probe,
	// spawn helper and reader. Their owners already join/reap before returning;
	// that sleep tests no lifecycle invariant and multiplies across five rounds.
	// The parent's runtime has already read GORACE: this changes descendants
	// only, retaining their instrumentation, diagnostics and failing exit codes.
	if err := os.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// Process-global clocks, boot hooks and lineage prevent these fixtures from
// sharing a parallel test process. Run the unchanged test body in its own
// process instead: its five-round leak baselines still cover every round, and
// the parent's parallel pool begins only after all serial leak gates finish.
// origin: hot-update S4 plan §8.4; docs/testing-standards.md §0.5 / §0.6.
func runIsolatedAgentTest(t *testing.T, name string) bool {
	t.Helper()
	const modeKey = "_TETHER_AGENT_TEST_HELPER"
	if os.Getenv(modeKey) == name {
		// Business children and version probes must not inherit test dispatch.
		if err := os.Unsetenv(modeKey); err != nil {
			t.Fatal(err)
		}
		return false
	}
	t.Parallel()
	out, err := isolatedAgentOutput(t, name, t.TempDir())
	if err != nil {
		t.Fatalf("isolated %s: %v\n%s", name, err, out)
	}
	// origin: hot-update S4 plan §8.4. A mistyped selector exits successfully
	// with no tests. Require the exact body's receipt before accepting its exit.
	if !bytes.Contains(out, []byte("=== RUN   "+name+"\n")) {
		t.Fatalf("isolated %s did not run its test body:\n%s", name, out)
	}
	t.Log(strings.TrimSpace(string(out)))
	return true
}

func isolatedAgentOutput(t *testing.T, name, dir string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	helper := filepath.Join(dir, "helper.test")
	if err := os.Link(os.Args[0], helper); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, helper, "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "TMPDIR="+dir, "_TETHER_AGENT_TEST_HELPER="+name)
	cmd.SysProcAttr = testProcessAttributes()
	cmd.WaitDelay = time.Second
	// The external supervisor owns adopted descendants even while this body
	// is running; a bare child leaves that wait duty in the shared test parent.
	// origin: S8 internal review FIX-AGENTRACE-A F1
	// PTYs establish their own sessions, so killing the runner's process
	// group cannot join their descendants after an early exit. The shared
	// supervisor retains that responsibility until its final child census.
	// origin: S8 internal review FIX-AGENTRACE-B F2 / S8 plan L21
	return testharness.CombinedOutput(cmd)
}
