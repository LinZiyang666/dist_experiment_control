//go:build linux

package cli_e2e_test

import (
	"bytes"
	"context"
	"flag"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Every reliable-run test owns its process as well as its NATS ports, DB and
// homes. Five-round leak baselines therefore never count another test's live
// broker/relay, and process-global build/race fixtures remain serial locally.
// Re-exec this binary rather than spawning another go test entrypoint.
// origin: hot-update S4 plan §8.4; docs/testing-standards.md §0.5 / §0.6.
func runIsolatedReliableTest(t *testing.T) bool {
	t.Helper()
	return runIsolatedBinaryTest(t, 5*time.Minute)
}

// New binary suites retain their existing 600-second matrix ceiling while the
// original reliable-run callers keep their five-minute helper limit above.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
const binaryFixtureTestBudget = 10 * time.Minute

func runIsolatedBinaryTest(t *testing.T, budget time.Duration) bool {
	t.Helper()
	const modeKey = "_TETHER_CLI_TEST_HELPER"
	name := t.Name()
	if os.Getenv(modeKey) == name {
		// Neither business children nor probes may inherit test dispatch.
		if err := os.Unsetenv(modeKey); err != nil {
			t.Fatal(err)
		}
		return false
	}
	// Keep subtest filters and caller fixture/profiling flags on Go's original
	// execution path; an exact root selector cannot preserve those arguments.
	if !binaryFixtureIsolationAllowed(flag.CommandLine) {
		return false
	}
	t.Parallel()
	bin := buildTether(t)
	// Private HOME must not force variant/S3 builds to recompile/download the
	// entire Go toolchain graph. The build and module caches are immutable
	// inputs to these fixtures, separate from each product's isolated home.
	cacheEnv, err := testharness.Output(exec.Command("go", "env", "GOCACHE", "GOPATH"))
	if err != nil {
		t.Fatalf("read Go cache locations: %v", err)
	}
	cachePaths := strings.Split(strings.TrimSpace(string(cacheEnv)), "\n")
	if len(cachePaths) != 2 {
		t.Fatalf("unexpected Go cache locations: %q", cacheEnv)
	}
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	// Keep the isolated TMPDIR short: another t.TempDir below this directory
	// includes the root test name again, otherwise valid admin socket paths
	// exceed sockaddr_un before the real broker can start. The supervisor and
	// independent census still own every descendant before directory removal.
	// origin: S7 internal review TRANSFER-MAKETEST F4 / S7 plan lead L16
	// A RAM slot (/dev/shm/tNN) is as short as needed and keeps scratch off the
	// system SSD; any longer TMPDIR still falls back to /tmp for sockaddr_un.
	// origin: 2026-10-23 system-SSD wear remediation (4.5 TB written through /tmp)
	root := "/tmp"
	if scratch := os.TempDir(); len(scratch) <= len("/dev/shm/t00") {
		root = scratch
	}
	dir, err := os.MkdirTemp(root, "tcli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	testharness.GuardProcessDirectory(t, dir)
	helper := filepath.Join(dir, "helper.test")
	if err := os.Link(os.Args[0], helper); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, helper, "-test.run=^"+name+"$", "-test.count=1", "-test.parallel="+flag.Lookup("test.parallel").Value.String(), "-test.v")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "TMPDIR="+dir, modeKey+"="+name,
		"_TETHER_CLI_TEST_BINARY="+bin, "HOME="+t.TempDir(), "TETHER_HOME="+t.TempDir(),
		"GOCACHE="+cachePaths[0], "GOPATH="+cachePaths[1])
	// The shared supervisor owns timeout/parent-death cleanup, including
	// grandchildren; Setpgid preserves the existing explicit group signals.
	// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures.
	cmd.SysProcAttr = agentProcessAttributes()
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	out, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("isolated %s: %v\n%s", name, err, out)
	}
	// A misspelled selector exits successfully without exercising anything.
	// Accept only the exact body's receipt as well as its successful exit.
	// origin: hot-update S4 plan §8.4; CLI-LATENCY isolation dispatch.
	if !bytes.Contains(out, []byte("=== RUN   "+name+"\n")) || !bytes.Contains(out, []byte("--- PASS: "+name+" (")) {
		t.Fatalf("isolated %s did not complete its test body:\n%s", name, out)
	}
	t.Log(strings.TrimSpace(string(out)))
	return true
}

// Ordinary matrix flags preserve the exact child selection; every other
// invocation must retain its original runner and caller arguments.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
func binaryFixtureIsolationAllowed(options *flag.FlagSet) bool {
	isolate := len(options.Args()) == 0
	options.Visit(func(option *flag.Flag) {
		switch option.Name {
		case "test.run":
			if strings.Contains(option.Value.String(), "/") {
				isolate = false
			}
		case "test.count", "test.timeout", "test.parallel", "test.v", "test.paniconexit0", "test.testlogfile", "test.outputdir":
		default:
			isolate = false
		}
	})
	return isolate
}

// These cases keep a root-only child from silently dropping a requested case or
// a fixture argument. The flag sets are private and never alter the test runner.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
func TestFixtureIsolationKeepsCallerSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		isolate bool
	}{
		{"package", []string{"-test.count=1", "-test.timeout=600s", "-test.parallel=8", "-test.v=test2json", "-test.paniconexit0=true", "-test.testlogfile=/tmp/testlog", "-test.outputdir=/tmp"}, true},
		{"root", []string{"-test.run=^TestExample$"}, true},
		{"subtest", []string{"-test.run=^TestExample$/^child$"}, false},
		{"slash-class", []string{"-test.run=^Test[AB/]$"}, false},
		{"fixture", []string{"-fixture-root=/fixture"}, false},
		{"short", []string{"-test.short=true"}, false},
		{"skip", []string{"-test.skip=child"}, false},
		{"cpu", []string{"-test.cpu=2,4"}, false},
		{"shuffle", []string{"-test.shuffle=on"}, false},
		{"profile", []string{"-test.cpuprofile=/tmp/profile"}, false},
		{"failfast", []string{"-test.failfast=true"}, false},
		{"positional", []string{"fixture-mode"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := flag.NewFlagSet("fixture", flag.ContinueOnError)
			for _, name := range []string{"test.run", "test.count", "test.timeout", "test.parallel", "test.v", "test.paniconexit0", "test.testlogfile", "test.outputdir", "test.short", "test.skip", "test.cpu", "test.shuffle", "test.cpuprofile", "test.failfast", "fixture-root"} {
				options.String(name, "", "")
			}
			if err := options.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if got := binaryFixtureIsolationAllowed(options); got != tc.isolate {
				t.Fatalf("isolation=%t, want %t for %v", got, tc.isolate, tc.args)
			}
		})
	}
}

// This deliberately long root name reproduces the nested directory geometry
// of the transfer image suite. An actual Unix bind verifies the kernel limit;
// merely checking a shorter root string would miss the child's nested path.
// origin: S7 internal review TRANSFER-MAKETEST F4 / S7 plan lead L16
func TestFixtureIsolationKeepsUnixSocketPathsWithinKernelLimit(t *testing.T) {
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFixtureUnixSocket(t)
	}
	testharness.AssertNoGoroutineLeak(t, "isolated fixture Unix socket", g)
	testharness.AssertNoFDLeak(t, "isolated fixture Unix socket", fd, 0)
}

func exerciseFixtureUnixSocket(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	listener, err := net.Listen("unix", filepath.Join(dir, "admin.sock"))
	if err != nil {
		t.Fatalf("isolated child cannot bind its private admin socket: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

// A fresh Go telemetry directory reproduces the detached uploader from even a
// go env probe. The original supervisor must remain strict; fixture Go tools
// instead need private configuration that cannot start that unrelated service.
// origin: S10 internal review POLISH-B F2 / plan L15
func TestFixtureIsolationKeepsGoTelemetryWithinSandbox(t *testing.T) {
	const mode = "_TETHER_CLI_TELEMETRY_CASE"
	if os.Getenv(mode) == "driver" {
		if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
			return
		}
		probe := exec.Command("go", "env", "GOTELEMETRY")
		out, err := testharness.Output(probe)
		if err != nil || strings.TrimSpace(string(out)) != "off" {
			t.Fatalf("fixture Go tools retained external telemetry: %v %q", err, out)
		}
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFixtureTelemetrySandbox(t)
	}
	testharness.AssertNoGoroutineLeak(t, "fixture telemetry sandbox", g)
	testharness.AssertNoFDLeak(t, "fixture telemetry sandbox", fd, 0)
}

func exerciseFixtureTelemetrySandbox(t *testing.T) {
	t.Helper()
	bin := buildTether(t)
	dir := testharness.GuardProcessDirectory(t, t.TempDir())
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.v")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "_TETHER_CLI_TELEMETRY_CASE=driver", "TEST_TELEMETRY_DIR="+dir, "_TETHER_CLI_TEST_BINARY="+bin)
	out, err := testharness.CombinedOutput(cmd)
	if err != nil || !bytes.Contains(out, []byte("--- PASS: "+t.Name()+" (")) {
		t.Fatalf("isolated fixture did not confine its Go tools: %v\n%s", err, out)
	}
}
