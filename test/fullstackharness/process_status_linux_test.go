package fullstackharness

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S10 internal review rv-harness F5
func TestSupervisorFailureOutranksTargetStatus(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorStatusPrecedence(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor status precedence", g)
	testharness.AssertNoFDLeak(t, "supervisor status precedence", f, 0)
}

func exerciseSupervisorStatusPrecedence(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		script string
		code   int
	}{
		{"exit 0", 0}, {"exit 1", 1}, {"kill -9 $$", 137},
		// The parent releases the target only after the READY receipt, making
		// this a supervisor failure during ownership rather than failed startup.
		{"read release; kill -9 \"$PPID\"; exit 1", 125},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", tc.script)
		input, err := cmd.StdinPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		wait, err := StartProcess(cmd)
		if err != nil {
			_ = input.Close()
			cancel()
			t.Fatal(err)
		}
		if tc.code == 125 {
			if _, err := input.Write([]byte("release\n")); err != nil {
				cancel()
				_ = wait()
				t.Fatal(err)
			}
		}
		_ = input.Close()
		err = wait()
		cancel()
		if ProcessExitCode(err) != tc.code {
			t.Fatalf("native status changed: got=%d want=%d err=%v", ProcessExitCode(err), tc.code, err)
		}
		if tc.code == 125 && (cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 1 || !strings.Contains(err.Error(), "supervisor failed")) {
			t.Fatalf("supervisor failure lost target result or diagnosis: target=%v err=%v", cmd.ProcessState, err)
		}
	}
}

// origin: S10 internal review rv-harness F5
func TestSupervisorPreservesCleanupFailureWithFailedTarget(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorFailedTarget(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor diagnostic lifecycle", g)
	testharness.AssertNoFDLeak(t, "supervisor diagnostic lifecycle", f, 0)
}

func exerciseSupervisorFailedTarget(t *testing.T) {
	t.Helper()
	// ctx-root: bounded regression fixture owns the supervised process.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The shared reaper kills and waits for the deliberately orphaned sleep.
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "sleep 3 >/dev/null 2>&1 & exit 1")
	err := RunProcess(cmd)
	if code := ProcessExitCode(err); code != 125 {
		t.Fatalf("cleanup failure overwritten by target status: code=%d err=%v; want 125", code, err)
	}
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 1 {
		t.Fatalf("target status lost: %v", cmd.ProcessState)
	}
	if !strings.Contains(err.Error(), "descendant pid=") || !strings.Contains(err.Error(), "start_ticks=") {
		t.Fatalf("leak census lost: %v", err)
	}
	// Native classification must not depend on the order of joined errors.
	reversed := errors.Join(&exec.ExitError{ProcessState: cmd.ProcessState}, err)
	if ProcessExitCode(reversed) != 125 {
		t.Fatalf("joined target status hid supervision: %v", reversed)
	}
}

// origin: S10 internal review rv-harness F5
func TestSupervisorReportsAdoptedZombieAsLeak(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorZombie(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor diagnostic lifecycle", g)
	testharness.AssertNoFDLeak(t, "supervisor diagnostic lifecycle", f, 0)
}

func exerciseSupervisorZombie(t *testing.T) {
	t.Helper()
	// ctx-root: bounded regression fixture owns the supervised process.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	script := `import os, time
p = os.fork()
if p == 0:
    os._exit(0)
limit = time.monotonic() + 2
while time.monotonic() < limit:
    if open('/proc/%d/stat' % p).read().rsplit(')', 1)[1].split()[0] == 'Z':
        print(p, flush=True)
        os._exit(0)
    time.sleep(.001)
os.waitpid(p, 0)
os._exit(97)
`
	output, err := CombinedOutput(exec.CommandContext(ctx, python, "-B", "-c", script))
	if len(output) == 0 {
		t.Fatalf("zombie control did not establish its premise: %v", err)
	}
	// Successful emergency reap must still fail the fixture, never sign GREEN.
	if code := ProcessExitCode(err); code != 125 {
		t.Fatalf("unwaited zombie accepted: code=%d err=%v output=%q; want 125", code, err, output)
	}

	pid := strings.SplitN(string(output), "\n", 2)[0]
	if !strings.Contains(err.Error(), "pid="+pid+" ") || !strings.Contains(err.Error(), "state=Z") {
		t.Fatalf("zombie census lost: %v", err)
	}
	if _, statErr := os.Stat("/proc/" + pid); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("adopted zombie was not reaped: %s: %v", pid, statErr)
	}
}
