//go:build linux

package fullstackharness

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// The intermediate parent has already returned when the target observes the
// orphan. Only the launcher's shared subreaper can discharge its new wait duty.
const adoptedLauncherChild = `import ctypes, json, os, pathlib, signal, subprocess, sys, time
root = pathlib.Path(sys.argv[1])
subprocess.run([sys.executable, '-I', '-B', '-c', sys.argv[2], str(root)], check=True)
row = json.loads((root / 'grandchild.json').read_bytes())
path = pathlib.Path('/proc/%d/stat' % row['pid'])
fields = path.read_text().rsplit(')', 1)[1].split()
assert int(fields[19]) == row['start_ticks'], 'orphan identity changed'
assert int(fields[1]) == os.getppid(), 'orphan was not adopted by the shared reaper'
assert fields[0] not in ('Z', 'X'), 'adopted orphan was not alive'
libc = ctypes.CDLL(None, use_errno=True)
fd = libc.pidfd_open(row['pid'], 0)
assert fd >= 0, 'orphan pidfd open failed'
try:
    assert libc.pidfd_send_signal(fd, signal.SIGKILL, None, 0) == 0, 'orphan pidfd signal failed'
finally:
    os.close(fd)
deadline = time.monotonic() + 3
while path.exists() and time.monotonic() < deadline:
    time.sleep(.001)
assert not path.exists(), 'launcher did not reap its adopted zombie while target lived'
(root / 'lifetime-reaped').write_text('reaped before target exit')
sys.exit(23)
`

// origin: S10 internal review FIX-SUPERVISE F1
func TestFullstackSupervisorReapsAdoptedZombiesWhileTargetLives(t *testing.T) {
	launcher := buildReapingLauncher(t)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLauncherAdoptedZombie(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "launcher adopted zombie duty x5", g)
	testharness.AssertNoFDLeak(t, "launcher adopted zombie duty x5", f, 0)
}

func exerciseLauncherAdoptedZombie(t *testing.T, launcher string) {
	t.Helper()
	root := t.TempDir()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, launcher, "--", python, "-I", "-B", "-c", adoptedLauncherChild, root, escapedChild)
	out, err := CombinedOutput(cmd)
	var orphan ProcessIdentity
	readProcessReceipt(root, "grandchild", &orphan)
	defer emergencyReap(t, []ProcessIdentity{orphan})
	if ProcessExitCode(err) != 23 || bytes.Contains(out, []byte("surviving descendants")) {
		t.Fatalf("adopted wait duty changed target status: rc=%d err=%v\n%s", ProcessExitCode(err), err, out)
	}
	if body, err := os.ReadFile(filepath.Join(root, "lifetime-reaped")); err != nil || string(body) != "reaped before target exit" {
		t.Fatalf("target did not witness lifetime reaping: %q %v", body, err)
	}
	if state := recordedProcessState(orphan); state != "" {
		t.Fatalf("adopted orphan survived: %+v state=%s", orphan, state)
	}
}

// Direct children that the target leaves unwaited must retain their exact exit
// census. Unlike lifetime adoption, their wait duty belonged to the target.
const unwaitedLauncherChild = `import json, os, pathlib, sys, time
pid = os.fork()
if pid == 0:
    os._exit(0)
path = pathlib.Path('/proc/%d/stat' % pid)
deadline = time.monotonic() + 3
while time.monotonic() < deadline:
    fields = path.read_text().rsplit(')', 1)[1].split()
    if fields[0] == 'Z':
        row = dict(pid=pid, start_ticks=int(fields[19]),
                   pid_namespace=os.readlink('/proc/%d/ns/pid' % os.getpid()),
                   cgroup=pathlib.Path('/proc/%d/cgroup' % os.getpid()).read_text())
        (pathlib.Path(sys.argv[1]) / 'grandchild.json').write_text(json.dumps(row))
        os._exit(0)
    time.sleep(.001)
raise AssertionError('unwaited child did not become a zombie')
`

// origin: S10 internal review FIX-SUPERVISE F1
func TestFullstackSupervisorRetainsExitDescendantCensus(t *testing.T) {
	launcher := buildReapingLauncher(t)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLauncherExitCensus(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "launcher exit descendant census x5", g)
	testharness.AssertNoFDLeak(t, "launcher exit descendant census x5", f, 0)
}

func exerciseLauncherExitCensus(t *testing.T, launcher string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := filepath.Abs(filepath.Join("..", "simcluster", "drills", "lib", "upgrade_tx_supervisor.py"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, script, state string }{
		{"setsid survivor", escapedChild, ""},
		{"unwaited zombie", unwaitedLauncherChild, "Z"},
		{"inherited setsid survivor", escapedChild + "\nos.execv(sys.argv[2], [sys.argv[2], '--', '/bin/true'])\n", ""},
		{"python inherited setsid survivor", escapedChild + "\nos.execv(sys.executable, [sys.executable, '-I', '-B', sys.argv[2], 'run', '--timeout-s', '5', '--', '/bin/true'])\n", ""},
	} {
		root := t.TempDir()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		cmd := exec.CommandContext(ctx, launcher, "--", python, "-I", "-B", "-c", tc.script, root)
		if tc.name == "inherited setsid survivor" {
			cmd = exec.CommandContext(ctx, python, "-I", "-B", "-c", tc.script, root, launcher)
		}
		if tc.name == "python inherited setsid survivor" {
			cmd = exec.CommandContext(ctx, python, "-I", "-B", "-c", tc.script, root, adapter)
		}
		out, err := CombinedOutput(cmd)
		cancel()
		var orphan ProcessIdentity
		readProcessReceipt(root, "grandchild", &orphan)
		defer emergencyReap(t, []ProcessIdentity{orphan})
		if ProcessExitCode(err) != 125 || (!bytes.Contains(out, []byte("surviving descendants")) && !bytes.Contains(out, []byte("inherited descendants survived workload")) && !bytes.Contains(out, []byte("inherited live descendants required cleanup"))) {
			t.Fatalf("%s accepted: rc=%d err=%v\n%s", tc.name, ProcessExitCode(err), err, out)
		}
		if strings.Contains(tc.name, "inherited") && (cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 125) {
			t.Fatalf("%s relied on outer emergency cleanup: target=%v err=%v\n%s", tc.name, cmd.ProcessState, err, out)
		}
		identity := fmt.Sprintf("pid=%d start_ticks=%d state=", orphan.PID, orphan.StartTicks)
		if !strings.Contains(string(out), identity+tc.state) {
			t.Fatalf("%s lost exact descendant census: %s", tc.name, out)
		}
		if state := recordedProcessState(orphan); state != "" {
			t.Fatalf("%s survived cleanup: %+v state=%s", tc.name, orphan, state)
		}
	}
}

func buildReapingLauncher(t *testing.T) string {
	t.Helper()
	launcher := filepath.Join(t.TempDir(), "supervise")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", launcher, "./cmd/supervise")
	if out, err := CombinedOutput(cmd); err != nil {
		t.Fatalf("launcher build: %v\n%s", err, out)
	}
	return launcher
}

// A large POSIX here-document has a writer process. exec transfers that shell's
// existing wait duty to the launcher or Python adapter, before either starts its
// supervised workload. The outer reaper must not have to repair that duty.
// origin: S10 internal review FIX-SUPERVISE F1
func TestFullstackSupervisorReapsInheritedHeredocWriters(t *testing.T) {
	launcher := buildReapingLauncher(t)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLauncherInheritedWriter(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "inherited here-document wait duty x5", g)
	testharness.AssertNoFDLeak(t, "inherited here-document wait duty x5", f, 0)
}

func exerciseLauncherInheritedWriter(t *testing.T, launcher string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := filepath.Abs(filepath.Join("..", "simcluster", "drills", "lib", "upgrade_tx_supervisor.py"))
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("# Force a pipe-backed shell here-document writer.\n", 1024) + "print('workload-reached', flush=True)\n"
	for _, tc := range []struct{ name, script string }{
		{"launcher", "exec \"$1\" -- \"$2\" -I -B - <<'WORKLOAD'\n" + payload + "WORKLOAD\n"},
		{"python", "exec \"$2\" -I -B \"$3\" run --timeout-s 5 -- \"$2\" -I -B - <<'WORKLOAD'\n" + payload + "WORKLOAD\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", tc.script, "inherited-writer", launcher, python, adapter)
			out, err := CombinedOutput(cmd)
			cancel()
			if ProcessExitCode(err) != 0 || !bytes.Equal(out, []byte("workload-reached\n")) {
				t.Fatalf("inherited writer polluted workload result: rc=%d err=%v\n%s", ProcessExitCode(err), err, out)
			}
		})
	}
}
