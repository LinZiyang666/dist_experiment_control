//go:build linux

package testharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const reapingRole = "_TETHER_TEST_REAPING_ROLE"
const reapingRoot = "_TETHER_TEST_REAPING_ROOT"

// origin: S7 internal review FIX-ZOMBIE F1
func TestProcessSupervisionReapsAdoptedZombiesWhileTargetLives(t *testing.T) {
	role, root := os.Getenv(reapingRole), os.Getenv(reapingRoot)
	if role == "parent" {
		// This intermediate parent exits just as a revoked PTY leader does.
		// The private supervisor then owns the orphan's eventual wait duty.
		cmd := exec.Command("/bin/sleep", "60")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		writeSupervisionReceipt(filepath.Join(root, "orphan.json"), supervisionProcessIdentity(cmd.Process.Pid))
		return
	}
	if role == "target" {
		cmd := reapingCommand(t, t.Name(), "parent", root)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("intermediate parent: %v\n%s", err, out)
		}
		id := readReapingIdentity(t, root)
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", id.PID))
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
		if fields[1] != fmt.Sprint(os.Getppid()) || !supervisionIdentityAlive(id) {
			t.Fatal("fixture orphan was not adopted alive by the private supervisor")
		}
		if err := signalFixtureProcess(fixtureProcess{pid: id.PID, ticks: id.Ticks}); err != nil {
			t.Fatal(err)
		}
		if !WaitFor(t, 3*time.Second, time.Millisecond, func() bool { return supervisionIdentityState(id) == "" }) {
			t.Fatalf("supervisor did not reap its adopted zombie while target lived: pid=%d ticks=%s state=%s", id.PID, id.Ticks, supervisionIdentityState(id))
		}
		return
	}
	g, f := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdoptedZombieReaping(t)
	}
	AssertNoGoroutineLeak(t, "adopted zombie reaping x5", g)
	AssertNoFDLeak(t, "adopted zombie reaping x5", f, 0)
}

func exerciseAdoptedZombieReaping(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	cmd := reapingCommand(t, t.Name(), "target", root)
	if out, err := CombinedOutput(cmd); err != nil {
		t.Fatalf("adopted zombie reaping: %v\n%s", err, out)
	}
	id := readReapingIdentity(t, root)
	if state := supervisionIdentityState(id); state != "" {
		t.Fatalf("adopted descendant survived terminal census: pid=%d ticks=%s state=%s", id.PID, id.Ticks, state)
	}
}

// A zombie still parented by the target is a missed target wait, rather than
// the supervisor's ordinary adopted-child duty. Preserve its exit census even
// though the emergency reaper subsequently removes all kernel evidence.
// origin: S7 internal review FIX-ZOMBIE F1
func TestProcessSupervisionRetainsUnwaitedZombieCensus(t *testing.T) {
	if os.Getenv(reapingRole) == "boundary" {
		exerciseExitedTargetReaping(t)
		return
	}
	if os.Getenv(reapingRole) == "unwaited" {
		cmd := exec.Command("/bin/true")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		id := supervisionProcessIdentity(cmd.Process.Pid)
		writeSupervisionReceipt(filepath.Join(os.Getenv(reapingRoot), "orphan.json"), id)
		if !WaitFor(t, 3*time.Second, time.Millisecond, func() bool { return supervisionIdentityState(id) == "Z" }) {
			t.Fatal("unwaited child never became a zombie")
		}
		return // Deliberately leave this direct child's wait duty to the exit census.
	}
	g, f := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnwaitedZombieCensus(t)
	}
	AssertNoGoroutineLeak(t, "unwaited zombie census x5", g)
	AssertNoFDLeak(t, "unwaited zombie census x5", f, 0)
}

func exerciseUnwaitedZombieCensus(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	cmd := reapingCommand(t, t.Name(), "unwaited", root)
	out, err := CombinedOutput(cmd)
	id := readReapingIdentity(t, root)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 125 || cmd.ProcessState == nil || !cmd.ProcessState.Success() {
		t.Fatalf("unwaited zombie lost supervisor failure: target=%v err=%v\n%s", cmd.ProcessState, err, out)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("pid=%d start_ticks=%s state=Z", id.PID, id.Ticks)) {
		t.Fatalf("unwaited zombie lost exact census: %v", err)
	}
	if state := supervisionIdentityState(id); state != "" {
		t.Fatalf("reported zombie survived emergency reaping: pid=%d ticks=%s state=%s", id.PID, id.Ticks, state)
	}
	// Exercise the maintenance helper at a known exited-target boundary too;
	// scheduler selection between the exit channel and ticker cannot mask it.
	boundary := reapingCommand(t, t.Name(), "boundary", t.TempDir())
	if out, err := CombinedOutput(boundary); err != nil {
		t.Fatalf("target exit boundary: %v\n%s", err, out)
	}
}

func exerciseExitedTargetReaping(t *testing.T) {
	t.Helper()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	root := os.Getenv(reapingRoot)
	cmd := reapingCommand(t, t.Name(), "unwaited", root)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	wait := sync.OnceFunc(func() {
		if err := cmd.Wait(); err != nil {
			t.Errorf("exit-boundary target wait: %v", err)
		}
	})
	t.Cleanup(wait)
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	id := readReapingIdentity(t, root)
	if err := reapExitedSupervisorDescendants(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	if state := supervisionIdentityState(id); state != "Z" {
		t.Fatalf("maintenance erased an exited target's census evidence: pid=%d ticks=%s state=%s", id.PID, id.Ticks, state)
	}
	leaked := reapSupervisorDescendants(cmd.Process.Pid)
	if leaked == nil || !strings.Contains(leaked.Error(), fmt.Sprintf("pid=%d start_ticks=%s state=Z", id.PID, id.Ticks)) {
		t.Fatalf("exit boundary lost original zombie census: %v", leaked)
	}
	if state := supervisionIdentityState(id); state != "" {
		t.Fatalf("exit-boundary zombie survived reaping: pid=%d ticks=%s state=%s", id.PID, id.Ticks, state)
	}
	// Join while the test context is live. Cleanup runs after testing cancels it,
	// which would turn an already successful target into a CommandContext error.
	// Keep the cleanup fallback for failures before the census completes.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F3; plan L15
	wait()
}

func reapingCommand(t *testing.T, name, role, root string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.count=1")
	for _, entry := range supervisionEnvironment("", root) {
		if !strings.HasPrefix(entry, reapingRole+"=") && !strings.HasPrefix(entry, reapingRoot+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, reapingRole+"="+role, reapingRoot+"="+root)
	return cmd
}

func readReapingIdentity(t *testing.T, root string) supervisionIdentity {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, "orphan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var id supervisionIdentity
	if err := json.Unmarshal(body, &id); err != nil {
		t.Fatal(err)
	}
	return id
}
