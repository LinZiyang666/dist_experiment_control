//go:build linux

// Real: Manager barriers, Linux processes, session/PGID changes, stopped jobs,
// and pidfd signals. No agent, broker, or protocol authority is simulated here.
package workload_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §3.4 / §4.1 / §6.2 / T15
func TestProcessControlGuardAndSessionCleanup(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Getenv(sessionControlMode) == "scan-failure" {
		sessionCleanupScanFailure(t)
		return
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			t.Run("control_guard", func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseControlGuard(t)
			})
			t.Run("identity_refusal", exerciseControlIdentityRefusal)
			t.Run("session_members", exerciseSessionMembers)
			t.Run("scan_failure_retains_pending", exerciseSessionScanFailure)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "process control and session cleanup", goroutines)
	testharness.AssertNoFDLeak(t, "process control and session cleanup", fds, 0)
}

func exerciseControlGuard(t *testing.T) {
	t.Helper()
	var manager workload.Manager
	child := startProcessFixture(t, 23)
	id := imageChildIdentity(t, processStateEnvelope(t), child, "controlled")
	if err := manager.Adopt(id, child.process); err != nil {
		t.Fatal(err)
	}
	child.owner, child.id = &manager, id.ID
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	entered, returned := make(chan error, 1), make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	receiptError := errors.New("committed control receipt")
	waited := observeExit(&manager, id.ID)
	go func() {
		returned <- manager.WithControl(id.ID, func(control workload.Control) error {
			if control.Identity != id || control.Exit != nil {
				entered <- fmt.Errorf("wrong live control identity: %+v", control)
				return receiptError
			}
			entered <- control.Signal(syscall.SIGKILL)
			<-release // Force Freeze to contend between Signal and Close/receipt.
			if err := reader.Close(); err != nil {
				return err
			}
			return receiptError
		})
	}()
	select {
	case err := <-entered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(processTestBudget):
		t.Fatal("control callback did not start")
	}
	waitUntilKernelChildWaitable(t, child.pid)
	// The deadline must include time waiting for this callback, not merely
	// work after a blocking Lock finally returns. Release on every failure.
	deadline := time.Now().Add(40 * time.Millisecond)
	timed := make(chan error, 1)
	go func() { _, err := manager.FreezeUntil(deadline); timed <- err }()
	select {
	case err := <-timed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("freeze crossed an unfinished control: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FreezeUntil ignored the deadline while acquiring its guard")
	}
	frozen := make(chan error, 1)
	go func() {
		_, err := manager.Freeze()
		if err == nil {
			if _, closeErr := reader.Stat(); !errors.Is(closeErr, os.ErrClosed) {
				err = fmt.Errorf("Freeze exported Signal before Close/receipt: %v", closeErr)
			}
		}
		frozen <- err
	}()
	select {
	case result := <-waited:
		t.Fatalf("Wait consumed exit inside a control transaction: %+v", result)
	case err := <-frozen:
		t.Fatalf("Freeze completed inside a control transaction: %v", err)
	case <-time.After(40 * time.Millisecond): // Negative observation, not readiness.
	}
	unblock()
	if err := <-returned; !errors.Is(err, receiptError) {
		t.Fatalf("callback receipt lost: %v", err)
	}
	if err := <-frozen; err != nil {
		t.Fatal(err)
	}
	called := false
	if err := manager.WithControl(id.ID, func(workload.Control) error { called = true; return nil }); !errors.Is(err, workload.ErrFrozen) || called {
		t.Fatalf("frozen callback executed: called=%t error=%v", called, err)
	}
	manager.Thaw()
	result := receiveExit(t, waited)
	child.done = true
	if result.err != nil || result.exit.Signal != int(syscall.SIGKILL) {
		t.Fatalf("control lost real kernel exit: %+v", result)
	}
	if err := manager.WithControl(id.ID, func(control workload.Control) error {
		if control.Exit == nil || control.Exit.Signal != int(syscall.SIGKILL) {
			return fmt.Errorf("terminal callback lost exit")
		}
		control.Exit.Code = 123 // An actor must not be able to edit Manager's result.
		return control.Signal(syscall.SIGKILL)
	}); !errors.Is(err, workload.ErrExited) {
		t.Fatalf("terminal control reacquired numeric PID authority: %v", err)
	}
	if row, _ := manager.Lookup(id.ID); row.Exit.Code != -1 {
		t.Fatal("terminal control aliases Manager exit")
	}
	if err := manager.WithControl("unknown", func(workload.Control) error { called = true; return nil }); !errors.Is(err, workload.ErrUnknown) || called {
		t.Fatalf("unknown callback executed: called=%t error=%v", called, err)
	}
	if _, err := manager.FreezeUntil(time.Now().Add(-time.Second)); !errors.Is(err, os.ErrDeadlineExceeded) || manager.Frozen() {
		t.Fatalf("expired freeze mutated ownership: %v", err)
	}
	if _, err := manager.FreezeUntil(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.FreezeUntil(time.Now().Add(time.Second)); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("second deadline freeze: %v", err)
	}
	manager.Thaw()
}

func exerciseControlIdentityRefusal(t *testing.T) {
	t.Helper()
	for _, field := range []string{"boot", "ticks"} {
		t.Run(field, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final subtest process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var manager workload.Manager
			child := startProcessFixture(t, 23)
			id := imageChildIdentity(t, processStateEnvelope(t), child, "wrong-identity")
			if field == "boot" {
				id.BootID += "-wrong"
			} else {
				id.StartTimeTicks++
			}
			if err := manager.Adopt(id, child.process); err != nil {
				t.Fatal(err)
			}
			child.owner, child.id = &manager, id.ID
			called := false
			if err := manager.WithControl(id.ID, func(c workload.Control) error { called = true; return c.Signal(syscall.SIGKILL) }); err == nil || called {
				t.Fatalf("wrong identity reached callback: called=%t error=%v", called, err)
			}
			child.finish(t)
			result := receiveExit(t, observeExit(&manager, id.ID))
			child.done = true
			if result.err != nil || result.exit.Code != 23 {
				t.Fatalf("identity refusal touched child: %+v", result)
			}
		})
	}
}

// Each descendant keeps a different PGID. The leader reaps them while alive;
// same-PID exec helpers additionally act as subreapers after killing the leader.
// Readiness is an atomic marker, and SIGSTOP is observed through /proc before
// cleanup starts. The detached member is an explicit boundary counterexample.
const sessionFixtureScript = `
import json, os, signal, sys, time
root = sys.argv[1]
signal.signal(signal.SIGHUP, signal.SIG_IGN)
def reap(*args):
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0: return
        except ChildProcessError: return
children = []
for kind in ('running', 'stopped', 'escaped'):
    pid = os.fork()
    if pid == 0:
        if kind == 'escaped':
            os.setsid()
            for fd in (0, 1, 2): os.close(fd)
        else: os.setpgid(0, 0)
        if kind == 'stopped': os.kill(os.getpid(), signal.SIGSTOP)
        while True: signal.pause()
    children.append(pid)
    while os.getpgid(pid) != pid: time.sleep(0.001)
    if kind == 'escaped':
        while os.getsid(pid) != pid: time.sleep(0.001)
if sys.stdin.isatty(): os.tcsetpgrp(0, children[1])
with open(root + '/ready.tmp', 'w') as f:
    json.dump({'leader': os.getpid(), 'members': children, 'environment': os.getenv('TETHER_TEST_SESSION_CONTROL_MODE', '') + os.getenv('_TETHER_HANDOFF', '')}, f)
os.rename(root + '/ready.tmp', root + '/ready')
while True:
    reap()
    time.sleep(0.005)
`

func exerciseSessionMembers(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	cmd := exec.Command("python3", "-c", sessionFixtureScript, root)
	cmd.Env = sessionControlEnvironment("")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var manager workload.Manager
	id := imageChildIdentity(t, processStateEnvelope(t), &processFixture{pid: cmd.Process.Pid}, "session")
	if err := manager.AdoptDraining(id, cmd.Process); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatal(err)
	}
	var fixture sessionControlProcesses
	t.Cleanup(func() {
		// Kill descendants while their real parent can still reap them. This
		// fallback is independent of the helpers being mutation-tested.
		for _, pid := range fixture.Members {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		for _, pid := range fixture.Members {
			waitSessionCondition(t, "fixture descendant reaped", func() bool { _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); return errors.Is(err, os.ErrNotExist) })
		}
		manager.Thaw()
		_ = manager.Signal(id.ID, syscall.SIGKILL)
		_ = manager.AllowReap(id.ID)
		_ = receiveExit(t, observeExit(&manager, id.ID))
	})
	readSessionJSON(t, filepath.Join(root, "ready"), &fixture)
	if fixture.Leader != id.OSPID || len(fixture.Members) != 3 || fixture.Environment != "" {
		t.Fatalf("invalid real session fixture: %+v", fixture)
	}
	waitSessionCondition(t, "stopped foreground job", func() bool { return sessionProcessState(t, fixture.Members[1]) == "T" })
	for _, pid := range fixture.Members {
		if pgid, err := syscall.Getpgid(pid); err != nil || pgid != pid {
			t.Fatalf("fixture did not establish independent PGID: pid=%d pgid=%d error=%v", pid, pgid, err)
		}
	}
	members, err := workload.ScanSession(id)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]bool{id.OSPID: true, fixture.Members[0]: true, fixture.Members[1]: true}
	if len(members) != len(want) {
		t.Fatalf("census missed other PGIDs or included setsid escape: %+v", members)
	}
	for _, member := range members {
		if !want[member.Identity.OSPID] || member.SessionID != id.OSPID || member.Identity.BootID != id.BootID || member.Identity.StartTimeTicks <= 0 {
			t.Fatalf("wrong member identity: %+v", member)
		}
		delete(want, member.Identity.OSPID)
		for _, field := range []string{"boot", "ticks", "sid"} {
			wrong := member
			switch field {
			case "boot":
				wrong.Identity.BootID += "-wrong"
			case "ticks":
				wrong.Identity.StartTimeTicks++
			case "sid":
				wrong.SessionID++
			}
			// Keep the descendant reaper alive even when identity verification is
			// deliberately removed by a mutation. Descendants use the lethal
			// signal; the leader refusal uses signal 0 with the same predicate.
			sig := syscall.SIGKILL
			if member.Identity.OSPID == id.OSPID {
				sig = 0
			}
			if err := workload.SignalIdentity(wrong, sig); err == nil {
				t.Fatalf("%s mismatch signalled live PID %d", field, member.Identity.OSPID)
			}
			if state := sessionProcessState(t, member.Identity.OSPID); state == "Z" || state == "X" {
				t.Fatalf("%s identity refusal killed live process", field)
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("census duplicated identities: %v", want)
	}
	for _, field := range []string{"boot", "ticks", "missing"} {
		wrong := id
		switch field {
		case "boot":
			wrong.BootID += "-wrong"
		case "ticks":
			wrong.StartTimeTicks++
		case "missing":
			wrong.OSPID = 1 << 30
		}
		if rows, err := workload.ScanSession(wrong); err == nil || len(rows) != 0 {
			t.Fatalf("failed census became an empty-success receipt: rows=%v error=%v", rows, err)
		}
	}
	for _, member := range members {
		if member.Identity.OSPID != id.OSPID {
			if err := workload.SignalIdentity(member, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, pid := range fixture.Members[:2] {
		waitSessionCondition(t, "cross-PGID member killed and reaped", func() bool { _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); return errors.Is(err, os.ErrNotExist) })
	}
	if state := sessionProcessState(t, fixture.Members[2]); state == "Z" || state == "X" {
		t.Fatal("cleanup crossed the documented setsid boundary")
	}
	remaining, err := workload.ScanSession(id)
	if err != nil || len(remaining) != 1 || remaining[0].Identity.OSPID != id.OSPID {
		t.Fatalf("session cleanup left live members: %+v error=%v", remaining, err)
	}
	// Remove the out-of-scope member explicitly before killing its reaper.
	_ = syscall.Kill(fixture.Members[2], syscall.SIGKILL)
	waitSessionCondition(t, "escaped fixture reaped", func() bool {
		_, err := os.Stat(fmt.Sprintf("/proc/%d", fixture.Members[2]))
		return errors.Is(err, os.ErrNotExist)
	})
	if err := manager.Signal(id.ID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitUntilKernelChildWaitable(t, id.OSPID)
	for census := 0; census < 2; census++ {
		rows, err := workload.ScanSession(id)
		if err != nil || len(rows) != 0 {
			t.Fatalf("held zombie prevented empty cleanup census: %+v %v", rows, err)
		}
	}
	fixture.Members = nil // Already reaped; the fallback must not reuse their PIDs.
}

func waitSessionCondition(t *testing.T, label string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(processTestBudget)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("deadline waiting for %s", label)
}

func sessionProcessState(t *testing.T, pid int) string {
	t.Helper()
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
	return fields[0]
}
