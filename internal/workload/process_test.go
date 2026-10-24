//go:build linux

// Real: OS children, process groups, wait status, kernel zombies and descriptors.
// Stubbed: no agent, stdio pumps, NATS, runtime import or upgrade coordinator.
// This checks the process-table barrier, not full business I/O quiescence.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const processTestBudget = 10 * time.Second

type processFixture struct {
	process *os.Process
	pid     int
	input   *os.File
	owner   *workload.Manager
	id      string
	done    bool
}

type observedExit struct {
	exit workload.Exit
	err  error
}

func TestManagerReapsExitForEveryObserver(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, tc := range []struct {
		name string
		code int
		sig  syscall.Signal
	}{
		{name: "normal", code: 23},
		{name: "signal", code: -1, sig: syscall.SIGKILL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var manager workload.Manager
			child := startProcessFixture(t, 23)
			child.adopt(t, &manager, "task")
			const observers = 8
			results := make(chan observedExit, observers)
			var ready sync.WaitGroup
			ready.Add(observers)
			for i := 0; i < observers; i++ {
				go func() {
					ready.Done()
					result, err := manager.Wait("task")
					results <- observedExit{exit: result, err: err}
				}()
			}
			ready.Wait()
			if tc.sig != 0 {
				if err := manager.Signal("task", tc.sig); err != nil {
					t.Fatal(err)
				}
			} else {
				child.finish(t)
			}
			var first workload.Exit
			for i := 0; i < observers; i++ {
				result := receiveExit(t, results)
				if result.err != nil || result.exit.Code != tc.code || result.exit.Signal != int(tc.sig) {
					t.Fatalf("observer %d: exit=%+v err=%v", i, result.exit, result.err)
				}
				if result.exit.EndedAt.IsZero() || result.exit.WaitError != "" {
					t.Fatalf("exit lacks a real retained result: %+v", result.exit)
				}
				if i == 0 {
					first = result.exit
				} else if !reflect.DeepEqual(first, result.exit) {
					t.Fatalf("observers disagree: first=%+v now=%+v", first, result.exit)
				}
			}
			child.done = true
			_ = child.input.Close()
			status := syscall.WaitStatus(first.Status)
			if tc.sig == 0 && (!status.Exited() || status.ExitStatus() != tc.code) {
				t.Fatalf("raw wait status=%#x, want exit %d", first.Status, tc.code)
			}
			if tc.sig != 0 && (!status.Signaled() || status.Signal() != tc.sig) {
				t.Fatalf("raw wait status=%#x, want signal %s", first.Status, tc.sig)
			}
			if err := manager.Signal("task", syscall.SIGKILL); !errors.Is(err, workload.ErrExited) {
				t.Fatalf("signal after reap=%v, want ErrExited", err)
			}
			assertKernelChildReaped(t, child.pid)
		})
	}
}

func TestManagerFreezeRetainsWaitableChildAndRejectsMutation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	child := startProcessFixture(t, 37)
	child.adopt(t, &manager, "frozen")
	snapshot, err := manager.Freeze()
	if err != nil || len(snapshot) != 1 || snapshot[0].Exit != nil {
		t.Fatalf("freeze live child: snapshot=%+v err=%v", snapshot, err)
	}
	if _, err := manager.Freeze(); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("second Freeze=%v, want ErrFrozen", err)
	}
	if err := manager.Signal("frozen", syscall.SIGKILL); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen Signal=%v, want ErrFrozen", err)
	}
	if err := manager.SignalAll(syscall.SIGKILL); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen SignalAll=%v, want ErrFrozen", err)
	}
	if err := manager.Forget("frozen"); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen Forget=%v, want ErrFrozen", err)
	}
	candidate := startProcessFixture(t, 11)
	if err := manager.Adopt(candidate.identity("new"), candidate.process); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen Adopt=%v, want ErrFrozen", err)
	}
	waiting := observeExit(&manager, "frozen")
	child.finish(t)
	waitUntilKernelChildWaitable(t, child.pid)
	if rows := manager.Snapshot(); len(rows) != 1 || rows[0].Exit != nil {
		t.Fatalf("frozen reaper consumed kernel result: %+v", rows)
	}
	// Negative observation window, not a readiness barrier or product deadline:
	// give an incorrectly active reaper time to consume the already-exited child.
	select {
	case result := <-waiting:
		t.Fatalf("Wait completed before Thaw: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	waitUntilKernelChildWaitable(t, child.pid)
	manager.Thaw()
	result := receiveExit(t, waiting)
	if result.err != nil || result.exit.Code != 37 || result.exit.Signal != 0 {
		t.Fatalf("thawed result=%+v err=%v", result.exit, result.err)
	}
	child.done = true
	assertKernelChildReaped(t, child.pid)
	// Rejected adoption leaves the other process owned and waitable by its caller.
	candidate.finish(t)
	state, err := candidate.process.Wait()
	candidate.done = true
	if err != nil || state.ExitCode() != 11 {
		t.Fatalf("caller lost rejected child: state=%v err=%v", state, err)
	}
	if err := manager.Forget("frozen"); err != nil {
		t.Fatal(err)
	}
}

func TestManagerFreezePreservesAlreadyRecordedExit(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	child := startProcessFixture(t, 41)
	child.adopt(t, &manager, "terminal")
	child.finish(t)
	result := receiveExit(t, observeExit(&manager, "terminal"))
	if result.err != nil || result.exit.Code != 41 {
		t.Fatalf("exit=%+v err=%v", result.exit, result.err)
	}
	child.done = true
	rows, err := manager.Freeze()
	if err != nil || len(rows) != 1 || rows[0].Exit == nil || !reflect.DeepEqual(*rows[0].Exit, result.exit) {
		t.Fatalf("freeze omitted prior exit: rows=%+v err=%v", rows, err)
	}
	if err := manager.Forget("terminal"); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("Forget changed frozen terminal census: %v", err)
	}
	// Frozen terminal results are still observable; observing is not a mutation.
	again := receiveExit(t, observeExit(&manager, "terminal"))
	if again.err != nil || !reflect.DeepEqual(result.exit, again.exit) {
		t.Fatalf("frozen observer changed result: %+v", again)
	}
	manager.Thaw()
	if err := manager.Forget("terminal"); err != nil {
		t.Fatal(err)
	}
}

func TestManagerSnapshotCopiesRecordsAndRejectsDuplicateOwners(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	first := startProcessFixture(t, 17)
	first.adopt(t, &manager, "z-task")
	second := startProcessFixture(t, 19)
	if err := manager.Adopt(second.identity("z-task"), second.process); err == nil {
		t.Fatal("duplicate task ID was accepted")
	}
	if err := manager.Adopt(first.identity("alias"), first.process); err == nil {
		t.Fatal("same live OS child was adopted twice")
	}
	if err := manager.Forget("z-task"); err == nil {
		t.Fatal("live child was forgotten")
	}
	second.adopt(t, &manager, "a-task")
	before := manager.Snapshot()
	if len(before) != 2 || before[0].ID != "a-task" || before[1].ID != "z-task" {
		t.Fatalf("census is incomplete or unordered: %+v", before)
	}
	before[0].ID = "tampered"
	before[1].OSPID = 1
	first.finish(t)
	second.finish(t)
	for _, fixture := range []*processFixture{first, second} {
		result := receiveExit(t, observeExit(&manager, fixture.id))
		if result.err != nil {
			t.Fatal(result.err)
		}
		fixture.done = true
	}
	snapshot := manager.Snapshot()
	if snapshot[0].ID != "a-task" || snapshot[1].OSPID != first.pid || snapshot[0].Exit.Code != 19 || snapshot[1].Exit.Code != 17 {
		t.Fatalf("snapshot edits mutated manager: %+v", snapshot)
	}
	snapshot[0].Exit.Code = 99
	snapshot[0].Exit.WaitError = "forged"
	snapshot[1].Exit = nil
	frozen, err := manager.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	frozen[0].Exit.Code = 88
	manager.Thaw()
	for _, fixture := range []*processFixture{first, second} {
		result := receiveExit(t, observeExit(&manager, fixture.id))
		want := 17
		if fixture == second {
			want = 19
		}
		if result.err != nil || result.exit.Code != want {
			t.Fatalf("aliased exit result: got=%+v err=%v want=%d", result.exit, result.err, want)
		}
		if err := manager.Forget(fixture.id); err != nil {
			t.Fatal(err)
		}
	}
	if len(manager.Snapshot()) != 0 {
		t.Fatal("Forget retained terminal records")
	}
	if _, err := manager.Wait("z-task"); !errors.Is(err, workload.ErrUnknown) {
		t.Fatalf("Wait forgotten ID=%v, want ErrUnknown", err)
	}
	if err := manager.Signal("z-task", syscall.SIGTERM); !errors.Is(err, workload.ErrUnknown) {
		t.Fatalf("Signal forgotten ID=%v, want ErrUnknown", err)
	}
}

func TestManagerDoesNotReapUnownedChild(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	owned := startProcessFixture(t, 5)
	owned.adopt(t, &manager, "owned")
	other := startProcessFixture(t, 47)
	other.finish(t)
	waitUntilKernelChildWaitable(t, other.pid)
	// Force actual managed reap work while the unrelated child remains a zombie.
	owned.finish(t)
	result := receiveExit(t, observeExit(&manager, "owned"))
	if result.err != nil || result.exit.Code != 5 {
		t.Fatalf("manager took another child's result: %+v err=%v", result.exit, result.err)
	}
	owned.done = true
	state, err := other.process.Wait()
	other.done = true
	if err != nil || state.ExitCode() != 47 {
		t.Fatalf("manager stole unowned child: state=%v err=%v", state, err)
	}
}

func TestManagerSignalAllStopsOwnedGroups(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	children := []*processFixture{startProcessFixture(t, 0), startProcessFixture(t, 0)}
	for i, child := range children {
		child.adopt(t, &manager, fmt.Sprintf("task-%d", i))
	}
	if err := manager.SignalAll(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		result := receiveExit(t, observeExit(&manager, child.id))
		if result.err != nil || result.exit.Code != -1 || result.exit.Signal != int(syscall.SIGKILL) {
			t.Fatalf("SignalAll child=%s exit=%+v err=%v", child.id, result.exit, result.err)
		}
		child.done = true
		_ = child.input.Close()
	}
	if err := manager.SignalAll(syscall.SIGKILL); err != nil {
		t.Fatalf("SignalAll should ignore retained exits: %v", err)
	}
}

func TestManagerSignalReachesWholeProcessGroup(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	leader := startProcessFixture(t, 0)
	leader.adopt(t, &manager, "leader")
	// The extra group member is also our direct child, so the test can reap it
	// itself without orphaning a grandchild or changing process-wide subreaping.
	member := startProcessFixtureInGroup(t, 0, leader.pid)
	if err := manager.Signal("leader", syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	result := receiveExit(t, observeExit(&manager, "leader"))
	if result.err != nil || result.exit.Signal != int(syscall.SIGKILL) {
		t.Fatalf("group leader exit=%+v err=%v", result.exit, result.err)
	}
	leader.done = true
	_ = leader.input.Close()
	waitUntilKernelChildWaitable(t, member.pid)
	state, err := member.process.Wait()
	member.done = true
	_ = member.input.Close()
	if err != nil {
		t.Fatal(err)
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("Signal did not reach the original process group: %v", state)
	}
}

func TestManagerRepeatedLifecycleReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var manager workload.Manager
	exerciseProcessLifecycle(t, &manager, "warmup")
	goroutines := runtime.NumGoroutine()
	fds := testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseProcessLifecycle(t, &manager, fmt.Sprintf("round-%d", i))
	}
	testharness.AssertNoGoroutineLeak(t, "workload process lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "workload process lifecycle", fds, 0)
}

func exerciseProcessLifecycle(t *testing.T, manager *workload.Manager, id string) {
	t.Helper()
	child := startProcessFixture(t, 29)
	child.adopt(t, manager, id)
	if _, err := manager.Freeze(); err != nil {
		t.Fatal(err)
	}
	child.finish(t)
	waitUntilKernelChildWaitable(t, child.pid)
	manager.Thaw()
	result := receiveExit(t, observeExit(manager, id))
	if result.err != nil || result.exit.Code != 29 {
		t.Fatalf("lifecycle exit=%+v err=%v", result.exit, result.err)
	}
	child.done = true
	assertKernelChildReaped(t, child.pid)
	if err := manager.Forget(id); err != nil {
		t.Fatal(err)
	}
}

func startProcessFixture(t *testing.T, code int) *processFixture {
	t.Helper()
	return startProcessFixtureInGroup(t, code, 0)
}

func startProcessFixtureInGroup(t *testing.T, code, group int) *processFixture {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("IFS= read -r token; exit %d", code))
	cmd.Dir = dir
	cmd.Stdin, cmd.Stdout, cmd.Stderr = reader, os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: group}
	if err := cmd.Start(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	_ = reader.Close()
	child := &processFixture{process: cmd.Process, pid: cmd.Process.Pid, input: writer}
	t.Cleanup(func() {
		_ = child.input.Close()
		if child.done {
			return
		}
		if child.owner == nil {
			_ = child.process.Kill()
			_, _ = child.process.Wait()
			return
		}
		child.owner.Thaw()
		_ = child.owner.Signal(child.id, syscall.SIGKILL)
		result := receiveExit(t, observeExit(child.owner, child.id))
		if result.err != nil && !errors.Is(result.err, workload.ErrUnknown) {
			t.Errorf("cleanup child %s: %v", child.id, result.err)
		}
		// A broken waiter can falsely report this child exited after reaping a
		// different PID. Only this cleanup fallback consumes kernel state; after
		// the manager published a terminal result it must no longer reap this ID.
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(child.pid, &status, syscall.WNOHANG, nil)
		if err == nil && pid == 0 {
			_ = syscall.Kill(-child.pid, syscall.SIGKILL)
			_, _ = syscall.Wait4(child.pid, &status, 0, nil)
		}
	})
	return child
}

func (p *processFixture) identity(id string) workload.Identity {
	return workload.Identity{ID: id, OSPID: p.pid, BootID: "fixture-boot", StartTimeTicks: int64(p.pid), StartedAt: time.Now().UTC()}
}

func (p *processFixture) adopt(t *testing.T, manager *workload.Manager, id string) {
	t.Helper()
	if err := manager.Adopt(p.identity(id), p.process); err != nil {
		t.Fatal(err)
	}
	p.owner, p.id = manager, id
}

func (p *processFixture) finish(t *testing.T) {
	t.Helper()
	if _, err := p.input.WriteString("finish\n"); err != nil {
		t.Fatal(err)
	}
	if err := p.input.Close(); err != nil {
		t.Fatal(err)
	}
}

func observeExit(manager *workload.Manager, id string) <-chan observedExit {
	result := make(chan observedExit, 1)
	go func() {
		exit, err := manager.Wait(id)
		result <- observedExit{exit: exit, err: err}
	}()
	return result
}

func receiveExit(t *testing.T, ch <-chan observedExit) observedExit {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(processTestBudget):
		t.Fatal("process result was not available before the test deadline")
		return observedExit{}
	}
}

func waitUntilKernelChildWaitable(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(processTestBudget)
	for time.Now().Before(deadline) {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
		if err != nil && !errors.Is(err, syscall.EINTR) {
			t.Fatalf("child %d no longer waitable: %v", pid, err)
		}
		if err == nil && info.Signo != 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("child %d did not exit into a waitable state", pid)
}

func assertKernelChildReaped(t *testing.T, pid int) {
	t.Helper()
	var status syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); !errors.Is(err, syscall.ECHILD) {
		t.Fatalf("child %d was not reaped exactly once: err=%v status=%v", pid, err, status)
	}
}
