//go:build linux

// Real: a PTY child, terminal input/output and resize, and workload-owned wait.
// Stubbed: no NATS, agent callbacks, I/O freeze, image switch or runtime import.
// origin: docs/reviews/hot-update-plan.md §4.2.
package pty

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

const ownerPTYTestBudget = 10 * time.Second

type ownerPTYResult struct {
	code int
	err  error
}

type ownerPTYFixture struct {
	session  *Session
	manager  *workload.Manager
	id       string
	pid      int
	master   *os.File
	lines    chan string
	readDone chan struct{}
	waitOnce sync.Once
	waitDone chan struct{}
	result   ownerPTYResult
}

func TestOwnedSessionPreservesPTYIOAndResizeAcrossFreeze(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := startOwnerPTYFixture(t, `
		stty -echo
		printf 'OWNER_READY\n'
		IFS= read -r first
		printf 'OWNER_INPUT:%s\n' "$first"
		IFS= read -r second
		printf 'OWNER_SIZE:'
		stty size
		IFS= read -r third
		printf 'OWNER_DONE:%s\n' "$third"
		exit 37
	`)
	var manager workload.Manager
	f.adopt(t, &manager, "interactive")
	f.expectLine(t, "OWNER_READY")
	f.writeLine(t, "original-input")
	f.expectLine(t, "OWNER_INPUT:original-input")

	rows, err := manager.Freeze()
	if err != nil || len(rows) != 1 || rows[0].Exit != nil || rows[0].OSPID != f.pid {
		t.Fatalf("freeze live PTY: rows=%+v err=%v", rows, err)
	}
	if err := f.session.Signal(syscall.SIGTERM); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("Signal during freeze=%v, want ErrFrozen", err)
	}
	f.session.Hangup() // A rejected owner signal must not close the master.
	assertOwnerMasterOpen(t, f)

	// This is deliberately a PROCESS-table cut. Terminal I/O remains usable;
	// the higher runtime layer must separately pause I/O before image switching.
	if err := f.session.SetSize(137, 43); err != nil {
		t.Fatalf("resize after rejected Hangup: %v", err)
	}
	f.writeLine(t, "report-size")
	f.expectLine(t, "OWNER_SIZE:43 137")
	if f.session.OSPID() != f.pid {
		t.Fatalf("frozen OSPID changed: got=%d want=%d", f.session.OSPID(), f.pid)
	}
	manager.Thaw()
	f.writeLine(t, "same-terminal")
	f.expectLine(t, "OWNER_DONE:same-terminal")
	result := f.wait(t)
	if result.err != nil || result.code != 37 {
		t.Fatalf("owned PTY exit=%d err=%v, want 37", result.code, result.err)
	}
	retained, err := manager.Wait(f.id)
	if err != nil || retained.Code != result.code || retained.WaitError != "" {
		t.Fatalf("owner disagrees with PTY Wait: %+v err=%v", retained, err)
	}
	if err := manager.Forget(f.id); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedSessionKeepsPIDAndMasterAfterProcessRelease(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := startOwnerPTYFixture(t, `
		stty -echo
		printf 'OWNER_READY\n'
		IFS= read -r token
		printf 'OWNER_TAIL:%s\n' "$token"
		exit 17
	`)
	f.session.mu.Lock()
	process := f.session.cmd.Process
	f.session.mu.Unlock()
	var manager workload.Manager
	f.adopt(t, &manager, "finished")
	f.expectLine(t, "OWNER_READY")
	f.writeLine(t, "last-output")
	result := f.wait(t)
	if result.err != nil || result.code != 17 {
		t.Fatalf("exit=%d err=%v", result.code, result.err)
	}
	// Manager publishes done only after Release. The Wait completion is the
	// synchronization edge for inspecting the original os.Process below.
	if process.Pid != -1 {
		t.Fatalf("test did not cross Process.Release: Pid=%d", process.Pid)
	}
	if got := f.session.OSPID(); got != f.pid || got <= 0 {
		t.Fatalf("OSPID followed released Process.Pid: got=%d want=%d", got, f.pid)
	}
	// Signal 0 makes the regression safe: a wrong implementation must not be
	// allowed to send a real signal to PID 1 after negating Process.Pid == -1.
	if err := f.session.Signal(0); !errors.Is(err, workload.ErrExited) {
		t.Fatalf("signal after owner reaped child=%v, want ErrExited", err)
	}
	f.session.Hangup()
	assertOwnerMasterOpen(t, f)
	f.expectLine(t, "OWNER_TAIL:last-output")
	code, err := f.session.Wait()
	if err != nil || code != 17 {
		t.Fatalf("second owned Wait lost retained result: code=%d err=%v", code, err)
	}
}

func TestSessionAdoptRejectsCompetingAndPriorWaitOwners(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("duplicate-owner", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := startOwnerPTYFixture(t, "printf 'OWNER_READY\\n'; IFS= read -r token; exit 7")
		var manager, other workload.Manager
		f.adopt(t, &manager, "task")
		f.expectLine(t, "OWNER_READY")
		for _, tc := range []struct {
			manager *workload.Manager
			id      string
		}{{&manager, "task"}, {&manager, "alias"}, {&other, "task"}} {
			if err := f.session.Adopt(tc.manager, f.identity(tc.id)); err == nil {
				t.Fatal("duplicate PTY ownership transfer was accepted")
			}
		}
		if len(manager.Snapshot()) != 1 || len(other.Snapshot()) != 0 {
			t.Fatal("rejected adoption changed either manager's ownership table")
		}
		f.writeLine(t, "finish")
		if result := f.wait(t); result.err != nil || result.code != 7 {
			t.Fatalf("original owner lost child: %+v", result)
		}
	})

	t.Run("unmanaged-wait-finished", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := startOwnerPTYFixture(t, "printf 'OWNER_READY\\n'; IFS= read -r token; exit 13")
		f.expectLine(t, "OWNER_READY")
		f.writeLine(t, "finish")
		if result := f.wait(t); result.err != nil || result.code != 13 {
			t.Fatalf("unmanaged Wait=%+v", result)
		}
		var manager workload.Manager
		if err := f.session.Adopt(&manager, f.identity("too-late")); err == nil {
			t.Fatal("already-waited child was adopted")
		}
		if len(manager.Snapshot()) != 0 {
			t.Fatal("failed adoption left a phantom child")
		}
	})

	t.Run("unmanaged-wait-in-progress", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := startOwnerPTYFixture(t, "printf 'OWNER_READY\\n'; IFS= read -r token; exit 29")
		f.expectLine(t, "OWNER_READY")
		f.beginWait()
		// Observe the actual Wait entry, not a scheduler sleep that merely hopes
		// the competing goroutine started. The child is still blocked on input.
		deadline := time.Now().Add(ownerPTYTestBudget)
		for {
			f.session.mu.Lock()
			entered := f.session.waitStarted
			f.session.mu.Unlock()
			if entered {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("unmanaged Wait did not enter")
			}
			time.Sleep(time.Millisecond)
		}
		var manager workload.Manager
		if err := f.session.Adopt(&manager, f.identity("racing")); err == nil {
			t.Fatal("adoption raced an existing unmanaged waiter")
		}
		if len(manager.Snapshot()) != 0 {
			t.Fatal("competing waiter acquired the child")
		}
		f.writeLine(t, "finish")
		if result := f.wait(t); result.err != nil || result.code != 29 {
			t.Fatalf("original waiter lost exact exit: %+v", result)
		}
	})
}

func startOwnerPTYFixture(t *testing.T, script string) *ownerPTYFixture {
	t.Helper()
	session, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start([]string{"/bin/sh", "-c", script}, nil, "", ""); err != nil {
		_ = session.Close()
		t.Fatal(err)
	}
	f := &ownerPTYFixture{
		session: session, pid: session.OSPID(), master: session.Master,
		lines: make(chan string, 32), readDone: make(chan struct{}), waitDone: make(chan struct{}),
	}
	go func() {
		defer close(f.readDone)
		defer close(f.lines)
		scanner := bufio.NewScanner(f.master)
		for scanner.Scan() {
			f.lines <- strings.TrimSuffix(scanner.Text(), "\r")
		}
	}()
	t.Cleanup(func() {
		if f.manager != nil {
			f.manager.Thaw()
		}
		// A completed Wait may leave an unmanaged Process.Pid invalidated.
		// Avoid issuing any later raw signal on that path during cleanup.
		select {
		case <-f.waitDone:
		default:
			_ = f.session.Signal(syscall.SIGKILL)
			_ = f.wait(t)
		}
		_ = f.session.Close()
		select {
		case <-f.readDone:
		case <-time.After(ownerPTYTestBudget):
			t.Error("PTY reader did not stop after child exit and master close")
		}
	})
	return f
}

func (f *ownerPTYFixture) identity(id string) workload.Identity {
	return workload.Identity{ID: id, OSPID: f.pid, BootID: "pty-fixture", StartTimeTicks: int64(f.pid), StartedAt: time.Now().UTC()}
}

func (f *ownerPTYFixture) adopt(t *testing.T, manager *workload.Manager, id string) {
	t.Helper()
	if err := f.session.Adopt(manager, f.identity(id)); err != nil {
		t.Fatal(err)
	}
	f.manager, f.id = manager, id
}

func (f *ownerPTYFixture) writeLine(t *testing.T, line string) {
	t.Helper()
	payload := []byte(line + "\n")
	if n, err := f.session.WriteInput(payload); err != nil || n != len(payload) {
		t.Fatalf("PTY input n=%d err=%v, want %d", n, err, len(payload))
	}
}

func (f *ownerPTYFixture) expectLine(t *testing.T, want string) {
	t.Helper()
	deadline := time.NewTimer(ownerPTYTestBudget)
	defer deadline.Stop()
	var seen []string
	for {
		select {
		case line, ok := <-f.lines:
			if !ok {
				t.Fatalf("PTY ended before %q; saw %q", want, seen)
			}
			seen = append(seen, line)
			if line == want {
				return
			}
		case <-deadline.C:
			t.Fatalf("PTY did not emit %q; saw %q", want, seen)
		}
	}
}

func (f *ownerPTYFixture) beginWait() {
	f.waitOnce.Do(func() {
		go func() {
			f.result.code, f.result.err = f.session.Wait()
			close(f.waitDone)
		}()
	})
}

func (f *ownerPTYFixture) wait(t *testing.T) ownerPTYResult {
	t.Helper()
	f.beginWait()
	select {
	case <-f.waitDone:
		return f.result
	case <-time.After(ownerPTYTestBudget):
		t.Fatal("PTY Wait did not complete before the test deadline")
		return ownerPTYResult{}
	}
}

func assertOwnerMasterOpen(t *testing.T, f *ownerPTYFixture) {
	t.Helper()
	if f.session.Master != f.master {
		t.Fatal("owner signal replaced or removed the original PTY master")
	}
	if _, err := f.master.Stat(); err != nil {
		t.Fatalf("owner refusal closed the original PTY master: %v", err)
	}
}

// origin: hot-update S4 plan §3.1 / T10 / §12 P2
func TestSessionDrainingOwnerExcludesPriorIOAndWait(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSessionOwnerExclusion(t)
	}
	testharness.AssertNoGoroutineLeak(t, "Session draining ownership exclusion", beforeG)
	testharness.AssertNoFDLeak(t, "Session draining ownership exclusion", beforeFD, 0)
}

func exerciseSessionOwnerExclusion(t *testing.T) {
	t.Helper()
	for _, draining := range []bool{false, true} {
		s, err := Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start([]string{"/bin/sh", "-c", "exec sleep 30"}, nil, "", ""); err != nil {
			_ = s.Close()
			t.Fatal(err)
		}
		var manager workload.Manager
		id := ptyChildIdentity(t, s, "exclusion")
		adopt := s.Adopt
		if draining {
			adopt = s.AdoptDraining
		}
		if err := adopt(&manager, id); err != nil {
			_ = s.Signal(syscall.SIGKILL)
			_, _ = s.Wait()
			_ = s.Close()
			t.Fatal(err)
		}
		finished := false
		finish := func() {
			if finished {
				return
			}
			finished = true
			manager.Thaw()
			_ = manager.Signal(id.ID, syscall.SIGKILL)
			_ = s.Close()
			_ = manager.AllowReap(id.ID)
			_, _ = manager.Wait(id.ID)
			_ = manager.Forget(id.ID)
		}
		t.Cleanup(finish)
		if err := s.AdoptDraining(&manager, id); err == nil {
			t.Fatal("competing draining adopter accepted")
		}
		if err := s.AdoptDraining(nil, id); err == nil {
			t.Fatal("nil draining owner accepted")
		}
		if !draining {
			if _, _, err := s.TakeMaster(); err == nil {
				t.Fatal("non-draining adoption transferred master")
			}
			finish()
			continue
		}
		row, ok := manager.Lookup(id.ID)
		if !ok || !row.ReapHeld {
			t.Fatal("draining adoption did not retain leader identity")
		}
		// Establish that transfer would pass the kernel identity checks. A
		// rejection must therefore come from the active borrower/wait guard.
		identity, err := masterIdentityFromFile(s.Master)
		if err != nil {
			t.Fatal(err)
		}
		identity.Version, identity.Started = 1, true
		identity.BootID, identity.LeaderPID, identity.StartTimeTicks = id.BootID, id.OSPID, id.StartTimeTicks
		if err := CheckMaster(s.Master, identity, row); err != nil {
			t.Fatal(err)
		}
		if _, err := s.borrowMaster(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.TakeMaster(); err == nil {
			t.Fatal("active I/O borrower allowed master transfer")
		}
		s.releaseMaster()
		waitDone := make(chan ownerPTYResult, 1)
		go func() { code, err := s.Wait(); waitDone <- ownerPTYResult{code, err} }()
		deadline := time.Now().Add(ownerPTYTestBudget)
		for {
			s.mu.Lock()
			entered := s.waitStarted
			s.mu.Unlock()
			if entered {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("managed Wait did not establish ownership")
			}
			runtime.Gosched()
		}
		if _, _, err := s.TakeMaster(); err == nil {
			t.Fatal("managed Wait allowed master transfer")
		}
		_ = manager.Signal(id.ID, syscall.SIGKILL)
		_ = s.Close()
		_ = manager.AllowReap(id.ID)
		select {
		case result := <-waitDone:
			if result.err != nil {
				t.Fatal(result.err)
			}
		case <-time.After(ownerPTYTestBudget):
			t.Fatal("managed waiter did not join")
		}
		finish()
	}
}
