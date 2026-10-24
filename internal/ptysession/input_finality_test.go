//go:build linux

package ptysession

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S4 internal review rv-eof rv-eof-1
// Real kernel Write succeeds and the original child consumes all four bytes.
// Only delivery of that completed syscall receipt to the actor is delayed.
func TestPtyInputEndedWaitsForInflightWriteReceipt(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, consumed := range []bool{true, false} {
			t.Run(fmt.Sprintf("%d/consumed=%t", round, consumed), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseSourceEndFIN(t, consumed)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "source end writer receipt", beforeG)
	testharness.AssertNoFDLeak(t, "source end writer receipt", beforeFD, 0)
}

// Both verifier triggers are retained: child consumption and an unread kernel
// write followed by sole-slave close must finalize the same logical FIN.
func exerciseSourceEndFIN(t *testing.T, consumed bool) {
	t.Helper()
	body := "tty.setraw(0)\nmark('ready')\n"
	if consumed {
		body += "data=b''\nwhile len(data)<4: data+=os.read(0,4-len(data))\nmark('received',data)\n"
	}
	f := startActorFixture(t, body+"wait('exit')\nos.write(1,b'TAIL')\n")
	waitMark(t, f.dir, "ready")
	freezeActor(t, f.a)
	release := make(chan struct{})
	// Registered after fixture cleanup, so a failing assertion cannot strand Close.
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	m := &observedMaster{Master: f.a.cfg.Master, writeReturned: make(chan struct{}, 1), releaseWrite: release}
	f.a.cfg.Master = m
	activateActor(t, f.a)
	if _, err := f.a.AcceptInput(0, []byte("DONE"), true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.writeReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("kernel write receipt missing")
	}
	if consumed {
		waitMark(t, f.dir, "received")
		requireFileBytes(t, f.dir+"/received", []byte("DONE"))
	}
	m.mu.Lock()
	receipt := m.writes[0]
	m.mu.Unlock()
	if receipt.N != 4 || receipt.Error != "" {
		t.Fatalf("not a successful kernel write: %+v", receipt)
	}
	markActor(t, f.dir, "exit")
	awaitActor(t, "read source end", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.state.SourceEnded })
	st := f.a.InputStatus()

	row, ok := f.manager.Lookup("terminal")
	t.Logf("before delayed Write receipt: input=%+v manager_ok=%v reap_held=%v exit=%+v", st, ok, row.ReapHeld, row.Exit)
	unblock()
	awaitActor(t, "delayed Write accounted", func() bool { st := f.a.InputStatus(); return st.Applied == 4 && st.Ended })
	final := f.a.InputStatus()
	t.Logf("after delayed Write receipt: input=%+v", final)
	if st.Ended && st.Applied != final.Applied {
		t.Fatalf("input final counters changed after Ended: A=%d -> %d Q=%d FIN=%v error=%q", st.Applied, final.Applied, st.Accepted, st.FIN, st.Error)
	}
	if st.Ended {
		t.Fatal("published Ended before the outstanding Write receipt")
	}
	if !row.ReapHeld || row.Exit != nil {
		t.Fatal("released leader before writer's final byte receipt")
	}
	if final.Accepted != 4 || !final.FIN || !final.Ended || final.Error != "" {
		t.Fatalf("fully written FIN became an abort: %+v", final)
	}
	awaitActor(t, "original child exit", func() bool { row, ok := f.manager.Lookup("terminal"); return ok && row.Exit != nil })
	row, _ = f.manager.Lookup("terminal")
	if row.Exit.Code != 0 || string(actorBytes(t, f.a)) != "TAIL" || f.a.InputStatus() != final {
		t.Fatal("tail, terminal or final input receipt changed")
	}
}

// origin: hot-update S4 internal review rv-fd rv-fd-1
func TestInputEndedWaitsForInFlightWriteAccounting(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseSourceEndWithoutFIN(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "source end without FIN writer receipt", beforeG)
	testharness.AssertNoFDLeak(t, "source end without FIN writer receipt", beforeFD, 0)
}

func exerciseSourceEndWithoutFIN(t *testing.T) {
	t.Helper()
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	freezeActor(t, f.a)
	release := make(chan struct{})
	m := &observedMaster{Master: f.a.cfg.Master, writeReturned: make(chan struct{}, 1), releaseWrite: release}
	f.a.cfg.Master = m
	defer func() {
		if !stopped(release) {
			close(release)
		}
	}()
	activateActor(t, f.a)
	const data = "kernel-accepted-before-source-end"
	if _, err := f.a.AcceptInput(0, []byte(data), false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.writeReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("real master Write did not return")
	}
	m.mu.Lock()
	n := m.writes[0].N
	m.mu.Unlock()
	if n != len(data) {
		t.Fatalf("fixture needs real complete kernel write: n=%d", n)
	}
	markActor(t, f.dir, "exit")
	awaitActor(t, "last slave reached source EOF", func() bool {
		f.a.mu.Lock()
		defer f.a.mu.Unlock()
		return f.a.state.SourceEnded
	})
	before := f.a.InputStatus()
	close(release)
	awaitActor(t, "in-flight Write recorded its bytes", func() bool { st := f.a.InputStatus(); return st.Applied == uint64(n) && st.Ended })
	after := f.a.InputStatus()
	if before.Ended && before.Applied != after.Applied {
		t.Fatalf("InputEnded published mutable final ACK: before=%+v after=%+v; real Write n=%d", before, after, n)
	}
}

// origin: hot-update S4 internal review rv-eof rv-eof-1 / rv-fd rv-fd-1
func TestPtyInputAbortWaitsForInflightWriteReceipt(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, mode := range []string{"replay", "revocation", "source_then_revocation"} {
			t.Run(fmt.Sprintf("%d/%s", round, mode), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseInflightInputAbort(t, mode)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "inflight input abort receipt", beforeG)
	testharness.AssertNoFDLeak(t, "inflight input abort receipt", beforeFD, 0)
}

func exerciseInflightInputAbort(t *testing.T, mode string) {
	t.Helper()
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	freezeActor(t, f.a)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	m := &observedMaster{Master: f.a.cfg.Master, writeReturned: make(chan struct{}, 1), releaseWrite: release}
	f.a.cfg.Master = m
	activateActor(t, f.a)
	if _, err := f.a.AcceptInput(0, []byte("DONE"), mode == "source_then_revocation"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.writeReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("real Write receipt missing")
	}
	m.mu.Lock()
	receipt := m.writes[0]
	m.mu.Unlock()
	if receipt.N != 4 || receipt.Error != "" {
		t.Fatalf("kernel write premise: %+v", receipt)
	}
	if mode == "replay" {
		_, file, err := f.a.cfg.Input.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		f.a.cfg.Input.Thaw()
		restore := inputReadOnlyFault(t, file)
		defer restore()
	}
	if mode == "source_then_revocation" {
		markActor(t, f.dir, "exit")
		awaitActor(t, "source ended before revocation", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.state.SourceEnded })
	}
	done := make(chan error, 1)
	go func() {
		if mode == "replay" {
			_, err := f.a.AcceptInput(4, []byte("NEXT"), true)
			done <- err
		} else {
			done <- f.a.Revoke("revoked")
		}
	}()
	awaitActor(t, "abort closes intake", func() bool {
		f.a.mu.Lock()
		defer f.a.mu.Unlock()
		return f.a.inputClosing != "" && (mode == "replay" || f.a.state.Revoked != "")
	})
	before := f.a.InputStatus()
	if before.Ended || before.Applied != 0 || before.Accepted != 4 {
		unblock()
		<-done
		t.Fatalf("abort published an unaccounted final receipt: %+v", before)
	}
	if mode == "replay" {
		select {
		case err := <-done:
			t.Fatalf("committed abort returned before the writer barrier: %v", err)
		default:
		}
	}
	unblock()
	var err error
	select {
	case err = <-done:
	case <-time.After(actorTestBudget):
		t.Fatal("abort did not join the writer")
	}
	wantError := "pty input aborted: revoked"
	if mode == "replay" {
		var aborted *InputAbortError
		if !errors.As(err, &aborted) {
			t.Fatalf("committed replay abort lacks typed result: %v", err)
		}
		wantError = "pty input aborted: replay_error"
	} else if err != nil {
		t.Fatal(err)
	}
	final := f.a.InputStatus()
	if !final.Ended || final.Applied != 4 || final.Accepted != 4 || final.Error != wantError || final.FIN != (mode == "source_then_revocation") {
		t.Fatalf("lost write receipt or abort: %+v", final)
	}
	// A replay failure must leave the real child/output live until the child
	// chooses to close. Revocation instead delegates termination to cleanup.
	if mode == "replay" {
		markActor(t, f.dir, "exit")
	}
	awaitActor(t, "true source ended", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.state.SourceEnded })
	if f.a.InputStatus() != final {
		t.Fatal("final abort receipt changed at source end")
	}
}

// A read-only alias preserves reads of the real ring but fails actual pwrite.
// Restoration precedes fixture teardown, including on a failing assertion.
func inputReadOnlyFault(t *testing.T, file *os.File) func() {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	target, backup := -1, -1
	var inner error
	if err := raw.Control(func(fd uintptr) { target = int(fd); backup, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || inner != nil {
		t.Fatalf("backup: %v %v", err, inner)
	}
	ro, err := os.Open("/proc/self/fd/" + strconv.Itoa(target))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	roRaw, err := ro.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := roRaw.Control(func(fd uintptr) { inner = unix.Dup3(int(fd), target, unix.O_CLOEXEC) }); err != nil || inner != nil {
		t.Fatalf("fault: %v %v", err, inner)
	}
	return sync.OnceFunc(func() {
		if err := unix.Dup3(backup, target, unix.O_CLOEXEC); err != nil {
			t.Error(err)
		}
		_ = unix.Close(backup)
	})
}
