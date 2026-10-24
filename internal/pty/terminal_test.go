//go:build linux

// Real: PTY children, original master descriptors, Linux identity and flags,
// deadlines and last-slave EIO. Controlled: synthetic n+error classification.
// No transport, actor, image courier or agent upgrade is exercised here.
package pty

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

// origin: hot-update S4 plan §3.3 / T01
func TestPtyEIOOnlyInStartedDomain(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTerminalEIODomains(t)
	}
	testharness.AssertNoGoroutineLeak(t, "terminal EIO domains", beforeG)
	testharness.AssertNoFDLeak(t, "terminal EIO domains", beforeFD, 0)
}

func exerciseTerminalEIODomains(t *testing.T) {
	t.Helper()
	f := newTerminalFixture(t, "printf 'real-tail'; exit 23")
	if !f.identity.Started {
		t.Fatal("successful start/adoption was not proved")
	}
	file := f.master.Borrow()
	got, err := io.ReadAll(f.master)
	if err != nil || !bytes.Equal(got, []byte("real-tail")) {
		t.Fatalf("started source tail=%q err=%v", got, err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("natural FIN closed master: %v", err)
	}
	// The leader is retained as a zombie by AdoptDraining. Current tty SID
	// observation may now be unavailable; read-only import must still accept
	// its permanent identity and the master's already ended source.
	borrowed := duplicateTerminalFile(t, file)
	restored, err := RestoreMaster(borrowed, f.identity, f.row)
	if err != nil {
		_ = borrowed.Close()
		t.Fatalf("draining zombie master refused: %v", err)
	}
	var ended [1]byte
	if _, err := restored.Read(ended[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("restored last-slave source did not retain FIN: %v", err)
	}
	_ = restored.Close()
	// Linux may still accept master writes after the last slave closes. That
	// observation cannot establish a write-EIO fixture; the classifier table
	// below injects that error explicitly, while this checks the real wrapper.
	if _, err := f.master.Write([]byte("x")); errors.Is(err, io.EOF) {
		t.Fatalf("terminal write became FIN: %v", err)
	}
	if err := f.manager.AllowReap(f.row.ID); err != nil {
		t.Fatal(err)
	}
	exit, err := f.manager.Wait(f.row.ID)
	if err != nil || exit.Code != 23 {
		t.Fatalf("original exit=%+v err=%v", exit, err)
	}
	f.finish(t)

	// The same classifier receives an n+EIO pair: FIN may never discard the
	// bytes returned alongside the final error, even if today's kernel splits it.
	if n, err := terminalReadResult(7, syscall.EIO, true); n != 7 || !errors.Is(err, io.EOF) {
		t.Fatalf("final read lost bytes before FIN: n=%d err=%v", n, err)
	}
	for _, domain := range []string{"unstarted", "failed-start", "generic-pipe", "write", "unverified-master"} {
		if n, err := terminalReadResult(7, syscall.EIO, false); n != 7 || !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s EIO incorrectly became FIN: n=%d err=%v", domain, n, err)
		}
	}
	for _, err := range []error{os.ErrDeadlineExceeded, os.ErrClosed} {
		if n, got := terminalReadResult(3, err, true); n != 3 || got != err {
			t.Fatalf("non-source end changed: %d %v", n, got)
		}
	}
	for _, failed := range []bool{false, true} {
		s, err := Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		if failed {
			if err := s.Start([]string{"/no/such/pty-command"}, nil, "", ""); err == nil {
				t.Fatal("failed Start fixture unexpectedly succeeded")
			}
			if !s.started || s.ready {
				t.Fatal("fixture did not distinguish attempt from successful Start")
			}
		} else {
			_ = s.slave.Close()
		}
		if legacyTerminalVerified(s) {
			t.Fatal("attempt-only Session qualified as started domain")
		}
		if err := s.AdoptDraining(&workload.Manager{}, workload.Identity{}); err == nil {
			t.Fatal("unstarted child adopted for draining")
		}
		var b [1]byte
		n, readErr := s.Master.Read(b[:])
		if n != 0 || !errors.Is(readErr, syscall.EIO) {
			t.Fatalf("unstarted source n=%d err=%v", n, readErr)
		}
		if _, _, err := s.TakeMaster(); err == nil {
			t.Fatal("unstarted master transferred")
		}
		_ = s.Close()
	}
	blocked := newTerminalFixture(t, "stty -echo; printf READY; IFS= read -r token; exit 23")
	ready := make([]byte, 5)
	if _, err := io.ReadFull(blocked.master, ready); err != nil || string(ready) != "READY" {
		t.Fatalf("read readiness=%q %v", ready, err)
	}
	if err := blocked.master.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := blocked.master.Read(b[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("deadline became FIN: %v", err)
	}
	if err := blocked.master.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := blocked.master.Read(b[:]); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("explicit close became FIN: %v", err)
	}
	blocked.finish(t)
}

// origin: hot-update S4 plan §3.1 / T10
func TestPtyMasterTransferAndWrongDescriptorRefusal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTerminalTransfer(t)
	}
	testharness.AssertNoGoroutineLeak(t, "terminal master transfer", beforeG)
	testharness.AssertNoFDLeak(t, "terminal master transfer", beforeFD, 0)
}

func exerciseTerminalTransfer(t *testing.T) {
	t.Helper()
	f := newTerminalFixture(t, "stty -echo; printf READY; IFS= read -r token; printf 'TAIL:%s' \"$token\"; exit 23")
	if f.master.Borrow() != f.original {
		t.Fatal("TakeMaster manufactured another wrapper for the same descriptor")
	}
	if f.session.Master != nil {
		t.Fatal("old Session retained terminal authority")
	}
	if _, _, err := f.session.TakeMaster(); err == nil {
		t.Fatal("second TakeMaster accepted")
	}
	if err := f.session.Close(); err != nil {
		t.Fatal(err)
	}
	f.session.Hangup()
	f.session.KillAndWaitAfterAbandonedStart()
	if _, err := f.session.WriteInput([]byte("wrong")); err == nil {
		t.Fatal("old Session wrote after transfer")
	}
	if err := f.session.SetSize(90, 30); err == nil {
		t.Fatal("old Session resized after transfer")
	}
	if err := f.session.Signal(syscall.SIGTERM); err == nil {
		t.Fatal("old Session signaled after transfer")
	}
	ready := make([]byte, 5)
	if _, err := io.ReadFull(f.master, ready); err != nil || string(ready) != "READY" {
		t.Fatalf("old Session touched new owner: %q %v", ready, err)
	}
	observation, err := f.master.Observe()
	if err != nil || observation.Cols != 80 || observation.Rows != 24 || len(observation.Termios) == 0 ||
		!observation.ControlTTYKnown || observation.SessionID != f.row.OSPID {
		t.Fatalf("live terminal observation=%+v err=%v", observation, err)
	}

	other, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	otherID, err := masterIdentityFromFile(other.Master)
	if err != nil {
		t.Fatal(err)
	}
	if otherID.Dev != f.identity.Dev || otherID.Ino != f.identity.Ino || otherID.Rdev != f.identity.Rdev || otherID.PTYIndex == f.identity.PTYIndex {
		t.Fatal("fixture did not establish indistinguishable fstat tuples and distinct PTY indices")
	}
	if err := CheckMaster(other.Master, f.identity, f.row); err == nil {
		t.Fatal("another ptmx open passed identity check")
	}
	pipe, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckMaster(pipe, f.identity, f.row); err == nil {
		t.Fatal("non-terminal pipe accepted")
	}
	_ = pipe.Close()
	_ = pipeWriter.Close()
	for _, change := range []func(*TerminalIdentity){
		func(id *TerminalIdentity) { id.MountID++ }, func(id *TerminalIdentity) { id.PTYIndex++ },
		func(id *TerminalIdentity) { id.Started = false }, func(id *TerminalIdentity) { id.Version++ },
		func(id *TerminalIdentity) { id.StartTimeTicks++ }, func(id *TerminalIdentity) { id.BootID = "wrong-boot" },
	} {
		bad := f.identity
		change(&bad)
		if _, err := RestoreMaster(f.master.Borrow(), bad, f.row); err == nil {
			t.Fatal("invalid identity imported")
		}
		masterRequirePollable(t, f.master.Borrow())
	}
	for _, flag := range []string{"blocking", "no-cloexec"} {
		borrowed := duplicateTerminalFile(t, f.master.Borrow())
		masterPollControl(t, borrowed, func(fd int) error {
			if flag == "blocking" {
				return unix.SetNonblock(fd, false)
			}
			_, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0)
			return err
		})
		before := terminalFlags(t, borrowed)
		if err := CheckMaster(borrowed, f.identity, f.row); err == nil {
			t.Fatalf("read-only check accepted %s descriptor", flag)
		}
		if _, err := RestoreMaster(borrowed, f.identity, f.row); err == nil {
			t.Fatalf("%s descriptor accepted", flag)
		}
		if got := terminalFlags(t, borrowed); got != before {
			t.Fatalf("failed import repaired %s flags: before=%v after=%v", flag, before, got)
		}
		if _, err := borrowed.Stat(); err != nil {
			t.Fatalf("failed import consumed original: %v", err)
		}
		// Restore only the fixture's deliberate OFD corruption, after proving
		// that import did not repair it or consume the salvage descriptor.
		masterPollControl(t, borrowed, func(fd int) error { return unix.SetNonblock(fd, true) })
		_ = borrowed.Close()
	}
	borrowed := duplicateTerminalFile(t, f.master.Borrow())
	originalFD := terminalFD(t, borrowed)
	restored, err := RestoreMaster(borrowed, f.identity, f.row)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := borrowed.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("successful restore did not consume old wrapper: %v", err)
	}
	if terminalFD(t, restored.Borrow()) == originalFD {
		t.Fatal("restore reused descriptor integer for two wrappers")
	}
	_ = f.master.Close()
	f.master = restored
	if err := CheckMaster(restored.Borrow(), f.identity, f.row); err != nil {
		t.Fatal(err)
	}
	if err := restored.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Write([]byte("same-terminal\n")); err != nil {
		t.Fatal(err)
	}
	tail, err := io.ReadAll(restored)
	if err != nil || string(tail) != "TAIL:same-terminal" {
		t.Fatalf("salvaged terminal tail=%q err=%v", tail, err)
	}
	if err := f.manager.AllowReap(f.row.ID); err != nil {
		t.Fatal(err)
	}
	exit, err := f.manager.Wait(f.row.ID)
	if err != nil || exit.Code != 23 {
		t.Fatalf("restored child exit=%+v err=%v", exit, err)
	}
	f.finish(t)
}

type terminalFixture struct {
	session  *Session
	manager  *workload.Manager
	master   Master
	original *os.File
	identity TerminalIdentity
	row      workload.Record
	finished bool
}

func newTerminalFixture(t *testing.T, script string) *terminalFixture {
	t.Helper()
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start([]string{"/bin/sh", "-c", script}, nil, "", ""); err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	f := &terminalFixture{session: s, manager: &workload.Manager{}, original: s.Master}
	id := ptyChildIdentity(t, s, "terminal")
	if err := s.AdoptDraining(f.manager, id); err != nil {
		_ = s.Signal(syscall.SIGKILL)
		_, _ = s.Wait()
		_ = s.Close()
		t.Fatal(err)
	}
	f.row, _ = f.manager.Lookup(id.ID)
	// Register cleanup before Take: rejection must still clean the adopted child.
	t.Cleanup(func() { f.finish(t) })
	f.master, f.identity, err = s.TakeMaster()
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *terminalFixture) finish(t *testing.T) {
	t.Helper()
	if f.finished {
		return
	}
	f.finished = true
	if f.master != nil {
		_ = f.master.Close()
	}
	_ = f.session.Close()
	f.manager.Thaw()
	_ = f.manager.Signal(f.row.ID, syscall.SIGKILL)
	_ = f.manager.AllowReap(f.row.ID)
	if _, err := f.manager.Wait(f.row.ID); err != nil {
		t.Error(err)
	}
	if err := f.manager.Forget(f.row.ID); err != nil {
		t.Error(err)
	}
}
func duplicateTerminalFile(t *testing.T, file *os.File) *os.File {
	t.Helper()
	fd := -1
	masterPollControl(t, file, func(original int) error {
		var err error
		fd, err = unix.FcntlInt(uintptr(original), unix.F_DUPFD_CLOEXEC, 3)
		return err
	})
	return os.NewFile(uintptr(fd), "terminal-import")
}
func terminalFD(t *testing.T, file *os.File) int {
	t.Helper()
	fd := -1
	masterPollControl(t, file, func(n int) error { fd = n; return nil })
	return fd
}
func terminalFlags(t *testing.T, file *os.File) [2]int {
	t.Helper()
	var flags [2]int
	masterPollControl(t, file, func(fd int) error {
		var err error
		flags[0], err = unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if err == nil {
			flags[1], err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		}
		return err
	})
	return flags
}
