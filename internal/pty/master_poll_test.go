//go:build linux

// Real: Linux PTY pairs, descriptor flags, deadlines, resize ioctls, child exit
// and the kernel's final-master-read EIO. No output comes from a source ledger.
// Controlled: a borrowed legacy File isolates SetSize's blocking-mode hazard.
// These tests cover PTY master preparation, not the future resumable adapters.
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
	cpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func TestMasterAllocateIsPollableAndResizeKeepsIdentity(t *testing.T) {
	session, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	want := masterPollIdentity(t, session.Master)
	masterRequirePollable(t, session.Master)
	for _, size := range [][2]int{{137, 43}, {88, 31}, {0, 0}} {
		if err := session.SetSize(size[0], size[1]); err != nil {
			t.Fatal(err)
		}
		masterRequirePollable(t, session.Master)
		if got := masterPollIdentity(t, session.Master); got != want {
			t.Fatalf("resize replaced original PTY: got=%+v want=%+v", got, want)
		}
		var actual *unix.Winsize
		masterPollControl(t, session.Master, func(fd int) error {
			var err error
			actual, err = unix.IoctlGetWinsize(fd, unix.TIOCGWINSZ)
			return err
		})
		if size[0] <= 0 || size[1] <= 0 {
			size = [2]int{80, 24}
		}
		if int(actual.Col) != size[0] || int(actual.Row) != size[1] {
			t.Fatalf("kernel winsize=%dx%d want=%dx%d", actual.Col, actual.Row, size[0], size[1])
		}
	}
}

func TestMasterResizeDoesNotReblockBorrowedFile(t *testing.T) {
	master, slave, err := cpty.Open()
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{Master: master, slave: slave}
	defer func() { _ = session.Close() }()
	// creack's File originated in os.OpenFile; File.Fd may still toggle its
	// kernel O_NONBLOCK even after its owner repaired that flag. This isolates
	// resize itself rather than depending on the new Allocate wrapper.
	masterPollControl(t, master, func(fd int) error { return unix.SetNonblock(fd, true) })
	masterRequirePollable(t, master)
	if err := session.SetSize(121, 39); err != nil {
		t.Fatal(err)
	}
	masterRequirePollable(t, master)
}

func TestMasterReadDeadlineAfterResizePreservesOriginalPair(t *testing.T) {
	exerciseMasterReadDeadline(t)
}

func exerciseMasterReadDeadline(t *testing.T) {
	t.Helper()
	session, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	masterRequirePollable(t, session.Master)
	if err := session.SetSize(133, 47); err != nil {
		t.Fatal(err)
	}
	masterRequirePollable(t, session.Master)
	type readResult struct {
		n   int
		err error
	}
	done := make(chan readResult, 1)
	go func() {
		var b [1]byte
		n, err := session.Master.Read(b[:])
		done <- readResult{n, err}
	}()
	if err := session.Master.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.n != 0 || !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("idle master read was not paused by deadline: %+v", result)
		}
	case <-time.After(5 * time.Second):
		// A broken blocking implementation cannot strand the test's thread:
		// the original slave is still open and releases the real syscall.
		_, _ = session.slave.Write([]byte("!"))
		<-done
		t.Fatal("master read ignored deadline after resize")
	}
	if err := session.Master.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.slave.Write([]byte("still-same-pair")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("still-same-pair"))
	if _, err := io.ReadFull(session.Master, got); err != nil || string(got) != "still-same-pair" {
		t.Fatalf("deadline closed/replaced original PTY: %q %v", got, err)
	}
}

func TestMasterStartedChildTailPrecedesKernelEIO(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	session, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	if err := session.Start([]string{"/bin/sh", "-c", "printf 'pty-tail-without-newline'; exit 23"}, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = session.Signal(syscall.SIGKILL)
			_, _ = session.Wait()
		}
	}()
	// Child output is deliberately less than PTY capacity. The exit closes
	// the last slave handle; reading after Wait must still return every byte.
	exit, err := session.Wait()
	waited = true
	if err != nil || exit != 23 {
		t.Fatalf("original child exit=%d err=%v", exit, err)
	}
	got, readErr := io.ReadAll(session.Master)
	if !bytes.Equal(got, []byte("pty-tail-without-newline")) || !errors.Is(readErr, syscall.EIO) {
		t.Fatalf("last-slave-close facts: bytes=%q err=%v; want complete tail then EIO", got, readErr)
	}
	if _, err := session.Master.Stat(); err != nil {
		t.Fatalf("EIO should not mean the owner closed its master: %v", err)
	}
}

func TestMasterPollableLifecycleReleasesResources(t *testing.T) {
	exerciseMasterReadDeadline(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for i := 0; i < rounds; i++ {
		exerciseMasterReadDeadline(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY pollable master lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "PTY pollable master lifecycle", beforeFD, 0)
}

type masterIdentity struct {
	dev, inode uint64
}

func masterPollIdentity(t *testing.T, file *os.File) masterIdentity {
	t.Helper()
	var stat unix.Stat_t
	masterPollControl(t, file, func(fd int) error { return unix.Fstat(fd, &stat) })
	return masterIdentity{uint64(stat.Dev), stat.Ino}
}

func masterRequirePollable(t *testing.T, file *os.File) {
	t.Helper()
	var status, descriptor int
	masterPollControl(t, file, func(fd int) error {
		var err error
		status, err = unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if err != nil {
			return err
		}
		descriptor, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		return err
	})
	if status&unix.O_NONBLOCK == 0 || descriptor&unix.FD_CLOEXEC == 0 {
		t.Fatalf("master is not a pollable owned descriptor: status=%#x descriptor=%#x", status, descriptor)
	}
}

func masterPollControl(t *testing.T, file *os.File, fn func(int) error) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var inner error
	if err := raw.Control(func(fd uintptr) { inner = fn(int(fd)) }); err != nil {
		t.Fatal(err)
	}
	if inner != nil {
		t.Fatal(inner)
	}
}

// origin: hot-update S4 plan §3.1 / T03
func TestPtyResizePreservesPollability(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePtyResizeDeadlines(t)
		exerciseTerminalOwnerDeadlines(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY resize duplex deadlines", beforeG)
	testharness.AssertNoFDLeak(t, "PTY resize duplex deadlines", beforeFD, 0)
}

func exerciseTerminalOwnerDeadlines(t *testing.T) {
	t.Helper()
	f := newTerminalFixture(t, "stty raw -echo; printf READY; exec sleep 30")
	ready := make([]byte, 5)
	if _, err := io.ReadFull(f.master, ready); err != nil || string(ready) != "READY" {
		t.Fatalf("raw slave readiness=%q err=%v", ready, err)
	}
	for _, resized := range []bool{false, true} {
		if resized {
			if err := f.master.Resize(119, 41); err != nil {
				t.Fatal(err)
			}
			observation, err := f.master.Observe()
			if err != nil || observation.Cols != 119 || observation.Rows != 41 {
				t.Fatalf("owner resize observation=%+v err=%v", observation, err)
			}
		}
		masterRequirePollable(t, f.master.Borrow())
		for _, write := range []bool{false, true} {
			deadline := time.Now().Add(50 * time.Millisecond)
			setDeadline := f.master.SetReadDeadline
			if write {
				setDeadline = f.master.SetWriteDeadline
			}
			if err := setDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			done := make(chan ownerPTYResult, 1)
			go func() {
				var n int
				var err error
				if write {
					n, err = f.master.Write(bytes.Repeat([]byte{'a'}, 1<<20))
				} else {
					var b [1]byte
					n, err = f.master.Read(b[:])
				}
				done <- ownerPTYResult{n, err}
			}()
			select {
			case result := <-done:
				if !errors.Is(result.err, os.ErrDeadlineExceeded) || (!write && result.code != 0) || (write && result.code >= 1<<20) {
					t.Fatalf("owner deadline write=%v resized=%v: %+v", write, resized, result)
				}
				if write && !resized && result.code <= 0 {
					t.Fatal("write fixture never filled raw slave kernel queue")
				}
			case <-time.After(2 * time.Second):
				_ = f.master.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("owner I/O did not join after explicit close")
				}
				t.Fatalf("owner deadline returned success without waking actual I/O: write=%v resized=%v", write, resized)
			}
			if err := setDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.finish(t)
}

func exercisePtyResizeDeadlines(t *testing.T) {
	t.Helper()
	for _, borrowed := range []bool{false, true} {
		var session *Session
		if borrowed {
			master, slave, err := cpty.Open()
			if err != nil {
				t.Fatal(err)
			}
			session = &Session{Master: master, slave: slave}
			masterPollControl(t, master, func(fd int) error { return unix.SetNonblock(fd, true) })
		} else {
			var err error
			session, err = Allocate(80, 24)
			if err != nil {
				t.Fatal(err)
			}
		}
		// Keep the original slave only as this allocator-level fixture. It is
		// raw and never consumes input, so a large master write really blocks.
		masterPollControl(t, session.slave, func(fd int) error {
			term, err := unix.IoctlGetTermios(fd, unix.TCGETS)
			if err != nil {
				return err
			}
			term.Lflag &^= unix.ECHO | unix.ICANON | unix.ISIG
			term.Oflag &^= unix.OPOST
			if err := unix.IoctlSetTermios(fd, unix.TCSETS, term); err != nil {
				return err
			}
			return unix.SetNonblock(fd, true)
		})
		identity := masterPollIdentity(t, session.Master)
		for _, resize := range []bool{false, true} {
			if resize {
				if err := session.SetSize(137, 43); err != nil {
					t.Fatal(err)
				}
				masterPollControl(t, session.slave, func(fd int) error {
					size, err := unix.IoctlGetWinsize(fd, unix.TIOCGWINSZ)
					if err == nil && (size.Col != 137 || size.Row != 43) {
						t.Fatalf("remote slave winsize=%+v", size)
					}
					return err
				})
			}
			masterRequirePollable(t, session.Master)
			if masterPollIdentity(t, session.Master) != identity {
				t.Fatal("resize replaced the original master")
			}
			assertMasterBlockedReadDeadline(t, session)
			assertMasterBlockedWriteDeadline(t, session)
			masterRequirePollable(t, session.Master)
		}
		_ = session.Close()
	}
}

func assertMasterBlockedReadDeadline(t *testing.T, session *Session) {
	t.Helper()
	if err := session.Master.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	done := make(chan ownerPTYResult, 1)
	go func() { var b [1]byte; n, err := session.Master.Read(b[:]); done <- ownerPTYResult{n, err} }()
	select {
	case result := <-done:
		if result.code != 0 || !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("real blocked read deadline=%+v", result)
		}
	case <-time.After(2 * time.Second):
		_, _ = session.slave.Write([]byte("!"))
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("read could not be released by original slave")
		}
		t.Fatal("real blocked read ignored deadline")
	}
	if err := session.Master.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func assertMasterBlockedWriteDeadline(t *testing.T, session *Session) {
	t.Helper()
	if err := session.Master.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	attempt := bytes.Repeat([]byte{'a'}, 1<<20)
	done := make(chan ownerPTYResult, 1)
	go func() { n, err := session.Master.Write(attempt); done <- ownerPTYResult{n, err} }()
	select {
	case result := <-done:
		if result.code <= 0 || result.code >= len(attempt) || !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("real blocked write deadline=%+v, attempted=%d", result, len(attempt))
		}
	case <-time.After(2 * time.Second):
		_ = session.slave.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("write could not be released by last slave close")
		}
		t.Fatal("real blocked write ignored deadline")
	}
	if err := session.Master.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Drain the raw slave's kernel input for the next before/after arm.
	masterPollControl(t, session.slave, func(fd int) error {
		var buffer [4096]byte
		for {
			_, err := unix.Read(fd, buffer[:])
			if errors.Is(err, unix.EAGAIN) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
}
