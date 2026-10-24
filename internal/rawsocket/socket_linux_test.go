//go:build linux

package rawsocket

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func rawPair(t *testing.T) (*Socket, *net.TCPConn, func()) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Connect(netip.MustParseAddrPort(ln.Addr().String()), time.Now().Add(3*time.Second), nil, nil)
	if err != nil {
		_ = ln.Close()
		t.Fatal(err)
	}
	if err = s.CompleteConnect(time.Now().Add(3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	peer, err := ln.AcceptTCP()
	_ = ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	return s, peer, func() { _ = s.Close(); _ = peer.Close() }
}
func rawFD(t *testing.T, s *Socket, fn func(int)) {
	t.Helper()
	if err := s.state.raw.Control(func(fd uintptr) { fn(int(fd)) }); err != nil {
		t.Fatal(err)
	}
}

// Real: TCP send-buffer pressure and one successful short syscall. No mock
// writer or timeout error is used as a substitute for positive partial progress.
// origin: hot-update S5 plan §3.1 / §3.2 / T08
func TestRawSocketPartialWriteAndDescriptorIdentity(t *testing.T) {
	s, peer, cleanup := rawPair(t)
	defer cleanup()
	rawFD(t, s, func(fd int) {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
			t.Fatal(err)
		}
	})
	payload := bytes.Repeat([]byte{0x51}, 1<<20)
	_ = s.SetWriteDeadline(time.Now().Add(time.Second))
	n, err := s.Write(payload)
	if err != nil || n <= 0 || n >= len(payload) {
		t.Fatalf("real partial write n=%d err=%v", n, err)
	}
	got := make([]byte, n)
	if _, err = io.ReadFull(peer, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload[:n]) {
		t.Fatal("short write progress differs from peer bytes")
	}
	if err = ValidateFile(s.state.file, s.Fingerprint()); err != nil {
		t.Fatal(err)
	}
	// net.TCPConn.File has a different File.Fd contract from os.NewFile:
	// Fd on this kind of file clears O_NONBLOCK on every duplicate of its OFD.
	peerFile, e := peer.File()
	if e != nil {
		t.Fatal(e)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peerFile.Close)
	peerRC, e := peerFile.SyscallConn()
	if e != nil {
		t.Fatal(e)
	}
	var peerFP Fingerprint
	if e = peerRC.Control(func(fd uintptr) {
		var stat unix.Stat_t
		if er := unix.Fstat(int(fd), &stat); er != nil {
			t.Error(er)
		}
		peerFP = Fingerprint{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT}
	}); e != nil {
		t.Fatal(e)
	}
	if e = ValidateFile(peerFile, peerFP); e != nil {
		t.Fatalf("read-only validation changed File flags: %v", e)
	}
	fp := s.Fingerprint()
	fp.Ino++
	if err = ValidateFile(s.state.file, fp); err == nil {
		t.Fatal("wrong original socket accepted")
	}
	other, _, closeOther := rawPair(t)
	defer closeOther()
	if ValidateFile(other.state.file, s.Fingerprint()) == nil {
		t.Fatal("swapped socket accepted")
	}
	f, err := os.CreateTemp(t.TempDir(), "regular")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(f.Close)
	if ValidateFile(f, s.Fingerprint()) == nil {
		t.Fatal("non-socket accepted")
	}
	rawFD(t, s, func(fd int) {
		flags, _ := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if flags&unix.O_NONBLOCK == 0 {
			t.Fatal("validation changed O_NONBLOCK through File.Fd")
		}
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags & ^unix.O_NONBLOCK)
	})
	if ValidateFile(s.state.file, s.Fingerprint()) == nil {
		t.Fatal("blocking descriptor accepted or repaired")
	}
	rawFD(t, s, func(fd int) {
		flags, _ := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if flags&unix.O_NONBLOCK != 0 {
			t.Fatal("validation repaired shared flags")
		}
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags|unix.O_NONBLOCK)
		_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0)
	})
	if ValidateFile(s.state.file, s.Fingerprint()) == nil {
		t.Fatal("inheritable descriptor accepted")
	}
	rawFD(t, s, func(fd int) { _, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC) })
}

// origin: hot-update S5 plan §8 / T51
func TestRawSocketFiveRoundLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRawSocketLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "raw socket lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "raw socket lifecycle", beforeFD, 0)
}
func exerciseRawSocketLifecycle(t *testing.T) {
	t.Helper()
	s, peer, cleanup := rawPair(t)
	defer cleanup()
	if f, err := s.ExportFile(); f != nil || !errors.Is(err, ErrBusy) {
		t.Fatalf("live export bypassed freeze: file=%v err=%v", f != nil, err)
	}
	if err := Hold(s); err != nil {
		t.Fatal(err)
	}
	f, err := BorrowHeldFile(s)
	if err != nil {
		t.Fatal(err)
	}
	// The live connector and image exporter must agree on kernel identity,
	// before any duplicate is passed through the candidate import path.
	// origin: hot-update S5 plan §3.2 / §5.1 / T08
	if exported, err := s.ExportFile(); err != nil || exported != f {
		t.Fatalf("live socket image identity: same=%v err=%v", exported == f, err)
	}
	rawFD(t, s, func(fd int) {
		dup, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			t.Fatal(e)
		}
		copyFile := os.NewFile(uintptr(dup), "flow-copy")
		if ValidateFile(copyFile, s.Fingerprint()) != nil {
			t.Fatal("duplicate identity changed")
		}
		got, err := TakeImageFile(&rawImageSource{file: copyFile}, "flow.copy", "raw", s.Fingerprint())
		if err != nil || got != copyFile {
			_ = copyFile.Close()
			t.Fatalf("live socket import identity: same=%v err=%v", got == copyFile, err)
		}
		adopted, err := Adopt(got, s.Fingerprint())
		if err != nil {
			_ = got.Close()
			t.Fatal(err)
		}
		_ = adopted.Close()
	})
	if f != s.state.file {
		t.Fatal("hold replaced the original file")
	}
	if err = Release(s); err != nil {
		t.Fatal(err)
	}
	if f, err := s.ExportFile(); f != nil || !errors.Is(err, ErrBusy) {
		t.Fatalf("released export bypassed freeze: file=%v err=%v", f != nil, err)
	}
	if _, err = s.Write([]byte("alive")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err = io.ReadFull(peer, got); err != nil || string(got) != "alive" {
		t.Fatalf("original peer: %q %v", got, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if f, err := s.ExportFile(); f != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed export returned ownership: file=%v err=%v", f != nil, err)
	}
}
