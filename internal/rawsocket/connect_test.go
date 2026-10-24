//go:build linux

package rawsocket

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Real: kernel EINPROGRESS and refused-connect SO_ERROR; validation and a held
// duplicate precede the unique completion. The test never creates a new target.
// origin: hot-update S5 plan §3.2 / T09
func TestConnectSOErrorHasOneConsumerAcrossCut(t *testing.T) {
	for round := 0; round < 5; round++ {
		t.Run("completed_while_connector_paused", exerciseCompletedConnectAfterPause)
		t.Run("pending_keeps_original_deadline", exercisePendingConnectDeadline)
	}
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := netip.MustParseAddrPort(ln.Addr().String())
	called := 0
	s, err := Connect(address, time.Now().Add(time.Second), nil, func(tuple Tuple) error {
		called++
		if tuple.Local.Port() == 0 || tuple.Remote != address {
			t.Fatal("connect before tuple reservation")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(s.Close)
	if s.state.connectState != ConnectConnecting {
		t.Fatal("missing real EINPROGRESS precondition")
	}
	if called != 1 {
		t.Fatal("before-connect callback was not unique")
	}
	if err = Hold(s); err != nil {
		t.Fatal(err)
	}
	f, err := BorrowHeldFile(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateFile(f, s.Fingerprint()); err != nil {
		t.Fatal(err)
	}
	if err = Release(s); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteConnect(time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	peer, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	_ = ln.Close()
	// The port is now refused. Its pending error must survive repeated read-only
	// ValidateFile calls, then survive an image-style wrapper replacement.
	bad, err := Connect(address, time.Now().Add(time.Second), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(bad.Close)
	rawFD(t, bad, func(fd int) {
		p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
		if n, e := unix.Poll(p, 1000); e != nil || n == 0 {
			t.Fatalf("refused socket not ready: %d %v", n, e)
		}
	})
	if err = ValidateFile(bad.state.file, bad.Fingerprint()); err != nil {
		t.Fatal(err)
	}
	err = bad.CompleteConnect(time.Now().Add(time.Second), nil)
	if !errors.Is(err, unix.ECONNREFUSED) {
		t.Fatalf("SO_ERROR was consumed by validation: %v", err)
	}
	if second := bad.CompleteConnect(time.Now().Add(time.Second), nil); !errors.Is(second, unix.ECONNREFUSED) {
		t.Fatalf("failed connect was retried: %v", second)
	}
	phase, consumed, tuple, deadline, _ := ConnectReceipt(bad)
	if err = Hold(bad); err != nil {
		t.Fatal(err)
	}
	var copyFile *os.File
	rawFD(t, bad, func(fd int) {
		dup, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			t.Fatal(e)
		}
		copyFile = os.NewFile(uintptr(dup), "connecting-copy")
	})
	restored, err := Adopt(copyFile, bad.Fingerprint())
	if err != nil {
		_ = copyFile.Close()
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(restored.Close)
	if err = RestoreConnectReceipt(restored, phase, consumed, uint64(unix.ECONNREFUSED), tuple, deadline); err != nil {
		t.Fatal(err)
	}
	if err = Release(restored); err != nil {
		t.Fatal(err)
	}
	if err = restored.CompleteConnect(time.Now().Add(time.Second), nil); !errors.Is(err, unix.ECONNREFUSED) {
		t.Fatalf("cut lost failed connect receipt: %v", err)
	}
	if restored.Fingerprint() != bad.Fingerprint() {
		t.Fatal("cut redialed the failed socket")
	}
}

// A synthetic connecting receipt on a real full socket proves that checking a
// ready result first does not renew the original wait budget or ignore Stop.
// origin: hot-update S5 plan §3.2 / T09 / DRILL-RUN 75.A
func exercisePendingConnectDeadline(t *testing.T) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(pair[0]), "pending-deadline-control")
	defer func() { _ = unix.Close(pair[1]) }()
	var stat unix.Stat_t
	if err := unix.Fstat(pair[0], &stat); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	fp := Fingerprint{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT}
	s, err := Adopt(file, fp)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	buffer := make([]byte, 32768)
	for {
		if _, err := unix.Write(pair[0], buffer); errors.Is(err, unix.EAGAIN) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(-time.Second)
	if err := RestoreConnectReceipt(s, ConnectConnecting, false, 0, Tuple{}, deadline); err != nil {
		t.Fatal(err)
	}
	if err := Release(s); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := s.CompleteConnect(time.Now().Add(time.Second), nil); !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(started) > 50*time.Millisecond {
		t.Fatalf("pending connector renewed its expired budget: %v", err)
	}
	stop := make(chan struct{})
	close(stop)
	if err := s.CompleteConnect(time.Now().Add(time.Second), stop); !errors.Is(err, ErrBusy) {
		t.Fatalf("connector ignored Stop: %v", err)
	}
	phase, consumed, _, original, _ := ConnectReceipt(s)
	if phase != ConnectConnecting || consumed || original != deadline || s.Fingerprint() != fp {
		t.Fatal("pending timeout consumed SO_ERROR or replaced responsibility")
	}
}

// The kernel finishes the original SYN before its deadline, while the cut
// prevents the unique SO_ERROR consumer from running until after it. The
// deadline must still bound waiting, rather than erase an already-ready result.
// origin: hot-update S5 plan §3.2 / §9.1 / T09 / DRILL-RUN 75.A
func exerciseCompletedConnectAfterPause(t *testing.T) {
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	deadline := time.Now().Add(100 * time.Millisecond)
	s, err := Connect(netip.MustParseAddrPort(ln.Addr().String()), deadline, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if s.state.connectState != ConnectConnecting {
		t.Fatal("missing original EINPROGRESS receipt")
	}
	_ = ln.SetDeadline(deadline)
	peer, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	fp := s.Fingerprint()
	if err := Hold(s); err != nil {
		t.Fatal(err)
	}
	file, err := BorrowHeldFile(s)
	if err != nil || ValidateFile(file, fp) != nil {
		t.Fatal("cut lost the original pending socket")
	}
	time.Sleep(max(time.Until(deadline), 0) + time.Millisecond)
	if err := Release(s); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteConnect(time.Now().Add(time.Second), nil); err != nil {
		t.Fatalf("completed original connect was discarded after pause: %v", err)
	}
	phase, consumed, _, original, err := ConnectReceipt(s)
	if err != nil || phase != ConnectConnected || !consumed || original != deadline || s.Fingerprint() != fp {
		t.Fatal("completion renewed the deadline or replaced the original socket")
	}
	if err := s.CompleteConnect(time.Now().Add(-time.Second), nil); err != nil {
		t.Fatalf("completed receipt was consumed twice: %v", err)
	}
}
