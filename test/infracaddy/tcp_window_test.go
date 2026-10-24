//go:build infracaddy_integration && linux && amd64

package infracaddy_test

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"golang.org/x/sys/unix"
)

func constrainBlockedReceiver(t *testing.T, tcp *net.TCPConn) {
	t.Helper()
	// A post-handshake 1 KiB buffer traps Linux rcv_ssthresh below one loopback
	// segment. Growing SO_RCVBUF later leaves that threshold tiny and drains by
	// retransmission timers. Bound the unread receiver above a full segment;
	// the original 10 MiB stream still fills it and blocks the sender in-kernel.
	// origin: S9 internal review INFRA-SUITE-SUPPLIES F7
	if err := tcp.SetReadBuffer(128 << 10); err != nil {
		t.Fatal("cannot establish actual receiver backpressure")
	}
}

// This isolates the receiver window from TLS, WebSocket framing and Caddy.
// The same original finite payload must resume after genuine kernel pressure.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F7
func TestCaddyTCPBackpressureReleasesOriginalStream(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTCPBackpressureRelease(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TCP backpressure release", g)
	testharness.AssertNoFDLeak(t, "TCP backpressure release", fd, 0)
}

func exerciseTCPBackpressureRelease(t *testing.T) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	receiver, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receiver.Close() }()
	sender, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sender.Close() }()
	constrainBlockedReceiver(t, receiver)
	if err := sender.SetWriteBuffer(128 << 10); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("stock-writer-prefix:"), 32<<10)
	done := make(chan error, 1)
	go func() {
		for message := 0; message < 16; message++ {
			if _, err := io.Copy(sender, bytes.NewReader(payload)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() { _ = receiver.Close(); _ = sender.Close(); <-done }()
	control, err := sender.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	waitFixture(t, time.Second, func() bool {
		var info *unix.TCPInfo
		var socketErr error
		if err := control.Control(func(fd uintptr) { info, socketErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO) }); err != nil || socketErr != nil {
			t.Fatal("cannot prove blocked kernel TCP write")
		}
		return info.Notsent_bytes > 0 && info.Snd_wnd == 0
	})
	if err := receiver.SetReadBuffer(4 << 20); err != nil {
		t.Fatal(err)
	}
	if err := receiver.SetReadDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	for message := 0; message < 16; message++ {
		body := make([]byte, len(payload))
		if n, err := io.ReadFull(receiver, body); err != nil || !bytes.Equal(body, payload) {
			t.Fatalf("original TCP bytes did not resume within budget: message=%d bytes=%d want=%d: %v", message, n, len(body), err)
		}
	}
}
