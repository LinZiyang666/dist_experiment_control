//go:build darwin

package rawsocket_test

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
)

// Real: portable socket refusal bodies and a loopback listener. No stubs.
// This does not exercise Linux descriptor adoption or same-PID exec.
// origin: hot-update S5 plan §7.4 / §12.2 P23
func TestRawSocketDarwinImageUnsupported(t *testing.T) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr, err := netip.ParseAddrPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// A valid descriptor distinguishes platform refusal from nil-input rejection.
	file, err := os.CreateTemp(t.TempDir(), "socket-refusal")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	before, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Adopt", func(t *testing.T) {
		socket, err := rawsocket.Adopt(file, rawsocket.Fingerprint{})
		if socket != nil || !errors.Is(err, rawsocket.ErrUnsupported) {
			t.Fatalf("Darwin Adopt must refuse ownership with ErrUnsupported: socket=%t error=%v", socket != nil, err)
		}
	})
	t.Run("ValidateFile", func(t *testing.T) {
		if err := rawsocket.ValidateFile(file, rawsocket.Fingerprint{}); !errors.Is(err, rawsocket.ErrUnsupported) {
			t.Fatalf("Darwin ValidateFile must return ErrUnsupported: %v", err)
		}
	})
	t.Run("Connect", func(t *testing.T) {
		calls := 0
		socket, err := rawsocket.Connect(addr, time.Now().Add(time.Second), nil, func(rawsocket.Tuple) error {
			calls++
			return nil
		})
		if socket != nil || !errors.Is(err, rawsocket.ErrUnsupported) || calls != 0 {
			t.Fatalf("Darwin Connect must refuse before reservation or dial: socket=%t error=%v callbacks=%d", socket != nil, err, calls)
		}
		// No SYN may reach the target before the unsupported platform is rejected.
		if err := ln.SetDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		conn, err := ln.AcceptTCP()
		if conn != nil {
			_ = conn.Close()
			t.Fatal("unsupported Connect reached the target listener")
		}
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("target listener did not remain idle: %v", err)
		}
	})
	t.Run("CompleteConnect", func(t *testing.T) {
		if err := (&rawsocket.Socket{}).CompleteConnect(time.Now().Add(time.Second), nil); !errors.Is(err, rawsocket.ErrUnsupported) {
			t.Fatalf("Darwin CompleteConnect must return ErrUnsupported: %v", err)
		}
	})
	t.Run("ExportFile", func(t *testing.T) {
		file, err := (&rawsocket.Socket{}).ExportFile()
		if file != nil || !errors.Is(err, rawsocket.ErrUnsupported) {
			t.Fatalf("Darwin ExportFile must refuse an image descriptor: file=%t error=%v", file != nil, err)
		}
	})
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("unsupported adoption consumed or replaced the caller's descriptor: %v", err)
	}
}
