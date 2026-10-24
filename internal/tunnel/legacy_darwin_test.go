//go:build darwin

package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real: TLS REGISTER, yamux, public listener and local TCP bytes. No stubs.
// Legacy availability is not an image-continuity or Darwin exec receipt.
// origin: hot-update S5 plan §7.4 / T46
func TestLegacyExposeDarwinLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("legacy", func(t *testing.T) { exerciseLegacyExposeDarwin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "Darwin legacy expose", goroutines)
	// The shared FD counter is unmeasurable on Darwin; listener reuse and EOF
	// below still prove that the product releases its observable socket owners.
	testharness.AssertNoFDLeak(t, "Darwin legacy expose", fds, 0)

	t.Run("image-unsupported", func(t *testing.T) {
		client := NewClient("127.0.0.1:1", "darwin", "darwin-1", nil, silentLog())
		frozen, err := client.FreezeFlows(time.Now().Add(time.Second))
		if frozen != nil || !darwinTunnelUnsupported(err) || errors.Is(err, ErrFlowNotImplemented) {
			t.Fatalf("Darwin FreezeFlows must refuse image ownership with a platform error: frozen=%t error=%v", frozen != nil, err)
		}
	})
}

func darwinTunnelUnsupported(err error) bool {
	return errors.Is(err, handoff.ErrUnsupported) || errors.Is(err, rawsocket.ErrUnsupported) || errors.Is(err, sealedstate.ErrUnsupported)
}

func exerciseLegacyExposeDarwin(t *testing.T) {
	t.Helper()
	localAddr, stopTarget := startDarwinExposeTarget(t)
	defer stopTarget()
	localPort := localAddr.(*net.TCPAddr).Port
	publicPort := findFreePort(t)
	const sid, nid, token = "darwin", "darwin-1", "darwin-legacy-token"
	lookupCalls := 0
	var lookupMu sync.Mutex
	server := NewServer("127.0.0.1:0", "127.0.0.1", func(gotSID, gotNID string, port int, hash string, epoch int64) error {
		lookupMu.Lock()
		defer lookupMu.Unlock()
		lookupCalls++
		if gotSID != sid || gotNID != nid || port != publicPort || hash != hashToken(token) || epoch != 0 {
			return errors.New("allocation mismatch")
		}
		return nil
	}, silentLog())
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: this round owns the legacy carrier lifecycle.
	defer cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("legacy listener start: %v", err)
	}
	defer server.Close()
	server.mu.Lock()
	listener := server.ln
	server.mu.Unlock()
	if listener == nil {
		t.Fatal("legacy Start returned success without a control listener")
	}
	controlAddr := listener.Addr().String()
	client := NewClient(controlAddr, sid, nid, func(int) (int, error) { return localPort, nil }, silentLog())
	client.Start(ctx)
	defer client.Close(publicPort)
	if err := client.Open(publicPort, localPort, token); err != nil {
		t.Fatalf("legacy Open must work without Linux replay: %v", err)
	}
	lookupMu.Lock()
	validated := lookupCalls
	lookupMu.Unlock()
	if validated == 0 {
		t.Fatal("legacy REGISTER never validated the allocation")
	}
	client.mu.Lock()
	session := client.sessions[publicPort]
	client.mu.Unlock()
	if session == nil {
		t.Fatal("legacy Open returned success without a carrier owner")
	}
	carrier := session.conn
	conn, ok := carrier.(*tls.Conn)
	if !ok || conn.ConnectionState().NegotiatedProtocol != "" {
		t.Fatal("Darwin legacy expose selected the Linux flow protocol")
	}
	public, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = public.Close() }()
	if err := public.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	greeting := []byte("legacy-target\x00\xff")
	got := make([]byte, len(greeting))
	if _, err := io.ReadFull(public, got); err != nil || !bytes.Equal(got, greeting) {
		t.Fatalf("raw first business bytes were framed, lost or changed: error=%v", err)
	}
	for _, payload := range [][]byte{[]byte("request-one\x00\xff"), bytes.Repeat([]byte{0xff, 0, 1, 2}, 8192)} {
		if n, err := public.Write(payload); err != nil || n != len(payload) {
			t.Fatalf("legacy public write: n=%d error=%v", n, err)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(public, got); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("legacy duplex bytes differ: error=%v", err)
		}
	}
	client.Close(publicPort)
	if client.HasSession(publicPort) {
		t.Fatal("legacy Close retained the session slot")
	}
	var one [1]byte
	n, err := public.Read(one[:])
	var timeout net.Error
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatalf("legacy Close did not close the original public connection: n=%d error=%v", n, err)
	}
	// Binding the exact released port observes cleanup without creating a new
	// business connection that could hide an old session's remaining responsibility.
	var reused net.Listener
	if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
		reused, err = net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)))
		return err == nil
	}) {
		t.Fatalf("legacy public listener was not released: %v", err)
	}
	_ = reused.Close()
}

func startDarwinExposeTarget(t *testing.T) (net.Addr, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted, done := make(chan net.Conn, 1), make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte("legacy-target\x00\xff")); err == nil {
			// Hide TCP's splice fast path: its cached pipe pairs belong to the
			// Linux validation fixture, not to the product's socket lifecycle.
			_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
		}
	}()
	return ln.Addr(), func() {
		_ = ln.Close()
		select {
		case conn := <-accepted:
			_ = conn.Close()
		case <-done:
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("legacy target worker did not join")
		}
	}
}
