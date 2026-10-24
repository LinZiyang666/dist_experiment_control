//go:build darwin

package ssproxy

import (
	"bytes"
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

// Real: the legacy SS listener, AEAD client and TCP targets, including revoke.
// No stubs; this does not claim independent cipher or same-PID exec coverage.
// origin: hot-update S5 plan §7.4 / T46
func TestLegacySSDarwinLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("legacy", func(t *testing.T) { exerciseLegacySSDarwin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "Darwin legacy SS", goroutines)
	// FDCount is unmeasurable on Darwin. Stop/Serving, original-connection EOF
	// and exact port reuse below provide the portable lifecycle observations.
	testharness.AssertNoFDLeak(t, "Darwin legacy SS", fds, 0)

	t.Run("image-unsupported", func(t *testing.T) {
		server := New(testharness.SilentLog())
		if _, err := server.Start(0, []Key{{KeyID: "idle", Secret: "darwin-idle-psk"}}); err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		frozen, err := server.Freeze(time.Now().Add(time.Second))
		unsupported := errors.Is(err, handoff.ErrUnsupported) || errors.Is(err, rawsocket.ErrUnsupported) || errors.Is(err, sealedstate.ErrUnsupported)
		if frozen != nil || !unsupported || errors.Is(err, ErrNotImplemented) {
			t.Fatalf("Darwin Freeze must refuse an idle listener image with a platform error: frozen=%t error=%v", frozen != nil, err)
		}
		if !server.Serving() {
			t.Fatal("unsupported image cut stopped the legacy SS listener")
		}
	})
}

func exerciseLegacySSDarwin(t *testing.T) {
	t.Helper()
	targetAddr, stopTarget := startDarwinSSTarget(t)
	defer stopTarget()
	server := New(testharness.SilentLog())
	keys := []Key{{KeyID: "alice", Secret: "darwin-alice-psk"}, {KeyID: "bob", Secret: "darwin-bob-psk"}}
	port, err := server.Start(0, keys)
	if err != nil || port == 0 || !server.Serving() {
		t.Fatalf("legacy SS must start without Linux replay: port=%d error=%v", port, err)
	}
	defer server.Stop()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	alice := dialSS(t, addr, keys[0].Secret, targetAddr)
	defer func() { _ = alice.conn.Close() }()
	bob := dialSS(t, addr, keys[1].Secret, targetAddr)
	defer func() { _ = bob.conn.Close() }()
	for _, client := range []*ssClient{alice, bob} {
		if err := client.conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte{0, 0xff, 1, 2}, 8192)
		client.send(t, payload)
		if got := client.recv(t, len(payload)); !bytes.Equal(got, payload) {
			t.Fatal("legacy SS duplex bytes differ")
		}
	}
	// Alice is silent after a complete round trip; only the product revokes her.
	// Bob's original connection must continue with its existing AEAD state.
	if err := server.SetKeys(keys[1:]); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	n, err := alice.conn.Read(one[:])
	var timeout net.Error
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatalf("legacy key removal did not close silent Alice: n=%d error=%v", n, err)
	}
	payload := []byte("bob-after-revoke\x00\xff")
	bob.send(t, payload)
	if got := bob.recv(t, len(payload)); !bytes.Equal(got, payload) {
		t.Fatal("legacy revoke interrupted Bob's original connection")
	}
	server.Stop()
	if server.Serving() {
		t.Fatal("stopped legacy SS still reports Serving")
	}
	n, err = bob.conn.Read(one[:])
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatalf("Stop did not close Bob's original connection: n=%d error=%v", n, err)
	}
	if err := server.SetKeys(keys); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped legacy server accepted keys: %v", err)
	}
	if _, err := server.Start(port, keys); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped legacy server returned false Start success: %v", err)
	}
	reused, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Stop did not release the SS listener: %v", err)
	}
	_ = reused.Close()
}

func startDarwinSSTarget(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	conns := make(map[net.Conn]struct{})
	acceptDone := make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[conn] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				// Keep fixture-only Linux splice pipe caches out of the FD oracle.
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
			}()
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		// Join accept before closing its published sockets so none can be missed.
		<-acceptDone
		mu.Lock()
		for conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	}
}
