package ssproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// ssClient is a minimal Shadowsocks AEAD client used only by these tests to
// exercise the server end to end (same crypto primitives, opposite role).
type ssClient struct {
	conn   net.Conn
	master []byte
	w      *aeadWriter
	r      *aeadReader // lazily built after the response salt arrives
}

func dialSS(t *testing.T, serverAddr, password, target string) *ssClient {
	t.Helper()
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		t.Fatalf("dial ss server: %v", err)
	}
	master := evpBytesToKey(password, keySize)
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("rand salt: %v", err)
	}
	if _, err := conn.Write(salt); err != nil {
		t.Fatalf("write salt: %v", err)
	}
	sk, err := sessionSubkey(master, salt)
	if err != nil {
		t.Fatalf("subkey: %v", err)
	}
	aead, err := chacha20poly1305.New(sk)
	if err != nil {
		t.Fatalf("aead: %v", err)
	}
	cl := &ssClient{conn: conn, master: master, w: newAEADWriter(conn, aead)}
	// First chunk: the SOCKS target header (domain ATYP).
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("split target: %v", err)
	}
	pn, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	p := uint16(pn)
	hdr := []byte{0x03, byte(len(host))}
	hdr = append(hdr, host...)
	hdr = append(hdr, byte(p>>8), byte(p))
	if _, err := cl.w.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	return cl
}

func (cl *ssClient) send(t *testing.T, b []byte) {
	t.Helper()
	if _, err := cl.w.Write(b); err != nil {
		t.Fatalf("ss send: %v", err)
	}
}

func (cl *ssClient) recv(t *testing.T, n int) []byte {
	t.Helper()
	if cl.r == nil {
		rsalt := make([]byte, saltSize)
		if _, err := io.ReadFull(cl.conn, rsalt); err != nil {
			t.Fatalf("read resp salt: %v", err)
		}
		sk, err := sessionSubkey(cl.master, rsalt)
		if err != nil {
			t.Fatalf("resp subkey: %v", err)
		}
		aead, err := chacha20poly1305.New(sk)
		if err != nil {
			t.Fatalf("resp aead: %v", err)
		}
		cl.r = newAEADReader(cl.conn, aead)
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(cl.r, out); err != nil {
		t.Fatalf("ss recv: %v", err)
	}
	return out
}

// startEcho starts a TCP echo server and returns its addr + a closer.
func startEcho(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close(); close(done) }
}

func TestServerRoundTripMultiKey(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	srv := New(nil)
	port, err := srv.Start(context.Background(), 0, []Key{
		{KeyID: "alice", Secret: "alice-psk-aaaa"},
		{KeyID: "bob", Secret: "bob-psk-bbbb"},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))

	for _, tc := range []struct{ name, pw string }{
		{"alice", "alice-psk-aaaa"},
		{"bob", "bob-psk-bbbb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := dialSS(t, srvAddr, tc.pw, echoAddr)
			defer func() { _ = cl.conn.Close() }()
			msg := []byte("hello-" + tc.name)
			cl.send(t, msg)
			got := cl.recv(t, len(msg))
			if !bytes.Equal(got, msg) {
				t.Fatalf("echo mismatch: got %q want %q", got, msg)
			}
		})
	}
}

func TestServerRejectsUnknownKey(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, []Key{{KeyID: "alice", Secret: "good-psk"}})
	defer srv.Stop()
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))

	cl := dialSS(t, srvAddr, "WRONG-psk", echoAddr)
	defer func() { _ = cl.conn.Close() }()
	assertRejected(t, cl, "unknown key")
}

// assertRejected proves the server refused the handshake: either the follow-up
// write fails (the server already closed = early broken pipe, a valid
// rejection) or no response salt ever arrives. Tolerating the write error
// removes a timing flake (reviewer suggestion).
func assertRejected(t *testing.T, cl *ssClient, why string) {
	t.Helper()
	if _, err := cl.w.Write([]byte("payload")); err != nil {
		return // server closed mid-write — rejected
	}
	_ = cl.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	rsalt := make([]byte, saltSize)
	if _, err := io.ReadFull(cl.conn, rsalt); err == nil {
		t.Fatalf("server accepted a handshake it must reject (%s)", why)
	}
}

func TestServerEmptyKeysetRejectsAll(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, nil)
	defer srv.Stop()
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))

	cl := dialSS(t, srvAddr, "any-psk", echoAddr)
	defer func() { _ = cl.conn.Close() }()
	assertRejected(t, cl, "empty keyset")
}

func TestRevokeForceClosesInflight(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, []Key{
		{KeyID: "alice", Secret: "alice-psk"},
		{KeyID: "bob", Secret: "bob-psk"},
	})
	defer srv.Stop()
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))

	// Establish a live conn for each subscriber and prove both work.
	clA := dialSS(t, srvAddr, "alice-psk", echoAddr)
	defer func() { _ = clA.conn.Close() }()
	clB := dialSS(t, srvAddr, "bob-psk", echoAddr)
	defer func() { _ = clB.conn.Close() }()
	clA.send(t, []byte("a1"))
	if got := clA.recv(t, 2); !bytes.Equal(got, []byte("a1")) {
		t.Fatalf("A pre-revoke echo: %q", got)
	}
	clB.send(t, []byte("b1"))
	if got := clB.recv(t, 2); !bytes.Equal(got, []byte("b1")) {
		t.Fatalf("B pre-revoke echo: %q", got)
	}

	// Revoke bob: his in-flight conn must be force-closed; alice unaffected.
	if err := srv.SetKeys([]Key{{KeyID: "alice", Secret: "alice-psk"}}); err != nil {
		t.Fatalf("setkeys: %v", err)
	}
	_ = clB.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := clB.conn.Read(buf); err == nil {
		t.Fatalf("bob's connection should be force-closed after revoke")
	}

	clA.send(t, []byte("a2"))
	if got := clA.recv(t, 2); !bytes.Equal(got, []byte("a2")) {
		t.Fatalf("A post-revoke echo: %q", got)
	}
}

func TestStopIdempotentAndNoLeak(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	before := runtime.NumGoroutine()

	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, []Key{{KeyID: "k", Secret: "psk"}})
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))
	for i := 0; i < 5; i++ {
		cl := dialSS(t, srvAddr, "psk", echoAddr)
		cl.send(t, []byte("x"))
		_ = cl.recv(t, 1)
		_ = cl.conn.Close()
	}
	srv.Stop()
	srv.Stop() // idempotent

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+3 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, n)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// A payload larger than one AEAD chunk (maxChunk=16383) must round-trip
// correctly through the multi-chunk length-framing on both directions.
func TestServerLargePayloadSpansChunks(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, []Key{{KeyID: "k", Secret: "psk"}})
	defer srv.Stop()
	cl := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "psk", echoAddr)
	defer func() { _ = cl.conn.Close() }()

	// 100 KiB → ~7 chunks each way; mix bytes so a dropped/duplicated chunk shows.
	payload := make([]byte, 100*1024)
	for i := range payload {
		payload[i] = byte(i*7 + 3)
	}
	writeErr := make(chan error, 1)
	go func() { _, err := cl.w.Write(payload); writeErr <- err }()
	got := cl.recv(t, len(payload))
	if err := <-writeErr; err != nil {
		t.Fatalf("ss send: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("large payload mismatch: got %d bytes", len(got))
	}
}

// SetKeys must be safe to call concurrently with live traffic and repeated
// adds/removes — the -race detector is the real assertion here.
func TestConcurrentSetKeysDuringTraffic(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	srv := New(nil)
	port, _ := srv.Start(context.Background(), 0, []Key{{KeyID: "keep", Secret: "keep-psk"}})
	defer srv.Stop()
	addr := net.JoinHostPort("127.0.0.1", itoa(port))

	// One long-lived "keep" connection that must survive every key swap.
	keep := dialSS(t, addr, "keep-psk", echoAddr)
	defer func() { _ = keep.conn.Close() }()
	keep.send(t, []byte("k0"))
	_ = keep.recv(t, 2)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Churn the key set: keep "keep" always present, add/drop a transient.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			keys := []Key{{KeyID: "keep", Secret: "keep-psk"}}
			if i%2 == 0 {
				keys = append(keys, Key{KeyID: "tmp", Secret: "tmp-psk"})
			}
			_ = srv.SetKeys(keys)
		}
	}()
	// Drive traffic on "keep" while the churn runs.
	for i := 0; i < 50; i++ {
		keep.send(t, []byte("kk"))
		if got := keep.recv(t, 2); !bytes.Equal(got, []byte("kk")) {
			close(stop)
			wg.Wait()
			t.Fatalf("keep conn corrupted during key churn at i=%d: %q", i, got)
		}
	}
	close(stop)
	wg.Wait()
}
