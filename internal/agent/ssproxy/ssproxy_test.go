package ssproxy

import (
	"bytes"
	"crypto/rand"
	"errors"
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
	port, err := srv.Start(0, []Key{
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
	port, _ := srv.Start(0, []Key{{KeyID: "alice", Secret: "good-psk"}})
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
	port, _ := srv.Start(0, nil)
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
	port, _ := srv.Start(0, []Key{
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
	port, _ := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
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
	port, _ := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
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
	port, _ := srv.Start(0, []Key{{KeyID: "keep", Secret: "keep-psk"}})
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

// startSilentUpstream accepts TCP connections and then does NOTHING with them: it never
// reads, never writes, and never closes. It models an upstream that ignores a half-close
// (a long-poll endpoint, an idle SSH session, a hostile peer) — the shape that made Stop()
// unbounded before this increment.
func startSilentUpstream(t *testing.T) (addr string, accepted <-chan struct{}, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("silent upstream listen: %v", err)
	}
	got := make(chan struct{}, 8)
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // hold it open; deliberately never read/write/close
			mu.Unlock()
			select {
			case got <- struct{}{}:
			default:
			}
		}
	}()
	return ln.Addr().String(), got, func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	}
}

// origin: 2026-08-21 weilandserver incident, plan A2 (docs/reviews/proxy-lifecycle-plan.md).
// shutdown() used to close only ACCEPTED conns, so relay's second copy direction stayed
// blocked in remote.Read() and Stop()'s wg.Wait() never returned. Stop() is called while the
// agent holds p.mu, so one idle upstream could wedge the entire proxy runtime — and this
// increment makes teardown-then-rebuild a routine path rather than a rare one.
func TestStopIsBoundedWhenUpstreamIgnoresHalfClose(t *testing.T) {
	upstreamAddr, accepted, stopUpstream := startSilentUpstream(t)
	defer stopUpstream()

	srv := New(nil)
	port, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	cl := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "psk", upstreamAddr)
	defer func() { _ = cl.conn.Close() }()
	cl.send(t, []byte("wake the relay"))

	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never saw the relay dial; test did not reach the condition it asserts")
	}

	// The deadline MUST be well under dialTimeout (10s). origin: proxy-lifecycle internal
	// review MAJOR (concurrency lane) — the first cut used exactly 10s, which is dialTimeout,
	// so a Stop() that sat out a full upstream dial passed it. A bound that equals the thing
	// you are trying to bound cannot fail. 3s is comfortably above the real cost here (the
	// relay is already established; shutdown only has to close two conns and join two copies)
	// and far below the pathological case.
	done := make(chan struct{})
	go func() { srv.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3s: the upstream conn is not in shutdown's close set, " +
			"so relay's remote->client copy is still blocked in Read (this is the p.mu wedge)")
	}
}

// origin: proxy-lifecycle external review F2
// Revocation promises to force-close every in-flight connection owned by the removed key.
// Closing only the accepted/client half is insufficient when the upstream ignores the resulting
// half-close: the remote->client copy remains blocked and retains both sockets plus the handler.
func TestRevocationReclaimsSilentUpstreamConnections(t *testing.T) {
	upstreamAddr, accepted, stopUpstream := startSilentUpstream(t)
	defer stopUpstream()

	srv := New(nil)
	port, err := srv.Start(0, []Key{{KeyID: "revoked", Secret: "psk"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	cl := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "psk", upstreamAddr)
	defer func() { _ = cl.conn.Close() }()
	cl.send(t, []byte("establish relay"))

	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		srv.Stop()
		t.Fatal("silent upstream was never reached; test did not establish an in-flight relay")
	}
	// The upstream Accept can win just before handleConn registers the remote and starts relay.
	// Wait for the response salt: it is written only after remote is tracked and immediately
	// before relay begins, so revoking earlier cannot accidentally test the pre-relay cleanup path.
	_ = cl.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	responseSalt := make([]byte, saltSize)
	if _, err := io.ReadFull(cl.conn, responseSalt); err != nil {
		srv.Stop()
		t.Fatalf("relay did not reach its established state: %v", err)
	}
	_ = cl.conn.SetReadDeadline(time.Time{})

	if err := srv.SetKeys(nil); err != nil {
		srv.Stop()
		t.Fatalf("revoke key: %v", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		remaining := len(srv.allConns)
		srv.mu.Unlock()
		if remaining == 0 {
			srv.Stop()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	srv.mu.Lock()
	remaining := len(srv.allConns)
	srv.mu.Unlock()
	srv.Stop()
	if remaining != 0 {
		t.Fatalf("revoking a key left %d accepted/upstream connections tracked: the accepted socket was "+
			"closed, but the silent upstream remains blocked in relay and pins sockets plus a handler "+
			"until the whole proxy server stops", remaining)
	}
}

// origin: 2026-08-21 weilandserver incident, plan A1 (docs/reviews/proxy-lifecycle-plan.md).
// Start's `if s.ln != nil` early return used to sit BEFORE the `if s.closed` check, and
// shutdown() closes the listener without nil'ing it — so Start on a stopped Server returned
// (oldPort, nil): a SUCCESS handing back a dead listener. Unreachable while every caller
// constructed a fresh New(), which this increment stops guaranteeing.
func TestStartOnStoppedServerFailsInsteadOfReturningDeadPort(t *testing.T) {
	srv := New(nil)
	port, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
	if err != nil || port == 0 {
		t.Fatalf("first start: port=%d err=%v", port, err)
	}
	srv.Stop()

	got, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
	if err == nil {
		t.Fatalf("Start on a stopped server returned (%d, nil) — a dead listener reported as success", got)
	}
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("want ErrStopped, got %v", err)
	}
	if got == port {
		t.Fatalf("Start returned the pre-Stop port %d alongside its error; callers that ignore the "+
			"error would bind nothing and believe they had", got)
	}
}

// Serving must answer "can I take traffic right now", not "was I ever started" — the
// distinction the incident turned on: p.srv != nil stayed true for 7h40m after the server died.
func TestServingIsFalseOnceStopped(t *testing.T) {
	srv := New(nil)
	if srv.Serving() {
		t.Fatal("a never-started server must not report Serving")
	}
	if _, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !srv.Serving() {
		t.Fatal("a started server must report Serving")
	}
	srv.Stop()
	if srv.Serving() {
		t.Fatal("a stopped server must NOT report Serving — this predicate is what the agent uses " +
			"to decide whether to advertise the node as a live exit")
	}
	// SetKeys must agree with Serving, since that is the signal the agent actually acts on.
	if err := srv.SetKeys([]Key{{KeyID: "k2", Secret: "psk2"}}); !errors.Is(err, ErrStopped) {
		t.Fatalf("SetKeys on a stopped server: want ErrStopped, got %v", err)
	}
}

// allConns now holds UPSTREAM conns as well as accepted ones (that is what makes Stop bounded).
// A missing untrack would therefore leak one map entry per relayed connection for the life of
// the server — invisible to a goroutine count, and unbounded on a busy exit.
//
// origin: proxy-lifecycle internal review MAJOR (concurrency lane).
func TestRelayedConnectionsDrainFromTheTrackingMap(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()

	srv := New(nil)
	port, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	srvAddr := net.JoinHostPort("127.0.0.1", itoa(port))

	const rounds = 6
	for i := 0; i < rounds; i++ {
		cl := dialSS(t, srvAddr, "psk", echoAddr)
		cl.send(t, []byte("x"))
		_ = cl.recv(t, 1)
		_ = cl.conn.Close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		srv.mu.Lock()
		n := len(srv.allConns)
		srv.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("allConns still holds %d entries after %d completed relays; every tracked conn "+
				"(accepted AND upstream) must be untracked when its relay ends, or the map grows "+
				"without bound on a live exit", n, rounds)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// acceptExited is the half of Serving() that catches an accept loop which died on a
// non-temporary listener error while `closed` stayed false — the one corpse cause SetKeys
// cannot see, because SetKeys only consults `closed`.
//
// origin: proxy-lifecycle internal review MAJOR (tests lane); moved into this package by
// external review F6 so the helper that constructs the state can stay unexported.
func TestServingIsFalseWhenAcceptLoopDiesWithoutClose(t *testing.T) {
	srv := New(nil)
	if _, err := srv.Start(0, []Key{{KeyID: "k", Secret: "psk"}}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Stop()
	if !srv.Serving() {
		t.Fatal("precondition: server should be serving after Start")
	}

	// Kill ONLY the listener, the way a non-temporary Accept error would.
	srv.closeListenerForTest()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Serving() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if srv.Serving() {
		t.Fatal("Serving() still true after the accept loop exited: without the acceptExited latch " +
			"a server with no accept loop reports itself healthy")
	}
	if err := srv.SetKeys(nil); err != nil {
		t.Fatalf("SetKeys must still SUCCEED here (closed==false) — that is precisely why Serving "+
			"needs its own latch rather than deferring to the SetKeys error; got %v", err)
	}
}
