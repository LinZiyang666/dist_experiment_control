package proxydial

import (
	"bufio"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return u
}

// fakeHTTPProxy is an in-process HTTP CONNECT proxy for tests.
type fakeHTTPProxy struct {
	ln         net.Listener
	wantAuth   string // if non-empty, require this Proxy-Authorization value
	rejectWith string // if non-empty, reply this status line instead of 200 (e.g. "407 Proxy Authentication Required")
	bundle     []byte // bytes written in the SAME write() as the 200 (exercises bufConn)

	mu        sync.Mutex
	gotTarget string
	gotAuth   string
}

func newFakeHTTPProxy(t *testing.T) *fakeHTTPProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeHTTPProxy{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go p.serve()
	return p
}

func (p *fakeHTTPProxy) addr() string { return p.ln.Addr().String() }

func (p *fakeHTTPProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *fakeHTTPProxy) handle(c net.Conn) {
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.gotTarget = req.Host
	p.gotAuth = req.Header.Get("Proxy-Authorization")
	p.mu.Unlock()

	if p.rejectWith != "" {
		_, _ = c.Write([]byte("HTTP/1.1 " + p.rejectWith + "\r\n\r\n"))
		_ = c.Close()
		return
	}
	if p.wantAuth != "" && req.Header.Get("Proxy-Authorization") != p.wantAuth {
		_, _ = c.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))
		_ = c.Close()
		return
	}
	// Write the 200 and (critically) any bundle bytes in a SINGLE write so they
	// may land in the same TCP segment — the bufConn regression.
	resp := append([]byte("HTTP/1.1 200 Connection established\r\n\r\n"), p.bundle...)
	if _, err := c.Write(resp); err != nil {
		_ = c.Close()
		return
	}
	// Echo subsequent client bytes so tests can verify the tunnel carries data.
	go func() {
		defer func() { _ = c.Close() }()
		buf := make([]byte, 4096)
		for {
			n, err := br.Read(buf)
			if n > 0 {
				if _, werr := c.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
}

func (p *fakeHTTPProxy) target() string { p.mu.Lock(); defer p.mu.Unlock(); return p.gotTarget }
func (p *fakeHTTPProxy) auth() string   { p.mu.Lock(); defer p.mu.Unlock(); return p.gotAuth }

func TestHTTPConnect_HappyAndHostnamePassed(t *testing.T) {
	p := newFakeHTTPProxy(t)
	d := &dialer{proxy: mustURL(t, "http://"+p.addr()), timeout: 2 * time.Second}

	// Target is a name that does NOT resolve in DNS — proves we never locally
	// resolve it (the fake-ip命门): if the dialer tried net.LookupHost it'd fail.
	conn, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if got := p.target(); got != "proxytest.invalid:4222" {
		t.Errorf("proxy received CONNECT target %q, want the hostname proxytest.invalid:4222 (not a pre-resolved IP)", got)
	}
	// Tunnel works (echo).
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != "ping" {
		t.Errorf("echo = %q", got)
	}
}

// TestHTTPConnect_BufConnNoLoss is the load-bearing regression: the proxy bundles
// "tunnel bytes" (simulating the NATS INFO line) in the SAME write as the 200,
// and the dialer's first Read must still yield them (bufConn must not drop the
// bytes bufio read ahead during response parsing).
func TestHTTPConnect_BufConnNoLoss(t *testing.T) {
	const info = "INFO {\"server_id\":\"x\"}\r\n"
	p := newFakeHTTPProxy(t)
	p.bundle = []byte(info)
	d := &dialer{proxy: mustURL(t, "http://"+p.addr()), timeout: 2 * time.Second}

	conn, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	got := make([]byte, len(info))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readFull(conn, got); err != nil {
		t.Fatalf("read bundled INFO: %v", err)
	}
	if string(got) != info {
		t.Fatalf("bundled tunnel bytes lost/garbled: got %q want %q (bufConn regression)", got, info)
	}
}

func TestHTTPConnect_RejectNoAuthLeak(t *testing.T) {
	p := newFakeHTTPProxy(t)
	p.rejectWith = "403 Forbidden"
	d := &dialer{proxy: mustURL(t, "http://user:s3cret@"+p.addr()), timeout: 2 * time.Second}

	_, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error should mention status: %v", err)
	}
	if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "Basic ") {
		t.Errorf("error must NOT leak credentials: %v", err)
	}
}

func TestHTTPConnect_AuthHeaderSent(t *testing.T) {
	p := newFakeHTTPProxy(t)
	p.wantAuth = "Basic dXNlcjpwYXNz" // base64("user:pass")
	d := &dialer{proxy: mustURL(t, "http://user:pass@"+p.addr()), timeout: 2 * time.Second}

	conn, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err != nil {
		t.Fatalf("dial with auth: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if p.auth() != p.wantAuth {
		t.Errorf("Proxy-Authorization = %q, want %q", p.auth(), p.wantAuth)
	}
}

// TestDial_NoProxyBypassesProxy: a NO_PROXY/localhost target is dialed directly,
// never through the proxy (proxy receives nothing).
func TestDial_LocalhostBypass(t *testing.T) {
	// Stand up a plain backend on loopback.
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backend.Close() }()
	go func() {
		c, err := backend.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("direct"))
		_ = c.Close()
	}()

	// Proxy URL points at a dead port — if the dialer wrongly used it, dial fails.
	d := &dialer{proxy: mustURL(t, "http://127.0.0.1:1"), timeout: 2 * time.Second}
	conn, err := d.Dial("tcp", backend.Addr().String()) // 127.0.0.1:NNN -> loopback bypass
	if err != nil {
		t.Fatalf("loopback target must dial directly (bypass proxy): %v", err)
	}
	defer func() { _ = conn.Close() }()
	got := make([]byte, 6)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "direct" {
		t.Errorf("got %q, want direct (proxy was bypassed)", got)
	}
}

// TestHTTPConnect_HTTPSProxyTLSBranch exercises the https:// proxy hop: the
// dialer wraps the hop in tls.Client before CONNECT. A plain (non-TLS) listener
// makes that handshake fail; assert the error is the TLS-to-proxy error and the
// conn is closed (no leak). (The happy path needs a CA the dialer trusts via
// system roots; a self-signed proxy cert can't be exercised without changing
// the dialer, so we cover the branch + error path here.)
func TestHTTPConnect_HTTPSProxyTLSBranch(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("not-a-tls-record\r\n")) // breaks the client TLS handshake
		<-done
		_ = c.Close()
	}()
	d := &dialer{proxy: mustURL(t, "https://"+ln.Addr().String()), timeout: 2 * time.Second}
	_, derr := d.Dial("tcp", "proxytest.invalid:4222")
	if derr == nil {
		t.Fatal("expected a TLS handshake failure to the https proxy")
	}
	if !strings.Contains(derr.Error(), "TLS") {
		t.Errorf("expected a TLS-to-proxy error, got: %v", derr)
	}
}

// TestHTTPConnect_HalfResponseTimeout: a proxy that writes a truncated status
// line then stalls must not hang the dial — the conn deadline (dialTimeout) must
// bound http.ReadResponse.
func TestHTTPConnect_HalfResponseTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		br := bufio.NewReader(c)
		_, _ = http.ReadRequest(br)
		_, _ = c.Write([]byte("HTTP/1.1 2")) // truncated; then stall
		<-done
	}()
	d := &dialer{proxy: mustURL(t, "http://"+ln.Addr().String()), timeout: 300 * time.Millisecond}
	start := time.Now()
	_, derr := d.Dial("tcp", "proxytest.invalid:4222")
	if derr == nil {
		t.Fatal("expected a timeout error on a stalled half-response")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("dial took %v, expected ~dialTimeout (deadline not honored)", elapsed)
	}
}

// readFull reads len(buf) bytes or errors.
func readFull(c net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := c.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
