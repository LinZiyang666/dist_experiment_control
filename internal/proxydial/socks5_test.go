package proxydial

import (
	"bytes"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSOCKS5 is an in-process SOCKS5 proxy for tests.
type fakeSOCKS5 struct {
	ln       net.Listener
	wantUser string // if non-empty, require RFC1929 auth with these creds
	wantPass string
	rep      byte // REP code to return (0x00 success)

	mu        sync.Mutex
	gotATYP   byte
	gotDomain string
}

func newFakeSOCKS5(t *testing.T) *fakeSOCKS5 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeSOCKS5{ln: ln, rep: socks5RepOK}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *fakeSOCKS5) addr() string { return p.ln.Addr().String() }

func (p *fakeSOCKS5) handle(c net.Conn) {
	defer func() { _ = c.Close() }()
	// Greeting.
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil {
		return
	}
	methods := make([]byte, head[1])
	if _, err := io.ReadFull(c, methods); err != nil {
		return
	}
	if p.wantUser != "" {
		_, _ = c.Write([]byte{socks5Ver, socks5UserPass})
		// RFC1929: VER ULEN USER PLEN PASS.
		h := make([]byte, 2)
		if _, err := io.ReadFull(c, h); err != nil {
			return
		}
		user := make([]byte, h[1])
		_, _ = io.ReadFull(c, user)
		pl := make([]byte, 1)
		_, _ = io.ReadFull(c, pl)
		pass := make([]byte, pl[0])
		_, _ = io.ReadFull(c, pass)
		if string(user) != p.wantUser || string(pass) != p.wantPass {
			_, _ = c.Write([]byte{socks5AuthRFC, 0x01}) // failure
			return
		}
		_, _ = c.Write([]byte{socks5AuthRFC, 0x00}) // success
	} else {
		_, _ = c.Write([]byte{socks5Ver, socks5NoAuth})
	}

	// CONNECT request: VER CMD RSV ATYP ...
	rhead := make([]byte, 4)
	if _, err := io.ReadFull(c, rhead); err != nil {
		return
	}
	atyp := rhead[3]
	p.mu.Lock()
	p.gotATYP = atyp
	if atyp == socks5ATYPDom {
		l := make([]byte, 1)
		_, _ = io.ReadFull(c, l)
		dom := make([]byte, l[0])
		_, _ = io.ReadFull(c, dom)
		p.gotDomain = string(dom)
	}
	p.mu.Unlock()
	_, _ = io.ReadFull(c, make([]byte, 2)) // port

	// Reply: VER REP RSV ATYP=ipv4 BND(4) PORT(2).
	_, _ = c.Write([]byte{socks5Ver, p.rep, 0x00, socks5ATYPv4, 0, 0, 0, 0, 0, 0})
	if p.rep != socks5RepOK {
		return
	}
	// Echo subsequent bytes.
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *fakeSOCKS5) atyp() byte     { p.mu.Lock(); defer p.mu.Unlock(); return p.gotATYP }
func (p *fakeSOCKS5) domain() string { p.mu.Lock(); defer p.mu.Unlock(); return p.gotDomain }

func TestSOCKS5_HappyRemoteResolve(t *testing.T) {
	p := newFakeSOCKS5(t)
	d := &dialer{proxy: mustURL(t, "socks5://"+p.addr()), timeout: 2 * time.Second}

	conn, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	// 命门: target sent as a DOMAIN name (remote resolution), never an IP.
	if p.atyp() != socks5ATYPDom {
		t.Errorf("CONNECT ATYP = 0x%02x, want 0x03 (domain / remote resolve)", p.atyp())
	}
	if p.domain() != "proxytest.invalid" {
		t.Errorf("CONNECT domain = %q, want proxytest.invalid", p.domain())
	}
	// Tunnel echoes.
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 2)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readFull(conn, got); err != nil || string(got) != "hi" {
		t.Errorf("echo got=%q err=%v", got, err)
	}
}

func TestSOCKS5_AuthHappy(t *testing.T) {
	p := newFakeSOCKS5(t)
	p.wantUser, p.wantPass = "alice", "pw"
	d := &dialer{proxy: mustURL(t, "socks5://alice:pw@"+p.addr()), timeout: 2 * time.Second}
	conn, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err != nil {
		t.Fatalf("auth dial: %v", err)
	}
	_ = conn.Close()
}

func TestSOCKS5_AuthFailNoLeak(t *testing.T) {
	p := newFakeSOCKS5(t)
	p.wantUser, p.wantPass = "alice", "right"
	d := &dialer{proxy: mustURL(t, "socks5://alice:wrongpass@"+p.addr()), timeout: 2 * time.Second}
	_, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err == nil {
		t.Fatal("expected auth failure")
	}
	if strings.Contains(err.Error(), "wrongpass") {
		t.Errorf("error must not leak the password: %v", err)
	}
}

func TestSOCKS5_RepError(t *testing.T) {
	p := newFakeSOCKS5(t)
	p.rep = 0x04 // host unreachable (the classic fake-ip symptom)
	d := &dialer{proxy: mustURL(t, "socks5://"+p.addr()), timeout: 2 * time.Second}
	_, err := d.Dial("tcp", "proxytest.invalid:4222")
	if err == nil || !strings.Contains(err.Error(), "host unreachable") {
		t.Errorf("expected 'host unreachable' error, got %v", err)
	}
}

func TestSOCKS5_HostTooLong(t *testing.T) {
	d := &dialer{proxy: mustURL(t, "socks5://127.0.0.1:1080"), timeout: time.Second}
	long := strings.Repeat("a", 256) + ".example.com"
	_, err := d.Dial("tcp", long+":443")
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Errorf("hostname >255 must be rejected, got %v", err)
	}
}

// fakeConn feeds a scripted byte stream for Read and discards Writes.
type fakeConn struct{ r io.Reader }

func (c *fakeConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *fakeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) LocalAddr() net.Addr              { return nil }
func (c *fakeConn) RemoteAddr() net.Addr             { return nil }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

// scriptedSOCKS5 accepts one conn, performs the greeting (noauth), reads the
// CONNECT request (recording the raw ATYP+addr bytes), then writes a configurable
// reply followed by sentinel bytes — used to prove BND consumption alignment and
// the IP-literal ATYP wire form.
func scriptedSOCKS5(t *testing.T, reply, after []byte) (addr string, gotReq func() []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var mu sync.Mutex
	var reqAddr []byte
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		h := make([]byte, 2)
		if _, err := io.ReadFull(c, h); err != nil {
			return
		}
		_, _ = io.ReadFull(c, make([]byte, h[1]))
		_, _ = c.Write([]byte{socks5Ver, socks5NoAuth})
		rh := make([]byte, 4) // VER CMD RSV ATYP
		if _, err := io.ReadFull(c, rh); err != nil {
			return
		}
		var addrBytes []byte
		switch rh[3] {
		case socks5ATYPv4:
			addrBytes = make([]byte, 4)
		case socks5ATYPv6:
			addrBytes = make([]byte, 16)
		case socks5ATYPDom:
			l := make([]byte, 1)
			_, _ = io.ReadFull(c, l)
			addrBytes = make([]byte, l[0])
		}
		_, _ = io.ReadFull(c, addrBytes)
		_, _ = io.ReadFull(c, make([]byte, 2)) // port
		mu.Lock()
		reqAddr = append([]byte{rh[3]}, addrBytes...)
		mu.Unlock()
		_, _ = c.Write(append(append([]byte{}, reply...), after...))
	}()
	return ln.Addr().String(), func() []byte { mu.Lock(); defer mu.Unlock(); return reqAddr }
}

// TestSOCKS5_BNDAlignment proves socks5DrainBND consumes the BND.ADDR by ATYP
// (domain/IPv6/IPv4) so the very next bytes nats.go reads are aligned.
func TestSOCKS5_BNDAlignment(t *testing.T) {
	const sentinel = "SENTINEL-INFO-BYTES"
	cases := []struct {
		name  string
		reply []byte // VER REP RSV ATYP BND.ADDR BND.PORT
	}{
		{"domain_bnd", append([]byte{socks5Ver, socks5RepOK, 0x00, socks5ATYPDom, 9}, append([]byte("proxy.bnd"), 0, 0)...)},
		{"ipv6_bnd", append([]byte{socks5Ver, socks5RepOK, 0x00, socks5ATYPv6}, append(make([]byte, 16), 0, 0)...)},
		{"ipv4_bnd", []byte{socks5Ver, socks5RepOK, 0x00, socks5ATYPv4, 1, 2, 3, 4, 0, 80}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			addr, _ := scriptedSOCKS5(t, c.reply, []byte(sentinel))
			d := &dialer{proxy: mustURL(t, "socks5://"+addr), timeout: 2 * time.Second}
			conn, err := d.Dial("tcp", "proxytest.invalid:4222")
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer func() { _ = conn.Close() }()
			got := make([]byte, len(sentinel))
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := readFull(conn, got); err != nil {
				t.Fatalf("read sentinel: %v", err)
			}
			if string(got) != sentinel {
				t.Fatalf("BND misalignment for %s: post-handshake Read got %q, want %q", c.name, got, sentinel)
			}
		})
	}
}

// TestSOCKS5_IPLiteralATYP: an IP-literal target uses ATYP=0x01/0x04 (RFC1928),
// while a hostname uses ATYP=0x03 (the remote-resolve命门).
func TestSOCKS5_IPLiteralATYP(t *testing.T) {
	okReply := []byte{socks5Ver, socks5RepOK, 0x00, socks5ATYPv4, 0, 0, 0, 0, 0, 0}
	cases := []struct {
		target   string
		wantATYP byte
	}{
		{"192.0.2.1:443", socks5ATYPv4},
		{"[2001:db8::1]:443", socks5ATYPv6},
		{"broker.example:443", socks5ATYPDom},
	}
	for _, c := range cases {
		t.Run(c.target, func(t *testing.T) {
			addr, gotReq := scriptedSOCKS5(t, okReply, nil)
			d := &dialer{proxy: mustURL(t, "socks5://"+addr), timeout: 2 * time.Second}
			conn, err := d.Dial("tcp", c.target)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			_ = conn.Close()
			if req := gotReq(); len(req) == 0 || req[0] != c.wantATYP {
				t.Errorf("target %s: CONNECT ATYP = 0x%02x, want 0x%02x", c.target, firstByte(req), c.wantATYP)
			}
		})
	}
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0xff
	}
	return b[0]
}

// TestSOCKS5_AuthReplyTruncated: a 0/1-byte or non-zero-status auth reply errors
// (never panics), and the password never appears in the error.
func TestSOCKS5_AuthReplyTruncated(t *testing.T) {
	pu := mustURL(t, "socks5://alice:s3cret@127.0.0.1:1080")
	for _, reply := range [][]byte{
		{},                       // empty
		{socks5AuthRFC},          // 1 byte (truncated)
		{socks5AuthRFC, 0x01},    // explicit failure
	} {
		d := &dialer{proxy: pu, timeout: time.Second}
		// Method=UserPass, then the (truncated/failing) auth reply.
		script := append([]byte{socks5Ver, socks5UserPass}, reply...)
		err := d.socks5Handshake(&fakeConn{r: bytes.NewReader(script)}, "example.com", 443)
		if err == nil {
			t.Errorf("reply %v: expected error", reply)
		}
		if err != nil && strings.Contains(err.Error(), "s3cret") {
			t.Errorf("reply %v: error must not leak password: %v", reply, err)
		}
	}
}

// FuzzSOCKS5Reply feeds arbitrary bytes as the method-selection + connect reply;
// the hand-written parser must never panic (only error).
func FuzzSOCKS5Reply(f *testing.F) {
	f.Add([]byte{0x05, 0x00}, []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0x05, 0x02}, []byte{0x05, 0x04, 0x00, 0x03, 3, 'a', 'b', 'c', 0, 0})
	pu, _ := url.Parse("socks5://u:p@127.0.0.1:1080") // creds -> exercises socks5Auth path
	f.Fuzz(func(t *testing.T, method, reply []byte) {
		script := append(append([]byte{}, method...), reply...)
		d := &dialer{proxy: pu, timeout: time.Second}
		_ = d.socks5Handshake(&fakeConn{r: bytes.NewReader(script)}, "example.com", 443)
	})
}
