// Package proxydial_test is the flagship end-to-end test for proxy-aware dial:
// a real TLS embedded nats-server reached through a fake HTTP CONNECT proxy,
// driven through the real nats.go connect path + internal/proxydial options.
//
// It proves the whole chain the unit tests can only prove in pieces:
//   - nats.SkipHostLookup makes the unresolvable hostname reach the dialer (not
//     a pre-resolved fake-ip), and the dialer hands the hostname to the proxy.
//   - The proxy carries raw TCP; nats.go does TLS end-to-end afterward, and the
//     SNI ServerName comes from the URL host (proxytest.invalid), NOT the dialer
//     address — so cert validation still works through the proxy.
//   - The first byte the proxy sees from the client is a TLS record (0x16) — the
//     broker traffic is never exposed to the proxy in plaintext.
//   - The real nats-server's INFO line survives the bufConn wrap.
//   - Reconnect re-dials THROUGH the proxy (MaxReconnects(-1)).
//   - Zero regression: with no proxy env, Options is nil and a direct connect works.
package proxydial_test

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proxydial"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func genServerTLS(t *testing.T, dnsName string) (*tls.Config, *x509.CertPool) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}}}, pool
}

// tunnelProxy is a fake HTTP CONNECT proxy that tunnels every CONNECT to a fixed
// backend (simulating "the proxy resolves the hostname to the broker") and
// records the CONNECT target, the accept count, and the first client byte after
// the 200 (the TLS ClientHello's 0x16).
type tunnelProxy struct {
	addr    string
	backend string
	mu      sync.Mutex
	target  string
	accepts int
	first   int
}

func startTunnelProxy(t *testing.T, backend string) *tunnelProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &tunnelProxy{addr: ln.Addr().String(), backend: backend, first: -1}
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

func (p *tunnelProxy) handle(c net.Conn) {
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.target = req.Host
	p.accepts++
	p.mu.Unlock()
	back, err := net.Dial("tcp", p.backend)
	if err != nil {
		_, _ = c.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		_ = c.Close()
		return
	}
	_, _ = c.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
	// Pipe both ways CONCURRENTLY (NATS servers speak first — INFO — so a
	// synchronous read of the client here would deadlock). Record the first
	// client byte non-invasively as it streams (TLS ClientHello starts 0x16).
	go func() { _, _ = io.Copy(back, &firstByteRecorder{r: br, p: p}); _ = back.Close() }()
	_, _ = io.Copy(c, back)
	_ = c.Close()
}

// firstByteRecorder records the first byte read (the client's first wire byte)
// into the proxy, without altering the stream.
type firstByteRecorder struct {
	r io.Reader
	p *tunnelProxy
}

func (f *firstByteRecorder) Read(b []byte) (int, error) {
	n, err := f.r.Read(b)
	if n > 0 {
		f.p.mu.Lock()
		if f.p.first < 0 {
			f.p.first = int(b[0])
		}
		f.p.mu.Unlock()
	}
	return n, err
}

func (p *tunnelProxy) Target() string { p.mu.Lock(); defer p.mu.Unlock(); return p.target }
func (p *tunnelProxy) Accepts() int   { p.mu.Lock(); defer p.mu.Unlock(); return p.accepts }
func (p *tunnelProxy) First() int     { p.mu.Lock(); defer p.mu.Unlock(); return p.first }

func proxyEnv(proxyAddr string) proxydial.Env {
	return func(k string) (string, bool) {
		if k == "HTTPS_PROXY" {
			return "http://" + proxyAddr, true
		}
		return "", false
	}
}

func TestIntegration_ProxyTLSEndToEnd(t *testing.T) {
	serverTLS, caPool := genServerTLS(t, "proxytest.invalid")
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.TLSConfig = serverTLS
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()
	natsAddr := ns.Addr().String()
	_, port, err := net.SplitHostPort(natsAddr)
	if err != nil {
		t.Fatal(err)
	}
	p := startTunnelProxy(t, natsAddr)
	url := "tls://proxytest.invalid:" + port // unresolvable host: any local DNS fails

	popts, err := proxydial.Options(proxyEnv(p.addr), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Two TLS-config variants: (1) CA only, ServerName left to nats.go's URL-host
	// default (the production path); (2) explicit ServerName set by an operator.
	for _, tc := range []struct {
		name string
		tls  *tls.Config
	}{
		{"default_servername", &tls.Config{RootCAs: caPool}},
		{"explicit_servername", &tls.Config{RootCAs: caPool, ServerName: "proxytest.invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := []nats.Option{nats.Secure(tc.tls), nats.Timeout(5 * time.Second)}
			nc, err := nats.Connect(url, append(base, popts...)...)
			if err != nil {
				t.Fatalf("connect through proxy: %v", err)
			}
			defer nc.Close()
			if !nc.IsConnected() {
				t.Fatal("not connected through proxy")
			}
			cs, err := nc.TLSConnectionState()
			if err != nil {
				t.Fatalf("TLS connection state: %v", err)
			}
			if !cs.HandshakeComplete {
				t.Error("TLS handshake did not complete end-to-end through the proxy")
			}
			if cs.ServerName != "proxytest.invalid" {
				t.Errorf("SNI ServerName = %q, want proxytest.invalid", cs.ServerName)
			}
			if got := p.Target(); got != "proxytest.invalid:"+port {
				t.Errorf("proxy CONNECT target %q, want hostname proxytest.invalid:%s", got, port)
			}
			if fb := p.First(); fb != 0x16 {
				t.Errorf("first byte the proxy saw = 0x%02x, want 0x16 (TLS record — no plaintext exposed)", fb)
			}
		})
	}
}

// TestIntegration_ReconnectThroughProxy proves reconnect re-dials through the
// proxy: after kill+restart on the same port, nats.go's reconnect goroutine
// invokes the dialer again and the proxy's CONNECT-accept count climbs.
func TestIntegration_ReconnectThroughProxy(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	tcp := ns.Addr().(*net.TCPAddr)
	natsAddr := tcp.String()
	port := tcp.Port

	p := startTunnelProxy(t, natsAddr)
	url := "nats://proxytest.invalid:" + strconv.Itoa(port)
	popts, err := proxydial.Options(proxyEnv(p.addr), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := []nats.Option{nats.MaxReconnects(-1), nats.ReconnectWait(100 * time.Millisecond), nats.Timeout(3 * time.Second)}
	nc, err := nats.Connect(url, append(base, popts...)...)
	if err != nil {
		t.Fatalf("initial connect: %v", err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("not connected")
	}

	ns.Shutdown()
	time.Sleep(200 * time.Millisecond) // let the client register the disconnect + the port free
	// Restart on the SAME port so the tunnel's fixed backend addr stays valid.
	opts2 := natstest.DefaultTestOptions
	opts2.Host = "127.0.0.1"
	opts2.Port = port
	ns2 := natstest.RunServer(&opts2)
	defer ns2.Shutdown()

	rdeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(rdeadline) && !nc.IsConnected() {
		time.Sleep(50 * time.Millisecond)
	}
	if !nc.IsConnected() {
		t.Fatal("did not reconnect through the proxy")
	}
	if n := p.Accepts(); n < 2 {
		t.Errorf("proxy CONNECT accepts = %d, want >=2 (reconnect must re-dial via proxy)", n)
	}
}

func TestIntegration_ZeroRegressionNoProxy(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	popts, err := proxydial.Options(func(string) (string, bool) { return "", false }, time.Second)
	if err != nil || popts != nil {
		t.Fatalf("no proxy env must yield nil options, got %v err %v", popts, err)
	}
	nc, err := nats.Connect(ns.ClientURL(), popts...)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if !nc.IsConnected() {
		t.Fatal("direct connect failed")
	}
}
