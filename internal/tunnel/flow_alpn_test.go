package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §4.1 / T15
func TestFlowALPNNMinusOneQuadrants(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowALPN(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow ALPN quadrants", goroutines)
	testharness.AssertNoFDLeak(t, "flow ALPN quadrants", fds, 0)
}

func exerciseFlowALPN(t *testing.T) {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	pins := proto.CertPins{Current: certFingerprintDER(cert.Certificate[0])}
	baseServer := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	baseClient := clientTLSConfigPinned(pins)
	wantFlow := ""
	if runtime.GOOS == "linux" {
		wantFlow = FlowALPN
	}
	// A real old peer is represented by its frozen TLS surface, not a new
	// executable impersonating historical source/artifact identity.
	for _, tc := range []struct {
		name                 string
		newServer, newClient bool
		want                 string
	}{
		{"new-new", true, true, wantFlow},
		{"new-old", true, false, ""},
		{"old-new", false, true, ""},
		{"old-old", false, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scfg, ccfg := baseServer, baseClient
			if tc.newServer {
				scfg = flowServerTLSConfig(baseServer, true)
			}
			if tc.newClient {
				ccfg = flowClientTLSConfig(baseClient, true)
			}
			peer := startALPNPeer(t, scfg, "")
			conn, err := dialALPNPeer(t, peer, ccfg, false)
			if err != nil {
				t.Fatal(err)
			}
			if got := conn.ConnectionState().NegotiatedProtocol; got != tc.want {
				t.Fatalf("selected ALPN = %q, want %q", got, tc.want)
			}
			flow, err := flowProtocol(conn.ConnectionState(), false)
			if err != nil || flow != (tc.want == FlowALPN) {
				t.Fatalf("selected protocol dispatch = %v/%v, want flow=%v", flow, err, tc.want == FlowALPN)
			}
			if _, err := conn.Write([]byte("legacy-or-flow-payload")); err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			peer.close()
			if peer.accepted.Load() != 1 || peer.bytes.Load() != int64(len("legacy-or-flow-payload")) {
				t.Fatalf("quadrant dial/payload receipts = %d/%d", peer.accepted.Load(), peer.bytes.Load())
			}
		})
	}
	// New server must clear NextProtos explicitly for unsupported clients.
	unsupported := baseClient.Clone()
	unsupported.NextProtos = []string{"unsupported/1"}
	peer := startALPNPeer(t, flowServerTLSConfig(baseServer, true), "")
	conn, err := dialALPNPeer(t, peer, unsupported, false)
	if err != nil {
		t.Fatalf("disjoint ALPN must complete as legacy: %v", err)
	}
	if conn.ConnectionState().NegotiatedProtocol != "" {
		t.Fatal("disjoint ALPN selected a protocol")
	}
	_ = conn.Close()
	peer.close()

	// A strict historical ALPN server produces the actual remote alert. Only
	// that handshake failure permits one fresh connection with no ALPN.
	strict := baseServer.Clone()
	strict.NextProtos = []string{"historical/1"}
	peer = startALPNPeer(t, strict, "")
	conn, err = dialALPNPeer(t, peer, flowClientTLSConfig(baseClient, true), false)
	if err != nil {
		t.Fatalf("strict old server retry: %v", err)
	}
	if conn.ConnectionState().NegotiatedProtocol != "" {
		t.Fatal("retry must select legacy")
	}
	_ = conn.Close()
	peer.close()
	wantAttempts := int64(1)
	if runtime.GOOS == "linux" {
		wantAttempts = 2
	}
	if peer.accepted.Load() != wantAttempts || peer.bytes.Load() != 0 {
		t.Fatalf("strict retry receipts = %d attempts/%d token bytes", peer.accepted.Load(), peer.bytes.Load())
	}
	// An incomplete runtime must not advertise v1 even on a Linux binary.
	for _, ready := range []struct{ server, client bool }{{false, true}, {true, false}} {
		peer = startALPNPeer(t, flowServerTLSConfig(baseServer, ready.server), "")
		conn, err = dialALPNPeer(t, peer, flowClientTLSConfig(baseClient, ready.client), false)
		if err != nil {
			t.Fatal(err)
		}
		if conn.ConnectionState().NegotiatedProtocol != "" {
			t.Fatal("incomplete flow runtime advertised v1")
		}
		_ = conn.Close()
		peer.close()
	}

	for _, tc := range []struct {
		name     string
		server   *tls.Config
		client   *tls.Config
		retained bool
		attempts int64
	}{
		{"wrong-pin", flowServerTLSConfig(baseServer, true), flowClientTLSConfig(clientTLSConfigPinned(proto.CertPins{Current: "sha256:wrong"}), true), false, 1},
		{"retry-keeps-pin", strict, flowClientTLSConfig(clientTLSConfigPinned(proto.CertPins{Current: "sha256:wrong"}), true), false, wantAttempts},
		{"retained-empty-alpn", baseServer, flowClientTLSConfig(baseClient, true), true, 1},
		{"retained-disjoint-alpn", strict, flowClientTLSConfig(baseClient, true), true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := startALPNPeer(t, tc.server, "")
			conn, err := dialALPNPeer(t, peer, tc.client, tc.retained)
			if conn != nil {
				_ = conn.Close()
			}
			peer.close()
			if err == nil || peer.accepted.Load() != tc.attempts || peer.bytes.Load() != 0 {
				t.Fatalf("must reject before REGISTER: err=%v attempts=%d bytes=%d", err, peer.accepted.Load(), peer.bytes.Load())
			}
		})
	}
	badPKI := flowClientTLSConfig(baseClient, true)
	badPKI.InsecureSkipVerify = false
	badPKI.RootCAs = x509.NewCertPool()
	badPKI.ServerName = "untrusted.invalid"
	peer = startALPNPeer(t, flowServerTLSConfig(baseServer, true), "")
	conn, err = dialALPNPeer(t, peer, badPKI, false)
	if conn != nil {
		_ = conn.Close()
	}
	peer.close()
	if err == nil || peer.accepted.Load() != 1 || peer.bytes.Load() != 0 {
		t.Fatalf("bad certificate retried or accepted: err=%v attempts=%d bytes=%d", err, peer.accepted.Load(), peer.bytes.Load())
	}

	// A malformed reply after selected-v1 TLS cannot cause a legacy handshake.
	peer = startALPNPeer(t, flowServerTLSConfig(baseServer, true), "OK flow-v1\n")
	conn, err = dialALPNPeer(t, peer, flowClientTLSConfig(baseClient, true), false)
	if err != nil {
		t.Fatal(err)
	}
	err = readRegisterReply(bufio.NewReader(conn))
	_ = conn.Close()
	peer.close()
	if err == nil || peer.accepted.Load() != 1 {
		t.Fatalf("malformed selected reply accepted or retried: %v / %d", err, peer.accepted.Load())
	}
	if _, err := flowProtocol(tls.ConnectionState{NegotiatedProtocol: "other/1"}, false); err == nil {
		t.Fatal("unknown selected protocol accepted")
	}
	for _, err := range []error{
		errors.New("tls: no application protocol"),
		&net.OpError{Op: "local error", Err: errors.New("tls: no application protocol")},
		&net.OpError{Op: "remote error", Err: errors.New("tls: no application protocol suffix")},
		&net.OpError{Op: "remote error", Err: errors.New("tls: bad certificate")},
	} {
		if isRemoteALPNRefusal(err) {
			t.Fatalf("non-ALPN remote alert classified for fallback: %v", err)
		}
	}
	// ClientHello customization must keep identity callbacks on both branches.
	var selected atomic.Int64
	custom := baseServer.Clone()
	custom.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		selected.Add(1)
		return baseServer, nil
	}
	configured := flowServerTLSConfig(custom, true)
	for _, protos := range [][]string{{FlowALPN}, {"unsupported/1"}} {
		hello, err := configured.GetConfigForClient(&tls.ClientHelloInfo{SupportedProtos: protos})
		if err != nil || len(hello.Certificates) != 1 || !reflect.DeepEqual(hello.Certificates[0].Certificate, cert.Certificate) {
			t.Fatalf("custom ClientHello lost certificate: %v", err)
		}
	}
	if selected.Load() != 2 || len(baseClient.NextProtos) != 0 || len(baseServer.NextProtos) != 0 {
		t.Fatal("ALPN configuration mutated shared TLS configuration")
	}
	if runtime.GOOS == "linux" {
		exerciseFlowRegisterFallback(t)
		exerciseFlowALPNRetryDeadline(t)
	}
}

// A real remote ALPN refusal consumes the first half of the dial budget. The
// second TLS handshake must expire on that original budget, before REGISTER.
// origin: hot-update S5 plan §4.1 / T15
func exerciseFlowALPNRetryDeadline(t *testing.T) {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"historical/1"}}
	cfg.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		timer := time.NewTimer(125 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-hello.Context().Done():
			return nil, hello.Context().Err()
		case <-timer.C:
			return nil, nil
		}
	}
	peer := startALPNPeer(t, cfg, "")
	client := flowClientTLSConfig(clientTLSConfigPinned(proto.CertPins{Current: certFingerprintDER(cert.Certificate[0])}), true)
	conn, err := dialFlowTLS(t.Context(), "tcp", peer.listener.Addr().String(), &net.Dialer{Timeout: 200 * time.Millisecond}, client, false)
	if conn != nil {
		_ = conn.Close()
	}
	peer.close()
	if !errors.Is(err, context.DeadlineExceeded) || peer.accepted.Load() != 2 || peer.bytes.Load() != 0 {
		t.Fatalf("ALPN retry renewed the original budget: err=%v attempts=%d bytes=%d", err, peer.accepted.Load(), peer.bytes.Load())
	}
}

// The helper's TLS quadrants cannot prove the product actually calls it. Run
// REGISTER through Client.dialAndRegister against the strict historical surface.
// origin: hot-update S5 plan §4.1 / T15
func exerciseFlowRegisterFallback(t *testing.T) {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                         string
		retained, wrongPin, selected bool
		attempts                     int64
		wantError                    bool
	}{
		{"strict-legacy", false, false, false, 2, false},
		{"retained-refusal", true, false, false, 1, true},
		{"retry-keeps-pin", false, true, false, 2, true},
		{"selected-malformed-reply", false, false, true, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"historical/1"}}
			if tc.selected {
				cfg.NextProtos = []string{FlowALPN}
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var workers sync.WaitGroup
			var attempts, registers atomic.Int64
			workers.Go(func() {
				for {
					raw, err := ln.Accept()
					if err != nil {
						return
					}
					attempts.Add(1)
					workers.Go(func() {
						conn := tls.Server(raw, cfg)
						defer func(closeFunc func() error) { _ = closeFunc() }(conn.Close)
						_ = conn.SetDeadline(time.Now().Add(ResumeAttempt))
						if conn.Handshake() != nil {
							return
						}
						line, err := bufio.NewReader(conn).ReadString('\n')
						if err != nil {
							return
						}
						sid, nid, port, token, epoch, ok := parseRegisterLine(line)
						if !ok || sid != "alpn-session" || nid != "alpn-node" || port != 14000 || token != "token" || epoch != 0 {
							t.Error("production REGISTER changed its six-field contract")
							return
						}
						registers.Add(1)
						reply := "OK\n"
						if tc.selected {
							reply = "OK flow-v1\n"
						}
						_, _ = io.WriteString(conn, reply)
						_, _ = io.Copy(io.Discard, conn)
					})
				}
			})
			defer func() { _ = ln.Close(); workers.Wait() }()
			client := NewClient(ln.Addr().String(), "alpn-session", "alpn-node", nil, silentLog())
			if err := client.SetFlowConfig(FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "alpn", BootID: "boot", OwnerGen: 1}); err != nil {
				t.Fatal(err)
			}
			defer func(closeFunc func() error) { _ = closeFunc() }(client.flowState.manager.Close)
			if tc.retained {
				client.flowState.endpoints[14000] = &clientEndpoint{}
			}
			pin := certFingerprintDER(cert.Certificate[0])
			if tc.wrongPin {
				pin = "sha256:wrong"
			}
			ctx, cancel := context.WithTimeout(t.Context(), ResumeAttempt)
			defer cancel()
			conn, mux, err := client.dialAndRegister(ctx, 14000, "token", "", 0, proto.CertPins{Current: pin})
			if conn != nil {
				_ = conn.Close()
			}
			if mux != nil {
				_ = mux.Close()
			}
			_ = ln.Close()
			workers.Wait()
			if (err != nil) != tc.wantError || attempts.Load() != tc.attempts {
				t.Fatalf("production ALPN retry: err=%v attempts=%d", err, attempts.Load())
			}
			wantRegisters := int64(1)
			if tc.retained || tc.wrongPin {
				wantRegisters = 0
			}
			if registers.Load() != wantRegisters {
				t.Fatal("REGISTER escaped failed TLS/pin negotiation")
			}
		})
	}
}

type alpnPeer struct {
	listener net.Listener
	wg       sync.WaitGroup
	accepted atomic.Int64
	bytes    atomic.Int64
}

func startALPNPeer(t *testing.T, cfg *tls.Config, reply string) *alpnPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &alpnPeer{listener: ln}
	p.wg.Go(func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			p.accepted.Add(1)
			p.wg.Go(func() {
				conn := tls.Server(raw, cfg)
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if err := conn.Handshake(); err != nil {
					return
				}
				if reply != "" {
					if _, err := io.WriteString(conn, reply); err != nil {
						return
					}
				}
				n, _ := io.Copy(io.Discard, conn)
				p.bytes.Add(n)
			})
		}
	})
	// Failed assertions must retire fixture workers too, so a protocol mutation
	// reports its violated invariant without leaving unrelated TLS readers alive.
	t.Cleanup(p.close)
	return p
}

func (p *alpnPeer) close() {
	_ = p.listener.Close()
	p.wg.Wait()
}

func dialALPNPeer(t *testing.T, peer *alpnPeer, cfg *tls.Config, retained bool) (*tls.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: isolated TLS operation in the test harness.
	defer cancel()
	conn, err := dialFlowTLS(ctx, "tcp", peer.listener.Addr().String(), &net.Dialer{}, cfg, retained)
	if conn != nil {
		t.Cleanup(func() { _ = conn.Close() })
	}
	return conn, err
}
