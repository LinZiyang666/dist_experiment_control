package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: control-connection-recovery plan, expose replay and restart acceptance.
// The local endpoint and control listener remain bound after choosing their ports;
// only the public port needs the production API's bounded retry for a lost bind.
type replayTunnelHarness struct {
	cli          *Client
	srv          *Server
	serverCancel context.CancelFunc
	addr         string
	publicPort   int
	localPort    int
	token        string
	epoch        int64
	cert         tls.Certificate
	pins         proto.CertPins
	registers    atomic.Int64
	expectedPort atomic.Int64
	onLookup     atomic.Pointer[func()]
}

func newReplayTunnelHarness(t *testing.T) *replayTunnelHarness {
	t.Helper()
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "replay-keeps-this-endpoint")
	}))
	t.Cleanup(local.Close)
	_, portString, err := net.SplitHostPort(strings.TrimPrefix(local.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	localPort, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	cert, pins := replayTestCertificate(t)
	h := &replayTunnelHarness{
		localPort: localPort, token: "replay-test-token", epoch: 3,
		cert: cert, pins: pins,
	}
	h.startServer(t, "127.0.0.1:0")
	h.cli = NewClient(h.addr, "lab", "replay-node", nil, silentLog())
	h.cli.SetBackoffForTest(10*time.Millisecond, 80*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	h.cli.Start(ctx)
	t.Cleanup(cancel)
	for attempt := 0; attempt < 8; attempt++ {
		h.publicPort = findFreePort(t)
		h.expectedPort.Store(int64(h.publicPort))
		err = h.cli.OpenHome(ctx, h.publicPort, h.localPort, h.token, h.addr, h.epoch, h.pins)
		if err == nil {
			h.waitTransport(t)
			return h
		}
		var deny *DenyError
		if !errors.As(err, &deny) || deny.Reason != "public_port_bind_failed" {
			t.Fatal(err)
		}
	}
	t.Fatalf("could not reserve the public port after bounded retries: %v", err)
	return nil
}

func replayTestCertificate(t *testing.T) (tls.Certificate, proto.CertPins) {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return cert, proto.CertPins{Current: CertFingerprint(leaf)}
}

func (h *replayTunnelHarness) startServer(t *testing.T, addr string) {
	t.Helper()
	lookup := func(sid, nid string, port int, tokenHash string, epoch int64) error {
		h.registers.Add(1)
		if fn := h.onLookup.Load(); fn != nil {
			(*fn)()
		}
		if sid != "lab" || nid != "replay-node" || int64(port) != h.expectedPort.Load() ||
			tokenHash != hashToken(h.token) || epoch != h.epoch {
			return errors.New("token_unknown_or_revoked")
		}
		return nil
	}
	srv := NewServerWithCert(addr, "127.0.0.1", lookup, &h.cert, silentLog())
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Close() })
	h.srv, h.serverCancel = srv, cancel
	srv.mu.Lock()
	h.addr = srv.ln.Addr().String()
	srv.mu.Unlock()
}

func (h *replayTunnelHarness) publicAddress() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(h.publicPort))
}

func (h *replayTunnelHarness) session() *clientSession {
	h.cli.mu.Lock()
	defer h.cli.mu.Unlock()
	return h.cli.sessions[h.publicPort]
}

// OK reaches the client before the server publishes its session.
// Establish that owner before a test tries to interrupt its transport.
// origin: hot-update S5 plan §8 / T12 / testing-standards T3
func (h *replayTunnelHarness) waitTransport(t *testing.T) {
	t.Helper()
	if !testharness.WaitFor(t, ResumeAttempt, time.Millisecond, func() bool {
		h.srv.mu.Lock()
		defer h.srv.mu.Unlock()
		return h.srv.sessions[h.publicPort] != nil
	}) {
		t.Fatal("server did not publish the accepted replay transport")
	}
}

func replayHTTPRoundTrip(conn net.Conn, reader *bufio.Reader) error {
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: replay\r\n\r\n"); err != nil {
		return err
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if string(body) != "replay-keeps-this-endpoint" {
		return fmt.Errorf("wrong local endpoint: %q", body)
	}
	return nil
}

func (h *replayTunnelHarness) freshRoundTrip() error {
	conn, err := net.DialTimeout("tcp", h.publicAddress(), time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return replayHTTPRoundTrip(conn, bufio.NewReader(conn))
}

func (h *replayTunnelHarness) waitRoundTrip(t *testing.T) {
	t.Helper()
	var last error
	if !testharness.WaitFor(t, 8*time.Second, 10*time.Millisecond, func() bool {
		last = h.freshRoundTrip()
		return last == nil
	}) {
		t.Fatalf("public payload did not recover: %v", last)
	}
}

func TestOpenHomeReplayPreservesLiveTransport(t *testing.T) {
	h := newReplayTunnelHarness(t)
	conn, err := net.DialTimeout("tcp", h.publicAddress(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	if err := replayHTTPRoundTrip(conn, reader); err != nil {
		t.Fatal(err)
	}
	before, registers := h.session(), h.registers.Load()
	for _, replay := range []struct {
		name string
		addr string
		pins proto.CertPins
	}{
		{"same", h.addr, h.pins},
		{"persisted-without-pins", h.addr, proto.CertPins{}},
		{"same-effective-default-address", "", proto.CertPins{}},
	} {
		t.Run(replay.name, func(t *testing.T) {
			if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort,
				h.token, replay.addr, h.epoch, replay.pins); err != nil {
				t.Fatalf("replay of the existing allocation failed: %v", err)
			}
			if h.session() != before || h.registers.Load() != registers {
				t.Fatal("identical replay replaced the supervisor or sent another REGISTER")
			}
			if err := replayHTTPRoundTrip(conn, reader); err != nil {
				t.Fatalf("replay interrupted an existing stream: %v", err)
			}
			if err := h.freshRoundTrip(); err != nil {
				t.Fatalf("replay broke a new public stream: %v", err)
			}
			h.cli.mu.Lock()
			pins := h.cli.sessions[h.publicPort].certPins
			h.cli.mu.Unlock()
			if pins.Current != h.pins.Current {
				t.Fatal("persisted replay erased the existing peer certificate pin")
			}
		})
	}
}

func TestOpenHomeReplayRetainsRecoveringSupervisor(t *testing.T) {
	h := newReplayTunnelHarness(t)
	before, registers := h.session(), h.registers.Load()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	hold := func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	h.onLookup.Store(&hold)
	if !h.srv.DropTransport(h.publicPort) {
		t.Fatal("no transport to interrupt")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the existing supervisor did not attempt recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.cli.OpenHome(ctx, h.publicPort, h.localPort, h.token, h.addr,
		h.epoch, proto.CertPins{}); err != nil {
		t.Fatalf("replay must retain the recovering supervisor without a second dial: %v", err)
	}
	if h.session() != before || h.registers.Load() != registers+1 {
		t.Fatal("replay created a competing supervisor or REGISTER while recovery was in flight")
	}
	unblock()
	h.waitRoundTrip(t)
	if h.session() != before {
		t.Fatal("recovery replaced the allocation's supervisor slot")
	}
}

func TestOpenHomeReplayRefreshesPinsWithoutRedial(t *testing.T) {
	h := newReplayTunnelHarness(t)
	before, registers := h.session(), h.registers.Load()
	rotated, pins := replayTestCertificate(t)
	h.srv.SetCertificate(&rotated)
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		h.addr, h.epoch, pins); err != nil {
		t.Fatalf("same allocation with rotated pins must update in place: %v", err)
	}
	if h.session() != before || h.registers.Load() != registers {
		t.Fatal("a same-epoch pin update caused another REGISTER or replaced the supervisor")
	}
	if !h.srv.DropTransport(h.publicPort) {
		t.Fatal("no transport to interrupt after certificate rotation")
	}
	h.waitRoundTrip(t)
	if h.registers.Load() <= registers {
		t.Fatal("the rotated certificate was never exercised by a reconnect")
	}
}

func TestOpenHomeDifferentIdentityDoesNotClaimReplay(t *testing.T) {
	for _, field := range []string{"token", "local-port", "home", "epoch"} {
		t.Run(field, func(t *testing.T) {
			h := newReplayTunnelHarness(t)
			before := h.session()
			localPort, token, addr, epoch, pins := h.localPort, h.token, h.addr, h.epoch, h.pins
			switch field {
			case "token":
				token = "not-the-authorized-token"
			case "local-port":
				localPort++
			case "home":
				other := startAdmissionServer(t)
				addr, pins = other.addr, proto.CertPins{Current: other.fp}
			case "epoch":
				epoch++
			}
			if err := h.cli.OpenHome(context.Background(), h.publicPort, localPort, token,
				addr, epoch, pins); err == nil {
				t.Fatal("changed allocation parameters bypassed a real, rejecting REGISTER")
			}
			if h.session() != before {
				t.Fatal("a failed change damaged the previous session")
			}
			if err := h.freshRoundTrip(); err != nil {
				t.Fatalf("a failed change damaged the previous endpoint: %v", err)
			}
		})
	}
}

func TestOpenHomeCancelledReplayCannotSucceed(t *testing.T) {
	h := newReplayTunnelHarness(t)
	before, registers := h.session(), h.registers.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := h.cli.OpenHome(ctx, h.publicPort, h.localPort, h.token, h.addr, h.epoch, h.pins)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled replay returned %v, want context.Canceled", err)
	}
	if h.session() != before || h.registers.Load() != registers {
		t.Fatal("cancelled replay changed an existing allocation")
	}
	if err := h.freshRoundTrip(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenHomeReplayDoesNotCrossNodeIdentityChanges(t *testing.T) {
	h := newReplayTunnelHarness(t)
	registers := h.registers.Load()
	h.cli.SetNID("replay-node-02")
	if h.cli.HasSession(h.publicPort) {
		t.Fatal("renaming retained an allocation authorized under the previous node name")
	}
	err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		h.addr, h.epoch, h.pins)
	var deny *DenyError
	if !errors.As(err, &deny) || deny.Reason != "token_unknown_or_revoked" {
		t.Fatalf("replay under another node identity must face authorization, got %v", err)
	}
	if h.registers.Load() != registers+1 || h.cli.HasSession(h.publicPort) {
		t.Fatal("renamed replay bypassed the server's refusal")
	}
}

// A transport established through the N=1 insecure fallback has never proved a
// named home's certificate. Equal socket addresses cannot replace that proof.
func TestOpenHomeReplayCannotPromoteAnUnverifiedTransport(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprintf("named-home=%t", named), func(t *testing.T) {
			h := newReplayTunnelHarness(t)
			h.cli.Close(h.publicPort)
			h.srv.CloseProxy(h.publicPort)
			if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
				"", h.epoch, proto.CertPins{}); err != nil {
				t.Fatalf("establish the unpinned N=1 fallback: %v", err)
			}
			addr := ""
			if named {
				addr = h.addr
			}
			assertReplayRejectsForeignCertificate(t, h, addr)
			registers := h.registers.Load()
			if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
				addr, h.epoch, h.pins); err != nil {
				t.Fatalf("replay should verify the actual peer before promoting it: %v", err)
			}
			if h.registers.Load() != registers {
				t.Fatal("certificate verification sent another REGISTER to the occupied port")
			}
		})
	}
}

func TestOpenHomeReplayRechecksTransportAfterUnpinnedRedial(t *testing.T) {
	h := newReplayTunnelHarness(t)
	h.cli.Close(h.publicPort)
	h.srv.CloseProxy(h.publicPort)
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		"", h.epoch, h.pins); err != nil {
		t.Fatalf("establish a pinned default-address transport: %v", err)
	}
	h.waitTransport(t)
	if err := h.cli.ApplyHome(h.publicPort, "", h.epoch, proto.CertPins{}); err != nil {
		t.Fatalf("return to the supported N=1 fallback: %v", err)
	}
	before := h.session()
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		h.addr, h.epoch, proto.CertPins{}); !errors.Is(err, ErrHomePinsRequired) {
		t.Errorf("named-home replay has no pins for its next redial: got %v, want ErrHomePinsRequired", err)
	}
	if h.session() != before {
		t.Fatal("failed named-home replay changed the fallback allocation")
	}
	registers := h.registers.Load()
	if !h.srv.DropTransport(h.publicPort) {
		t.Fatal("no transport to interrupt")
	}
	h.waitRoundTrip(t)
	if h.registers.Load() <= registers {
		t.Fatal("reconnect did not establish a replacement transport")
	}
	assertReplayRejectsForeignCertificate(t, h, h.addr)
}

func TestOpenHomeReplayPinsFenceAnInflightUnpinnedRedial(t *testing.T) {
	h := newReplayTunnelHarness(t)
	h.cli.Close(h.publicPort)
	h.srv.CloseProxy(h.publicPort)
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		"", h.epoch, proto.CertPins{}); err != nil {
		t.Fatalf("establish the unpinned fallback: %v", err)
	}
	h.waitTransport(t)
	before := h.session()
	foreign, _ := replayTestCertificate(t)
	h.srv.SetCertificate(&foreign)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	hold := func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	}
	h.onLookup.Store(&hold)
	if !h.srv.DropTransport(h.publicPort) {
		t.Fatal("no transport to interrupt")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the insecure redial did not reach REGISTER after its TLS handshake")
	}
	// The old peer certificate is genuine, but the already-handshaken redial
	// is not. Accepting these pins must fence out that in-flight transport.
	if err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		h.addr, h.epoch, h.pins); err != nil {
		t.Fatalf("verify the existing peer and install named-home pins: %v", err)
	}
	h.srv.SetCertificate(&h.cert)
	h.onLookup.Store(nil)
	unblock()
	h.waitRoundTrip(t)
	h.cli.mu.Lock()
	slot := h.cli.sessions[h.publicPort]
	var fingerprint string
	if slot != nil {
		if conn, ok := slot.conn.(*tls.Conn); ok {
			state := conn.ConnectionState()
			if len(state.PeerCertificates) > 0 {
				fingerprint = CertFingerprint(state.PeerCertificates[0])
			}
		}
	}
	h.cli.mu.Unlock()
	if slot != before {
		t.Fatal("certificate transition lost the allocation's supervisor")
	}
	if fingerprint != h.pins.Current {
		t.Fatalf("an in-flight unpinned redial bypassed newly accepted pins: peer=%s", fingerprint)
	}
}

func assertReplayRejectsForeignCertificate(t *testing.T, h *replayTunnelHarness, addr string) {
	t.Helper()
	before := h.session()
	_, foreignPins := replayTestCertificate(t)
	err := h.cli.OpenHome(context.Background(), h.publicPort, h.localPort, h.token,
		addr, h.epoch, foreignPins)
	if err == nil {
		t.Fatal("an unverified transport was accepted as a named home with a different certificate")
	}
	if h.session() != before {
		t.Fatal("failed certificate verification changed the previous N=1 allocation")
	}
	h.cli.mu.Lock()
	pins := h.cli.sessions[h.publicPort].certPins
	h.cli.mu.Unlock()
	if pins.Current != "" {
		t.Fatal("failed named-home verification installed unverified certificate pins")
	}
	if err := h.freshRoundTrip(); err != nil {
		t.Fatalf("failed security upgrade damaged the existing N=1 endpoint: %v", err)
	}
}
