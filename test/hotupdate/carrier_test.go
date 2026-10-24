//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tokenhash"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Both relay legs use real TLS with the same pinned loopback certificate. The
// plaintext tap forwards every byte unchanged and independently parses yamux
// and the first flow frames; it never uses the product's flow codec. A fault
// only closes TCP, leaving all business sockets untouched. Keeping this tap
// outside the product makes a missing RESUME bind observable even when a
// defective implementation happens to deliver the expected application bytes.
type flowCarrierRelay struct {
	ln       net.Listener
	upstream string
	mu       sync.Mutex
	pairs    map[net.Conn]net.Conn
	blocked  bool
	closed   bool
	accepted atomic.Int64
	wg       sync.WaitGroup
	cert     tls.Certificate
	roots    *x509.CertPool
	trace    flowCarrierTrace
}

func flowStartCarrierRelay(t *testing.T, upstream string, cert tls.Certificate) *flowCarrierRelay {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &flowCarrierRelay{ln: ln, upstream: upstream, pairs: make(map[net.Conn]net.Conn)}
	r.cert = cert
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	r.roots = x509.NewCertPool()
	r.roots.AddCert(leaf)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			if r.closed || r.blocked {
				r.mu.Unlock()
				_ = conn.Close()
				continue
			}
			r.pairs[conn] = nil
			r.wg.Add(1)
			r.mu.Unlock()
			go r.forward(conn)
		}
	}()
	t.Cleanup(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		_ = ln.Close()
		r.cut(t, true, false)
		r.wg.Wait()
	})
	return r
}

func (r *flowCarrierRelay) forward(conn net.Conn) {
	defer r.wg.Done()
	defer func() { _ = conn.Close(); r.mu.Lock(); delete(r.pairs, conn); r.mu.Unlock() }()
	up, err := tls.DialWithDialer(&net.Dialer{Timeout: tunnel.ResumeAttempt}, "tcp", r.upstream, &tls.Config{RootCAs: r.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{tunnel.FlowALPN}})
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	r.mu.Lock()
	if _, exists := r.pairs[conn]; !exists || r.closed || r.blocked {
		r.mu.Unlock()
		return
	}
	r.pairs[conn] = up
	r.accepted.Add(1)
	r.mu.Unlock()
	down := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{r.cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{tunnel.FlowALPN}})
	_ = conn.SetDeadline(time.Now().Add(tunnel.ResumeAttempt))
	if err := down.Handshake(); err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	if up.ConnectionState().NegotiatedProtocol != tunnel.FlowALPN || down.ConnectionState().NegotiatedProtocol != tunnel.FlowALPN {
		r.trace.fail("real TLS pair did not negotiate flow-v1 on both legs")
		return
	}
	attachment := &flowWireAttachment{trace: &r.trace, granted: make(map[uint32]bool)}
	toBroker := &flowYamuxTap{attachment: attachment, fromBroker: false, streams: make(map[uint32][]byte)}
	toAgent := &flowYamuxTap{attachment: attachment, fromBroker: true, streams: make(map[uint32][]byte)}
	done := make(chan struct{})
	go func() { _, _ = io.Copy(down, io.TeeReader(up, toAgent)); _ = conn.Close(); _ = up.Close(); close(done) }()
	_, _ = io.Copy(up, io.TeeReader(down, toBroker))
	_ = conn.Close()
	_ = up.Close()
	<-done
}

func (r *flowCarrierRelay) cut(t *testing.T, block, requireLive bool) {
	t.Helper()
	r.mu.Lock()
	r.blocked = block
	var connections []net.Conn
	live := 0
	for conn, up := range r.pairs {
		connections = append(connections, conn)
		if up != nil {
			connections = append(connections, up)
			live++
		}
		delete(r.pairs, conn)
	}
	r.mu.Unlock()
	if requireLive && live != 1 {
		t.Fatalf("carrier fault requires exactly one live TLS pair, found=%d", live)
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type flowCarrierTrace struct {
	mu           sync.Mutex
	err          error
	opens, binds uint64
}

func (s *flowCarrierTrace) fail(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = errors.New(message)
	}
}

func (s *flowCarrierTrace) snapshot() (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, s.binds, s.err
}

type flowWireAttachment struct {
	mu      sync.Mutex
	trace   *flowCarrierTrace
	granted map[uint32]bool
}

// This reader describes only the wire oracle. Header offsets are literal and
// independent of EncodeFrame/DecodeFrame, so both product peers cannot agree
// on an invalid first-frame convention and make this test pass together.
type flowYamuxTap struct {
	attachment         *flowWireAttachment
	fromBroker         bool
	registered, denied bool
	buf                []byte
	streams            map[uint32][]byte
	first              map[uint32]bool
}

func (s *flowYamuxTap) Write(b []byte) (int, error) {
	n := len(b)
	if s.denied {
		return n, nil
	}
	s.buf = append(s.buf, b...)
	if !s.registered {
		end := -1
		for i, c := range s.buf {
			if c == '\n' {
				end = i
				break
			}
		}
		if end < 0 {
			if len(s.buf) > 4096 {
				s.attachment.trace.fail("REGISTER preface exceeded independent limit")
				s.denied = true
			}
			return n, nil
		}
		line := string(s.buf[:end+1])
		s.buf = s.buf[end+1:]
		s.registered = true
		if s.fromBroker {
			if strings.HasPrefix(line, "DENY ") {
				s.denied = true
				s.buf = nil
				return n, nil
			}
			if line != "OK\n" {
				s.attachment.trace.fail("server did not emit exact REGISTER OK")
			}
		} else if fields := strings.Fields(line); len(fields) != 6 || fields[0] != "REGISTER" {
			s.attachment.trace.fail("client REGISTER is not the six-field contract")
		}
	}
	for len(s.buf) >= 12 {
		kind := s.buf[1]
		if s.buf[0] != 0 || kind > 3 {
			s.attachment.trace.fail("invalid independent yamux header")
			s.denied = true
			return n, nil
		}
		length := int(binary.BigEndian.Uint32(s.buf[8:12]))
		if kind != 0 {
			s.buf = s.buf[12:]
			continue
		}
		if length > 1<<20 {
			s.attachment.trace.fail("yamux DATA exceeded bounded tap capacity")
			s.denied = true
			return n, nil
		}
		if len(s.buf) < 12+length {
			return n, nil
		}
		stream := binary.BigEndian.Uint32(s.buf[4:8])
		s.streams[stream] = append(s.streams[stream], s.buf[12:12+length]...)
		s.buf = s.buf[12+length:]
		s.consumeStream(stream)
	}
	return n, nil
}

func (s *flowYamuxTap) consumeStream(stream uint32) {
	if s.first == nil {
		s.first = make(map[uint32]bool)
	}
	b := s.streams[stream]
	for len(b) >= 40 {
		bodyLen := int(binary.BigEndian.Uint32(b[4:8]))
		if b[0] != 1 || bodyLen > 32768 {
			s.attachment.trace.fail("invalid independent flow frame header")
			delete(s.streams, stream)
			return
		}
		if len(b) < 40+bodyLen {
			break
		}
		kind, body := b[1], b[40:40+bodyLen]
		s.attachment.mu.Lock()
		if !s.first[stream] {
			s.first[stream] = true
			if stream == 1 {
				if (s.fromBroker && kind != 4) || (!s.fromBroker && (kind != 3 || len(body) < 2 || body[0] != 0)) {
					s.attachment.trace.fail("endpoint control stream lacks first RESUME/RESUME_OK")
				}
			} else if s.fromBroker {
				// origin: hot-update S5 plan §2.2 / §8 / T40
				if kind != 1 && (kind != 3 || len(body) < 2 || body[0] != 1 || body[1] != 6) {
					s.attachment.trace.fail("data stream lacks first OPEN or flow RESUME bind")
				}
				s.attachment.trace.mu.Lock()
				if kind == 1 {
					s.attachment.trace.opens++
				}
				if kind == 3 {
					s.attachment.trace.binds++
				}
				s.attachment.trace.mu.Unlock()
			} else if kind != 2 && kind != 4 {
				s.attachment.trace.fail("agent data stream lacks first OPEN_ACK or RESUME_OK")
			}
		}
		if stream != 1 && !s.fromBroker && ((kind == 2 && len(body) == 75 && body[72] == 2) || (kind == 4 && len(body) > 112 && body[112] == 1)) {
			s.attachment.granted[stream] = true
		}
		if stream != 1 && kind == 6 && !s.attachment.granted[stream] {
			s.attachment.trace.fail("DATA preceded the successful stream bind receipt")
		}
		s.attachment.mu.Unlock()
		b = b[40+bodyLen:]
	}
	s.streams[stream] = b
}

func flowRequireCarrierTrace(t *testing.T, h *flowCarrierFixture) {
	t.Helper()
	opens, binds, err := h.relay.trace.snapshot()
	if err != nil {
		t.Fatalf("independent carrier wire oracle: %v", err)
	}
	if opens == 0 {
		t.Fatal("independent wire tap saw no real OPEN")
	}
	t.Logf("independent TLS/yamux wire trace: OPEN=%d RESUME-bind=%d", opens, binds)
}

type flowCarrierFixture struct {
	server                     *tunnel.Server
	client                     *tunnel.Client
	relay                      *flowCarrierRelay
	target                     *flowTargetListener
	publicPort                 int
	serverBudget, clientBudget workload.ReplayBudget
	epoch                      atomic.Int64
	lookups, grants, connects  atomic.Int64
	downEdges                  atomic.Int64
	ids                        chan flow.ID
	connectEntered             chan struct{}
	connectRelease             chan struct{}
	connectOnce                sync.Once
	ctx                        context.Context
}

func flowCarrierCertificate(t *testing.T) (tls.Certificate, proto.CertPins) {
	t.Helper()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "flow-loopback"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, private)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, proto.CertPins{Current: tunnel.CertFingerprint(leaf)}
}

func flowNewCarrierFixture(t *testing.T, gateOpen bool) *flowCarrierFixture {
	t.Helper()
	h := &flowCarrierFixture{ids: make(chan flow.ID, flow.MaxFlows), connectEntered: make(chan struct{}), connectRelease: make(chan struct{})}
	h.epoch.Store(3)
	h.target = flowListenTarget(t)
	if !gateOpen {
		close(h.connectRelease)
	}
	ctx, cancel := context.WithCancel(t.Context())
	h.ctx = ctx
	t.Cleanup(cancel)
	_, portText, err := net.SplitHostPort(flowFreeAddress(t))
	if err != nil {
		t.Fatal(err)
	}
	h.publicPort, err = strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cert, pins := flowCarrierCertificate(t)
	serverAddr := flowFreeAddress(t)
	const token = "private-flow-test-allocation"
	authorized := func(a tunnel.Allocation) error {
		if a.SID != "flow-lab" || a.NID != "flow-node" || a.Port != h.publicPort || a.TokenHash != tokenhash.Sum(token) || a.Epoch != h.epoch.Load() {
			return errors.New("allocation is revoked or stale")
		}
		return nil
	}
	lookup := func(req tunnel.AuthRequest) error {
		h.lookups.Add(1)
		if !req.Deadline.After(time.Now()) {
			return errors.New("authority deadline expired")
		}
		select {
		case <-req.Stop:
			return errors.New("authority stopped")
		default:
		}
		return authorized(req.Allocation)
	}
	authorize := func(req tunnel.AuthRequest) (tunnel.Authority, error) {
		h.grants.Add(1)
		if err := lookup(req); err != nil {
			return tunnel.Authority{}, err
		}
		return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 1, ProxyEpoch: req.Allocation.Epoch, AppliedRevision: 1}, nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.server = tunnel.NewServerWithCert(serverAddr, "127.0.0.1", func(sid, nid string, port int, hash string, epoch int64) error {
		return authorized(tunnel.Allocation{SID: sid, NID: nid, Port: port, TokenHash: hash, Epoch: epoch})
	}, &cert, logger)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	cfg := tunnel.FlowConfig{Budget: &h.serverBudget, InstanceID: "aaaaaaaaaaaaaaaaaaaaaaaaaa", OwnerGen: 1, BootID: strings.TrimSpace(string(boot)), MaxEndpoints: tunnel.MaxEndpoints, MaxPendingFDs: flow.MaxPendingFDs, TokenLookup: lookup, Authorize: authorize}
	// Fail at the real API when dependencies are stubs. Skipping or substituting
	// a legacy carrier here would falsely turn the lifecycle matrix green.
	if err := h.server.SetFlowConfig(cfg); err != nil {
		t.Fatalf("flow carrier implementation required (pending P1): %v", err)
	}
	if err := h.server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.server.Close)
	h.relay = flowStartCarrierRelay(t, serverAddr, cert)
	h.client = tunnel.NewClient(h.relay.ln.Addr().String(), "flow-lab", "flow-node", func(port int) (int, error) {
		if port != h.publicPort {
			return 0, errors.New("unknown public allocation")
		}
		return flowTCPPort(h.target.listener.Addr()), nil
	}, logger)
	h.client.SetSessionStateHook(func(_ int, up bool) {
		if !up {
			h.downEdges.Add(1)
		}
	})
	cfg.Budget = &h.clientBudget
	cfg.BeforeLocalConnect = func(id flow.ID, _ rawsocket.Tuple) error {
		h.connects.Add(1)
		select {
		case h.ids <- id:
		default:
			return errors.New("unexpected connector count")
		}
		h.connectOnce.Do(func() { close(h.connectEntered) })
		select {
		case <-h.connectRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(flowBusinessDeadline):
			return errors.New("controlled OPEN gate expired")
		}
	}
	if err := h.client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	h.client.Start(ctx)
	t.Cleanup(func() { h.client.Close(h.publicPort) })
	// Only pre-business public bind may retry. An original application never
	// redials, and any non-bind failure is immediately visible to this test.
	for attempt := 0; attempt < 8; attempt++ {
		err := h.client.OpenHome(ctx, h.publicPort, flowTCPPort(h.target.listener.Addr()), token, h.relay.ln.Addr().String(), h.epoch.Load(), pins)
		if err == nil {
			return h
		}
		var denied *tunnel.DenyError
		if !errors.As(err, &denied) || denied.Reason != "public_port_bind_failed" {
			t.Fatal(err)
		}
		_, text, err := net.SplitHostPort(flowFreeAddress(t))
		if err != nil {
			t.Fatal(err)
		}
		h.publicPort, err = strconv.Atoi(text)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("pre-business public bind exhausted eight attempts")
	return nil
}

func flowRequireV1Owner(t *testing.T, h *flowCarrierFixture) {
	t.Helper()
	if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool {
		census := h.client.FlowCensus()
		return !census.Unsupported && census.Legacy == 0 && census.Owned > 0
	}) {
		t.Fatalf("selected flow-v1 owner was not installed: %+v", h.client.FlowCensus())
	}
	if h.grants.Load() == 0 {
		t.Fatal("flow-v1 initial authority grant was never obtained")
	}
}

func flowRebuildCarrier(t *testing.T, h *flowCarrierFixture) {
	t.Helper()
	beforeCarriers, beforeGrants := h.relay.accepted.Load(), h.grants.Load()
	_, beforeBinds, traceErr := h.relay.trace.snapshot()
	if traceErr != nil {
		t.Fatal(traceErr)
	}
	needsBind := h.serverBudget.Used() != 0 && h.target.count.Load() != 0
	flowCutCarrier(t, h, false)
	if err := h.client.ResumeCarriers(time.Now().Add(flowBusinessDeadline)); err != nil {
		t.Fatalf("real TLS carrier resume: %v", err)
	}
	if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool { return h.relay.accepted.Load() > beforeCarriers && h.grants.Load() > beforeGrants }) {
		t.Fatal("carrier rebuild lacks a new real TLS pair and fresh authority grant")
	}
	if h.serverBudget.Used() == 0 && h.target.count.Load() == 0 {
		// A failed target has retired its flow and released replay. Its idle
		// allocation still owns the v1 listener and obtains a new authority.
		// Requiring a live flow here contradicts the terminal OPEN assertion.
		// origin: hot-update S5 internal review HOTUPDATE-T40
		c := h.client.FlowCensus()
		if c.Unsupported || c.Legacy != 0 || c.Owned != 0 || c.Retained != 1 || h.connects.Load() != 1 {
			t.Fatalf("refused OPEN lost its retained v1 endpoint: %+v", c)
		}
	} else {
		flowRequireV1Owner(t, h)
	}
	if needsBind && !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool { _, binds, err := h.relay.trace.snapshot(); return err != nil || binds > beforeBinds }) {
		t.Fatal("new carrier did not bind the retained original data stream")
	}
	flowRequireCarrierTrace(t, h)
}

// Closing the relay is asynchronous to the product reader. Wait for its real
// down edge before asking recovery to resume; otherwise a healthy attachment
// snapshot taken before EOF can look like a successful stale-epoch reconnect.
// origin: hot-update S5 internal review HOTUPDATE-T40
func flowCutCarrier(t *testing.T, h *flowCarrierFixture, block bool) {
	t.Helper()
	before := h.downEdges.Load()
	h.relay.cut(t, block, true)
	if !testharness.WaitFor(t, flowBusinessDeadline, time.Millisecond, func() bool { return h.downEdges.Load() > before }) {
		t.Fatal("carrier fault never reached the original owner's down edge")
	}
}

func flowExchangeRecord(t *testing.T, p flowBusinessPair, direction byte, seq uint64) {
	t.Helper()
	source, sink := p.public, p.target
	if direction == 1 {
		source, sink = p.target, p.public
	}
	written := make(chan error, 1)
	go func() { written <- flowWriteBytes(source, flowOracleRecord(p.nonce, direction, seq)) }()
	_ = sink.SetReadDeadline(time.Now().Add(flowBusinessDeadline))
	b := make([]byte, flowRecordBytes)
	_, readErr := io.ReadFull(sink, b)
	writeErr := <-written
	if readErr != nil || writeErr != nil {
		t.Fatalf("original flow exchange: read=%v write=%v", readErr, writeErr)
	}
	if !bytesEqualFlowRecord(b, p.nonce, direction, seq) {
		t.Fatal("original application byte record changed")
	}
}

func bytesEqualFlowRecord(b []byte, nonce [16]byte, direction byte, seq uint64) bool {
	want := flowOracleRecord(nonce, direction, seq)
	if len(b) != len(want) {
		return false
	}
	for i := range b {
		if b[i] != want[i] {
			return false
		}
	}
	return true
}

func flowRequireClosed(t *testing.T, conn *net.TCPConn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(flowBusinessDeadline))
	b := make([]byte, 1)
	n, err := conn.Read(b)
	var netErr net.Error
	if n != 0 || err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("terminal original socket remained open: n=%d err=%v", n, err)
	}
}

// origin: hot-update S5 plan §8 / T40
// origin: hot-update S5 internal review HOTUPDATE-T40
func TestFlowCarrierRebuildLifecycleMatrix(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"open", "idle", "slow", "active", "revoke", "target-failure", "target-eof", "stale-epoch", "expired-ticket"} {
			t.Run(fmt.Sprintf("round-%d/%s", round, arm), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseFlowCarrierLifecycle(t, arm)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "flow TLS carrier lifecycle matrix", beforeG)
	testharness.AssertNoFDLeak(t, "flow TLS carrier lifecycle matrix", beforeFD, 0)
}

func exerciseFlowCarrierLifecycle(t *testing.T, arm string) {
	h := flowNewCarrierFixture(t, arm == "open")
	if arm == "target-failure" {
		_ = h.target.listener.Close()
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", h.publicPort), flowBusinessDeadline)
		if err != nil {
			t.Fatal(err)
		}
		p := conn.(*net.TCPConn)
		defer func() { _ = p.Close() }()
		flowRequireClosed(t, p)
		if h.connects.Load() != 1 || h.target.count.Load() != 0 {
			t.Fatalf("target failure was not exercised: connects=%d accepts=%d", h.connects.Load(), h.target.count.Load())
		}
		flowRebuildCarrier(t, h)
		if h.connects.Load() != 1 {
			t.Fatal("failed OPEN was redialed by carrier resume")
		}
		return
	}
	var p flowBusinessPair
	if arm == "open" {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", h.publicPort), flowBusinessDeadline)
		if err != nil {
			t.Fatal(err)
		}
		p.public = conn.(*net.TCPConn)
		t.Cleanup(func() { _ = p.public.Close() })
		select {
		case <-h.connectEntered:
		case <-time.After(flowBusinessDeadline):
			t.Fatal("OPEN was never admitted before the cut")
		}
		if h.target.count.Load() != 0 {
			t.Fatal("OPEN gate failed to hold the original target dial")
		}
		flowCutCarrier(t, h, false)
		close(h.connectRelease)
		if err := h.client.ResumeCarriers(time.Now().Add(flowBusinessDeadline)); err != nil {
			t.Fatal(err)
		}
		select {
		case p.target = <-h.target.accepted:
		case <-time.After(flowBusinessDeadline):
			t.Fatal("original OPEN did not complete after carrier rebuild")
		}
		if _, err := rand.Read(p.nonce[:]); err != nil {
			t.Fatal(err)
		}
	} else {
		p = flowConnectPair(t, h.target, h.publicPort)
	}
	flowRequireV1Owner(t, h)
	var id flow.ID
	select {
	case id = <-h.ids:
	case <-time.After(flowBusinessDeadline):
		t.Fatal("original FlowID was not bound to its target tuple")
	}
	agentIdentity := flowProductSocketIdentity(t, os.Getpid(), p.target.RemoteAddr(), p.target.LocalAddr())
	brokerIdentity := flowProductSocketIdentity(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr())
	flowExchangeRecord(t, p, 0, 0)
	flowExchangeRecord(t, p, 1, 0)
	if h.serverBudget.Used() != workload.DefaultReplayCapacity || h.clientBudget.Used() != workload.DefaultReplayCapacity {
		t.Fatalf("nonempty flow replay capacity missing: broker=%d agent=%d", h.serverBudget.Used(), h.clientBudget.Used())
	}
	switch arm {
	case "active":
		finish := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(finish) }) }
		t.Cleanup(release)
		stream := flowStartDuplex(t, p, finish, 1, true)
		flowAwaitProgress(t, stream, 8)
		flowRebuildCarrier(t, h)
		flowCheckOriginalSocket(t, os.Getpid(), p.target.RemoteAddr(), p.target.LocalAddr(), agentIdentity)
		flowCheckOriginalSocket(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr(), brokerIdentity)
		release()
		flowFinishDuplex(t, stream)
	case "slow":
		flowExerciseSlowSink(t, h, p)
	case "revoke":
		// The victim stays silent and fixture-owned; shutdown/retirement must be
		// performed by the product while the second original flow still works.
		bob := flowConnectPair(t, h.target, h.publicPort)
		var bobID flow.ID
		select {
		case bobID = <-h.ids:
		case <-time.After(flowBusinessDeadline):
			t.Fatal("survivor FlowID missing")
		}
		flowRebuildCarrier(t, h)
		if err := h.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
			t.Fatal(err)
		}
		flowRequireClosed(t, p.public)
		flowRequireClosed(t, p.target)
		flowExchangeRecord(t, bob, 0, 0)
		flowExchangeRecord(t, bob, 1, 0)
		if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool {
			return h.serverBudget.Used() == workload.DefaultReplayCapacity && h.clientBudget.Used() == workload.DefaultReplayCapacity
		}) {
			t.Fatal("silent revoked flow retained replay capacity")
		}
		if h.target.count.Load() != 2 {
			t.Fatal("survivor or revoked original was redialed")
		}
		if err := h.client.RevokeFlow(bobID, flow.ReasonRevoked); err != nil {
			t.Fatal(err)
		}
	case "stale-epoch":
		h.epoch.Add(1)
		flowCutCarrier(t, h, false)
		if err := h.client.ResumeCarriers(time.Now().Add(flowBusinessDeadline)); err == nil {
			t.Fatal("old epoch resumed after authority changed")
		}
		flowRequireClosed(t, p.public)
		flowRequireClosed(t, p.target)
	case "expired-ticket":
		flowCutCarrier(t, h, true)
		// Poll real retirement, rather than extending grace with reconnects.
		if !testharness.WaitFor(t, tunnel.DetachedGrace+tunnel.ResumeAttempt, 20*time.Millisecond, func() bool { return h.serverBudget.Used() == 0 }) {
			t.Fatal("first-detach ticket survived its absolute deadline")
		}
		h.relay.mu.Lock()
		h.relay.blocked = false
		h.relay.mu.Unlock()
		if err := h.client.ResumeCarriers(time.Now().Add(tunnel.ResumeAttempt)); err == nil {
			t.Fatal("expired detached ticket resurrected its old owner")
		}
		flowRequireClosed(t, p.public)
		flowRequireClosed(t, p.target)
	default:
		if arm == "idle" {
			// A current-owner ticket has no short idle TTL. Keeping the original
			// socket idle longer than detach grace catches accidental TTL reuse.
			select {
			case <-time.After(tunnel.DetachedGrace + tunnel.ResumeAttempt):
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
		}
		if arm == "target-eof" {
			if err := p.target.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			_ = p.public.SetReadDeadline(time.Now().Add(flowBusinessDeadline))
			n, err := p.public.Read(make([]byte, 1))
			if n != 0 || err != io.EOF {
				t.Fatalf("target FIN was not propagated precisely: n=%d err=%v", n, err)
			}
		}
		flowRebuildCarrier(t, h)
		flowCheckOriginalSocket(t, os.Getpid(), p.target.RemoteAddr(), p.target.LocalAddr(), agentIdentity)
		flowCheckOriginalSocket(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr(), brokerIdentity)
		flowExchangeRecord(t, p, 0, 1)
		if arm != "target-eof" {
			flowExchangeRecord(t, p, 1, 1)
		}
	}
	if arm != "revoke" && h.target.count.Load() != 1 {
		t.Fatalf("original target Accept count=%d want=1", h.target.count.Load())
	}
	flowRequireCarrierTrace(t, h)
	// Join ownership while the server/client and fixture sockets remain alive.
	h.server.CloseProxy(h.publicPort)
	h.client.Close(h.publicPort)
	if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool { return h.serverBudget.Used() == 0 && h.clientBudget.Used() == 0 }) {
		t.Fatal("flow terminal owner did not release replay capacity")
	}
	if !testharness.WaitFor(t, flowBusinessDeadline, 10*time.Millisecond, func() bool {
		links := flowProductFDs(t, os.Getpid())
		return flowCountLinks(links, fmt.Sprintf("socket:[%d]", agentIdentity.Ino)) == 0 && flowCountLinks(links, fmt.Sprintf("socket:[%d]", brokerIdentity.Ino)) == 0
	}) {
		t.Fatal("terminal business descriptors remained owned before fixture teardown")
	}
}

func flowExerciseSlowSink(t *testing.T, h *flowCarrierFixture, p flowBusinessPair) {
	const count = 1536 // 24MiB exceeds one 8MiB replay plus kernel buffers.
	// Linux keeps a tiny receive-window threshold after a post-connect buffer
	// shrink, even when SO_RCVBUF is enlarged again. Pausing this reader already
	// fills replay and kernel buffers; keep a usable window for the later drain.
	// origin: hot-update S5 plan §3.4 / §8 T40
	if err := p.target.SetReadBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := p.public.SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	for seq := uint64(1); seq <= count; seq++ {
		payload = append(payload, flowOracleRecord(p.nonce, 0, seq)...)
	}
	_ = p.public.SetWriteDeadline(time.Now().Add(tunnel.CarrierWriteBudget))
	n, err := p.public.Write(payload)
	var netErr net.Error
	if n <= 0 || n >= len(payload) || !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("real short write precondition absent: attempt=%d n=%d err=%v", len(payload), n, err)
	}
	t.Logf("real application syscall: attempt=%d n=%d timeout=true", len(payload), n)
	flowRebuildCarrier(t, h)
	if err := p.public.SetWriteBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	done := make(chan flowStreamResult, 1)
	go func() {
		receipt, err := flowReadLedgerFrom(p.target, p.nonce, 0, 1, count, nil)
		done <- flowStreamResult{receipt: receipt, err: err}
	}()
	written := make(chan error, 1)
	go func() {
		writeErr := flowWriteBytes(p.public, payload[n:])
		if writeErr == nil {
			writeErr = p.public.CloseWrite()
		}
		written <- writeErr
	}()
	// Per-record deadlines alone permit an arbitrarily slow but progressing
	// TCP drain. Bound the entire saturation exercise and join both workers on
	// failure, while leaving the success oracle's exact bytes and EOF intact.
	// origin: hot-update S5 plan §8 T40
	timer := time.NewTimer(6 * flowBusinessDeadline)
	defer timer.Stop()
	var r flowStreamResult
	var writeErr error
	readDone, writeDone := false, false
	for !readDone || !writeDone {
		select {
		case r = <-done:
			readDone = true
		case writeErr = <-written:
			writeDone = true
		case <-timer.C:
			_ = p.public.Close()
			_ = p.target.Close()
			if !readDone {
				r = <-done
			}
			if !writeDone {
				writeErr = <-written
			}
			t.Fatalf("slow-sink drain exceeded total deadline: write=%v read=%v records=%d", writeErr, r.err, r.receipt.Records)
		}
	}
	if writeErr != nil || r.err != nil || r.receipt.Records != count {
		t.Fatalf("partial-write suffix lost across real carrier: write=%v read=%v records=%d", writeErr, r.err, r.receipt.Records)
	}
	flowExchangeRecord(t, p, 1, 1)
}

// origin: hot-update S5 plan §2.2 / §8 / T40 oracle controls
func TestFlowCarrierBindOracleControls(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for _, fault := range []string{"none-open", "none-bind", "missing-bind", "wrong-bind-mode", "data-before-receipt", "pending-open", "missing-control"} {
		t.Run(fault, func(t *testing.T) {
			trace := &flowCarrierTrace{}
			attachment := &flowWireAttachment{trace: trace, granted: make(map[uint32]bool)}
			brokerTap := &flowYamuxTap{attachment: attachment, fromBroker: true, registered: true, streams: make(map[uint32][]byte)}
			agentTap := &flowYamuxTap{attachment: attachment, registered: true, streams: make(map[uint32][]byte)}
			switch fault {
			case "missing-control":
				_, _ = agentTap.Write(flowTapPacket(1, 6, []byte{1}))
			case "missing-bind":
				_, _ = brokerTap.Write(flowTapPacket(2, 8, nil))
			default:
				if fault == "none-open" || fault == "pending-open" {
					_, _ = brokerTap.Write(flowTapPacket(2, 1, make([]byte, 74)))
					body := make([]byte, 75)
					body[72] = 2
					if fault == "pending-open" {
						body[72] = 1
					}
					_, _ = agentTap.Write(flowTapPacket(2, 2, body))
				} else {
					body := []byte{1, 6}
					if fault == "wrong-bind-mode" {
						body[1] = 2
					}
					_, _ = brokerTap.Write(flowTapPacket(2, 3, body))
					if fault != "data-before-receipt" {
						body = make([]byte, 113)
						body[112] = 1
						_, _ = agentTap.Write(flowTapPacket(2, 4, body))
					}
				}
				packet := flowTapPacket(2, 6, []byte{1})
				// Fragmenting both headers proves the tap is a stream parser, not
				// a packet-boundary assumption that only works on a quiet loopback.
				for _, part := range [][]byte{packet[:1], packet[1:13], packet[13:39], packet[39:]} {
					_, _ = brokerTap.Write(part)
				}
			}
			_, _, err := trace.snapshot()
			if strings.HasPrefix(fault, "none-") {
				if err != nil {
					t.Fatalf("healthy independent wire trace rejected: %v", err)
				}
			} else if err == nil {
				t.Fatalf("independent wire oracle accepted %s", fault)
			}
		})
	}
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("tls-round-%d", round), func(t *testing.T) { exerciseFlowTLSRelay(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "independent TLS/yamux tap controls", beforeG)
	testharness.AssertNoFDLeak(t, "independent TLS/yamux tap controls", beforeFD, 0)
}

// Verify the test fixture over actual pinned TLS before depending on it to
// judge unfinished product owners. These literal bytes exercise the tap's
// REGISTER boundary and OPEN/ACK/DATA ordering; they do not simulate the S5
// endpoint lifecycle or substitute for any of the matrix's implementations.
func exerciseFlowTLSRelay(t *testing.T) {
	cert, _ := flowCarrierCertificate(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{tunnel.FlowALPN}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	stopListener := context.AfterFunc(t.Context(), func() { _ = ln.Close() })
	t.Cleanup(func() { stopListener() })
	r := flowStartCarrierRelay(t, ln.Addr().String(), cert)
	body := make([]byte, 75)
	body[72] = 2
	ack := flowTapPacket(2, 2, body)
	open := flowTapPacket(2, 1, make([]byte, 74))
	data := flowTapPacket(2, 6, []byte("independent-original-bytes"))
	const register = "REGISTER flow-lab flow-node 14000 fixture-token 3\n"
	peerDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			peerDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		stopPeer := context.AfterFunc(t.Context(), func() { _ = conn.Close() })
		defer stopPeer()
		_ = conn.SetDeadline(time.Now().Add(flowBusinessDeadline))
		got := make([]byte, len(register))
		if _, err := io.ReadFull(conn, got); err != nil {
			peerDone <- err
			return
		}
		if string(got) != register {
			peerDone <- errors.New("TLS tap changed REGISTER bytes")
			return
		}
		// OK and the first yamux frame deliberately share a single TLS Write.
		if _, err := conn.Write(append([]byte("OK\n"), open...)); err != nil {
			peerDone <- err
			return
		}
		got = make([]byte, len(ack))
		if _, err := io.ReadFull(conn, got); err != nil {
			peerDone <- err
			return
		}
		if string(got) != string(ack) {
			peerDone <- errors.New("TLS tap changed ACK bytes")
			return
		}
		_, err = conn.Write(data)
		peerDone <- err
	}()
	t.Cleanup(func() {
		select {
		case <-peerDone:
		case <-time.After(flowBusinessDeadline):
			t.Error("independent TLS reference peer did not join")
		}
	})
	client, err := tls.DialWithDialer(&net.Dialer{Timeout: tunnel.ResumeAttempt}, "tcp", r.ln.Addr().String(), &tls.Config{RootCAs: r.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{tunnel.FlowALPN}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	_ = client.SetDeadline(time.Now().Add(flowBusinessDeadline))
	if client.ConnectionState().NegotiatedProtocol != tunnel.FlowALPN {
		t.Fatal("actual pinned TLS fixture did not select flow-v1")
	}
	if _, err := io.WriteString(client, register); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 3+len(open))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("real pinned TLS preface/OPEN: %v", err)
	}
	if string(got) != string(append([]byte("OK\n"), open...)) {
		t.Fatal("real TLS tap lost pre-read yamux bytes")
	}
	if _, err := client.Write(ack); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, len(data))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("real pinned TLS DATA: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("real TLS tap changed DATA bytes")
	}
	peerErr := <-peerDone
	peerDone <- peerErr
	if peerErr != nil {
		t.Fatal(peerErr)
	}
	opens, binds, err := r.trace.snapshot()
	if err != nil || opens != 1 || binds != 0 {
		t.Fatalf("real TLS independent trace incomplete: opens=%d binds=%d err=%v", opens, binds, err)
	}
	_ = client.Close()
}

func flowTapPacket(stream uint32, kind byte, body []byte) []byte {
	// An independent literal yamux DATA header followed by a literal flow
	// header. The tested production encoders do not produce these controls.
	b := make([]byte, 12+40+len(body))
	binary.BigEndian.PutUint32(b[4:8], stream)
	binary.BigEndian.PutUint32(b[8:12], uint32(40+len(body)))
	b[12], b[13] = 1, kind
	binary.BigEndian.PutUint32(b[16:20], uint32(len(body)))
	copy(b[52:], body)
	return b
}
