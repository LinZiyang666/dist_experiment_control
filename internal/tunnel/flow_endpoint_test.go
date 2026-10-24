//go:build linux

package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/yamux"
	"golang.org/x/sys/unix"
)

type endpointFixture struct {
	s       *Server
	e       *flowEndpoint
	lookups atomic.Int32
	allowed atomic.Bool
	cfg     FlowConfig
}

func endpointSetup(t *testing.T) *endpointFixture {
	t.Helper()
	f := &endpointFixture{}
	f.allowed.Store(true)
	f.s = NewServer("127.0.0.1:0", "127.0.0.1", func(string, string, int, string, int64) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.cfg = FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "instance", BootID: "boot", OwnerGen: 1, TokenLookup: func(AuthRequest) error {
		f.lookups.Add(1)
		if !f.allowed.Load() {
			return errFlowAuth
		}
		return nil
	}, Authorize: func(r AuthRequest) (Authority, error) {
		if !f.allowed.Load() {
			return Authority{}, errFlowAuth
		}
		return Authority{Enabled: true, Allocation: r.Allocation, Generation: 1, ProxyEpoch: 1, AppliedRevision: 9}, nil
	}}
	if err := f.s.SetFlowConfig(f.cfg); err != nil {
		t.Fatal(err)
	}
	a := Allocation{SID: "sid", NID: "nid", Port: 0, TokenHash: hashToken("token"), Epoch: 1}
	// Port zero asks the real kernel for a fresh listener; the allocation key
	// stays zero in this local owner test and is never sent through REGISTER.
	e, err := reserveEndpoint(f.s, a, fenceSnap{})
	if err != nil {
		t.Fatal(err)
	}
	f.e = e
	return f
}
func waitEndpoint(t *testing.T, predicate func() bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !predicate() {
		select {
		case <-timer.C:
			t.Fatal("endpoint transition timed out")
		case <-tick.C:
		}
	}
}
func listenerIdentity(t *testing.T, ln net.Listener) uint64 {
	t.Helper()
	tcp, ok := ln.(*net.TCPListener)
	if !ok {
		t.Fatal("not a TCP listener")
	}
	rc, err := tcp.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var ino uint64
	if err = rc.Control(func(fd uintptr) {
		var stat unix.Stat_t
		if e := unix.Fstat(int(fd), &stat); e != nil {
			t.Error(e)
		}
		ino = stat.Ino
	}); err != nil {
		t.Fatal(err)
	}
	return ino
}
func testCarrier(t *testing.T, m *flow.Manager, id flow.EndpointID) *flowCarrier {
	t.Helper()
	left, right := net.Pipe()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	client := tls.Client(left, clientTLSConfig())
	server := tls.Server(right, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	handshake := make(chan error, 1)
	go func() { handshake <- server.Handshake() }()
	if err = client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err = <-handshake; err != nil {
		t.Fatal(err)
	}
	aMux, err := yamux.Client(client, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	bMux, err := yamux.Server(server, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	control, err := aMux.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	peerControl, err := bMux.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
		_ = aMux.Close()
		_ = bMux.Close()
		_ = peerControl.Close()
	})
	return newFlowCarrier(client, aMux, control, m, id, 1, 1, 0)
}

// Real listener/pubConn kernel identities and TLS/yamux attachments. The OPEN
// frame path is separately exercised once P2's codec replaces its refusal stub.
// origin: hot-update S5 plan §4.2 / T12
func TestEndpointSurvivesSessionLossAndResumes(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			t.Run("resume", func(t *testing.T) { exerciseEndpointResume(t) })
		}
		testharness.AssertNoGoroutineLeak(t, "endpoint resume", beforeG)
		testharness.AssertNoFDLeak(t, "endpoint resume", beforeFD, 0)
	})
	t.Run("wire", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseEndpointWire(t)
		}
		testharness.AssertNoGoroutineLeak(t, "endpoint wire", beforeG)
		testharness.AssertNoFDLeak(t, "endpoint wire", beforeFD, 0)
	})
	t.Run("rehome", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseEndpointRehome(t)
		}
		testharness.AssertNoGoroutineLeak(t, "endpoint rehome", beforeG)
		testharness.AssertNoFDLeak(t, "endpoint rehome", beforeFD, 0)
	})
}

// A newer allocation opens a new endpoint only after revocation terminates
// the old original sockets. A stale directive cannot replace that successor.
// origin: hot-update S5 plan §0 / §4.2 / T12
func exerciseEndpointRehome(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	// Keep the public port reserved until REGISTER lookup has established
	// the control socket. Ephemeral control listeners and client dials must
	// not consume the port before the actual endpoint bind.
	// origin: hot-update S5 plan §8 / T12 / testing-standards T5
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reservation.Close() }()
	port := reservation.Addr().(*net.TCPAddr).Port
	releasePort := sync.OnceValue(reservation.Close)
	var blockLookup atomic.Bool
	var lookupCalls atomic.Int32
	entered, release := make(chan struct{}, 1), make(chan struct{})
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "rehome", BootID: "boot", OwnerGen: 1, TokenLookup: func(AuthRequest) error { return nil }, Authorize: func(r AuthRequest) (Authority, error) { return Authority{Enabled: true, Allocation: r.Allocation}, nil }}
	start := func() (*Server, string, proto.CertPins) {
		s := NewServer("127.0.0.1:0", "127.0.0.1", func(string, string, int, string, int64) error { return nil }, silentLog())
		scfg := cfg
		scfg.Budget = &workload.ReplayBudget{}
		scfg.TokenLookup = func(r AuthRequest) error {
			if err := releasePort(); err != nil {
				return err
			}
			if !blockLookup.Load() {
				return nil
			}
			lookupCalls.Add(1)
			select {
			case entered <- struct{}{}:
			default:
			}
			timer := time.NewTimer(max(time.Until(r.Deadline), 0))
			defer timer.Stop()
			select {
			case <-release:
				return nil
			case <-r.Stop:
				return errFlowAuth
			case <-timer.C:
				return errFlowAuth
			}
		}
		if err := s.SetFlowConfig(scfg); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		addr := s.ln.Addr().String()
		pins := proto.CertPins{Current: certFingerprintDER(s.tlsCert.Certificate[0])}
		s.mu.Unlock()
		return s, addr, pins
	}
	old, oldAddr, oldPins := start()
	defer old.Close()
	next, nextAddr, nextPins := start()
	defer next.Close()
	c := NewClient(oldAddr, "rehome-session", "rehome-node", func(int) (int, error) { return target.Addr().(*net.TCPAddr).Port, nil }, silentLog())
	if err := c.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	c.Start(ctx)
	// Close the manager before the deferred context cancellation: Start's
	// asynchronous cleanup also closes it and reads the immutable observer
	// hook. Leaving that close to a later goroutine races the next fixture's
	// hook installation even when the endpoint's sockets already closed.
	// origin: hot-update S5 plan §8 / T51 / GATES-FINAL-3
	defer func() {
		c.Close(port)
		if err := c.flowState.manager.Close(); err != nil {
			t.Errorf("close client flow manager: %v", err)
		}
	}()
	if err := c.OpenHome(ctx, port, target.Addr().(*net.TCPAddr).Port, "token", oldAddr, 1, oldPins); err != nil {
		t.Fatalf("open original allocation: %v", err)
	}
	open := func() (net.Conn, *net.TCPConn) {
		pub, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), ResumeAttempt)
		if err != nil {
			t.Fatal(err)
		}
		_ = target.SetDeadline(time.Now().Add(ResumeAttempt))
		local, err := target.AcceptTCP()
		if err != nil {
			_ = pub.Close()
			t.Fatal(err)
		}
		_ = pub.SetDeadline(time.Now().Add(ResumeAttempt))
		_ = local.SetDeadline(time.Now().Add(ResumeAttempt))
		if _, err := pub.Write([]byte("owned")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 5)
		if _, err := io.ReadFull(local, got); err != nil || string(got) != "owned" {
			t.Fatal("new allocation did not deliver bytes")
		}
		return pub, local
	}
	public, original := open()
	defer func(closeFunc func() error) { _ = closeFunc() }(public.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(original.Close)
	c.flowState.mu.Lock()
	firstID := c.flowState.endpoints[port].id
	c.flowState.mu.Unlock()
	if !old.CloseProxy(port) {
		t.Fatal("old allocation revocation lost endpoint")
	}
	// Hold the actual new REGISTER lookup while another directive times out.
	// It must wait at per-port admission, before a second authentication attempt.
	// origin: hot-update S5 plan §4.2 / T12
	blockLookup.Store(true)
	result := make(chan error, 1)
	go func() { result <- c.ApplyHome(port, nextAddr, 2, nextPins) }()
	select {
	case <-entered:
	case <-time.After(ResumeAttempt):
		t.Fatal("replacement did not reach bounded lookup")
	}
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	err = c.OpenHome(short, port, target.Addr().(*net.TCPAddr).Port, "token", nextAddr, 2, nextPins)
	cancelShort()
	if !errors.Is(err, context.DeadlineExceeded) || lookupCalls.Load() != 1 {
		t.Fatalf("concurrent replacement bypassed admission: err=%v lookups=%d", err, lookupCalls.Load())
	}
	blockLookup.Store(false)
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("new allocation refused replacement: %v", err)
	}
	if n, err := original.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("rehome retained the old target socket")
	}
	if n, err := public.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("rehome retained the revoked public socket")
	}
	newPublic, newTarget := open()
	defer func(closeFunc func() error) { _ = closeFunc() }(newPublic.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(newTarget.Close)
	c.flowState.mu.Lock()
	secondID := c.flowState.endpoints[port].id
	c.flowState.mu.Unlock()
	if secondID == firstID {
		t.Fatal("new allocation resumed an old endpoint")
	}
	if err := c.OpenHome(ctx, port, target.Addr().(*net.TCPAddr).Port, "token", oldAddr, 1, oldPins); err != nil {
		t.Fatalf("replay stale allocation: %v", err)
	}
	c.flowState.mu.Lock()
	currentID := c.flowState.endpoints[port].id
	c.flowState.mu.Unlock()
	if currentID != secondID {
		t.Fatal("stale allocation replaced the new endpoint")
	}
}
func exerciseEndpointResume(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	defer f.s.Close()
	e := f.e
	inode := listenerIdentity(t, e.listener)
	public, err := net.Dial("tcp", e.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(public.Close)
	_ = public.SetDeadline(time.Now().Add(5 * time.Second))
	var raw *rawsocket.Socket
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); raw = e.pendingRaw; return raw != nil })
	original := raw.Fingerprint()
	old := testCarrier(t, f.s.flowState.manager, e.id)
	// Pending accept ownership already exists before the first RESUME grant.
	f.s.mu.Lock()
	e.attachment = old
	e.phase = EndpointPhaseAttached
	f.s.mu.Unlock()
	old.Close()
	old.Join()
	detachEndpoint(f.s, e, old)
	if listenerIdentity(t, e.listener) != inode {
		t.Fatal("session loss closed listener")
	}
	same, err := reserveEndpoint(f.s, e.allocation, e.fence)
	if err != nil || same != e {
		t.Fatalf("reconnect rebound original listener: %v", err)
	}
	next := testCarrier(t, f.s.flowState.manager, e.id)
	request := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeInitial, RequestID: [16]byte{1}, Owner: 1, Target: 22}
	response, _, err := resumeEndpoint(f.s, e, request, next, time.Now().Add(time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Carrier != 1 || response.Endpoint != e.id {
		t.Fatal("resume did not attach original endpoint")
	}
	waitEndpoint(t, func() bool { return len(flow.States(f.s.flowState.manager, e.id)) == 1 })
	if raw.Fingerprint() != original || listenerIdentity(t, e.listener) != inode {
		t.Fatal("resume replaced an original business descriptor")
	}
	states := flow.States(f.s.flowState.manager, e.id)
	state := states[0]
	if err = flow.Receive(f.s.flowState.manager, flow.Frame{Version: flow.Version, Kind: flow.KindData, FlowID: state.ID, CarrierGen: 1, Flags: flow.FlagDirection, BodyLen: 5, Body: []byte("alive")}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err = io.ReadFull(public, got); err != nil || string(got) != "alive" {
		t.Fatalf("original public connection lost: %q %v", got, err)
	}
	next.Close()
	next.Join()
}

// origin: hot-update S5 plan §8 / T51
func TestTunnelFlowFiveRoundLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTunnelFlowLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "tunnel flow lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "tunnel flow lifecycle", beforeFD, 0)
}
func exerciseTunnelFlowLifecycle(t *testing.T) {
	t.Helper()
	exerciseIdleListenerAdmission(t)
	exerciseCapacityAdmissionObservation(t)
	f := endpointSetup(t)
	// A detached v1 endpoint on a different port cannot hide a legacy owner.
	c := &Client{sessions: map[int]*clientSession{9: {}}, flowState: &clientFlowState{manager: f.s.flowState.manager, endpoints: map[int]*clientEndpoint{10: {id: f.e.id}}}}
	if census := c.FlowCensus(); census.Legacy != 1 || census.Retained != 1 {
		t.Fatal("detached endpoint hid unrelated legacy responsibility")
	}
	inode := listenerIdentity(t, f.e.listener)
	if inode == 0 {
		t.Fatal("missing listener identity")
	}
	if infos := f.s.SessionInfos(); len(infos) != 1 {
		t.Fatal("detached endpoint missing from census")
	}
	closed := make(chan []int, 1)
	go func() { closed <- f.s.CloseSession("sid") }()
	var ports []int
	select {
	case ports = <-closed:
	case <-time.After(time.Second):
		_ = f.e.listener.Close()
		<-closed
		t.Fatal("endpoint cleanup failed to close and join listener")
	}
	if len(ports) != 1 {
		t.Fatal("close ignored detached endpoint")
	}
	if len(f.s.SessionInfos()) != 0 {
		t.Fatal("closed endpoint retained listener")
	}
	f.s.Close()
}

// The integrated wire exercise remains a real failure while P2 is a compile
// stub. It is never replaced with a fake encoder or a skipped success.
func exerciseEndpointWire(t *testing.T) {
	t.Helper()
	probe := flow.Frame{Version: flow.Version, Kind: flow.KindACK, FlowID: flow.ID{1}, CarrierGen: 1}
	if err := flow.EncodeFrame(io.Discard, probe); errors.Is(err, flow.ErrCodecNotImplemented) {
		t.Fatalf("NOT_RUN pending P2: %v", err)
	} else if err != nil {
		t.Fatal(err)
	}
	target, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	reservation, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	// Transfer the reserved port only at the first real REGISTER lookup;
	// an ephemeral control listener or client dial cannot steal it first.
	// origin: hot-update S5 plan §8 / T12 / testing-standards T5
	defer func() { _ = reservation.Close() }()
	releasePort := sync.OnceValue(reservation.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer("127.0.0.1:0", "127.0.0.1", func(string, string, int, string, int64) error { return nil }, logger)
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "wire", BootID: "boot", OwnerGen: 1, TokenLookup: func(AuthRequest) error { return releasePort() }, Authorize: func(r AuthRequest) (Authority, error) { return Authority{Enabled: true, Allocation: r.Allocation}, nil }}
	if err = server.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err = server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.mu.Lock()
	address := server.ln.Addr().String()
	server.mu.Unlock()
	client := NewClient(address, "wire-session", "wire-node", func(int) (int, error) { return target.Addr().(*net.TCPAddr).Port, nil }, logger)
	clientCfg := cfg
	clientCfg.Budget = &workload.ReplayBudget{}
	if err = client.SetFlowConfig(clientCfg); err != nil {
		t.Fatal(err)
	}
	client.Start(ctx)
	// Quiesce manager observations before the next fixture replaces Hook;
	// cancellation alone only schedules Start's asynchronous cleanup.
	// origin: hot-update S5 plan §8 / T51 / GATES-FINAL-3
	defer func() {
		client.Close(port)
		if err := client.flowState.manager.Close(); err != nil {
			t.Errorf("close client flow manager: %v", err)
		}
	}()
	if err = client.Open(port, target.Addr().(*net.TCPAddr).Port, "token"); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	endpoint := server.flowState.endpoints[port]
	server.mu.Unlock()
	if endpoint == nil {
		t.Fatal("complete production owners did not select flow ALPN")
	}
	inode := listenerIdentity(t, endpoint.listener)
	public, err := net.Dial("tcp", endpoint.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(public.Close)
	_ = public.SetDeadline(time.Now().Add(10 * time.Second))
	_ = target.SetDeadline(time.Now().Add(5 * time.Second))
	original, err := target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(original.Close)
	_ = original.SetDeadline(time.Now().Add(10 * time.Second))
	for step := 0; step < 2; step++ {
		want := []byte("same socket in both directions")
		if _, err = public.Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err = io.ReadFull(original, got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("target bytes: %q %v", got, err)
		}
		if _, err = original.Write(want); err != nil {
			t.Fatal(err)
		}
		if _, err = io.ReadFull(public, got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("public bytes: %q %v", got, err)
		}
		if step == 0 {
			if err = client.PrepareHandoff(2, [16]byte{19}, time.Now().Add(ResumeAttempt)); err != nil {
				t.Fatal(err)
			}
			server.mu.Lock()
			old := endpoint.attachment
			server.mu.Unlock()
			old.Close()
			old.Join()
			waitEndpoint(t, func() bool {
				server.mu.Lock()
				defer server.mu.Unlock()
				return endpoint.attachment != nil && endpoint.attachment != old
			})
		}
	}
	if listenerIdentity(t, endpoint.listener) != inode {
		t.Fatal("wire reconnect rebound listener")
	}
	// A completed FIN has local receipt bits beyond the two wire FIN bits.
	// Reconnect must retain CloseWrite while the reverse direction still writes.
	// origin: hot-update S5 plan §2.2 / §3.3 / T12
	if err = original.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if n, err := public.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("half-close did not reach public EOF: n=%d err=%v", n, err)
	}
	server.mu.Lock()
	last := endpoint.attachment
	server.mu.Unlock()
	last.Close()
	last.Join()
	waitEndpoint(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return endpoint.attachment != nil && endpoint.attachment != last
	})
	if _, err = public.Write([]byte("reverse-tail")); err != nil {
		t.Fatal(err)
	}
	tail := make([]byte, len("reverse-tail"))
	if _, err = io.ReadFull(original, tail); err != nil || string(tail) != "reverse-tail" {
		t.Fatalf("half-close reconnect lost reverse tail: %v", err)
	}
	frozen, err := freezeEndpointAfterResume(client, time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	_, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	stateFile := snapshot.Owned[0]
	if err = frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	if _, err := stateFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("client thaw retained a sealed descriptor")
	}
	waitEndpoint(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return endpoint.attachment != nil && endpoint.permit == nil
	})
	server.mu.Lock()
	owner := endpoint.ownerGen
	server.mu.Unlock()
	if owner != 1 {
		t.Fatal("old owner consumed handoff permit on reconnect/thaw")
	}

	_ = target.SetDeadline(time.Now().Add(25 * time.Millisecond))
	extra, err := target.AcceptTCP()
	if err == nil {
		_ = extra.Close()
		t.Fatal("wire reconnect redialed original target")
	}
}

// Business IO can resume before attachClientFlow releases admission and the
// port supervisor publishes its transport. The broker attachment alone is not
// a client safe point; use the same completed recovery barrier as image restore.
// origin: hot-update S5 plan §4.2 / §7.1 / T12
func freezeEndpointAfterResume(c *Client, deadline time.Time) (*ClientFrozen, error) {
	if err := c.ResumeCarriers(deadline); err != nil {
		return nil, err
	}
	return c.FreezeFlows(deadline)
}

type endpointResumeBarrier struct {
	slog.Handler
	resumed atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *endpointResumeBarrier) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "tunnel: flow carrier resumed" && b.resumed.Add(1) == 2 {
		close(b.entered)
		<-b.release
	}
	return b.Handler.Handle(ctx, record)
}

func (b *endpointResumeBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

// Hold the successful RESUME at its last log emission: data pumps already run,
// but admission and the supervisor's old transport still belong to the dial.
// This is the ordering which made the wire fixture's immediate Freeze fail.
// origin: hot-update S5 plan §4.2 / §7.1 / §8 / T12
func TestEndpointResumeWaitsForAdmissionBeforeFreeze(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseEndpointResumeAdmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "endpoint resume admission", beforeG)
	testharness.AssertNoFDLeak(t, "endpoint resume admission", beforeFD, 0)
}

func exerciseEndpointResumeAdmission(t *testing.T) {
	t.Helper()
	barrier := &endpointResumeBarrier{Handler: slog.NewTextHandler(io.Discard, nil), entered: make(chan struct{}), release: make(chan struct{})}
	defer barrier.unblock()
	f := settlementSetupWithDial(t, func(ctx context.Context, f *settlementFixture) error {
		f.client.logger = slog.New(barrier)
		clientCtx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		f.client.Start(clientCtx)
		t.Cleanup(func() { f.client.Close(f.port); cancel() })
		return f.client.OpenHome(ctx, f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{})
	})
	public, target, id := settlementConnection(t, f, 1)
	inode := listenerIdentity(t, f.endpoint.listener)
	f.server.mu.Lock()
	old := f.endpoint.attachment
	f.server.mu.Unlock()
	old.Close()
	old.Join()
	select {
	case <-barrier.entered:
	case <-time.After(ResumeAttempt):
		t.Fatal("replacement did not reach the admitted RESUME barrier")
	}
	f.server.mu.Lock()
	attached := f.endpoint.attachment != nil && f.endpoint.attachment != old
	f.server.mu.Unlock()
	if !attached || f.client.FlowCensus().Admission == 0 {
		t.Fatal("RESUME barrier did not retain broker attachment and client admission")
	}
	// Both original sockets can carry bytes while the successful dial is still
	// in flight. Waiting for a payload is therefore insufficient before Freeze.
	exchange := func() {
		t.Helper()
		for _, pair := range [][2]*net.TCPConn{{public, target}, {target, public}} {
			want := []byte("original socket before and after the admission cut")
			if _, err := pair[0].Write(want); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(pair[1], got); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("original endpoint bytes: %q %v", got, err)
			}
		}
	}
	exchange()
	// The held dial must consume this caller's deadline, without attempting a
	// cut. Removing the recovery barrier instead returns invalid owner state.
	frozen, err := freezeEndpointAfterResume(f.client, time.Now().Add(50*time.Millisecond))
	if frozen != nil || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("freeze bypassed admitted RESUME: frozen=%t err=%v", frozen != nil, err)
	}
	barrier.unblock()
	frozen, err = freezeEndpointAfterResume(f.client, time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := frozen.Export(); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	exchange()
	state, _, err := flow.Observe(f.client.flowState.manager, id)
	if err != nil || state.EndpointID != f.peer.id || listenerIdentity(t, f.endpoint.listener) != inode {
		t.Fatal("admission cut replaced the original endpoint or flow")
	}
	_ = f.target.SetDeadline(time.Now().Add(25 * time.Millisecond))
	if extra, err := f.target.AcceptTCP(); err == nil {
		_ = extra.Close()
		t.Fatal("admission cut redialed the original target")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("target accept census failed: %v", err)
	}
}

// Idle listeners share admission fairly: opening the 33rd listener must not
// depend on traffic arriving at any of the first 32 listeners.
// origin: hot-update S5 plan §3.4 / T51
func exerciseIdleListenerAdmission(t *testing.T) {
	f := endpointSetup(t)
	defer func() {
		f.s.mu.Lock()
		var listeners []net.Listener
		for _, e := range f.s.flowState.endpoints {
			listeners = append(listeners, e.listener)
		}
		f.s.mu.Unlock()
		done := make(chan struct{})
		go func() { f.s.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			for _, ln := range listeners {
				_ = ln.Close()
			}
			<-done
			t.Fatal("idle endpoint cleanup did not close original listeners")
		}
	}()
	var last *flowEndpoint
	for i := 0; i < flow.MaxPendingFDs+1; i++ {
		if i == flow.MaxPendingFDs-1 {
			waitEndpoint(t, func() bool { return len(f.s.flowState.pending) == flow.MaxPendingFDs })
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		last = &flowEndpoint{id: flow.EndpointID{byte(i + 1)}, allocation: Allocation{SID: "idle", NID: "nid", Port: port}, listener: listener, phase: EndpointPhaseBinding, stop: make(chan struct{}), done: make(chan struct{}), nextSeq: 1}
		f.s.mu.Lock()
		f.s.flowState.endpoints[port] = last
		f.s.mu.Unlock()
		go endpointAccept(f.s, last)
	}
	public, err := net.Dial("tcp", last.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(public.Close)
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return last.pendingRaw != nil })
	if used := f.cfg.Budget.Used(); used != 0 {
		t.Fatalf("idle/unowned listener reserved replay: %d", used)
	}
}

// A capacity receipt reports the first native refusal without changing the
// pending socket's owner, reservation limit or later FIFO recovery.
// origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
func exerciseCapacityAdmissionObservation(t *testing.T) {
	t.Helper()
	var receipts atomic.Int32
	type capacityReceipt struct {
		event  drillobserve.Event
		socket [32]byte
	}
	observed := make(chan capacityReceipt, 1)
	var socketMu sync.Mutex
	sockets := make(map[[16]byte][32]byte)
	prior := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) {
		socketMu.Lock()
		defer socketMu.Unlock()
		if e.Kind == "flow-socket" {
			sockets[e.Flow] = e.Digest
		}
		if e.Kind == "flow-capacity" {
			receipts.Add(1)
			select {
			case observed <- capacityReceipt{event: e, socket: sockets[e.Flow]}:
			default:
			}
		}
	}
	defer func() { drillobserve.Hook = prior }()
	f := endpointSetup(t)
	defer func() { f.s.Close() }()
	var logs []*workload.Replay
	for i := 0; i < 32; i++ {
		replay, err := workload.NewReplay(f.cfg.Budget, workload.DefaultReplayCapacity)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, replay)
	}
	defer func() {
		for _, replay := range logs {
			_ = replay.Close()
		}
	}()
	f.s.mu.Lock()
	f.e.targetPort, f.e.ownerGen, f.e.carrierGen = 22, 1, 1
	f.s.mu.Unlock()
	public, err := net.Dial("tcp", f.e.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = public.Close() }()
	var observedReceipt capacityReceipt
	select {
	case observedReceipt = <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("missing native capacity refusal")
	}
	// origin: hot-update S5 plan lead C1 / §9.1. The native producer
	// already reverses the broker tuple to match this real client socket;
	// reversing it again in the collector would lose the refusal binding.
	local, err := netip.ParseAddrPort(public.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	remote, err := netip.ParseAddrPort(public.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if observedReceipt.socket != drillobserve.SocketIdentity(local, remote) {
		t.Fatal("native capacity socket receipt does not match original peer tuple")
	}
	receipt := observedReceipt.event
	f.s.mu.Lock()
	original := f.e.pendingRaw
	f.s.mu.Unlock()
	if original == nil {
		t.Fatal("capacity refusal lost pending original socket")
	}
	fp := original.Fingerprint()
	if receipt.Endpoint != [16]byte(f.e.id) || receipt.Values[0] != uint64(flow.ReasonCapacity) || receipt.Values[1] != 256<<20 || receipt.Values[2] != 256<<20 || receipt.Values[3] != fp.Dev || receipt.Values[4] != fp.Ino {
		t.Fatalf("capacity receipt changed native reason, budget or socket: %v", receipt.Values)
	}
	time.Sleep(60 * time.Millisecond)
	if receipts.Load() != 1 || f.cfg.Budget.Used() != 256<<20 {
		t.Fatal("capacity wait duplicated receipt or changed reservation")
	}
	if _, err := public.Write([]byte("same-original-pending-socket")); err != nil {
		t.Fatal(err)
	}
	for _, replay := range logs {
		_ = replay.Close()
	}
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return f.e.pendingRaw == nil })
	if f.cfg.Budget.Used() != workload.DefaultReplayCapacity {
		t.Fatal("released capacity did not admit original pending socket")
	}
	states := flow.States(f.s.flowState.manager, f.e.id)
	if len(states) != 1 || original.Fingerprint() != fp {
		t.Fatal("capacity recovery replaced original socket")
	}
}
