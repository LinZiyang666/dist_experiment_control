//go:build linux

package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/yamux"
)

// Starting an idle adapter before Agent.New was valid before S5. Lifetime
// anchoring must neither select legacy mode nor orphan a later flow owner.
// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
func TestClientFlowConfigurationAfterStartKeepsLifetime(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStartedClientFlowConfig(t)
	}
	testharness.AssertNoGoroutineLeak(t, "started client flow configuration x5", beforeG)
	testharness.AssertNoFDLeak(t, "started client flow configuration x5", beforeFD, 0)
}

func exerciseStartedClientFlowConfig(t *testing.T) {
	t.Helper()
	exerciseClientCancellationWithBlockedTLS(t)
	c := NewClient("127.0.0.1:1", "lab", "node", nil, silentLog())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.Start(ctx)
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "started-client", BootID: "boot", OwnerGen: 1}
	if err := c.SetFlowConfig(cfg); err != nil {
		t.Fatalf("idle lifetime prevented owner construction: %v", err)
	}
	owner := c.flowState
	if c.FlowCensus().Unsupported || c.ctx.Err() != nil || owner.cfg.Budget != cfg.Budget {
		t.Fatal("configured owner lost its lifetime or shared budget")
	}
	if err := c.SetFlowConfig(cfg); !errors.Is(err, errFlowState) || c.flowState != owner {
		t.Fatal("configuration replaced an existing owner")
	}
	cancel()
	if !testharness.WaitFor(t, ResumeAttempt, time.Millisecond, func() bool {
		_, _, err := flow.Observe(owner.manager, flow.ID{})
		return errors.Is(err, flow.ErrClosed)
	}) {
		t.Fatal("lifetime cancellation did not close the configured owner")
	}
	canceled := NewClient("127.0.0.1:1", "lab", "node", nil, silentLog())
	canceled.Start(ctx)
	if err := canceled.SetFlowConfig(cfg); !errors.Is(err, errFlowState) {
		t.Fatal("cancelled lifetime admitted an owner after cleanup")
	}
}

type clientCloseProbe struct {
	net.Conn
	client *Client
	armed  atomic.Bool
	wrote  chan struct{}
	once   sync.Once
	closed chan bool
}

func (p *clientCloseProbe) Write(b []byte) (int, error) {
	if p.armed.Load() {
		p.once.Do(func() { close(p.wrote) })
	}
	return p.Conn.Write(b)
}

func (p *clientCloseProbe) Close() error {
	free := p.client.mu.TryLock()
	if free {
		p.client.mu.Unlock()
	}
	select {
	case p.closed <- free:
	default:
	}
	return p.Conn.Close()
}

// Cancellation must first interrupt the underlying socket, outside c.mu.
// An idle unread peer stalls TLS close_notify; an active shared writer must
// also be woken without waiting for yamux's independent timeout.
// origin: hot-update S5 plan §3.5 / §7.2 / P13-REGRESS
func exerciseClientCancellationWithBlockedTLS(t *testing.T) {
	t.Helper()
	for _, busy := range []bool{false, true} {
		exerciseClientTLSCancellation(t, busy)
	}
}

func exerciseClientTLSCancellation(t *testing.T, busy bool) {
	t.Helper()
	c := NewClient("127.0.0.1:1", "lab", "node", nil, silentLog())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.Start(ctx)
	if err := c.SetFlowConfig(FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "cancel", BootID: "boot", OwnerGen: 1}); err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer func() { _ = left.Close() }()
	gate := &pausedReadConn{Conn: right, closed: make(chan struct{})}
	defer func() { _ = gate.Close() }()
	probe := &clientCloseProbe{Conn: left, client: c, wrote: make(chan struct{}), closed: make(chan bool, 4)}
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	secure := tls.Client(probe, clientTLSConfig())
	peer := tls.Server(gate, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	handshake := make(chan error, 1)
	go func() { handshake <- peer.Handshake() }()
	if err := secure.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-handshake; err != nil {
		t.Fatal(err)
	}
	gate.paused.Store(true)
	probe.armed.Store(true)
	muxCfg := flowMuxConfig(io.Discard)
	// The oracle isolates lifetime cancellation from yamux's independent timer.
	muxCfg.ConnectionWriteTimeout = 4 * ResumeAttempt
	mux, err := yamux.Client(secure, muxCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = left.Close(); _ = mux.Close() }()
	opened := make(chan error, 1)
	if busy {
		go func() { _, err := mux.OpenStream(); opened <- err }()
		select {
		case <-probe.wrote:
		case <-time.After(ResumeAttempt):
			t.Fatal("TLS shared writer never reached the unread peer")
		}
	}
	_, sessionCancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.sessions[1] = &clientSession{conn: secure, yamuxSess: mux, cancel: sessionCancel, gen: 1}
	c.mu.Unlock()
	cancel()
	select {
	case free := <-probe.closed:
		if !free {
			t.Fatal("lifetime cleanup closed the carrier while holding c.mu")
		}
	case <-time.After(ResumeAttempt):
		t.Fatal("lifetime cleanup waited behind the blocked TLS writer")
	}
	if busy {
		select {
		case err := <-opened:
			if err == nil {
				t.Fatal("unread peer unexpectedly completed the yamux stream open")
			}
		case <-time.After(ResumeAttempt):
			t.Fatal("lifetime cancellation did not wake the shared writer")
		}
	}
	if !testharness.WaitFor(t, ResumeAttempt, time.Millisecond, func() bool {
		_, _, err := flow.Observe(c.flowState.manager, flow.ID{})
		return errors.Is(err, flow.ErrClosed)
	}) {
		t.Fatal("failed carrier prevented lifetime owner cleanup")
	}
}

// Pending and failed opens also pin transport mode: checking only installed
// sessions would allow configuration to overtake a legacy handshake.
// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
func TestClientFlowConfigurationCannotOvertakeOpen(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseClientFlowConfigAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "client flow configuration admission x5", beforeG)
	testharness.AssertNoFDLeak(t, "client flow configuration admission x5", beforeFD, 0)
}

func exerciseClientFlowConfigAdmission(t *testing.T) {
	t.Helper()
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "used-client", BootID: "boot", OwnerGen: 1}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := NewClient("127.0.0.1:1", "lab", "node", nil, silentLog())
	c.Start(ctx)
	if err := c.OpenHome(ctx, 1, 1, "token", "127.0.0.1:1", 0, proto.CertPins{}); !errors.Is(err, ErrHomePinsRequired) {
		t.Fatalf("open did not reach the pre-dial pin guard: %v", err)
	}
	if err := c.SetFlowConfig(cfg); !errors.Is(err, errFlowState) {
		t.Fatal("a failed open allowed transport mode to change")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	c = NewClient(ln.Addr().String(), "lab", "node", nil, silentLog())
	c.Start(ctx)
	done := make(chan error, 1)
	go func() {
		_, _, err := c.dialAndRegister(ctx, 1, "token", "", 0, proto.CertPins{})
		done <- err
	}()
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = accepted.Close() }()
	if c.SessionCount() != 0 {
		t.Fatal("blocked TLS handshake unexpectedly installed a session")
	}
	if err := c.SetFlowConfig(cfg); !errors.Is(err, errFlowState) {
		t.Fatal("configuration overtook a pending legacy dial")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked TLS handshake succeeded after cancellation")
		}
	case <-time.After(ResumeAttempt):
		t.Fatal("cancellation did not stop the failed carrier dial")
	}
}

type pausedReadConn struct {
	net.Conn
	paused atomic.Bool
	closed chan struct{}
	once   sync.Once
}

func (c *pausedReadConn) Read(p []byte) (int, error) {
	if c.paused.Load() {
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Read(p)
}
func (c *pausedReadConn) Close() error { c.once.Do(func() { close(c.closed) }); return c.Conn.Close() }
