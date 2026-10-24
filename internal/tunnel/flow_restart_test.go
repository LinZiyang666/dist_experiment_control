//go:build linux

package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/yamux"
)

// Broker exec continuity belongs to S6, but an ordinary broker restart must
// still serve new requests without replacing the agent or its port supervisor.
// origin: hot-update S5 plan §0 / §4.2 / P13-REGRESS
func TestOrdinarySupervisorRecoversAfterFlowBrokerRestart(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("restart", func(t *testing.T) { exerciseFlowBrokerRestart(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "ordinary flow broker restart x5", beforeG)
	testharness.AssertNoFDLeak(t, "ordinary flow broker restart x5", beforeFD, 0)
}

func exerciseFlowBrokerRestart(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.client.Start(ctx)
	f.client.SetBackoffForTest(time.Millisecond, 5*time.Millisecond)
	if err := f.client.OpenHome(ctx, f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{}); err != nil {
		t.Fatal(err)
	}
	f.client.mu.Lock()
	originalSession := f.client.sessions[f.port]
	f.client.mu.Unlock()
	public, target, _ := settlementConnection(t, f, 1)
	f.server.mu.Lock()
	addr, cert := f.server.ln.Addr().String(), f.server.tlsCert
	cfg := f.server.flowState.cfg
	f.server.mu.Unlock()
	f.server.Close()
	flowRequirePeerEOF(t, public)

	next := NewServerWithCert(addr, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, cert, silentLog())
	cfg.Budget, cfg.BootID = &workload.ReplayBudget{}, "successor-boot"
	if err := next.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	nextCtx, cancelNext := context.WithCancel(context.WithoutCancel(t.Context()))
	defer cancelNext()
	defer next.Close()
	if err := next.Start(nextCtx); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, DetachedGrace, time.Millisecond, func() bool {
		f.client.flowState.mu.Lock()
		defer f.client.flowState.mu.Unlock()
		e := f.client.flowState.endpoints[f.port]
		return e != nil && e.id != f.peer.id && e.attachment != nil && !e.attachment.Mux.IsClosed()
	}) {
		t.Fatal("ordinary supervisor did not replace the missing broker endpoint")
	}
	flowRequirePeerEOF(t, target)
	f.client.mu.Lock()
	same := f.client.sessions[f.port] == originalSession
	f.client.mu.Unlock()
	if !same {
		t.Fatal("broker restart replaced the allocation's supervisor")
	}
	if f.client.flowState.cfg.Budget.Used() != 0 {
		t.Fatal("lost business flow retained its replay reservation")
	}
	f.server = next
	next.mu.Lock()
	f.endpoint = next.flowState.endpoints[f.port]
	next.mu.Unlock()
	newPublic, newTarget, _ := settlementConnection(t, f, 1)
	if _, err := newPublic.Write([]byte("after-restart")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("after-restart"))
	if _, err := io.ReadFull(newTarget, got); err != nil || string(got) != "after-restart" {
		t.Fatalf("new request did not reach the original target listener: %q %v", got, err)
	}
	// Cancellation must join a failed carrier as well as a healthy one.
	next.Close()
	cancel()
	if !testharness.WaitFor(t, ResumeAttempt, time.Millisecond, func() bool {
		_, _, err := flow.Observe(f.client.flowState.manager, flow.ID{})
		return f.client.SessionCount() == 0 && errors.Is(err, flow.ErrClosed)
	}) {
		t.Fatal("cancelled supervisor kept a failed carrier or flow owner alive")
	}
}

func flowRequirePeerEOF(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(ResumeAttempt))
	n, err := conn.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("lost broker socket did not close: n=%d err=%v", n, err)
	}
}

// Missing-owner receipts cannot bypass authority, steal a live endpoint, or
// authorize an ACTIVATE whose permit vanished with the broker.
// origin: hot-update S5 plan §4.3 / §4.4 / §5.4 / P13-REGRESS
func TestMissingFlowOwnerStillRequiresAuthorityAndSupervisor(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("missing", func(t *testing.T) { exerciseMissingFlowOwner(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "missing flow owner authority x5", beforeG)
	testharness.AssertNoFDLeak(t, "missing flow owner authority x5", beforeFD, 0)
}

func exerciseMissingFlowOwner(t *testing.T) {
	t.Helper()
	exerciseMissingOwnerReceipt(t)
	f := endpointSetup(t)
	defer f.s.Close()
	req := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeReconnect, RequestID: [16]byte{1}, Endpoint: flow.EndpointID{1}, Owner: 1, Target: 22, Ticket: []byte("old-ticket")}
	if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(ResumeAttempt), nil); !errors.Is(err, &flow.Failure{Reason: flow.ReasonUnknownFlow}) {
		t.Fatalf("missing broker owner was not reported: %v", err)
	}
	if f.e.ownerGen != 0 || f.e.attachment != nil || f.e.receipt != nil {
		t.Fatal("missing-owner refusal installed ownership")
	}
	missingID := req.Endpoint
	req.Endpoint = flow.EndpointID{}
	if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(ResumeAttempt), nil); !errors.Is(err, errFlowAuth) {
		t.Fatal("an empty endpoint ID authorized restart retirement")
	}
	req.Endpoint = missingID
	f.allowed.Store(false)
	if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(ResumeAttempt), nil); !errors.Is(err, errFlowAuth) {
		t.Fatal("missing owner bypassed current authority")
	}
	f.allowed.Store(true)
	req.Mode = flow.ResumeModeActivate
	if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(ResumeAttempt), nil); !errors.Is(err, errFlowAuth) {
		t.Fatal("lost activation permit became a fresh initial owner")
	}
	req.Mode = flow.ResumeModeReconnect
	f.e.ownerGen = 1
	if _, _, err := resumeEndpoint(f.s, f.e, req, nil, time.Now().Add(ResumeAttempt), nil); !errors.Is(err, errFlowAuth) {
		t.Fatal("a stale ticket treated a live endpoint as missing")
	}

	c := NewClient("127.0.0.1:1", "lab", "node", nil, silentLog())
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "missing-owner", BootID: "boot", OwnerGen: 1}
	if err := c.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.Start(ctx)
	e := &clientEndpoint{id: flow.EndpointID{1}, stop: make(chan struct{}), request: req}
	c.flowState.endpoints[1] = e
	if retireLostClientEndpoint(c, 1, 1) || c.flowState.endpoints[1] != e {
		t.Fatal("explicit image recovery discarded its missing owner")
	}
	c.mu.Lock()
	c.sessions[1] = &clientSession{gen: 1}
	c.mu.Unlock()
	// This state-only slot has no transport. Remove it even after a mutant
	// trips an assertion, before lifetime cleanup visits real transport owners.
	defer func() {
		c.mu.Lock()
		delete(c.sessions, 1)
		c.mu.Unlock()
		c.Close(1)
	}()
	lifetime := c.ctx
	// Isolate the cancelled-lifetime guard from cleanup deleting this test slot;
	// the TLS exercise separately proves real lifetime cancellation and joining.
	stoppedLifetime, stop := context.WithCancel(lifetime)
	stop()
	for _, which := range []string{"superseded", "frozen", "permit", "activate", "cancelled"} {
		gen := uint64(1)
		c.mu.Lock()
		c.flowState.mu.Lock()
		switch which {
		case "superseded":
			gen = 2
		case "frozen":
			c.flowState.frozen = true
		case "permit":
			e.permit = &flowPermit{owner: 1, next: 2}
		case "activate":
			e.request.Mode = flow.ResumeModeActivate
		case "cancelled":
			c.ctx = stoppedLifetime
		}
		c.flowState.mu.Unlock()
		c.mu.Unlock()
		if retireLostClientEndpoint(c, 1, gen) {
			t.Fatalf("%s owner was abandoned by a restart retry", which)
		}
		c.mu.Lock()
		c.flowState.mu.Lock()
		c.flowState.frozen, e.permit, e.request.Mode = false, nil, flow.ResumeModeReconnect
		c.ctx = lifetime
		c.flowState.mu.Unlock()
		c.mu.Unlock()
	}
	c.mu.Lock()
	delete(c.sessions, 1)
	c.mu.Unlock()
	c.Close(1)
	cancel()
}

func exerciseMissingOwnerReceipt(t *testing.T) {
	t.Helper()
	for _, which := range []string{"valid", "request", "flow", "direction", "carrier"} {
		c := NewClient("127.0.0.1:1", "lab", "node", func(int) (int, error) { return 22, nil }, silentLog())
		if err := c.SetFlowConfig(FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "receipt", BootID: "boot", OwnerGen: 1}); err != nil {
			t.Fatal(err)
		}
		left, right := net.Pipe()
		client, err := yamux.Client(left, flowMuxConfig(io.Discard))
		if err != nil {
			t.Fatal(err)
		}
		server, err := yamux.Server(right, flowMuxConfig(io.Discard))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			stream, err := server.AcceptStream()
			if err != nil {
				done <- err
				return
			}
			frame, err := flow.DecodeFrame(stream)
			if err != nil {
				done <- err
				return
			}
			req, err := decodeResumeRequest(frame.Body)
			if err != nil {
				done <- err
				return
			}
			reply := flow.Frame{Version: flow.Version, Kind: flow.KindReject}
			switch which {
			case "request":
				req.RequestID[0] ^= 1
			case "flow":
				reply.FlowID[0] = 1
			case "direction":
				reply.Flags = flow.FlagDirection
			case "carrier":
				reply.CarrierGen = 1
			}
			reply.Body = binary.BigEndian.AppendUint16(append([]byte(nil), req.RequestID[:]...), uint16(flow.ReasonUnknownFlow))
			reply.BodyLen = uint32(len(reply.Body))
			done <- flow.EncodeFrame(stream, reply)
		}()
		err = attachClientFlow(c, left, client, 1, "token", "", 0, proto.CertPins{}, 0)
		// Receiving the frame can precede yamux's writer completion. Join the
		// peer before fixture teardown, or a valid receipt races session shutdown.
		writerErr := <-done
		_ = left.Close()
		_ = right.Close()
		_ = client.Close()
		_ = server.Close()
		_ = c.flowState.manager.Close()
		if writerErr != nil {
			t.Fatal(writerErr)
		}
		want := error(errFlowState)
		if which == "valid" {
			want = &flow.Failure{Reason: flow.ReasonUnknownFlow}
		}
		if !errors.Is(err, want) {
			t.Fatalf("%s missing-owner receipt accepted incorrectly: %v", which, err)
		}
	}
}
