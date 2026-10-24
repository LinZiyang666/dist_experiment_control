//go:build linux

package flowcarrier

import (
	"crypto/tls"
	"io"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/tunnel/transport"
	"github.com/hashicorp/yamux"
)

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

// Real TLS/yamux, a fast stream before congestion, an unread slow stream,
// minimum 256 KiB windows and all 64 shared writer slots occupied.
// origin: hot-update S5 plan §3.5 / T11
func TestYamuxSharedWriterCancellationBounds(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSharedWriterCancellation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "shared writer cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "shared writer cancellation", beforeFD, 0)
}
func exerciseSharedWriterCancellation(t *testing.T) {
	t.Helper()
	exerciseSlowFlowCreditIsolation(t)
	exerciseControlQueueBudget(t)
	left, right := net.Pipe()
	gate := &pausedReadConn{Conn: right, closed: make(chan struct{})}
	defer func(closeFunc func() error) { _ = closeFunc() }(left.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(gate.Close)
	cert, err := transport.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	client := tls.Client(left, transport.ClientTLSConfig())
	server := tls.Server(gate, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	handshake := make(chan error, 1)
	go func() { handshake <- server.Handshake() }()
	if err = client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err = <-handshake; err != nil {
		t.Fatal(err)
	}
	cfg := FlowMuxConfig(io.Discard)
	if cfg.ConnectionWriteTimeout != flowstate.CarrierWriteBudget {
		t.Fatal("carrier inherited default 10s writer timeout")
	}
	if cfg.MaxStreamWindowSize != 256<<10 {
		t.Fatal("non-minimum window precondition")
	}
	sender, err := yamux.Client(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := yamux.Server(server, FlowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(sender.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(receiver.Close)
	fast, err := sender.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	fastPeer, err := receiver.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	transfer := make(chan error, 1)
	go func() { _, e := fast.Write([]byte("fast")); transfer <- e }()
	buf := make([]byte, 4)
	if _, err = io.ReadFull(fastPeer, buf); err != nil || string(buf) != "fast" {
		t.Fatalf("fast flow baseline: %q %v", buf, err)
	}
	if err = <-transfer; err != nil {
		t.Fatal(err)
	}
	slow, err := sender.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	slowPeer, err := receiver.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(slowPeer.Close)
	a := NewFlowCarrier(client, sender, fast, nil, flow.EndpointID{1}, 1, 1, 0)
	gate.paused.Store(true)
	a.Launch(func() { _, _ = slow.Write(make([]byte, 1<<20)) })
	// GoAway uses waitForSend, the same shared queue as window updates. These
	// workers deliberately occupy it independently of the application codec.
	for range 80 {
		a.Launch(func() { _ = sender.GoAway() })
	}
	// origin: hot-update S5 plan §3.5 / T11
	a.Launch(func() {
		<-a.Stop
		a.Mu.Lock()
		closed := a.Closed
		a.Mu.Unlock()
		if !closed {
			t.Error("carrier stop preceded its closed fence")
		}
	})
	queue := reflect.ValueOf(sender).Elem().FieldByName("sendCh")
	waitEndpoint(t, func() bool { return queue.Len() == 64 })
	start := time.Now()
	joined := make(chan struct{})
	go func() { a.Close(); a.Join(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(2 * flowstate.CarrierWriteBudget):
		_ = left.Close()
		_ = gate.Close()
		t.Fatal("shared writer did not join after transport cancellation")
	}
	if time.Since(start) > flowstate.CarrierWriteBudget {
		t.Fatal("fast flow recovery inherited shared writer delay")
	}
	_ = gate.Close()
	_ = fastPeer.Close()
	_ = receiver.Close()
	_ = sender.Close()
}

// origin: hot-update S5 plan §3.5 / T11 / DRILL-RUN. An unread DATA
// stream can exhaust its own credit while the shared TLS writer and control
// stream remain healthy. Its credit wait must not fence every other flow.
func exerciseSlowFlowCreditIsolation(t *testing.T) {
	t.Helper()
	left, right := net.Pipe()
	cert, err := transport.GenerateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	client := tls.Client(left, transport.ClientTLSConfig())
	server := tls.Server(right, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	handshake := make(chan error, 1)
	go func() { handshake <- server.Handshake() }()
	if err = client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err = <-handshake; err != nil {
		t.Fatal(err)
	}
	sender, err := yamux.Client(client, FlowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := yamux.Server(server, FlowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	control, err := sender.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	controlPeer, err := receiver.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	a := NewFlowCarrier(client, sender, control, nil, flow.EndpointID{1}, 1, 1, 0)
	defer func() { a.Close(); a.Join(); _ = receiver.Close(); _ = server.Close() }()
	slow, err := sender.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	slowPeer, err := receiver.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, (32<<10)-flow.HeaderBytes)
	frame := flow.Frame{Version: flow.Version, Kind: flow.KindData, FlowID: flow.ID{1}, CarrierGen: 1, BodyLen: uint32(len(body)), Body: body}
	for range 8 {
		if err = a.Write(slow, frame); err != nil {
			t.Fatal(err)
		}
	}
	blocked := make(chan error, 1)
	a.Launch(func() { blocked <- a.Write(slow, frame) })
	peerErr := make(chan error, 1)
	go func() {
		for {
			f, e := flow.DecodeFrame(controlPeer)
			if e != nil {
				peerErr <- e
				return
			}
			if e = flow.EncodeFrame(controlPeer, f); e != nil {
				peerErr <- e
				return
			}
		}
	}()
	defer func() { a.Close(); _ = receiver.Close(); <-peerErr }()
	until := time.Now().Add(flowstate.CarrierWriteBudget + flowstate.CarrierWriteBudget/4)
	for time.Now().Before(until) {
		select {
		case err = <-blocked:
			t.Fatalf("slow credit wait fenced healthy carrier: %v", err)
		default:
		}
		ping := flow.Frame{Version: flow.Version, Kind: flow.KindACK, FlowID: flow.ID{2}, CarrierGen: 1}
		if err = a.Write(control, ping); err != nil {
			t.Fatalf("healthy control lost to DATA credit wait: %v", err)
		}
		_ = control.SetReadDeadline(time.Now().Add(flowstate.CarrierWriteBudget))
		if _, err = flow.DecodeFrame(control); err != nil {
			t.Fatalf("healthy control receipt lost: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = slowPeer.SetReadDeadline(time.Now().Add(flowstate.CarrierWriteBudget))
	for range 9 {
		f, e := flow.DecodeFrame(slowPeer)
		if e != nil || f.FlowID != frame.FlowID || len(f.Body) != len(body) {
			t.Fatalf("original slow stream failed after credit release: %v", e)
		}
	}
	if err = <-blocked; err != nil {
		t.Fatal(err)
	}
}

// Use complete P1 RESUME bodies. The queue's resource guard is independent of
// the absent P2 codec, and closing a refused owner must return every byte.
// origin: hot-update S5 plan §3.4 / T11
func exerciseControlQueueBudget(t *testing.T) {
	frame := func(id byte) flow.Frame {
		r := flowstate.ResumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeReconnect, RequestID: [16]byte{id}, Endpoint: flow.EndpointID{1}, Owner: 1, Target: 22, Ticket: make([]byte, flow.MaxTicketBytes)}
		for i := range flow.MaxFlows {
			r.Entries = append(r.Entries, flow.State{ID: flow.ID{byte(i + 1)}, OpenSeq: uint64(i + 1), OpenStatus: 1})
		}
		body := flowstate.EncodeResumeRequest(r)
		return flow.Frame{Version: flow.Version, Kind: flow.KindResume, CarrierGen: 1, BodyLen: uint32(len(body)), Body: body}
	}
	a := NewFlowCarrier(nil, nil, nil, nil, flow.EndpointID{1}, 1, 1, 0)
	for i := 1; i <= 20; i++ {
		a.Enqueue(frame(byte(i)))
	}
	if !a.Closed || a.ControlBudget.Load() != 0 {
		a.Close()
		t.Fatal("endpoint control queue exceeded 64 KiB or leaked its reservation")
	}
	var budget atomic.Int64
	var owners []*Carrier
	defer func() {
		for _, owner := range owners {
			owner.Close()
		}
	}()
	refused := false
	for range 80 {
		owner := NewFlowCarrier(nil, nil, nil, nil, flow.EndpointID{1}, 1, 1, 0)
		owner.ControlBudget = &budget
		owners = append(owners, owner)
		for i := 1; i <= 15; i++ {
			owner.Enqueue(frame(byte(i)))
		}
		if owner.Closed {
			refused = true
		}
		if budget.Load() > 4<<20 {
			t.Fatal("node control queue exceeded 4 MiB")
		}
	}
	if !refused {
		t.Fatal("node control queue admitted beyond its byte cap")
	}
	for _, owner := range owners {
		owner.Close()
	}
	if budget.Load() != 0 {
		t.Fatal("closed carriers retained node control bytes")
	}
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
