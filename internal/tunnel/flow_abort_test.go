//go:build linux

package tunnel

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/yamux"
)

// origin: hot-update S5 internal review rv-flow-2
func TestAbortCloseKeepsDataStreamUntilTailReceipt(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseAbortStreamTail(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "abort stream tail", beforeG)
	testharness.AssertNoFDLeak(t, "abort stream tail", beforeFD, 0)
}

func exerciseAbortStreamTail(t *testing.T) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(listener.Close)
	raw, err := rawsocket.Connect(netip.MustParseAddrPort(listener.Addr().String()), time.Now().Add(ResumeAttempt), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(raw.Close)
	if err = raw.CompleteConnect(time.Now().Add(ResumeAttempt), nil); err != nil {
		t.Fatal(err)
	}
	peer, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	manager, err := flow.New(flow.Config{Budget: &workload.ReplayBudget{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(manager.Close)
	req := flow.OpenRequest{ID: flow.ID{1}, EndpointID: flow.EndpointID{2}, OpenSeq: 1, TargetPort: 22, OwnerGen: 1, CarrierGen: 1}
	req.Intent = flow.OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort)
	if _, err = flow.Open(manager, req, raw); err != nil {
		t.Fatal(err)
	}
	settlementState(t, manager, req.ID, func(s flow.State) bool { return s.OpenStatus == 2 })
	left, right := net.Pipe()
	defer func(closeFunc func() error) { _ = closeFunc() }(left.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(right.Close)
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS := tls.Client(left, clientTLSConfig())
	serverTLS := tls.Server(right, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	handshake := make(chan error, 1)
	go func() { handshake <- serverTLS.Handshake() }()
	if err = clientTLS.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err = <-handshake; err != nil {
		t.Fatal(err)
	}
	clientMux, err := yamux.Client(clientTLS, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(clientMux.Close)
	serverMux, err := yamux.Server(serverTLS, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(serverMux.Close)
	control, err := clientMux.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	controlPeer, err := serverMux.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	data, err := clientMux.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	dataPeer, err := serverMux.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	carrier := newFlowCarrier(clientTLS, clientMux, control, manager, req.EndpointID, 1, 1, 0)
	defer func() { carrier.Close(); carrier.Join() }()
	carrier.Mu.Lock()
	carrier.Bound[req.ID] = true
	carrier.Streams[req.ID] = data
	carrier.Mu.Unlock()
	carrier.Start()
	tail := []byte("retained source tail before abort")
	if _, err = peer.Write(tail); err != nil {
		t.Fatal(err)
	}
	settlementState(t, manager, req.ID, func(s flow.State) bool { return s.TxEnd == uint64(len(tail)) && s.TxBase == 0 })
	body := make([]byte, 35)
	body[0] = byte(flow.CloseModeReadAbort)
	binary.BigEndian.PutUint16(body[1:3], uint16(flow.ReasonIOFailure))
	binary.BigEndian.PutUint64(body[3:11], uint64(len(tail)))
	closeFrame := flow.Frame{Version: flow.Version, Kind: flow.KindClose, FlowID: req.ID, CarrierGen: 1, Flags: flow.FlagDirection, BodyLen: 35, Body: body}
	if err = flow.EncodeFrame(controlPeer, closeFrame); err != nil {
		t.Fatal(err)
	}
	state := settlementState(t, manager, req.ID, func(s flow.State) bool { return s.Phase == flow.FlowPhaseDraining && s.FINFlags&4 != 0 })
	if state.TxEnd != uint64(len(tail)) || state.TxBase != 0 {
		t.Fatal("abort dropped unreceipted source bytes")
	}
	// Observe the peer stream BEFORE scheduling DATA. A control CLOSE may
	// overtake DATA, but no EOF is allowed while its source tail is unreceipted.
	_ = dataPeer.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if n, err := dataPeer.Read(make([]byte, 1)); n != 0 || err == io.EOF || err == nil {
		t.Fatalf("abort half-closed unreceipted data stream: n=%d err=%v", n, err)
	}
	_ = dataPeer.SetReadDeadline(time.Now().Add(ResumeAttempt))
	frame, _, err := flow.NextData(manager, req.ID, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = carrier.Write(data, frame); err != nil {
		t.Fatalf("retained tail dispatch failed: %v", err)
	}
	got, err := flow.DecodeFrame(dataPeer)
	if err != nil || string(got.Body) != string(tail) {
		t.Fatalf("retained tail lost: %q %v", got.Body, err)
	}
	closeFrame.Flags |= flow.FlagCloseReceipt
	binary.BigEndian.PutUint64(body[19:27], uint64(len(tail)))
	if err = flow.EncodeFrame(controlPeer, closeFrame); err != nil {
		t.Fatal(err)
	}
	settlementState(t, manager, req.ID, func(s flow.State) bool {
		return s.Phase == flow.FlowPhaseTerminal && s.FinalApplied[0] == uint64(len(tail))
	})
	waitEndpoint(t, func() bool {
		carrier.Mu.Lock()
		defer carrier.Mu.Unlock()
		return len(carrier.Bound) == 0 && len(carrier.Streams) == 0
	})
}
