//go:build linux

package flow

import (
	"io"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.1 / §9.1 / DRILL-OBSERVER
func TestFlowObservationKeepsOriginalBindingAndCommittedOffsets(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowObservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "observed flow", g)
	testharness.AssertNoFDLeak(t, "observed flow", fd, 0)
}
func exerciseFlowObservation(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var events []drillobserve.Event
	prior := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { mu.Lock(); events = append(events, e); mu.Unlock() }
	defer func() { drillobserve.Hook = prior }()
	// origin: hot-update S5 plan §9.1 / DRILL-RUN. A raw FD must be
	// observed while its owner is Connecting, before CompleteConnect consumes
	// SO_ERROR. An Active-only receipt cannot identify a genuinely blocked OPEN.
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	connecting, err := New(Config{Budget: &workload.ReplayBudget{}, Dial: func(_ ID, _ uint16, deadline time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		return rawsocket.Connect(netip.MustParseAddrPort(ln.Addr().String()), deadline, stop, nil)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connecting.Close() }()
	connectingReq := OpenRequest{ID: ID{221}, EndpointID: EndpointID{222}, OpenSeq: 1, TargetPort: uint16(ln.Addr().(*net.TCPAddr).Port), OwnerGen: 1, CarrierGen: 1, Deadline: time.Now().Add(3 * time.Second)}
	connectingReq.Intent = OpenIntent(connectingReq.EndpointID, connectingReq.ID, connectingReq.OpenSeq, connectingReq.TargetPort)
	if _, err = Open(connecting, connectingReq, nil); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, connecting, connectingReq.ID, func(s State) bool { return s.OpenStatus == 2 })
	_ = ln.SetDeadline(time.Now().Add(time.Second))
	connectedPeer, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connectedPeer.Close() }()
	record := connecting.state.records[connectingReq.ID]
	record.mu.Lock()
	connectingFP := record.raw.Fingerprint()
	record.mu.Unlock()
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	body := []byte("original-business-bytes")
	receive(t, m, req, incoming(req, KindData, 0, body))
	b := make([]byte, len(body))
	if _, err := io.ReadFull(peer, b); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.RxApplied == uint64(len(body)) })
	if _, err := peer.Write(body); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == uint64(len(body)) })
	receive(t, m, req, incoming(req, KindACK, uint64(len(body)), nil))
	if err := m.RevokeFlow(req.ID, ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	_ = Retirement(m, req.EndpointID)
	mu.Lock()
	defer mu.Unlock()
	var original uint64
	applied, ack, closed, retired, pendingFD := false, false, false, false, false
	for _, e := range events {
		if e.Flow == [16]byte(connectingReq.ID) {
			if e.Kind == "flow-state" && e.Values[3] == uint64(FlowPhaseConnecting) {
				pendingFD = e.Endpoint == [16]byte(connectingReq.EndpointID) && e.Values[4] == 1 &&
					e.Values[5] == connectingFP.Dev && e.Values[6] == connectingFP.Ino && e.Values[7] == uint64(connectingFP.Type) && e.Values[14] == 1
			}
			continue
		}
		if e.Endpoint == [16]byte(connectingReq.EndpointID) {
			continue
		}
		if e.Kind == "flow-retirement" {
			retired = e.Endpoint == [16]byte(req.EndpointID) && e.Values[1] >= req.OpenSeq
			continue
		}
		if e.Kind != "flow-state" {
			continue
		}
		if e.Flow != [16]byte(req.ID) || e.Endpoint != [16]byte(req.EndpointID) {
			t.Fatal("foreign binding observed")
		}
		if e.Values[6] != 0 {
			if original == 0 {
				original = e.Values[6]
			}
			if e.Values[6] != original {
				t.Fatal("original FD changed")
			}
		}
		applied = applied || e.Values[10] == uint64(len(body))
		ack = ack || e.Values[8] == uint64(len(body)) && e.Values[12] == 8<<20
		closed = closed || e.Values[3] == uint64(FlowPhaseTerminal) && e.Values[12] == 0 && e.Values[14] == 0 && e.Values[15] == 0
	}
	if original == 0 || !applied || !ack || !closed || !retired {
		t.Fatal("missing original FD, committed write, ACK reservation, close or retirement receipt")
	}
	if !pendingFD {
		t.Fatal("missing original raw FD before connect completion")
	}
}
