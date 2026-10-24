//go:build linux

package flow

import (
	"errors"
	"net"
	"net/netip"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// OPEN absent at the target, connector still in progress, and a lost connected
// receipt all retain one journal entry and exactly one target Accept.
// origin: hot-update S5 plan §3.2 / T04
func exerciseFlowOpenReconciliationKeepsOriginalDial(t *testing.T) {
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(ln.Close)
	var dials atomic.Int32
	entered, release := make(chan struct{}, 4), make(chan struct{})
	m, err := New(Config{Budget: &workload.ReplayBudget{}, Dial: func(_ ID, _ uint16, deadline time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		dials.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-stop:
			return nil, rawsocket.ErrBusy
		}
		return rawsocket.Connect(netip.MustParseAddrPort(ln.Addr().String()), deadline, stop, nil)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
	req := OpenRequest{ID: ID{3}, EndpointID: EndpointID{4}, OpenSeq: 1, TargetPort: uint16(ln.Addr().(*net.TCPAddr).Port), OwnerGen: 1, CarrierGen: 1, Direction: FlagDirection, Deadline: time.Now().Add(5 * time.Second)}
	req.Intent = OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort)
	if err = Bind(m, req.ID, req.EndpointID, req.Intent, 1, 1, 0); !errors.Is(err, &Failure{Reason: ReasonUnknownFlow}) {
		t.Fatalf("unknown RESUME created a flow: %v", err)
	}
	if dials.Load() != 0 {
		t.Fatal("RESUME dialed a target before OPEN")
	}
	first, err := Open(m, req, nil)
	if err != nil || first.OpenStatus != 1 {
		t.Fatalf("missing pending receipt: %+v %v", first, err)
	}
	<-entered
	duplicate, err := Open(m, req, nil)
	if err != nil || duplicate.OpenStatus != 1 || dials.Load() != 1 {
		t.Fatalf("connecting OPEN redialed: %+v %v", duplicate, err)
	}
	conflict := req
	conflict.ID = ID{9}
	conflict.Intent = OpenIntent(conflict.EndpointID, conflict.ID, conflict.OpenSeq, conflict.TargetPort)
	if _, err = Open(m, conflict, nil); !errors.Is(err, &Failure{Reason: ReasonIntentConflict}) {
		t.Fatalf("OpenSeq reused: %v", err)
	}
	close(release)
	_ = ln.SetDeadline(time.Now().Add(3 * time.Second))
	peer, err := ln.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	waitFlow(t, m, req.ID, func(s State) bool { return s.OpenStatus == 2 })
	r := m.state.records[req.ID]
	r.mu.Lock()
	original := r.raw.Fingerprint()
	r.mu.Unlock()
	if _, err = Open(m, req, nil); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatal("lost OPEN_ACK redialed target")
	}
	if err = Bind(m, req.ID, req.EndpointID, req.Intent, 1, 2, 0); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	after := r.raw.Fingerprint()
	r.mu.Unlock()
	if after != original {
		t.Fatal("bind replaced connecting OFD")
	}
	_ = ln.SetDeadline(time.Now().Add(25 * time.Millisecond))
	extra, err := ln.AcceptTCP()
	if err == nil {
		_ = extra.Close()
		t.Fatal("target Accept exceeded one")
	}
}

// origin: hot-update S5 plan §8 / TestFlowOpenReconciliationKeepsOriginalDial
func TestFlowOpenReconciliationKeepsOriginalDial(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowOpenReconciliationKeepsOriginalDial(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowOpenReconciliationKeepsOriginalDial", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowOpenReconciliationKeepsOriginalDial", beforeFD, 0)
}
