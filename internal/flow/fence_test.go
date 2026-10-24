//go:build linux

package flow

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.5 / T10
func exerciseFlowFreezeJoinsWithoutClosingBusiness(t *testing.T) {
	exerciseFreezeConnectorTimeout(t)
	exerciseFreezeRejectsUnownedResource(t)
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	if _, err := peer.Write([]byte("inbound source")); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == 14 })
	r := m.state.records[req.ID]
	r.mu.Lock()
	fingerprint := r.raw.Fingerprint()
	oldDone := r.done
	joinGate := make(chan struct{})
	r.done = joinGate
	r.mu.Unlock()
	var release sync.Once
	defer release.Do(func() { close(joinGate) })
	type freezeOutcome struct {
		f   *Frozen
		err error
	}
	resultCh := make(chan freezeOutcome, 1)
	go func() { f, e := m.Freeze(time.Now().Add(time.Second)); resultCh <- freezeOutcome{f, e} }()
	<-oldDone
	select {
	case <-resultCh:
		t.Fatal("freeze omitted worker join receipt")
	case <-time.After(5 * time.Millisecond):
	}
	release.Do(func() { close(joinGate) })
	outcome := <-resultCh
	frozen, err := outcome.f, outcome.err
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldDone:
	default:
		t.Fatal("freeze returned before both raw workers joined")
	}
	r.mu.Lock()
	if r.running {
		t.Fatal("frozen raw worker still active")
	}
	r.mu.Unlock()
	if err = frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	receive(t, m, req, incoming(req, KindData, 0, []byte("still same TCP")))
	got := make([]byte, 14)
	if _, err = io.ReadFull(peer, got); err != nil || string(got) != "still same TCP" {
		t.Fatalf("freeze closed business socket: %q %v", got, err)
	}
	if r.raw.Fingerprint() != fingerprint {
		t.Fatal("freeze replaced the original socket")
	}
	m.state.mu.Lock()
	deadline := time.Now().Add(15 * time.Millisecond)
	result := make(chan error, 1)
	go func() { _, e := m.Freeze(deadline); result <- e }()
	select {
	case err = <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("lock wait escaped entry deadline: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		m.state.mu.Unlock()
		t.Fatal("freeze ignores lock wait budget")
	}
	m.state.mu.Unlock()
	if _, err = peer.Write([]byte("after timeout")); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == 27 })
	r.mu.Lock()
	running := r.running
	r.mu.Unlock()
	if !running {
		t.Fatal("failed freeze stopped original pumps")
	}
}

// origin: hot-update S5 plan §8 / TestFlowFreezeJoinsWithoutClosingBusiness
func TestFlowFreezeJoinsWithoutClosingBusiness(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowFreezeJoinsWithoutClosingBusiness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowFreezeJoinsWithoutClosingBusiness", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowFreezeJoinsWithoutClosingBusiness", beforeFD, 0)
}

func exerciseFreezeConnectorTimeout(t *testing.T) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(ln.Close)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var dials atomic.Int32
	m, err := New(Config{Budget: &workload.ReplayBudget{}, Dial: func(_ ID, _ uint16, deadline time.Time, _ <-chan struct{}) (*rawsocket.Socket, error) {
		dials.Add(1)
		entered <- struct{}{}
		<-release
		return rawsocket.Connect(netip.MustParseAddrPort(ln.Addr().String()), deadline, nil, nil)
	}})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
	defer once.Do(func() { close(release) })
	req := OpenRequest{ID: ID{5}, EndpointID: EndpointID{6}, OpenSeq: 1, TargetPort: 22, OwnerGen: 1, CarrierGen: 1, Deadline: time.Now().Add(time.Second)}
	req.Intent = OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort)
	if _, err = Open(m, req, nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err = m.Freeze(time.Now().Add(15 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("connector freeze did not time out: %v", err)
	}
	if dials.Load() != 1 {
		t.Fatal("timed-out cut started a second connector")
	}
	once.Do(func() { close(release) })
	waitFlow(t, m, req.ID, func(s State) bool { return s.OpenStatus == 2 })
	if dials.Load() != 1 {
		t.Fatal("thaw redialed original connector")
	}
}

// A refusal returns all files and no capacity, including files with no record.
// origin: hot-update S5 plan §5.3 / T10
func exerciseFreezeRejectsUnownedResource(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "unclaimed-replay")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(file.Close)
	budget := &workload.ReplayBudget{}
	cut := sealedstate.CutState{Resources: []handoff.Resource{{ID: "unclaimed", Kind: "flow-v1", Files: map[string]*os.File{"replay": file}}}}
	manager, salvage, err := RestoreSnapshot(Config{Budget: budget}, cut)
	if err == nil || manager != nil || len(salvage) != 1 || salvage[0].Files["replay"] != file || budget.Used() != 0 {
		t.Fatal("import lost unclaimed resource or granted ownership")
	}
	if _, err = file.Stat(); err != nil {
		t.Fatal("failed import closed borrowed resource")
	}
}
