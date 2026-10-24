//go:build linux

package flow

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

func flowFixture(t *testing.T) (*Manager, OpenRequest, *net.TCPConn, func()) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rawsocket.Connect(netip.MustParseAddrPort(ln.Addr().String()), time.Now().Add(3*time.Second), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = raw.CompleteConnect(time.Now().Add(3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	peer, err := ln.AcceptTCP()
	_ = ln.Close()
	if err != nil {
		t.Fatal(err)
	}
	_ = peer.SetDeadline(time.Now().Add(15 * time.Second))
	manager, err := New(Config{Budget: &workload.ReplayBudget{}})
	if err != nil {
		t.Fatal(err)
	}
	req := OpenRequest{ID: ID{1}, EndpointID: EndpointID{2}, OpenSeq: 1, TargetPort: 22, OwnerGen: 1, CarrierGen: 1}
	req.Intent = OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort)
	if _, err = Open(manager, req, raw); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, manager, req.ID, func(s State) bool { return s.OpenStatus == 2 })
	return manager, req, peer, func() { _ = manager.Close(); _ = peer.Close() }
}
func waitFlow(t *testing.T, m *Manager, id ID, accept func(State) bool) State {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		state, ch, err := Observe(m, id)
		if err != nil {
			t.Fatal(err)
		}
		if accept(state) {
			return state
		}
		select {
		case <-ch:
		case <-timer.C:
			t.Fatalf("flow did not reach expected state: %+v", state)
		}
	}
}
func incoming(req OpenRequest, kind Kind, offset uint64, body []byte) Frame {
	return Frame{Version: Version, Kind: kind, FlowID: req.ID, CarrierGen: req.CarrierGen, Flags: FlagDirection, Offset: offset, BodyLen: uint32(len(body)), Body: body}
}
func receive(t *testing.T, m *Manager, req OpenRequest, f Frame) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		_, ch, err := Observe(m, req.ID)
		if err != nil {
			t.Fatal(err)
		}
		err = Receive(m, f)
		if err == nil {
			return
		}
		if !errors.Is(err, workload.ErrBackpressure) {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-timer.C:
			t.Fatal("receive capacity did not recover")
		}
	}
}

// origin: hot-update S5 plan §3.1 / T02
func exerciseFlowACKUsesAppliedNotCarrierSent(t *testing.T) {
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	if _, err := peer.Write([]byte("previous carrier bytes")); err != nil {
		t.Fatal(err)
	}
	before := waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == 22 })
	r := m.state.records[req.ID]
	r.mu.Lock()
	sent := r.sent
	r.mu.Unlock()
	if sent != 0 || before.TxBase != 0 {
		t.Fatal("missing Sent=0/unacknowledged precondition")
	}
	if err := Bind(m, req.ID, req.EndpointID, req.Intent, 1, 2, 11); err != nil {
		t.Fatalf("peer applied from previous carrier rejected: %v", err)
	}
	got, _, _ := Observe(m, req.ID)
	if got.TxBase != 11 || got.TxEnd != 22 {
		t.Fatalf("ACK used carrier delivery: %+v", got)
	}
	_, notification, _ := Observe(m, req.ID)
	duplicate := incoming(req, KindACK, 11, nil)
	duplicate.CarrierGen = 2
	if err := Receive(m, duplicate); err != nil {
		t.Fatal(err)
	}
	_, afterNotification, _ := Observe(m, req.ID)
	if afterNotification != notification {
		t.Fatal("duplicate ACK created a control feedback loop")
	}
	if m.state.cfg.Budget.Used() != workload.DefaultReplayCapacity {
		t.Fatal("ACK released reserved capacity")
	}
	if err := Bind(m, req.ID, req.EndpointID, req.Intent, 1, 3, 10); !errors.Is(err, &Failure{Reason: ReasonOffsetExpired}) {
		t.Fatalf("expired offset accepted: %v", err)
	}
	if err := Bind(m, req.ID, req.EndpointID, req.Intent, 1, 3, 23); err == nil {
		t.Fatal("future applied accepted")
	}
}

func pressureFlow(t *testing.T, m *Manager, req OpenRequest, peer *net.TCPConn) (uint64, State) {
	t.Helper()
	r := m.state.records[req.ID]
	r.mu.Lock()
	raw := r.raw
	r.mu.Unlock()
	if err := rawsocket.Hold(raw); err != nil && !errors.Is(err, rawsocket.ErrBusy) {
		t.Fatal(err)
	}
	// A pollable duplicate configures only the test's send-buffer size; no
	// descriptor flags or production ownership are changed.
	if err := rawsocket.Release(raw); err != nil {
		t.Fatal(err)
	}
	frozen, err := m.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	file, err := rawsocket.BorrowHeldFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err = rc.Control(func(fd uintptr) {
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); e != nil {
			t.Error(e)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadBuffer(4096)
	if err = frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	var end uint64
	payload := bytes.Repeat([]byte{0x6b}, MaxDataBytes)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		state, ch, err := Observe(m, req.ID)
		if err != nil {
			t.Fatal(err)
		}
		if state.RxApplied > 0 && state.RxApplied < state.RxReceived && state.RxApplied%MaxDataBytes != 0 {
			return end, state
		}
		err = Receive(m, incoming(req, KindData, end, payload))
		if err == nil {
			end += uint64(len(payload))
			continue
		}
		if !errors.Is(err, workload.ErrBackpressure) {
			t.Fatal(err)
		}
		select {
		case <-ch:
		case <-timer.C:
			t.Fatalf("missing positive partial write A<R: %+v", state)
		}
	}
}

// origin: hot-update S5 plan §3.1 / T03
func exerciseFlowDedupeBeforePartialWrite(t *testing.T) {
	exerciseAppliedDuplicate(t)
	// Distinct new bytes expose trimming defects that uniform pending data hides.
	// origin: hot-update S5 internal review rv-flow-4
	exerciseAppliedOverlapSuffix(t)
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	end, partial := pressureFlow(t, m, req, peer)
	if partial.RxApplied == 0 || partial.RxApplied >= partial.RxReceived {
		t.Fatal("missing real partial syscall precondition")
	}
	r := m.state.records[req.ID]
	r.mu.Lock()
	r.desired = false
	stopPumpsLocked(r)
	done := r.done
	r.mu.Unlock()
	<-done
	r.mu.Lock()
	if r.rxReceived-r.rxApplied != uint64(len(r.rxPending)) {
		r.mu.Unlock()
		t.Fatal("partial write dropped pending suffix")
	}
	empty := len(r.rxPending) == 0
	partial.RxApplied = r.rxApplied
	r.mu.Unlock()
	if empty {
		receive(t, m, req, incoming(req, KindData, end, bytes.Repeat([]byte{0x6b}, MaxDataBytes)))
		end += MaxDataBytes
	}
	// A-1 is a legal duplicate, including an overlap with retained pending.
	if err := Receive(m, incoming(req, KindData, partial.RxApplied-1, []byte{0x6b, 0x6b})); err != nil {
		t.Fatalf("A-1 duplicate rejected: %v", err)
	}
	if err := Receive(m, incoming(req, KindData, end+1, []byte{0x6b})); !errors.Is(err, &Failure{Reason: ReasonOffsetGap}) {
		t.Fatalf("gap accepted: %v", err)
	}
	r.mu.Lock()
	a := r.rxApplied
	pending := len(r.rxPending)
	r.mu.Unlock()
	if pending == 0 {
		t.Fatal("pending suffix disappeared before conflict check")
	}
	if err := Receive(m, incoming(req, KindData, a, []byte{0x00})); !errors.Is(err, &Failure{Reason: ReasonIntentConflict}) {
		t.Fatalf("pending conflict accepted: %v", err)
	}
	r.mu.Lock()
	r.desired = true
	startPumpsLocked(r)
	r.mu.Unlock()
	got := make([]byte, end)
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.RxApplied == end })
	if !bytes.Equal(got, bytes.Repeat([]byte{0x6b}, int(end))) {
		t.Fatal("pending suffix corrupted")
	}
	_ = peer.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	one := make([]byte, 1)
	if n, err := peer.Read(one); n != 0 || err == nil {
		t.Fatal("dedupe wrote an already-applied byte")
	}
}

// Real forward EOF and a reverse tail larger than the one-log reservation.
// origin: hot-update S5 plan §3.3 / T06
func exerciseFlowFINCloseWritePreservesReverseTail(t *testing.T) {
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	forward := []byte("FIN must wait for these bytes")
	if err := Receive(m, incoming(req, KindFIN, uint64(len(forward)), nil)); err != nil {
		t.Fatal(err)
	}
	controls, _, _ := Controls(m, req.ID, 1)
	if controls[0].Flags&FlagFINReceipt != 0 {
		t.Fatal("FIN receipt preceded raw CloseWrite")
	}
	receive(t, m, req, incoming(req, KindData, 0, forward))
	got := make([]byte, len(forward))
	if _, err := io.ReadFull(peer, got); err != nil || !bytes.Equal(got, forward) {
		t.Fatalf("forward tail: %q %v", got, err)
	}
	if n, err := peer.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("missing TCP half-close: %d %v", n, err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.FINFlags&4 != 0 })
	tail := bytes.Repeat([]byte("reverse!"), (9<<20)/8)
	want := sha256.Sum256(tail)
	written := make(chan error, 1)
	go func() {
		_, err := peer.Write(tail)
		if err == nil {
			err = peer.CloseWrite()
		}
		written <- err
	}()
	hash := sha256.New()
	offset := uint64(0)
	for offset < uint64(len(tail)) {
		frame, ch, err := NextData(m, req.ID, 1, offset)
		if err != nil {
			t.Fatal(err)
		}
		if len(frame.Body) == 0 {
			select {
			case <-ch:
			case <-time.After(10 * time.Second):
				t.Fatal("reverse tail stopped")
			}
			continue
		}
		_, _ = hash.Write(frame.Body)
		offset += uint64(len(frame.Body))
		receive(t, m, req, incoming(req, KindACK, offset, nil))
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	final := waitFlow(t, m, req.ID, func(s State) bool { return s.FINFlags&1 != 0 })
	if final.TxFIN != offset || !bytes.Equal(hash.Sum(nil), want[:]) {
		t.Fatal("half-close lost reverse tail or forged final end")
	}
	receipt := incoming(req, KindACK, offset, nil)
	receipt.Flags |= FlagFINReceipt
	if err := Receive(m, receipt); err != nil {
		t.Fatal(err)
	}
	settled := waitFlow(t, m, req.ID, func(s State) bool { return s.Phase == FlowPhaseTerminal })
	if m.state.cfg.Budget.Used() != 0 || settled.FinalEnd != settled.FinalApplied {
		t.Fatal("normal FIN settlement retained capacity or lost tail")
	}
	controls, _, _ = Controls(m, req.ID, 1)
	var terminal Frame
	for _, f := range controls {
		if f.Kind == KindClose {
			terminal = f
		}
	}
	terminal.Flags ^= FlagDirection
	terminal.Flags |= FlagCloseReceipt
	if err := Receive(m, terminal); err != nil {
		t.Fatalf("valid terminal receipt: %v", err)
	}
	for _, index := range []int{0, 2, 10, 26} {
		forged := terminal
		forged.Body = append([]byte(nil), terminal.Body...)
		forged.Body[index] ^= 0xff
		if err := Receive(m, forged); err == nil {
			t.Fatal("terminal receipt accepted malformed or conflicting CLOSE")
		}
	}

}

// The commit path is shared with the real syscall pump; the adversarial n+err
// pair tests an allowed io.Reader result that the kernel need not co-produce.
// origin: hot-update S5 plan §3.3 / T07
func exerciseFlowAbortKeepsFinalTailReceipt(t *testing.T) {
	exercisePeerRevoke(t)
	exerciseAbortDoesNotForgeFINReceipt(t)
	exerciseAbortReceiptSettlement(t)
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	frozen, err := m.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r := m.state.records[req.ID]
	r.mu.Lock()
	recordSourceResult(r, []byte("last prefix"), 4, io.ErrUnexpectedEOF)
	recordSourceResult(r, nil, 0, io.EOF)
	if r.txError != "io_failure" {
		r.mu.Unlock()
		t.Fatal("late EOF erased an already committed abort")
	}
	r.rxApplied = 2
	r.rxReceived = 7
	r.rxPending = []byte("12345")
	r.writerError = "io_failure"
	r.mu.Unlock()
	if err = frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	state := waitFlow(t, m, req.ID, func(s State) bool { return s.Reason == ReasonIOFailure })
	if state.TxEnd != 4 || state.FinalEnd[0] != 4 || state.FinalApplied[1] != 2 || state.FinalEnd[1] != 7 {
		t.Fatalf("abort lost n or forged delivery: %+v", state)
	}
	r.mu.Lock()
	before := r.replay.Snapshot()
	r.mu.Unlock()
	if before.FIN {
		t.Fatal("source error became FIN")
	}
	if _, err = peer.Write([]byte("after published abort")); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	after, _, err := Observe(m, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.TxEnd != state.TxEnd || after.FinalEnd != state.FinalEnd {
		t.Fatal("published abort End grew")
	}
	frames, _, err := Controls(m, req.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if frames[len(frames)-1].Kind != KindClose {
		t.Fatal("missing abort receipt")
	}
}

// origin: hot-update S5 plan §8 / T51
func TestFlowFiveRoundLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "flow lifecycle", beforeFD, 0)
}
func exerciseFlowLifecycle(t *testing.T) {
	t.Helper()
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	if _, err := peer.Write([]byte("source")); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, m, req.ID, func(s State) bool { return s.TxEnd == 6 })
	f, err := m.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Resources) != 1 || len(cut.Resources[0].Files) != 2 {
		t.Fatal("cut lost business or replay descriptor")
	}
	if err = f.Thaw(); err != nil {
		t.Fatal(err)
	}
	if err = m.RevokeFlow(req.ID, ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if m.state.cfg.Budget.Used() != 0 {
		t.Fatal("revoke leaked replay reservation")
	}
}

// origin: hot-update S5 plan §8 / TestFlowACKUsesAppliedNotCarrierSent
func TestFlowACKUsesAppliedNotCarrierSent(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowACKUsesAppliedNotCarrierSent(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowACKUsesAppliedNotCarrierSent", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowACKUsesAppliedNotCarrierSent", beforeFD, 0)
}

// origin: hot-update S5 plan §8 / TestFlowDedupeBeforePartialWrite
func TestFlowDedupeBeforePartialWrite(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowDedupeBeforePartialWrite(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowDedupeBeforePartialWrite", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowDedupeBeforePartialWrite", beforeFD, 0)
}

// origin: hot-update S5 plan §8 / TestFlowFINCloseWritePreservesReverseTail
func TestFlowFINCloseWritePreservesReverseTail(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowFINCloseWritePreservesReverseTail(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowFINCloseWritePreservesReverseTail", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowFINCloseWritePreservesReverseTail", beforeFD, 0)
}

// origin: hot-update S5 plan §8 / TestFlowAbortKeepsFinalTailReceipt
func TestFlowAbortKeepsFinalTailReceipt(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowAbortKeepsFinalTailReceipt(t)
	}
	testharness.AssertNoGoroutineLeak(t, "TestFlowAbortKeepsFinalTailReceipt", beforeG)
	testharness.AssertNoFDLeak(t, "TestFlowAbortKeepsFinalTailReceipt", beforeFD, 0)
}

func exerciseAppliedDuplicate(t *testing.T) {
	t.Helper()
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	receive(t, m, req, incoming(req, KindData, 0, []byte("dedupe")))
	waitFlow(t, m, req.ID, func(s State) bool { return s.RxApplied == 6 })
	got := make([]byte, 6)
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatal(err)
	}
	receive(t, m, req, incoming(req, KindData, 5, []byte("e")))
	_ = peer.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("applied prefix was written before deduplication")
	}
}

// A peer's explicit revoke reaches the original OFD, including inherited
// duplicates; retaining only a CLOSE receipt must not leave the business live.
// origin: hot-update S5 plan §3.3 / §7 / T07
func exercisePeerRevoke(t *testing.T) {
	m, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	body := make([]byte, 35)
	body[0] = byte(CloseModeRevoke)
	binary.BigEndian.PutUint16(body[1:3], uint16(ReasonRevoked))
	if err := Receive(m, incoming(req, KindClose, 0, body)); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("peer revoke left original socket live")
	}
	state, _, err := Observe(m, req.ID)
	if err != nil || state.Phase != FlowPhaseTerminal || m.state.cfg.Budget.Used() != 0 {
		t.Fatal("peer revoke retained original flow resources")
	}
}

// Two actual socket owners exchange their final source ends independently of
// carrier ordering. Both retained tails survive until explicit control receipts.
// origin: hot-update S5 plan §3.3 / T07
func exerciseAbortReceiptSettlement(t *testing.T) {
	left, req, leftBusiness, cleanupLeft := flowFixture(t)
	defer cleanupLeft()
	right, _, rightBusiness, cleanupRight := flowFixture(t)
	defer cleanupRight()
	rr := right.state.records[req.ID]
	rr.mu.Lock()
	rr.open.Direction = FlagDirection
	rr.mu.Unlock()
	leftTail, rightTail := []byte("left final tail"), []byte("right final tail")
	if _, err := leftBusiness.Write(leftTail); err != nil {
		t.Fatal(err)
	}
	if _, err := rightBusiness.Write(rightTail); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, left, req.ID, func(s State) bool { return s.TxEnd == uint64(len(leftTail)) })
	waitFlow(t, right, req.ID, func(s State) bool { return s.TxEnd == uint64(len(rightTail)) })

	frozen, err := left.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lr := left.state.records[req.ID]
	lr.mu.Lock()
	recordSourceResult(lr, nil, 0, io.ErrUnexpectedEOF)
	lr.mu.Unlock()
	if err = frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	waitFlow(t, left, req.ID, func(s State) bool { return s.Reason == ReasonIOFailure })
	deadline := time.Now().Add(2 * time.Second)
	var offsets [2]uint64
	for time.Now().Before(deadline) {
		for direction, pair := range [][2]*Manager{{left, right}, {right, left}} {
			data, _, dataErr := NextData(pair[0], req.ID, 1, offsets[direction])
			if dataErr == nil && len(data.Body) > 0 {
				if err = Receive(pair[1], data); err != nil {
					t.Fatal(err)
				}
				offsets[direction] += uint64(len(data.Body))
			}
			controls, _, err := Controls(pair[0], req.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range controls {
				if err = Receive(pair[1], f); err != nil {
					t.Fatalf("abort exchange %d: %v", f.Kind, err)
				}
			}
		}
		l, _, _ := Observe(left, req.ID)
		r, _, _ := Observe(right, req.ID)
		if l.Phase == FlowPhaseTerminal && r.Phase == FlowPhaseTerminal {
			if left.state.cfg.Budget.Used() != 0 || right.state.cfg.Budget.Used() != 0 {
				t.Fatal("abort receipts retained replay capacity")
			}
			for _, check := range []struct {
				peer *net.TCPConn
				want []byte
			}{{leftBusiness, rightTail}, {rightBusiness, leftTail}} {
				got := make([]byte, len(check.want))
				if _, err = io.ReadFull(check.peer, got); err != nil || !bytes.Equal(got, check.want) {
					t.Fatalf("abort lost retained final tail: %v", err)
				}
			}
			if l.FinalEnd != l.FinalApplied || r.FinalEnd != r.FinalApplied {
				t.Fatal("drained abort reported a false partial discard")
			}
			return
		}
		timer := time.NewTimer(time.Millisecond)
		<-timer.C
	}
	t.Fatal("abort final receipts did not settle both original owners")
}

// CloseWrite can finish while the opposite source is still joining an abort.
// That interval is not a FIN receipt: the source never published a FIN.
// origin: hot-update S5 plan §3.3 / T07
func exerciseAbortDoesNotForgeFINReceipt(t *testing.T) {
	m, req, _, cleanup := flowFixture(t)
	defer cleanup()
	r := m.state.records[req.ID]
	r.mu.Lock()
	r.desired = false
	stopPumpsLocked(r)
	done := r.done
	r.mu.Unlock()
	<-done
	if err := r.raw.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.txError = "io_failure"
	r.sourceEnded = true
	r.rxCloseWrite = true
	r.rxFINSet = true
	r.mu.Unlock()
	frames, _, err := Controls(m, req.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if frames[0].Flags&FlagFINReceipt != 0 {
		t.Fatal("joining abort emitted a FIN receipt without source FIN")
	}
}

// origin: hot-update S6 plan §6.2 / T23 / T56; hot-update S5 DRILL-OBSERVER
func TestFlowCloseJoinsRetirementCallback(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowCloseRetirementJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow retirement callback join", g)
	testharness.AssertNoFDLeak(t, "flow retirement callback join", fd, 0)
}
func exerciseFlowCloseRetirementJoin(t *testing.T) {
	t.Helper()
	m, req, _, cleanup := flowFixture(t)
	defer cleanup()
	entered, release, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := m.state.records[req.ID]
	r.mu.Lock()
	previous := r.settle
	r.settle = func(join bool) { close(entered); <-release; previous(join); close(settled) }
	r.mu.Unlock()
	result := make(chan error, 1)
	go func() { result <- m.Close() }()
	released, joined := false, false
	defer func() {
		if !released {
			close(release)
		}
		if !joined {
			<-result
		}
		<-settled
	}()
	<-entered
	select {
	case err := <-result:
		joined = true
		t.Fatal("Close returned before retirement callback joined", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	released = true
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	joined = true
}
