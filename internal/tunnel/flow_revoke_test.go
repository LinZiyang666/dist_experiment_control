//go:build linux

package tunnel

import (
	"bytes"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review SS-REVOKE-1
func TestHardRevokeClosesPublicFlowWithoutTailReceipt(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseHardRevokePublicFlow(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "hard revoke public flow", g0)
	testharness.AssertNoFDLeak(t, "hard revoke public flow", fd0, 0)
}

func exerciseHardRevokePublicFlow(t *testing.T) {
	t.Helper()
	t.Run("queued ACK", exerciseQueuedRevokeACK)
	t.Run("sent CLOSE", exerciseSentRevokeClose)
	t.Run("early DATA EOF", exerciseDataEOFBeforeControl)
	t.Run("finished DATA EOF", exerciseFinishedDataEOF)
	f := settlementSetup(t)
	public, target, id := settlementConnection(t, f, 1)
	survivor, survivorTarget, _ := settlementConnection(t, f, 2)
	for _, direction := range [][2]io.ReadWriter{{public, target}, {target, public}} {
		if _, err := direction[0].Write([]byte("duplex")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 6)
		if _, err := io.ReadFull(direction[1], got); err != nil || string(got) != "duplex" {
			t.Fatal("duplex prerequisite failed", err)
		}
	}
	if f.server.flowState.cfg.Budget.Used() == 0 || f.client.flowState.cfg.Budget.Used() == 0 {
		t.Fatal("both owners must retain replay before revoke")
	}
	f.server.mu.Lock()
	brokerCarrier := f.endpoint.attachment
	f.server.mu.Unlock()
	// A withheld control reply cannot retain the sender's DATA stream after
	// the broker has applied revoke and closed its direction. The public/raw
	// retirement and reverse EOF remain real TLS/yamux decisions.
	brokerCarrier.ControlWriteMu.Lock()
	releasedWriter := false
	defer func() {
		if !releasedWriter {
			brokerCarrier.ControlWriteMu.Unlock()
		}
	}()
	if err := f.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	// Neither application peer sends FIN or closes. Authorization withdrawal
	// must discard the tail and release the broker's original public socket.
	settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool {
		return s.Phase == flow.FlowPhaseTerminal && s.Reason == flow.ReasonRevoked
	})
	waitEndpoint(t, func() bool {
		return f.server.flowState.cfg.Budget.Used() == workload.DefaultReplayCapacity && f.client.flowState.cfg.Budget.Used() == workload.DefaultReplayCapacity
	})
	f.client.flowState.mu.Lock()
	carrier := f.peer.attachment
	f.client.flowState.mu.Unlock()
	carrier.Mu.Lock()
	closed := carrier.Closed
	carrier.Mu.Unlock()
	if closed {
		t.Fatal("victim revoke interrupted the survivor carrier")
	}
	_ = public.SetReadDeadline(time.Now().Add(ResumeAttempt))
	var b [1]byte
	if n, err := public.Read(b[:]); n != 0 || err == nil {
		t.Fatal("hard revoke left the public victim writable")
	} else if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		t.Fatal("public victim needed fixture close")
	}
	waitEndpoint(t, func() bool {
		carrier.Mu.Lock()
		defer carrier.Mu.Unlock()
		return !carrier.Bound[id] && carrier.Streams[id] == nil
	})
	releasedWriter = true
	brokerCarrier.ControlWriteMu.Unlock()
	for _, direction := range [][2]io.ReadWriter{{survivor, survivorTarget}, {survivorTarget, survivor}} {
		if _, err := direction[0].Write([]byte("survivor")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 8)
		if _, err := io.ReadFull(direction[1], got); err != nil || string(got) != "survivor" {
			t.Fatal("victim revoke interrupted survivor bytes", err)
		}
	}
}

// A real ordered control stream gives a receipt that the late ACK was handled
// before inspecting DATA ownership. Eventual socket closure cannot prove this
// boundary: a fast peer can hide premature local retirement.
// origin: hot-update S5 plan §6.5 / T21 — integration mutation replay.
func exerciseQueuedRevokeACK(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_, _, id := settlementConnection(t, f, 1)
	a, peer := f.peer.attachment, f.endpoint.attachment
	waitEndpoint(t, func() bool {
		a.Mu.Lock()
		defer a.Mu.Unlock()
		return a.Streams[id] != nil
	})
	a.ControlWriteMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(a.ControlWriteMu.Unlock) }
	defer unlock()
	if err := f.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(peer.Control, flow.Frame{Version: flow.Version, Kind: flow.KindACK, FlowID: id, CarrierGen: a.Gen, Flags: peer.Direction}); err != nil {
		t.Fatalf("late ACK write: %v", err)
	}
	requestID := [16]byte{1}
	barrier := make(chan resumeResponse, 1)
	a.Mu.Lock()
	a.Requests[requestID] = barrier
	a.Mu.Unlock()
	defer func() { a.Mu.Lock(); delete(a.Requests, requestID); a.Mu.Unlock() }()
	body := encodeResumeResponse(resumeResponse{RequestID: requestID, Endpoint: a.Endpoint, Owner: a.Owner, Carrier: a.Gen, Verdict: flow.ResumeVerdictAccepted, Pages: 1, Retirement: flow.RetirementImage{NextOpenSeq: 2}})
	if err := peer.Write(peer.Control, flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}); err != nil {
		t.Fatalf("ordered control barrier write: %v", err)
	}
	select {
	case <-barrier:
	case <-time.After(CarrierWriteBudget):
		t.Fatal("late ACK control barrier did not arrive")
	}
	a.Mu.Lock()
	retained := a.Bound[id] && a.Streams[id] != nil
	a.Mu.Unlock()
	if !retained {
		t.Fatal("late ACK retired DATA before peer received revoke CLOSE")
	}
	state, _, err := flow.Observe(f.server.flowState.manager, id)
	if err != nil || state.Phase == flow.FlowPhaseTerminal {
		t.Fatal("peer processed revoke while its CLOSE was still held", err)
	}
	unlock()
}

// These pipe peers exercise the shared writer and DATA reader independently:
// reading the complete CLOSE does not yet apply it or close the peer's DATA.
// The real TLS/yamux lifecycle above separately proves end-to-end retirement.
// origin: hot-update S5 plan §3.3 / T21 — integration mutation replay.
func exerciseSentRevokeClose(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_, _, id := settlementConnection(t, f, 1)
	if err := f.client.flowState.manager.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	control, peerControl := net.Pipe()
	data, peerData := net.Pipe()
	defer func() {
		if err := peerControl.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := peerData.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	a := newFlowCarrier(nil, nil, control, f.client.flowState.manager, f.peer.id, f.peer.owner, f.peer.carrier, f.peer.attachment.Direction)
	defer func() {
		a.Close()
		a.Join()
		if err := data.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	a.Bound[id] = true
	a.PumpData(id, data, 0)
	frames, _, err := flow.Controls(a.Manager, id, a.Gen)
	if err != nil || len(frames) == 0 || frames[len(frames)-1].Kind != flow.KindClose {
		t.Fatal("revoke CLOSE prerequisite missing", err)
	}
	closeFrame := frames[len(frames)-1]
	read := make(chan error, 1)
	go func() {
		got, err := flow.DecodeFrame(peerControl)
		if err == nil && (got.Kind != flow.KindClose || !bytes.Equal(got.Body, closeFrame.Body)) {
			err = io.ErrUnexpectedEOF
		}
		read <- err
	}()
	if err := flushControls(a, map[controlKey]flow.Frame{{ID: id, Kind: flow.KindClose}: closeFrame}); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	a.Mu.Lock()
	retained := a.Bound[id] && a.Streams[id] != nil
	a.Mu.Unlock()
	if !retained {
		t.Fatal("sent revoke CLOSE retired DATA before peer closed it")
	}
	_ = peerData.Close()
	waitEndpoint(t, func() bool {
		a.Mu.Lock()
		defer a.Mu.Unlock()
		return !a.Bound[id] && a.Streams[id] == nil
	})
}

type flowEOFReceiptConn struct {
	net.Conn
	eof chan struct{}
	mu  sync.Once
}

func (c *flowEOFReceiptConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err == io.EOF {
		c.mu.Do(func() { close(c.eof) })
	}
	return n, err
}

// DATA EOF can precede terminal evidence on the separate control stream. Hold
// that evidence until the actual reader reports EOF, then release the manager.
// origin: hot-update S5 plan §3.3 / T21 — integration mutation replay.
func exerciseDataEOFBeforeControl(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_, _, id := settlementConnection(t, f, 1)
	control, peerControl := net.Pipe()
	data, peerData := net.Pipe()
	defer func() {
		if err := peerControl.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := peerData.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	a := newFlowCarrier(nil, nil, control, f.client.flowState.manager, f.peer.id, f.peer.owner, f.peer.carrier, f.peer.attachment.Direction)
	defer func() {
		a.Close()
		a.Join()
		if err := data.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	receipt := &flowEOFReceiptConn{Conn: data, eof: make(chan struct{})}
	a.Bound[id] = true
	a.PumpData(id, receipt, 0)
	_ = peerData.Close()
	select {
	case <-receipt.eof:
	case <-time.After(CarrierWriteBudget):
		t.Fatal("DATA reader did not observe original EOF")
	}
	select {
	case <-a.Stop:
		t.Fatal("DATA EOF fenced carrier before terminal control could arrive")
	case <-time.After(CarrierWriteBudget / 10):
	}
	if err := a.Manager.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	a.Join()
	select {
	case <-a.Stop:
		t.Fatal("late terminal control did not preserve healthy carrier")
	default:
	}
}

// Applied inbound FIN proves DATA EOF safe while the reverse FIN receipt is
// still outstanding. A terminal-only EOF check silently kills that live half.
// origin: hot-update S5 plan §3.3 / T06 — integration mutation replay.
func exerciseFinishedDataEOF(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_, _, id := settlementConnection(t, f, 1)
	manager := f.client.flowState.manager
	if err := flow.Receive(manager, flow.Frame{Version: flow.Version, Kind: flow.KindFIN, FlowID: id, CarrierGen: f.peer.carrier}); err != nil {
		t.Fatal(err)
	}
	settlementState(t, manager, id, func(s flow.State) bool { return s.FINFlags&6 == 6 && s.Phase != flow.FlowPhaseTerminal })
	control, peerControl := net.Pipe()
	data, peerData := net.Pipe()
	defer func() {
		if err := peerControl.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := peerData.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	a := newFlowCarrier(nil, nil, control, manager, f.peer.id, f.peer.owner, f.peer.carrier, f.peer.attachment.Direction)
	defer func() {
		a.Close()
		a.Join()
		if err := data.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	a.Bound[id] = true
	a.PumpData(id, data, 0)
	_ = peerData.Close()
	select {
	case <-a.Stop:
		t.Fatal("applied inbound FIN did not preserve reverse carrier")
	case <-time.After(2 * CarrierWriteBudget):
	}
}

// origin: hot-update S5 internal review SS-REVOKE-1
func TestHardRevokeRetainedReceiptStillSubmitsPublicClose(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRetainedRevokeSubmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retained revoke submission", g0)
	testharness.AssertNoFDLeak(t, "retained revoke submission", fd0, 0)
}

func exerciseRetainedRevokeSubmission(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	_, _, id := settlementConnection(t, f, 1)
	if err := f.client.flowState.manager.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	if len(flow.States(f.client.flowState.manager, f.peer.id)) != 0 {
		t.Fatal("terminal receipt still owns live IO")
	}
	// Isolate the callback's submission from the old attachment's watchers.
	// This uses the real carrier transport and owner identity, with its writer
	// held before launch so the queued CLOSE can be observed independently.
	st := f.client.flowState
	st.mu.Lock()
	old := f.peer.attachment
	queued := newFlowCarrier(old.Conn, old.Mux, old.Control, st.manager, old.Endpoint, old.Owner, old.Gen, old.Direction)
	f.peer.attachment = queued
	st.mu.Unlock()
	defer func() {
		st.mu.Lock()
		f.peer.attachment = old
		st.mu.Unlock()
	}()
	if err := f.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	queued.Mu.Lock()
	frame, submitted := queued.Queued[controlKey{ID: id, Kind: flow.KindClose}]
	queued.Mu.Unlock()
	if !submitted || len(frame.Body) != 35 || flow.CloseMode(frame.Body[0]) != flow.CloseModeRevoke {
		t.Fatal("retained victim receipt lost its explicit public CLOSE")
	}
}

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestFlowRevokeClosesPeerBeforeDataStreamRetires(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseFlowPeerRevoke(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "peer flow revoke x5", beforeG)
	testharness.AssertNoFDLeak(t, "peer flow revoke x5", beforeFD, 0)
}

func exerciseFlowPeerRevoke(t *testing.T) {
	t.Helper()
	t.Run("queued ACK", exerciseQueuedRevokeACK)
	t.Run("sent CLOSE", exerciseSentRevokeClose)
	t.Run("early DATA EOF", exerciseDataEOFBeforeControl)
	f := settlementSetup(t)
	public, target, id := settlementConnection(t, f, 1)
	// Nonzero counters and a rebuilt attachment expose unfinished shutdown
	// snapshots and queued ACKs that an idle zero-offset victim cannot reveal.
	want := bytes.Repeat([]byte("original-flow-before-revoke"), 512)
	for _, peers := range [][2]*net.TCPConn{{public, target}, {target, public}} {
		if _, err := peers[0].Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(peers[1], got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("original bytes before revoke: %v", err)
		}
	}
	bob, bobTarget, _ := settlementConnection(t, f, 2)
	f.client.flowState.mu.Lock()
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	old.Close()
	old.Join()
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	f.client.flowState.mu.Lock()
	attachment := f.peer.attachment
	f.client.flowState.mu.Unlock()
	// Hold the shared control writer before the authorization decision. Its
	// queued CLOSE must keep the data stream alive until it actually crosses
	// that writer; otherwise the peer sees EOF without the revoke evidence.
	attachment.ControlWriteMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(attachment.ControlWriteMu.Unlock) }
	defer unlock()
	if err := f.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	attachment.Mu.Lock()
	_, retained := attachment.Streams[id]
	attachment.Mu.Unlock()
	if !retained {
		t.Fatal("revoked data stream retired before its CLOSE crossed the control writer")
	}
	unlock()
	for _, conn := range []*net.TCPConn{public, target} {
		_ = conn.SetReadDeadline(time.Now().Add(ResumeAttempt))
		n, err := conn.Read(make([]byte, 1))
		if n != 0 || err != io.EOF {
			t.Fatalf("revoked original peer remained open: n=%d err=%v", n, err)
		}
	}
	settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseTerminal })
	select {
	case <-attachment.Stop:
		t.Fatal("revoking one flow broke the carrier")
	default:
	}
	for _, peers := range [][2]*net.TCPConn{{bob, bobTarget}, {bobTarget, bob}} {
		_ = peers[0].SetWriteDeadline(time.Now().Add(ResumeAttempt))
		_ = peers[1].SetReadDeadline(time.Now().Add(ResumeAttempt))
		if _, err := peers[0].Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(peers[1], got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("survivor bytes after revoke: %v", err)
		}
	}
}
