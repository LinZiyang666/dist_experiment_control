//go:build linux

package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/yamux"
)

func pauseDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}
func pauseRecords(t *testing.T, cut serverRuntimeCut) serverStateSnapshot {
	t.Helper()
	snap, err := cut.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
func pauseRecord(snap serverStateSnapshot, kind string, id flow.EndpointID) map[string]sealedstate.Field {
	for _, record := range snap.Records {
		fields := clientFields(record)
		if record.Kind == kind && (id == (flow.EndpointID{}) || bytes.Equal(fields["id"].Bytes, id[:])) {
			return fields
		}
	}
	return nil
}

// Pause/export preserves accepted original FDs, their reserved slots and the
// exact FIFO sequence. A new connect remains in the same LISTEN backlog and is
// delivered after thaw; neither terminal stop nor deadline retry may consume it.
// origin: hot-update S6 plan §6.2 / T21
func TestEndpointPauseKeepsPendingFIFO(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseEndpointPauseFIFO(t)
	}
	testharness.AssertNoGoroutineLeak(t, "endpoint pending FIFO", beforeG)
	testharness.AssertNoFDLeak(t, "endpoint pending FIFO", beforeFD, 0)
}
func exerciseEndpointPauseFIFO(t *testing.T) {
	t.Helper()
	exerciseEndpointDeadlinePause(t)
	exercisePausedRevoke(t)
	f := endpointSetup(t)
	defer f.s.Close()
	endpoints := []*flowEndpoint{f.e}
	for i := 0; i < 3; i++ {
		var e *flowEndpoint
		if i == 0 {
			e = f.e
		} else {
			var err error
			e, err = reserveEndpoint(f.s, Allocation{SID: "sid", NID: "nid", Port: 0, TokenHash: hashToken("token")}, fenceSnap{})
			if err != nil {
				t.Fatal(err)
			}
			endpoints = append(endpoints, e)
		}
		// Port zero binds atomically. Retain the kernel-assigned port as each real
		// allocation's table key before requesting the next independent listener.
		f.s.mu.Lock()
		delete(f.s.flowState.endpoints, 0)
		e.allocation.Port = e.listener.Addr().(*net.TCPAddr).Port
		f.s.flowState.endpoints[e.allocation.Port] = e
		f.s.mu.Unlock()
	}
	var peers []net.Conn
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()
	for index, e := range endpoints {
		peer, err := net.DialTimeout("tcp", e.listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer)
		waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return len(f.s.flowState.waiters) == index+1 })
	}
	f.s.mu.Lock()
	var fifo []byte
	for _, pending := range f.s.flowState.waiters {
		fifo = append(fifo, pending.id[:]...)
	}
	sockets := make([]*rawsocket.Socket, len(endpoints))
	fingerprints := make([]rawsocket.Fingerprint, len(endpoints))
	listenerInodes := make([]uint64, len(endpoints))
	for i, e := range endpoints {
		sockets[i] = e.pendingRaw
		fingerprints[i] = e.pendingRaw.Fingerprint()
		listenerInodes[i] = listenerIdentity(t, e.listener)
	}
	f.s.mu.Unlock()
	cut, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	snap := pauseRecords(t, cut)
	server := pauseRecord(snap, "server", flow.EndpointID{})
	if !sameWaiterCycle(server["waiters"].Bytes, fifo) || server["pending_slots"].U64 != 3 {
		t.Fatal("pause lost reserved slots or FIFO")
	}
	for i, e := range endpoints {
		fields := pauseRecord(snap, "endpoint", e.id)
		if e.keeper.Descriptor().Inode != listenerInodes[i] || fields["next_seq"].U64 != 1 || fields["pending_ino"].U64 != fingerprints[i].Ino || e.pendingRaw != sockets[i] {
			t.Fatal("pause replaced pending original socket or OPEN cursor")
		}
		select {
		case <-e.stop:
			t.Fatal("pause used terminal stop")
		default:
		}
	}
	// Re-export is a fresh projection with the same original closure.
	again := pauseRecords(t, cut)
	if !reflect.DeepEqual(snap.Records, again.Records) {
		t.Fatal("re-export drifted paused records")
	}
	peer, err := net.DialTimeout("tcp", endpoints[0].keeper.Descriptor().Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	peers = append(peers, peer)
	if _, err = peers[0].Write([]byte("original")); err != nil {
		t.Fatal(err)
	}
	if err = cut.thaw(pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	// Pending raws have no running flow pumps yet, so direct IO proves the exact
	// retained owner was released after snapshot, rather than replaced on resume.
	if err = sockets[0].SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	if _, err = io.ReadFull(sockets[0], got); err != nil || string(got) != "original" {
		t.Fatalf("original pending bytes: %q %v", got, err)
	}
	f.s.mu.Lock()
	same := sameWaiterCycle(serverWaiterIDs(f.s.flowState.waiters), fifo)
	slots := len(f.s.flowState.pending)
	f.s.mu.Unlock()
	if !same || slots < 3 {
		t.Fatal("thaw reordered waiters or released retained slots")
	}
	// Admit the first retained original, then the backlog connection, proving
	// fresh acceptance remains usable after pause without a replacement bind.
	f.s.mu.Lock()
	endpoints[0].ownerGen = 1
	endpoints[0].carrierGen = 1
	endpoints[0].targetPort = 22
	f.s.mu.Unlock()
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return endpoints[0].nextSeq >= 2 })
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return endpoints[0].nextSeq >= 3 })
	if endpoints[0].keeper.Descriptor().Inode == 0 {
		t.Fatal("keeper identity missing")
	}
}

// Idle endpoints yield their turn by rotating the head; that preserves the
// relative cyclic FIFO order during live scheduling. While paused, re-export
// above compares the complete records byte-for-byte, with no allowed rotation.
func sameWaiterCycle(actual, expected []byte) bool {
	if len(actual) != len(expected) {
		return false
	}
	for offset := 0; offset < len(expected); offset += 16 {
		rotated := append(append([]byte(nil), expected[offset:]...), expected[:offset]...)
		if bytes.Equal(actual, rotated) {
			return true
		}
	}
	return len(expected) == 0
}

func exercisePausedRevoke(t *testing.T) {
	exercisePendingAliasDisposition(t, true)
	exercisePendingAliasDisposition(t, false)
}
func exercisePendingAliasDisposition(t *testing.T, revoke bool) {
	t.Helper()
	f := endpointSetup(t)
	defer f.s.Close()
	f.s.mu.Lock()
	delete(f.s.flowState.endpoints, 0)
	f.e.allocation.Port = f.e.listener.Addr().(*net.TCPAddr).Port
	f.s.flowState.endpoints[f.e.allocation.Port] = f.e
	f.s.mu.Unlock()

	peer, err := net.DialTimeout("tcp", f.e.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServerTestResource(t, peer)
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return f.e.pendingRaw != nil })
	cut, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	snap := pauseRecords(t, cut)
	copies := handofftest.DupResources(t, snap.Resources)
	defer copies.Close()
	fields := pauseRecord(snap, "endpoint", f.e.id)
	if len(fields["pending_ref"].Bytes) == 0 {
		t.Fatal("pause lost pending original socket")
	}
	file, err := copies.TakeFile(string(fields["pending_ref"].Bytes), "raw")
	if err != nil {
		t.Fatal(err)
	}
	alias, err := rawsocket.Adopt(file, f.e.pendingRaw.Fingerprint())
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer closeServerTestResource(t, alias)
	if !revoke {
		if err = cut.discard(); err != nil {
			t.Fatal(err)
		}
		if _, err = peer.Write([]byte("successor")); err != nil {
			t.Fatal(err)
		}
		_ = alias.SetReadDeadline(time.Now().Add(time.Second))
		got := make([]byte, 9)
		if _, err = io.ReadFull(alias, got); err != nil || string(got) != "successor" {
			t.Fatalf("discard shut down successor pending OFD: %q %v", got, err)
		}
		return
	}
	if !f.s.CloseProxyIf(endpointInfo(f.e)) {
		t.Fatal("paused allocation revoke did not find its owner")
	}
	_ = alias.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := alias.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("paused revoke left same-OFD pending alias usable: n=%d err=%v", n, err)
	}
	if err = cut.thaw(pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	f.s.mu.Lock()
	count := len(f.s.flowState.endpoints)
	f.s.mu.Unlock()
	if count != 0 {
		t.Fatal("thaw revived revoked endpoint")
	}
}

type pausePollListener struct {
	*net.TCPListener
	entered chan struct{}
	calls   atomic.Int32
}

func (ln *pausePollListener) Accept() (net.Conn, error) {
	if ln.calls.Add(1) == 1 {
		close(ln.entered)
	}
	return ln.TCPListener.Accept()
}
func exerciseEndpointDeadlinePause(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	defer f.s.Close()
	// First pause the real initial accept owner, then give a fresh view to the
	// actual loop with a counted native deadline wake. No invented socket error.
	f.s.mu.Lock()
	f.e.paused = true
	close(f.e.pause)
	ln := f.e.listener.(*net.TCPListener)
	f.s.mu.Unlock()
	_ = ln.SetDeadline(time.Now())
	if err := serverPauseWait(f.e.done, pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	_ = ln.SetDeadline(time.Time{})
	view := &pausePollListener{TCPListener: ln, entered: make(chan struct{})}
	f.s.mu.Lock()
	f.e.paused = false
	f.e.pause = make(chan struct{})
	f.e.done = make(chan struct{})
	f.e.listener = view
	f.s.mu.Unlock()
	go endpointAccept(f.s, f.e)
	<-view.entered
	f.s.mu.Lock()
	f.e.paused = true
	close(f.e.pause)
	f.s.mu.Unlock()
	_ = ln.SetDeadline(time.Now())
	if err := serverPauseWait(f.e.done, pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	if view.calls.Load() != 1 {
		t.Fatal("deadline wake retried Accept across pause")
	}
}

// A nil timer pointer must not erase an already-fired callback's join or its
// cleanup duty. The owner itself is gated, never a sleep that hopes to race Stop.
// origin: hot-update S6 plan §6.2 / T22
func TestEndpointTimerCallbackJoin(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseEndpointTimerJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "endpoint timer join", beforeG)
	testharness.AssertNoFDLeak(t, "endpoint timer join", beforeFD, 0)
}
func exerciseEndpointTimerJoin(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	defer f.s.Close()
	f.s.mu.Lock()
	stopEndpointTimerLocked(f.s, f.e)
	// A real AfterFunc completion channel represents a callback which has fired
	// and owns cleanup work. It stays in the server census after timer=nil.
	work := &endpointTimer{done: make(chan struct{})}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.e.timerWork = work
	f.s.callbacks[work] = struct{}{}
	f.e.timer = time.AfterFunc(0, func() {
		defer finishEndpointTimer(f.s, work)
		f.s.mu.Lock()
		f.e.fired = true
		f.e.cleanupPending = true
		f.e.timer = nil
		f.s.mu.Unlock()
		close(entered)
		<-release
	})
	f.s.mu.Unlock()
	<-entered
	type outcome struct {
		cut serverRuntimeCut
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		cut, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt))
		result <- outcome{cut, err}
	}()
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return f.s.imagePaused })
	select {
	case <-result:
		t.Fatal("freeze ignored fired callback with nil timer")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	out := <-result
	if out.err != nil {
		t.Fatal(out.err)
	}
	select {
	case <-work.done:
	default:
		t.Fatal("callback returned no completion receipt")
	}
	fields := pauseRecord(pauseRecords(t, out.cut), "endpoint", f.e.id)
	if fields["fired"].U64 != 1 || fields["cleanup_pending"].U64 != 1 {
		t.Fatal("export forgot fired cleanup obligation")
	}
	if err := out.cut.thaw(pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	// Exercise the production expiry callback itself, including its terminal
	// close path. It cannot self-join the completion record it still owns.
	f.s.mu.Lock()
	stopEndpointTimerLocked(f.s, f.e)
	f.e.detachDeadline = time.Now()
	f.e.detachBootNS = 0
	armDetachLocked(f.s, f.e, time.Now())
	f.s.mu.Unlock()
	waitEndpoint(t, func() bool {
		f.s.mu.Lock()
		defer f.s.mu.Unlock()
		return len(f.s.flowState.endpoints) == 0 && len(f.s.callbacks) == 0
	})
}

// The carrier's unread stream creates a real positive partial yamux Write with
// an error on transport cancellation. A held monitor completion keeps raw pumps
// running until the complete carrier census joins; offsets must survive Export.
// origin: hot-update S6 plan §6.2 / T23
func TestServerCarrierJoinBeforeRawFreeze(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseServerCarrierJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "server carrier joins", beforeG)
	testharness.AssertNoFDLeak(t, "server carrier joins", beforeFD, 0)
}
func exerciseServerCarrierJoin(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	defer f.s.Close()
	peer, err := net.DialTimeout("tcp", f.e.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServerTestResource(t, peer)
	f.s.mu.Lock()
	f.e.ownerGen = 1
	f.e.carrierGen = 1
	f.e.targetPort = 22
	f.s.mu.Unlock()
	waitEndpoint(t, func() bool { return len(flow.States(f.s.flowState.manager, f.e.id)) == 1 })
	states := flow.States(f.s.flowState.manager, f.e.id)
	id := states[0].ID
	if _, err = peer.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, func() bool { st, _, err := flow.Observe(f.s.flowState.manager, id); return err == nil && st.TxEnd == 6 })
	a, slow, slowPeer, cleanup := pauseCarrier(t, f.s.flowState.manager, f.e.id)
	defer cleanup()
	f.s.mu.Lock()
	f.e.attachment = a
	f.e.phase = EndpointPhaseAttached
	stopEndpointTimerLocked(f.s, f.e)
	f.e.detachDeadline = time.Time{}
	f.s.mu.Unlock()
	startServerCarrierMonitor(f.s, f.e, a)
	f.s.mu.Lock()
	realMonitor := f.s.monitors[a]
	heldMonitor := make(chan struct{})
	f.s.monitors[a] = heldMonitor
	f.s.mu.Unlock()
	var release sync.Once
	defer release.Do(func() { close(heldMonitor) })
	type writeResult struct {
		n   int
		err error
	}
	written := make(chan writeResult, 1)
	payload := bytes.Repeat([]byte{0x6d}, 4*(256<<10))
	a.Launch(func() { n, err := slow.Write(payload); written <- writeResult{n, err} })
	// A byte in the receiver's first TLS record does not prove that yamux's
	// first window write returned. Read into the second window before closing:
	// this forces a positive completed prefix even on one physical CPU, while
	// the unread remainder still exceeds the peer's entire receive window.
	// origin: hot-update S6 plan §6.2 / T23
	prefix := make([]byte, (256<<10)+1)
	if _, err = io.ReadFull(slowPeer, prefix); err != nil || !bytes.Equal(prefix, payload[:len(prefix)]) {
		t.Fatal("stalled stream never delivered its completed first window")
	}
	type outcome struct {
		cut serverRuntimeCut
		err error
	}
	result := make(chan outcome, 1)
	go func() {
		cut, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt))
		result <- outcome{cut, err}
	}()
	if err = serverPauseWait(a.Stop, pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatalf("carrier transport cancellation did not stop shared writer: %v", err)
	}
	write := <-written
	if write.n <= 0 || write.n >= len(payload) || write.err == nil {
		t.Fatalf("missing positive partial yamux Write: n=%d err=%v", write.n, write.err)
	}
	<-realMonitor
	if _, err = peer.Write([]byte("during")); err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, func() bool {
		st, _, err := flow.Observe(f.s.flowState.manager, id)
		return err == nil && st.TxEnd == 12
	})
	select {
	case <-result:
		t.Fatal("raw freeze preceded monitor completion")
	default:
	}
	release.Do(func() { close(heldMonitor) })
	out := <-result
	if out.err != nil {
		t.Fatal(out.err)
	}
	im, snapshot, err := out.cut.flowExport()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || len(im.Records) != 1 || im.Records[0].TxEnd != 12 {
		t.Fatal("carrier shutdown dropped committed source progress")
	}
	if err = out.cut.thaw(pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	if _, err = peer.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, func() bool {
		st, _, err := flow.Observe(f.s.flowState.manager, id)
		return err == nil && st.TxEnd == 17
	})
}
func pauseCarrier(t *testing.T, m *flow.Manager, id flow.EndpointID) (*flowCarrier, net.Conn, net.Conn, func()) {
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
	sender, err := yamux.Client(client, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := yamux.Server(server, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	control, err := sender.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	peerControl, err := receiver.AcceptStream()
	if err != nil {
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
	a := newFlowCarrier(client, sender, control, m, id, 1, 1, 0)
	return a, slow, slowPeer, func() { a.Close(); _ = right.Close(); _ = receiver.Close(); _ = peerControl.Close(); a.Join() }
}

// A refused join retains a real recoverable cut, closes stalled REGISTER sockets
// and leaves the original listener identity available to a later thaw.
// origin: hot-update S6 plan §4.2 / §6.2 / T21–T23
func TestBrokerServerPauseLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseServerPauseFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "server pause failure", beforeG)
	testharness.AssertNoFDLeak(t, "server pause failure", beforeFD, 0)
}
func exerciseServerPauseFailure(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: test control listener lifetime.
	defer cancel()
	defer f.s.Close()
	if cut, err := freezeServerRuntime(f.s, 0); cut != nil || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("zero deadline changed admission: %v", err)
	}
	f.s.mu.Lock()
	paused := f.s.imagePaused
	f.s.mu.Unlock()
	if paused {
		t.Fatal("expired deadline fenced live owner")
	}
	legacyRelease := make(chan struct{})
	var legacyOnce sync.Once
	defer legacyOnce.Do(func() { close(legacyRelease) })
	launchLegacyServerWorker(f.s, func() { <-legacyRelease })
	if cut, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt)); cut != nil || !errors.Is(err, errServerLegacyBusy) {
		t.Fatalf("closing legacy worker escaped busy census: %v", err)
	}
	f.s.mu.Lock()
	paused = f.s.imagePaused
	f.s.mu.Unlock()
	if paused {
		t.Fatal("legacy busy refusal fenced live owner")
	}
	legacyOnce.Do(func() { close(legacyRelease) })
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return f.s.legacyWorkers == 0 })
	if err := f.s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	peer, err := net.DialTimeout("tcp", f.s.ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServerTestResource(t, peer)
	waitEndpoint(t, func() bool { f.s.mu.Lock(); defer f.s.mu.Unlock(); return len(f.s.controlConns) == 1 })
	f.s.mu.Lock()
	work := &endpointTimer{done: make(chan struct{})}
	f.s.callbacks[work] = struct{}{}
	f.s.mu.Unlock()
	defer finishEndpointTimer(f.s, work)
	cut, err := freezeServerRuntime(f.s, pauseDeadline(t, 25*time.Millisecond))
	if cut == nil || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("join refusal lost cut: %v", err)
	}
	if _, err = cut.snapshot(); err == nil {
		t.Fatal("partial cut manufactured a completed snapshot")
	}
	// The stalled TLS/REGISTER must be canceled, rather than waiting for its old
	// five-second read timer and abandoning a handler after a failed image cut.
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled REGISTER survived transport fence")
	}
	finishEndpointTimer(f.s, work)
	if err = cut.thaw(pauseDeadline(t, ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	fresh, err := net.DialTimeout("tcp", f.s.ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = fresh.Close()
	// Repeated cut/export/thaw retires every cut-owned sealed state in one live
	// process. No receiver exit or test cleanup can conceal an original FD leak.
	again, err := freezeServerRuntime(f.s, pauseDeadline(t, ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	_, snapshot, err := again.flowExport()
	if err != nil || snapshot == nil {
		t.Fatalf("empty flow export: %v", err)
	}
	previousFile := snapshot.Owned[0]
	_, replacement, err := again.flowExport()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = previousFile.Stat(); err == nil {
		t.Fatal("re-export leaked previous sealed FD")
	}
	replacementFile := replacement.Owned[0]
	if err = again.discard(); err != nil {
		t.Fatal(err)
	}
	if _, err = replacementFile.Stat(); err == nil {
		t.Fatal("discard leaked latest sealed FD")
	}
	if _, err = again.snapshot(); err == nil {
		t.Fatal("discarded cut remains exportable")
	}
	if err = again.thaw(pauseDeadline(t, ResumeAttempt)); err == nil {
		t.Fatal("discarded cut restarted admission")
	}
}
