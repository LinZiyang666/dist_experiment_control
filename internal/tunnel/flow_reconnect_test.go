//go:build linux

package tunnel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/yamux"
)

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestConcurrentCarrierRecoverySharesOneAttachment(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseConcurrentCarrierRecovery(t) })
		t.Run("initial authority", func(t *testing.T) { exerciseInitialResumeRetry(t) })
		t.Run("initial open authority", func(t *testing.T) { exerciseInitialOpenAuthority(t) })
		t.Run("legacy initial authority", func(t *testing.T) { exerciseLegacyInitialAuthority(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "shared carrier recovery x5", beforeG)
	testharness.AssertNoFDLeak(t, "shared carrier recovery x5", beforeFD, 0)
}

// origin: hot-update S5 plan §4.1 / §4.3 / T13 / DRILL-RUN 75.C
// A fresh home can refuse REGISTER while applying its allocation. Waiting for
// the proxy's next heartbeat after that refusal loses the initial open window.
func exerciseInitialOpenAuthority(t *testing.T) {
	t.Helper()
	var lookups atomic.Int32
	var refusal atomic.Pointer[string]
	f := settlementSetupWithDial(t, func(ctx context.Context, f *settlementFixture) error {
		clientCtx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		f.client.Start(clientCtx)
		t.Cleanup(func() { f.client.Close(f.port); cancel() })
		return f.client.OpenHome(ctx, f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{})
	}, func(AuthRequest) error {
		n := lookups.Add(1)
		if reason := refusal.Load(); reason != nil {
			return errors.New(*reason)
		}
		if n == 1 || n == 3 {
			return errors.New(proto.ReasonHomeCatchingUp)
		}
		return nil
	})
	if lookups.Load() < 5 {
		t.Fatal("initial open did not recheck REGISTER and RESUME authority")
	}
	public, target, _ := settlementConnection(t, f, 1)
	want := []byte("original business after initial home authority")
	if _, err := public.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(target, got); err != nil || string(got) != string(want) {
		t.Fatalf("initial open failed original business: %v", err)
	}
	port := findFreePort(t)
	terminal := "session_closed"
	refusal.Store(&terminal)
	before := lookups.Load()
	err := f.client.OpenHome(t.Context(), port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{})
	var deny *DenyError
	if !errors.As(err, &deny) || deny.Reason != terminal || lookups.Load() != before+1 {
		t.Fatalf("initial terminal refusal was retried: %v lookups=%d", err, lookups.Load()-before)
	}
	transient := proto.ReasonHomeCatchingUp
	refusal.Store(&transient)
	for _, duration := range []time.Duration{40 * time.Millisecond, 2 * ResumeAttempt} {
		ctx, cancel := context.WithTimeout(t.Context(), duration)
		start := time.Now()
		before := lookups.Load()
		err = f.client.OpenHome(ctx, port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{})
		cancel()
		limit := duration + time.Second/2
		if duration > ResumeAttempt {
			limit = ResumeAttempt + time.Second/2
		}
		if err == nil || time.Since(start) > limit || f.client.HasSession(port) {
			t.Fatalf("initial refusal extended its original budget or installed responsibility: %v", err)
		}
		window := min(duration, ResumeAttempt)
		if lookups.Load()-before > int32(window/(20*time.Millisecond))+2 {
			t.Fatal("transient initial authority refusal caused a busy retry loop")
		}
	}
}

// origin: hot-update S5 plan §4.1 / §7.1 / T15 / DRILL-RUN 75.C
func exerciseLegacyInitialAuthority(t *testing.T) {
	t.Helper()
	h := newReconnectHarness(t, silentLog())
	h.cli.Close(h.publicPort)
	reason := proto.ReasonHomeCatchingUp
	h.denyReason.Store(&reason)
	before := h.registers.Load()
	err := h.cli.Open(h.publicPort, h.localPort, h.token)
	var deny *DenyError
	if !errors.As(err, &deny) || deny.Reason != reason || h.registers.Load() != before+1 {
		t.Fatalf("legacy initial open changed its single-attempt contract: %v lookups=%d", err, h.registers.Load()-before)
	}
}

// origin: hot-update S5 plan §4.3 / T13 / DRILL-RUN 75.A
func exerciseInitialResumeRetry(t *testing.T) {
	t.Helper()
	var lookups atomic.Int32
	f := settlementSetupWithDial(t, func(ctx context.Context, f *settlementFixture) error {
		// A fast post-REGISTER refusal can close yamux before OpenStream,
		// rather than during the RESUME reply read. Both retain the owner
		// and the same bounded retry classification.
		// origin: hot-update S5 plan §4.3 / T13 / GATES-FINAL-3
		left, right := net.Pipe()
		defer func() { _ = left.Close(); _ = right.Close() }()
		closed, err := yamux.Client(left, flowMuxConfig(io.Discard))
		if err != nil {
			t.Fatal(err)
		}
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		_, _, err = exchangeClientFlowResume(f.client.flowState, closed, resumeRequest{})
		var stopped *DenyError
		if !errors.As(err, &stopped) || stopped.Reason != "try_again" {
			t.Fatalf("closed RESUME stream lost retry classification: %v", err)
		}
		_, _, err = f.client.dialAndRegister(ctx, f.port, "token", "", 0, proto.CertPins{})
		var deny *DenyError
		if !errors.As(err, &deny) || deny.Reason != "try_again" {
			t.Fatalf("post-REGISTER authority loss lost retry classification: %v", err)
		}
		_, _, err = f.client.dialAndRegister(ctx, f.port, "token", "", 0, proto.CertPins{})
		return err
	}, func(AuthRequest) error {
		if lookups.Add(1) == 2 {
			return errors.New(proto.ReasonHomeCatchingUp)
		}
		return nil
	})
	if lookups.Load() < 4 {
		t.Fatal("initial retry did not recheck REGISTER and RESUME authority")
	}
	public, target, _ := settlementConnection(t, f, 1)
	want := []byte("first business after fresh authority")
	if _, err := public.Write(want); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(target, got); err != nil || string(got) != string(want) {
		t.Fatal("initial retry did not bind the original business socket")
	}
}

func exerciseConcurrentCarrierRecovery(t *testing.T) {
	t.Helper()
	var lookups atomic.Int32
	var refusing atomic.Bool
	f := settlementSetup(t, func(AuthRequest) error {
		// The original owner registers successfully. Its first recovery sees
		// a real transient authority refusal before the cursor catches up.
		// origin: hot-update S5 plan §4.4 / T13 / DRILL-RUN
		if refusing.Load() && lookups.Add(1) == 1 {
			return errors.New(proto.ReasonHomeCatchingUp)
		}
		return nil
	})
	public, target, _ := settlementConnection(t, f, 1)
	f.client.flowState.mu.Lock()
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	old.Close()
	old.Join()
	refusing.Store(true)
	if err := waitClientBindings(f.client, f.peer, time.Now().Add(ResumeAttempt)); err == nil {
		t.Fatal("closed attachment satisfied the original flow binding census")
	}
	// origin: hot-update S5 plan §4.4 / T13 / DRILL-RUN. A carrier can
	// close between census and PREPARE without losing either original FD.
	if err := f.client.PrepareHandoff(2, [16]byte{1}, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatalf("PREPARE abandoned the resumable original owner: %v", err)
	}
	f.client.flowState.mu.Lock()
	prepared := f.peer.permit != nil && f.peer.permit.next == 2 && f.peer.attachment != old
	f.client.flowState.mu.Unlock()
	f.server.mu.Lock()
	brokerPrepared := f.endpoint.permit != nil && f.endpoint.permit.next == 2
	f.server.mu.Unlock()
	if !prepared || !brokerPrepared {
		t.Fatal("PREPARE did not retain the new carrier's accepted permit")
	}
	if lookups.Load() < 2 {
		t.Fatal("PREPARE reused authority after a transient REGISTER refusal")
	}
	ctx, cancel := context.WithTimeout(t.Context(), ResumeAttempt)
	defer cancel()
	type result struct {
		conn net.Conn
		mux  *yamux.Session
		err  error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			conn, mux, err := f.client.dialAndRegister(ctx, f.port, "token", "", 0, proto.CertPins{})
			results <- result{conn, mux, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("parallel recovery refused same current owner: %v / %v", first.err, second.err)
	}
	if first.conn != second.conn || first.mux != second.mux {
		t.Fatal("parallel recovery installed competing TLS/yamux attachments")
	}
	select {
	case <-first.mux.CloseChan():
		t.Fatal("parallel recovery returned an already fenced attachment")
	default:
	}
	f.client.Start(t.Context())
	t.Cleanup(func() { f.client.Close(f.port) })
	if err := f.client.OpenHome(t.Context(), f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{}); err != nil {
		t.Fatal(err)
	}
	f.client.mu.Lock()
	session := f.client.sessions[f.port]
	gen := session.gen
	f.client.mu.Unlock()
	if installed, _ := f.client.swapTransport(f.port, gen, first.conn, first.mux, false); !installed || first.mux.IsClosed() {
		t.Fatal("adopting the shared transport closed its own current attachment")
	}
	want := []byte("original business survives prepare recovery")
	for _, direction := range [][2]*net.TCPConn{{public, target}, {target, public}} {
		if _, err := direction[0].Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(direction[1], got); err != nil || string(got) != string(want) {
			t.Fatalf("PREPARE replaced or lost the original business: %v", err)
		}
	}
}

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestExpiredCarrierRecoveryRetainsRefusal(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseExpiredCarrierRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "expired carrier refusal x5", beforeG)
	testharness.AssertNoFDLeak(t, "expired carrier refusal x5", beforeFD, 0)
}

func exerciseExpiredCarrierRefusal(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	public, target, _ := settlementConnection(t, f, 1)
	f.client.flowState.mu.Lock()
	f.peer.detachDeadline = time.Now().Add(-time.Second)
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	// Both owners use the same elapsed first-detach deadline. Closing the
	// real carrier lets the broker's normal deadline timer retire its side.
	f.server.mu.Lock()
	f.endpoint.detachDeadline = time.Now().Add(-time.Second)
	f.server.mu.Unlock()
	old.Close()
	old.Join()
	_, _, _, err := f.client.redialWithBackoff(t.Context(), f.port, "token", "", 0, proto.CertPins{}, 1)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expired recovery was not refused: %v", err)
	}
	if err := f.client.ResumeCarriers(time.Now().Add(ResumeAttempt)); err == nil {
		t.Fatal("expired owner disappeared into an empty successful recovery")
	}
	for _, conn := range []*net.TCPConn{public, target} {
		_ = conn.SetReadDeadline(time.Now().Add(ResumeAttempt))
		n, err := conn.Read(make([]byte, 1))
		if n != 0 || err != io.EOF {
			t.Fatalf("expired owner kept original peer open: n=%d err=%v", n, err)
		}
	}
}

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestCarrierRecoveryWaitsForSupervisorPublication(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseSupervisorPublication(t) })
		t.Run("restored_readiness", func(t *testing.T) { exerciseRestoredCarrierReadiness(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor publication x5", beforeG)
	testharness.AssertNoFDLeak(t, "supervisor publication x5", beforeFD, 0)
}

// A restored endpoint has no ordinary port slot. Its lifetime supervisor must
// publish the same real attachment edges or the agent's eight-second planned
// readiness expires and the reaper removes a healthy SS allocation.
// origin: hot-update S5 plan §4.4 / §7.1 / T38 / DRILL-RUN
func exerciseRestoredCarrierReadiness(t *testing.T) {
	t.Helper()
	var recovering atomic.Bool
	var calls atomic.Int32
	f := settlementSetup(t, func(AuthRequest) error {
		// REGISTER and endpoint RESUME succeed; the first original DATA bind
		// then encounters real transient authority loss and closes its carrier.
		// origin: hot-update S5 plan §4.4 / T13 / DRILL-RUN 75.R
		if recovering.Load() && calls.Add(1) == 3 {
			return errors.New(proto.ReasonHomeCatchingUp)
		}
		return nil
	})
	public, target, _ := settlementConnection(t, f, 1)
	edges := make(chan bool, 8)
	f.client.SetSessionStateHook(func(port int, up bool) {
		if port != f.port {
			t.Error("restored readiness changed the original allocation")
		}
		edges <- up
	})
	wantEdge := func(want bool) {
		t.Helper()
		select {
		case got := <-edges:
			if got != want {
				t.Fatalf("restored readiness edge=%t, want %t", got, want)
			}
		case <-time.After(ResumeAttempt):
			t.Fatalf("restored carrier omitted readiness edge %t", want)
		}
	}
	f.peer.attachment.Close()
	f.peer.attachment.Join()
	if err := f.client.ResumeCarriers(time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	wantEdge(true)
	f.client.flowState.mu.Lock()
	a := f.peer.attachment
	f.client.flowState.mu.Unlock()
	recovering.Store(true)
	a.Close()
	wantEdge(false)
	wantEdge(true)
	if calls.Load() < 6 {
		t.Fatal("restored recovery did not retry after accepted RESUME lost its DATA bind")
	}
	for _, pair := range [][2]*net.TCPConn{{public, target}, {target, public}} {
		want := []byte("original restored carrier readiness")
		if _, err := pair[0].Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(pair[1], got); err != nil || string(got) != string(want) {
			t.Fatalf("readiness did not retain original business: %v", err)
		}
	}
}

func exerciseSupervisorPublication(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	public, target, _ := settlementConnection(t, f, 1)
	down, release := make(chan struct{}), make(chan struct{})
	var downOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	f.client.SetSessionStateHook(func(_ int, up bool) {
		if !up {
			downOnce.Do(func() { close(down) })
			<-release
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	f.client.Start(ctx)
	t.Cleanup(func() { unblock(); f.client.Close(f.port); cancel() })
	if err := f.client.OpenHome(ctx, f.port, f.target.Addr().(*net.TCPAddr).Port, "token", "", 0, proto.CertPins{}); err != nil {
		t.Fatal(err)
	}
	f.client.flowState.mu.Lock()
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	old.Close()
	select {
	case <-down:
	case <-time.After(ResumeAttempt):
		t.Fatal("supervisor never observed old carrier close")
	}
	// A real reconnect completes RESUME while the ordinary supervisor is held
	// just before its slot publication. The next cut must wait for that slot.
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	if err := f.client.ResumeCarriers(time.Now().Add(50 * time.Millisecond)); err == nil {
		t.Fatal("recovery completed while supervisor still held the superseded mux")
	}
	unblock()
	if err := f.client.ResumeCarriers(time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	if _, err := public.Write([]byte("published-carrier")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("published-carrier"))
	if _, err := io.ReadFull(target, got); err != nil || string(got) != "published-carrier" {
		t.Fatalf("original flow after publication: %q %v", got, err)
	}
}

// origin: hot-update S5 internal review HOTUPDATE-T40
func TestRejectedRegisterKeepsDifferentAllocation(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRegisterRefusalIsolation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "register refusal isolation x5", beforeG)
	testharness.AssertNoFDLeak(t, "register refusal isolation x5", beforeFD, 0)
}

func exerciseRegisterRefusalIsolation(t *testing.T) {
	t.Helper()
	f := endpointSetup(t)
	defer closeEndpoint(f.s, f.e)
	before := listenerIdentity(t, f.e.listener)
	f.allowed.Store(false)
	request := f.e.allocation
	request.TokenHash = hashToken("wrong-token")
	left, right := net.Pipe()
	defer func() {
		if err := left.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := right.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	_ = right.SetDeadline(time.Now().Add(ResumeAttempt))
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleFlowRegister(f.s, left, bufio.NewReader(left), request, fenceSnap{}, t.Context().Done())
	}()
	defer func() { _ = right.Close(); <-done }()
	reply, err := bufio.NewReader(right).ReadString('\n')
	if err != nil || reply != "DENY session_closed\n" {
		t.Fatalf("invalid credential was not refused: %q %v", reply, err)
	}
	<-done
	f.s.mu.Lock()
	retained := endpointLiveLocked(f.s, f.e)
	f.s.mu.Unlock()
	if !retained || listenerIdentity(t, f.e.listener) != before {
		t.Fatal("refusing another allocation retired the original listener")
	}
}
