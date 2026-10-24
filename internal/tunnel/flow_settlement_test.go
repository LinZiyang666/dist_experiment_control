//go:build linux

package tunnel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Direct production REGISTER/RESUME keeps reconnect scheduling under the test's
// control. Business sockets, TLS, yamux and both logical owners remain real.
type settlementFixture struct {
	server   *Server
	client   *Client
	target   *net.TCPListener
	port     int
	endpoint *flowEndpoint
	peer     *clientEndpoint
}

func settlementSetup(t *testing.T, lookups ...func(AuthRequest) error) *settlementFixture {
	t.Helper()
	return settlementSetupWithDial(t, func(ctx context.Context, f *settlementFixture) error {
		_, _, err := f.client.dialAndRegister(ctx, f.port, "token", "", 0, proto.CertPins{})
		return err
	}, lookups...)
}

func settlementSetupWithDial(t *testing.T, dial func(context.Context, *settlementFixture) error, lookups ...func(AuthRequest) error) *settlementFixture {
	t.Helper()
	f := &settlementFixture{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var err error
	f.target, err = net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.target.Close() })
	reservation, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	f.port = reservation.Addr().(*net.TCPAddr).Port
	// Keep the public port reserved while the control listener and client dial
	// choose ephemeral ports. Transfer it only at the first real REGISTER lookup.
	// origin: hot-update S5 plan §8 / T12 / testing-standards T5
	t.Cleanup(func() { _ = reservation.Close() })
	releasePort := sync.OnceValue(reservation.Close)
	f.server = NewServer("127.0.0.1:0", "127.0.0.1", func(string, string, int, string, int64) error { return nil }, logger)
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "settlement", BootID: "boot", OwnerGen: 1, TokenLookup: func(AuthRequest) error { return nil }, Authorize: func(r AuthRequest) (Authority, error) { return Authority{Enabled: true, Allocation: r.Allocation}, nil }}
	cfg.TokenLookup = func(r AuthRequest) error {
		if err := releasePort(); err != nil {
			return err
		}
		if len(lookups) != 0 {
			return lookups[0](r)
		}
		return nil
	}
	if err = f.server.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	// Test contexts cancel before cleanup. Keep the server's close trigger
	// separate so the explicit cleanup joins every carrier before cancellation
	// can start a competing Close that returns while the first is still active.
	// origin: hot-update S5 plan §8 / DRILL-OBSERVER / CLOSE-LEDGERS race receipt
	serverCtx, cancelServer := context.WithCancel(context.WithoutCancel(t.Context()))
	t.Cleanup(func() { f.server.Close(); cancelServer() })
	if err = f.server.Start(serverCtx); err != nil {
		t.Fatal(err)
	}
	f.server.mu.Lock()
	address := f.server.ln.Addr().String()
	f.server.mu.Unlock()
	f.client = NewClient(address, "settlement-session", "settlement-node", func(int) (int, error) { return f.target.Addr().(*net.TCPAddr).Port, nil }, logger)
	cfg.Budget = &workload.ReplayBudget{}
	if err = f.client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeClientEndpoint(f.client, f.port); _ = f.client.flowState.manager.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), ResumeAttempt)
	defer cancel()
	if err = dial(ctx, f); err != nil {
		t.Fatal(err)
	}
	f.server.mu.Lock()
	f.endpoint = f.server.flowState.endpoints[f.port]
	f.server.mu.Unlock()
	f.client.flowState.mu.Lock()
	f.peer = f.client.flowState.endpoints[f.port]
	f.client.flowState.mu.Unlock()
	if f.endpoint == nil || f.peer == nil {
		t.Fatal("flow carrier was not selected")
	}
	return f
}

func settlementConnection(t *testing.T, f *settlementFixture, seq uint64) (*net.TCPConn, *net.TCPConn, flow.ID) {
	t.Helper()
	public, err := net.DialTCP("tcp4", nil, f.endpoint.listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = public.Close() })
	_ = public.SetDeadline(time.Now().Add(5 * time.Second))
	_ = f.target.SetDeadline(time.Now().Add(5 * time.Second))
	target, err := f.target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	_ = target.SetDeadline(time.Now().Add(5 * time.Second))
	var id flow.ID
	waitEndpoint(t, func() bool {
		for _, state := range flow.States(f.server.flowState.manager, f.endpoint.id) {
			if state.OpenSeq == seq {
				id = state.ID
				return state.OpenStatus == 2
			}
		}
		return false
	})
	return public, target, id
}

func settlementState(t *testing.T, m *flow.Manager, id flow.ID, predicate func(flow.State) bool) flow.State {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		state, changed, err := flow.Observe(m, id)
		if err != nil {
			t.Fatal(err)
		}
		if predicate(state) {
			return state
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("flow transition timed out: %+v", state)
		}
	}
}

// origin: hot-update S5 internal review rv-flow-1
func TestTerminalFlowReceiptsDoNotBreakCarrierResume(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTerminalFlowResume(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal flow resume", beforeG)
	testharness.AssertNoFDLeak(t, "terminal flow resume", beforeFD, 0)
}

func exerciseTerminalFlowResume(t *testing.T) {
	t.Helper()
	t.Run("detached revoke", exerciseDetachedRevokeResume)
	// Exercise EOF before the reverse terminal receipt as well as fully settled
	// history. Otherwise a fast control reader masks the unsafe EOF mutation.
	// origin: hot-update S5 plan §3.3 / T06 — integration mutation replay.
	t.Run("finished DATA EOF", exerciseFinishedDataEOF)
	f := settlementSetup(t)
	for seq := uint64(1); seq <= flow.MaxFlows+1; seq++ {
		public, target, id := settlementConnection(t, f, seq)
		want := []byte(fmt.Sprintf("flow-%02d-distinct-bytes", seq))
		if _, err := public.Write(want); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := io.ReadFull(target, got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("forward bytes: %q %v", got, err)
		}
		if _, err := target.Write(want); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(public, got); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reverse bytes: %q %v", got, err)
		}
		if err := public.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if err := target.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseTerminal })
		settlementState(t, f.client.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseTerminal })
		waitEndpoint(t, func() bool {
			return flow.Retirement(f.server.flowState.manager, f.endpoint.id).RetiredThrough == seq && flow.Retirement(f.client.flowState.manager, f.peer.id).RetiredThrough == seq
		})
		_ = public.Close()
		_ = target.Close()
	}
	waitEndpoint(t, func() bool {
		return f.client.flowState.cfg.Budget.Used() == 0 && f.server.flowState.cfg.Budget.Used() == 0
	})
	if got := f.client.flowState.cfg.Budget.Used(); got != 0 {
		t.Fatalf("settlement retained %d replay bytes", got)
	}
	if r := flow.Retirement(f.client.flowState.manager, f.peer.id); r.RetiredThrough != flow.MaxFlows+1 {
		t.Fatalf("retirement lost completed flows: %+v", r)
	}
	f.client.flowState.mu.Lock()
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	old.Close()
	old.Join()
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatalf("completed flow history broke RESUME: %v", err)
	}
	if got := len(flow.States(f.client.flowState.manager, f.peer.id)); got != 0 {
		t.Fatalf("RESUME live set retained %d settled receipts", got)
	}
	frozen, err := f.client.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := frozen.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	receipts, listed := 0, 0
	for _, rec := range cut.Records {
		fields := clientFields(rec)
		if rec.Kind == sealedstate.KindFlow {
			receipts++
			if fields["phase"].U64 != uint64(flow.FlowPhaseTerminal) || len(fields["raw_ref"].Bytes) != 0 || len(fields["replay_ref"].Bytes) != 0 {
				t.Fatal("settled history retained business resources")
			}
		}
		if rec.Kind == sealedstate.KindEndpoint {
			listed += len(fields["flow_ids"].Bytes) / 16
		}
	}
	if receipts != flow.MaxFlows+1 || listed != receipts {
		t.Fatalf("frozen journal omitted retained terminal receipts: records=%d listed=%d", receipts, listed)
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	// A fresh flow must still use this same healthy endpoint and its next seq.
	public, target, _ := settlementConnection(t, f, flow.MaxFlows+2)
	if _, err := public.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(target, got); err != nil || string(got) != "next" {
		t.Fatalf("next flow: %q %v", got, err)
	}
}

// A revoke after carrier loss has no delivered CLOSE. Its retained receipt
// must refuse the peer's repeated OPEN without tearing down the survivor's
// carrier or creating a replacement target connection.
// origin: hot-update S5 plan §6.5 / §9.1 / T21 / DRILL-RUN 75.R
func exerciseDetachedRevokeResume(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	victim, _, id := settlementConnection(t, f, 1)
	survivor, target, _ := settlementConnection(t, f, 2)
	f.peer.attachment.Close()
	f.peer.attachment.Join()
	if err := f.client.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	local, _, err := flow.Observe(f.client.flowState.manager, id)
	if err != nil {
		t.Fatal(err)
	}
	req := flow.OpenRequest{ID: id, EndpointID: local.EndpointID, OpenSeq: local.OpenSeq, Intent: local.Intent, TargetPort: local.TargetPort}
	for _, defect := range []string{"endpoint", "sequence", "intent", "target", "live"} {
		bad := req
		switch defect {
		case "endpoint":
			bad.EndpointID[0] ^= 1
		case "sequence":
			bad.OpenSeq++
		case "intent":
			bad.Intent[0] ^= 1
		case "target":
			bad.TargetPort++
		case "live":
			live := flow.States(f.client.flowState.manager, f.peer.id)[0]
			bad.ID, bad.OpenSeq, bad.Intent = live.ID, live.OpenSeq, live.Intent
		}
		if _, err := retainedClientOpenReceipt(f.client.flowState.manager, bad); err == nil {
			t.Fatalf("retained refusal accepted %s conflict", defect)
		}
	}
	if state, _, err := flow.Observe(f.server.flowState.manager, id); err != nil || state.Phase == flow.FlowPhaseTerminal {
		t.Fatal("revoke reached the disconnected peer before RESUME", err)
	}
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal("detached revoke broke survivor RESUME", err)
	}
	settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool {
		return s.Phase == flow.FlowPhaseTerminal && s.Reason == flow.ReasonRevoked
	})
	for _, direction := range [][2]io.ReadWriter{{survivor, target}, {target, survivor}} {
		if _, err := direction[0].Write([]byte("original-survivor")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len("original-survivor"))
		if _, err := io.ReadFull(direction[1], got); err != nil || string(got) != "original-survivor" {
			t.Fatal("detached revoke lost survivor bytes", err)
		}
	}
	var b [1]byte
	if n, err := victim.Read(b[:]); n != 0 || err == nil {
		t.Fatal("detached revoke left the original public victim open")
	} else if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		t.Fatal("public victim needed fixture close")
	}
	if f.client.flowState.cfg.Budget.Used() != workload.DefaultReplayCapacity || f.server.flowState.cfg.Budget.Used() != workload.DefaultReplayCapacity {
		t.Fatal("detached revoke retained victim replay")
	}
}

// origin: hot-update S5 internal review rv-flow-V1
func TestReconnectReconcilesLostFinalFINReceipt(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLostFinalFINReceipt(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "lost final FIN receipt", beforeG)
	testharness.AssertNoFDLeak(t, "lost final FIN receipt", beforeFD, 0)
}

func exerciseLostFinalFINReceipt(t *testing.T) {
	t.Helper()
	f := settlementSetup(t)
	public, target, id := settlementConnection(t, f, 1)
	if _, err := public.Write([]byte("final forward")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("final forward"))
	if _, err := io.ReadFull(target, buf); err != nil || string(buf) != "final forward" {
		t.Fatal("forward precondition", err)
	}
	if _, err := target.Write([]byte("last reverse")); err != nil {
		t.Fatal(err)
	}
	buf = make([]byte, len("last reverse"))
	if _, err := io.ReadFull(public, buf); err != nil || string(buf) != "last reverse" {
		t.Fatal("reverse precondition", err)
	}
	f.client.flowState.mu.Lock()
	old := f.peer.attachment
	f.client.flowState.mu.Unlock()
	f.server.mu.Lock()
	brokerCarrier := f.endpoint.attachment
	f.server.mu.Unlock()
	old.Close()
	old.Join()
	brokerCarrier.Close()
	brokerCarrier.Join()
	// The lost-control window is deterministic: original raw pumps record both
	// EOFs, and only one reciprocal FIN receipt is withheld before real RESUME.
	if err := public.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := target.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	broker := settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool { return s.FINFlags&1 != 0 })
	client := settlementState(t, f.client.flowState.manager, id, func(s flow.State) bool { return s.FINFlags&1 != 0 })
	for _, side := range []struct {
		manager *flow.Manager
		gen     uint64
		flags   uint16
		end     uint64
	}{{f.server.flowState.manager, brokerCarrier.Gen, flow.FlagDirection, client.TxEnd}, {f.client.flowState.manager, old.Gen, 0, broker.TxEnd}} {
		if err := flow.Receive(side.manager, flow.Frame{Version: flow.Version, Kind: flow.KindFIN, FlowID: id, CarrierGen: side.gen, Flags: side.flags, Offset: side.end}); err != nil {
			t.Fatal(err)
		}
		settlementState(t, side.manager, id, func(s flow.State) bool { return s.FINFlags&4 != 0 })
	}
	if err := flow.Receive(f.server.flowState.manager, flow.Frame{Version: flow.Version, Kind: flow.KindACK, FlowID: id, CarrierGen: brokerCarrier.Gen, Flags: flow.FlagDirection | flow.FlagFINReceipt, Offset: broker.TxEnd}); err != nil {
		t.Fatal(err)
	}
	settlementState(t, f.server.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseTerminal })
	client = settlementState(t, f.client.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseDraining })
	if client.FINFlags != 7 || f.client.flowState.cfg.Budget.Used() != workload.DefaultReplayCapacity {
		t.Fatal("lost FIN receipt precondition missing")
	}
	if err := resumeClientEndpoint(f.client, f.peer, time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatalf("terminal receipt failed to reconcile: %v", err)
	}
	settlementState(t, f.client.flowState.manager, id, func(s flow.State) bool { return s.Phase == flow.FlowPhaseTerminal })
	// A normally settled journal has no failed OPEN reason. Its replayed
	// OPEN still needs a valid irreversible refusal, never cached success.
	// origin: hot-update S5 plan §3.2 / T05 / DRILL-RUN 75.R
	state, _, err := flow.Observe(f.client.flowState.manager, id)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := retainedClientOpenReceipt(f.client.flowState.manager, flow.OpenRequest{ID: id, EndpointID: state.EndpointID, OpenSeq: state.OpenSeq, Intent: state.Intent, TargetPort: state.TargetPort})
	if err != nil || retained.OpenStatus != 3 || retained.OpenReason != flow.ReasonRetired {
		t.Fatal("settled OPEN replay was not a valid refusal", err)
	}
	if f.client.flowState.cfg.Budget.Used() != 0 {
		t.Fatal("reconciled flow retained replay reservation")
	}
	f.client.flowState.mu.Lock()
	rebuilt := f.peer.attachment
	f.client.flowState.mu.Unlock()
	if rebuilt.Mux.NumStreams() != 1 {
		t.Fatalf("terminal recovery created a data stream: %d", rebuilt.Mux.NumStreams())
	}
}
