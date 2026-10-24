//go:build linux

package tunnel

import (
	"bytes"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review PRIVATE-STATE-1
func TestClientCutKeepsTerminalReceiptDecisions(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseClientPrivateReceipts(t)
	}
	testharness.AssertNoGoroutineLeak(t, "client private receipts", g0)
	testharness.AssertNoFDLeak(t, "client private receipts", fd0, 0)
}

func exerciseClientPrivateReceipts(t *testing.T) {
	t.Helper()
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "private-client", BootID: "private-boot", OwnerGen: 1}
	c := NewClient("", "private-session", "private-node", nil, silentLog())
	if err := c.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.flowState.manager.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	m, err := flow.New(flow.Config{Budget: cfg.Budget, Dial: func(_ flow.ID, _ uint16, _ time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		<-stop
		return nil, rawsocket.ErrBusy
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.flowState.manager.Close()
	c.flowState.manager = m
	defer func() {
		if err := m.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	e := &clientEndpoint{id: flow.EndpointID{2}, owner: 1, carrier: 3, target: 22, token: "private-token", allocation: Allocation{SID: "private-session", NID: "private-node", Port: 12345, TokenHash: hashToken("private-token"), Epoch: 1}, stop: make(chan struct{})}
	e.permit = &flowPermit{owner: 1, next: 2, requestID: [16]byte{6}, cutID: [16]byte{5}, deadline: time.Now().Add(time.Minute)}
	c.flowState.endpoints[12345] = e
	id := flow.ID{4}
	intent := flow.OpenIntent(e.id, id, 1, 22)
	if _, err := flow.Open(m, flow.OpenRequest{ID: id, EndpointID: e.id, OpenSeq: 1, Intent: intent, TargetPort: 22, OwnerGen: 1, CarrierGen: 3}, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.RevokeFlow(id, flow.ReasonRevoked); err != nil {
		t.Fatal(err)
	}
	f, err := c.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Discard(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	var receipt, journal map[string]sealedstate.Field
	for _, rec := range cut.Records {
		if rec.Kind == sealedstate.KindReceipt {
			receipt = clientFields(rec)
		}
		if rec.Kind == sealedstate.KindFlow {
			journal = clientFields(rec)
		}
	}
	if receipt == nil {
		t.Fatal("runtime cut omitted terminal receipt")
	}
	if !bytes.Equal(receipt["flow_id"].Bytes, id[:]) || !bytes.Equal(receipt["intent"].Bytes, intent[:]) || receipt["verdict"].U64 != uint64(flow.ResumeVerdictRejected) || receipt["reason"].U64 != uint64(flow.ReasonRevoked) {
		t.Fatal("receipt changed identity, intent or verdict")
	}
	for _, name := range []string{"close_mode", "fin_flags", "final_applied", "final_end", "open_status", "revoked"} {
		if !reflect.DeepEqual(receipt[name], journal[name]) {
			t.Fatalf("receipt changed %s", name)
		}
	}
	encoded, err := encodeClientCut(cut)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeClientCut(encoded)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := restoreClientRecords(cfg, decoded)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := next.flowState.manager.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := next.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	state, _, err := flow.Observe(next.flowState.manager, id)
	if err != nil || state.Phase != flow.FlowPhaseTerminal || state.Intent != intent {
		t.Fatal("restore lost the terminal journal")
	}
	// A terminal journal is retained across later owner generations even when
	// RESUME no longer lists it. Its original receipt identity must survive.
	e.owner, e.carrier, e.permit.consumed = 2, 4, true
	later, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeClientCut(later); err != nil {
		t.Fatalf("later owner lost predecessor receipt: %v", err)
	}
	for _, kind := range []string{sealedstate.KindReceipt, sealedstate.KindEndpoint, sealedstate.KindPermit} {
		for _, rec := range decoded.Records {
			if rec.Kind != kind {
				continue
			}
			for _, field := range rec.Fields {
				if kind == sealedstate.KindPermit && field.Name != "purpose" && field.Name != "resources_digest" {
					continue
				}
				if kind == sealedstate.KindEndpoint && !privateClientEndpointGuard(field.Name) {
					continue
				}
				bad, err := decodeClientCut(encoded)
				if err != nil {
					t.Fatal(err)
				}
				for i := range bad.Records {
					if bad.Records[i].Kind != kind {
						continue
					}
					for j := range bad.Records[i].Fields {
						v := &bad.Records[i].Fields[j]
						if v.Name != field.Name {
							continue
						}
						if v.Type <= sealedstate.FieldBool {
							v.U64 ^= 1
						} else if len(v.Bytes) > 0 {
							v.Bytes[0] ^= 1
						} else {
							v.Bytes = []byte{1}
						}
					}
				}
				accepted, _, err := restoreClientRecords(cfg, bad)
				if accepted != nil {
					_ = accepted.flowState.manager.Close()
				}
				if err == nil {
					t.Fatalf("restore ignored %s.%s", kind, field.Name)
				}
			}
		}
	}
}

func privateClientEndpointGuard(name string) bool {
	for _, guarded := range []string{"cleanup_pending", "closed_allocation", "fence_allocation_gen", "fence_port_gen", "fence_session_gen", "flow_ids", "forgotten", "listener_ref", "pending_raw_refs", "permit_id", "phase", "ready_latch", "ready_latch_deadline_boot_ns", "receipt_ids"} {
		if name == guarded {
			return true
		}
	}
	return false
}
