//go:build linux

package tunnel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review rv-image-2
func TestClientImageRetainsTerminalReceiptsBeyondActiveLimit(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseClientTerminalCapacity(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "client terminal capacity", beforeG)
	testharness.AssertNoFDLeak(t, "client terminal capacity", beforeFD, 0)
}

func exerciseClientTerminalCapacity(t *testing.T) {
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "capacity-client", BootID: "capacity-boot", OwnerGen: 1}
	c := NewClient("", "capacity-session", "capacity-node", nil, silentLog())
	if err := c.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(c.flowState.manager.Close)
	m, err := flow.New(flow.Config{Budget: cfg.Budget, MaxFlows: 1, Dial: func(_ flow.ID, _ uint16, _ time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		<-stop
		return nil, rawsocket.ErrBusy
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.flowState.manager.Close()
	c.flowState.manager = m
	defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
	ep := &clientEndpoint{id: flow.EndpointID{2}, owner: 1, carrier: 1, target: 22, token: "capacity-token", allocation: Allocation{SID: "capacity-session", NID: "capacity-node", Port: 12345, TokenHash: hashToken("capacity-token"), Epoch: 1}, stop: make(chan struct{})}
	c.flowState.endpoints[12345] = ep
	for seq := 1; seq <= 33; seq++ {
		id := flow.ID{1}
		binary.BigEndian.PutUint64(id[8:], uint64(seq))
		req := flow.OpenRequest{ID: id, EndpointID: ep.id, OpenSeq: uint64(seq), TargetPort: 22, OwnerGen: 1, CarrierGen: 1}
		req.Intent = flow.OpenIntent(ep.id, id, req.OpenSeq, 22)
		if _, err := flow.Open(m, req, nil); err != nil {
			t.Fatal(err)
		}
		if err := m.RevokeFlow(id, flow.ReasonRevoked); err != nil {
			t.Fatal(err)
		}
	}
	frozen, err := c.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	im, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatalf("33 terminal receipts refused by client exporter: %v", err)
	}
	cut, err := frozen.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	exerciseClientCodecCapacity(t, cut, ep.id)
	if len(im.Flows.Records) != 33 || len(im.Endpoints[0].FlowIDs) != 33 || im.Flows.ReservedBytes != 0 {
		t.Fatal("client projection lost terminal proof")
	}
	source := handofftest.DupResources(t, snapshot.Resources)
	restored, salvage, err := RestoreClient(cfg, im, source)
	if err != nil {
		closeClientImageResources(salvage)
		t.Fatal(err)
	}
	resources, err := restored.Detach()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClientImageResources(resources)
	if len(resources) != 1 || resources[0].ID != im.StateRef {
		t.Fatal("terminal-only client lost original sealed state")
	}
}

// Constructed records extend the real terminal snapshot to the node quota.
// The expectation counts flow phases independently of endpoint/retirement records.
func exerciseClientCodecCapacity(t *testing.T, original sealedstate.CutState, endpoint flow.EndpointID) {
	t.Helper()
	var prototype sealedstate.Record
	for _, rec := range original.Records {
		if rec.Kind == sealedstate.KindFlow {
			prototype = rec
			break
		}
	}
	for _, tc := range []struct {
		count  int
		active bool
		valid  bool
	}{{flow.MaxTerminalMetadata, false, true}, {flow.MaxTerminalMetadata + 1, false, false}, {flow.MaxFlows + 1, true, false}} {
		var cut sealedstate.CutState
		var ids, receiptIDs []byte
		for i := 1; i <= tc.count; i++ {
			id := flow.ID{1}
			binary.BigEndian.PutUint64(id[8:], uint64(i))
			fields := clientFields(prototype)
			fields["id"] = endpointBytes(id[:])
			fields["open_seq"] = endpointNum(uint64(i))
			intent := flow.OpenIntent(endpoint, id, uint64(i), 22)
			fields["intent"] = endpointBytes(intent[:])
			if tc.active {
				fields["phase"] = endpointNum(uint64(flow.FlowPhaseOpening))
				ref := fmt.Sprintf("flow.%x", id)
				fields["replay_ref"] = endpointText(ref)
				cut.Resources = append(cut.Resources, handoff.Resource{ID: ref, Kind: "flow-v1", Files: map[string]*os.File{"replay": nil}})
			}
			rec, err := clientPrivateRecord(sealedstate.KindFlow, fields)
			if err != nil {
				t.Fatal(err)
			}
			cut.Records = append(cut.Records, rec)
			if !tc.active {
				e := &clientEndpoint{id: endpoint}
				receipt, err := clientTerminalReceipt(e, clientFields(rec))
				if err != nil {
					t.Fatal(err)
				}
				cut.Records = append(cut.Records, receipt)
				receiptIDs = append(receiptIDs, id[:]...)
			}
			ids = append(ids, id[:]...)
		}
		for _, rec := range original.Records {
			if rec.Kind == sealedstate.KindFlow || rec.Kind == sealedstate.KindReceipt {
				continue
			}
			fields := clientFields(rec)
			if rec.Kind == sealedstate.KindEndpoint {
				fields["flow_ids"] = endpointBytes(ids)
				fields["receipt_ids"] = endpointBytes(receiptIDs)
			}
			if rec.Kind == sealedstate.KindRetirement {
				fields["next_open_seq"] = endpointNum(uint64(tc.count + 1))
				through := tc.count
				if tc.active {
					through = 0
				}
				fields["retired_through"] = endpointNum(uint64(through))
			}
			updated, err := clientPrivateRecord(rec.Kind, fields)
			if err != nil {
				t.Fatal(err)
			}
			cut.Records = append(cut.Records, updated)
		}
		encoded, err := encodeClientCut(cut)
		if tc.valid {
			if err != nil {
				t.Fatal(err)
			}
			restored, err := decodeClientCut(encoded)
			if err != nil || len(restored.Records) != 2*tc.count+2 {
				t.Fatalf("full terminal quota lost in client codec: %v", err)
			}
		} else if !errors.Is(err, sealedstate.ErrCapacity) {
			t.Fatalf("client quota overflow count=%d active=%v: %v", tc.count, tc.active, err)
		}
	}
}
