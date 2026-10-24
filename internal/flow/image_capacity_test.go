//go:build linux

package flow

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review rv-image-2
func TestFlowImageRetainsTerminalReceiptsBeyondActiveLimit(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTerminalImageCapacity(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal image capacity", beforeG)
	testharness.AssertNoFDLeak(t, "terminal image capacity", beforeFD, 0)
}

func exerciseTerminalImageCapacity(t *testing.T) {
	for _, count := range []int{33, MaxTerminalMetadata} {
		budget := &workload.ReplayBudget{}
		cfg := Config{Budget: budget, MaxFlows: 1, Dial: func(_ ID, _ uint16, _ time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
			<-stop
			return nil, rawsocket.ErrBusy
		}}
		m, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(m.Close)
		for seq := 1; seq <= count; seq++ {
			id := ID{1}
			binary.BigEndian.PutUint64(id[8:], uint64(seq))
			req := OpenRequest{ID: id, EndpointID: EndpointID{2}, OpenSeq: uint64(seq), TargetPort: 22, OwnerGen: 1, CarrierGen: 1}
			req.Intent = OpenIntent(req.EndpointID, id, req.OpenSeq, req.TargetPort)
			if _, err := Open(m, req, nil); err != nil {
				t.Fatal(err)
			}
			if err := m.RevokeFlow(id, ReasonRevoked); err != nil {
				t.Fatal(err)
			}
		}
		if budget.Used() != 0 {
			t.Fatal("terminal receipts retain replay reservation")
		}
		f, err := m.Freeze(time.Now().Add(3 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(f.Discard)
		im, snapshot, err := f.Export()
		if err != nil {
			t.Fatalf("%d terminal receipts exceed active limit: %v", count, err)
		}
		if len(im.Records) != count || im.ReservedBytes != 0 || im.Retirement[0].RetiredThrough != uint64(count) {
			t.Fatal("export lost terminal or retirement proof")
		}
		src := handofftest.DupResources(t, snapshot.Resources)
		restored, salvage, err := Restore(cfg, im, src)
		if err != nil {
			for _, r := range salvage {
				for _, file := range r.Files {
					_ = file.Close()
				}
			}
			t.Fatalf("restore terminal receipts at original active limit: %v", err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(restored.Close)
		// Capacity is independent in both the private codec and public metadata.
		cut, err := f.SnapshotState()
		if err != nil {
			t.Fatal(err)
		}
		overflow := cut.Records[0]
		overflow.Fields = append([]sealedstate.Field(nil), overflow.Fields...)
		id := ID{3}
		binary.BigEndian.PutUint64(id[8:], uint64(count+1))
		setPrivateField(&overflow, "id", sealedstate.Field{Bytes: id[:]})
		if count == MaxTerminalMetadata {
			if _, err := encodeFlowState(append(cut.Records, overflow)); !errors.Is(err, sealedstate.ErrCapacity) {
				t.Fatalf("terminal overflow accepted: %v", err)
			}
		}
	}
}

// origin: hot-update S5 internal review rv-image-2
func TestFlowImageCapacityRejectsActiveAndTerminalOverflow(t *testing.T) {
	fixture := flowImageFixture(t)
	for _, tc := range []struct {
		count           int
		terminal, valid bool
	}{{MaxFlows, false, true}, {MaxFlows + 1, false, false}, {MaxTerminalMetadata, true, true}, {MaxTerminalMetadata + 1, true, false}} {
		var records []sealedstate.Record
		for i := 0; i < tc.count; i++ {
			rec := fixture[0]
			rec.Fields = append([]sealedstate.Field(nil), rec.Fields...)
			id := ID{1}
			binary.BigEndian.PutUint64(id[8:], uint64(i+1))
			setPrivateField(&rec, "id", sealedstate.Field{Bytes: id[:]})
			setPrivateField(&rec, "replay_ref", sealedstate.Field{Bytes: []byte(flowResourceRef(id))})
			if tc.terminal {
				setPrivateField(&rec, "replay_ref", sealedstate.Field{})
				setPrivateField(&rec, "phase", sealedstate.Field{U64: uint64(FlowPhaseTerminal)})
			}
			records = append(records, rec)
		}
		_, checkedErr := checkedFlowRecords(records)
		if tc.valid && checkedErr != nil || !tc.valid && !errors.Is(checkedErr, sealedstate.ErrCapacity) {
			t.Fatalf("private record quota count=%d terminal=%v: %v", tc.count, tc.terminal, checkedErr)
		}
		data, err := encodeFlowState(records)
		if (err == nil) != tc.valid {
			t.Fatalf("private capacity count=%d terminal=%v: %v", tc.count, tc.terminal, err)
		}
		if tc.valid {
			if _, err := decodeFlowState(data); err != nil {
				t.Fatal(err)
			}
		}
		im := Image{Version: Version, StateRef: "flow-state.00000000000000000000000000000001"}
		for i := 0; i < tc.count; i++ {
			id := ID{1}
			binary.BigEndian.PutUint64(id[8:], uint64(i+1))
			phase := FlowPhaseOpening
			if tc.terminal {
				phase = FlowPhaseTerminal
			}
			im.Records = append(im.Records, RecordImage{ID: id, OpenSeq: uint64(i + 1), Phase: phase})
		}
		if err := validatePublicImage(im); (err == nil) != tc.valid {
			t.Fatalf("public capacity count=%d terminal=%v: %v", tc.count, tc.terminal, err)
		}
	}
}
