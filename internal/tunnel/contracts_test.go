package tunnel

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// Real: frozen signatures and owner sentinels. Stubbed: all S5 IO/state owners.
// Does not exercise sockets, goroutines, image codecs or carriers.
// origin: hot-update S5 plan §2 / §12.2 P0
func TestTunnelFlowContractStubsRejectOperations(t *testing.T) {
	s, c, f := &Server{}, &Client{}, &ClientFrozen{}
	cases := []struct {
		name string
		want error
		call func() []any
	}{
		{"Server.SetFlowConfig", errFlowState, func() []any { a := s.SetFlowConfig(FlowConfig{}); return []any{a} }},
		{"Client.SetFlowConfig", errFlowState, func() []any { a := c.SetFlowConfig(FlowConfig{}); return []any{a} }},
		{"PrepareHandoff", errFlowState, func() []any { a := c.PrepareHandoff(1, [16]byte{}, time.Time{}); return []any{a} }},
		{"FreezeFlows", errFlowState, func() []any { a, b := c.FreezeFlows(time.Time{}); return []any{a, b} }},
		{"Export", sealedstate.ErrInvalid, func() []any { a, b, c := f.Export(); return []any{a, b, c} }},
		{"Thaw", errFlowState, func() []any { a := f.Thaw(); return []any{a} }},
		{"Discard", errFlowState, func() []any { a := f.Discard(); return []any{a} }},
		{"RestoreClient", sealedstate.ErrInvalid, func() []any { a, b, c := RestoreClient(FlowConfig{}, ClientImage{}, nil); return []any{a, b, c} }},
		{"ActivateRaw", errFlowState, func() []any { a := c.ActivateRaw(); return []any{a} }},
		{"ResumeCarriers", errFlowState, func() []any { a := c.ResumeCarriers(time.Time{}); return []any{a} }},
		{"RevokeFlow", errFlowState, func() []any { a := c.RevokeFlow(flow.ID{}, flow.ReasonRevoked); return []any{a} }},
		{"SnapshotState", errFlowState, func() []any { a, b := f.SnapshotState(); return []any{a, b} }},
		{"restoreCutState", errFlowState, func() []any { a, b, c := restoreCutState(FlowConfig{}, sealedstate.CutState{}); return []any{a, b, c} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := tc.call()
			err, ok := values[len(values)-1].(error)
			if !ok || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.want.Error()) {
				t.Fatalf("stub returned %v instead of owner sentinel %v", values[len(values)-1], tc.want)
			}
			for i, value := range values[:len(values)-1] {
				if value != nil && !reflect.ValueOf(value).IsZero() {
					t.Errorf("stub result %d grants ownership or progress: %T", i, value)
				}
			}
		})
	}
}

// origin: hot-update S5 plan §2.1 / §12.2 P0
func TestTunnelFlowCensusStubRejectsAdmission(t *testing.T) {
	if got := (&Client{}).FlowCensus(); got != (Census{Unsupported: true}) {
		t.Fatal("missing owner must refuse census admission")
	}
}

// origin: hot-update S5 plan §12.1
var _ func(FlowConfig, sealedstate.CutState) (*Client, []handoff.Resource, error) = restoreCutState
