package flow

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// Real: frozen signatures and owner sentinels. Stubbed: all S5 IO/state owners.
// Does not exercise sockets, goroutines, image codecs or carriers.
// origin: hot-update S5 plan §2 / §12.2 P0
func TestFlowContractStubsRejectOperations(t *testing.T) {
	m, f := &Manager{}, &Frozen{}
	cases := []struct {
		name string
		want error
		call func() []any
	}{
		{"New", &Failure{Reason: ReasonCapacity}, func() []any { a, b := New(Config{}); return []any{a, b} }},
		{"Freeze", ErrClosed, func() []any { a, b := m.Freeze(time.Time{}); return []any{a, b} }},
		{"Thaw", ErrClosed, func() []any { a := f.Thaw(); return []any{a} }},
		{"Discard", ErrClosed, func() []any { a := f.Discard(); return []any{a} }},
		{"ActivateRaw", ErrClosed, func() []any { a := m.ActivateRaw(); return []any{a} }},
		{"RevokeFlow", ErrClosed, func() []any { a := m.RevokeFlow(ID{}, ReasonRevoked); return []any{a} }},
		{"Close", ErrClosed, func() []any { a := m.Close(); return []any{a} }},
		{"SnapshotState", ErrClosed, func() []any { a, b := f.SnapshotState(); return []any{a, b} }},
		{"restoreCutState", &Failure{Reason: ReasonCapacity}, func() []any { a, b, c := restoreCutState(Config{}, sealedstate.CutState{}); return []any{a, b, c} }},
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

// origin: hot-update S5 plan §12.1
var _ func(Config, sealedstate.CutState) (*Manager, []handoff.Resource, error) = restoreCutState
