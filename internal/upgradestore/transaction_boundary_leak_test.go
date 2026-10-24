package upgradestore_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv3-leaves F3 F4 F5 F9
func TestTransactionBoundaryLeakCycles(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransactionBoundary(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transaction boundaries x5", g)
	testharness.AssertNoFDLeak(t, "transaction boundaries x5", fd, 0)
}

func exerciseTransactionBoundary(t *testing.T) {
	t.Helper()
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			s := &admissionAfterControlStore{Store: r.store, runtime: r}
			v, err := boundaryEndpoint(s, r, r).Start(t.Context(), r.grant, r.receipt)
			if err != nil || !s.fired || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
				t.Fatalf("boundary cycle did not complete its admitted resources: %v", err)
			}
		})
	}
}
