package upgraderoll

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review ROLL-SETTLEMENT F3; hot-update S8 plan §3.1
func TestAcceptedArmingResultQueriesTheOriginalParentAndChild(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAcceptedArmingSettlement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "accepted arming settlement x5", g)
	testharness.AssertNoFDLeak(t, "accepted arming settlement x5", fds, 0)
}

func exerciseAcceptedArmingSettlement(t *testing.T) {
	t.Helper()
	m, endpoint := newRollModel(t)
	parent := m.plan.Parent
	m.startHook = func(upgradetx.Step) error {
		return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "broker store cut was not armed", Accepted: true}
	}
	view, err := Run(t.Context(), m, endpoint, m.plan)
	if err != nil || view.Historical.Forward.Phase != upgradetx.PhaseCompleted || !upgradetx.SameCut(view.Historical.Forward.Handle, parent) {
		t.Fatalf("accepted arming result abandoned the original parent: %v", err)
	}
	for _, step := range m.plan.Steps {
		if isCut(step) && m.starts[step.Grant.Handle.TxID] != 1 {
			t.Fatal("settlement reissued an original child Start")
		}
	}
}
