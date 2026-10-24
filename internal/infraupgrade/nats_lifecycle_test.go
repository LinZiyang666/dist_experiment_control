package infraupgrade

import (
	"runtime"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The driver owns no background goroutines or descriptors. Exercise the actual
// success, refusal, retained halt and recovery paths five times under both gates.
// origin: hot-update S9 plan §12 P13 / TestNATSDriverLifecycle
func TestNATSDriverLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNATSDriver(t)
	}
	testharness.AssertNoGoroutineLeak(t, "NATS driver", beforeG)
	testharness.AssertNoFDLeak(t, "NATS driver", beforeFD, 0)
}

func exerciseNATSDriver(t *testing.T) {
	t.Helper()
	for _, single := range []bool{false, true} {
		f := newNATSFixture(t, single)
		f.fail["recovery.verify"] = 1
		status, err := f.driver.Advance(f.plan, f.deadline, nil)
		if err == nil || status.Phase != c.Halted {
			t.Fatalf("lifecycle failure: %+v %v", status, err)
		}
		status, err = f.driver.Recover(f.plan, f.deadline, nil)
		if err != nil || status.Phase != c.Completed {
			t.Fatalf("lifecycle recovery: %+v %v", status, err)
		}
		if f.transfersHeld || f.recoveryHeld || f.cohortHeld {
			t.Fatal("lifecycle retained short seals")
		}
		f = newNATSFixture(t, single)
		f.fail["install.outbound.2"] = 1
		if _, err := f.driver.Advance(f.plan, f.deadline, nil); err == nil {
			t.Fatal("missing ACK accepted")
		}
		f.noSignals(t)
	}
}
