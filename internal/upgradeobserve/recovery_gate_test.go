package upgradeobserve

import (
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv3-leaves F9
func TestRecoveryGateRetainsAdmissionUntilCompletion(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRecoveryGate(t)
	}
	testharness.AssertNoGoroutineLeak(t, "recovery gate x5", g)
	testharness.AssertNoFDLeak(t, "recovery gate x5", fd, 0)
}

func exerciseRecoveryGate(t *testing.T) {
	t.Helper()
	for _, defect := range []string{"none", "unsealed", "revision", "producer", "sequence", "scope", "write-failure"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			r := newRegistry(t, f)
			stamp := stampFor(t, f, 1)
			if defect != "unsealed" {
				if _, err := r.Seal(f.attempt, stamp.Nonce); err != nil {
					t.Fatal(err)
				}
			}
			switch defect {
			case "revision":
				stamp.Attempt.Revision--
			case "producer":
				stamp.Producer.PID++
			case "sequence":
				stamp.Sequence++
			case "scope":
				stamp.ScopeDigest = "wrong-scope"
			}
			calls := 0
			err := Reconcile(r, stamp, func(upgradetx.Census) error {
				calls++
				if defect == "write-failure" {
					return unsettled("journal write refused")
				}
				return nil
			})
			if defect == "none" {
				if err != nil || calls != 1 {
					t.Fatalf("valid recovery fence refused: %v", err)
				}
			} else if err == nil || defect != "write-failure" && calls != 0 {
				t.Fatalf("unproven recovery reached the journal: calls=%d err=%v", calls, err)
			}
			if defect != "unsealed" {
				row := f.resources[0]
				row.ID = "new-request"
				if _, err := r.Begin(row); err == nil {
					t.Fatal("reconciliation reopened admission before durable completion")
				}
			}
		})
	}
}
