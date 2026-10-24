//go:build linux

package agentimage

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real: CLOCK_BOOTTIME and the composition's receipt gate. Domain success
// receipts are explicit fixture state here, not simulated carrier/SS owners;
// no network-continuity or SS authorization claim comes from this unit test.
// origin: hot-update S5 plan §7.1 / TestDomainCompleteActivationWindow
func TestDomainCompleteActivationWindow(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseDomainCompleteActivationWindow(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "domain-complete-window", beforeG)
	testharness.AssertNoFDLeak(t, "domain-complete-window", beforeFD, 0)
}

func exerciseDomainCompleteActivationWindow(t *testing.T) {
	start, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	// Exhaust all readiness combinations, including retained-but-idle domains.
	// Checking only raw-ready would publish a successful cut before permission.
	for mask := 0; mask < 8; mask++ {
		flow, ss := flowSSMetadata()
		imp := &Imported{Bundle: Bundle{Flow: flow, SS: ss}, FrozenAt: start, thawed: true,
			rawReady: mask&1 != 0, carrierReady: mask&2 != 0, ssReady: mask&4 != 0}
		finishActivation(imp)
		if mask != 7 {
			if imp.FreezeWindow() != 0 || imp.thawedAt != 0 || imp.ActivationComplete() {
				t.Fatalf("window ended before all domains were ready: mask=%d", mask)
			}
		} else {
			if imp.FreezeWindow() <= 0 || !imp.ActivationComplete() {
				t.Fatal("complete domain activation did not end the window")
			}
			ended := imp.thawedAt
			finishActivation(imp)
			if imp.thawedAt != ended {
				t.Fatal("later NATS/control readiness moved the data-plane endpoint")
			}
		}
	}
	// Exec/PTY-only imports preserve their existing local-thaw endpoint.
	local := &Imported{FrozenAt: start, thawed: true, rawReady: true}
	finishActivation(local)
	if local.FreezeWindow() <= 0 {
		t.Fatal("local-only activation never completed")
	}
	// Public operations cannot mark domain success before raw activation.
	incomplete := &Imported{}
	if err := incomplete.ResumeCarriers(time.Now().Add(bundleTestBudget)); err == nil {
		t.Fatal("carrier completion accepted before raw activation")
	}
	if err := incomplete.AuthorizeSS(zeroSSGrant()); err == nil {
		t.Fatal("SS grant accepted before raw activation")
	}
}
