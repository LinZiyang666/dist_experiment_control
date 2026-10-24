//go:build linux

package cli_e2e_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// The short AUTH_TIMEOUT belongs to the injected reconnect black window.
// Applying it to initial PIN verification can abort the fixture before the
// original agent is online, especially under the race matrix's CPU limit.
// origin: S10 internal review POLISH-B F3 / plan §11.1
func TestCalloutFaultTimeoutStartsAfterBootstrap(t *testing.T) {
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCalloutFaultTimeout(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "callout fault timeout", g)
	testharness.AssertNoFDLeak(t, "callout fault timeout", fd, 0)
}

func exerciseCalloutFaultTimeout(t *testing.T) {
	t.Helper()
	const faultTimeout = 0.5
	s := newRealStackFastAuthTimeout(t, faultTimeout)
	initial, err := s.server().Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if initial.AuthTimeout != natsserver.AUTH_TIMEOUT.Seconds() {
		t.Fatalf("bootstrap inherited fault AUTH_TIMEOUT=%g, want product default %g", initial.AuthTimeout, natsserver.AUTH_TIMEOUT.Seconds())
	}
	if s.authTimeout != faultTimeout {
		t.Fatal("reconnect fault timeout was not retained")
	}
	s.restartNATS(0)
	restarted, err := s.server().Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.AuthTimeout != faultTimeout {
		t.Fatalf("restarted server AUTH_TIMEOUT=%g, want injected %g", restarted.AuthTimeout, faultTimeout)
	}
}
