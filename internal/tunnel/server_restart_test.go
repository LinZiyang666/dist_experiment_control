package tunnel

import (
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: control-connection-recovery plan, broker-restart evidence correction.
// Destroy the TLS tunnel service, not just one yamux transport. Each successor
// serves the original address/certificate/allocation, while the same client and
// supervisor recover without another Open. Real broker/NATS lifecycle is covered
// separately; this test proves the data-plane recovery contract only.
func TestTunnelClientRecoversAcrossServerRestarts(t *testing.T) {
	h := newReplayTunnelHarness(t)
	h.waitRoundTrip(t)
	before := h.session()
	baselineGoroutines, baselineFDs := runtime.NumGoroutine(), testharness.FDCount()
	const leakExerciseRounds = testharness.LeakExerciseRounds
	for round := 0; round < leakExerciseRounds; round++ {
		registers := h.registers.Load()
		addr := h.addr
		h.serverCancel()
		h.srv.Close()
		// Establish the outage as a precondition: a new public stream must fail.
		if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
			conn, err := net.DialTimeout("tcp", h.publicAddress(), 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
			}
			return err != nil
		}) {
			t.Fatalf("round %d: the destroyed service still accepted public connections", round)
		}
		if h.session() != before {
			t.Fatalf("round %d: a server outage terminated the allocation's supervisor", round)
		}
		h.startServer(t, addr)
		h.waitRoundTrip(t)
		if h.registers.Load() <= registers {
			t.Fatalf("round %d: no REGISTER reached the successor service", round)
		}
		if h.session() != before {
			t.Fatalf("round %d: recovery needed a replacement client session", round)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "tunnel service restart", baselineGoroutines)
	testharness.AssertNoFDLeak(t, "tunnel service restart", baselineFDs, 2)
}
