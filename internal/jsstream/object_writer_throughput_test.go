//go:build !race

package jsstream

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Race instrumentation changes hashing and raw receipt readback costs. This
// separate matrix unit measures the product floor with all T09 correctness
// assertions; the race variant still checks every receipt and control budget.
// origin: hot-update S7 plan §2.2 / T09 / INTEG-E lead throughput decision
// origin: S7 internal review MAKETEST-TRANSFER F10
func TestTransferWriterThroughputAt100msWSSRTT(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) { exerciseWriterRTT(t, 4*(1<<20)) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "writer RTT", goroutines)
	testharness.AssertNoFDLeak(t, "writer RTT", fds, 0)
}
