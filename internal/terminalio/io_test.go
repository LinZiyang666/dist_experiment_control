//go:build linux

// Real: kernel TTY/FIFO pairs, saturated output queues, regular-file offsets,
// /dev/null, socket descriptors, original flags/termios and worker teardown.
// Controlled: poll-stack observations establish blocked I/O before cancellation;
// rescue wakes exist only to let a defective implementation fail without hanging.
package terminalio

import (
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 plan §7.3 / T43 / lead L5
func TestTerminalIOCancellationAndDescriptorOwnership(t *testing.T) {
	// Warm the Go poller before taking the strict descriptor baseline.
	exerciseTerminalIO(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for round := 0; round < rounds; round++ {
		exerciseTerminalIO(t)
	}
	testharness.AssertNoGoroutineLeak(t, "local cancellable terminal I/O", beforeG)
	testharness.AssertNoFDLeak(t, "local cancellable terminal I/O", beforeFD, 0)
}
