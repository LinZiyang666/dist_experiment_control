package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestDiskMonitorHonorsConfiguredInterval (#39) proves the operator knob actually changes the
// disk-pressure sampling cadence: a small configured interval fires the probe many times in the
// window, whereas the default (5m) fires exactly once (the startup tick). Both cases share the same
// harness, so the delta isolates the interval as the cause.
func TestDiskMonitorHonorsConfiguredInterval(t *testing.T) {
	// Custom interval: expect several probes in the window.
	var customCalls atomic.Int32
	customB := &Broker{cfg: Config{
		Logger: silentLogger(), Now: time.Now, StoreDir: t.TempDir(),
		DiskCheckInterval: 20 * time.Millisecond,
		DiskUsageFn:       func(string) (uint64, uint64, error) { customCalls.Add(1); return 1, 100, nil },
	}}
	ctxC, cancelC := context.WithCancel(context.Background())
	customB.startDiskMonitor(ctxC)
	time.Sleep(220 * time.Millisecond)
	cancelC()
	if got := customCalls.Load(); got < 4 {
		t.Errorf("20ms interval: probe called %d times in 220ms — the configured interval did not take effect (a 5m default would be 1)", got)
	}

	// Default (unset ⇒ 0 ⇒ 5m): expect exactly the single startup tick in the same window.
	var defCalls atomic.Int32
	defB := &Broker{cfg: Config{
		Logger: silentLogger(), Now: time.Now, StoreDir: t.TempDir(),
		DiskCheckInterval: 0, // built-in default (5m)
		DiskUsageFn:       func(string) (uint64, uint64, error) { defCalls.Add(1); return 1, 100, nil },
	}}
	ctxD, cancelD := context.WithCancel(context.Background())
	defB.startDiskMonitor(ctxD)
	time.Sleep(220 * time.Millisecond)
	cancelD()
	if got := defCalls.Load(); got != 1 {
		t.Errorf("default interval: probe called %d times in 220ms, want exactly 1 (the 5m default must NOT sample again in the window)", got)
	}
}
