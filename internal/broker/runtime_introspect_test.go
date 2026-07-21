package broker

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestRuntimeSnapshotReportsRealProcessValues proves the snapshot returns THIS process's live
// numbers, not zeros/placeholders: a live goroutine count, an OS thread count, and (on Linux) an fd
// count and an RSS. It also proves uptime is measured from bootAt via the injectable clock.
func TestRuntimeSnapshotReportsRealProcessValues(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: func() time.Time { return base.Add(42 * time.Second) }}}
	b.bootAt = base

	snap := b.runtimeSnapshot()
	if snap == nil {
		t.Fatal("nil snapshot")
	}
	if snap.Schema != "admin_runtime" || snap.SchemaVersion != 1 {
		t.Errorf("schema contract: got %q v%d", snap.Schema, snap.SchemaVersion)
	}
	// Goroutines is the in-process truth. A real Go process ALWAYS has >1 live goroutine.
	if got, live := snap.Goroutines, runtime.NumGoroutine(); got < 1 || abs(got-live) > 8 {
		t.Errorf("goroutines=%d not near live NumGoroutine=%d", got, live)
	}
	if snap.Threads < 1 {
		t.Errorf("threads=%d, want >=1 (an M always exists)", snap.Threads)
	}
	// On Linux (the v1 target) fds + rss are measurable and positive; -1 only on a platform without
	// /proc/self. The CI + drill hosts are Linux, so assert the real values there.
	if runtime.GOOS == "linux" {
		if snap.OpenFDs < 1 {
			t.Errorf("open_fds=%d, want >=1 on linux", snap.OpenFDs)
		}
		if snap.RSSBytes < 1 {
			t.Errorf("rss_bytes=%d, want >0 on linux", snap.RSSBytes)
		}
	}
	if snap.UptimeSeconds != 42 {
		t.Errorf("uptime=%v, want 42 (now-bootAt via injectable clock)", snap.UptimeSeconds)
	}
}

// TestRuntimeSnapshotGoroutinesTrackNumGoroutineNotThreads is the MUTATION-VERIFICATION guard for
// the roadmap's hard rule "NumGoroutine, never Threads-as-a-proxy". It spawns N goroutines that PARK
// (blocked on a channel). Parked goroutines add ~0 OS threads, so:
//   - snap.Goroutines MUST jump by ~N  (it reads runtime.NumGoroutine)
//   - snap.Threads MUST NOT jump by N  (parked goroutines create no OS threads)
//
// If runtime_introspect.go were mutated to report the OS thread count as `Goroutines` (the forbidden
// proxy), the first assertion fails: the count would stay flat while N goroutines leak. That is
// exactly the 10k-leaked-goroutines / 0-thread-growth scenario the rule exists for.
func TestRuntimeSnapshotGoroutinesTrackNumGoroutineNotThreads(t *testing.T) {
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now}}
	b.bootAt = time.Now()

	before := b.runtimeSnapshot()

	const n = 300
	release := make(chan struct{})
	var wg sync.WaitGroup
	baseline := runtime.NumGoroutine()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-release }()
	}
	// Wait until all n are actually parked and counted by the scheduler.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() < baseline+n && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	after := b.runtimeSnapshot()
	close(release)
	wg.Wait()

	if d := after.Goroutines - before.Goroutines; d < n-20 {
		t.Errorf("goroutines rose by %d after spawning %d parked goroutines — want ~%d. "+
			"A small delta means the snapshot is NOT reading runtime.NumGoroutine (a Threads proxy "+
			"would stay flat while goroutines leak — the exact forbidden mutation).", d, n, n)
	}
	// Sanity that the two domains genuinely diverged: threads did not track the goroutine spawn.
	if d := after.Threads - before.Threads; d >= n {
		t.Errorf("threads rose by %d (>= %d) — parked goroutines should not create OS threads; "+
			"the test's premise (goroutines != threads) is broken on this runtime", d, n)
	}
}

// TestRuntimeSnapshotProjectsReconcilerLastTick proves the snapshot projects the R7 registry's
// per-pass last-tick, and that last-tick ADVANCES when a pass actually fires — the "stalled
// reconciler" signal. A pass that never comes due reports a zero LastTick ("never").
func TestRuntimeSnapshotProjectsReconcilerLastTick(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: func() time.Time { return base }}}
	b.bootAt = base

	reg := newReconcileRegistry(silentLogger(), nil)
	var fired int
	reg.register("fast", time.Second, false, func(context.Context, time.Time) error { fired++; return nil })
	reg.register("slow", time.Hour, false, func(context.Context, time.Time) error { return nil })
	reg.start(base)
	b.reconcilers = reg

	// Before any tick, both report LastTick zero (never invoked).
	snap := b.runtimeSnapshot()
	byName := map[string]int{}
	for i, r := range snap.Reconcilers {
		byName[r.Name] = i
		if !r.LastTick.IsZero() {
			t.Errorf("%s LastTick should be zero before any tick, got %v", r.Name, r.LastTick)
		}
	}
	if _, ok := byName["fast"]; !ok {
		t.Fatalf("snapshot missing 'fast' reconciler; got %+v", snap.Reconcilers)
	}
	if snap.Reconcilers[byName["fast"]].IntervalMS != 1000 {
		t.Errorf("fast interval_ms=%d, want 1000", snap.Reconcilers[byName["fast"]].IntervalMS)
	}

	// Drive the fast pass past its deadline; the slow pass stays un-fired.
	tick := base.Add(time.Second)
	reg.runDue(context.Background(), tick)

	snap = b.runtimeSnapshot()
	fastRow := snap.Reconcilers[byName["fast"]]
	if !fastRow.LastTick.Equal(tick) {
		t.Errorf("fast LastTick=%v, want %v (advanced when the pass fired)", fastRow.LastTick, tick)
	}
	if fastRow.Runs != 1 {
		t.Errorf("fast Runs=%d, want 1", fastRow.Runs)
	}
	if slowRow := snap.Reconcilers[byName["slow"]]; !slowRow.LastTick.IsZero() {
		t.Errorf("slow LastTick=%v, want zero (never came due) — a stalled pass must read 'never'", slowRow.LastTick)
	}
}

// TestRuntimeSnapshotNilRegistryIsSafe: an admin call landing in the sliver of Run before the
// registry is wired must return an empty reconciler list, never panic.
func TestRuntimeSnapshotNilRegistryIsSafe(t *testing.T) {
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now}}
	snap := b.runtimeSnapshot()
	if snap == nil {
		t.Fatal("nil snapshot")
	}
	if len(snap.Reconcilers) != 0 {
		t.Errorf("nil registry should yield no reconcilers, got %d", len(snap.Reconcilers))
	}
}

// TestOpenFDCountDoesNotLeak: the fd probe must CLOSE its own directory handle — a leak detector
// that leaks an fd per call is its own worst bug. Call it many times; the process fd count must not
// grow monotonically.
func TestOpenFDCountDoesNotLeak(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("open fd count is Linux-only")
	}
	first := openFDCount()
	if first < 0 {
		t.Skip("/proc/self/fd unreadable")
	}
	for i := 0; i < 200; i++ {
		_ = openFDCount()
	}
	last := openFDCount()
	if last > first+2 {
		t.Errorf("openFDCount leaked fds: first=%d last=%d after 200 calls", first, last)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
