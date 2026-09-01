// spawnsafe_stress_test.go — the spawnsafe concurrency gate mandated by
// docs/reviews/remote-fs-resilience-plan.md §6 and flagged missing by the
// internal review (M5). Hammers Policy.Prepare + RunStart from many goroutines
// while the mount table flips (generation change) under them, asserting no
// goroutine leak via the repo's count-based assertNoGoroutineLeak. Run under
// -race it also exercises the lock-drop self-heal probe + wedged-slot accounting
// for data races. Counter-part of the unit tests in internal/spawnsafe — this
// puts the primitives under sustained cross-goroutine pressure.
package concurrency_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/spawnsafe"
)

func mountinfoLines(entries ...[2]string) []byte {
	var b []byte
	for i, e := range entries {
		b = append(b, []byte(fmt.Sprintf("%d 35 0:%d / %s rw - %s src rw\n", 36+i, i, e[0], e[1]))...)
	}
	return b
}

func TestSpawnsafePolicy_concurrentGenSwap(t *testing.T) {
	before := runtime.NumGoroutine()

	// Two mount-table snapshots that differ ⇒ refreshIfChanged re-snapshots and
	// resets the health cache when the flag flips, racing the probe state machine.
	genA := mountinfoLines([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"})
	genB := mountinfoLines([2]string{"/nfs", "nfs"}, [2]string{"/other", "nfs"}, [2]string{"/", "ext4"})
	var useB atomic.Bool
	src := func() ([]byte, error) {
		if useB.Load() {
			return genB, nil
		}
		return genA, nil
	}
	// Probe flaps healthy/unhealthy to drive the sticky/self-heal transitions.
	// Every fourth probe is also SLOW-but-healthy: it answers true only after the
	// deadline, so the launcher demotes it and a later drain heals it. That
	// oscillation is the only state in which a healthy verdict can be re-armed
	// while a probe from the previous generation is still landing — i.e. the state
	// gotcha #81's fix introduced (origin: docs/deploy-tier-gotchas.md #81).
	var probes atomic.Int64
	probe := func(string) bool {
		n := probes.Add(1)
		if n%4 == 0 {
			time.Sleep(8 * time.Millisecond) // outlives ProbeTimeout below
			return true
		}
		return n%2 == 0
	}

	// The health TTL is driven by an injected clock rather than wall time so the
	// re-validation path is exercised deterministically at this test's timescale.
	var clockNS atomic.Int64
	clockNS.Store(time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clockNS.Load()) }

	p, err := spawnsafe.New(spawnsafe.Config{
		Mode: spawnsafe.ModeAuto, ProbeTimeout: 5 * time.Millisecond, WedgeCeiling: 8,
		HealthTTL: time.Minute, MountSource: src, Probe: probe, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // clock driver: steps across the TTL so T11 re-arms mid-flight
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clockNS.Add(int64(20 * time.Second))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Add(1)
	go func() { // evidence-driven invalidation, concurrent with everything else
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				p.InvalidateHealthy()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Add(1)
	go func() { // mount-table flipper (generation churn)
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				useB.Store(!useB.Load())
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < 12; i++ { // workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			env := []string{"PATH=/nfs/bin:/usr/bin"}
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = p.Prepare([]string{"sh"}, "", "/nfs/bin:/usr/bin", env, true)
					_ = p.RunStart(func() error { return nil }, 50*time.Millisecond)
				}
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Every wedge slot must come back. Polled rather than asserted outright: an
	// abandoned start releases its slot from the reaper goroutine, so a bare check
	// here would be a race against a correct implementation.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && p.WedgedCount() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := p.WedgedCount(); n != 0 {
		t.Errorf("wedged=%d after the storm, want 0: slots are not being released", n)
	}
	assertNoGoroutineLeak(t, "spawnsafe concurrent gen-swap", before)
}
