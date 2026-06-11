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
	var probes atomic.Int64
	probe := func(string) bool { return probes.Add(1)%2 == 0 }

	p, err := spawnsafe.New(spawnsafe.Config{
		Mode: spawnsafe.ModeAuto, ProbeTimeout: 5 * time.Millisecond, WedgeCeiling: 8,
		MountSource: src, Probe: probe,
	})
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

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
	assertNoGoroutineLeak(t, "spawnsafe concurrent gen-swap", before)
}
