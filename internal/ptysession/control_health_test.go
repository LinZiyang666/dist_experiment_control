//go:build linux

package ptysession

import (
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

type controlHealthVerifier struct{ generation atomic.Uint64 }

func (v *controlHealthVerifier) Current(generation uint64) bool {
	return v.generation.Load() == generation
}

// Real PTY and control token; injected boot clock and generation evidence.
// The proof is produced before an occupied token is released, and therefore
// the generation check must happen at consumption rather than at production.
// origin: hot-update S4 internal review rv-auth RV-AUTH-3
func TestPTYHealthProofRechecksGenerationAfterControlWait(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, change := range []bool{false, true} {
			t.Run(map[bool]string{false: "stable", true: "generation-changed"}[change], func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				f := startActorFixture(t, "signal.signal(signal.SIGHUP,signal.SIG_IGN)\nmark('ready')\nwait('exit')\n")
				waitMark(t, f.dir, "ready")
				var clock atomic.Int64
				clock.Store(int64(time.Hour))
				var block atomic.Bool
				entered := make(chan struct{})
				f.a.cfg.Clock = func() (int64, error) {
					if block.CompareAndSwap(true, false) {
						close(entered)
					}
					return clock.Load(), nil
				}
				f.a.mu.Lock()
				f.a.state.KAIntervalMS = 1000
				f.a.state.KADeadlineBootNS = clock.Load()
				f.a.mu.Unlock()
				verifier := &controlHealthVerifier{}
				verifier.generation.Store(1)
				requireDependency(t, f.a.Tick(HealthProof{}))
				clock.Add(int64(5 * time.Second))
				proof := HealthProof{Generation: 1, AtBootNS: clock.Load(), Healthy: true, Verifier: verifier}
				requireDependency(t, f.a.Tick(proof))
				clock.Add(int64(time.Second))
				proof.AtBootNS = clock.Load()
				// Hold the actual actor token, not a replacement synchronization
				// mechanism. Tick cannot consume the second proof until released.
				<-f.a.control
				block.Store(true)
				done := make(chan error, 1)
				go func() { done <- f.a.Tick(proof) }()
				select {
				case <-entered:
				case <-time.After(actorTestBudget):
					f.a.control <- struct{}{}
					t.Fatal("Tick did not reach the occupied token")
				}
				if change {
					verifier.generation.Store(2)
				}
				f.a.control <- struct{}{}
				select {
				case err := <-done:
					requireDependency(t, err)
				case <-time.After(actorTestBudget):
					t.Fatal("Tick did not finish after releasing the token")
				}
				f.a.mu.Lock()
				closed, strike := f.a.state.MasterClosed, f.a.proof.Healthy
				f.a.mu.Unlock()
				if closed == change || (change && strike) {
					t.Fatalf("proof consumption: generation changed=%v closed=%v strike retained=%v", change, closed, strike)
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "health control wait", g0)
	testharness.AssertNoFDLeak(t, "health control wait", fd0, 0)
}
