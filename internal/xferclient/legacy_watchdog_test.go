package xferclient

import (
	"context"
	"errors"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"strings"
	"testing"
	"time"
)

// origin: gotcha #84 (simcluster-speed internal review round 1 R1-F1 / plan X27). The Put-leg watchdog: a
// put that finishes wins (its own result, success or failure, even while probes fail); `strikes`
// consecutive probe failures — not fewer, and not a single failure surrounded by successes — cancel the
// put with stallNoAnswer; a recovered probe resets the count. The second half (image #8 receipt): `strikes`
// consecutive healthy probes whose sequence did not move cancel it with stallNoProgress; a moving sequence
// resets that count, so a slow-but-landing upload is never cut. Injected put/probe fakes, millisecond
// intervals, no NATS.
// origin: TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes
func TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyProbeStrikes(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy watchdog", g)
	testharness.AssertNoFDLeak(t, "legacy watchdog", fd, 0)
}
func exerciseLegacyProbeStrikes(t *testing.T) {
	blockUntilCancelled := func(c context.Context) error { <-c.Done(); return c.Err() }
	failing := func(context.Context) (uint64, error) { return 0, errors.New("no leader") }
	// advancing: a healthy stream taking the upload — the sequence moves on every reading.
	advancing := func() func(context.Context) (uint64, error) {
		var n uint64
		return func(context.Context) (uint64, error) { n++; return n, nil }
	}
	// stuck: a healthy-looking stream that accepts nothing — leader present, sequence frozen.
	stuck := func(context.Context) (uint64, error) { return 94, nil }
	slowPut := func(d time.Duration) func(context.Context) error {
		return func(c context.Context) error {
			select {
			case <-time.After(d):
				return nil
			case <-c.Done():
				return errors.New("cut by the watchdog")
			}
		}
	}

	t.Run("healthy JS: a slow put is never cut", func(t *testing.T) {
		if err := putWithJSWatchdog(t.Context(), slowPut(120*time.Millisecond), advancing(), 10*time.Millisecond, 5*time.Millisecond, 3); err != nil {
			t.Fatalf("healthy, advancing probes must never cancel a put: %v", err)
		}
	})
	t.Run("stalled JS: cancelled after exactly `strikes` failed probes", func(t *testing.T) {
		probes := 0
		probe := func(c context.Context) (uint64, error) { probes++; return failing(c) }
		start := time.Now()
		err := putWithJSWatchdog(t.Context(), blockUntilCancelled, probe, 20*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if !errors.As(err, &stall) || stall.kind != stallNoAnswer {
			t.Fatalf("want a stallNoAnswer *jetStreamStallError, got %T %v", err, err)
		}
		// probes = 1 baseline reading + 3 failed samples.
		if probes != 4 || stall.strikes != 3 {
			t.Fatalf("probes=%d strikes=%d, want 4/3 (baseline + cancel on the third consecutive failure, no more)", probes, stall.strikes)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("took %s — the put was not cancelled by the watchdog", took)
		}
		if !strings.Contains(err.Error(), "3 probes over") || !errors.Is(err, stall.last) {
			t.Fatalf("stall error must carry the count, the window and the last probe error: %v", err)
		}
	})
	t.Run("a recovered probe resets the count: fail,ok,fail,ok,fail → still running at strikes=2", func(t *testing.T) {
		adv := advancing()
		seq := []bool{true, false, true, false, true} // true = failing; never two in a row
		i := 0
		probe := func(c context.Context) (uint64, error) {
			if i < len(seq) {
				f := seq[i]
				i++
				if f {
					return failing(c)
				}
			}
			return adv(c) // healthy from here on; the put decides
		}
		// strikes=2 here (the production value is 3): the reset is what is under test, and a threshold
		// other than the production constant keeps the parameter a real parameter.
		if err := putWithJSWatchdog(t.Context(), slowPut(200*time.Millisecond), probe, 15*time.Millisecond, 5*time.Millisecond, 2); err != nil {
			t.Fatalf("alternating failure and success must never reach two consecutive strikes: %v", err)
		}
	})
	t.Run("healthy but frozen: cancelled with stallNoProgress after exactly `strikes` still readings", func(t *testing.T) {
		probes := 0
		probe := func(c context.Context) (uint64, error) { probes++; return stuck(c) }
		start := time.Now()
		// A bounded context: a watchdog that never notices the frozen sequence would otherwise block this
		// subtest on the never-returning put until the package timeout (the mutation reads as a hang).
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		err := putWithJSWatchdog(ctx, blockUntilCancelled, probe, 20*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if !errors.As(err, &stall) || stall.kind != stallNoProgress {
			t.Fatalf("want a stallNoProgress *jetStreamStallError, got %T %v", err, err)
		}
		if probes != 4 || stall.strikes != 3 || stall.lastSeq != 94 {
			t.Fatalf("probes=%d strikes=%d lastSeq=%d, want 4/3/94 (baseline + three unmoved readings)", probes, stall.strikes, stall.lastSeq)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("took %s — the put was not cancelled by the watchdog", took)
		}
		if !strings.Contains(err.Error(), "accepted none of the upload") || !strings.Contains(err.Error(), "last_seq stuck at 94") {
			t.Fatalf("no-progress error must say so and carry the frozen sequence: %v", err)
		}
	})
	t.Run("a moving sequence resets the no-progress count: still,still,moved,still,still → still running", func(t *testing.T) {
		seqs := []uint64{94, 94, 94, 95, 95, 95} // baseline 94; two still; moved; two still — never three in a row
		i, n := 0, uint64(1000)
		probe := func(context.Context) (uint64, error) {
			if i < len(seqs) {
				s := seqs[i]
				i++
				return s, nil
			}
			n++
			return n, nil // moving from here on; the put decides
		}
		if err := putWithJSWatchdog(t.Context(), slowPut(200*time.Millisecond), probe, 15*time.Millisecond, 5*time.Millisecond, 3); err != nil {
			t.Fatalf("a sequence that moves every third reading must not reach three still strikes: %v", err)
		}
	})
	t.Run("the put's own failure wins over a probe failure in flight", func(t *testing.T) {
		putErr := errors.New("object too large")
		put := func(context.Context) error { return putErr }
		err := putWithJSWatchdog(t.Context(), put, failing, 10*time.Millisecond, 5*time.Millisecond, 3)
		if !errors.Is(err, putErr) {
			t.Fatalf("want the put's own error, got %v", err)
		}
	})
	t.Run("caller ctx cancelled: the put's ctx error comes back, not a stall", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		go func() { time.Sleep(30 * time.Millisecond); cancel() }()
		err := putWithJSWatchdog(ctx, blockUntilCancelled, advancing(), 10*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if errors.As(err, &stall) || !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled from the put, got %v", err)
		}
	})
}
