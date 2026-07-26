package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Batch-A A5. The property that matters is the one the old hand-written
// loopCount could silently break: Join must not return until every started loop
// has exited. If it returns early, clusterShutdownOrdered advances to nc.Drain
// while a loop may still be publishing — the "publish-after-Drain silently loses
// an audit record" hazard that clusterwrite.go documents and that the goroutine
// leak gate cannot catch (the goroutine does exit, just too late).

func TestLoopSetJoinWaitsForEveryLoop(t *testing.T) {
	for _, n := range []int{1, 4, 5, 9} {
		t.Run("", func(t *testing.T) {
			ls := newLoopSet()
			ctx, cancel := context.WithCancel(context.Background())
			var running atomic.Int32
			var exited atomic.Int32

			for i := 0; i < n; i++ {
				running.Add(1)
				ls.Go(ctx, "loop", func(c context.Context) {
					<-c.Done()
					// Hold briefly so a Join that miscounts returns while this
					// is demonstrably still inside the loop body.
					time.Sleep(20 * time.Millisecond)
					exited.Add(1)
					running.Add(-1)
				})
			}
			if got := ls.Count(); got != n {
				t.Fatalf("Count()=%d, want %d — the set must count what it actually started", got, n)
			}

			cancel()
			ls.Join(2*time.Second, nil)

			if got := exited.Load(); got != int32(n) {
				t.Errorf("Join returned with %d/%d loops exited; shutdown would proceed to nc.Drain "+
					"while a loop is still publishing", got, n)
			}
			if got := running.Load(); got != 0 {
				t.Errorf("%d loop(s) still running after Join", got)
			}
		})
	}
}

// TestLoopSetJoinHonoursTimeout pins the other direction: a wedged loop must not
// hang shutdown forever.
func TestLoopSetJoinHonoursTimeout(t *testing.T) {
	ls := newLoopSet()
	stuck := make(chan struct{})
	defer close(stuck)
	ls.Go(context.Background(), "wedged", func(context.Context) { <-stuck })

	start := time.Now()
	ls.Join(150*time.Millisecond, nil)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Join blocked %s on a wedged loop; shutdown must stay bounded", elapsed)
	}
}

// TestLoopSetCountIsNotHardcoded is the regression this whole item exists for:
// the count must follow the number of Go calls, with no constant to update.
func TestLoopSetCountFollowsRegistrations(t *testing.T) {
	ls := newLoopSet()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 1; i <= 7; i++ {
		ls.Go(ctx, "loop", func(c context.Context) { <-c.Done() })
		if got := ls.Count(); got != i {
			t.Fatalf("after %d registrations Count()=%d; a count that lags its registrations is the "+
				"exact defect A5 removed", i, got)
		}
	}
	cancel()
	ls.Join(2*time.Second, nil)
}

func TestLoopSetSnapshotReportsLiveness(t *testing.T) {
	ls := newLoopSet()
	ctx, cancel := context.WithCancel(context.Background())
	ls.Go(ctx, "observe", func(c context.Context) { <-c.Done() })
	// Give the goroutine a moment to record its start.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s, ok := ls.Snapshot()["observe"]; ok && !s.StartedAt.IsZero() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := ls.Snapshot()
	st, ok := snap["observe"]
	if !ok {
		t.Fatal("snapshot has no entry for a started loop; RuntimeReport would show nothing")
	}
	if st.StartedAt.IsZero() {
		t.Error("loop recorded no start time — these four loops were previously entirely unobservable")
	}
	cancel()
	ls.Join(2*time.Second, nil)
}

// TestLoopSetJoinDoesNotAbandonLoopsAfterOneTimeout is batch-A review M1.
//
// Join used to `return` on the first timeout, abandoning every remaining loop.
// That is the same hazard loopSet exists to remove — clusterShutdownOrdered
// advancing to nc.Drain while a loop is still publishing — reached from the
// other side: not by miscounting, but by giving up early.
func TestLoopSetJoinDoesNotAbandonLoopsAfterOneTimeout(t *testing.T) {
	ls := newLoopSet()
	ctx, cancel := context.WithCancel(context.Background())
	wedged := make(chan struct{})
	defer close(wedged)

	var finished atomic.Int32
	// One loop never exits; two exit promptly once cancelled.
	ls.Go(ctx, "wedged", func(context.Context) { <-wedged })
	for i := 0; i < 2; i++ {
		ls.Go(ctx, "prompt", func(c context.Context) {
			<-c.Done()
			time.Sleep(30 * time.Millisecond)
			finished.Add(1)
		})
	}

	cancel()
	ls.Join(200*time.Millisecond, nil)

	if got := finished.Load(); got != 2 {
		t.Errorf("Join returned with %d/2 healthy loops joined — one wedged loop must not cause the "+
			"rest to be abandoned; shutdown would proceed to nc.Drain while they are still publishing", got)
	}
}

// TestLoopSetConcurrentGoIsRaceFree is batch-A review F-01.
//
// The existing tests all called Go serially, so -race stayed green over a field
// (`done`) that was initialised OUTSIDE the mutex while every sibling field was
// guarded. A green race detector under serial calls is not evidence about a
// concurrent one.
func TestLoopSetConcurrentGoIsRaceFree(t *testing.T) {
	ls := newLoopSet()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	const n = 16
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ls.Go(ctx, "concurrent", func(c context.Context) { <-c.Done() })
		}()
	}
	wg.Wait()
	if got := ls.Count(); got != n {
		t.Fatalf("Count()=%d after %d concurrent Go calls, want %d", got, n, n)
	}
	cancel()
	ls.Join(2*time.Second, nil)
}
