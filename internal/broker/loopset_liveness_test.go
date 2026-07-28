package broker

import (
	"context"
	"testing"
	"time"
)

// loopset_liveness_test.go — per-iteration loop liveness (B7).
//
// The property under test is the one batch-A's review F-03 deleted a field for failing to have:
// distinguishing "this loop is running" from "this loop returned on its first line". F-03's Runs was
// written once at startup, so it was permanently 1 and answered neither question. Iters/LastIter are
// written by the loop body, so they answer both.

// TestLoopSetBeatRecordsIterations proves a live loop's counter and timestamp advance.
func TestLoopSetBeatRecordsIterations(t *testing.T) {
	l := newLoopSet()
	ctx, cancel := context.WithCancel(context.Background())

	beat := make(chan struct{})
	l.GoEvery(ctx, "ticker", 10*time.Millisecond, func(c context.Context) {
		for {
			select {
			case <-c.Done():
				return
			case <-beat:
				l.Beat("ticker")
			}
		}
	})

	const iterations = 5
	for i := 0; i < iterations; i++ {
		beat <- struct{}{}
	}
	// The 6th send is only there to establish that the 5th Beat has been observed: the loop reads from
	// the channel before beating, so a send returning proves the PREVIOUS iteration's Beat completed.
	beat <- struct{}{}

	snap := l.Snapshot()
	st, ok := snap["ticker"]
	if !ok {
		t.Fatalf("loop not in snapshot: %+v", snap)
	}
	if st.Cadence != 10*time.Millisecond {
		t.Errorf("Cadence = %v, want 10ms — a periodic loop must DECLARE its period or Iters cannot be "+
			"interpreted", st.Cadence)
	}
	if st.Iters < iterations {
		t.Errorf("Iters = %d after %d beats, want >= %d", st.Iters, iterations+1, iterations)
	}
	if st.LastIter.IsZero() {
		t.Error("LastIter is zero after real iterations — a stalled loop is detected by this timestamp " +
			"ceasing to advance, so a permanently zero one detects nothing")
	}
	cancel()
	l.Join(2*time.Second, silentLogger())
}

// TestLoopSetLoopThatReturnsImmediatelyLooksDead is the whole point, stated as a test.
//
// runTopologyReconcileLoop returns on its first line when NatsConfPath is empty, and before B7 that
// loop was indistinguishable from a healthy one: both reported a StartedAt and nothing else. A liveness
// signal that cannot tell those apart is decoration.
func TestLoopSetLoopThatReturnsImmediatelyLooksDead(t *testing.T) {
	l := newLoopSet()
	ctx := context.Background()

	// A loop that returns at once, exactly like the inert topology reconciler.
	l.GoEvery(ctx, "inert", time.Second, func(context.Context) {})
	// And one that genuinely iterates.
	done := make(chan struct{})
	l.GoEvery(ctx, "live", time.Second, func(context.Context) {
		l.Beat("live")
		close(done)
	})
	<-done
	l.Join(2*time.Second, silentLogger())

	snap := l.Snapshot()
	inert, live := snap["inert"], snap["live"]

	if inert.StartedAt.IsZero() {
		t.Fatal("fixture broken: even the inert loop must record a StartedAt — that is precisely why " +
			"StartedAt alone could not answer the question")
	}
	if inert.Iters != 0 {
		t.Errorf("the inert loop reported %d iterations; it never ran a body", inert.Iters)
	}
	if !inert.LastIter.IsZero() {
		t.Errorf("the inert loop reported LastIter = %v; it never completed an iteration", inert.LastIter)
	}
	if live.Iters == 0 {
		t.Error("the live loop reported 0 iterations — then the signal cannot distinguish the two, " +
			"which is the F-03 defect again")
	}
	// The load-bearing assertion: the two are DISTINGUISHABLE. Both have a StartedAt; only one has
	// evidence of work.
	if (inert.Iters == 0) == (live.Iters == 0) {
		t.Error("an inert loop and a working loop report the same liveness — per-iteration observability " +
			"is not actually wired")
	}
}

// TestLoopSetBeatOnAnUnknownNameIsSafe: a Beat racing shutdown must not panic. The loops are cancelled
// and joined in order, but a beat already in flight when the map is read must be inert, not fatal.
func TestLoopSetBeatOnAnUnknownNameIsSafe(t *testing.T) {
	l := newLoopSet()
	l.Beat("never-registered") // must not panic
	if len(l.Snapshot()) != 0 {
		t.Error("a Beat for an unregistered loop must not create a phantom entry")
	}
}
