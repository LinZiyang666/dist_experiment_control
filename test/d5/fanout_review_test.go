//go:build d5_integration

package d5_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
)

// TestD5ReviewBoundedFanout (E-B9 — the plan listed it but NO test shipped): a reconcile
// pass over many sessions must never run more than MaxPar history/xfer reconciles in
// flight at once. The XferState hook runs strictly INSIDE the per-session worker (after
// EnsureHistoryStream + CollectStreamState), so a high-water counter wrapped around it
// upper-bounds the live worker count. A regression that drops the semaphore (unbounded
// goroutine spray) makes the high-water blow past MaxPar.
func TestD5ReviewBoundedFanout(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 20 sessions (each history stream reserves 1 GiB; 20 GiB fits the 32 GiB store quota
	// with OBJ_xfer headroom) — enough to drive the MaxPar=8 bound non-trivially.
	const nSessions = 20
	const maxPar = 8
	sids := make([]string, nSessions)
	for i := range sids {
		sids[i] = pid("s", i%10) + pid("t", i/10) // 20 distinct ids
	}
	// Pre-create every history stream at R1 so EnsureHistoryStream/CollectStreamState in the
	// worker hit an existing stream (fast path) rather than a slow create+replicate.
	for _, sid := range sids {
		ensureHistory(t, c, li, sid, 1)
	}

	var inFlight, highWater atomic.Int64
	updHigh := func(v int64) {
		for {
			h := highWater.Load()
			if v <= h || highWater.CompareAndSwap(h, v) {
				return
			}
		}
	}

	p := broker.NewAuditPublisher(broker.AuditPublisherConfig{
		Node:     c.nodes[li],
		JS:       c.js[li],
		MaxPar:   maxPar,
		ListSIDs: staticSIDs(sids...),
		XferState: func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error) {
			// This runs inside the bounded worker. Bump the live counter, hold briefly so
			// the bound is observable, then report a benign at-target state.
			n := inFlight.Add(1)
			updHigh(n)
			time.Sleep(15 * time.Millisecond)
			inFlight.Add(-1)
			// Report a benign at-target state (nil error) so the worker's xfer switch takes
			// the success arm and the pass does not error; we only care about the bound.
			return jsstream.StreamReplicaState{Name: "OBJ_xfer-" + sid, Target: target, Actual: target, Ready: true}, nil
		},
	})

	// Target is ReplicasFor(3)=3; histories are at R1 so the worker also issues an
	// UpdateStream raise — fine, the bound is what we measure, not the outcome.
	if _, err := p.ReconcileOnce(ctx); err != nil {
		t.Fatalf("reconcile once: %v", err)
	}
	hw := highWater.Load()
	if hw == 0 {
		t.Fatal("XferState never ran — the fan-out probe is vacuous (setup failed)")
	}
	if hw > maxPar {
		t.Fatalf("fan-out exceeded MaxPar: high-water=%d > %d (semaphore not bounding concurrency)", hw, maxPar)
	}
	t.Logf("bounded fan-out OK: high-water=%d (cap %d) over %d sessions", hw, maxPar, nSessions)
}
