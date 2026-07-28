package broker

import (
	"context"
	"testing"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// audit_publisher_replicas_test.go — ObserveReplicas' own contract.
//
// origin: batch B2 debt external review F1, then main-process audit of that fix.
//
// F1 was a real defect and its fix is right: a bounded observation that times out partway through
// per-stream collection used to return an EMPTY report, so the cache kept a stale short count and the
// undersized deadline could repeat forever. The fix enumerates the complete work set BEFORE collection
// and returns that count on the failure path, so the next budget is sized correctly without any partial
// replica state being mistaken for a measurement.
//
// The fix shipped with NO test on the function it changed. `ObserveReplicas` had zero test callers in
// this package; the only new assertion hand-built a ReplicaReport and fed it straight to
// cacheReplicaSnapshot, which proves the CACHE honours a StreamCount but never that ObserveReplicas
// produces one. Reverting `failed()` to `return ReplicaReport{}, err` — a byte-exact revert of the fix —
// left the whole package green. That is the same coverage shape (a changed function tested only through
// its consumer) that this batch's internal review raised as M3, reproduced inside the fix for it.
//
// So these drive the real function.

// observeFixture stands up a hermetic JetStream and an AuditPublisher over it. Nothing is created in the
// stream domain, so every CollectStreamState call fails — which is exactly the failure path under test.
func observeFixture(t *testing.T, sids []string, withXfer bool) *AuditPublisher {
	t.Helper()
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	cfg := AuditPublisherConfig{
		Node:     &fakeReader{leader: true},
		JS:       js,
		ListSIDs: func(context.Context) ([]string, error) { return sids, nil },
	}
	if withXfer {
		// Non-nil is the only thing ObserveReplicas reads it for: it is the gate that enables the
		// OBJ_xfer enumeration, exactly as ReconcileOnce uses it.
		cfg.XferState = func(context.Context, string, int) (jsstream.StreamReplicaState, error) {
			return jsstream.StreamReplicaState{}, nil
		}
	}
	return NewAuditPublisher(cfg)
}

// TestObserveReplicasReportsWorkSetCountWhenCollectionFails is the F1 guard at its source.
//
// A failed pass must carry two things at once, and they pull in opposite directions:
//
//	Observed=false     nothing downstream may read partial state as a measurement — the gauge, the
//	                   retire gate and AllAtTarget all stay fail-closed.
//	StreamCount=N      the deadline for the NEXT pass must still learn how much work there was, or a
//	                   timeout caused by an OBJ_xfer burst preserves the short deadline that caused it.
//
// Returning an empty report satisfies the first and silently loses the second, which is the shape F1
// found in production code.
func TestObserveReplicasReportsWorkSetCountWhenCollectionFails(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sids      []string
		withXfer  bool
		wantCount int
	}{
		// events only.
		{"no sessions", nil, false, 1},
		// events + one history per active session.
		{"three sessions", []string{"a", "b", "c"}, false, 4},
		// same, with the OBJ_xfer enumeration enabled: no buckets exist in this fixture, so the count is
		// unchanged — which is the point. The enumeration must RUN (and must not error) even on a pass
		// that is about to fail, or the count it contributes is missing precisely when it matters.
		{"three sessions with xfer enumeration enabled", []string{"a", "b", "c"}, true, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := observeFixture(t, tc.sids, tc.withXfer)

			rep, err := p.ObserveReplicas(context.Background())
			if err == nil {
				t.Fatal("fixture: no streams exist, so per-stream collection must fail; a nil error here " +
					"means this test is exercising the success path and proves nothing about the failure one")
			}
			if rep.Observed {
				t.Error("a failed collection reported Observed=true — partial state must never read as a " +
					"measurement; the retire gate and the replica gauge both trust this flag")
			}
			if len(rep.Streams) != 0 {
				t.Errorf("a failed collection returned %d stream state(s); a partial slice would let "+
					"AllAtTarget answer from an incomplete pass", len(rep.Streams))
			}
			if rep.StreamCount != tc.wantCount {
				t.Errorf("StreamCount = %d, want %d. The next observation's deadline is sized from this "+
					"number, so losing it on the failure path is what makes an undersized deadline repeat "+
					"forever — the F1 defect.", rep.StreamCount, tc.wantCount)
			}
		})
	}
}

// TestObserveReplicasEnumeratesBeforeCollecting pins the ORDER that makes the count above possible.
//
// The work set has to be discovered before the first CollectStreamState, not after: collection is where
// the deadline expires, so an enumeration placed behind it contributes nothing on exactly the pass that
// needs it. This asserts the observable consequence — ListSIDs is called even though collection fails on
// the events stream, which is walked first — rather than the source layout.
func TestObserveReplicasEnumeratesBeforeCollecting(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	listed := 0
	p := NewAuditPublisher(AuditPublisherConfig{
		Node: &fakeReader{leader: true},
		JS:   js,
		ListSIDs: func(context.Context) ([]string, error) {
			listed++
			return []string{"a", "b"}, nil
		},
	})

	rep, err := p.ObserveReplicas(context.Background())
	if err == nil {
		t.Fatal("fixture: the events stream does not exist, so this pass must fail")
	}
	if listed != 1 {
		t.Fatalf("ListSIDs called %d time(s); it must run exactly once and BEFORE collection, or the "+
			"failure path cannot report the work-set size", listed)
	}
	if rep.StreamCount != 3 {
		t.Errorf("StreamCount = %d, want 3 (events + two sessions). The enumeration ran but its result "+
			"did not reach the report, so the next deadline learns nothing from this pass.", rep.StreamCount)
	}
}
