//go:build d5_integration

package d5_test

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// TestD5StreamExpandsR1toR3 (E-B2): a stream created at R1 expands to R3 on a 3-node JS
// cluster via the ensureStream raise path, and pre-expand messages survive (no loss). A
// second ensure at the same target is a no-op (idempotent).
func TestD5StreamExpandsR1toR3(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := jsstream.HistoryStreamName("lab")

	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 1); err != nil {
		t.Fatalf("create R1: %v", err)
	}
	if !waitForStreamReplicas(t, c.js[li], name, 1) {
		t.Fatal("stream not at R1")
	}
	// Write a pre-expansion message so we can prove no loss across the reconfig.
	if _, err := c.js[li].Publish(ctx, proto.SubjAuditProc("lab"), []byte(`{"v":1}`)); err != nil {
		t.Fatalf("pre-expand publish: %v", err)
	}

	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 3); err != nil {
		t.Fatalf("expand to R3: %v", err)
	}
	if !waitForStreamReplicas(t, c.js[li], name, 3) {
		t.Fatalf("stream never reached R3 (have %d)", streamReplicas(t, c.js[li], name))
	}
	if got := streamMsgs(t, c.js[li], name); got != 1 {
		t.Fatalf("pre-expand message lost: msgs = %d, want 1", got)
	}
	// Idempotent: a second ensure at R3 does not error.
	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 3); err != nil {
		t.Fatalf("re-ensure R3: %v", err)
	}
}

// TestD5ReconfigGatedOnMetaCapacity (E-B3): raising a stream toward a target the meta-group
// cannot host (R5 on a 3-server cluster) is REJECTED as the retriable ErrMetaGroupNotReady,
// NOT a hard failure — the UpdateStream-rejection IS the readiness gate (R-17).
func TestD5ReconfigGatedOnMetaCapacity(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "gated", 1); err != nil {
		t.Fatalf("create R1: %v", err)
	}
	if !waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("gated"), 1) {
		t.Fatal("stream not at R1")
	}
	// Target 5 exceeds the 3-server meta capacity => UpdateStream rejected => ErrMetaGroupNotReady.
	err := jsstream.EnsureHistoryStream(ctx, c.js[li], "gated", 5)
	if err == nil || !isMetaNotReady(err) {
		t.Fatalf("raising beyond meta capacity must return ErrMetaGroupNotReady, got %v", err)
	}
	// The stream stays at its current (R1) factor — no partial/false expansion.
	if got := streamReplicas(t, c.js[li], jsstream.HistoryStreamName("gated")); got != 1 {
		t.Fatalf("a rejected raise must leave the stream unchanged (R1), got R%d", got)
	}
}

func isMetaNotReady(err error) bool {
	return err == jsstream.ErrMetaGroupNotReady ||
		(err != nil && err.Error() == jsstream.ErrMetaGroupNotReady.Error())
}

// TestD5ShrinkIsNoop (E-B7): targeting a LOWER replica factor than current must NOT shrink
// the stream (shrink is the gated D7 retire path, never an automatic reconcile).
func TestD5ShrinkIsNoop(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := jsstream.HistoryStreamName("lab")

	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 3); err != nil {
		t.Fatalf("create R3: %v", err)
	}
	if !waitForStreamReplicas(t, c.js[li], name, 3) {
		t.Fatal("stream not at R3")
	}
	// Target 1 < current 3: raise-only => no shrink.
	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 1); err != nil {
		t.Fatalf("ensure R1 (should be a no-op): %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := streamReplicas(t, c.js[li], name); got != 3 {
		t.Fatalf("stream was shrunk to R%d; shrink must be a no-op (D7 owns retire)", got)
	}
}

// TestD5AllThreeFamiliesReconcile (E-B8 / R-18/R-20): one ReconcileOnce pass raises events,
// history-<sid>, AND OBJ_xfer-<sid> to target and reports a canonical AllAtTarget over the
// full set. Pins the third Replicas:1 literal (the object store, via UpdateObjectStore).
func TestD5AllThreeFamiliesReconcile(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Seed all three families at R1.
	if err := jsstream.EnsureHistoryStream(ctx, c.js[li], "lab", 1); err != nil {
		t.Fatalf("history R1: %v", err)
	}
	if err := jsstream.EnsureEventsStream(ctx, c.js[li], 1); err != nil {
		t.Fatalf("events R1: %v", err)
	}
	if _, err := c.js[li].CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: proto.XferBucketName("lab"), Storage: jetstream.FileStorage, Replicas: 1,
		MaxBytes: 8 * 1024 * 1024 * 1024,
	}); err != nil {
		t.Fatalf("xfer bucket R1: %v", err)
	}

	p := broker.NewAuditPublisher(broker.AuditPublisherConfig{
		Node:     c.nodes[li],
		JS:       c.js[li],
		ListSIDs: staticSIDs("lab"),
		XferState: func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error) {
			return broker.XferReplicaState(ctx, c.js[li], sid, target)
		},
	})

	// Drive reconcile passes until every family reaches target (the raise is async).
	var rep broker.ReplicaReport
	if !waitForCond(20*time.Second, func() bool {
		r, err := p.ReconcileOnce(ctx)
		if err != nil {
			return false
		}
		rep = r
		return r.AllAtTarget()
	}) {
		t.Fatalf("not all streams reached target: %+v", rep.Streams)
	}
	// The canonical report covers ALL THREE families (events + history-lab + OBJ_xfer-lab).
	names := map[string]bool{}
	for _, s := range rep.Streams {
		names[s.Name] = true
	}
	for _, want := range []string{jsstream.EventsStreamName, jsstream.HistoryStreamName("lab"), "OBJ_" + proto.XferBucketName("lab")} {
		if !names[want] {
			t.Errorf("ReconcileOnce report missing stream %q (families: %+v)", want, rep.Streams)
		}
	}
	if rep.Degraded() {
		t.Errorf("all-at-target report must not be Degraded: %+v", rep.Streams)
	}
}
