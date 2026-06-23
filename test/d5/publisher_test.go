//go:build d5_integration

package d5_test

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/nats-io/nats.go/jetstream"
)

// waitLeaderExcept waits for a raft leader that is NOT old (used after a
// TransferLeadership to find the NEW leader index).
func (c *cluster5) waitLeaderExcept(t *testing.T, old int) int {
	t.Helper()
	idx := -1
	if !waitForCond(8*time.Second, func() bool {
		for i, nd := range c.nodes {
			if i != old && nd != nil && nd.IsLeader() {
				idx = i
				return true
			}
		}
		return false
	}) {
		t.Fatal("leadership did not move to a new node")
	}
	return idx
}

// TestD5PostElectionSweep (§13.9 exit / E-A1): a ReconcileBatch commits under the leader
// but is NEVER published there (no publisher runs on it); leadership moves to a follower;
// the NEW leader re-derives the committed-but-unpublished entry from its REPLICATED log
// and publishes it. The empty-before-election baseline proves the SWEEP did it (not a
// no-op): the old leader published 0, so a full stream after the new leader's first drain
// is attributable to the post-election sweep alone.
func TestD5PostElectionSweep(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ensureHistory(t, c, li, "lab", 3)
	if !waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3) {
		t.Fatal("history-lab never reached R3")
	}

	commitReconcile(t, c.nodes[li], "", "lab",
		[]proc.ReconProcAudit{auditProc("lab-1", "01aaa", "reconciled_closed"), auditProc("lab-1", "01bbb", "killed_orphan")}, nil)

	// Baseline: the old leader published NOTHING (no publisher ran on it).
	if got := streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")); got != 0 {
		t.Fatalf("pre-election baseline must be empty, got %d (sweep would be vacuous)", got)
	}

	if err := c.nodes[li].TransferLeadership(); err != nil {
		t.Fatalf("transfer leadership: %v", err)
	}
	ni := c.waitLeaderExcept(t, li)

	// The NEW leader sweeps the replicated entry. Under the heavy concurrent -race
	// load of `make e2e`, the election settle + the JS publish/propagation can exceed
	// a tight single-shot window, so RETRY the (idempotent, dedup-id-keyed → no
	// double-publish) sweep until the records land or a generous deadline. This is the
	// same flake class the D5 stream-placement retry already addresses.
	p := newPublisher(c, ni, "lab")
	const want = 2
	swept := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		if _, err := p.PublishOnce(ctx); err != nil {
			t.Fatalf("new-leader sweep: %v", err)
		}
		if streamMsgs(t, c.js[ni], jsstream.HistoryStreamName("lab")) == want {
			swept = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !swept {
		t.Fatalf("post-election sweep published %d records, want %d", streamMsgs(t, c.js[ni], jsstream.HistoryStreamName("lab")), want)
	}
}

// TestD5ForwardedReconcileNoDoublePublish (E-A8 / R-10): the bug none of the drafts caught.
// A forwarded reconcile carries a deterministic ReconcileReqID; an ack-lost RETRY of the
// same reconcile commits a SECOND entry at a NEW raft index via appliedDedup (op skipped,
// index advanced) that STILL carries the audit Aux. A raft-index-keyed publisher would
// re-derive it under a different id and DOUBLE-publish; the reqID-keyed id collapses it.
func TestD5ForwardedReconcileNoDoublePublish(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ensureHistory(t, c, li, "lab", 3)
	if !waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3) {
		t.Fatal("history-lab never reached R3")
	}

	// Build the records ONCE so both commits carry identical Aux (the real retry would).
	recs := []proc.ReconProcAudit{auditProc("lab-1", "01ccc", "reconciled_closed")}

	commitReconcile(t, c.nodes[li], testReqID, "lab", recs, nil)
	if got := c.nodes[li].DedupCount(); got != 0 {
		t.Fatalf("first commit DedupCount = %d, want 0", got)
	}
	// Retry the SAME reqID → a second committed entry that hits appliedDedup at a NEW index.
	commitReconcile(t, c.nodes[li], testReqID, "lab", recs, nil)
	if got := c.nodes[li].DedupCount(); got != 1 {
		t.Fatalf("retry DedupCount = %d, want 1 (the retry must commit a second entry that dedups)", got)
	}

	p := newPublisher(c, li, "lab")
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("publish once: %v", err)
	}
	// Both committed entries carry identical Aux + ReqID → identical reqID-keyed dedup ids →
	// JetStream collapses → EXACTLY ONE record (a raft-index-keyed id would yield two).
	time.Sleep(300 * time.Millisecond)
	if got := streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")); got != 1 {
		t.Fatalf("forwarded-reconcile retry double-published: %d records, want 1 (R-10)", got)
	}
}

// TestD5DedupCollapsesIdenticalID (B1 / R-12 — the load-bearing single-writer proof). The
// cross-election sweep tests are structurally vacuous (the checkpoint advances past the
// entry, so a new leader re-derives nothing), so this proves the WIRE invariant directly:
// a SECOND publish of the EXACT id the publisher emitted (= what a racing other-leader would
// emit) is COLLAPSED by the history stream's Duplicates window; a DIFFERENT id is NOT (the
// control arm — without which the collapse assertion would pass even if dedup were a no-op).
// The publisher's first publish (count 0->1) proves it emits id "q<testReqID>:proc:0", and
// the collapse of a re-publish of THAT id proves JetStream's identical-id collapse.
func TestD5DedupCollapsesIdenticalID(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := jsstream.HistoryStreamName("lab")

	ensureHistory(t, c, li, "lab", 3)
	waitForStreamReplicas(t, c.js[li], name, 3)

	// The publisher publishes the reqID-keyed reconcile's proc[0] -> id "q<testReqID>:proc:0".
	commitReconcile(t, c.nodes[li], testReqID, "lab", []proc.ReconProcAudit{auditProc("lab-1", "01ccc", "reconciled_closed")}, nil)
	if _, err := newPublisher(c, li, "lab").PublishOnce(ctx); err != nil {
		t.Fatalf("publisher publish: %v", err)
	}
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], name) == 1 }) {
		t.Fatalf("publisher did not publish its record (count %d, want 1)", streamMsgs(t, c.js[li], name))
	}

	republish := func(msgID string) {
		t.Helper()
		rec := schema.AuditProc{V: schema.AuditSchemaVersion, Kind: "reconciled_closed", Session: "lab", Node: "lab-1", PID: "01ccc"}
		payload, _ := json.Marshal(rec)
		if _, err := c.js[li].Publish(ctx, proto.SubjAuditProc("lab"), payload, jetstream.WithMsgID(msgID)); err != nil {
			t.Fatalf("republish %s: %v", msgID, err)
		}
	}

	// COLLAPSE: re-publishing the EXACT id the publisher emitted is dropped by the Duplicates
	// window → count stays 1 (the R-12 single-writer wire guarantee).
	republish("q" + testReqID + ":proc:0")
	time.Sleep(400 * time.Millisecond)
	if got := streamMsgs(t, c.js[li], name); got != 1 {
		t.Fatalf("identical id must COLLAPSE — msgs=%d, want 1 (Duplicates window / msg-id grammar broken)", got)
	}

	// CONTROL (non-vacuity): a DIFFERENT id is NOT collapsed → count grows to 2. Without this
	// arm the collapse assertion would hold even if dedup did nothing.
	republish("q" + testReqID + ":proc:999")
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], name) == 2 }) {
		t.Fatalf("a DIFFERENT id must NOT collapse — msgs=%d, want 2 (collapse arm would be vacuous)", streamMsgs(t, c.js[li], name))
	}
}

// TestD5IdleLeaderNoCheckpointGrowth (external review F1, real-node proof): after the
// publisher advances the checkpoint once (committing an OpAuditCheckpointSet entry), two
// further IDLE ticks must commit NO new raft entries — i.e. the cursor op does not beget
// another cursor op. Without the fix, each idle tick would advance past the prior checkpoint
// entry and write a fresh one, growing the log forever.
func TestD5IdleLeaderNoCheckpointGrowth(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ensureHistory(t, c, li, "lab", 3)
	waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3)

	p := newPublisher(c, li, "lab")
	commitReconcile(t, c.nodes[li], "", "lab", []proc.ReconProcAudit{auditProc("lab-1", "01idl", "reconciled_closed")}, nil)
	if _, err := p.PublishOnce(ctx); err != nil { // publishes + commits one OpAuditCheckpointSet
		t.Fatalf("first publish: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	ci1 := c.nodes[li].CommitIndex()

	// Two idle ticks (no new reconcile, no other writes): the leader must commit nothing.
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("idle publish 1: %v", err)
	}
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("idle publish 2: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	ci2 := c.nodes[li].CommitIndex()
	if ci2 != ci1 {
		t.Fatalf("idle leader committed %d new raft entries after a checkpoint (self-begetting F1): %d -> %d", ci2-ci1, ci1, ci2)
	}
}

// TestD5ProcPortSameSeqBothSurvive (E-A5 / R-8): a single reconcile carrying proc[0] AND
// port[0] (both seq 0) publishes BOTH — the `kind` discriminator partitions them. A coarse
// kind="audit" would collide (idx:audit:0) and silently drop one.
func TestD5ProcPortSameSeqBothSurvive(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ensureHistory(t, c, li, "lab", 3)
	waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3)

	commitReconcile(t, c.nodes[li], "", "lab",
		[]proc.ReconProcAudit{auditProc("lab-1", "01ppp", "reconciled_closed")},
		[]proc.ReconPortAudit{{NID: "lab-1", Port: 8080, Name: "web", LocalPort: 3000, Kind: "reconciled", Ts: time.Now()}})
	if _, err := newPublisher(c, li, "lab").PublishOnce(ctx); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")) == 2 }) {
		t.Fatalf("proc[0]+port[0] both seq 0: BOTH must land (depth 2) — got %d (coarse kind would collapse one)", streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")))
	}
}

// TestD5FollowerNeverPublishes (E-A16 / R-12/L-2): a Run loop on a FOLLOWER publishes
// NOTHING despite committed audit existing — proven over the wire via the OnPublish tap
// (the gate short-circuits in tick before any publish). Uses the tap rather than a
// goroutine count for a deterministic negative assertion.
func TestD5FollowerNeverPublishes(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ensureHistory(t, c, li, "lab", 3)
	waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3)
	// Committed audit exists + replicates to followers (so a follower's PublishOnce WOULD
	// publish if it were not gated).
	commitReconcile(t, c.nodes[li], "", "lab", []proc.ReconProcAudit{auditProc("lab-1", "01fff", "reconciled_closed")}, nil)

	fi := (li + 1) % 3
	if c.nodes[fi].IsLeader() {
		fi = (li + 2) % 3
	}
	var tap atomic.Int64
	p := broker.NewAuditPublisher(broker.AuditPublisherConfig{
		Node: c.nodes[fi], JS: c.js[fi], ListSIDs: staticSIDs("lab"), Poll: 50 * time.Millisecond,
		OnPublish: func(subject, msgID string) { tap.Add(1) },
	})
	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()
	time.Sleep(600 * time.Millisecond) // ~12 follower ticks
	runCancel()
	<-done
	if got := tap.Load(); got != 0 {
		t.Fatalf("follower published %d records over the wire; only the leader publishes (R-12)", got)
	}
}

// TestD5QueueNotDropCheckpointStaysOnFailure (M3 / R-22): a publish FAILURE (the target
// history stream does not exist yet) must NOT advance the checkpoint past the un-ACKed
// entry, and a re-run is a no-op (stuck) — proving queue-not-drop at the CHECKPOINT level,
// not just eventual stream presence. After the stream is created the queued audit publishes
// exactly once and the checkpoint advances (re-runnable, no loss).
func TestD5QueueNotDropCheckpointStaysOnFailure(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// NO history-orphan stream yet. Commit a reconcile for it.
	commitReconcile(t, c.nodes[li], "", "orphan", []proc.ReconProcAudit{auditProc("orphan-1", "01ooo", "reconciled_closed")}, nil)
	p := newPublisher(c, li, "orphan")

	// Short-deadline ctx so the failing publish (no stream → no ack) fails fast.
	fctx, fcancel := context.WithTimeout(ctx, 3*time.Second)
	if _, err := p.PublishOnce(fctx); err == nil {
		fcancel()
		t.Fatal("publishing to a non-existent stream must fail (R-22 trigger)")
	}
	fcancel()
	cp1, _ := c.nodes[li].AuditPublishedIndex()

	// Re-run while still failing: the checkpoint is STUCK (does not advance past the entry).
	fctx2, fcancel2 := context.WithTimeout(ctx, 3*time.Second)
	_, _ = p.PublishOnce(fctx2)
	fcancel2()
	cp2, _ := c.nodes[li].AuditPublishedIndex()
	if cp2 != cp1 {
		t.Fatalf("checkpoint advanced across a persistent publish failure: %d -> %d (must stay put — queue, not drop)", cp1, cp2)
	}

	// Recover: create the stream, retry → publishes exactly once, checkpoint advances.
	ensureHistory(t, c, li, "orphan", 3)
	waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("orphan"), 3)
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("post-recovery publish: %v", err)
	}
	cp3, _ := c.nodes[li].AuditPublishedIndex()
	if cp3 <= cp1 {
		t.Fatalf("checkpoint did not advance after recovery: %d -> %d", cp1, cp3)
	}
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], jsstream.HistoryStreamName("orphan")) == 1 }) {
		t.Fatalf("queued audit lost: orphan msgs=%d, want 1", streamMsgs(t, c.js[li], jsstream.HistoryStreamName("orphan")))
	}
}

// TestD5RunPublishesAndNoLeak (E-A15): the long-lived Run goroutine publishes committed
// audit and, after ctx cancel, releases its goroutines + reply-inbox fds (CLAUDE.md §5
// NumGoroutine + fd gate; goleak deliberately NOT used). Bracketing discipline (OQ-5): a
// WARM-UP publish opens the JS file-storage block fds (events + history) BEFORE the
// baseline, so the leak assertion measures ONLY the Run lifecycle delta, not one-time JS
// storage — without weakening the d4 +2/+4 tolerance.
func TestD5RunPublishesAndNoLeak(t *testing.T) {
	c := startCluster5(t, 3)
	li := c.leaderIdx(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ensureHistory(t, c, li, "lab", 3)
	if !waitForStreamReplicas(t, c.js[li], jsstream.HistoryStreamName("lab"), 3) {
		t.Fatal("history-lab never reached R3")
	}

	p := newPublisher(c, li, "lab")
	// Warm-up: a synchronous drain (publish + reconcile pass) opens the events/history
	// storage-block fds so they are part of the baseline, not the measured Run delta.
	commitReconcile(t, c.nodes[li], "", "lab",
		[]proc.ReconProcAudit{auditProc("lab-1", "01wup", "reconciled_closed")}, nil)
	if _, err := p.PublishOnce(ctx); err != nil {
		t.Fatalf("warm-up publish: %v", err)
	}
	if _, err := p.ReconcileOnce(ctx); err != nil {
		t.Fatalf("warm-up reconcile: %v", err)
	}
	if !waitForCond(5*time.Second, func() bool { return streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")) == 1 }) {
		t.Fatal("warm-up did not publish")
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	before, fdBefore := runtime.NumGoroutine(), fdCount()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()

	commitReconcile(t, c.nodes[li], "", "lab",
		[]proc.ReconProcAudit{auditProc("lab-1", "01ddd", "reconciled_closed")}, nil)
	if !waitForCond(8*time.Second, func() bool { return streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")) == 2 }) {
		t.Fatalf("Run did not publish the committed audit (got %d, want 2)", streamMsgs(t, c.js[li], jsstream.HistoryStreamName("lab")))
	}

	runCancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
	assertNoGoroutineLeak(t, "publisher-run", before)
	assertNoFDLeak(t, "publisher-run", fdBefore)
}
