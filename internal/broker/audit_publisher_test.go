package broker

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/hashicorp/raft"
)

// fakeReader is a controllable ClusterReader for the deterministic publisher-branch tests
// (truncation / ceiling / skip / idle / fence). cmds[idx]: a non-nil command is returned;
// a nil VALUE => ErrLogPoison; a MISSING key => ErrLogNonCommand (a noop/config entry);
// idx < first => ErrLogTruncated. The branch tests use NON-reconcile commands so no
// js.Publish fires — the reconcile publish path is proven end-to-end in test/d5.
type fakeReader struct {
	leader       bool
	stale        bool
	commit       uint64
	first        uint64
	cmds         map[uint64]*cluster.Command
	published    uint64
	advanceCalls int
	advanceErr   error
}

func (f *fakeReader) IsLeader() bool                       { return f.leader }
func (f *fakeReader) LeaderContactStale(time.Time) bool    { return f.stale }
func (f *fakeReader) CommitIndex() uint64                  { return f.commit }
func (f *fakeReader) LogFirstIndex() (uint64, error)       { return f.first, nil }
func (f *fakeReader) AuditPublishedIndex() (uint64, error) { return f.published, nil }
func (f *fakeReader) NumVoters() (int, error)              { return 3, nil }
func (f *fakeReader) CommittedCommandAt(idx uint64) (*cluster.Command, error) {
	if idx < f.first {
		return nil, cluster.ErrLogTruncated
	}
	c, ok := f.cmds[idx]
	if !ok {
		return nil, cluster.ErrLogNonCommand
	}
	if c == nil {
		return nil, cluster.ErrLogPoison
	}
	return c, nil
}
func (f *fakeReader) AdvanceAuditPublished(to uint64) error {
	f.advanceCalls++
	if f.advanceErr != nil {
		return f.advanceErr
	}
	if to > f.published {
		f.published = to
	}
	return nil
}

func nonReconcile() *cluster.Command { return cluster.NewCommand(cluster.OpClusterMetaSet) }

func newFakePublisher(f *fakeReader) *AuditPublisher {
	return NewAuditPublisher(AuditPublisherConfig{Node: f})
}

// TestD5PublishTruncationNoWedge (E-A6): a snapshot truncated [1..4] before they were
// published. The publisher must NOT panic, must count the loss, must ADVANCE the cursor
// past the gap (never wedge), and must keep handling entries > FirstIndex.
func TestD5PublishTruncationNoWedge(t *testing.T) {
	f := &fakeReader{leader: true, commit: 10, first: 5, published: 0, cmds: map[uint64]*cluster.Command{}}
	for i := uint64(5); i <= 10; i++ {
		f.cmds[i] = nonReconcile()
	}
	p := newFakePublisher(f)
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv != 10 || f.published != 10 {
		t.Fatalf("cursor did not advance past the gap: advanced=%d published=%d (want 10) — WEDGE", adv, f.published)
	}
	if got := p.TruncationLossCount(); got != 4 {
		t.Fatalf("truncation loss = %d, want 4 (indices 1..4)", got)
	}
}

// TestD5PublishIdleZeroWrites (E-A14): an idle loop (commit == published) issues ZERO raft
// writes (R-5).
func TestD5PublishIdleZeroWrites(t *testing.T) {
	f := &fakeReader{leader: true, commit: 10, first: 1, published: 10, cmds: map[uint64]*cluster.Command{}}
	p := newFakePublisher(f)
	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if f.advanceCalls != 0 {
		t.Fatalf("idle loop issued %d raft writes, want 0", f.advanceCalls)
	}
}

// TestD5PublishBatchedCheckpoint (M5 / R-5): a large drain advances the checkpoint in
// BATCHES (one raft write per Batch entries + one final), not per-entry (self-DoS) and not
// once-at-end (unbounded lag). commit=1000, Batch=256 => flushes at 257/513/769/1000 = 4.
func TestD5PublishBatchedCheckpoint(t *testing.T) {
	f := &fakeReader{leader: true, commit: 1000, first: 1, published: 0, cmds: map[uint64]*cluster.Command{}}
	for i := uint64(1); i <= 1000; i++ {
		f.cmds[i] = nonReconcile()
	}
	p := NewAuditPublisher(AuditPublisherConfig{Node: f, Batch: 256})
	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if f.advanceCalls != 4 {
		t.Fatalf("batched checkpoint advanceCalls = %d, want 4 (256-batched, not 1 once-at-end, not 1000 per-entry)", f.advanceCalls)
	}
	if f.published != 1000 {
		t.Fatalf("final cursor = %d, want 1000", f.published)
	}
}

// TestD5PublishLagBoundFires (M2 / R-7): when CommitIndex - checkpoint breaches MaxLag
// (= TrailingLogs in production), the publisher counts + warns the truncation-loss risk.
func TestD5PublishLagBoundFires(t *testing.T) {
	f := &fakeReader{leader: true, commit: 20, first: 1, published: 0, cmds: map[uint64]*cluster.Command{}}
	for i := uint64(1); i <= 20; i++ {
		f.cmds[i] = nonReconcile()
	}
	p := NewAuditPublisher(AuditPublisherConfig{Node: f, MaxLag: 10}) // shrink the bound
	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if got := p.LagExceededCount(); got != 1 {
		t.Fatalf("lag-bound breach count = %d, want 1 (lag 20 >= bound 10)", got)
	}
	// Within bound => no breach.
	f2 := &fakeReader{leader: true, commit: 5, first: 1, published: 0, cmds: f.cmds}
	p2 := NewAuditPublisher(AuditPublisherConfig{Node: f2, MaxLag: 10})
	if _, err := p2.PublishOnce(context.Background()); err != nil {
		t.Fatalf("publish once (in-bound): %v", err)
	}
	if got := p2.LagExceededCount(); got != 0 {
		t.Fatalf("in-bound lag breach count = %d, want 0", got)
	}
}

// TestD5TruncationNeverExceedsCommit (m1): a snapshot that pushes FirstIndex BEYOND the
// captured CommitIndex must not advance the cursor or count loss past the ceiling.
func TestD5TruncationNeverExceedsCommit(t *testing.T) {
	f := &fakeReader{leader: true, commit: 10, first: 100, published: 0, cmds: map[uint64]*cluster.Command{}}
	p := newFakePublisher(f)
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv > 10 || f.published > 10 {
		t.Fatalf("cursor advanced past the commit ceiling: advanced=%d published=%d, want <=10 (R-4)", adv, f.published)
	}
	if got := p.TruncationLossCount(); got > 10 {
		t.Fatalf("truncation loss = %d, want <=10 (must not over-count past the ceiling)", got)
	}
}

// TestD5PublishCeilingIsCommit (E-A10): the publisher never reads past CommitIndex even if
// later (uncommitted) entries exist in the log — they could be overwritten by a new leader.
func TestD5PublishCeilingIsCommit(t *testing.T) {
	f := &fakeReader{leader: true, commit: 5, first: 1, published: 0, cmds: map[uint64]*cluster.Command{}}
	for i := uint64(1); i <= 10; i++ { // entries up to 10 exist but only 5 are committed
		f.cmds[i] = nonReconcile()
	}
	p := newFakePublisher(f)
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv != 5 || f.published != 5 {
		t.Fatalf("published past the commit ceiling: advanced=%d published=%d, want 5", adv, f.published)
	}
}

// TestD5PublishSkipsPoisonAndNonCommand (E-A9/E-A12): non-command (noop) and poison entries
// carry no replayable audit — they are skipped and the cursor advances past them.
func TestD5PublishSkipsPoisonAndNonCommand(t *testing.T) {
	f := &fakeReader{leader: true, commit: 5, first: 1, published: 0, cmds: map[uint64]*cluster.Command{
		// 1,2 missing => ErrLogNonCommand; 3 => poison; 4,5 => non-reconcile
		3: nil,
		4: nonReconcile(),
		5: nonReconcile(),
	}}
	p := newFakePublisher(f)
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv != 5 || f.published != 5 {
		t.Fatalf("cursor = %d/%d, want 5 (all skipped, advanced)", adv, f.published)
	}
	if got := p.TruncationLossCount(); got != 0 {
		t.Fatalf("loss = %d, want 0 (poison/non-command is not truncation loss)", got)
	}
}

// TestD5DeposedLeaderCheckpointRejected (E-A13 unit): a deposed leader whose
// AdvanceAuditPublished is rejected (raft.ErrNotLeader) surfaces the error without
// corrupting the cursor — no silent advance, no split-brain checkpoint.
func TestD5DeposedLeaderCheckpointRejected(t *testing.T) {
	f := &fakeReader{leader: true, commit: 5, first: 1, published: 0, advanceErr: raft.ErrNotLeader,
		cmds: map[uint64]*cluster.Command{1: nonReconcile(), 2: nonReconcile(), 3: nonReconcile(), 4: nonReconcile(), 5: nonReconcile()}}
	p := newFakePublisher(f)
	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("a rejected checkpoint advance must surface an error")
	}
	if f.published != 0 {
		t.Fatalf("rejected advance corrupted the cursor to %d, want 0", f.published)
	}
}

// TestD5TickFenceGates (E-A13/E-A15 fence): a fenced leader (IsLeader but LeaderContactStale)
// does NOTHING — proven by the tick not even reaching the (nil-JS) reconcile pass, which
// would panic if run. advanceCalls stays 0.
func TestD5TickFenceGates(t *testing.T) {
	f := &fakeReader{leader: true, stale: true, commit: 5, first: 1, published: 0,
		cmds: map[uint64]*cluster.Command{1: nonReconcile()}}
	p := NewAuditPublisher(AuditPublisherConfig{Node: f}) // JS nil: a non-gated tick would panic
	p.tick(context.Background())                          // must not panic
	if f.advanceCalls != 0 {
		t.Fatalf("a fenced leader published/advanced (%d writes); the fence must gate it", f.advanceCalls)
	}
	// A non-leader is likewise gated.
	f.leader, f.stale = false, false
	p.tick(context.Background())
	if f.advanceCalls != 0 {
		t.Fatalf("a follower published/advanced (%d writes); only the leader publishes", f.advanceCalls)
	}
}

// TestD5AuditMsgIDKeying (E-A4/R-10): the dedup id is index-keyed for an unkeyed entry but
// REQID-keyed for a forwarded reconcile (so an ack-lost retry committing at a NEW raft index
// re-derives the SAME id and JetStream collapses it), and kind partitions proc/port.
func TestD5AuditMsgIDKeying(t *testing.T) {
	if got := auditMsgID(100, "", "proc", 0); got != "r100:proc:0" {
		t.Fatalf("unkeyed id = %q, want r100:proc:0", got)
	}
	if got := auditMsgID(100, "abcd", "proc", 0); got != "qabcd:proc:0" {
		t.Fatalf("reqid id = %q, want qabcd:proc:0", got)
	}
	// R-10: a reqID-keyed id is INDEPENDENT of the raft index (cross-retry stable).
	if auditMsgID(100, "abcd", "proc", 0) != auditMsgID(200, "abcd", "proc", 0) {
		t.Fatal("reqid-keyed id must not depend on the raft index (the appliedDedup double-publish bug)")
	}
	// kind partitions proc/port so a (idx,seq)-coincident proc and port never collide.
	if auditMsgID(100, "", "proc", 0) == auditMsgID(100, "", "port", 0) {
		t.Fatal("proc and port records must not share a dedup id")
	}
}

// TestD5AllAtTargetCanonical (E-B4/E-B6/R-20): AllAtTarget is fail-closed — empty/unobserved
// => false; a single laggard (even last) => false; Degraded is the complement on an observed
// non-empty set.
func TestD5AllAtTargetCanonical(t *testing.T) {
	ready := func(name string) jsstream.StreamReplicaState {
		return jsstream.StreamReplicaState{Name: name, Target: 3, Actual: 3, Ready: true}
	}
	lag := jsstream.StreamReplicaState{Name: "history-late", Target: 3, Actual: 1, Ready: false}

	// empty / unobserved => false (a fresh cluster must not permit retire).
	if (ReplicaReport{Observed: true}).AllAtTarget() {
		t.Fatal("empty set AllAtTarget must be false")
	}
	if (ReplicaReport{Streams: []jsstream.StreamReplicaState{ready("a")}, Observed: false}).AllAtTarget() {
		t.Fatal("unobserved AllAtTarget must be false")
	}
	// laggard pinned LAST defeats a first-K sampler.
	streams := []jsstream.StreamReplicaState{}
	for i := 0; i < 49; i++ {
		streams = append(streams, ready("history-"+string(rune('a'+i%26))))
	}
	streams = append(streams, lag)
	r := ReplicaReport{Streams: streams, Observed: true}
	if r.AllAtTarget() {
		t.Fatal("a single laggard (last) must make AllAtTarget false")
	}
	if !r.Degraded() {
		t.Fatal("a laggard must make Degraded true")
	}
	// all ready => true, not degraded.
	allReady := ReplicaReport{Streams: []jsstream.StreamReplicaState{ready("a"), ready("b")}, Observed: true}
	if !allReady.AllAtTarget() || allReady.Degraded() {
		t.Fatal("all-ready must be AllAtTarget && !Degraded")
	}
}
