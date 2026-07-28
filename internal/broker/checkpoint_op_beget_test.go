package broker

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

type checkpointBegetReader struct {
	leader       bool
	commit       uint64
	first        uint64
	published    uint64
	cmds         map[uint64]*cluster.Command
	advanceCalls int
}

func (f *checkpointBegetReader) IsLeader() bool                    { return f.leader }
func (f *checkpointBegetReader) LeaderContactStale(time.Time) bool { return false }
func (f *checkpointBegetReader) CommitIndex() uint64               { return f.commit }
func (f *checkpointBegetReader) LogFirstIndex() (uint64, error)    { return f.first, nil }
func (f *checkpointBegetReader) NumVoters() (int, error)           { return 1, nil }
func (f *checkpointBegetReader) AuditPublishedIndex() (uint64, error) {
	return f.published, nil
}
func (f *checkpointBegetReader) CommittedCommandAt(idx uint64) (*cluster.Command, error) {
	cmd, ok := f.cmds[idx]
	if !ok {
		return nil, cluster.ErrLogNonCommand
	}
	return cmd, nil
}
func (f *checkpointBegetReader) AdvanceAuditPublished(to uint64) error {
	f.advanceCalls++
	if to > f.published {
		f.published = to
	}
	// Model the real raft side effect: advancing the replicated cursor is itself a
	// committed OpAuditCheckpointSet entry at the next log index.
	f.commit++
	f.cmds[f.commit] = cluster.NewCommand(cluster.OpAuditCheckpointSet)
	return nil
}

// origin: audit_publisher_review_test.go (renamed in B6) — docs/reviews/d5-review.md
//
// TestD5ReviewCheckpointOpDoesNotBegetCheckpoint proves the cursor op is not itself a
// source entry requiring another cursor op. Otherwise every idle publisher tick commits a
// fresh OpAuditCheckpointSet forever.
func TestD5ReviewCheckpointOpDoesNotBegetCheckpoint(t *testing.T) {
	f := &checkpointBegetReader{
		leader:    true,
		commit:    1,
		first:     1,
		published: 0,
		cmds:      map[uint64]*cluster.Command{1: cluster.NewCommand(cluster.OpClusterMetaSet)},
	}
	p := NewAuditPublisher(AuditPublisherConfig{Node: f})

	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if f.advanceCalls != 1 || f.published != 1 || f.commit != 2 {
		t.Fatalf("setup mismatch after first pass: advanceCalls=%d published=%d commit=%d", f.advanceCalls, f.published, f.commit)
	}

	if _, err := p.PublishOnce(context.Background()); err != nil {
		t.Fatalf("second publish over checkpoint op: %v", err)
	}
	if f.advanceCalls != 1 {
		t.Fatalf("checkpoint op begat another checkpoint advance: advanceCalls=%d, want 1", f.advanceCalls)
	}
}
