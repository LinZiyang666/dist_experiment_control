package cluster

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
)

// TestD5TrailingLogsMatchesRaft pins cluster.TrailingLogs to raft's actual default so the
// §6.3 R-7 lag bound cannot silently drift if raftConfig ever overrides TrailingLogs.
func TestD5TrailingLogsMatchesRaft(t *testing.T) {
	if TrailingLogs != raft.DefaultConfig().TrailingLogs {
		t.Fatalf("cluster.TrailingLogs=%d != raft.DefaultConfig().TrailingLogs=%d", TrailingLogs, raft.DefaultConfig().TrailingLogs)
	}
}

// TestD5AuditCheckpointMonotonic proves the §6.3 replicated audit-publish cursor advances
// monotonically and a REGRESS is a no-op — enforced by the FSM-baked WHERE guard (R-3),
// NOT merely the leader-side skip. The direct-Apply of a lower value (simulating a stale
// ex-leader that bypassed AdvanceAuditPublished's skip) must leave the cursor unchanged on
// the replica.
func TestD5AuditCheckpointMonotonic(t *testing.T) {
	n := mustNode(t, t.TempDir(), raft.ServerID("ckpt"))

	if got, _ := n.AuditPublishedIndex(); got != 0 {
		t.Fatalf("initial checkpoint = %d, want 0", got)
	}
	if err := n.AdvanceAuditPublished(5); err != nil {
		t.Fatalf("advance to 5: %v", err)
	}
	if got, _ := n.AuditPublishedIndex(); got != 5 {
		t.Fatalf("checkpoint = %d, want 5", got)
	}
	// Leader-side skip (R-5): to <= cur proposes nothing.
	if err := n.AdvanceAuditPublished(5); err != nil {
		t.Fatalf("advance to 5 again: %v", err)
	}
	if err := n.AdvanceAuditPublished(3); err != nil {
		t.Fatalf("advance to 3 (skipped): %v", err)
	}
	if got, _ := n.AuditPublishedIndex(); got != 5 {
		t.Fatalf("after no-op advances checkpoint = %d, want 5", got)
	}
	// FSM GUARD (R-3): Apply a LOWER value directly, bypassing the leader-side skip — the
	// baked WHERE makes it a no-op on the replica (a stale ex-leader cannot rewind).
	low, err := planAuditCheckpointSet(2)
	if err != nil {
		t.Fatalf("plan low: %v", err)
	}
	if err := n.Apply(low); err != nil {
		t.Fatalf("apply low: %v", err)
	}
	if got, _ := n.AuditPublishedIndex(); got != 5 {
		t.Fatalf("FSM guard failed: a lower Apply rewound the cursor to %d (want 5)", got)
	}
	// A higher value advances.
	if err := n.AdvanceAuditPublished(8); err != nil {
		t.Fatalf("advance to 8: %v", err)
	}
	if got, _ := n.AuditPublishedIndex(); got != 8 {
		t.Fatalf("checkpoint = %d, want 8", got)
	}
}

// TestD5AdvanceAuditPublishedNotLeader proves a non-leader's checkpoint advance is
// rejected (raft.ErrNotLeader family) rather than silently corrupting the cursor.
func TestD5AdvanceAuditPublishedNotLeader(t *testing.T) {
	n := mustNode(t, t.TempDir(), raft.ServerID("ckpt-nl"))
	if err := n.Shutdown(); err != nil { // a shut-down node is not a leader
		t.Fatalf("shutdown: %v", err)
	}
	err := n.AdvanceAuditPublished(9)
	if err == nil {
		t.Fatal("advance on a non-leader must fail")
	}
}

// TestD5ReadPrimitives exercises the committed-log readers the publisher consumes (R-4/R-6).
func TestD5ReadPrimitives(t *testing.T) {
	n := mustNode(t, t.TempDir(), raft.ServerID("read"))

	// Commit a known op so there is a LogCommand entry to read back.
	if err := n.ApplyMetaSet("t:probe", "v"); err != nil {
		t.Fatalf("apply meta: %v", err)
	}
	ci := n.CommitIndex()
	if ci == 0 {
		t.Fatal("CommitIndex = 0 after a commit")
	}
	first, err := n.LogFirstIndex()
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	last, err := n.LogLastIndex()
	if err != nil {
		t.Fatalf("last: %v", err)
	}
	if first == 0 || last < ci {
		t.Fatalf("log range [%d,%d] inconsistent with commit %d", first, last, ci)
	}
	// The latest committed entry is our ClusterMetaSet.
	cmd, err := n.CommittedCommandAt(ci)
	if err != nil {
		t.Fatalf("CommittedCommandAt(%d): %v", ci, err)
	}
	if cmd.Op != OpClusterMetaSet {
		t.Fatalf("committed op = %q, want %q", cmd.Op, OpClusterMetaSet)
	}
	// The bootstrap configuration entry (index 1) is a non-command.
	if _, err := n.CommittedCommandAt(1); !errors.Is(err, ErrLogNonCommand) {
		t.Fatalf("index 1 = %v, want ErrLogNonCommand", err)
	}
	// Index 0 is below FirstIndex => truncated/absent.
	if _, err := n.CommittedCommandAt(0); !errors.Is(err, ErrLogTruncated) {
		t.Fatalf("index 0 = %v, want ErrLogTruncated", err)
	}
	if nv, err := n.NumVoters(); err != nil || nv != 1 {
		t.Fatalf("NumVoters = %d, %v; want 1, nil", nv, err)
	}
}
