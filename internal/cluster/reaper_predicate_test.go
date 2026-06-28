package cluster

import (
	"testing"
	"time"
)

// TestRaftAppliedIndexCatchesUpToCommitOnLeader pins the v0.4.4-review G-reaper fix at the cluster layer.
// The broker's reaperMayDelete caught-up gate compares RaftAppliedIndex (RAFT domain — advances on the
// election LogNoop + config entries) against CommitIndex, NOT the command-domain AppliedIndex (advances
// ONLY on a LogCommand, via fsm.Apply). hashicorp/raft appends a LogNoop on every leadership win that
// bumps CommitIndex but is FSM-ignored, so on a steady leader the command-domain AppliedIndex sits
// PERMANENTLY below CommitIndex — which made the original `AppliedIndex >= CommitIndex` predicate
// structurally false and SILENTLY DISABLED the boot orphan reaper in cluster mode (a leader never reaped).
// This asserts the raft-domain predicate DOES converge, while the command-domain one does NOT.
func TestRaftAppliedIndexCatchesUpToCommitOnLeader(t *testing.T) {
	n := mustNode(t, t.TempDir(), "n-reaper")

	var raftApplied, commit uint64
	caughtUp := false
	for i := 0; i < 100; i++ { // the leader applies the election LogNoop shortly after winning
		raftApplied = n.RaftAppliedIndex()
		commit = n.CommitIndex()
		if commit > 0 && raftApplied >= commit {
			caughtUp = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatalf("raft-domain caught-up predicate never held: RaftAppliedIndex=%d CommitIndex=%d "+
			"(reaperMayDelete would never let a real leader reap)", raftApplied, commit)
	}

	// Prove WHY the old predicate was broken: the command-domain AppliedIndex is BELOW CommitIndex on a
	// fresh leader (the election LogNoop bumped commit but never reached fsm.Apply), so the original
	// `AppliedIndex >= CommitIndex` gate would be false here — the exact structural inertness the fix cures.
	cmdApplied, err := n.AppliedIndex()
	if err != nil {
		t.Fatalf("AppliedIndex: %v", err)
	}
	if cmdApplied >= commit {
		t.Fatalf("expected command-domain AppliedIndex(%d) < CommitIndex(%d) on a fresh leader (the LogNoop is "+
			"FSM-ignored); if equal, the old gate was not actually broken — re-examine the fix", cmdApplied, commit)
	}
}
