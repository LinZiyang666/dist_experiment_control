package broker

import "testing"

// TestReaperMayDeleteGate (v0.4.4 review G) pins the boot orphan-reaper DELETE gate's cheap branches:
// single mode (no cluster runtime) ALWAYS reaps — the local DB is the authority; cluster mode with no live
// raft node wired (mis-wired / pre-leadership) must NOT reap, because a fresh joiner's stale/empty local
// view would classify every cluster-wide history-<sid> / OBJ_xfer bucket as orphan and WIPE it. The
// leader-AND-caught-up positive path (RaftAppliedIndex >= CommitIndex) is proven at the cluster layer in
// TestRaftAppliedIndexCatchesUpToCommitOnLeader (the command-domain predicate was structurally inert).
func TestReaperMayDeleteGate(t *testing.T) {
	if single := (&Broker{}); !single.reaperMayDelete() {
		t.Fatal("single-mode broker (b.cl == nil) must reap orphans — the local DB is the authority")
	}
	if noNode := (&Broker{cl: &clusterRuntime{}}); noNode.reaperMayDelete() {
		t.Fatal("cluster-mode broker with a nil raft node must NOT reap (would wipe cluster history from a stale view)")
	}
}
