package broker

import (
	"strings"
	"testing"
)

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

// TestReaperCaughtUpGate (#58/P10) pins reaperCaughtUp — the LEADER-NEUTRAL sibling of
// reaperMayDelete used by the xfer-object reaper, whose blast radius is partitioned by
// homeOwnsXferBucket rather than by leader-exclusivity. Single mode always reaps (the local
// DB is the authority); a cluster broker with no wired raft node is NOT caught up and must
// refuse (an empty/stale view would classify live cluster-wide objects as orphan). The
// DISTINCTION from reaperMayDelete — that a caught-up NON-LEADER (a follower that is a
// session's home) may reap — is the whole #58 fix and is verified end-to-end at the deploy
// tier (drill 96), where a real non-leader home exists; a caught-up follower cannot be built
// hermetically (d7SingleNode always bootstraps as leader), so the raft-index positive path is
// the same accessor comparison reaperMayDelete's is (TestRaftAppliedIndexCatchesUpToCommitOnLeader).
func TestReaperCaughtUpGate(t *testing.T) {
	if single := (&Broker{}); !single.reaperCaughtUp() {
		t.Fatal("single-mode broker (b.cl == nil) must be allowed to reap — the local DB is the authority")
	}
	if noNode := (&Broker{cl: &clusterRuntime{}}); noNode.reaperCaughtUp() {
		t.Fatal("cluster-mode broker with a nil raft node must NOT be considered caught up (would reap from a stale view)")
	}
}

// TestMembershipLockReapsGateOnCatchUp (external review M-2): the grow/upgrade lock reapers must gate on
// reaperCaughtUp, so a freshly-elected leader replaying a long committed tail (RaftAppliedIndex <
// CommitIndex under FSM dispatch backpressure) does NOT reap a live membership lock off a stale view.
//
// This is a SOURCE-PIN, deliberately. A fully behavioral lagging-leader test is INFEASIBLE-AS-SPECCED:
// (1) the hermetic fixtures always bootstrap caught-up; and (2) the residual reaperCaughtUp does NOT
// cover — a SINGLE committed-but-unapplied renewal in the µs–ms SQLite-apply window (RaftAppliedIndex
// advances at FSM DISPATCH, not Apply-return) — cannot be closed by a raft Barrier before the clear,
// because Barrier().Error() has no apply deadline and would HANG the reconcile goroutine on a wedged
// FSM (verified: it blocks indefinitely). That narrower window is a recorded, backstopped residual (see
// reconcile_upgrade_lock.go), not a testable behavior here. The accessor-level positive path is proven
// at the cluster layer (TestRaftAppliedIndexCatchesUpToCommitOnLeader / Node.CaughtUp tests).
func TestMembershipLockReapsGateOnCatchUp(t *testing.T) {
	for _, f := range []string{"reconcile_upgrade_lock.go", "reconcile_grow_lock.go"} {
		src := readRepoFile(t, f)
		if !strings.Contains(src, "b.reaperCaughtUp()") {
			t.Fatalf("%s no longer gates its reap on b.reaperCaughtUp() — a lagging new leader could reap a live "+
				"membership lock from a stale view (M-2)", f)
		}
	}
}
