package cluster

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestCaughtUpRequiresFirstLeaderSync (external re-review lock-reap-F2) pins the commit>0 conjunct of
// Node.CaughtUp(): a node in a 2-voter config whose peer transport never exists can never reach a
// quorum, so it never wins an election and its volatile CommitIndex stays 0 forever. The bare
// applied>=commit predicate is VACUOUSLY true there (0>=0) — the exact pre-first-commit window a
// snapshot-carrying restart or a never-elected node hits — and would let a reaper act on an empty
// view. CaughtUp() must reject it. The discrimination guard (the OLD predicate IS true) proves the
// test actually exercises the commit>0 bound and is not vacuously green.
func TestCaughtUpRequiresFirstLeaderSync(t *testing.T) {
	dir := t.TempDir()
	_, trans := raft.NewInmemTransport("solo-of-two")
	n, err := New(Config{
		LocalID:            "solo-of-two",
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          trans,
		Logger:             quietLogger(),
		ApplyTimeout:       30 * time.Second,
		HeartbeatTimeout:   MultinodeHeartbeatTimeout,
		ElectionTimeout:    MultinodeElectionTimeout,
		LeaderLeaseTimeout: MultinodeLeaderLeaseTimeout,
		BootstrapPeers: []raft.Server{
			{Suffrage: raft.Voter, ID: "solo-of-two", Address: "solo-of-two"},
			{Suffrage: raft.Voter, ID: "phantom-peer", Address: "phantom-unreachable"},
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })

	// Let many election cycles pass; with no reachable second voter none can win, so commit stays 0.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if n.CommitIndex() != 0 {
			t.Fatalf("a node that can never reach quorum must never advance CommitIndex, got %d", n.CommitIndex())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n.CaughtUp() {
		t.Fatal("CaughtUp() must be false before the first leader sync (commit==0); the commit>0 bound is missing")
	}
	// Discrimination: the OLD bare predicate is vacuously true here — without the commit>0 bound the
	// gate would pass. If this assert ever fails, the fixture stopped producing the 0>=0 window and the
	// test no longer proves anything.
	oldBarePredicate := n.RaftAppliedIndex() >= n.CommitIndex()
	if !oldBarePredicate {
		t.Fatal("expected the bare applied>=commit predicate to be vacuously true (0>=0) — test does not discriminate")
	}
}

// TestCaughtUpIslandedFollowerFrozenCommitResidual is an EXECUTABLE HONESTY PIN for CaughtUp's HONEST
// LIMIT #2: an islanded follower whose CommitIndex froze at a POSITIVE value (and whose applied caught
// that frozen ceiling before it was cut off) still reads CaughtUp()==true on a stale view. That case is
// deliberately fenced by the WRITE path (a quorum-less Propose cannot commit; a JS delete fails without
// quorum), not by this predicate. If someone later adds a leader-contact freshness bound to CaughtUp,
// this test FLIPS — and they must update it deliberately rather than silently changing reap semantics.
func TestCaughtUpIslandedFollowerFrozenCommitResidual(t *testing.T) {
	ca := newTestCA(t)
	ids := []raft.ServerID{"fc-a", "fc-b"}
	trans := make([]*raft.NetworkTransport, len(ids))
	dirs := make([]string, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		trans[i] = tr
		dirs[i] = t.TempDir()
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: tr.LocalAddr()}
	}
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		n, err := New(fastMultinodeCfg(id, dirs[i], trans[i], servers))
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Shutdown()
			}
		}
	})

	leader := waitNodeLeader(t, nodes, 6*time.Second)
	// Commit one entry so both nodes carry a POSITIVE commit index.
	if err := leader.Propose(func(*sql.DB) (*Command, error) { return planAuditCheckpointSet(1) }); err != nil {
		t.Fatalf("propose: %v", err)
	}
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	if !waitForCond(3*time.Second, func() bool { return follower.CommitIndex() > 0 && follower.CaughtUp() }) {
		t.Fatalf("follower never became caught up with a positive commit: commit=%d applied=%d",
			follower.CommitIndex(), follower.RaftAppliedIndex())
	}

	// Kill the leader → the follower is a 1-of-2 minority whose CommitIndex is frozen at a positive value.
	for i, n := range nodes {
		if n == leader {
			_ = n.Shutdown()
			nodes[i] = nil
		}
	}
	if !waitForCond(3*time.Second, func() bool { return !follower.IsLeader() }) {
		t.Fatal("surviving minority node must not be leader after losing quorum")
	}
	if !follower.CaughtUp() {
		t.Fatal("KNOWN RESIDUAL: an islanded follower with a frozen positive commit reads CaughtUp()==true. " +
			"If this now fails, a leader-contact freshness bound was added to CaughtUp — update this pin and the " +
			"reaper comments deliberately (the write-path backstop no longer stands alone).")
	}
}

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
	// Tie the hand-transcribed predicate above to the shipped SSOT method so they can never drift
	// (the drift the external review caught: this test guarded commit>0, production did not).
	if !n.CaughtUp() {
		t.Fatal("Node.CaughtUp() must agree with the hand-transcribed commit>0 && applied>=commit predicate on a healthy leader")
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
