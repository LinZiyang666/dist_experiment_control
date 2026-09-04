package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
)

// TestFSM_NotConfigurationStore guards the LogCommand-only invariant: the
// "applied_index tracks the last COMMAND index" property depends on the FSM NOT
// implementing raft.ConfigurationStore (which would route config entries through
// a different path and bump raft lastIndex). It also must not be a BatchingFSM in
// D1. D3 (membership / AddVoter) must reckon with the lastIndex-vs-applied_index
// gap when it touches this.
func TestFSM_NotConfigurationStore(t *testing.T) {
	if _, ok := any((*fsm)(nil)).(raft.ConfigurationStore); ok {
		t.Fatal("*fsm must NOT implement raft.ConfigurationStore in D1")
	}
	if _, ok := any((*fsm)(nil)).(raft.BatchingFSM); ok {
		t.Fatal("*fsm must NOT implement raft.BatchingFSM in D1")
	}
}

// TestNode_SnapshotNothingNewContract pins the N=1 snapshot contract so D2/D3 know
// it: Snapshot() before any committed op wraps raft.ErrNothingNewToSnapshot, then
// succeeds after one op.
func TestNode_SnapshotNothingNewContract(t *testing.T) {
	n := mustNode(t, t.TempDir(), "s")
	if err := n.Snapshot(); !errors.Is(err, raft.ErrNothingNewToSnapshot) {
		t.Fatalf("Snapshot before any op must wrap ErrNothingNewToSnapshot, got %v", err)
	}
	mustApply(t, n, "t:x", "1")
	if err := n.Snapshot(); err != nil {
		t.Fatalf("Snapshot after one op must succeed: %v", err)
	}
}

// TestNode_SnapshotForJoinSwallowsNothingNew pins the grow-path contract: SnapshotForJoin
// must NOT surface ErrNothingNewToSnapshot (an existing snapshot already covers the state —
// exactly what the joiner installs). The driveJoin grow path calls it before staging a joiner
// so the joiner catches up via InstallSnapshot, not log replay (grow-onto-migrated-leader fix).
func TestNode_SnapshotForJoinSwallowsNothingNew(t *testing.T) {
	n := mustNode(t, t.TempDir(), "s")
	// Fresh node: Snapshot() would wrap ErrNothingNewToSnapshot, but SnapshotForJoin swallows it.
	if err := n.SnapshotForJoin(); err != nil {
		t.Fatalf("SnapshotForJoin on a fresh node must swallow ErrNothingNewToSnapshot, got %v", err)
	}
	mustApply(t, n, "t:x", "1")
	if err := n.SnapshotForJoin(); err != nil {
		t.Fatalf("SnapshotForJoin after an op must succeed: %v", err)
	}
}

// TestIsNotLeaderRecognizesEveryUncommittedWriteError pins the SEMANTIC predicate
// behind IsNotLeader: every raft error that means "this write was not committed by
// a leader, retry" must be recognized, and errors that do NOT mean that must not be.
//
// origin: prerelease audit broker-cluster-write/L3-F1 (+ its verifier's second
// sentinel). The negative half is load-bearing in its own right: it pins the
// boundary against the opposite over-correction, "make every raft error retriable".
// ErrRaftShutdown is the canonical thing that must stay non-retriable — this
// process is going away, so a caller looping on it would spin forever.
func TestIsNotLeaderRecognizesEveryUncommittedWriteError(t *testing.T) {
	retriable := []error{
		raft.ErrNotLeader,
		raft.ErrLeadershipLost,
		// A drain / retire / transfer-leader rejects EVERY Apply with this while
		// the old leader still reports State()==Leader. Falling through the
		// predicate makes handleRegister answer store_error, which the agent's
		// register loop treats as authoritative and EXITS the process on.
		raft.ErrLeadershipTransferInProgress,
		// The log never entered applyCh, so it was definitely not committed.
		raft.ErrEnqueueTimeout,
	}
	for _, err := range retriable {
		if !IsNotLeader(err) {
			t.Errorf("IsNotLeader(%v) = false; a write that was not committed by a leader "+
				"must be retriable, else the broker answers store_error and the agent exits", err)
		}
		// Wrapped, because every real call site sees it through fmt.Errorf("%w").
		if !IsNotLeader(fmt.Errorf("cluster: apply: %w", err)) {
			t.Errorf("IsNotLeader(wrapped %v) = false; call sites always see it wrapped", err)
		}
	}
	notRetriable := []error{
		raft.ErrRaftShutdown, // retrying inside a dying process is meaningless
		errors.New("unrelated"),
	}
	for _, err := range notRetriable {
		if IsNotLeader(err) {
			t.Errorf("IsNotLeader(%v) = true; the predicate must not swallow errors that are "+
				"NOT 'uncommitted, retry' — that would turn a fatal condition into an infinite loop", err)
		}
	}
	if IsNotLeader(nil) {
		t.Error("IsNotLeader(nil) = true")
	}
}
