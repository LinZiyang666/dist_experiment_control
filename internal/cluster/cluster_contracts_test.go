package cluster

import (
	"errors"
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
