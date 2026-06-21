package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// restartable opens a node under dir and tracks it for cleanup across an
// in-process restart (so a failure before manual Shutdown doesn't leak raft
// goroutines under -race). Call swap() before re-opening.
type restartable struct {
	t       *testing.T
	dir     string
	id      raft.ServerID
	current *Node
}

func newRestartable(t *testing.T, dir string, id raft.ServerID) *restartable {
	r := &restartable{t: t, dir: dir, id: id}
	t.Cleanup(func() {
		if r.current != nil {
			_ = r.current.Shutdown()
		}
	})
	return r
}

func (r *restartable) open() *Node {
	n, err := openNodeRaw(r.dir, r.id)
	if err != nil {
		r.t.Fatalf("open node: %v", err)
	}
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		r.t.Fatalf("wait leader: %v", err)
	}
	r.current = n
	return n
}

func (r *restartable) restart() *Node {
	if err := r.current.Shutdown(); err != nil {
		r.t.Fatalf("shutdown for restart: %v", err)
	}
	r.current = nil
	n := r.open()
	if err := n.Barrier(10 * time.Second); err != nil {
		r.t.Fatalf("post-restart barrier: %v", err)
	}
	return n
}

func mustApply(t *testing.T, n *Node, key, val string) {
	t.Helper()
	if err := n.ApplyMetaSet(key, val); err != nil {
		t.Fatalf("apply %s: %v", key, err)
	}
}

// TestSnapshotThenRestart is the non-vacuous §3.7 survival chain (replacing the
// vacuous meta.Index<=applied_index assertion): apply N ops, snapshot (truncating
// the pre-snapshot log), apply M more, restart on the SAME dir, and assert ALL
// N+M ops survived — purely from durable SQLite — and that exactly the M
// post-snapshot ops were replayed as idempotent no-ops (reapplyCount==M).
func TestSnapshotThenRestart(t *testing.T) {
	const N, M = 10, 7
	r := newRestartable(t, t.TempDir(), "a")
	n1 := r.open()

	for i := 0; i < N; i++ {
		mustApply(t, n1, fmt.Sprintf("t:a%02d", i), "v")
	}
	if err := n1.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for i := 0; i < M; i++ {
		mustApply(t, n1, fmt.Sprintf("t:b%02d", i), "v")
	}
	wantHash := clusterMetaHash(t, n1.db)

	n2 := r.restart()

	if got := clusterMetaHash(t, n2.db); got != wantHash {
		t.Fatalf("content diverged across snapshot+restart")
	}
	for i := 0; i < N; i++ {
		if _, ok := mustMetaOK(t, n2.fsm, fmt.Sprintf("t:a%02d", i)); !ok {
			t.Fatalf("pre-snapshot op t:a%02d lost after restart", i)
		}
	}
	for i := 0; i < M; i++ {
		if _, ok := mustMetaOK(t, n2.fsm, fmt.Sprintf("t:b%02d", i)); !ok {
			t.Fatalf("post-snapshot op t:b%02d lost after restart", i)
		}
	}
	// The snapshot truncated the pre-snapshot log; on restart raft replays only the
	// M post-snapshot entries, which the FSM self-skips because SQLite already has
	// them — proving survival came from durable SQLite, not re-execution.
	if rc := n2.fsm.reapplyCount.Load(); rc != uint64(M) {
		t.Fatalf("reapplyCount=%d, want %d (exactly the post-snapshot ops replayed as no-ops)", rc, M)
	}
}

// TestSnapshotAfterBarrier constructs the BENIGN §3.7 gap the corrected invariant
// allows: a LogBarrier advances raft's lastIndex past applied_index (it carries no
// mutation), so a snapshot taken right after pins meta.Index > applied_index — yet
// recovery still converges (no command is lost).
func TestSnapshotAfterBarrier(t *testing.T) {
	const N = 5
	r := newRestartable(t, t.TempDir(), "a")
	n1 := r.open()
	for i := 0; i < N; i++ {
		mustApply(t, n1, fmt.Sprintf("t:c%d", i), "v")
	}
	appliedBefore, err := n1.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	// A Barrier reaches the FSM but is not a LogCommand, so it advances raft's
	// lastIndex WITHOUT advancing applied_index.
	if err := n1.Barrier(3 * time.Second); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if err := n1.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	meta, rc := openSnapshot(t, r.dir)
	_ = rc.Close()
	if uint64(meta.Index) <= appliedBefore {
		t.Fatalf("expected the benign gap snapshot.Index(%d) > applied_index(%d) after a Barrier", meta.Index, appliedBefore)
	}
	wantHash := clusterMetaHash(t, n1.db)

	n2 := r.restart()

	if got := clusterMetaHash(t, n2.db); got != wantHash {
		t.Fatalf("recovery diverged after a snapshot taken past applied_index (barrier gap)")
	}
	for i := 0; i < N; i++ {
		if _, ok := mustMetaOK(t, n2.fsm, fmt.Sprintf("t:c%d", i)); !ok {
			t.Fatalf("op t:c%d lost across a barrier-gap snapshot", i)
		}
	}
}
