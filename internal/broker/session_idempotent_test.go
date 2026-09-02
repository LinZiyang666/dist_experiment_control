package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
)

// session_idempotent_test.go (formerly r14_session_idempotent_test.go) — Q4 (docs/reviews/r6-findings.md): in a cluster a session create
// routes through raft; the write can COMMIT on the leader but the caller's replica lags on read-back
// under a partition, and reporting that as a failure (rc=70) left `session create` structurally
// unable to go green. The fix: once proposeOrForward has committed the write, a read-back timeout is
// NON-FATAL — createSession returns a best-effort success. Duplicate rejection is UNCHANGED (the D9
// cluster tests depend on it, and a retry is indistinguishable from a fresh duplicate). These run on
// a REAL single-voter raft leader so createSession's Propose + read-back path is the actual one.

func newLeaderBrokerForSessions(t *testing.T) *Broker {
	t.Helper()
	n := newSingleVoterNode(t, "node-A")
	b := &Broker{clusterMode: true, cl: &clusterRuntime{node: n}}
	b.cfg.DB = n.RODB()
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now
	return b
}

func newSingleVoterNode(t *testing.T, id string) *cluster.Node {
	t.Helper()
	dir := t.TempDir()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	n, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID(id), DataDir: dir, DBPath: filepath.Join(dir, "state.db"),
		Transport: trans, ApplyTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	return n
}

// TestSessionCreateSucceedsAndDuplicateStillRejected pins the UNCHANGED contract the D9 cluster tests
// depend on: a routed create commits, and a second create of the same name is rejected with
// ErrAlreadyExists (proving the first committed to the FSM). Regressing this would break test/d9.
func TestSessionCreateSucceedsAndDuplicateStillRejected(t *testing.T) {
	b := newLeaderBrokerForSessions(t)

	s1, err := b.createSession("lab", "fp-owner-A", "pinhash-A")
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if s1.SID != "lab" || s1.OwnerPubkeyFP != "fp-owner-A" || s1.State != session.StateActive {
		t.Fatalf("unexpected first session: %+v", s1)
	}

	// Same owner, same name — a fresh duplicate (indistinguishable from a retry) MUST be rejected.
	if _, err := b.createSession("lab", "fp-owner-A", "pinhash-A"); !errors.Is(err, session.ErrAlreadyExists) {
		t.Fatalf("same-owner duplicate must be ErrAlreadyExists (the D9 contract), got: %v", err)
	}
	// Different owner racing the same name — also a genuine clash, rejected.
	if _, err := b.createSession("lab", "fp-owner-B", "pinhash-B"); !errors.Is(err, session.ErrAlreadyExists) {
		t.Fatalf("different-owner duplicate must be ErrAlreadyExists, got: %v", err)
	}
}

// TestSessionCreateReportsSuccessWhenCommittedButNotYetVisible is the Q4 core: when the write has
// COMMITTED but this replica's read-back cannot see it, createSession must report SUCCESS (not the
// committed-but-reported-failed rc=70). We force the divergence by pointing the broker's read DB at
// a SEPARATE empty DB while the FSM commits to the node's own DB — so the read-back times out yet the
// write is genuinely durable. That the FSM really committed (asserted via the node's own RODB) proves
// this is not a manufactured false success.
func TestSessionCreateReportsSuccessWhenCommittedButNotYetVisible(t *testing.T) {
	n := newSingleVoterNode(t, "node-B")

	emptyDB, err := storage.OpenWAL("file:" + filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyDB.Close() })

	b := &Broker{clusterMode: true, cl: &clusterRuntime{node: n}}
	b.cfg.DB = emptyDB // read-back reads THIS (empty) — models a follower whose Apply has not caught up
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now

	s, err := b.createSession("lab", "fp-owner-A", "pinhash-A")
	if err != nil {
		t.Fatalf("committed-but-not-visible create must report success, got: %v", err)
	}
	if s.SID != "lab" || s.OwnerPubkeyFP != "fp-owner-A" || s.State != session.StateActive {
		t.Fatalf("best-effort session wrong: %+v", s)
	}
	// Not a false success: the write must actually be in the FSM's committed store.
	if _, gerr := session.Get(n.RODB(), "lab"); gerr != nil {
		t.Fatalf("write was reported success but is NOT in the FSM store (false success!): %v", gerr)
	}
}

// TestSessionReadBackWindowCoversMeasuredApplyLag locks the read-back window against regression: R6
// measured 1.37s follower apply-lag under a partition. The window must comfortably exceed that yet
// stay under the 5s ctl request deadline (cmd/tether/session.go) so the broker's reply still lands.
func TestSessionReadBackWindowCoversMeasuredApplyLag(t *testing.T) {
	window := time.Duration(sessionReadBackAttempts) * sessionReadBackInterval
	if window < 3*time.Second {
		t.Fatalf("read-back window %v is too short: R6 measured 1.37s apply-lag; want >= 3s", window)
	}
	if window >= 5*time.Second {
		t.Fatalf("read-back window %v must stay under the 5s ctl request deadline", window)
	}
}
