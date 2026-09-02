package d3_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
)

// origin: follower_pin_review_test.go (renamed in B6) — docs/reviews/d3-review.md
//
// TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review is a reviewer-owned
// regression for D3-R3. The D3 contract says a follower PIN write must be a
// transient not_leader deny, not a permanent business error and not an
// unreplicated local write. A follower may have a stale local replica; if the seam
// simply calls Node.Propose(PlanProvisionWithPIN), Propose runs Plan on the
// follower DB before raft.Apply can return ErrNotLeader. That incorrectly turns
// follower lag into ErrSessionMissing.
func TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review(t *testing.T) {
	addrs := make([]raft.ServerAddress, 2)
	trans := make([]*raft.InmemTransport, 2)
	for i := range trans {
		addrs[i], trans[i] = raft.NewInmemTransport("")
	}
	for i := range trans {
		for j := range trans {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}

	ids := []raft.ServerID{"review-a", "review-b"}
	servers := []raft.Server{
		{Suffrage: raft.Voter, ID: ids[0], Address: addrs[0]},
		{Suffrage: raft.Voter, ID: ids[1], Address: addrs[1]},
	}
	dirs := []string{t.TempDir(), t.TempDir()}
	nodes := make([]*cluster.Node, 2)
	for i := range nodes {
		n, err := cluster.New(cluster.Config{
			LocalID:            ids[i],
			DataDir:            dirs[i],
			DBPath:             filepath.Join(dirs[i], "state.db"),
			Transport:          trans[i],
			BootstrapPeers:     servers,
			HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
			ElectionTimeout:    cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
			ApplyTimeout:       2 * time.Second,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", ids[i], err)
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

	// The assertion below is only meaningful while the node we poke is genuinely a
	// FOLLOWER. Observing leadership and then acting on that observation is a
	// race: leadership can move in between, and then Propose runs on the real
	// leader, which legitimately executes the business plan and reports
	// ErrSessionMissing (the seed only wrote the other node's DB). That is
	// precisely the symptom this test saw under a 20-worker parallel load.
	//
	// The fix is to re-establish the premise, NOT to relax the assertion: the
	// contract under test ("a follower PIN write is a transient not_leader deny")
	// is unchanged, it simply cannot be evaluated on a node that is no longer a
	// follower. Leadership is therefore confirmed immediately before AND after the
	// call, and the whole observe-then-act sequence is retried when it moved.
	// A test that instead accepted ErrSessionMissing "because leadership may have
	// changed" would assert nothing at all.
	//
	// That observe → act → re-observe → retry loop was hand-written here first; it is now
	// clusterharness.WithLeader (the T3 primitive), which binds fn to whichever node leads NOW and
	// retries — discarding fn's RESULT, not its side effects — if that node no longer leads when fn
	// returns. What that proves is exactly two readings: the node led before fn and leads after it.
	// It does not rule out an A→B→A move inside fn (no term is compared), so in this two-node
	// cluster the other node was "a follower at both readings", not "a follower throughout"; the
	// hand-written loop proved no more (external review, suggestion 1). fn here is idempotent under
	// retry: the seed is an upsert and the proposal is what is being measured. This test is the
	// primitive's first suite caller (test/determinism/leader_premise_test.go keeps it that way).
	probes := make([]clusterharness.LeaderProbe, len(nodes))
	for i, n := range nodes {
		probes[i] = n
	}
	var err error
	retries, werr := clusterharness.WithLeader(t, probes, 10*time.Second, func(leaderIdx int) error {
		followerIdx := 1 - leaderIdx
		// Model a bounded-stale follower: the leader already has the session, but this
		// follower has not applied that session row yet.
		seedSession(t, filepath.Join(dirs[leaderIdx], "state.db"), "lab", "test-pin")
		_, fp := freshClient(t)
		err = nodes[followerIdx].Propose(func(db *sql.DB) (*cluster.Command, error) {
			return agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", fp, "test-pin", auth.VerifyPIN, time.Now())
		})
		return nil
	})
	if werr != nil {
		t.Fatalf("could not evaluate the follower PIN write against a stable leader (%d retr(y/ies)): %v", retries, werr)
	}
	if !cluster.IsNotLeader(err) {
		t.Fatalf("follower PIN write must classify as transient not_leader before local Plan business errors; got %T %v", err, err)
	}
	if errors.Is(err, agentprov.ErrSessionMissing) {
		t.Fatalf("stale follower local replica leaked as permanent ErrSessionMissing instead of transient not_leader")
	}
}
