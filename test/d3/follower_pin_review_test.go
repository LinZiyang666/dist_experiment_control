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
	"github.com/hashicorp/raft"
)

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
	var err error
	const attempts = 5
	for attempt := 1; ; attempt++ {
		leaderIdx := -1
		if !waitForCond(10*time.Second, func() bool {
			for i, n := range nodes {
				if n.IsLeader() {
					leaderIdx = i
					return true
				}
			}
			return false
		}) {
			t.Fatal("no leader elected")
		}
		followerIdx := 1 - leaderIdx

		// Model a bounded-stale follower: the leader already has the session, but this
		// follower has not applied that session row yet.
		seedSession(t, filepath.Join(dirs[leaderIdx], "state.db"), "lab", "test-pin")
		_, fp := freshClient(t)

		if nodes[followerIdx].IsLeader() {
			if attempt == attempts {
				t.Fatalf("leadership kept moving across %d attempts; cannot establish a stable follower", attempts)
			}
			continue
		}
		err = nodes[followerIdx].Propose(func(db *sql.DB) (*cluster.Command, error) {
			return agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", fp, "test-pin", auth.VerifyPIN, time.Now())
		})
		if nodes[followerIdx].IsLeader() {
			// It became the leader while we were proposing, so whatever came back
			// says nothing about follower behaviour. Not a pass, not a failure.
			if attempt == attempts {
				t.Fatalf("target became leader mid-Propose on all %d attempts", attempts)
			}
			continue
		}
		break
	}
	if !cluster.IsNotLeader(err) {
		t.Fatalf("follower PIN write must classify as transient not_leader before local Plan business errors; got %T %v", err, err)
	}
	if errors.Is(err, agentprov.ErrSessionMissing) {
		t.Fatalf("stale follower local replica leaked as permanent ErrSessionMissing instead of transient not_leader")
	}
}
