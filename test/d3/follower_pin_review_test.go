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
			HeartbeatTimeout:   80 * time.Millisecond,
			ElectionTimeout:    80 * time.Millisecond,
			LeaderLeaseTimeout: 40 * time.Millisecond,
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

	leaderIdx := -1
	if !waitForCond(5*time.Second, func() bool {
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

	err := nodes[followerIdx].Propose(func(db *sql.DB) (*cluster.Command, error) {
		return agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", fp, "test-pin", auth.VerifyPIN, time.Now())
	})
	if !cluster.IsNotLeader(err) {
		t.Fatalf("follower PIN write must classify as transient not_leader before local Plan business errors; got %T %v", err, err)
	}
	if errors.Is(err, agentprov.ErrSessionMissing) {
		t.Fatalf("stale follower local replica leaked as permanent ErrSessionMissing instead of transient not_leader")
	}
}
