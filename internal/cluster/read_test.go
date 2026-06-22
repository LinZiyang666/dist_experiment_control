package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestFenceExceedsWorstCaseElection is the R6 invariant: T_fence must dominate the
// worst-case multinode election so it never trips on a benign election (§8.4 ①).
// Pinned against the PROD constants so a test-only timeout shortening cannot drift.
func TestFenceExceedsWorstCaseElection(t *testing.T) {
	const kFence = 10
	if TFence < kFence*MultinodeElectionTimeout {
		t.Fatalf("TFence (%v) must be >= k_fence(%d) × MultinodeElectionTimeout (%v) per §8.4",
			TFence, kFence, MultinodeElectionTimeout)
	}
}

// TestD3FailClosedBoundary tables the pure fail-closed predicate and proves it is
// NON-VACUOUS by checking the table discriminates against two deliberately-wrong
// predicates (green-after-red baked in: at least one row must disagree with each).
func TestD3FailClosedBoundary(t *testing.T) {
	base := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	last := base // last leader contact at t=base
	at := func(d time.Duration) time.Time { return last.Add(d) }

	cases := []struct {
		name  string
		state raft.RaftState
		now   time.Time
		stale bool // expected from the CORRECT predicate
	}{
		{"leader fresh regardless of clock", raft.Leader, at(1000 * time.Hour), false},
		{"follower just below T_fence allows", raft.Follower, at(TFence - time.Millisecond), false},
		{"follower exactly at T_fence allows (strict >)", raft.Follower, at(TFence), false},
		{"follower just past T_fence fences", raft.Follower, at(TFence + time.Millisecond), true},
		{"follower zero-contact fences (cold start)", raft.Follower, base, true}, // lastContact zero-value path below
		{"candidate ages like a follower, fresh", raft.Candidate, at(time.Second), false},
		{"candidate past T_fence fences", raft.Candidate, at(TFence + time.Second), true},
		{"candidate zero-contact fences (first election)", raft.Candidate, base, true}, // zero-value path below
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lc := last
			if tc.name == "follower zero-contact fences (cold start)" ||
				tc.name == "candidate zero-contact fences (first election)" {
				lc = time.Time{} // never heard a leader
			}
			if got := leaderContactStaleAt(tc.state, lc, tc.now); got != tc.stale {
				t.Fatalf("leaderContactStaleAt(%v, lc, now)=%v, want %v", tc.state, got, tc.stale)
			}
		})
	}

	// Vacuity controls: the correct table must DISAGREE with each wrong predicate
	// on at least one row — else the boundary assertions above prove nothing.
	wrongGE := func(state raft.RaftState, lastContact, now time.Time) bool {
		if state == raft.Leader {
			return false
		}
		return now.Sub(lastContact) >= TFence // ">=" instead of ">" (off-by-one at the boundary)
	}
	wrongIgnoreContact := func(state raft.RaftState, _, _ time.Time) bool {
		return state != raft.Leader // ignores LastContact entirely (always fences a follower)
	}
	for name, wrong := range map[string]func(raft.RaftState, time.Time, time.Time) bool{
		"wrongGE":            wrongGE,
		"wrongIgnoreContact": wrongIgnoreContact,
	} {
		disagreed := false
		for _, tc := range cases {
			lc := last
			if tc.name == "follower zero-contact fences (cold start)" ||
				tc.name == "candidate zero-contact fences (first election)" {
				lc = time.Time{}
			}
			if leaderContactStaleAt(tc.state, lc, tc.now) != wrong(tc.state, lc, tc.now) {
				disagreed = true
				break
			}
		}
		if !disagreed {
			t.Fatalf("vacuity: the boundary table never disagrees with %s — it has no discriminating power", name)
		}
	}
}

// TestD3LeaderContactStaleLive wires the predicate to a real 2-node mTLS cluster:
// a follower in contact is fresh; after the leader dies the survivor (minority,
// no longer a leader) is fenced under an injected future clock; and the predicate
// returns instantly (no VerifyLeader round-trip) even with quorum lost.
func TestD3LeaderContactStaleLive(t *testing.T) {
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
	var follower *Node
	var followerIdx int
	for i, n := range nodes {
		if n != leader {
			follower = n
			followerIdx = i
		}
	}

	// In-contact follower: fresh at the real now.
	if follower.LeaderContactStale(time.Now()) {
		t.Fatal("a follower in active contact with the leader must not be fenced")
	}

	// Kill the leader. The follower is now a 1-of-2 minority: it can never win an
	// election (pre-vote), so it stays non-leader with a frozen LastContact.
	for i, n := range nodes {
		if n == leader {
			_ = n.Shutdown()
			nodes[i] = nil
		}
	}
	if !waitForCond(3*time.Second, func() bool { return !follower.IsLeader() }) {
		t.Fatal("surviving minority node must not be leader after losing quorum")
	}

	// Predicate must return promptly (no VerifyLeader round-trip) even with quorum
	// lost, and must FENCE under an injected clock past T_fence.
	start := time.Now()
	stale := follower.LeaderContactStale(time.Now().Add(TFence + time.Second))
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("predicate must be a local O(1) read (no leader round-trip); took %v", elapsed)
	}
	if !stale {
		t.Fatal("after losing leader contact past T_fence, the predicate must fence (fail closed)")
	}
	// "Still fresh within T_fence" is asserted before the kill (in-contact) above and
	// exhaustively by the pure-function boundary table; we deliberately do NOT assert a
	// near-boundary live value here (it is timing-fragile under -race/slow CI because the
	// real lastContact recedes during teardown — d3-review m6).
	_ = followerIdx
}

// TestD3MultinodeRaftConfigValid (M3): the PROD multinode timeout constants must
// pass raft's own ValidateConfig (Lease <= Heartbeat <= Election). Asserting the
// numeric inequality alone (TestFenceExceedsWorstCaseElection) would not catch a
// future edit that breaks raft's ordering rule until D9 production startup.
func TestD3MultinodeRaftConfigValid(t *testing.T) {
	rc := raftConfig(Config{
		LocalID:            "prod",
		HeartbeatTimeout:   MultinodeHeartbeatTimeout,
		ElectionTimeout:    MultinodeElectionTimeout,
		LeaderLeaseTimeout: MultinodeLeaderLeaseTimeout,
	})
	if err := raft.ValidateConfig(rc); err != nil {
		t.Fatalf("multinode raftConfig must satisfy raft.ValidateConfig: %v", err)
	}
}

func waitForCond(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}
