package cluster

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// PreVote merge gate (D0, §3.1/§18.2.14). This is a DEPENDENCY-VERIFICATION
// test: it stands up an inmem hashicorp/raft cluster with a throwaway MockFSM
// purely to prove the pinned raft version's pre-vote actually works at runtime
// ("配置字段存在 ≠ 行为生效"). It is NOT the D1 product FSM.
//
// Doc correction landed in D0: raft v1.7.x has NO `Config.PreVote` field — the
// real field is `Config.PreVoteDisabled bool` (DefaultConfig leaves it false →
// pre-vote enabled). See §3.1/§18.2.14.

// transport-capability guard: raft silently disables pre-vote when the
// transport does not implement WithPreVote (api.go:
// preVoteDisabled = conf.PreVoteDisabled || !transportSupportPreVote). A
// PreVote test on a non-WithPreVote transport would pass for the WRONG reason,
// so make the capability a compile-time invariant of the gate.
var _ raft.WithPreVote = (*raft.InmemTransport)(nil)

type prevoteNode struct {
	id    raft.ServerID
	addr  raft.ServerAddress
	r     *raft.Raft
	trans *raft.InmemTransport
}

func prevoteConfig(id raft.ServerID, preVoteDisabled bool) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = id
	// Self-consistent sub-second timeouts: ElectionTimeout >= HeartbeatTimeout
	// and LeaderLeaseTimeout <= HeartbeatTimeout are required by raft.
	c.HeartbeatTimeout = 50 * time.Millisecond
	c.ElectionTimeout = 50 * time.Millisecond
	c.LeaderLeaseTimeout = 50 * time.Millisecond
	c.CommitTimeout = 5 * time.Millisecond
	c.PreVoteDisabled = preVoteDisabled
	c.LogOutput = io.Discard // silence raft's election chatter in test output
	return c
}

// newPrevoteCluster bootstraps a 3-node inmem raft cluster and returns the
// nodes. All transports are fully connected. Nodes are Shutdown on cleanup.
func newPrevoteCluster(t *testing.T, preVoteDisabled bool) []*prevoteNode {
	t.Helper()
	ids := []raft.ServerID{"prevote-a", "prevote-b", "prevote-c"}
	nodes := make([]*prevoteNode, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		addr, trans := raft.NewInmemTransport("")
		nodes[i] = &prevoteNode{id: id, addr: addr, trans: trans}
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: addr}
	}
	// Fully connect every transport to every peer (two-way is implicit per
	// pair since we visit both (i,j) and (j,i)).
	for i := range nodes {
		for j := range nodes {
			if i != j {
				nodes[i].trans.Connect(nodes[j].addr, nodes[j].trans)
			}
		}
	}
	cfg := raft.Configuration{Servers: servers}
	for _, n := range nodes {
		store := raft.NewInmemStore() // implements both LogStore and StableStore
		snaps := raft.NewInmemSnapshotStore()
		if err := raft.BootstrapCluster(
			prevoteConfig(n.id, preVoteDisabled), store, store, snaps, n.trans, cfg,
		); err != nil {
			t.Fatalf("bootstrap %s: %v", n.id, err)
		}
		r, err := raft.NewRaft(
			prevoteConfig(n.id, preVoteDisabled), &raft.MockFSM{}, store, store, snaps, n.trans,
		)
		if err != nil {
			t.Fatalf("NewRaft %s: %v", n.id, err)
		}
		n.r = r
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.r.Shutdown().Error()
		}
	})
	return nodes
}

func waitForLeader(t *testing.T, nodes []*prevoteNode, within time.Duration) *prevoteNode {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.r.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", within)
	return nil
}

// isolate two-way-disconnects n from every other node; heal reconnects them.
func isolate(target *prevoteNode, all []*prevoteNode) {
	for _, n := range all {
		if n != target {
			target.trans.Disconnect(n.addr)
			n.trans.Disconnect(target.addr)
		}
	}
}

func heal(target *prevoteNode, all []*prevoteNode) {
	for _, n := range all {
		if n != target {
			target.trans.Connect(n.addr, n.trans)
			n.trans.Connect(target.addr, target.trans)
		}
	}
}

func firstFollower(leader *prevoteNode, all []*prevoteNode) *prevoteNode {
	for _, n := range all {
		if n != leader {
			return n
		}
	}
	return nil
}

// TestPreVotePreventsTermInflation is the D0 merge gate. Pre-vote's DIRECT,
// deterministically-observable effect is that an isolated follower runs pre-vote
// campaigns that can never reach majority and therefore NEVER increments its own
// term — so it can't disrupt the cluster when it rejoins. With pre-vote ENABLED
// (default), the isolated follower's CurrentTerm() stays at termBefore, and the
// healthy leader (still holding 2/3 quorum) stays leader at the same term across
// the partition and after the follower reconnects.
//
// The discriminating signal is the ISOLATED NODE's own term, not the leader's:
// in the inmem harness the leader's term is undisturbed under BOTH pre-vote
// settings (the rejoin-disruption is not deterministically reproducible here),
// so a leader-term assertion alone has no discriminating power. See §18.2.14.
func TestPreVotePreventsTermInflation(t *testing.T) {
	nodes := newPrevoteCluster(t, false) // pre-vote enabled
	leader := waitForLeader(t, nodes, 3*time.Second)
	termBefore := leader.r.CurrentTerm()

	f := firstFollower(leader, nodes)
	isolate(f, nodes)
	// Several election timeouts: f's pre-vote campaigns fail (refused by the
	// unreachable majority) WITHOUT ever incrementing its real term.
	time.Sleep(1500 * time.Millisecond)

	// PRIMARY (discriminating) assertion: pre-vote kept the isolated node's term
	// pinned at termBefore.
	if ft := f.r.CurrentTerm(); ft != termBefore {
		t.Fatalf("pre-vote must prevent the isolated follower from inflating its term: "+
			"termBefore=%d isolatedTerm=%d", termBefore, ft)
	}
	// Secondary invariant: the healthy leader is undisturbed.
	if got := leader.r.State(); got != raft.Leader {
		t.Fatalf("healthy leader must not step down while a single follower is isolated; state=%v", got)
	}
	if lt := leader.r.CurrentTerm(); lt != termBefore {
		t.Fatalf("healthy leader's term must not rise while a single follower is isolated: "+
			"termBefore=%d leaderTerm=%d", termBefore, lt)
	}

	heal(f, nodes)
	time.Sleep(500 * time.Millisecond)

	if got := leader.r.State(); got != raft.Leader {
		t.Fatalf("leader stepped down after the isolated follower reconnected; state=%v", got)
	}
	if lt := leader.r.CurrentTerm(); lt != termBefore {
		t.Fatalf("leader term changed after a clean pre-vote rejoin: termBefore=%d termAfter=%d", termBefore, lt)
	}
	// Leader IDENTITY must be continuous, not merely "some node is leader at the
	// same term": a clean pre-vote rejoin must not have triggered a re-election.
	if addr, id := leader.r.LeaderWithID(); id != leader.id || addr != leader.addr {
		t.Fatalf("leader identity must be continuous across a clean pre-vote rejoin: got (%q,%q), want (%q,%q)",
			addr, id, leader.addr, leader.id)
	}
}

// TestPreVoteDisabledControlInflatesTerm is the reverse control that proves the
// gate has discriminating power (is not a恒真 no-op). With pre-vote DISABLED,
// the isolated follower keeps timing out and incrementing ITS OWN term on every
// election round; its term climbs well above termBefore. If this control did NOT
// inflate, the gate above would be meaningless.
func TestPreVoteDisabledControlInflatesTerm(t *testing.T) {
	nodes := newPrevoteCluster(t, true) // pre-vote DISABLED
	leader := waitForLeader(t, nodes, 3*time.Second)
	termBefore := leader.r.CurrentTerm()

	f := firstFollower(leader, nodes)
	isolate(f, nodes)
	// With no pre-vote gate, f bumps its own term on every election timeout.
	time.Sleep(1500 * time.Millisecond)

	// Require a MEANINGFUL margin, not just +1: a degenerate control that
	// inflated by a single term would have near-zero discriminating power.
	// Empirically the isolated follower reaches ~20+ over 1.5s of 50ms timeouts.
	if ft := f.r.CurrentTerm(); ft < termBefore+3 {
		t.Fatalf("control: with pre-vote disabled, an isolated follower should inflate its "+
			"own term by a clear margin, but termBefore=%d isolatedTerm=%d "+
			"(the PreVote gate would have near-zero discriminating power)", termBefore, ft)
	}
}

// TestInmemTransportDisconnectIsRefusedNotGranted pins the exact raft v1.7.3
// dependency behavior the PreVote gate silently relies on: raft's askPeer only
// counts an "unexpected command" error as a GRANTED pre-vote (raft.go); a
// disconnected peer must therefore return a DIFFERENT error so the isolated
// follower's pre-vote is correctly REFUSED. If a future raft bump changed the
// disconnect error text to contain that sentinel, the gate would silently break
// (isolated pre-votes falsely granted → term never pinned).
func TestInmemTransportDisconnectIsRefusedNotGranted(t *testing.T) {
	_, tr := raft.NewInmemTransport("")
	var resp raft.RequestPreVoteResponse
	err := tr.RequestPreVote("peer", raft.ServerAddress("nonexistent-peer"), &raft.RequestPreVoteRequest{}, &resp)
	if err == nil {
		t.Fatal("RequestPreVote to a disconnected peer must return an error")
	}
	if strings.Contains(err.Error(), "unexpected command") {
		t.Fatalf("disconnect error %q must NOT contain the 'unexpected command' sentinel that raft "+
			"counts as a GRANTED pre-vote — the gate would silently break", err.Error())
	}
}
