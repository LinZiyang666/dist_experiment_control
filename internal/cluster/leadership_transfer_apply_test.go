package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// leadership_transfer_apply_test.go — the REAL-CLUSTER half of the IsNotLeader
// contract.
//
// TestIsNotLeaderRecognizesEveryUncommittedWriteError (cluster_contracts_test.go)
// pins the PREDICATE, and that is the half a unit test can pin. It cannot pin the
// fact the predicate exists for: that hashicorp/raft really does reject Apply with
// ErrLeadershipTransferInProgress while the old leader still reports
// State()==Leader. That fact is an UPSTREAM behaviour, it is the entire reason the
// gap was reachable in production, and if a raft upgrade ever changed it this file
// is what would notice.
//
// origin: prerelease audit broker-cluster-write/L3-F1 — the verifier's closing note
// was precisely "the predicate unit test cannot prove raft returns this sentinel,
// and that is where all of F1's risk lives".

// TestApplyDuringLeadershipTransferIsRecognizedAsRetriable drives a real 3-node
// raft (real mTLS transport) into a leadership transfer and hammers Apply on the
// transferring leader until it observes raft's transfer rejection, then asserts
// IsNotLeader classifies it as retriable.
//
// Not vacuous, in both directions: it FAILS if the sentinel is never observed
// (raft changed, or the window closed) AND it FAILS if IsNotLeader does not
// recognize what was observed. There is no Skip: a Skip here would silently retire
// the only evidence that the production hazard is real.
func TestApplyDuringLeadershipTransferIsRecognizedAsRetriable(t *testing.T) {
	ca := newTestCA(t)
	ids := []raft.ServerID{"lt-a", "lt-b", "lt-c"}
	trans := make([]*raft.NetworkTransport, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		t.Cleanup(func() { _ = tr.Close() })
		trans[i] = tr
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: tr.LocalAddr()}
	}
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		n, err := New(fastMultinodeCfg(id, t.TempDir(), trans[i], servers))
		if err != nil {
			t.Fatalf("node %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Shutdown() })
		nodes[i] = n
	}

	// leaderWithin re-polls until some node reports leadership, and returns it (nil
	// on timeout).
	//
	// The read lives INSIDE the waitForCond predicate on purpose, not in a plain
	// helper closure that returns the answer. That is what
	// test/determinism/leader_premise_test.go requires and the requirement is right
	// even here: a closure that reads once and hands back a *Node produces a value
	// whose truth expired the moment it was returned. Keeping the read in a
	// predicate the poller re-evaluates means the reading this returns is as fresh
	// as the last evaluation.
	//
	// The value is STILL stale by the time Apply runs — that is unavoidable, and in
	// this test it is the POINT (see the hammer loop below, which is built to
	// tolerate exactly that). What the shape buys is that the staleness window is
	// one call wide rather than one poll-cycle wide.
	leaderWithin := func(d time.Duration) *Node {
		var found *Node
		waitForCond(d, func() bool {
			found = nil
			for _, n := range nodes {
				if n.IsLeader() {
					found = n
					return true
				}
			}
			return false
		})
		return found
	}
	if leaderWithin(15*time.Second) == nil {
		t.Fatal("no leader elected within 15s")
	}

	// Hammer Apply on whichever node currently believes it is the leader while
	// leadership is repeatedly handed around. The transfer window is bounded by
	// ElectionTimeout, so we re-arm it until the sentinel is observed.
	deadline := time.Now().Add(30 * time.Second)
	var (
		mu       sync.Mutex
		observed error
	)
	for time.Now().Before(deadline) && observed == nil {
		ldr := leaderWithin(5 * time.Second)
		if ldr == nil {
			continue
		}
		// Pick any other voter as the transfer target.
		var target raft.Server
		for _, s := range servers {
			if s.ID != raft.ServerID(ldr.SelfID()) {
				target = s
				break
			}
		}
		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Errors here are expected and uninteresting (the transfer can fail if a
			// follower is momentarily behind); the point is that it FLIPS THE FLAG.
			_ = ldr.LeadershipTransferToServer(string(target.ID), string(target.Address))
			close(stop)
		}()
		for {
			err := ldr.ApplyMetaSet("t:lt-probe", "1")
			if err != nil && errors.Is(err, raft.ErrLeadershipTransferInProgress) {
				mu.Lock()
				observed = err
				mu.Unlock()
				break
			}
			select {
			case <-stop:
			default:
				continue
			}
			break
		}
		wg.Wait()
		if observed == nil {
			// Let the new leader settle before re-arming.
			time.Sleep(50 * time.Millisecond)
		}
	}

	if observed == nil {
		t.Fatal("never observed raft.ErrLeadershipTransferInProgress from Apply during a real " +
			"leadership transfer in 30s. Either raft's behaviour changed (then IsNotLeader's " +
			"comment and the production hazard it describes need rewriting), or this test lost " +
			"its window — do NOT weaken it into a Skip, that would retire the only evidence " +
			"that the production hazard is real")
	}
	if !IsNotLeader(observed) {
		t.Fatalf("raft returned %v during a leadership transfer and IsNotLeader() says it is NOT "+
			"retriable — this is the exact production path where handleRegister answers "+
			"store_error and the agent's register loop exits the process", observed)
	}
	t.Logf("observed during a real transfer: %v (IsNotLeader=true)", observed)
}
