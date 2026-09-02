package clusterharness

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// leader.go — WithLeader: observe → act → re-observe, with bounded retries, as ONE helper.
//
// docs/testing-standards.md T3: a test must not assume the distributed state it just observed still
// holds one statement later. `if n.IsLeader() { leaderIdx = i }; nodes[1-leaderIdx].Propose(...)`
// was the shape behind the d3 load-only flake (parallel-flake-rootcause.md root cause 2), and the d7
// fix — bind the orchestrator to whichever node holds leadership NOW, and if it moved, bind again —
// was written as a suite-local helper (adminForLeader). This is that fix as a shared primitive, so
// the next suite does not re-derive it, and so test/determinism/leader_premise_test.go has a shape
// to point at: a bare `.IsLeader()` outside a polling closure is red unless it sits in the ledger.
//
// The contract: fn runs against the node that was leader at the moment it was called. If, when fn
// returns, that node is NO LONGER leader, fn's result is discarded and the whole observe→act cycle
// is retried — leadership moved underneath it, so whatever it asserted was about a node that is not
// what the test thinks it is. Retries are bounded by `budget`; running out is a T4-style signal
// (the cluster is thrashing under load), reported as such, never absorbed.
//
// WHAT IT DOES NOT PROVE (external review, suggestion 1). Two boolean readings bracket fn; no term
// or epoch is compared, so an A→B→A move entirely inside fn passes both readings. And "discarded"
// means fn's RETURN VALUE — a side effect fn already performed on the old leader is not undone.
// This is a re-observe primitive, not a transaction boundary: callers make fn idempotent under
// retry (an upsert, a proposal that is itself the thing measured), and a test that needs "no
// election happened during fn" must carry the term through its probe.

// LeaderProbe is the one method WithLeader needs from a node. *cluster.Node and *broker.ClusterAdmin
// both have it.
type LeaderProbe interface {
	IsLeader() bool
}

// ErrNoLeader is returned when no probe reported leadership inside the budget.
var ErrNoLeader = errors.New("clusterharness: no leader within budget")

// ErrLeadershipUnstable is returned when leadership moved during fn on every attempt inside the budget.
var ErrLeadershipUnstable = errors.New("clusterharness: leadership moved during fn on every attempt")

// WithLeader finds the current leader among probes, runs fn against its index, and returns fn's result
// only if that node still holds leadership afterwards. Otherwise it retries until budget is spent.
// The returned retries count is for callers that want to log churn.
func WithLeader(t testing.TB, probes []LeaderProbe, budget time.Duration, fn func(leader int) error) (retries int, err error) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		idx := -1
		for time.Now().Before(deadline) && idx < 0 {
			for i, p := range probes {
				if p.IsLeader() {
					idx = i
					break
				}
			}
			if idx < 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		if idx < 0 {
			// The budget ran out while RE-observing after at least one move: that is instability,
			// not absence. The first version returned ErrNoLeader here, and its own thrashing
			// test caught it — after 535,288 spins in 100ms, because it also had no backoff.
			if retries > 0 {
				return retries, fmt.Errorf("%w (%d retr(y/ies); budget exhausted while re-observing)", ErrLeadershipUnstable, retries)
			}
			return retries, fmt.Errorf("%w", ErrNoLeader)
		}
		ferr := fn(idx)
		if probes[idx].IsLeader() {
			return retries, ferr
		}
		// Leadership moved during fn: its result is about a node that is no longer the leader.
		retries++
		if !time.Now().Before(deadline) {
			return retries, fmt.Errorf("%w (%d retr(y/ies); last fn error: %v)", ErrLeadershipUnstable, retries, ferr)
		}
		// Do not spin: a cluster that is re-electing needs CPU more than this loop does, and the
		// retry count should mean "elections observed", not "iterations survived".
		time.Sleep(5 * time.Millisecond)
	}
}
