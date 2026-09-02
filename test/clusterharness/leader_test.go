package clusterharness

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeProbe flips leadership from one index to another after `flipAfter` IsLeader calls, modelling
// an election that lands in the middle of fn.
type fakeProbe struct {
	leader *atomic.Int32
	me     int32
}

func (p fakeProbe) IsLeader() bool { return p.leader.Load() == p.me }

func fakeCluster(leader int32) ([]LeaderProbe, *atomic.Int32) {
	var l atomic.Int32
	l.Store(leader)
	return []LeaderProbe{fakeProbe{&l, 0}, fakeProbe{&l, 1}, fakeProbe{&l, 2}}, &l
}

func TestWithLeaderStableLeadershipRunsOnceAndReturnsFnResult(t *testing.T) {
	probes, _ := fakeCluster(1)
	calls := 0
	retries, err := WithLeader(t, probes, time.Second, func(leader int) error {
		calls++
		if leader != 1 {
			t.Fatalf("fn ran against %d, leader is 1", leader)
		}
		return errors.New("fn's own error")
	})
	if calls != 1 || retries != 0 || err == nil || err.Error() != "fn's own error" {
		t.Fatalf("calls=%d retries=%d err=%v", calls, retries, err)
	}
}

// The property the helper exists for: leadership moves DURING fn, so fn's first result — however
// confident — is about the wrong node and must be discarded; the second run lands on the new leader.
func TestWithLeaderRetriesWhenLeadershipMovesDuringFn(t *testing.T) {
	probes, leader := fakeCluster(0)
	var seen []int
	retries, err := WithLeader(t, probes, 2*time.Second, func(idx int) error {
		seen = append(seen, idx)
		if len(seen) == 1 {
			leader.Store(2) // election completes while fn is running
		}
		return nil
	})
	if err != nil || retries != 1 || len(seen) != 2 || seen[0] != 0 || seen[1] != 2 {
		t.Fatalf("retries=%d seen=%v err=%v", retries, seen, err)
	}
}

func TestWithLeaderReportsNoLeaderInsideBudget(t *testing.T) {
	probes, _ := fakeCluster(-1)
	_, err := WithLeader(t, probes, 50*time.Millisecond, func(int) error { t.Fatal("fn must not run"); return nil })
	if !errors.Is(err, ErrNoLeader) {
		t.Fatalf("err=%v want ErrNoLeader", err)
	}
}

func TestWithLeaderReportsThrashingAsUnstableNotAsSuccess(t *testing.T) {
	probes, leader := fakeCluster(0)
	_, err := WithLeader(t, probes, 100*time.Millisecond, func(idx int) error {
		leader.Store(int32((idx + 1) % 3)) // moves every single time
		return nil
	})
	if !errors.Is(err, ErrLeadershipUnstable) {
		t.Fatalf("err=%v want ErrLeadershipUnstable — a thrashing cluster must never be reported as fn success", err)
	}
}
