package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// stubClock swaps nowFunc for a manually-advanced clock and restores it. The stubbed
// fetchClusterStatusReport advances the clock on each call, so the settle loop is driven
// deterministically by fetch calls rather than by wall time.
func stubClock(t *testing.T) (advance func(time.Duration)) {
	t.Helper()
	var mu sync.Mutex
	cur := time.Unix(1_700_000_000, 0)
	orig := nowFunc
	nowFunc = func() time.Time { mu.Lock(); defer mu.Unlock(); return cur }
	t.Cleanup(func() { nowFunc = orig })
	return func(d time.Duration) { mu.Lock(); cur = cur.Add(d); mu.Unlock() }
}

func rep(exit int) *adminsock.ClusterStatusReport {
	return &adminsock.ClusterStatusReport{ExitCode: exit, SchemaVersion: 1}
}

// TestSettleReturnsHealthyImmediately: an already-HEALTHY cluster settles on the first poll.
func TestSettleReturnsHealthyImmediately(t *testing.T) {
	advance := stubClock(t)
	calls := 0
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		calls++
		advance(time.Second)
		return rep(0), nil
	})
	got, err := settleClusterStatus(waitCmd(context.Background(), t), "/sock", 30*time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExitCode != 0 {
		t.Errorf("exit=%d want 0", got.ExitCode)
	}
	if calls != 1 {
		t.Errorf("polled %d times, want 1 (healthy settles at once)", calls)
	}
}

// TestSettleDebouncesTransientDegraded: a DEGRADED verdict that CLEARS to HEALTHY within the window
// returns HEALTHY (exit 0) — the benign voter-restart blip is debounced.
func TestSettleDebouncesTransientDegraded(t *testing.T) {
	advance := stubClock(t)
	seq := []int{1, 1, 0} // degraded, degraded, healthy
	i := 0
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		e := seq[i]
		if i < len(seq)-1 {
			i++
		}
		advance(2 * time.Second)
		return rep(e), nil
	})
	got, err := settleClusterStatus(waitCmd(context.Background(), t), "/sock", 30*time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExitCode != 0 {
		t.Errorf("exit=%d want 0 (transient DEGRADED should have cleared to HEALTHY within the window)", got.ExitCode)
	}
}

// TestSettleSurfacesSustainedDegraded: a DEGRADED that NEVER clears returns exit 1 after the window
// — a sustained degradation (e.g. permanently NOT-HA N=2) is surfaced honestly, NOT masked.
func TestSettleSurfacesSustainedDegraded(t *testing.T) {
	advance := stubClock(t)
	polls := 0
	stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
		polls++
		advance(3 * time.Second)
		return rep(1), nil
	})
	got, err := settleClusterStatus(waitCmd(context.Background(), t), "/sock", 10*time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExitCode != 1 {
		t.Errorf("exit=%d want 1 (sustained DEGRADED must be surfaced, not debounced away)", got.ExitCode)
	}
	if polls < 2 {
		t.Errorf("polled %d times; expected repeated polling until the window elapsed", polls)
	}
}

// TestSettleReturnsQuorumLostImmediately / TestSettleReturnsForceSingleImmediately: exit 2/3 are
// NOT benign restart blips — they surface on the first poll, never debounced into looking transient.
func TestSettleReturnsHardStatesImmediately(t *testing.T) {
	for _, hard := range []int{2, 3} {
		advance := stubClock(t)
		calls := 0
		stubFetch(t, func(string) (*adminsock.ClusterStatusReport, error) {
			calls++
			advance(time.Second)
			return rep(hard), nil
		})
		got, err := settleClusterStatus(waitCmd(context.Background(), t), "/sock", time.Hour, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if got.ExitCode != hard {
			t.Errorf("exit=%d want %d (hard state surfaces immediately)", got.ExitCode, hard)
		}
		if calls != 1 {
			t.Errorf("hard state polled %d times, want 1 (never debounced)", calls)
		}
	}
}
