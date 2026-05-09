package testharness

import (
	"testing"
	"time"
)

func TestWaitForReturnsTrueWhenPredicateBecomesTrue(t *testing.T) {
	flipAt := time.Now().Add(50 * time.Millisecond)
	got := WaitFor(t, time.Second, 10*time.Millisecond, func() bool {
		return time.Now().After(flipAt)
	})
	if !got {
		t.Errorf("WaitFor returned false; predicate flipped before deadline")
	}
}

func TestWaitForReturnsFalseOnTimeout(t *testing.T) {
	got := WaitFor(t, 50*time.Millisecond, 10*time.Millisecond, func() bool {
		return false
	})
	if got {
		t.Errorf("WaitFor returned true; predicate is always false")
	}
}

func TestWaitForReturnsTrueImmediatelyOnFirstCall(t *testing.T) {
	calls := 0
	got := WaitFor(t, time.Second, 100*time.Millisecond, func() bool {
		calls++
		return true
	})
	if !got || calls != 1 {
		t.Errorf("WaitFor: got=%v calls=%d, want true / 1 call", got, calls)
	}
}

func TestWaitForReChecksAfterDeadline(t *testing.T) {
	// Pin the "predicate may flip after deadline expired but before
	// the loop's last iteration finishes" edge: WaitFor returns the
	// final post-loop predicate evaluation, so a predicate that
	// becomes true exactly at deadline still returns true.
	deadline := time.Now().Add(30 * time.Millisecond)
	got := WaitFor(t, 30*time.Millisecond, 50*time.Millisecond, func() bool {
		return time.Now().After(deadline)
	})
	if !got {
		t.Errorf("WaitFor: predicate true at deadline boundary should still return true")
	}
}
