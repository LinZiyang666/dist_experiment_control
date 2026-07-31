package main

import (
	"testing"
	"time"
)

// deadline_test.go — the round's global budget must follow the HOST, not the machine the runner was
// written on.
//
// origin: a 2-physical-core GitHub runner. `autoWorkerCount` correctly seated 1 worker, the 99 units
// then ran serially, and the hard-coded 25m budget ran out at unit 85. The round reported
// `FAIL D5:test/d5[1/8]: signal: killed` and "17 scheduled item(s) produced no result" — true,
// actionable-looking, and pointing at entirely the wrong thing. The only clue that the budget rather
// than the suite was at fault was the wall clock reading 25m0.002s.

func TestSlotsPerWorkerIsQueueDepthNotUnitCount(t *testing.T) {
	for _, tc := range []struct {
		name           string
		units, workers int
		want           int
	}{
		// The two shapes that actually occur, both measured.
		{"44-core dev box", 99, 20, 5},
		{"2-core CI runner", 99, 1, 99},
		// Rounding must go UP: 99/18 is 5.5 and a worker cannot run half a unit. Truncating here
		// would under-budget every host whose worker count does not divide the unit count — which
		// is most of them, and the error grows exactly as the host gets smaller.
		{"non-dividing worker count rounds up", 99, 18, 6},
		{"more workers than units", 3, 20, 1},
		// Defensive: workers is derived from a topology probe, and a zero would divide by zero at
		// the worst possible moment (startup, before anything has run).
		{"zero workers is treated as one", 10, 0, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := slotsPerWorker(tc.units, tc.workers); got != tc.want {
				t.Errorf("slotsPerWorker(%d, %d) = %d, want %d", tc.units, tc.workers, got, tc.want)
			}
		})
	}
}

// TestDefaultDeadlineScalesWithTheHostAndNeverShrinks pins BOTH halves of the contract. The second
// half is the one a future edit is likely to break: making the budget derived must not make it
// SMALLER than the 25m the 44-core box has always had, or this fix would trade a CI failure for a
// dev-box failure.
func TestDefaultDeadlineScalesWithTheHostAndNeverShrinks(t *testing.T) {
	const units = 99

	big := defaultDeadline(units, 20)
	if big != minDeadline {
		t.Errorf("44-core shape (99 units / 20 workers) = %s, want the historical %s.\n"+
			"Deriving the budget must not change what the development host has always had — a "+
			"regression there would be found by breaking the gate everyone actually runs.", big, minDeadline)
	}

	small := defaultDeadline(units, 1)
	if small <= big {
		t.Errorf("1-worker shape = %s, which is not more than the 20-worker shape (%s).\n"+
			"A host that runs every unit back-to-back needs MORE wall clock, not the same amount. "+
			"This is the exact defect the CI runner hit.", small, big)
	}
	// The 2-core round was MEASURED at 42m22s (ALL PASS). A budget that is not comfortably above
	// that just moves the failure later — and the CI runner may well be slower than the box the
	// measurement came from. The floor is the measurement plus a margin, not a round number.
	const measuredTwoCoreRound = 42*time.Minute + 22*time.Second
	if small < measuredTwoCoreRound+30*time.Minute {
		t.Errorf("1-worker budget is %s; the measured 2-core round is %s and needs real headroom "+
			"above it.\nperSlotBudget=%s deadlineFactor=%d — if the measurement changed, re-measure "+
			"and update BOTH the constant and this number, do not lower the assertion to fit.",
			small, measuredTwoCoreRound, perSlotBudget, deadlineFactor)
	}

	// Monotone in the host size: every step down in workers must be a step up (or equal) in budget.
	// A non-monotone budget would mean some middle-sized host gets less time than a bigger one.
	prev := time.Duration(0)
	for w := 20; w >= 1; w-- {
		d := defaultDeadline(units, w)
		if d < prev {
			t.Errorf("budget is not monotone: %d workers -> %s, but %d workers -> %s",
				w+1, prev, w, d)
		}
		prev = d
	}
}

// TestSuggestDeadlineIsAlwaysAboveWhatWasObserved: the number printed in the DEADLINE EXCEEDED block
// gets pasted straight back into a command line. If it can come out at or below the budget that just
// failed, the operator re-runs and fails again — a suggestion that reproduces the problem is worse
// than none, because it costs another full round to disbelieve.
func TestSuggestDeadlineIsAlwaysAboveWhatWasObserved(t *testing.T) {
	for _, elapsed := range []time.Duration{
		0,
		time.Second,
		25 * time.Minute, // the budget that actually blew on the CI runner
		25*time.Minute + 2*time.Millisecond,
		99 * time.Minute,
	} {
		got := suggestDeadline(elapsed)
		if got <= elapsed {
			t.Errorf("suggestDeadline(%s) = %s, which is not more than what was already spent",
				elapsed, got)
		}
		if got < 30*time.Minute {
			t.Errorf("suggestDeadline(%s) = %s, below the floor", elapsed, got)
		}
	}
}
