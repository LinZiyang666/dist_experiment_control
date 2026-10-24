package binaryupgrade

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: upgrade-safety plan §7 — table-driven coverage of the pure boot
// decision (plan §3.1), including the adversarial corners the review lenses
// asked for: deadline exactly reached, zero deadline, degenerate prev==new,
// budget at/over the line, and a hand-swapped binary.

func testMarker(state string, bootCount int, deadline time.Time) *Marker {
	return &Marker{
		State: state, PrevSHA: "prevsha", NewSHA: "newsha",
		PrevVersion: "v0.1.0", NewVersion: "v0.2.0",
		Deadline: deadline, BootCount: bootCount, BootBudget: upgradebudget.BootBudget,
		UpgradeID: "test-upgrade-id",
	}
}

func TestDecideBootTable(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)

	cases := []struct {
		name string
		m    *Marker
		self string
		want BootAction
	}{
		{"no_marker", nil, "anything", BootProceed},
		{"committed_marker", testMarker(StateCommitted, 1, future), "newsha", BootProceed},
		{"rolled_back_marker", testMarker(StateRolledBack, 1, future), "prevsha", BootProceed},
		{"rollback_failed_marker", testMarker(StateRollbackFailed, 1, future), "x", BootProceed},

		{"pending_new_inside_budget_and_deadline", testMarker(StatePending, 0, future), "newsha", BootContinuePending},
		{"pending_new_last_budget_slot", testMarker(StatePending, upgradebudget.BootBudget-1, future), "newsha", BootContinuePending},
		{"pending_new_budget_exhausted", testMarker(StatePending, upgradebudget.BootBudget, future), "newsha", BootRollback},
		{"pending_new_budget_overshot", testMarker(StatePending, upgradebudget.BootBudget+5, future), "newsha", BootRollback},
		{"pending_new_deadline_passed", testMarker(StatePending, 0, past), "newsha", BootRollback},
		// !now.Before(deadline): AT the deadline is already too late.
		{"pending_new_deadline_exactly_now", testMarker(StatePending, 0, now), "newsha", BootRollback},
		// A zero deadline (corrupt or hand-written marker) must fail toward
		// rollback, not toward an unbounded pending.
		{"pending_new_zero_deadline", testMarker(StatePending, 0, time.Time{}), "newsha", BootRollback},

		{"pending_self_is_prev", testMarker(StatePending, 0, future), "prevsha", BootConvergeRolledBack},
		// Converge wins even with the budget burned: we ARE the restored
		// binary; rolling back again would be a self-loop.
		{"pending_self_is_prev_budget_burned", testMarker(StatePending, 99, past), "prevsha", BootConvergeRolledBack},

		{"pending_self_matches_neither", testMarker(StatePending, 0, future), "handswapped", BootMarkOrphan},
	}
	// Degenerate re-upgrade to the identical binary: prev==new==self. The
	// new-sha branch must win (continue pending) — treating it as an
	// interrupted rollback would converge a perfectly healthy staging.
	degenerate := testMarker(StatePending, 0, future)
	degenerate.PrevSHA = "newsha"
	cases = append(cases, struct {
		name string
		m    *Marker
		self string
		want BootAction
	}{"pending_prev_equals_new_equals_self", degenerate, "newsha", BootContinuePending})

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DecideBoot(c.m, c.self, now); got != c.want {
				t.Errorf("decideBoot = %q, want %q", got, c.want)
			}
		})
	}
}
