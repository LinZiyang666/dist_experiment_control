package acceptance

import (
	"fmt"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"testing"
)

// origin: S10 internal review rv-harness F4
func TestCompletionRejectsEarlierUnknownOutcome(t *testing.T) {
	e := rolloutEvidence(t)
	prior := cloneEvidence(t, e).Views[0]
	prior.Stamp.QueryNonce = "earlier-unknown-query"
	prior.Stamp.Sequence = 1
	prior.Stamp.AtBootNS--
	prior.Phase, prior.Outcome = "recovery_required", "unknown"
	editCurrent(t, &prior, func(o *transactionObservation) {
		o.Nonce, o.At = prior.Stamp.QueryNonce, prior.Stamp.AtBootNS
		o.Census.Stamp.Nonce = prior.Stamp.QueryNonce
		o.Census.Stamp.Sequence = prior.Stamp.Sequence
		o.Census.Stamp.At = prior.Stamp.AtBootNS
		o.Current.Phase, o.Current.Outcome = prior.Phase, prior.Outcome
	})
	prior.Raw = fixtureRaw("unknown-before-success", "s8_view")
	e.Raw = append(e.Raw, prior.Raw)
	e.Views = append([]c.View{prior}, e.Views...)
	result, err := Judge(e)
	if err != nil {
		t.Fatal(err)
	}
	if result.RC == 0 {
		t.Fatalf("earlier recovery_required/unknown erased by completion: verdict=%s rc=%d", result.Verdict, result.RC)
	}
}

// A normal rollout may progress through pending observations, including on an
// explicit reverse step. Unknown or abnormal terminal outcomes must remain in
// the failure union even when every later completion and retirement is valid.
// origin: S10 internal review rv-harness F4
func TestCompletionPreservesObservedOutcomeFailures(t *testing.T) {
	baseline := rolloutEvidence(t)
	result, err := Judge(baseline)
	if err != nil || result.RC != 0 {
		t.Fatalf("baseline rollout: rc=%d failures=%v missing=%v err=%v", result.RC, result.Failures, result.Missing, err)
	}
	for _, state := range []struct{ phase, stage, outcome string }{
		{"recovery_required", "active", "unknown"},
		{"abandoned", "active", "unknown"},
		{"cancelled", "pristine", "cancelled"},
		{"rolled_back", "released", "rolled_back"},
	} {
		t.Run(state.phase, func(t *testing.T) {
			e := cloneEvidence(t, baseline)
			prior := outcomeObservation(t, e.Views[0], state.phase, state.stage, state.outcome, 1)
			e.Raw = append(e.Raw, prior.Raw)
			e.Views = append([]c.View{prior}, e.Views...)
			result, err := Judge(e)
			// All subsequent steps still provide valid coverage. The red must come
			// from the earlier outcome, not a broken stamp or a missing later receipt.
			if err != nil || result.RC != 1 || result.Verdict != "ASSERT-FAIL" || len(result.Failures) != 1 || len(result.Missing) != 0 {
				t.Fatalf("earlier %s must remain the sole failure after completion: rc=%d failures=%v missing=%v err=%v", state.outcome, result.RC, result.Failures, result.Missing, err)
			}
		})
	}
	for _, index := range []int{0, 7} {
		direction := baseline.Views[index].Stamp.Direction
		t.Run("pending "+direction, func(t *testing.T) {
			e := cloneEvidence(t, baseline)
			var prior []c.View
			for i, state := range []struct{ phase, stage string }{
				{"prepared", "none"}, {"staged", "none"}, {"ready", "none"},
				{"freezing", "pristine"}, {"activating", "pristine"},
				{"activating", "partial_active"}, {"serving_new", "partial_active"},
				{"serving_new", "active"}, {"migrating", "active"}, {"migrating", "retiring"},
			} {
				view := outcomeObservation(t, e.Views[index], state.phase, state.stage, "pending", uint64(i+1))
				e.Raw = append(e.Raw, view.Raw)
				prior = append(prior, view)
			}
			e.Views = append(e.Views[:index], append(prior, e.Views[index:]...)...)
			result, err := Judge(e)
			if err != nil || result.RC != 0 || result.Verdict != "GREEN" || len(result.Failures)+len(result.Missing) != 0 {
				t.Fatalf("valid %s pending sequence rejected: rc=%d failures=%v missing=%v err=%v", direction, result.RC, result.Failures, result.Missing, err)
			}
		})
	}
}

func outcomeObservation(t *testing.T, completed c.View, phase, stage, outcome string, sequence uint64) c.View {
	t.Helper()
	prior := completed
	prior.Stamp.QueryNonce = fmt.Sprintf("prior-%s-%s-%d", completed.Stamp.CutID, phase, sequence)
	prior.Stamp.Sequence = sequence
	prior.Stamp.AtBootNS = completed.Stamp.AtBootNS - 20 + int64(sequence)
	prior.Phase, prior.Outcome = phase, outcome
	editCurrent(t, &prior, func(o *transactionObservation) {
		o.Nonce, o.At = prior.Stamp.QueryNonce, prior.Stamp.AtBootNS
		o.Census.Stamp.Nonce, o.Census.Stamp.Sequence, o.Census.Stamp.At = o.Nonce, sequence, o.At
		o.Current.Phase, o.Current.CutStage, o.Current.Outcome = phase, stage, outcome
	})
	prior.Raw = fixtureRaw(prior.Stamp.QueryNonce, "s8_view")
	return prior
}
