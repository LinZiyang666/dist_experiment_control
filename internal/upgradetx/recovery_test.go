package upgradetx

import "testing"

// origin: S8 internal review rv3-leaves F9
func TestRecoveryTransitionRequiresBoundTargetEvidence(t *testing.T) {
	for _, defect := range []string{"none", "missing", "owner", "custody", "identity", "revision", "nonce", "producer", "running", "selector", "control", "phase", "pristine-observation"} {
		t.Run(defect, func(t *testing.T) {
			a, o := completionFixture(DirectionForward)
			a.Phase, a.CutStage, a.Outcome = PhaseRecoveryRequired, CutRetiring, OutcomeUnknown
			r := recordFixture(a)
			o.Current = cloneAttempt(a)
			o.Current.Control.Stamp, o.Current.Control.Census = o.Census.Stamp, o.Census
			event := Event{Recovery: &o, Phase: PhaseMigrating, CutStage: CutRetiring, Outcome: OutcomePending}
			switch defect {
			case "missing":
				event.Recovery = nil
			case "owner":
				o.OwnerKnown = false
			case "custody":
				o.Census.Resources[0].Generation = a.SourceGeneration
			case "identity":
				o.Census.Resources[0].Origin = "replacement"
			case "revision":
				o.Census.Stamp.Attempt.Revision--
			case "nonce":
				o.Nonce = a.Control.Stamp.Nonce
			case "producer":
				o.Census.Stamp.Producer = sourceOwner()
			case "running":
				o.RunningSHA = a.SourceSHA
			case "selector":
				o.EffectiveSHA = a.SourceSHA
			case "control":
				o.Current.Control.Known = false
			case "phase":
				event.Phase, event.CutStage, event.Outcome = PhaseCompleted, CutReleased, OutcomeSuccess
			case "pristine-observation":
				o.Current.CutStage = CutPristine
			}
			next, err := Transition(r, event)
			if defect == "none" {
				if err != nil || next.Forward.Phase != PhaseMigrating || next.Revision != r.Revision+1 {
					t.Fatalf("known accepted cut could not resume: %v", err)
				}
			} else {
				assertUnsettled(t, err)
			}
		})
	}
}
