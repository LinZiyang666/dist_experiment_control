package upgradetx

import (
	"reflect"
	"testing"
)

func cancellationFixture() (Record, CancellationProof) {
	a, o := completionFixture(DirectionForward)
	a.Phase, a.CutStage, a.Outcome = PhaseFreezing, CutPristine, OutcomePending
	a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
	o.Current, o.RunningSHA, o.EffectiveSHA = a, a.SourceSHA, a.SourceSHA
	o.Census.Stamp.Producer = sourceOwner()
	o.Census.Resources = append([]Resource{}, a.Scope.Expected...)
	r := recordFixture(a)
	r.EffectiveSHA = a.SourceSHA
	return r, CancellationProof{Thawed: true, Observation: o}
}

// origin: S8 internal review rv-fault F4
func TestCancellationEvidenceCannotEraseUnresolvedEffects(t *testing.T) {
	for _, kind := range []ActionKind{ActionPublish, ActionSelect, ActionFreeze, ActionDispatch, ActionActivate} {
		for _, proofPresent := range []bool{false, true} {
			t.Run(string(kind)+map[bool]string{false: "/unknown", true: "/proven"}[proofPresent], func(t *testing.T) {
				r, p := cancellationFixture()
				r.Actions = []Action{{ID: testID(900), Handle: r.Forward.Handle, Kind: kind, IntentDigest: testSHA("d"), Nonce: testID(901), State: ActionIntent}}
				if kind == ActionPublish {
					r.Actions[0].State, r.Actions[0].ResultDigest = ActionDurable, testSHA("e")
				}
				event := Event{Phase: PhaseCancelled, CutStage: CutNone, Outcome: OutcomeCancelled}
				if proofPresent {
					event.Cancellation = &p
				}
				got, err := Transition(r, event)
				want := kind == ActionPublish || proofPresent && (kind == ActionSelect || kind == ActionFreeze)
				if want {
					if err != nil || !CancellationSettled(got) {
						t.Fatalf("safe cancellation unavailable: %v", err)
					}
					before := snapshotRecord(t, got)
					p.Observation.Census.Resources[0].ID = "aliased"
					if !reflect.DeepEqual(got, before) {
						t.Fatal("cancellation borrows caller proof")
					}
				} else if err == nil && CancellationSettled(got) {
					t.Fatal("unresolved effect released Current")
				}
			})
		}
	}
}

// origin: S8 internal review rv-fault F4
func TestCancellationProofUsesSameValidatorAfterDecode(t *testing.T) {
	for _, defect := range []string{"thaw", "nonce", "cut", "revision", "producer", "running", "selector", "owner", "custody", "generation", "admitting", "old-owner", "missing", "commitment"} {
		t.Run(defect, func(t *testing.T) {
			r, p := cancellationFixture()
			got, err := Transition(r, Event{Phase: PhaseCancelled, Outcome: OutcomeCancelled, Cancellation: &p})
			if err != nil {
				t.Fatal(err)
			}
			o := &got.Cancellation.Observation
			switch defect {
			case "thaw":
				got.Cancellation.Thawed = false
			case "nonce":
				o.Nonce = testID(999)
			case "cut":
				o.Census.Stamp.Attempt.CutID = testID(998)
			case "revision":
				o.Census.Stamp.Attempt.Revision = got.Revision + 1
			case "producer":
				o.Census.Stamp.Producer = targetOwner()
			case "running":
				o.RunningSHA = r.Grant.TargetSHA
			case "selector":
				o.EffectiveSHA = r.Grant.TargetSHA
			case "owner":
				o.OwnerKnown = false
			case "custody":
				o.Census.Resources[0].Owner = targetOwner()
			case "generation":
				o.Census.Resources[0].Generation++
			case "admitting":
				o.Census.Admitting++
			case "old-owner":
				o.Census.OldOwners = []Owner{targetOwner()}
			case "missing":
				o.Census.Resources = nil
			case "commitment":
				o.Census.ExpectedDigest = testSHA("f")
			}
			if ValidateRecord(got) == nil || CancellationSettled(got) {
				t.Fatal("decoded cancellation accepted corrupted source evidence")
			}
		})
	}
}

// origin: S8 internal review rv-fault F4
func TestRecoveryCancellationRequiresSourceProof(t *testing.T) {
	r, p := cancellationFixture()
	r.Forward.Phase, r.Forward.Outcome = PhaseRecoveryRequired, OutcomeUnknown
	p.Observation.Current = r.Forward
	event := Event{Phase: PhaseCancelled, Outcome: OutcomeCancelled}
	if _, err := Transition(r, event); err == nil {
		t.Fatal("phase-only cancellation erased recovery uncertainty")
	}
	event.Cancellation = &p
	got, err := Transition(r, event)
	if err != nil || !CancellationSettled(got) {
		t.Fatalf("source proof cannot resolve pristine recovery: %v", err)
	}
}

// origin: S8 internal review roll-recovery-stall F5
func TestUnqueuedActivationCancellationRequiresPristineSourceCustody(t *testing.T) {
	for _, defect := range []string{"none", "missing proof", "active observation", "target running", "unknown owner", "target custody", "missing identity", "missing thaw", "observed activation", "missing freeze", "control action"} {
		t.Run(defect, func(t *testing.T) {
			r, proof := cancellationFixture()
			r.Forward.Phase, r.Forward.CutStage, r.Forward.Outcome = PhaseRecoveryRequired, CutPartialActive, OutcomeUnknown
			proof.Observation.Current = r.Forward
			proof.Observation.Current.CutStage = CutPristine
			r.Actions = []Action{
				{ID: testID(910), Handle: r.Forward.Handle, Kind: ActionFreeze, IntentDigest: testSHA("d"), ResultDigest: testSHA("e"), Nonce: testID(911), State: ActionDurable},
				{ID: testID(912), Handle: r.Forward.Handle, Kind: ActionActivate, IntentDigest: testSHA("d"), Nonce: testID(913), State: ActionIntent},
			}
			event := Event{Phase: PhaseCancelled, CutStage: CutPristine, Outcome: OutcomeCancelled, Cancellation: &proof}
			switch defect {
			case "missing proof":
				event.Cancellation = nil
			case "active observation":
				proof.Observation.Current.CutStage = CutActive
			case "target running":
				proof.Observation.RunningSHA = r.Forward.TargetSHA
			case "unknown owner":
				proof.Observation.OwnerKnown = false
			case "target custody":
				proof.Observation.Census.Resources[0].Generation = r.Forward.Handle.Generation
			case "missing identity":
				proof.Observation.Census.Resources = nil
			case "missing thaw":
				proof.Thawed = false
			case "observed activation":
				r.Actions[1].State, r.Actions[1].ResultDigest = ActionObserved, testSHA("f")
			case "missing freeze":
				r.Actions = r.Actions[1:]
			case "control action":
				action := r.Actions[1]
				action.Kind, action.ID = ActionControl, testID(914)
				r.Actions = append(r.Actions, action)
			}
			got, err := Transition(r, event)
			if defect == "none" {
				if err != nil || !CancellationSettled(got) || ValidateRecord(got) != nil || !ColdSelectionSafe(got) {
					t.Fatalf("proven unqueued activation did not settle R0: %v", err)
				}
			} else if err == nil {
				t.Fatal("unproven activation released Current")
			}
		})
	}
}
