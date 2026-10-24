package upgradetx

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// These unit tests exercise real pure validators with explicit evidence values.
// No runtime, observer, process, FD or store is substituted for a real integration
// proof. Collection, durability and the caller's query challenge are tested by
// their owners; leak anchors are N/A because this package starts no resources.

// origin: hot-update S8 plan §4.2 / §9.1 / P01
func TestCompletedRequiresRunningImageProof(t *testing.T) {
	for _, direction := range []Direction{DirectionForward, DirectionReverse} {
		t.Run(string(direction), func(t *testing.T) {
			a, observation := completionFixture(direction)
			if err := Completed(a, observation); err != nil {
				t.Fatal(err)
			}
		})
	}
	mutations := []struct {
		name string
		edit func(*Attempt, *Observation)
	}{
		{"disk_changed_running_old", func(a *Attempt, o *Observation) { o.RunningSHA = a.SourceSHA }},
		{"running_unknown", func(_ *Attempt, o *Observation) { o.RunningSHA = "" }},
		{"effective_old", func(a *Attempt, o *Observation) { o.EffectiveSHA = a.SourceSHA }},
		{"owner_unknown", func(_ *Attempt, o *Observation) { o.OwnerKnown = false }},
		{"current_transaction", func(_ *Attempt, o *Observation) { o.Current.Handle.TxID = testID(999) }},
		{"current_cut", func(_ *Attempt, o *Observation) { o.Current.Handle.CutID = testID(999) }},
		{"current_generation", func(_ *Attempt, o *Observation) { o.Current.Handle.Generation++ }},
		{"current_revision", func(_ *Attempt, o *Observation) { o.Current.Handle.Revision++ }},
		{"current_key", func(_ *Attempt, o *Observation) { o.Current.Handle.Key.UID++ }},
		{"current_direction", func(_ *Attempt, o *Observation) { o.Current.Direction = DirectionReverse }},
		{"current_source", func(_ *Attempt, o *Observation) { o.Current.SourceSHA = testSHA("c") }},
		{"current_target", func(_ *Attempt, o *Observation) { o.Current.TargetSHA = testSHA("c") }},
		{"current_source_generation", func(_ *Attempt, o *Observation) { o.Current.SourceGeneration++ }},
		{"current_phase", func(_ *Attempt, o *Observation) { o.Current.Phase = PhaseMigrating }},
		{"current_cut_stage", func(_ *Attempt, o *Observation) { o.Current.CutStage = CutRetiring }},
		{"current_outcome", func(_ *Attempt, o *Observation) { o.Current.Outcome = OutcomeUnknown }},
		{"current_scope", func(_ *Attempt, o *Observation) { o.Current.Scope.ID = testID(999) }},
		{"partial_active", func(a *Attempt, o *Observation) {
			a.Phase, a.CutStage, a.Outcome = PhaseActivating, CutPartialActive, OutcomePending
			o.Current = cloneAttempt(*a)
		}},
		{"partial_with_success", func(a *Attempt, o *Observation) { a.CutStage = CutPartialActive; o.Current = cloneAttempt(*a) }},
		{"unknown_direction", func(a *Attempt, o *Observation) { a.Direction = "future"; o.Current = cloneAttempt(*a) }},
		{"missing_domain", func(a *Attempt, o *Observation) {
			a.Scope.Domains = slices.Delete(a.Scope.Domains, 5, 6)
			o.Current = cloneAttempt(*a)
		}},
		{"unknown_domain", func(a *Attempt, o *Observation) { a.Scope.Domains[5] = "future"; o.Current = cloneAttempt(*a) }},
		{"duplicate_participant", func(a *Attempt, o *Observation) {
			a.Scope.Participants = append(a.Scope.Participants, a.Handle.Key)
			o.Current = cloneAttempt(*a)
		}},
		{"missing_participant", func(a *Attempt, o *Observation) { a.Scope.Participants = nil; o.Current = cloneAttempt(*a) }},
		{"scope_missing_self", func(a *Attempt, o *Observation) { a.Scope.Participants[0].UID++; o.Current = cloneAttempt(*a) }},
		{"scope_duplicate_resource", func(a *Attempt, o *Observation) {
			a.Scope.Expected = append(a.Scope.Expected, a.Scope.Expected[0])
			o.Current = cloneAttempt(*a)
		}},
		{"census_known_false_zero_count", func(_ *Attempt, o *Observation) { o.Census.Known = false; o.Census.Resources = nil }},
		{"census_incomplete", func(_ *Attempt, o *Observation) { o.Census.Complete = false }},
		{"census_missing_original", func(_ *Attempt, o *Observation) { o.Census.Resources = nil }},
		{"census_changed_origin", func(_ *Attempt, o *Observation) { o.Census.Resources[0].Origin = "replacement" }},
		{"census_duplicate", func(_ *Attempt, o *Observation) {
			o.Census.Resources = append(o.Census.Resources, o.Census.Resources[0])
		}},
		{"census_admission_pending", func(_ *Attempt, o *Observation) { o.Census.Admitting = 1 }},
		{"census_retained_owner", func(_ *Attempt, o *Observation) { o.Census.OldOwners = []Owner{sourceOwner()} }},
		{"census_old_worker", func(_ *Attempt, o *Observation) { o.Census.Resources[0].Generation = 1 }},
		{"census_unknown_worker_owner", func(_ *Attempt, o *Observation) { o.Census.Resources[0].Owner = Owner{} }},
		{"census_live_as_not_applicable", func(_ *Attempt, o *Observation) {
			r := &o.Census.Resources[0]
			r.State, r.Custody, r.Owner, r.Generation = "not_applicable", "not_applicable", Owner{}, 0
		}},
		{"census_terminal_retained", func(_ *Attempt, o *Observation) {
			r := &o.Census.Resources[0]
			r.Generation, r.State, r.Custody = 1, "terminal", "retained"
		}},
		{"census_late_old_admission", func(_ *Attempt, o *Observation) {
			r := o.Census.Resources[0]
			r.ID, r.Generation = "late-worker", 1
			o.Census.Resources = append(o.Census.Resources, r)
			o.Census.Stamp.Sequence++
		}},
		{"census_sequence_behind", func(_ *Attempt, o *Observation) { o.Census.Stamp.Sequence-- }},
		{"census_sequence_unknown", func(_ *Attempt, o *Observation) { o.Census.Stamp.Sequence = 0 }},
		{"census_cut", func(_ *Attempt, o *Observation) { o.Census.Stamp.Attempt.CutID = testID(999) }},
		{"census_revision", func(_ *Attempt, o *Observation) { o.Census.Stamp.Attempt.Revision-- }},
		{"census_producer", func(_ *Attempt, o *Observation) { o.Census.Stamp.Producer = sourceOwner() }},
		{"census_expected_digest", func(_ *Attempt, o *Observation) { o.Census.ExpectedDigest = testSHA("f") }},
		{"census_scope_digest", func(_ *Attempt, o *Observation) { o.Census.Stamp.ScopeDigest = testSHA("f") }},
		{"nonce_unknown", func(_ *Attempt, o *Observation) { o.Nonce = "" }},
		{"nonce_mismatch", func(_ *Attempt, o *Observation) { o.Nonce = testID(999) }},
		{"nonce_historical", func(a *Attempt, o *Observation) { o.Nonce = a.Control.Stamp.Nonce; o.Census.Stamp.Nonce = o.Nonce }},
		{"time_historical", func(_ *Attempt, o *Observation) { o.At = 1; o.Census.Stamp.At = 1 }},
		{"time_mismatch", func(_ *Attempt, o *Observation) { o.At++ }},
		{"time_unknown", func(_ *Attempt, o *Observation) { o.At = 0; o.Census.Stamp.At = 0 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			a, observation := completionFixture(DirectionForward)
			mutation.edit(&a, &observation)
			assertUnsettled(t, Completed(a, observation))
		})
	}
	// Each receipt is removed/corrupted independently; a single aggregate failure
	// assertion would allow two missing guards to hide behind the remaining one.
	for index, kind := range []ReceiptKind{ReceiptControlReady, ReceiptResourcesMigrated, ReceiptOldGenerationReleased} {
		for _, defect := range []string{"missing", "kind", "known", "complete", "running", "evidence", "cut", "generation", "revision", "nonce", "producer", "sequence", "time", "scope", "expected", "census_stamp", "census_known", "census_complete", "original", "late_identity"} {
			t.Run(string(kind)+"/"+defect, func(t *testing.T) {
				a, observation := completionFixture(DirectionForward)
				receipt := []*Receipt{&a.Control, &a.Resources, &a.Released}[index]
				switch defect {
				case "missing":
					*receipt = Receipt{}
				case "kind":
					receipt.Kind = "future"
				case "known":
					receipt.Known = false
				case "complete":
					receipt.Complete = false
				case "running":
					receipt.RunningSHA = a.SourceSHA
				case "evidence":
					receipt.EvidenceDigest = ""
				case "cut":
					receipt.Stamp.Attempt.CutID = testID(999)
				case "generation":
					receipt.Stamp.Attempt.Generation++
				case "revision":
					receipt.Stamp.Attempt.Revision++
				case "nonce":
					receipt.Stamp.Nonce = ""
				case "producer":
					receipt.Stamp.Producer = sourceOwner()
				case "sequence":
					receipt.Stamp.Sequence = 0
				case "time":
					receipt.Stamp.At = 0
				case "scope":
					receipt.Stamp.ScopeDigest = testSHA("f")
				case "expected":
					receipt.Census.ExpectedDigest = testSHA("f")
				case "census_stamp":
					receipt.Census.Stamp.Nonce = testID(999)
				case "census_known":
					receipt.Census.Known = false
				case "census_complete":
					receipt.Census.Complete = false
				case "original":
					receipt.Census.Resources = nil
				case "late_identity":
					r := receipt.Census.Resources[0]
					r.ID = "new-after-cut"
					receipt.Census.Resources = append(receipt.Census.Resources, r)
				}
				if defect != "census_stamp" {
					receipt.Census.Stamp = receipt.Stamp
				}
				observation.Current = cloneAttempt(a)
				assertUnsettled(t, Completed(a, observation))
			})
		}
	}
	for _, positive := range []string{"empty_known_scope", "terminal_released", "explicit_not_applicable", "new_current_generation", "older_receipt_revision", "control_admission_then_seal"} {
		t.Run(positive, func(t *testing.T) {
			a, o := completionFixture(DirectionForward)
			switch positive {
			case "empty_known_scope":
				a.Scope.Expected = nil
				for _, c := range []*Census{&a.Control.Census, &a.Resources.Census, &a.Released.Census, &o.Census} {
					c.Resources = nil
				}
			case "terminal_released", "explicit_not_applicable":
				for _, c := range []*Census{&a.Control.Census, &a.Resources.Census, &a.Released.Census, &o.Census} {
					r := &c.Resources[0]
					r.State, r.Custody, r.Generation = "terminal", "released", 1
					if positive == "explicit_not_applicable" {
						r.State, r.Custody, r.Owner, r.Generation = "not_applicable", "not_applicable", Owner{}, 0
					}
				}
				if positive == "explicit_not_applicable" {
					a.Scope.Expected = slices.Clone(o.Census.Resources)
				}
			case "new_current_generation":
				r := o.Census.Resources[0]
				r.ID = "post-completion"
				o.Census.Resources = append(o.Census.Resources, r)
				o.Census.Stamp.Sequence++
			case "older_receipt_revision":
				a.Control.Stamp.Attempt.Revision--
				a.Control.Census.Stamp = a.Control.Stamp
			case "control_admission_then_seal":
				a.Control.Census.Admitting = 1
			}
			refreshCommitments(&a, &o)
			o.Current = cloneAttempt(a)
			if err := Completed(a, o); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testID(n uint64) string      { return fmt.Sprintf("%032x", n) }
func testSHA(digit string) string { return strings.Repeat(digit, 64) }
func sourceOwner() Owner {
	return Owner{BootID: "boot-source", PID: 12, StartTicks: 100, InstanceID: testID(12)}
}
func targetOwner() Owner {
	return Owner{BootID: "boot-target", PID: 12, StartTicks: 100, InstanceID: testID(13)}
}

func completionFixture(direction Direction) (Attempt, Observation) {
	key := Key{DomainID: "install-domain", Component: ComponentAgent, SID: "session", ConfiguredNID: "node-1", UID: 1000}
	a := Attempt{
		Handle:    Handle{Key: key, TxID: testID(1), CutID: testID(2), Generation: 2, Revision: 20},
		Direction: direction, SourceSHA: testSHA("a"), TargetSHA: testSHA("b"), SourceGeneration: 1,
		Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess,
		Scope: Scope{ID: testID(3), Participants: []Key{key},
			Domains:  []Domain{DomainAuthority, DomainBrokerRuntime, DomainBrokerStore, DomainControl, DomainExec, DomainFlow, DomainPTY, DomainSS, DomainTransferTracker, DomainTransferWorker, DomainUpgrade},
			Expected: []Resource{{Domain: DomainExec, ID: "original-job", Origin: "node-1", Owner: sourceOwner(), Generation: 1, State: "running", Custody: "owned"}}},
	}
	scopeDigest, _ := ScopeDigest(a.Scope)
	expectedDigest, _ := ExpectedDigest(a.Scope)
	for i, receipt := range []*Receipt{&a.Control, &a.Resources, &a.Released} {
		stamp := Stamp{Attempt: a.Handle, Producer: targetOwner(), Nonce: testID(uint64(110 + i)), Sequence: 42, At: int64(100 + i), ScopeDigest: scopeDigest}
		resources := slices.Clone(a.Scope.Expected)
		resources[0].Owner, resources[0].Generation = targetOwner(), 2
		*receipt = Receipt{Kind: []ReceiptKind{ReceiptControlReady, ReceiptResourcesMigrated, ReceiptOldGenerationReleased}[i], Stamp: stamp,
			Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: testSHA("c"),
			Census: Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: expectedDigest}}
	}
	census := cloneReceipt(a.Released).Census
	census.Stamp.Nonce, census.Stamp.At = testID(201), 200
	return a, Observation{Nonce: census.Stamp.Nonce, At: census.Stamp.At, Current: cloneAttempt(a), Census: census,
		OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
}

func assertUnsettled(t *testing.T, err error) {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != CodeUnsettled || !failure.Accepted {
		t.Fatalf("wanted accepted unsettled result, got %v", err)
	}
}

// origin: hot-update S8 plan §3.3 / §4.2 / P01
func TestCompletionDoesNotMutateEvidence(t *testing.T) {
	a, o := completionFixture(DirectionForward)
	wantAttempt, wantObservation := completionFixture(DirectionForward)
	for round := 0; round < 5; round++ {
		if err := Completed(a, o); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(a, wantAttempt) || !reflect.DeepEqual(o, wantObservation) {
		t.Fatal("completion validation changed caller-owned evidence")
	}
}

// origin: hot-update S8 plan §2.1 / §4.2; S9 plan §8 / T44
func TestInfrastructureCompletionKeepsComponentIdentity(t *testing.T) {
	for _, component := range []Component{ComponentNATS, ComponentCaddy} {
		for _, direction := range []Direction{DirectionForward, DirectionReverse} {
			t.Run(string(component)+"/"+string(direction), func(t *testing.T) {
				a, o := completionFixture(direction)
				key := Key{DomainID: testID(100), Component: component, ConfiguredNID: "node", ConfigPath: "/managed/config", UID: 1000}
				a.Handle.Key, a.Scope.Participants[0] = key, key
				for _, receipt := range []*Receipt{&a.Control, &a.Resources, &a.Released} {
					receipt.Stamp.Attempt.Key = key
					receipt.Census.Stamp = receipt.Stamp
				}
				refreshCommitments(&a, &o)
				o.Current, o.Census.Stamp.Attempt.Key = cloneAttempt(a), key
				if err := Completed(a, o); err != nil {
					t.Fatal(err)
				}
				// Forward and reverse share the pure proof validator, but a
				// record's completion edge still requires the original three
				// independent receipts, each bound to this exact component.
				if direction == DirectionForward {
					current := cloneAttempt(a)
					current.Phase, current.CutStage, current.Outcome = PhaseMigrating, CutRetiring, OutcomePending
					record := recordFixture(current)
					if _, err := Transition(record, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess}); err != nil {
						t.Fatal(err)
					}
				}
				for _, foreign := range []Component{ComponentAgent, ComponentBroker, ComponentNATS, ComponentCaddy, "unknown"} {
					if foreign == component {
						continue
					}
					wrong := o
					wrong.Current = cloneAttempt(o.Current)
					wrong.Current.Handle.Key.Component = foreign
					assertUnsettled(t, Completed(a, wrong))
					for i := range 3 {
						wrongAttempt := cloneAttempt(a)
						r := []*Receipt{&wrongAttempt.Control, &wrongAttempt.Resources, &wrongAttempt.Released}[i]
						r.Stamp.Attempt.Key.Component = foreign
						r.Census.Stamp = r.Stamp
						assertUnsettled(t, Completed(wrongAttempt, o))
					}
				}
			})
		}
		for _, defect := range []string{"unknown", "sid", "node", "long_node", "relative", "unclean", "long_path", "path_control"} {
			t.Run(string(component)+"/"+defect, func(t *testing.T) {
				key := Key{DomainID: testID(100), Component: component, ConfiguredNID: "node", ConfigPath: "/managed/config"}
				switch defect {
				case "unknown":
					key.Component = "unknown"
				case "sid":
					key.SID = "agent-session"
				case "node":
					key.ConfiguredNID = ""
				case "long_node":
					key.ConfiguredNID = strings.Repeat("n", 257)
				case "relative":
					key.ConfigPath = "config"
				case "unclean":
					key.ConfigPath = "/managed/../config"
				case "long_path":
					key.ConfigPath = "/" + strings.Repeat("p", 4096)
				case "path_control":
					key.ConfigPath = "/managed/config\n"
				}
				if validKey(key) {
					t.Fatal("invalid infrastructure key accepted")
				}
			})
		}
	}
}

func refreshCommitments(a *Attempt, o *Observation) {
	scope, _ := ScopeDigest(a.Scope)
	expected, _ := ExpectedDigest(a.Scope)
	for _, r := range []*Receipt{&a.Control, &a.Resources, &a.Released} {
		r.Stamp.ScopeDigest = scope
		r.Census.Stamp = r.Stamp
		r.Census.ExpectedDigest = expected
	}
	o.Census.Stamp.ScopeDigest, o.Census.ExpectedDigest = scope, expected
}
