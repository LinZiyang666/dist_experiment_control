package upgradetx

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

var (
	_ ValidatePlanFunc = ValidatePlan
	_ TransitionFunc   = Transition
	_ CompletedFunc    = Completed
)

// origin: hot-update S8 plan §4.1 / §9.1 / P01
func TestTransactionJointTransitions(t *testing.T) {
	// This independent transcription of the frozen joint table includes invalid
	// empty/future enum values. Phase-only testing missed partial ACTIVE in S6.
	rows := []struct {
		phase       Phase
		cuts        string
		outcome     Outcome
		forwardOnly bool
	}{
		{PhasePrepared, "none", OutcomePending, false}, {PhaseStaged, "none", OutcomePending, false},
		{PhaseReady, "none", OutcomePending, false}, {PhaseFreezing, "pristine", OutcomePending, false},
		{PhaseActivating, "pristine partial_active active", OutcomePending, false},
		{PhaseServingNew, "partial_active active", OutcomePending, false},
		{PhaseMigrating, "active retiring", OutcomePending, false}, {PhaseCompleted, "released", OutcomeSuccess, false},
		{PhaseDeferred, "none", OutcomeDeferred, false}, {PhaseRejected, "none", OutcomeRejected, false},
		{PhaseRollbackPending, "partial_active active retiring", OutcomePending, true},
		{PhaseRollbackBlocked, "partial_active active retiring", OutcomePending, true},
		{PhaseRolledBack, "released", OutcomeRolledBack, true},
		{PhaseRecoveryRequired, "none pristine partial_active active retiring released", OutcomeUnknown, false},
		{PhaseCancelled, "none pristine", OutcomeCancelled, false},
		{PhaseAbandoned, "none pristine partial_active active retiring released", OutcomeUnknown, false},
		{"", "", "", false}, {"future", "", "", false},
	}
	count := 0
	for _, row := range rows {
		for _, cut := range []CutStage{"", "future", CutNone, CutPristine, CutPartialActive, CutActive, CutRetiring, CutReleased} {
			for _, direction := range []Direction{"", "future", DirectionForward, DirectionReverse} {
				for _, outcome := range []Outcome{"", "future", OutcomePending, OutcomeSuccess, OutcomeDeferred, OutcomeRejected, OutcomeRolledBack, OutcomeLegacyInterrupting, OutcomeUnknown, OutcomeCancelled} {
					want := slices.Contains(strings.Fields(row.cuts), string(cut)) && outcome == row.outcome &&
						(direction == DirectionForward || direction == DirectionReverse && !row.forwardOnly)
					got := validJointState(Attempt{Phase: row.phase, CutStage: cut, Direction: direction, Outcome: outcome})
					if got != want {
						t.Fatalf("joint state %s/%s/%s/%s: got %t want %t", row.phase, cut, direction, outcome, got, want)
					}
					count++
				}
			}
		}
	}
	if count != 5760 {
		t.Fatalf("joint-table coverage shrank: %d", count)
	}

	for _, direction := range []Direction{DirectionForward, DirectionReverse} {
		t.Run("ordered_"+string(direction), func(t *testing.T) {
			proof, observation := completionFixture(direction)
			a := cloneAttempt(proof)
			a.Phase, a.CutStage, a.Outcome = PhasePrepared, CutNone, OutcomePending
			a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
			record := recordFixture(a)
			events := []Event{
				{Phase: PhaseStaged}, {Phase: PhaseReady}, {Phase: PhaseFreezing, CutStage: CutPristine},
				{Phase: PhaseActivating}, {CutStage: CutPartialActive},
				{Receipt: proof.Resources}, // Resources may arrive before Control.
				{Phase: PhaseServingNew, Receipt: proof.Control},
				{Phase: PhaseMigrating, CutStage: CutActive}, {CutStage: CutRetiring},
				{Receipt: proof.Released}, {Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess},
			}
			for i, event := range events {
				old := snapshotRecord(t, record)
				next, err := Transition(record, event)
				if err != nil {
					t.Fatalf("event %d: %v", i, err)
				}
				if next.Revision != old.Revision+1 || !reflect.DeepEqual(record, old) {
					t.Fatalf("event %d changed input or lost revision", i)
				}
				record = next
			}
			current := record.Forward
			if direction == DirectionReverse {
				current = record.Reverse
			}
			observation.Current = cloneAttempt(current)
			observation.Census.Stamp.Attempt = current.Handle
			if err := Completed(current, observation); err != nil {
				t.Fatal(err)
			}
			for _, event := range []Event{{Reason: "rewrite"}, {Phase: PhaseReady, CutStage: CutNone, Outcome: OutcomePending}, {Receipt: proof.Control}} {
				assertTransitionRejected(t, record, event)
			}
		})
	}
	// Message arrival order is independent of the logical phase progression.
	// All six receipt permutations retain their own kind and stamp.
	for _, order := range [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
		a, _ := completionFixture(DirectionForward)
		proofs := []Receipt{a.Control, a.Resources, a.Released}
		a.Phase, a.CutStage, a.Outcome = PhaseActivating, CutActive, OutcomePending
		a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
		record := recordFixture(a)
		for _, index := range order {
			var err error
			record, err = Transition(record, Event{Receipt: proofs[index]})
			if err != nil {
				t.Fatalf("receipt order %v: %v", order, err)
			}
		}
		if !reflect.DeepEqual(record.Forward.Control, proofs[0]) || !reflect.DeepEqual(record.Forward.Resources, proofs[1]) || !reflect.DeepEqual(record.Forward.Released, proofs[2]) {
			t.Fatalf("receipt order %v lost an independent proof", order)
		}
	}
	// Enumerate all outgoing phase/cut pairs from every pending combination.
	// The strings below are explicit legal destinations, independent of the
	// production edge predicate. Terminal, recovery and failure edges follow.
	edges := map[string]string{
		"prepared/none":                   "prepared/none staged/none",
		"staged/none":                     "staged/none ready/none",
		"ready/none":                      "ready/none freezing/pristine",
		"freezing/pristine":               "freezing/pristine activating/pristine activating/partial_active",
		"activating/pristine":             "activating/pristine activating/partial_active serving_new/partial_active",
		"activating/partial_active":       "activating/partial_active activating/active serving_new/partial_active serving_new/active rollback_pending/partial_active rollback_pending/active rollback_pending/retiring rollback_blocked/partial_active rollback_blocked/active rollback_blocked/retiring",
		"activating/active":               "activating/active serving_new/active rollback_pending/active rollback_pending/retiring rollback_blocked/active rollback_blocked/retiring",
		"serving_new/partial_active":      "serving_new/partial_active serving_new/active migrating/active migrating/retiring rollback_pending/partial_active rollback_pending/active rollback_pending/retiring rollback_blocked/partial_active rollback_blocked/active rollback_blocked/retiring",
		"serving_new/active":              "serving_new/active migrating/active migrating/retiring rollback_pending/active rollback_pending/retiring rollback_blocked/active rollback_blocked/retiring",
		"migrating/active":                "migrating/active migrating/retiring completed/released rollback_pending/active rollback_pending/retiring rollback_blocked/active rollback_blocked/retiring",
		"migrating/retiring":              "migrating/retiring completed/released rollback_pending/retiring rollback_blocked/retiring",
		"rollback_pending/partial_active": "rollback_pending/partial_active rollback_pending/active rollback_pending/retiring rollback_blocked/partial_active rollback_blocked/active rollback_blocked/retiring",
		"rollback_pending/active":         "rollback_pending/active rollback_pending/retiring rollback_blocked/active rollback_blocked/retiring",
		"rollback_pending/retiring":       "rollback_pending/retiring rollback_blocked/retiring",
		"rollback_blocked/partial_active": "rollback_pending/partial_active rollback_pending/active rollback_pending/retiring rollback_blocked/partial_active rollback_blocked/active rollback_blocked/retiring",
		"rollback_blocked/active":         "rollback_pending/active rollback_pending/retiring rollback_blocked/active rollback_blocked/retiring",
		"rollback_blocked/retiring":       "rollback_pending/retiring rollback_blocked/retiring",
	}
	for from, destinations := range edges {
		parts := strings.Split(from, "/")
		for _, row := range rows[:16] {
			for _, cut := range strings.Fields(row.cuts) {
				a, _ := completionFixture(DirectionForward)
				a.Phase, a.CutStage, a.Outcome = Phase(parts[0]), CutStage(parts[1]), OutcomePending
				if a.CutStage == CutNone || a.CutStage == CutPristine {
					a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
				}
				record := recordFixture(a)
				event := Event{Phase: row.phase, CutStage: CutStage(cut), Outcome: row.outcome, Reason: "edge"}
				want := slices.Contains(strings.Fields(destinations), string(row.phase)+"/"+cut)
				if row.phase == PhaseRecoveryRequired && cut == parts[1] {
					want = true
				}
				if (row.phase == PhaseDeferred || row.phase == PhaseRejected) && parts[1] == "none" {
					want = true
				}
				if row.phase == PhaseCancelled && (parts[1] == "none" || parts[1] == "pristine") {
					want = true
				}
				// The edge is structurally reachable, but a pristine record has
				// no control receipt and cannot claim serving in one write.
				if from == "activating/pristine" && row.phase == PhaseServingNew {
					want = false
				}
				_, err := Transition(record, event)
				if (err == nil) != want {
					t.Fatalf("edge %s -> %s/%s: %v, want allowed=%t", from, row.phase, cut, err, want)
				}
			}
		}
	}
}

// origin: hot-update S8 plan §4.1 / §8.1 / M07
func TestReverseRequiresIndependentAttemptReceipts(t *testing.T) {
	forward, _ := completionFixture(DirectionForward)
	forward.Phase, forward.CutStage, forward.Outcome = PhaseRollbackPending, CutActive, OutcomePending
	record := recordFixture(forward)
	reverse := cloneAttempt(forward)
	reverse.Direction, reverse.Handle.CutID, reverse.Handle.Generation = DirectionReverse, testID(4), 3
	reverse.SourceSHA, reverse.TargetSHA, reverse.SourceGeneration = forward.TargetSHA, record.RecordedPreviousSHA, 2
	reverse.Phase, reverse.CutStage = PhaseMigrating, CutRetiring
	for i, r := range []*Receipt{&reverse.Control, &reverse.Resources, &reverse.Released} {
		r.Stamp.Attempt, r.Stamp.Nonce = reverse.Handle, testID(uint64(400+i))
		r.RunningSHA, r.Census.Stamp = reverse.TargetSHA, r.Stamp
		r.Census.Resources[0].Generation = 3
	}
	record.Reverse = reverse
	blocked := cloneRecord(record)
	blocked.Forward.Released = Receipt{}
	assertTransitionRejected(t, blocked, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess})
	blocked.Reverse.Phase, blocked.Reverse.CutStage = PhaseActivating, CutPristine
	blocked.Reverse.Control, blocked.Reverse.Resources, blocked.Reverse.Released = Receipt{}, Receipt{}, Receipt{}
	if _, err := Transition(blocked, Event{Reason: "waiting for forward generation retirement"}); err != nil {
		t.Fatal(err)
	}
	assertTransitionRejected(t, blocked, Event{CutStage: CutPartialActive})
	// A partial forward failure may never have acquired control readiness.
	// Its retirement proof is still necessary, and reverse obtains all three
	// of its own receipts before it can project rolled_back on that failure.
	partial := cloneRecord(record)
	partial.Forward.Control, partial.Forward.Resources = Receipt{}, Receipt{}
	partial.Forward.CutStage = CutPartialActive
	if next, err := Transition(partial, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess}); err != nil || next.Forward.Phase != PhaseRolledBack {
		t.Fatalf("partial forward could not complete an independent reverse: %v", err)
	}
	for i := range 3 {
		bad := cloneRecord(record)
		*[]*Receipt{&bad.Reverse.Control, &bad.Reverse.Resources, &bad.Reverse.Released}[i] = cloneReceipt([]Receipt{forward.Control, forward.Resources, forward.Released}[i])
		assertTransitionRejected(t, bad, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess})
	}
	for _, mutation := range []func(*Record){
		func(r *Record) { r.Reverse.Direction = DirectionForward },
		func(r *Record) { r.Reverse.SourceSHA = r.Forward.SourceSHA },
		func(r *Record) { r.Reverse.TargetSHA = testSHA("f") },
		func(r *Record) { r.Reverse.SourceGeneration-- },
		func(r *Record) {
			r.Reverse.Handle.CutID = r.Forward.Handle.CutID
			for _, receipt := range []*Receipt{&r.Reverse.Control, &r.Reverse.Resources, &r.Reverse.Released} {
				receipt.Stamp.Attempt.CutID = r.Reverse.Handle.CutID
				receipt.Census.Stamp = receipt.Stamp
			}
		},
		func(r *Record) { r.Reverse.Scope.Expected = nil },
		func(r *Record) {
			r.Forward.Phase, r.Forward.CutStage, r.Forward.Outcome = PhaseCompleted, CutReleased, OutcomeSuccess
		},
	} {
		bad := cloneRecord(record)
		mutation(&bad)
		assertTransitionRejected(t, bad, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess})
	}
	next, err := Transition(record, Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess})
	if err != nil {
		t.Fatal(err)
	}
	if next.Reverse.Phase != PhaseCompleted || next.Forward.Phase != PhaseRolledBack || next.Forward.Outcome != OutcomeRolledBack ||
		!reflect.DeepEqual(next.Forward.Control, forward.Control) || next.Reverse.TargetSHA != record.RecordedPreviousSHA {
		t.Fatal("reverse completion lost its independent proof or rewrote forward receipts")
	}
	// Cancelling an unactivated reverse preserves the known new running image;
	// it cannot claim that the old code ran or close the forward responsibility.
	for _, cut := range []CutStage{CutNone, CutPristine} {
		r := cloneRecord(record)
		r.Reverse.Control, r.Reverse.Resources, r.Reverse.Released = Receipt{}, Receipt{}, Receipt{}
		r.Reverse.CutStage = cut
		r.Reverse.Phase = PhasePrepared
		if cut == CutPristine {
			r.Reverse.Phase = PhaseFreezing
		}
		next, err := Transition(r, Event{Phase: PhaseCancelled, Outcome: OutcomeCancelled})
		if err != nil {
			t.Fatal(err)
		}
		if next.Reverse.Phase != PhaseCancelled || next.Forward.Phase != PhaseRollbackPending || next.EffectiveSHA != record.EffectiveSHA {
			t.Fatal("reverse cancellation reported a rollback")
		}
	}
	assertTransitionRejected(t, record, Event{Phase: PhaseCancelled, CutStage: CutPristine, Outcome: OutcomeCancelled})
}

// origin: hot-update S8 plan §3.2 / §3.3 / P01
func TestTransactionActionsPreserveIntentAndHistory(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	a.Phase, a.CutStage, a.Outcome = PhaseActivating, CutPartialActive, OutcomePending
	for i, kind := range []ActionKind{ActionPublish, ActionSelect, ActionFreeze, ActionDispatch, ActionActivate, ActionControl, ActionMigrate, ActionRetire, ActionReverse, ActionRelease} {
		record := recordFixture(a)
		action := Action{ID: testID(uint64(500 + i)), Handle: a.Handle, Kind: kind, IntentDigest: testSHA("a"), State: ActionIntent, Nonce: testID(600)}
		observed := action
		observed.State, observed.ResultDigest = ActionObserved, testSHA("b")
		durable := observed
		durable.State = ActionDurable
		assertTransitionRejected(t, record, Event{Action: observed})
		intentRecord, err := Transition(record, Event{Action: action})
		if err != nil {
			t.Fatal(err)
		}
		assertTransitionRejected(t, intentRecord, Event{Action: durable})
		observedRecord, err := Transition(intentRecord, Event{Action: observed})
		if err != nil {
			t.Fatal(err)
		}
		final, err := Transition(observedRecord, Event{Action: durable})
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := Transition(final, Event{Action: durable})
		if err != nil {
			t.Fatal(err)
		}
		if len(replayed.Actions) != 1 || replayed.Actions[0] != durable || len(record.Actions) != 0 || intentRecord.Actions[0].State != ActionIntent {
			t.Fatal("action replay duplicated or mutated history")
		}
		assertTransitionRejected(t, final, Event{Action: action})
		assertTransitionRejected(t, final, Event{Action: observed})
		for _, edit := range []func(*Action){
			func(v *Action) { v.Kind = "future" }, func(v *Action) { v.Handle.CutID = testID(900) },
			func(v *Action) { v.Handle.Revision = 0 }, func(v *Action) { v.Handle.Revision = 999 },
			func(v *Action) { v.IntentDigest = testSHA("f") }, func(v *Action) { v.ResultDigest = testSHA("f") },
			func(v *Action) { v.Nonce = testID(900) }, func(v *Action) { v.ID = "" },
			func(v *Action) { v.State = "future" },
		} {
			bad := durable
			edit(&bad)
			assertTransitionRejected(t, final, Event{Action: bad})
		}
	}
	// Unknown selection outcomes are recovery_required, never rejected/cancelled.
	for _, phase := range []Phase{PhaseDeferred, PhaseRejected, PhaseCancelled} {
		a.Phase, a.CutStage, a.Outcome = PhaseStaged, CutNone, OutcomePending
		a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
		record := recordFixture(a)
		record.Actions = []Action{{ID: testID(700), Handle: a.Handle, Kind: ActionSelect, IntentDigest: testSHA("a"), State: ActionIntent, Nonce: testID(701)}}
		outcome := map[Phase]Outcome{PhaseDeferred: OutcomeDeferred, PhaseRejected: OutcomeRejected, PhaseCancelled: OutcomeCancelled}[phase]
		assertTransitionRejected(t, record, Event{Phase: phase, Outcome: outcome})
		next, err := Transition(record, Event{Phase: PhaseRecoveryRequired, Outcome: OutcomeUnknown})
		if err != nil {
			t.Fatal(err)
		}
		assertTransitionRejected(t, next, Event{Phase: PhaseReady, Outcome: OutcomePending})
		assertTransitionRejected(t, next, Event{Phase: PhaseAbandoned, Outcome: OutcomeUnknown})
		if _, err := Transition(next, Event{Reason: "custody still unknown"}); err != nil {
			t.Fatal(err)
		}
	}
}

// origin: hot-update S8 plan §4.1 / §4.2 / P01
func TestTransactionRejectsUnprovenCompletionAndCorruptRecords(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	a.Phase, a.CutStage, a.Outcome = PhaseMigrating, CutRetiring, OutcomePending
	finish := Event{Phase: PhaseCompleted, CutStage: CutReleased, Outcome: OutcomeSuccess}
	for _, edit := range []func(*Record){
		func(r *Record) { r.Forward.Control = Receipt{} }, func(r *Record) { r.Forward.Resources = Receipt{} }, func(r *Record) { r.Forward.Released = Receipt{} },
		func(r *Record) { r.Forward.Released.Census.OldOwners = []Owner{sourceOwner()} },
		func(r *Record) { r.Version = 1 }, func(r *Record) { r.Revision = 0 }, func(r *Record) { r.Revision = ^uint64(0) },
		func(r *Record) { r.Revision++; r.Forward.Handle.Revision = r.Revision - 1 },
		func(r *Record) { r.Revision = ^uint64(0); r.Forward.Handle.Revision = r.Revision },
		func(r *Record) { r.Key.UID++ }, func(r *Record) { r.Grant.Handle.TxID = testID(999) },
		func(r *Record) { r.Grant.SourceSHA = testSHA("f") }, func(r *Record) { r.Grant.TargetSHA = testSHA("f") },
		func(r *Record) { r.Grant.ScopeDigest = testSHA("f") }, func(r *Record) { r.Grant.Mode = "" },
		func(r *Record) { r.Grant.Mode = ModeLegacyInterrupting },
		func(r *Record) { r.Forward.Direction = DirectionReverse }, func(r *Record) { r.Forward.Control.Kind = ReceiptResourcesMigrated },
		func(r *Record) { r.Forward.Handle.Generation = 0 }, func(r *Record) { r.Forward.SourceGeneration = 0 },
		func(r *Record) { r.RecordedPreviousSHA = "" }, func(r *Record) { r.EffectiveSHA = "" },
	} {
		record := recordFixture(a)
		edit(&record)
		assertTransitionRejected(t, record, finish)
	}
	for _, kind := range []ReceiptKind{ReceiptControlReady, ReceiptResourcesMigrated, ReceiptOldGenerationReleased} {
		record := recordFixture(a)
		receipt := cloneReceipt(record.Forward.Control)
		receipt.Kind = kind
		receipt.Stamp.Attempt.CutID = testID(999)
		receipt.Census.Stamp = receipt.Stamp
		assertTransitionRejected(t, record, Event{Receipt: receipt})
	}
	for _, receipt := range []Receipt{a.Control, a.Resources, a.Released} {
		record := recordFixture(a)
		if _, err := Transition(record, Event{Receipt: receipt}); err != nil {
			t.Fatal(err)
		}
		receipt.EvidenceDigest = testSHA("f")
		assertTransitionRejected(t, record, Event{Receipt: receipt})
	}
	assertTransitionRejected(t, recordFixture(a), Event{})
	assertTransitionRejected(t, recordFixture(a), Event{Reason: "bad\x00reason"})
	// Without a stored partial-active boundary, even a well-shaped receipt must
	// not smuggle a completed activation into the same first journal update.
	proof := cloneReceipt(a.Control)
	a.Phase, a.CutStage = PhaseActivating, CutPristine
	a.Control, a.Resources, a.Released = Receipt{}, Receipt{}, Receipt{}
	assertTransitionRejected(t, recordFixture(a), Event{Phase: PhaseServingNew, CutStage: CutPartialActive, Receipt: proof})
	assertTransitionRejected(t, recordFixture(a), Event{CutStage: CutActive})
}

// origin: hot-update S8 plan §3.1 / §5.1 / M09
func TestPlanKeepsEveryParticipantAndStableGrant(t *testing.T) {
	plan := planFixture()
	if err := ValidatePlan(plan); err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		edit func(*Plan)
	}{
		{"missing_participant", func(p *Plan) { p.Scope.Participants = p.Scope.Participants[:1] }},
		{"unplanned_participant", func(p *Plan) {
			key := p.Scope.Participants[1]
			key.ConfiguredNID = "node-2"
			p.Scope.Participants = append(p.Scope.Participants, key)
		}},
		{"missing_cut", func(p *Plan) { p.Steps[1].Kind = StepHomeConverge }},
		{"missing_release", func(p *Plan) { p.Steps = p.Steps[:2] }},
		{"duplicate_step", func(p *Plan) { p.Steps[1].Ordinal = p.Steps[0].Ordinal }},
		{"duplicate_participant_cut", func(p *Plan) {
			p.Steps[1].Participant = p.Steps[0].Participant
			p.Steps[1].Grant.Handle.Key = p.Steps[0].Participant
			p.Steps[1].Kind = p.Steps[0].Kind
		}},
		{"forward_dependency", func(p *Plan) { p.Steps[0].DependsOn = []uint32{2} }},
		{"unknown_dependency", func(p *Plan) { p.Steps[1].DependsOn = []uint32{99} }},
		{"duplicate_dependency", func(p *Plan) { p.Steps[1].DependsOn = []uint32{1, 1} }},
		{"unbound_release", func(p *Plan) { p.Steps[2].DependsOn = []uint32{1} }},
		{"release_not_last", func(p *Plan) { p.Steps[0].Kind = StepRelease }},
		{"grant_for_other_target", func(p *Plan) { p.Steps[0].Grant.Handle.Key.UID++ }},
		{"grant_for_other_parent", func(p *Plan) { p.Steps[0].Grant.ParentTxID = testID(999) }},
		{"reused_transaction", func(p *Plan) { p.Steps[1].Grant.Handle.TxID = p.Steps[0].Grant.Handle.TxID }},
		{"reused_cut", func(p *Plan) { p.Steps[1].Grant.Handle.CutID = p.Steps[0].Grant.Handle.CutID }},
		{"reused_slot", func(p *Plan) { p.Steps[1].Grant.ParentSlot = p.Steps[0].Grant.ParentSlot }},
		{"unbound_slot", func(p *Plan) { p.Steps[0].Grant.ParentSlot = "" }},
		{"extended_deadline", func(p *Plan) { p.Steps[0].Grant.ExpiresAt = p.Deadline + 1 }},
		{"unknown_deadline", func(p *Plan) { p.Deadline = 0 }},
		{"unknown_source_manifest", func(p *Plan) { p.SourceManifestDigest = "" }},
		{"invalid_target_manifest", func(p *Plan) { p.TargetManifestDigest = strings.Repeat("G", 64) }},
		{"unknown_kind", func(p *Plan) { p.Steps[0].Kind = "future" }},
		{"wrong_role", func(p *Plan) { p.Steps[0].Kind = StepAgentCut }},
		{"issuer_pid_only", func(p *Plan) { p.Steps[0].Grant.Issuer = Owner{PID: 10} }},
		{"implicit_interrupting", func(p *Plan) { p.Steps[0].Grant.Mode = "" }},
		{"reserved_component", func(p *Plan) { p.Parent.Key.Component = ComponentNATS }},
		{"noncanonical_config", func(p *Plan) { p.Parent.Key.ConfigPath = "/tmp/../broker.yaml" }},
		{"unbounded_identity", func(p *Plan) { p.Parent.Key.DomainID = strings.Repeat("x", 257) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			p := planFixture()
			mutation.edit(&p)
			var failure *Error
			if err := ValidatePlan(p); !errors.As(err, &failure) || failure.Code != CodeRejected || failure.Accepted {
				t.Fatalf("invalid plan was not refused: %v", err)
			}
		})
	}
	// A convergence leg is causally after its component; the final release may
	// depend transitively on the entire DAG rather than repeat every ordinal.
	converging := planFixture()
	converging.Steps[2].Kind = StepHomeConverge
	last := converging.Steps[2]
	last.Ordinal, last.Kind, last.DependsOn = 4, StepRelease, []uint32{3}
	last.Grant.Handle.TxID, last.Grant.Handle.CutID, last.Grant.ParentSlot = testID(1100), testID(1101), testID(1102)
	converging.Steps = append(converging.Steps, last)
	if err := ValidatePlan(converging); err != nil {
		t.Fatal(err)
	}
	converging.Steps[2].DependsOn = nil
	if err := ValidatePlan(converging); err == nil {
		t.Fatal("convergence lost its component dependency")
	}
	siblings := planFixture()
	agent, broker := siblings.Scope.Participants[1], siblings.Scope.Participants[0]
	agent.DomainID = broker.DomainID
	siblings.Scope.Participants = []Key{agent, broker}
	siblings.Steps[1].Participant, siblings.Steps[1].Grant.Handle.Key = agent, agent
	siblings.Steps[1].Grant.ParentSlot = siblings.Steps[0].Grant.ParentSlot
	if err := ValidatePlan(siblings); err != nil {
		t.Fatal(err)
	}
	siblings.Steps[1].DependsOn = nil
	if err := ValidatePlan(siblings); err == nil {
		t.Fatal("sibling reservation was shared without serialization")
	}
}

// origin: hot-update S8 plan §3.3 / §4.1 / P01
func TestTransitionOwnsReturnedEvidence(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	a.Phase, a.CutStage, a.Outcome = PhaseActivating, CutPartialActive, OutcomePending
	receipt := cloneReceipt(a.Control)
	receipt.Census.OldOwners = []Owner{sourceOwner()}
	a.Resources.Census.OldOwners = []Owner{sourceOwner()}
	a.Control = Receipt{}
	record := recordFixture(a)
	original := snapshotRecord(t, record)
	next, err := Transition(record, Event{Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	next.Forward.Scope.Participants[0].UID++
	next.Forward.Scope.Domains[0] = "future"
	next.Forward.Scope.Expected[0].ID = "changed"
	next.Forward.Resources.Census.Resources[0].ID = "changed"
	next.Forward.Control.Census.Resources[0].ID = "changed"
	next.Forward.Resources.Census.OldOwners[0].PID++
	next.Forward.Control.Census.OldOwners[0].PID++
	if !reflect.DeepEqual(record, original) || receipt.Census.Resources[0].ID != "original-job" || receipt.Census.OldOwners[0] != sourceOwner() {
		t.Fatal("returned record aliases its input or event")
	}
}

func recordFixture(a Attempt) Record {
	scopeDigest, _ := ScopeDigest(a.Scope)
	r := Record{Version: RecordVersion, Key: a.Handle.Key, Revision: a.Handle.Revision,
		Grant: Grant{Handle: a.Handle, ClientNonce: testID(10), Issuer: sourceOwner(), ActorFP: "actor", Mode: ModeHot,
			SourceRelease: "source", TargetRelease: "target", SourceSHA: a.SourceSHA, TargetSHA: a.TargetSHA,
			ScopeDigest: scopeDigest, CapabilityDigest: testSHA("f"), ExpiresAt: 1000},
		EffectiveSHA: a.TargetSHA, RecordedPreviousSHA: a.SourceSHA}
	if a.Direction == DirectionReverse {
		r.Reverse = cloneAttempt(a)
		r.RecordedPreviousSHA = a.TargetSHA
	} else {
		r.Forward = cloneAttempt(a)
	}
	return r
}

func planFixture() Plan {
	a, _ := completionFixture(DirectionForward)
	agent := a.Handle.Key
	broker := Key{DomainID: "broker-domain", Component: ComponentBroker, ConfigPath: "/var/lib/example/broker.yaml", UID: 1000}
	p := Plan{Parent: a.Handle, Scope: a.Scope, SourceManifestDigest: testSHA("a"), TargetManifestDigest: testSHA("b"), Deadline: 1000}
	p.Parent.Key = broker
	p.Scope.Participants = []Key{broker, agent}
	for i, kind := range []StepKind{StepBrokerCut, StepAgentCut, StepRelease} {
		key := broker
		if i == 1 {
			key = agent
		}
		g := recordFixture(a).Grant
		g.Handle.Key, g.Handle.TxID, g.Handle.CutID = key, testID(uint64(800+i)), testID(uint64(900+i))
		g.ParentTxID, g.ParentSlot = p.Parent.TxID, testID(uint64(1000+i))
		step := Step{Ordinal: uint32(i + 1), Participant: key, Grant: g, Kind: kind}
		if i > 0 {
			step.DependsOn = []uint32{uint32(i)}
		}
		p.Steps = append(p.Steps, step)
	}
	return p
}

func assertTransitionRejected(t *testing.T, record Record, event Event) {
	t.Helper()
	want := snapshotRecord(t, record)
	result, err := Transition(record, event)
	var failure *Error
	if !errors.As(err, &failure) || !failure.Accepted || !reflect.DeepEqual(result, Record{}) {
		t.Fatalf("unsafe transition was not rejected: %v", err)
	}
	if !reflect.DeepEqual(record, want) {
		t.Fatal("rejected event changed the original record")
	}
}

// Use a different mechanism from the production clone functions: a broken
// clone must not corrupt the test's saved oracle along with the input value.
func snapshotRecord(t *testing.T, record Record) Record {
	t.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Record
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
