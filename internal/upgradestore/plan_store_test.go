package upgradestore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgraderoll"
)

// origin: S8 internal review remaining-orchestration F6 (R6)
func TestJournalImplementsDurableParentPlanStore(t *testing.T) {
	s, _ := newFixture(t)
	if _, ok := any(s).(upgraderoll.PlanStore); !ok {
		t.Fatal("sole journal writer has no durable parent plan adapter; resume is permanently unwired")
	}
}

func parentPlanFixture(t *testing.T) (*Store, upgradetx.Plan) {
	t.Helper()
	s, req := newFixture(t)
	s.now = time.Now
	s.options.Owner.InstanceID = strings.Repeat("9", 32)
	req.Scope.Expected[0].Owner = s.options.Owner
	req.ClientNonce = strings.Repeat("8", 32)
	req.ExpiresAt = time.Now().Add(time.Minute).UnixNano()
	grant := prepare(t, s, req)
	record := start(t, s, grant)
	plan := upgradetx.Plan{Parent: record.Forward.Handle, Scope: record.Forward.Scope, SourceManifestDigest: strings.Repeat("a", 64), TargetManifestDigest: strings.Repeat("b", 64), Deadline: grant.ExpiresAt}
	for i, kind := range []upgradetx.StepKind{upgradetx.StepAgentCut, upgradetx.StepHomeConverge, upgradetx.StepRelease, upgradetx.StepRelease} {
		child := grant
		child.Handle.TxID = fmt.Sprintf("%032x", i+20)
		child.Handle.CutID = fmt.Sprintf("%032x", i+40)
		child.ParentTxID, child.ParentSlot = plan.Parent.TxID, "host-slot"
		child.ClientNonce = fmt.Sprintf("%032x", i+60)
		var deps []uint32
		if i > 0 {
			deps = []uint32{uint32(i)}
		}
		plan.Steps = append(plan.Steps, upgradetx.Step{Ordinal: uint32(i + 1), Participant: grant.Handle.Key, Grant: child, Kind: kind, DependsOn: deps})
	}
	if err := upgraderoll.ValidatePlan(plan); err != nil {
		t.Fatal(err)
	}
	return s, plan
}

// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §3.1 M09
func TestParentPlanSurvivesReopenWithoutReplanning(t *testing.T) {
	s, plan := parentPlanFixture(t)
	journal := any(s).(upgraderoll.PlanStore)
	sealed, err := journal.SealPlan(plan)
	if err != nil || !reflect.DeepEqual(sealed, plan) {
		t.Fatalf("seal: %+v %v", sealed, err)
	}
	sealed.Steps[1].DependsOn[0] = 99
	sealed.Scope.Participants[0].SID = "changed"
	fresh := reopen(t, s)
	loaded, err := fresh.LoadPlan(plan.Parent)
	if err != nil || !reflect.DeepEqual(loaded, plan) {
		t.Fatalf("original plan lost on reopen: %+v %v", loaded, err)
	}
	for _, mutate := range []func(*upgradetx.Plan){
		func(p *upgradetx.Plan) { p.Steps[0].Grant.Handle.TxID = strings.Repeat("e", 32) },
		func(p *upgradetx.Plan) { p.TargetManifestDigest = strings.Repeat("e", 64) },
		func(p *upgradetx.Plan) {
			p.Deadline++
			for i := range p.Steps {
				p.Steps[i].Grant.ExpiresAt = p.Deadline
			}
		},
	} {
		changed := copyPlan(plan)
		mutate(&changed)
		if _, err := fresh.SealPlan(changed); err == nil {
			t.Fatal("changed snapshot replaced the original plan")
		}
	}
	if _, err := fresh.SealPlan(plan); err != nil {
		t.Fatalf("lost-reply retry: %v", err)
	}
	for _, handle := range []upgradetx.Handle{func() upgradetx.Handle { h := plan.Parent; h.CutID = strings.Repeat("e", 32); return h }(), func() upgradetx.Handle { h := plan.Parent; h.Revision += 100; return h }()} {
		if _, err := fresh.LoadPlan(handle); err == nil {
			t.Fatal("another cut or future revision loaded a plan")
		}
	}
}

// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §3.1
func TestParentPlanSealRequiresDurabilityAndOriginalParent(t *testing.T) {
	for _, kind := range []string{"file", "directory"} {
		t.Run(kind, func(t *testing.T) {
			s, plan := parentPlanFixture(t)
			s.beforeSync = func(at string) error {
				if at == kind {
					return syscall.ENOSPC
				}
				return nil
			}
			if got, err := s.SealPlan(plan); err == nil || got.Parent.TxID != "" {
				t.Fatal("failed sync published a plan receipt")
			}
			s.beforeSync = nil
			if _, err := reopen(t, s).SealPlan(plan); err != nil {
				t.Fatal(err)
			}
		})
	}
	s, plan := parentPlanFixture(t)
	stale := copyPlan(plan)
	stale.Parent.CutID = strings.Repeat("f", 32)
	if _, err := s.SealPlan(stale); err == nil {
		t.Fatal("plan sealed against another parent cut")
	}
	action := upgradetx.Action{ID: strings.Repeat("3", 32), Handle: plan.Parent, Kind: upgradetx.ActionDispatch, IntentDigest: strings.Repeat("4", 64), State: upgradetx.ActionIntent, Nonce: strings.Repeat("5", 32)}
	if _, err := s.Apply(plan.Parent, upgradetx.Event{Action: action}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SealPlan(plan); err == nil {
		t.Fatal("plan invented after first effect intent")
	}
}

// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §3.1
func TestParentPlanDecoderRejectsChecksumValidPlanTampering(t *testing.T) {
	for _, mutate := range []func(*upgradetx.Plan){
		func(p *upgradetx.Plan) { p.Steps = p.Steps[1:] },
		func(p *upgradetx.Plan) { p.Parent.CutID = strings.Repeat("f", 32) },
		func(p *upgradetx.Plan) { p.Scope.Expected = nil },
	} {
		s, plan := parentPlanFixture(t)
		if _, err := s.SealPlan(plan); err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(s.options.Path)
		if err != nil {
			t.Fatal(err)
		}
		var disk diskFile
		if err := json.Unmarshal(body, &disk); err != nil {
			t.Fatal(err)
		}
		mutate(&disk.Entries[0].Plans[0])
		disk.Checksum = ""
		body, _ = json.Marshal(disk)
		disk.Checksum = digestBytes(body)
		body, _ = json.Marshal(disk)
		if err := os.WriteFile(s.options.Path, body, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = Open(s.options)
		requireCode(t, err, upgradetx.CodeStateCorrupt)
	}
}

// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §4.2
func TestParentCompletionCannotInventStepResults(t *testing.T) {
	s, plan := parentPlanFixture(t)
	if _, err := s.SealPlan(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompletePlan(plan.Parent, upgradetx.Observation{}); err == nil {
		t.Fatal("parent completed without receipts and durable step results")
	}
}

func parentObservation(t *testing.T, s *Store, plan upgradetx.Plan) upgradetx.Observation {
	t.Helper()
	view, err := s.Load(plan.Parent.Key, plan.Parent.TxID)
	if err != nil {
		t.Fatal(err)
	}
	attempt := view.Historical.Forward
	attempt.Phase, attempt.CutStage, attempt.Outcome = upgradetx.PhaseCompleted, upgradetx.CutReleased, upgradetx.OutcomeSuccess
	rows := append([]upgradetx.Resource(nil), attempt.Scope.Expected...)
	for i := range rows {
		rows[i].Owner = s.options.Owner
		rows[i].Generation = attempt.Handle.Generation
		rows[i].Custody = "target"
	}
	nonce := strings.Repeat("6", 32)
	digest, err := upgradetx.ExpectedDigest(attempt.Scope)
	if err != nil {
		t.Fatal(err)
	}
	stamp := upgradetx.Stamp{Attempt: attempt.Handle, Producer: s.options.Owner, Nonce: nonce, Sequence: 10, At: 100, ScopeDigest: upgraderoll.ScopeDigest(attempt.Scope)}
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: rows, ExpectedDigest: digest}
	receipts := []*upgradetx.Receipt{&attempt.Control, &attempt.Resources, &attempt.Released}
	for i, kind := range []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased} {
		*receipts[i] = upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: attempt.TargetSHA, Census: census, EvidenceDigest: strings.Repeat("7", 64)}
	}
	return upgradetx.Observation{Nonce: nonce, At: 100, Current: attempt, Census: census, OwnerKnown: true, RunningSHA: attempt.TargetSHA, EffectiveSHA: attempt.TargetSHA}
}

// origin: S8 internal review remaining-orchestration F6 (R6) / hot-update S8 plan §4.2 §5.1
func TestParentCompletionPersistsOnlyAggregateProof(t *testing.T) {
	s, plan := parentPlanFixture(t)
	if _, err := s.SealPlan(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompletePlan(plan.Parent, parentObservation(t, s, plan)); err == nil {
		t.Fatal("three receipts replaced missing step results")
	}
	for _, step := range plan.Steps {
		body, _ := json.Marshal(step)
		sum := sha256.Sum256(body)
		kind := upgradetx.ActionDispatch
		if step.Kind == upgradetx.StepHomeConverge {
			kind = upgradetx.ActionControl
		}
		if step.Kind == upgradetx.StepRelease {
			kind = upgradetx.ActionRelease
		}
		action := upgradetx.Action{ID: hex.EncodeToString(sum[:])[:32], Handle: plan.Parent, Kind: kind, IntentDigest: upgraderoll.PlanDigest(plan), Nonce: step.Grant.ClientNonce}
		for _, state := range []upgradetx.ActionState{upgradetx.ActionIntent, upgradetx.ActionObserved, upgradetx.ActionDurable} {
			action.State = state
			if state != upgradetx.ActionIntent {
				action.ResultDigest = strings.Repeat("c", 64)
			}
			view, err := s.Load(plan.Parent.Key, plan.Parent.TxID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Apply(view.Historical.Forward.Handle, upgradetx.Event{Action: action}); err != nil {
				t.Fatal(err)
			}
		}
	}
	proof := parentObservation(t, s, plan)
	for _, mutate := range []func(*upgradetx.Observation){
		func(o *upgradetx.Observation) { o.RunningSHA = plan.Steps[0].Grant.SourceSHA },
		func(o *upgradetx.Observation) { o.Current.Released.Known = false },
		func(o *upgradetx.Observation) { o.Current.Resources.Census.Resources[0].Generation = 1 },
	} {
		body, _ := json.Marshal(proof)
		var bad upgradetx.Observation
		_ = json.Unmarshal(body, &bad)
		mutate(&bad)
		if _, err := s.CompletePlan(proof.Current.Handle, bad); err == nil {
			t.Fatal("incomplete aggregate proof completed parent")
		}
	}
	view, err := s.CompletePlan(proof.Current.Handle, proof)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopen(t, s).Load(plan.Parent.Key, plan.Parent.TxID)
	if err != nil || !reflect.DeepEqual(loaded.Historical, view.Historical) || loaded.Historical.Forward.Phase != upgradetx.PhaseCompleted {
		t.Fatalf("completion did not survive reopen: %+v %v", loaded, err)
	}
	if _, err := reopen(t, s).LoadPlan(plan.Parent); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompletePlan(proof.Current.Handle, proof); err == nil {
		t.Fatal("stale completion rewrote history")
	}
}
