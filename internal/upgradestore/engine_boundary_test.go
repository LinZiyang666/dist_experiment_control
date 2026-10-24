package upgradestore_test

// Copy into internal/upgradestore. These tests reuse the existing local-facts
// fixture while exercising the real Store, Transition, Registry, and engines.
import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func boundaryEndpoint(s upgradetx.Store, r *composedRuntime, port upgradeport.RuntimePort) upgradeport.Endpoint {
	if r.options.Key.Component == upgradetx.ComponentAgent {
		return agentupgrade.New(s, port, r.registry)
	}
	return brokerupgrade.New(s, port, r.registry)
}

type admissionAfterControlStore struct {
	*upgradestore.Store
	runtime *composedRuntime
	fired   bool
}

func (s *admissionAfterControlStore) Apply(h upgradetx.Handle, e upgradetx.Event) (upgradetx.Record, error) {
	out, err := s.Store.Apply(h, e)
	if err != nil {
		return out, err
	}
	// Read every successful write using an independently reopened real store.
	s.runtime.reopen()
	if e.Receipt.Kind == upgradetx.ReceiptControlReady && !s.fired {
		s.fired = true
		r := s.runtime
		row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "between-control-and-seal", Origin: "live-admission", Owner: r.owner, Generation: composedAttempt(out).Handle.Generation, State: "active", Custody: "target"}
		seq, err := r.registry.Begin(row)
		if err != nil {
			r.t.Fatalf("legitimate target Begin: %v", err)
		}
		r.resources = append(r.resources, row)
		r.sequence = seq
		if err = r.registry.Finish(seq, row); err != nil {
			r.t.Fatal(err)
		}
	}
	return out, nil
}

// origin: S8 internal review rv3-leaves F4
func TestCompletionIncludesAdmissionBetweenControlAndSeal(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			s := &admissionAfterControlStore{Store: r.store, runtime: r}
			v, err := boundaryEndpoint(s, r, r).Start(t.Context(), r.grant, r.receipt)
			if !s.fired {
				t.Fatal("admission boundary was not exercised")
			}
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
				persisted, loadErr := r.store.Load(r.options.Key, "")
				t.Fatalf("settled target admission prevented forward completion: returned=%s persisted_forward=%s persisted_reverse=%s effects=%d err=%v load=%v", composedAttempt(v.Historical).Phase, persisted.Historical.Forward.Phase, persisted.Historical.Reverse.Phase, r.effects, err, loadErr)
			}
		})
	}
}

// origin: S8 internal review rv3-leaves F6
func TestCancelledAllocationComposesWithGenerationPreflight(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := &cancellationRuntime{composedRuntime: newComposedRuntime(t, role), defect: "none"}
			record := cancellationFrozen(t, r)
			v, err := r.endpoint().Cancel(t.Context(), composedAttempt(record).Handle)
			if err != nil {
				t.Fatal(err)
			}
			r.reopen()
			source := composedAttempt(v.Historical).SourceGeneration
			r.prepare([]byte("A"), []byte("C"))
			proof, err := r.Preflight(t.Context(), r.grant, r.receipt)
			if err != nil {
				t.Fatal(err)
			}
			if err = upgradecompat.CheckGenerations(proof.Census, source, r.grant.Handle.Generation); err != nil {
				t.Fatalf("cancelled allocation poisons next preflight: source=%d target=%d live=%d err=%v", source, r.grant.Handle.Generation, len(proof.Census.Resources), err)
			}
			// Continue through all three complete compatibility gates and a real
			// second Start, preserving source ownership across the skipped grant.
			port := &generationPreflightRuntime{composedRuntime: r.composedRuntime, source: source}
			v, err = boundaryEndpoint(r.store, r.composedRuntime, port).Start(t.Context(), r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || port.checks != 3 || r.effects != 1 || composedAttempt(v.Historical).SourceGeneration != source {
				t.Fatalf("next legal Start did not compose with compatibility: checks=%d effects=%d err=%v", port.checks, r.effects, err)
			}
		})
	}
}

// origin: S8 internal review rv3-leaves F1
func TestBrokerRechecksPreflightBeforeFreezeAndActivation(t *testing.T) {
	for _, failure := range []int{2, 3} {
		t.Run(string(rune('0'+failure)), func(t *testing.T) {
			r := newComposedRuntime(t, upgradetx.ComponentBroker)
			r.failPreflightAt = failure
			v, err := r.endpoint().Start(t.Context(), r.grant, r.receipt)
			if err == nil || r.effects != 0 {
				t.Fatalf("withdrawn reader was never rechecked: calls=%d effects=%d phase=%s err=%v", r.preflights, r.effects, composedAttempt(v.Historical).Phase, err)
			}
			if r.running != r.grant.SourceSHA || v.Historical.EffectiveSHA != r.grant.SourceSHA || r.resources[0].Generation != 1 {
				t.Fatal("withdrawn reader changed source selector or custody")
			}
			if failure == 2 && v.Historical.Forward.CutStage != upgradetx.CutNone {
				t.Fatal("second preflight ran after the Freeze boundary")
			}
		})
	}
}

// origin: S8 internal review rv3-leaves F8
func TestAgentManualReverseReaderRefusalKeepsLiveDiagnostic(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentAgent)
	v, err := r.endpoint().Start(t.Context(), r.grant, r.receipt)
	if err != nil {
		t.Fatal(err)
	}
	a := composedAttempt(v.Historical)
	original := v.Historical
	r.failPreflightAt = r.preflights + 1
	fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: v.Historical.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
	v, err = r.endpoint().Rollback(t.Context(), fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: a.Scope})
	var txerr *upgradetx.Error
	if !errors.As(err, &txerr) || txerr.Code != upgradetx.CodeUnsettled || v.Code != upgradetx.CodeUnsettled || !strings.Contains(v.Detail, "previous reader") || !strings.Contains(txerr.Detail, "previous reader") {
		t.Fatalf("reader refusal was replaced by a write to immutable history: view_code=%q detail=%q err=%v", v.Code, v.Detail, err)
	}
	r.reopen()
	persisted, loadErr := r.store.Load(r.options.Key, a.Handle.TxID)
	if loadErr != nil || !reflect.DeepEqual(original, persisted.Historical) {
		t.Fatal("reader refusal changed immutable completed history")
	}
}

type terminalAfterSealStore struct {
	*upgradestore.Store
	runtime *composedRuntime
	fired   bool
	defect  string
}

func (s *terminalAfterSealStore) Apply(h upgradetx.Handle, e upgradetx.Event) (upgradetx.Record, error) {
	out, err := s.Store.Apply(h, e)
	if err != nil {
		return out, err
	}
	s.runtime.reopen()
	if e.Receipt.Kind == upgradetx.ReceiptResourcesMigrated && !s.fired {
		s.fired = true
		s.runtime.resources[0].State = "terminal"
		s.runtime.resources[0].Custody = "released"
		switch s.defect {
		case "retained":
			s.runtime.resources[0].Custody = "retained"
			s.runtime.resources[0].Generation = 1
		case "identity":
			s.runtime.resources[0].ID = "replacement"
		case "origin":
			s.runtime.resources[0].Origin = "replacement"
		case "owner":
			s.runtime.resources[0].State, s.runtime.resources[0].Custody = "active", "source"
			s.runtime.resources[0].Generation = 1
		}
	}
	return out, nil
}

// origin: S8 internal review rv3-leaves F4 F5
func TestCompletionRejectsUnsettledSealedCustody(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for _, defect := range []string{"retained", "identity", "origin", "owner"} {
			t.Run(string(role)+"/"+defect, func(t *testing.T) {
				r := newComposedRuntime(t, role)
				s := &terminalAfterSealStore{Store: r.store, runtime: r, defect: defect}
				v, err := boundaryEndpoint(s, r, r).Start(t.Context(), r.grant, r.receipt)
				if !s.fired || err == nil || v.Historical.Forward.Phase == upgradetx.PhaseCompleted || r.effects != 1 {
					t.Fatalf("sealed %s custody was declared completed: effects=%d err=%v", defect, r.effects, err)
				}
			})
		}
	}
}

// origin: S8 internal review rv3-leaves F5
func TestCompletionAllowsSealedResourceToFinish(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			s := &terminalAfterSealStore{Store: r.store, runtime: r}
			v, err := boundaryEndpoint(s, r, r).Start(t.Context(), r.grant, r.receipt)
			if !s.fired {
				t.Fatal("seal boundary was not exercised")
			}
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("legitimate durable terminal invalidated completion: phase=%s effects=%d err=%v", composedAttempt(v.Historical).Phase, r.effects, err)
			}
		})
	}
}

// origin: S8 internal review rv3-leaves F7
func TestManualReverseCanCancelBeforeAnyReverseEffect(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentAgent)
	v, err := r.endpoint().Start(t.Context(), r.grant, r.receipt)
	if err != nil {
		t.Fatal(err)
	}
	a := composedAttempt(v.Historical)
	r.failPreflightAt = r.preflights + 2
	fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: v.Historical.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
	_, err = r.endpoint().Rollback(t.Context(), fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: a.Scope})
	if err == nil || r.effects != 1 {
		t.Fatal("reverse preflight fault not exercised")
	}
	r.reopen()
	pending := r.current()
	if pending.Direction != upgradetx.DirectionReverse || pending.CutStage != upgradetx.CutNone {
		t.Fatalf("wrong reverse boundary: %+v", pending)
	}
	v, err = r.endpoint().Cancel(t.Context(), pending.Handle)
	if err != nil {
		t.Fatal(err)
	}
	if !upgradetx.CancellationSettled(v.Historical) {
		t.Error("cancelled pristine manual reverse retains Current despite source proof")
	}
	r.reopen()
	if _, err = r.store.CloseWindow(r.current().Handle); err != nil {
		t.Fatalf("cancelled reverse cannot close: %v", err)
	}
	r.reopen()
	r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
	r.prepare([]byte("B"), []byte("C"))
	v, err = r.endpoint().Start(t.Context(), r.grant, r.receipt)
	if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 2 || composedAttempt(v.Historical).SourceGeneration != pending.SourceGeneration {
		t.Fatalf("cancelled manual reverse prevented the next Start: effects=%d err=%v", r.effects, err)
	}
}

type oneFinalProbeFailure struct {
	*composedRuntime
	fired bool
}

func (r *oneFinalProbeFailure) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	a := r.current()
	if a.Direction == upgradetx.DirectionForward && a.Released.Kind != "" && !r.fired {
		r.fired = true
		return upgradetx.Observation{}, errors.New("one final probe reply lost after durable retirement")
	}
	return r.composedRuntime.Observe(s)
}

// origin: S8 internal review rv3-leaves F3
func TestAutomaticReverseAfterDurableForwardRetirement(t *testing.T) {
	r := &oneFinalProbeFailure{composedRuntime: newComposedRuntime(t, upgradetx.ComponentAgent)}
	_, err := agentupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err == nil || !r.fired {
		t.Fatal("post-retirement failure was not injected")
	}
	r.reopen()
	v, loadErr := r.store.Load(r.options.Key, "")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if v.Historical.Forward.Phase != upgradetx.PhaseRolledBack || v.Historical.Reverse.Phase != upgradetx.PhaseCompleted || r.effects != 2 {
		t.Fatalf("automatic reverse cannot reuse independently proven retirement: forward=%s reverse=%s stage=%s effects=%d err=%v", v.Historical.Forward.Phase, v.Historical.Reverse.Phase, v.Historical.Reverse.CutStage, r.effects, err)
	}
}
