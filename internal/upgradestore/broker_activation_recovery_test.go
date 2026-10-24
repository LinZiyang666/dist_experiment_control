package upgradestore_test

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type notQueuedFailure struct{ error }

func (notQueuedFailure) ActivationUnstarted() bool { return true }

type notQueuedRuntime struct {
	*composedRuntime
	defect    string
	attempted bool
	attempts  int
}

type uncertainQueueFailure struct{ error }

func (uncertainQueueFailure) ActivationUnstarted() bool { return false }

func (r *notQueuedRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	r.attempted = true
	r.attempts++
	failure := errors.New("maintenance admission refused before queue")
	if r.defect == "unknown admission result" {
		return upgradeport.Activation{}, failure
	}
	if r.defect == "negative no-queue receipt" {
		return upgradeport.Activation{}, uncertainQueueFailure{failure}
	}
	if r.defect == "target already active" {
		_, _ = r.composedRuntime.Activate(c)
	}
	return upgradeport.Activation{}, notQueuedFailure{failure}
}

func (r *notQueuedRuntime) Thaw(h upgradetx.Handle) error {
	if r.defect == "thaw unavailable" {
		return errors.New("source thaw failed")
	}
	return r.composedRuntime.Thaw(h)
}

func (r *notQueuedRuntime) Observe(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	o, err := r.composedRuntime.Observe(stamp)
	if r.attempted && r.defect == "source observer unavailable" {
		o.OwnerKnown = false
	}
	return o, err
}

// origin: S8 internal review roll-recovery-stall F5
func TestBrokerUnqueuedActivationReleasesCurrentWithSourceProof(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerUnqueuedActivation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker unqueued activation", beforeG)
	testharness.AssertNoFDLeak(t, "broker unqueued activation", beforeFD, 0)
}

func exerciseBrokerUnqueuedActivation(t *testing.T) {
	t.Helper()
	r := &notQueuedRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker)}
	v, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err == nil || r.effects != 0 {
		t.Fatalf("missing refusal before the first activation effect: effects=%d err=%v", r.effects, err)
	}
	if !upgradetx.CancellationSettled(v.Historical) || !upgradetx.ColdSelectionSafe(v.Historical) || v.Historical.EffectiveSHA != r.grant.SourceSHA {
		t.Fatalf("known unqueued activation stranded Current: phase=%s stage=%s", composedAttempt(v.Historical).Phase, composedAttempt(v.Historical).CutStage)
	}
	r.reopen()
	durable, err := r.store.Load(r.options.Key, r.grant.Handle.TxID)
	if err != nil || !upgradetx.CancellationSettled(durable.Historical) {
		t.Fatalf("pristine cancellation did not survive reopen: %v", err)
	}
	retry, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err != nil || !upgradetx.CancellationSettled(retry.Historical) || retry.Historical.Revision != durable.Historical.Revision || r.attempts != 1 {
		t.Fatalf("accepted retry repeated the unqueued activation: attempts=%d err=%v", r.attempts, err)
	}
	// A new authorization is possible only after the original no-dispatch and
	// source-custody proof is durable; the rejected target was never executed.
	g := r.prepare([]byte("A"), []byte("C"))
	next, err := r.store.Start(g, upgradetx.Fence{Current: composedAttempt(durable.Historical).Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: durable.Historical.RecordedPreviousSHA})
	if err != nil {
		t.Fatalf("settled refusal still occupies Current: %v", err)
	}
	if next.Forward.SourceGeneration != durable.Historical.Forward.SourceGeneration {
		t.Fatal("a never-started candidate changed the live source generation")
	}
}

// origin: S8 internal review roll-recovery-stall F5
func TestBrokerUnqueuedCancellationKeepsUnprovenAttemptsPending(t *testing.T) {
	for _, defect := range []string{"unknown admission result", "negative no-queue receipt", "target already active", "thaw unavailable", "source observer unavailable"} {
		t.Run(defect, func(t *testing.T) {
			r := &notQueuedRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker), defect: defect}
			v, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
			if err == nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseRecoveryRequired || upgradetx.CancellationSettled(v.Historical) || upgradetx.ColdSelectionSafe(v.Historical) {
				t.Fatalf("unproven cancellation released Current: phase=%s err=%v", composedAttempt(v.Historical).Phase, err)
			}
			r.reopen()
			loaded, err := r.store.Load(r.options.Key, r.grant.Handle.TxID)
			if err != nil || composedAttempt(loaded.Historical).Phase != upgradetx.PhaseRecoveryRequired || len(loaded.Metadata.Pins) == 0 {
				t.Fatalf("unproven cancellation lost its journal or pins: %v", err)
			}
		})
	}
}

type unavailableActivationRuntime struct{ *armingRuntime }

func (r *unavailableActivationRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	if _, err := r.composedRuntime.Activate(c); err != nil {
		return upgradeport.Activation{}, err
	}
	return upgradeport.Activation{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "activation_unavailable"}
}

type carriedActivationRuntime struct {
	*armingRuntime
	carried upgradetx.Handle
}

func (r *carriedActivationRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	r.carried = c.Attempt
	return r.armingRuntime.Activate(c)
}

// origin: S8 internal review roll-recovery-stall F3
func TestImportedBrokerRecoversAfterSourceJournalsUncertainty(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseImportedBrokerRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker carried recovery handle", beforeG)
	testharness.AssertNoFDLeak(t, "broker carried recovery handle", beforeFD, 0)
}

func exerciseImportedBrokerRecovery(t *testing.T) {
	t.Helper()
	r := &carriedActivationRuntime{armingRuntime: &armingRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker)}}
	r.failActivate = true
	v, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err == nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseRecoveryRequired ||
		r.carried.TxID == "" || r.carried.Revision+1 != v.Historical.Revision || r.effects != 1 {
		t.Fatalf("source recovery did not race the carried activation handle: carried=%+v revision=%d err=%v", r.carried, v.Historical.Revision, err)
	}
	r.reopen()
	// The importer carries the handle sealed before exec, rather than loading
	// whichever revision is current. A pre-activation handle has no authority.
	e := brokerupgrade.New(r.store, r, r.registry)
	beforeActivation := r.carried
	beforeActivation.Revision--
	if _, err = brokerupgrade.Resume(t.Context(), e, beforeActivation); err == nil {
		t.Fatal("a handle predating activation acquired recovery authority")
	}
	deps := &brokerupgrade.Dependencies{Store: r.store, Runtime: r, Registry: r.registry, Imported: r.carried}
	brokerupgrade.ResumeImported(t.Context(), e, deps, nil)
	v, err = r.store.Load(r.options.Key, r.carried.TxID)
	if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
		t.Fatalf("source uncertainty stranded the imported cut: phase=%s effects=%d err=%v", composedAttempt(v.Historical).Phase, r.effects, err)
	}
	if _, err = brokerupgrade.Resume(t.Context(), e, r.carried); err == nil {
		t.Fatal("a delayed import callback acquired a completed revision")
	}
	if deps.Imported != r.carried {
		t.Fatal("recovery rewrote the captured import identity")
	}
}

// origin: S8 internal review roll-recovery-stall F2
func TestBrokerRecoveryRetainsTheFailedActivationBoundary(t *testing.T) {
	r := &unavailableActivationRuntime{&armingRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker)}}
	v, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err == nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseRecoveryRequired {
		t.Fatalf("missing accepted activation failure: %v", err)
	}
	if !strings.Contains(v.Historical.Reason, "activate") || !strings.Contains(v.Historical.Reason, "activation_unavailable") {
		t.Fatalf("journal erased the failed boundary and safe refusal: %q", v.Historical.Reason)
	}
}

// origin: S8 internal review roll-recovery-stall F1
func TestBrokerRecoversActivationWithoutDurableSelector(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerActivationRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker activation recovery", beforeG)
	testharness.AssertNoFDLeak(t, "broker activation recovery", beforeFD, 0)
}

func exerciseBrokerActivationRecovery(t *testing.T) {
	t.Helper()
	// Reproduce the preserved journals' boundary with real durable actions:
	// freeze is durable; dispatch/activate have intents but no results. The
	// runtime crossed activation while its reply was lost, leaving the source
	// selected in the journal. Observations read that selector from disk, as
	// the production broker adapter does, independently of the running bytes.
	r := &armingRuntime{composedRuntime: newComposedRuntime(t, upgradetx.ComponentBroker)}
	r.failActivate = true
	v, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	a := composedAttempt(v.Historical)
	if err == nil || r.effects != 1 || a.Phase != upgradetx.PhaseRecoveryRequired ||
		a.CutStage != upgradetx.CutPartialActive || a.Outcome != upgradetx.OutcomeUnknown ||
		v.Historical.EffectiveSHA != r.grant.SourceSHA {
		t.Fatalf("lost activation reply missed the journal boundary: phase=%s stage=%s effects=%d err=%v", a.Phase, a.CutStage, r.effects, err)
	}
	for _, action := range v.Historical.Actions {
		if action.Kind == upgradetx.ActionFreeze && action.State != upgradetx.ActionDurable ||
			(action.Kind == upgradetx.ActionDispatch || action.Kind == upgradetx.ActionActivate) && action.State != upgradetx.ActionIntent {
			t.Fatalf("unexpected action at the lost-reply boundary: %+v", action)
		}
	}
	r.reopen()
	expected := r.current().Handle
	v, err = brokerupgrade.Resume(t.Context(), brokerupgrade.New(r.store, r, r.registry), expected)
	a = composedAttempt(v.Historical)
	if err != nil || a.Phase != upgradetx.PhaseCompleted || a.Outcome != upgradetx.OutcomeSuccess ||
		v.Historical.EffectiveSHA != r.grant.TargetSHA || r.effects != 1 {
		t.Fatalf("target remained stuck behind the missing activation result: phase=%s stage=%s effects=%d err=%v", a.Phase, a.CutStage, r.effects, err)
	}
	for _, action := range v.Historical.Actions {
		if action.State != upgradetx.ActionDurable {
			t.Fatalf("completed with an unsettled action: %+v", action)
		}
	}
	if a.Control.Kind == "" || a.Resources.Kind == "" || a.Released.Kind == "" {
		t.Fatal("recovery omitted an independent completion receipt")
	}
	r.reopen()
	durable, err := r.store.Load(r.options.Key, expected.TxID)
	if err != nil || composedAttempt(durable.Historical).Phase != upgradetx.PhaseCompleted {
		t.Fatalf("completion did not survive reopen: %v", err)
	}
}
