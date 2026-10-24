package brokerupgrade

import (
	"context"
	"errors"
	"log/slog"
	"reflect"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Resume reconciles an accepted cut after boot using the endpoint that owns the
// component sequencer. It never re-dispatches an image or opens an old database.
// The caller captures expected at import; loading a newer Current is not a way
// for a delayed boot callback to acquire that newer transaction's authority.
// origin: hot-update S8 plan §3.3 / §4 / §8.1
func Resume(ctx context.Context, endpoint upgradeport.Endpoint, expected upgradetx.Handle) (upgradetx.View, error) {
	e, ok := endpoint.(*engine)
	if !ok {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "resume requires the owning broker endpoint")
	}
	if err := e.enter(ctx, expected.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	view, err := e.loadExpected(expected)
	if err != nil {
		return view, err
	}
	a := active(view.Historical)
	if a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack {
		return e.query(ctx, expected.Key, expected.TxID, nonce())
	}
	if a.CutStage != upgradetx.CutPartialActive && a.CutStage != upgradetx.CutActive && a.CutStage != upgradetx.CutRetiring {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "boot cannot redispatch an unproven cut"))
	}
	return e.complete(ctx, view.Historical, upgradetx.Owner{})
}

func (e *engine) drive(ctx context.Context, record upgradetx.Record, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradetx.View, error) {
	if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseStaged, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}); err != nil {
		return e.failed(record, err)
	}
	if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseReady, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}); err != nil {
		return e.failed(record, err)
	}
	if err := ctx.Err(); err != nil {
		return e.failed(record, err)
	}
	// origin: S8 internal review rv3-leaves F1
	// The accepted attempt keeps its own grant and publication binding. A
	// reverse must not borrow the failed forward's reader/authority evidence.
	if _, err := e.preflight(ctx, grant, artifact); err != nil {
		return e.failed(record, err)
	}
	freeze, err := e.intent(&record, upgradetx.ActionFreeze, upgradetx.PhaseFreezing, upgradetx.CutPristine)
	if err != nil {
		return e.failed(record, err)
	}
	frozen, err := e.runtime.Freeze(handle(record))
	if err != nil {
		return e.failed(record, err)
	}
	if !validCut(frozen, record) || frozen.Stage != upgradetx.CutPristine {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "freeze did not prove pristine current custody"))
	}
	if err = e.result(&record, freeze, digest(frozen.Scope)); err != nil {
		return e.failed(record, err)
	}
	dispatch, err := e.intent(&record, upgradetx.ActionDispatch, upgradetx.PhaseFreezing, upgradetx.CutPristine)
	if err != nil {
		return e.failed(record, err)
	}
	// Export the handle only after the activation intent is durable. Nothing
	// may increment its revision between this export and an exec: the imported
	// boot callback must carry the exact CAS handle, not reload and claim Current.
	activation, err := e.intent(&record, upgradetx.ActionActivate, upgradetx.PhaseActivating, upgradetx.CutPartialActive)
	if err != nil {
		return e.failed(record, err)
	}
	// origin: hot-update S8 plan §8.1 / §11.2 P10 (old DB mutation)
	// Freeze's image is never a rollback input. ExportCurrent must include the
	// latest ACK overwrites/deletions and partial-Take salvage before any Replay.
	// origin: S8 internal review roll-recovery-stall F7
	// These failures precede our Activate call. Preserve that causal boundary
	// so a conservative partial-active intent can settle after real source thaw.
	cut, err := e.runtime.ExportCurrent(handle(record))
	if err != nil {
		return e.failed(record, ArmNotQueued(err), "export_current")
	}
	if !validCut(cut, record) || cut.Stage != upgradetx.CutPristine || cut.Sequence < frozen.Sequence {
		return e.failed(record, ArmNotQueued(refusal(upgradetx.CodeUnsettled, "current export is stale or has incomplete custody")), "export_current")
	}
	// origin: S8 internal review rv3-leaves F1
	// Reader/authority/budget facts may change during Freeze or final export.
	// No activation may rely on either earlier preflight's stale conclusion.
	if _, err = e.preflight(ctx, grant, artifact); err != nil {
		return e.failed(record, ArmNotQueued(err), "preflight_activation")
	}
	if err = ctx.Err(); err != nil {
		return e.failed(record, ArmNotQueued(err), "activation_context")
	}
	activated, err := e.runtime.Activate(cut)
	// origin: hot-update S8 plan §3.1 / §4.1; S8 BROKER-EXPORTER R-11. The S6
	// store cut is armed: its final image exists over the closed store, and the
	// exec follows once this Start's reply has drained. The durable activation
	// intent stays as journaled; the imported target settles this exact handle
	// (Resume), and a reload that returns instead journals recovery itself.
	if activationDispatched(err) {
		return upgradetx.View{Historical: record, Code: upgradetx.CodeUnsettled, Detail: ReasonActivationDispatched}, err
	}
	if err != nil {
		return e.failed(record, err, "activate")
	}
	a := active(record)
	if !sameAttempt(activated.Attempt, a.Handle) || !knownOwner(activated.Owner) ||
		activated.Stage != upgradetx.CutActive || activated.RunningSHA != a.TargetSHA {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "activation did not prove the target running image"))
	}
	// Export alone cannot prove delivery. Only successful activation (or the
	// boot readback below) establishes that dispatch actually transferred custody.
	if err = e.result(&record, dispatch, digest(cut.Scope)); err != nil {
		return e.failed(record, err)
	}
	if err = e.result(&record, activation, digest(activated)); err != nil {
		return e.failed(record, err)
	}
	return e.complete(ctx, record, activated.Owner)
}

func (e *engine) complete(ctx context.Context, record upgradetx.Record, activatedOwner upgradetx.Owner) (upgradetx.View, error) {
	if err := resumeBrokerActivation(ctx, e, &record, activatedOwner); err != nil {
		return e.failed(record, err)
	}
	a := active(record)
	// The nonce-bearing control probe is itself an action: a crash after its
	// request must not leave an effect that preceded the durable intent.
	control, err := e.intent(&record, upgradetx.ActionControl, "", "")
	if err != nil {
		return e.failed(record, err)
	}
	stamp := makeStamp(handle(record), record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
	o, err := e.runtime.Observe(stamp)
	if err != nil || !currentProof(o, stamp, a) || (knownOwner(activatedOwner) && o.Census.Stamp.Producer != activatedOwner) {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "fresh running-image and current-cut proof is required"))
	}
	// A boot that crossed exec may have lost the activate result. Observing the
	// exact running cut settles that intent; it never invokes Activate again.
	if err = e.settle(&record, upgradetx.ActionActivate, digest(o.Current)); err != nil {
		return e.failed(record, err)
	}
	if err = e.settle(&record, upgradetx.ActionDispatch, digest(o.Current.Scope)); err != nil {
		return e.failed(record, err)
	}
	if !validReceipt(o.Current.Control, upgradetx.ReceiptControlReady, a, stamp, o.Census.Stamp.Producer) || o.Current.Control.Stamp.Nonce != stamp.Nonce {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "broker control subscriptions and new-request proof are incomplete"))
	}
	phase, stage := upgradetx.PhaseServingNew, upgradetx.CutActive
	if a.Phase == upgradetx.PhaseMigrating || a.CutStage == upgradetx.CutRetiring {
		phase = upgradetx.PhaseMigrating
	}
	if a.CutStage == upgradetx.CutRetiring {
		stage = a.CutStage
	}
	controlReceipt := o.Current.Control
	if a.Control.Kind != "" {
		controlReceipt = a.Control
	}
	if err = e.apply(&record, upgradetx.Event{Receipt: controlReceipt, Phase: phase, CutStage: stage, Outcome: upgradetx.OutcomePending}); err != nil {
		return e.failed(record, err)
	}
	if err = e.result(&record, control, digest(o.Current.Control)); err != nil {
		return e.failed(record, err)
	}
	if err = migrateBrokerResources(e, &record, stamp, o, stage); err != nil {
		return e.failed(record, err)
	}
	// origin: hot-update S8 plan §4.2 / §8.3 M02, M15
	// EffectiveSHA describes disk selection only. Independently re-observe the
	// running process and the registry after retirement, at one admission seq.
	stamp = makeStamp(handle(record), o.Census.Stamp.Producer, record.Grant.ScopeDigest, nonce())
	final, err := e.runtime.Observe(stamp)
	if err != nil || !currentProof(final, stamp, active(record)) || len(final.Census.OldOwners) != 0 || final.Census.Stamp.Producer != stamp.Producer {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "final broker running proof is unavailable"))
	}
	err = upgradeobserve.Commit(e.registry, final.Census.Stamp, func(census upgradetx.Census) error {
		// origin: S8 internal review rv3-leaves F4 F5
		if census.Stamp.At < stamp.At || upgradetx.ValidateCompletionBarrier(active(record), final, census) != nil {
			return refusal(upgradetx.CodeUnsettled, "admission or custody changed before completion")
		}
		for _, action := range record.Actions {
			if sameAttempt(action.Handle, active(record).Handle) && action.State != upgradetx.ActionDurable {
				return refusal(upgradetx.CodeUnsettled, "an action still lacks durable effect evidence")
			}
		}
		return e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
	})
	if err != nil {
		return e.failed(record, err)
	}
	return upgradetx.View{Historical: record, Observation: final}, nil
}

// resumeBrokerActivation settles only this cut's durable intent from fresh
// running-image evidence; it never dispatches the target a second time.
// origin: hot-update S8 plan §3.2 / §4.2; S10 LINT-L4 responsibility split
func resumeBrokerActivation(ctx context.Context, e *engine, record *upgradetx.Record, activatedOwner upgradetx.Owner) error {
	owner := activatedOwner
	if !knownOwner(owner) {
		owner = record.Grant.Issuer
	}
	if err := upgradeobserve.Advance(e.registry, active(*record), owner); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A target that crossed exec settles the activation and dispatch intents
	// from a fresh running-image proof of exactly this cut first (as
	// agentupgrade.Boot does): only that durable result moves the journal's
	// effective selector to the target, which the control proof below
	// requires. The proof is the current-cut proof minus that selector.
	// origin: hot-update S8 plan §3.2 / §4.2; S8 BROKER-EXPORTER R-11
	if pendingAction(*record, upgradetx.ActionActivate) || pendingAction(*record, upgradetx.ActionDispatch) {
		stamp := makeStamp(handle(*record), record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
		o, err := e.runtime.Observe(stamp)
		if err != nil || !runningProof(o, stamp, active(*record)) || (knownOwner(activatedOwner) && o.Census.Stamp.Producer != activatedOwner) {
			return refusal(upgradetx.CodeUnsettled, "fresh running-image and current-cut proof is required")
		}
		if err = e.settle(record, upgradetx.ActionActivate, digest(o.Current)); err != nil {
			return err
		}
		if err = e.settle(record, upgradetx.ActionDispatch, digest(o.Current.Scope)); err != nil {
			return err
		}
	}
	// origin: S8 internal review roll-recovery-stall F1
	// A lost activation result leaves the source selected even when the target
	// owns this exact cut. Settle that result from the independent running proof
	// above before recovery requires the durable target selector. Recovery still
	// obtains its own fresh control/custody proof under the admission barrier.
	// origin: S8 internal review rv3-leaves F9
	if active(*record).Phase == upgradetx.PhaseRecoveryRequired {
		stamp := makeStamp(handle(*record), owner, record.Grant.ScopeDigest, nonce())
		if err := upgradeobserve.Recover(e.registry, *record, stamp, e.runtime.Observe, func(event upgradetx.Event) error {
			return e.apply(record, event)
		}); err != nil {
			return err
		}
	}
	return nil
}

// migrateBrokerResources preserves each action's original receipt while proving
// the current registry seal and old-owner retirement in journal order.
// origin: hot-update S8 plan §4.2 / §8.1; S10 LINT-L4 responsibility split
func migrateBrokerResources(e *engine, record *upgradetx.Record, stamp upgradetx.Stamp, o upgradetx.Observation, stage upgradetx.CutStage) error {
	migration, err := e.intent(record, upgradetx.ActionMigrate, upgradetx.PhaseMigrating, stage)
	if err != nil {
		return err
	}
	a := active(*record)
	resources, err := e.registry.Seal(a, stamp.Nonce)
	if err != nil {
		return err
	}
	if !validReceipt(resources, upgradetx.ReceiptResourcesMigrated, a, stamp, o.Census.Stamp.Producer) ||
		resources.Stamp.Sequence < o.Census.Stamp.Sequence || resources.Stamp.Nonce != stamp.Nonce {
		return refusal(upgradetx.CodeUnsettled, "resource seal is incomplete or predates admission")
	}
	resourcesReceipt := resources
	if a.Resources.Kind != "" {
		resourcesReceipt = a.Resources
	}
	if err = e.apply(record, upgradetx.Event{Receipt: resourcesReceipt}); err != nil {
		return err
	}
	if err = e.result(record, migration, digest(resources)); err != nil {
		return err
	}
	retirement, err := e.intent(record, upgradetx.ActionRetire, upgradetx.PhaseMigrating, upgradetx.CutRetiring)
	if err != nil {
		return err
	}
	released := active(*record).Released
	if released.Kind == "" {
		released, err = e.runtime.Retire(handle(*record))
	}
	if err != nil {
		return err
	}
	releaseStamp := stamp
	if active(*record).Released.Kind != "" {
		releaseStamp.At = active(*record).Released.Stamp.At
	}
	if !validReceipt(released, upgradetx.ReceiptOldGenerationReleased, active(*record), releaseStamp, o.Census.Stamp.Producer) || len(released.Census.OldOwners) != 0 {
		return refusal(upgradetx.CodeUnsettled, "old broker owners or custody are not released")
	}
	if err = e.apply(record, upgradetx.Event{Receipt: released}); err != nil {
		return err
	}
	if err = e.result(record, retirement, digest(released)); err != nil {
		return err
	}
	return nil
}

func validCut(c upgradeport.Cut, r upgradetx.Record) bool {
	a := active(r)
	if c.Attempt != handle(r) || !knownOwner(c.Owner) || c.Owner != r.Grant.Issuer ||
		len(c.Descriptor) == 0 || upgradetx.ValidateExportScope(a.Scope, c.Scope) != nil {
		return false
	}
	seen := make(map[uint32]bool)
	for _, fd := range c.Auxiliary {
		if fd.File == nil || fd.Kind == "" || !knownOwner(fd.Owner) || seen[fd.Index] {
			return false
		}
		seen[fd.Index] = true
	}
	return true
}

func currentProof(o upgradetx.Observation, s upgradetx.Stamp, a upgradetx.Attempt) bool {
	// RunningSHA must never be replaced with EffectiveSHA in this predicate.
	return runningProof(o, s, a) && o.EffectiveSHA == a.TargetSHA
}

// runningProof is the current-cut proof without the durable selector: the
// target bytes run this exact cut, actively, over its sealed scope.
func runningProof(o upgradetx.Observation, s upgradetx.Stamp, a upgradetx.Attempt) bool {
	return fresh(o, s) && sameAttempt(o.Current.Handle, a.Handle) && o.Current.Direction == a.Direction &&
		o.Current.SourceSHA == a.SourceSHA && o.Current.TargetSHA == a.TargetSHA && o.RunningSHA == a.TargetSHA &&
		(o.Current.CutStage == upgradetx.CutActive || o.Current.CutStage == upgradetx.CutRetiring || o.Current.CutStage == upgradetx.CutReleased) &&
		reflect.DeepEqual(o.Current.Scope, a.Scope) && covers(a.Scope, o.Census)
}

// pendingAction reports an intent of this attempt whose effect is not durable.
func pendingAction(r upgradetx.Record, kind upgradetx.ActionKind) bool {
	for _, action := range r.Actions {
		if sameAttempt(action.Handle, active(r).Handle) && action.Kind == kind && action.State != upgradetx.ActionDurable {
			return true
		}
	}
	return false
}

func validReceipt(r upgradetx.Receipt, kind upgradetx.ReceiptKind, a upgradetx.Attempt, s upgradetx.Stamp, owner upgradetx.Owner) bool {
	return r.Kind == kind && r.Known && r.Complete && r.EvidenceDigest != "" && r.RunningSHA == a.TargetSHA &&
		sameAttempt(r.Stamp.Attempt, a.Handle) && r.Stamp.Producer == owner && r.Stamp.Nonce != "" &&
		r.Stamp.At >= s.At && r.Stamp.ScopeDigest == s.ScopeDigest && r.Census.Known && r.Census.Complete &&
		r.Census.Admitting == 0 && r.Census.ExpectedDigest != "" && r.Census.Stamp == r.Stamp && covers(a.Scope, r.Census)
}

func covers(scope upgradetx.Scope, census upgradetx.Census) bool {
	return scope.ID != "" && len(scope.Domains) > 0 && len(scope.Participants) > 0 && upgradetx.CensusCovers(scope, census)
}

func (e *engine) apply(r *upgradetx.Record, event upgradetx.Event) error {
	next, err := e.store.Apply(handle(*r), event)
	if err == nil {
		*r = next
	}
	return err
}

func (e *engine) intent(r *upgradetx.Record, kind upgradetx.ActionKind, phase upgradetx.Phase, stage upgradetx.CutStage) (upgradetx.Action, error) {
	// Readback operations resume their original action ID. Otherwise every
	// failed observation would leave a permanent orphan intent at completion.
	if kind == upgradetx.ActionControl || kind == upgradetx.ActionMigrate || kind == upgradetx.ActionRetire {
		for _, action := range r.Actions {
			if sameAttempt(action.Handle, active(*r).Handle) && action.Kind == kind {
				return action, nil
			}
		}
	}
	action := upgradetx.Action{ID: nonce(), Handle: handle(*r), Kind: kind, IntentDigest: digest(active(*r)), State: upgradetx.ActionIntent, Nonce: nonce()}
	// An action-only write preserves the joint state, particularly unknown
	// recovery states. Merely probing must not turn unknown into pending.
	event := upgradetx.Event{Action: action, Phase: phase, CutStage: stage}
	if phase != "" {
		event.Outcome = upgradetx.OutcomePending
	}
	err := e.apply(r, event)
	return action, err
}

func (e *engine) result(r *upgradetx.Record, action upgradetx.Action, evidence string) error {
	if action.State == upgradetx.ActionDurable {
		return nil
	}
	// A reopened observed result already names its original evidence. Fresh
	// external readback above confirms the effect; do not rewrite that digest
	// merely because the observation's time/nonce advanced after the crash.
	if action.State == upgradetx.ActionObserved && action.ResultDigest != "" {
		action.State = upgradetx.ActionDurable
		return e.apply(r, upgradetx.Event{Action: action})
	}
	action.State, action.ResultDigest = upgradetx.ActionObserved, evidence
	if err := e.apply(r, upgradetx.Event{Action: action}); err != nil {
		return err
	}
	action.State = upgradetx.ActionDurable
	return e.apply(r, upgradetx.Event{Action: action})
}

func (e *engine) settle(r *upgradetx.Record, kind upgradetx.ActionKind, evidence string) error {
	for _, action := range r.Actions {
		if sameAttempt(action.Handle, active(*r).Handle) && action.Kind == kind && action.State != upgradetx.ActionDurable {
			if err := e.result(r, action, evidence); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *engine) failed(record upgradetx.Record, cause error, boundary ...string) (upgradetx.View, error) {
	// Failure to journal the uncertainty cannot erase acceptance. In particular,
	// no failed exec/partial Take path reports rolled_back before real recovery.
	// origin: S8 internal review roll-recovery-stall F2
	// Retain a bounded public refusal in the private journal; raw adapter errors
	// belong to local diagnostics. Otherwise a timeout leaves no way to tell a
	// failed final export from an exec that returned after Start already replied.
	step := string(active(record).Phase)
	if len(boundary) != 0 {
		step = boundary[0]
	}
	reason := "broker upgrade requires current-state recovery: " + step
	var typed *upgradetx.Error
	if errors.As(cause, &typed) && len(typed.Code)+len(typed.Detail) <= 1024 {
		reason += ": " + typed.Code + ": " + typed.Detail
	}
	slog.Error("broker: accepted upgrade requires current-state recovery", "boundary", step, "cut_stage", active(record).CutStage, "error", cause)
	err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseRecoveryRequired, CutStage: active(record).CutStage, Outcome: upgradetx.OutcomeUnknown, Reason: reason})
	if err != nil {
		cause = refusal(upgradetx.CodeUnsettled, "cannot persist broker recovery state")
	} else if activationUnstarted(cause) {
		// origin: S8 internal review roll-recovery-stall F5
		// This adapter proved it never queued a receiver. R0 still needs actual
		// thaw and current source custody, rather than treating its error as a
		// rollback receipt or leaving an impossible target recovery pending.
		if recoveryErr := settleUnstartedBroker(e, &record); recoveryErr != nil {
			slog.Error("broker: unqueued activation still lacks source recovery proof", "error", recoveryErr)
		}
	}
	return upgradetx.View{Historical: record, Code: upgradetx.CodeUnsettled, Detail: "broker upgrade remains accepted"}, accepted(cause)
}

func settleUnstartedBroker(e *engine, record *upgradetx.Record) error {
	a := active(*record)
	if a.CutStage != upgradetx.CutPartialActive || a.Control.Kind != "" || a.Resources.Kind != "" || a.Released.Kind != "" {
		return refusal(upgradetx.CodeUnsettled, "unqueued activation already has target effects")
	}
	thaw, err := e.intent(record, upgradetx.ActionFreeze, "", "")
	if err != nil {
		return err
	}
	if err = e.runtime.Thaw(handle(*record)); err != nil {
		return err
	}
	observe := func() (upgradetx.CancellationProof, error) {
		stamp := makeStamp(handle(*record), record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
		o, err := e.runtime.Observe(stamp)
		if err != nil || !fresh(o, stamp) {
			return upgradetx.CancellationProof{}, refusal(upgradetx.CodeUnsettled, "unqueued activation lacks current source custody")
		}
		// The admission owner's causal no-queue receipt refines the conservative
		// journal intent. It authorizes no target effect and cannot substitute
		// for the independent running SHA, source generation and custody below.
		o.Current.CutStage = upgradetx.CutPristine
		proof := upgradetx.CancellationProof{Thawed: true, Observation: o}
		return proof, upgradetx.ValidateCancellation(active(*record), record.Grant.Issuer, proof)
	}
	proof, err := observe()
	if err != nil {
		return err
	}
	if err = e.result(record, thaw, digest(proof.Observation.Census)); err != nil {
		return err
	}
	proof, err = observe()
	if err != nil {
		return err
	}
	return e.apply(record, upgradetx.Event{Phase: upgradetx.PhaseCancelled, CutStage: upgradetx.CutPristine, Outcome: upgradetx.OutcomeCancelled, Cancellation: &proof,
		Reason: "broker activation cancelled before queue; source thaw and custody verified; " + record.Reason})
}
