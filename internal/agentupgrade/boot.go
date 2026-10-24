package agentupgrade

import (
	"context"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Boot attaches to an accepted cut after image import. It never reissues Start,
// replays a dispatch intent, or promotes a v1 marker/register into v2 completion.
// The caller supplies the handle carried by the validated sealed adjunct.
// origin: hot-update S8 plan §3.2 / §3.3 / §4.2
func Boot(ctx context.Context, endpoint upgradeport.Endpoint, expected upgradetx.Handle) (upgradetx.View, error) {
	e, ok := endpoint.(*engine)
	if !ok {
		return upgradetx.View{}, refusal(upgradetx.CodeNotWired, "agent transaction engine is unavailable", false)
	}
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	view, err := e.store.Load(expected.Key, expected.TxID)
	if err != nil {
		return view, err
	}
	a := current(view.Historical)
	if !sameAttempt(a.Handle, expected) || expected.Revision == 0 || expected.Revision > view.Historical.Revision {
		return view, refusal(upgradetx.CodeRejected, "boot handle no longer owns the current revision", true)
	}
	// The sealed cut precedes durable dispatch/activation intent writes. Its
	// revision is therefore a lower bound, not the next write CAS. Only the
	// same immutable cut plus fresh running evidence can refresh that CAS.
	if a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseCancelled || a.Phase == upgradetx.PhaseAbandoned {
		// Cold boot changes observation, never an archived execution fact.
		return e.query(expected.Key, expected.TxID, newID())
	}
	o, err := e.observe(a.Handle, upgradetx.Owner{}, view.Historical.Grant.ScopeDigest, newID())
	if err != nil {
		return view, err
	}
	if !o.OwnerKnown || !sameAttempt(o.Current.Handle, a.Handle) || o.RunningSHA != a.TargetSHA || !validOwner(o.Census.Stamp.Producer) {
		return e.markUnknown(view.Historical, "boot cannot prove the current running cut")
	}
	if a.CutStage != upgradetx.CutPartialActive && a.CutStage != upgradetx.CutActive && a.CutStage != upgradetx.CutRetiring {
		return e.markUnknown(view.Historical, "boot has no accepted activation intent")
	}
	record := view.Historical
	// origin: S8 internal review rv3-leaves F9
	if a.Phase == upgradetx.PhaseRecoveryRequired {
		stamp := upgradetx.Stamp{Attempt: a.Handle, Producer: o.Census.Stamp.Producer, Nonce: newID(), At: time.Now().UnixNano(), ScopeDigest: record.Grant.ScopeDigest}
		if err = upgradeobserve.Recover(e.registry, record, stamp, e.runtime.Observe, func(event upgradetx.Event) error {
			return e.apply(&record, event)
		}); err != nil {
			return e.markUnknown(record, "boot recovery proof is unavailable")
		}
	}
	for _, action := range record.Actions {
		if sameAttempt(action.Handle, a.Handle) && action.State != upgradetx.ActionDurable && (action.Kind == upgradetx.ActionActivate || action.Kind == upgradetx.ActionDispatch || action.Kind == upgradetx.ActionSelect) {
			if err = e.result(&record, action, digest(o.Current), upgradetx.Event{}); err != nil {
				return e.markUnknown(record, "boot action evidence could not be persisted")
			}
		}
	}
	record, err = e.finish(record, o.Census.Stamp.Producer)
	if err != nil {
		return e.markUnknown(record, "boot completion evidence is unavailable")
	}
	return e.query(expected.Key, expected.TxID, newID())
}

// Watchdog is a joined caller-owned wait, not a detached timer callback. Its
// immutable fence is captured when the attempt is armed. Cancellation releases
// the timer; expiry can affect only this cut, never whichever Tx is Current later.
// origin: hot-update S8 plan §3.3 / §8.1 / M11
func Watchdog(ctx context.Context, endpoint upgradeport.Endpoint, expected upgradetx.Fence, deadline int64, request upgradetx.ReverseRequest) (upgradetx.View, error) {
	if ctx == nil || deadline <= 0 {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "watchdog requires a context and absolute deadline", false)
	}
	timer := time.NewTimer(time.Until(time.Unix(0, deadline)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return upgradetx.View{}, ctx.Err()
	case <-timer.C:
	}
	e, ok := endpoint.(*engine)
	if !ok {
		return upgradetx.View{}, refusal(upgradetx.CodeNotWired, "agent transaction engine is unavailable", false)
	}
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	view, err := e.query(expected.Current.Key, expected.Current.TxID, newID())
	if err != nil {
		return view, err
	}
	o := view.Observation
	// origin: hot-update S8 plan §3.3 / M11. Loading A's archive may be
	// successful while B is running. Neither that read nor a reused PID
	// authorizes this callback to substitute B's handle or previous image.
	if !sameAttempt(current(view.Historical).Handle, expected.Current) || !sameAttempt(o.Current.Handle, expected.Current) || !o.OwnerKnown || o.Census.Stamp.Producer != expected.Owner || o.RunningSHA != expected.RunningSHA || view.Historical.RecordedPreviousSHA != expected.PreviousSHA {
		return view, refusal(upgradetx.CodeUnsettled, "watchdog no longer owns the running attempt", true)
	}
	a := current(view.Historical)
	if a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseCancelled {
		return view, nil
	}
	// Revisions may advance inside the captured attempt. Only after all
	// immutable fence fields match may this callback refresh the write CAS.
	expected.Current.Revision = view.Historical.Revision
	if a.CutStage == upgradetx.CutNone || a.CutStage == upgradetx.CutPristine {
		return e.cancel(expected.Current)
	}
	return e.rollback(ctx, expected, request)
}

func (e *engine) intent(record *upgradetx.Record, kind upgradetx.ActionKind, phase upgradetx.Phase, stage upgradetx.CutStage) (upgradetx.Action, error) {
	a := current(*record)
	for _, old := range record.Actions {
		if sameAttempt(old.Handle, a.Handle) && old.Kind == kind {
			// Reverse salvage retires G before activation; final retirement
			// retires G+1 after target control. These are independent effects,
			// while retries on either side retain their original action ID.
			// origin: S8 internal review rv-txn F04 F05
			if kind == upgradetx.ActionRetire && a.Control.Kind != "" && old.Handle.Revision <= a.Control.Stamp.Attempt.Revision {
				continue
			}
			return old, nil
		}
	}
	action := upgradetx.Action{ID: newID(), Handle: a.Handle, Kind: kind, IntentDigest: digest(a), State: upgradetx.ActionIntent, Nonce: newID()}
	event := upgradetx.Event{Action: action, Phase: phase, CutStage: stage}
	if phase != "" {
		event.Outcome = upgradetx.OutcomePending
	}
	return action, e.apply(record, event)
}

func (e *engine) result(record *upgradetx.Record, action upgradetx.Action, evidence string, event upgradetx.Event) error {
	if action.State == upgradetx.ActionDurable {
		if reflect.DeepEqual(event, upgradetx.Event{}) {
			return nil
		}
		return e.apply(record, event)
	}
	if action.State == upgradetx.ActionObserved && action.ResultDigest != "" {
		action.State = upgradetx.ActionDurable
		event.Action = action
		return e.apply(record, event)
	}
	action.ResultDigest, action.State = evidence, upgradetx.ActionObserved
	if err := e.apply(record, upgradetx.Event{Action: action}); err != nil {
		return err
	}
	action.State = upgradetx.ActionDurable
	event.Action = action
	return e.apply(record, event)
}

func (e *engine) finish(record upgradetx.Record, owner upgradetx.Owner) (upgradetx.Record, error) {
	if err := upgradeobserve.Advance(e.registry, current(record), owner); err != nil {
		return record, err
	}
	control, err := e.intent(&record, upgradetx.ActionControl, "", "")
	if err != nil {
		return record, err
	}
	a := current(record)
	o, err := e.observe(a.Handle, owner, record.Grant.ScopeDigest, control.Nonce)
	if err != nil {
		return record, err
	}
	if !o.OwnerKnown || o.RunningSHA != a.TargetSHA || o.EffectiveSHA != a.TargetSHA || !sameAttempt(o.Current.Handle, a.Handle) || o.Current.Direction != a.Direction || !validReceipt(o.Current.Control, upgradetx.ReceiptControlReady, a.Handle, owner, a.TargetSHA, control.Nonce, record.Grant.ScopeDigest) {
		return record, refusal(upgradetx.CodeUnsettled, "control readiness is unproven", true)
	}
	controlEvent := upgradetx.Event{Receipt: o.Current.Control, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive, Outcome: upgradetx.OutcomePending}
	if a.Control.Kind != "" {
		controlEvent = upgradetx.Event{}
	}
	if err = e.result(&record, control, digest(o.Current.Control), controlEvent); err != nil {
		return record, err
	}
	stage := current(record).CutStage
	migrate, err := e.intent(&record, upgradetx.ActionMigrate, upgradetx.PhaseMigrating, stage)
	if err != nil {
		return record, err
	}
	a = current(record)
	resources, err := e.registry.Seal(a, migrate.Nonce)
	if err != nil {
		return record, err
	}
	if !validReceipt(resources, upgradetx.ReceiptResourcesMigrated, a.Handle, owner, a.TargetSHA, migrate.Nonce, record.Grant.ScopeDigest) || !coversScope(resources.Census, a.Scope) || !targetCustody(resources.Census, a.Handle, owner) {
		return record, refusal(upgradetx.CodeUnsettled, "resource migration is unproven", true)
	}
	migrationEvent := upgradetx.Event{}
	// A recovered action may already be durable while its phase projection is
	// still serving_new. Advance that phase without replacing old evidence.
	// origin: S8 internal review rv-txn F04
	if a.Phase == upgradetx.PhaseServingNew {
		migrationEvent.Phase = upgradetx.PhaseMigrating
	}
	if a.Resources.Kind == "" {
		migrationEvent.Receipt = resources
	}
	if err = e.result(&record, migrate, digest(resources), migrationEvent); err != nil {
		return record, err
	}
	retire, err := e.intent(&record, upgradetx.ActionRetire, "", "")
	if err != nil {
		return record, err
	}
	a = current(record)
	released := a.Released
	if released.Kind == "" {
		released, err = e.runtime.Retire(a.Handle)
	}
	if err != nil {
		return record, err
	}
	// Retire has no nonce argument. The adapter returns independently sampled
	// evidence for this cut; its nonce must still be nonempty and internally
	// consistent. It cannot copy a forward receipt into a reverse attempt.
	if !validReceipt(released, upgradetx.ReceiptOldGenerationReleased, a.Handle, owner, a.TargetSHA, "", record.Grant.ScopeDigest) || len(released.Census.OldOwners) != 0 {
		return record, refusal(upgradetx.CodeUnsettled, "old generation release is unproven", true)
	}
	if err = e.result(&record, retire, digest(released), upgradetx.Event{Receipt: released, CutStage: upgradetx.CutRetiring}); err != nil {
		return record, err
	}
	a = current(record)
	o, err = e.observe(a.Handle, owner, record.Grant.ScopeDigest, newID())
	if err != nil {
		return record, err
	}
	// Store.Apply performs P01's joint transition and the durable completion
	// CAS. This engine does not publish success before that write succeeds.
	// The admission owner holds its fence until the durable CAS returns.
	err = upgradeobserve.Commit(e.registry, o.Census.Stamp, func(snapshot upgradetx.Census) error {
		if !completionProof(a, o, snapshot, record.Grant.ScopeDigest) {
			return refusal(upgradetx.CodeUnsettled, "completion admission barrier is unproven", true)
		}
		return e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
	})
	if err != nil {
		return record, err
	}
	return record, nil
}

func checkPreflight(proof upgradeport.PreflightProof, grant upgradetx.Grant, scope *upgradetx.Scope, singleGeneration bool) error {
	// origin: hot-update S8 plan §6.1 / §6.3. A compatible reader digest is
	// necessary alongside config and custody evidence at every gate. Before
	// reverse activation, retained forward ownership must already be gone.
	c := proof.Census
	if !reflect.DeepEqual(proof.Grant, grant) || !hexValue(proof.ReadersDigest, 64) || !hexValue(proof.ConfigDigest, 64) || !validCensus(c, grant.Handle, grant.Issuer, grant.ScopeDigest) {
		return refusal(upgradetx.CodeRejected, "complete preflight proof is required", false)
	}
	if scope != nil && !coversScope(c, *scope) {
		return refusal(upgradetx.CodeRejected, "preflight omits a required domain or resource", false)
	}
	generations := make(map[uint64]bool)
	for _, resource := range c.Resources {
		if upgradetx.ResourceNotApplicable(resource) || upgradetx.ResourceReleased(resource) {
			continue // Durable history/artifact pins do not count as live owners.
		}
		if resource.Generation == 0 || !validOwner(resource.Owner) {
			return refusal(upgradetx.CodeRejected, "live resource ownership is unknown", false)
		}
		if resource.Generation != 0 {
			generations[resource.Generation] = true
		}
	}
	limit := upgradebudget.MaxGenerations
	if singleGeneration {
		limit = 1
	}
	if len(generations) > limit || singleGeneration && len(c.OldOwners) != 0 {
		return refusal(upgradetx.CodeRejected, "live generation limit is exceeded", false)
	}
	return nil
}

func validCensus(c upgradetx.Census, handle upgradetx.Handle, owner upgradetx.Owner, scope string) bool {
	// origin: hot-update S8 plan §4.2. Counts alone do not bind a snapshot to
	// a cut or producer, and a live admission can invalidate an empty census.
	return c.Known && c.Complete && c.Admitting == 0 && sameAttempt(c.Stamp.Attempt, handle) && c.Stamp.Producer == owner && validOwner(owner) && hexValue(c.Stamp.Nonce, 32) && c.Stamp.Sequence != 0 && c.Stamp.At > 0 && c.Stamp.ScopeDigest == scope && hexValue(c.ExpectedDigest, 64)
}

func validReceipt(r upgradetx.Receipt, kind upgradetx.ReceiptKind, handle upgradetx.Handle, owner upgradetx.Owner, sha, nonce, scope string) bool {
	// origin: hot-update S8 plan §4.2 / §8.1. The receipt and nested census
	// must be the same observation, not individually valid evidence borrowed
	// from two cuts. Revision advances do not change their immutable identity.
	return r.Kind == kind && r.Known && r.Complete && r.RunningSHA == sha && r.Stamp == r.Census.Stamp && (nonce == "" || r.Stamp.Nonce == nonce) && hexValue(r.EvidenceDigest, 64) && validCensus(r.Census, handle, owner, scope)
}

// A known empty set is useful only with explicit domain witnesses. Runtime
// adapters represent an inapplicable domain with a not_applicable resource;
// absent collection is not the same as a zero census.
// origin: hot-update S8 plan §4.2 / M06
func coversScope(c upgradetx.Census, scope upgradetx.Scope) bool {
	// Preserve origin in the index so a large sealed set is checked in one
	// pass rather than rescanned for every expected resource during freeze.
	return len(scope.Domains) > 0 && len(scope.Participants) > 0 && upgradetx.CensusCovers(scope, c)
}

func completionProof(a upgradetx.Attempt, o upgradetx.Observation, snapshot upgradetx.Census, scope string) bool {
	// origin: hot-update S8 plan §4.2 / M04. A newer admission sequence
	// invalidates older resource/release seals even when scope is unchanged;
	// Control may precede those admissions and must retain its original proof.
	// P03's Seal fence must cover this final snapshot through the store CAS;
	// a second read alone cannot close a race with an unfenced business Begin.
	// The gated snapshot may show a live transfer settled after the running
	// sample; that monotone progress is all it may differ by.
	// origin: S8 BROKER-EXPORTER F1
	owner := o.Census.Stamp.Producer
	if !o.OwnerKnown || o.RunningSHA != a.TargetSHA || o.EffectiveSHA != a.TargetSHA || !sameAttempt(o.Current.Handle, a.Handle) || o.Current.Direction != a.Direction || !validCensus(o.Census, a.Handle, owner, scope) || !validCensus(snapshot, a.Handle, owner, scope) || !upgradetx.CensusProgressed(o.Census, snapshot) || !coversScope(snapshot, a.Scope) || !targetCustody(snapshot, a.Handle, owner) || len(snapshot.OldOwners) != 0 {
		return false
	}
	// origin: S8 internal review rv3-leaves F4 F5
	return upgradetx.ValidateCompletionBarrier(a, o, snapshot) == nil
}

func targetCustody(c upgradetx.Census, handle upgradetx.Handle, owner upgradetx.Owner) bool {
	// origin: hot-update S8 plan §4.2. Historical terminal evidence can keep
	// its original generation; any remaining executable responsibility must
	// belong to this attempt's target, not merely have the right resource ID.
	for _, resource := range c.Resources {
		if upgradetx.ResourceNotApplicable(resource) || upgradetx.ResourceReleased(resource) {
			continue
		}
		if resource.Owner != owner || resource.Generation != handle.Generation || resource.Custody == "" {
			return false
		}
	}
	return true
}
