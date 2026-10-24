package upgraderoll

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Run executes only the supplied, durably sealed parent. Callers prepare all
// grants and stage artifacts first; this function never re-plans or re-prepares.
// Its contexts supervise the driver/keeper, not already accepted child work.
// Cancellation leaves reservations and pending evidence with their daemon owner.
// origin: hot-update S8 plan §2.2 / §3.1 / §5.1 / P11
func Run(ctx context.Context, store upgradetx.Store, endpoint upgradeport.Endpoint, plan upgradetx.Plan) (view upgradetx.View, err error) {
	// Validate the complete dependency graph before asking a daemon to reserve
	// anything. A partially collected plan has no authority to dispatch its
	// otherwise valid first child, because a restart could omit the rest.
	if err = validatePlan(plan); err != nil {
		return view, err
	}
	// Keep our own immutable slices: a caller's or adapter's later roster edits
	// cannot change the plan after its durable digest has authorized an effect.
	plan = clonePlan(plan)
	journal, ok := store.(PlanStore)
	// The frozen Store has no full-plan persistence operation. Requiring this
	// typed extension prevents an integration from silently keeping the plan
	// only in the CLI's memory or writing a competing journal beside the daemon.
	if !ok {
		return view, refusal(upgradetx.CodeNotWired, "parent journal adapter is not implemented (S8 P02)", false)
	}
	port, ok := endpoint.(Endpoint)
	if !ok {
		return view, refusal(upgradetx.CodeNotWired, "roll reservation adapter is not implemented (S8 P15/P19)", false)
	}
	if ctx == nil {
		return view, refusal(upgradetx.CodeRejected, "driver context is required", false)
	}
	// Already accepted children outlive a cancelled ctl. The driver itself must
	// inherit the ctl context, so a stopped operator cannot keep sending steps
	// or renewing authority while the daemon settles those existing children.
	if err = ctx.Err(); err != nil {
		return view, err
	}
	if view, err = sealParentPlan(ctx, journal, port, plan); err != nil {
		return view, err
	}
	lease, reserveErr := port.Reserve(ctx, clonePlan(plan))
	if reserveErr != nil {
		return view, acceptedError(reserveErr)
	}
	if lease == nil {
		return view, refusal(upgradetx.CodeUnsettled, "reservation owner is unknown", true)
	}
	driveCtx, cancelDrive := context.WithCancelCause(ctx)
	// Reservation ownership is durable and belongs to the parent/daemon. This
	// goroutine is only a courier for renewals; stopping it must not release a
	// borrowed host lock or erase pending authorization after a lost reply.
	stopKeeper := keepReservation(driveCtx, cancelDrive, lease, plan.Parent)
	defer func() {
		stopKeeper()
		cancelDrive(nil)
		if stopErr := lease.Stop(); stopErr != nil && err == nil {
			err = refusal(upgradetx.CodeUnsettled, "driver reservation cleanup failed", true)
		}
	}()
	if view, err = drivePlanSteps(driveCtx, journal, port, lease, plan, view, stopKeeper); err != nil {
		return view, err
	}
	return completeParentPlan(driveCtx, journal, port, lease, plan, view)
}

// Sealing precedes every reservation effect, including on a resumed parent.
// origin: hot-update S8 plan §3.1 / LINT-s8
func sealParentPlan(ctx context.Context, journal PlanStore, port Endpoint, plan upgradetx.Plan) (view upgradetx.View, err error) {
	if view, err = loadParent(journal, plan); err != nil {
		return view, err
	}
	if time.Until(time.Unix(0, plan.Deadline)) > upgradebudget.FullStackRollout {
		return view, refusal(upgradetx.CodeRejected, "parent deadline exceeds the absolute execution window", true)
	}
	// SealPlan returns only after the sole writer's durability chain. Reservation
	// RPCs are effects too: putting the seal here makes the snapshot available
	// even when the process dies immediately after the first reservation reply.
	sealed, sealErr := journal.SealPlan(clonePlan(plan))
	if sealErr != nil {
		return view, sealErr
	}
	// A different caller plan must never overwrite the accepted snapshot, even
	// when it merely omits an offline agent or regenerates an equivalent grant.
	// origin: hot-update S8 plan §3.1 / §8.3 M09
	if !reflect.DeepEqual(sealed, plan) {
		return view, refusal(upgradetx.CodeUnsettled, "resume differs from the original sealed plan", true)
	}
	if time.Now().UnixNano() >= plan.Deadline && view.Historical.Forward.Phase != upgradetx.PhaseCompleted {
		// Expiry removes unexecuted authority, not the obligation to query every
		// original accepted child. Stop without renewing or minting another plan;
		// explicit replanning must settle the old rights at the daemon boundary.
		queryExpired(ctx, port, plan)
		return view, refusal(upgradetx.CodeUnsettled, "parent deadline expired; query accepted children and explicitly replan", true)
	}
	return view, nil
}

// Each original step reloads the captured parent and renews its fleet proof.
// The keeper join remains part of the final barrier before holder release.
// origin: hot-update S8 plan §5.1 / LINT-s8
func drivePlanSteps(ctx context.Context, journal PlanStore, port Endpoint, lease Reservation, plan upgradetx.Plan, view upgradetx.View, stopKeeper func()) (upgradetx.View, error) {
	var err error
	if err = observeFleet(ctx, lease, plan, !finalReleaseStarted(view.Historical, plan)); err != nil {
		return view, err
	}
	if err = inspectPlanScopes(ctx, port, plan); err != nil {
		return view, err
	}
	for i, step := range plan.Steps {
		// Reload by the captured parent ID, never by Current. A concurrent next
		// transaction or a late writer must be visible before another side effect.
		// The step identities and dependencies remain the original sealed values.
		if cause := context.Cause(ctx); cause != nil {
			return view, acceptedError(cause)
		}
		if view, err = loadParent(journal, plan); err != nil {
			return view, err
		}
		last := i == len(plan.Steps)-1
		// Fleet facts can change between steps, including agent registrations and
		// retained helpers. A reservation's initial census is not sufficient to
		// prove that this next child would still leave at most two releases.
		if err = observeFleet(ctx, lease, plan, !finalReleaseStarted(view.Historical, plan)); err != nil {
			return view, err
		}
		if isCut(step) {
			if err = driveChild(ctx, journal, port, lease, plan, step); err != nil {
				return view, err
			}
			continue
		}
		host := step.Participant.DomainID
		if last {
			// Join before releasing the cluster holder, rather than cancelling the
			// keeper afterwards. Otherwise an in-flight renew can race the release
			// CAS or report holder loss after a genuinely successful release.
			host = ""
		}
		// Host release waits for its siblings/home, while a broker child itself
		// can complete before the agent is dispatched. Cluster release waits for
		// every host and a fresh full resource/domain union.
		// origin: hot-update S8 plan §5.1 / §11.2 P11
		if err = proveChildren(ctx, port, plan, host); err != nil {
			return view, err
		}
		if last {
			stopKeeper()
		} // No renewal may race the final holder CAS.
		if err = driveBarrier(ctx, journal, port, lease, plan, step); err != nil {
			return view, err
		}
	}
	return view, nil
}

// Parent completion requires fresh aggregate proof and durable step readback.
// origin: hot-update S8 plan §3.3 / §5.1 / LINT-s8
func completeParentPlan(ctx context.Context, journal PlanStore, port Endpoint, lease Reservation, plan upgradetx.Plan, view upgradetx.View) (upgradetx.View, error) {
	var err error
	if err = proveChildren(ctx, port, plan, ""); err != nil {
		return view, err
	}
	if err = observeFleet(ctx, lease, plan, false); err != nil {
		return view, err
	}
	if err = observeReleasedHolder(ctx, lease, plan); err != nil {
		return view, err
	}
	// Component receipts are necessary but cannot sign the parent's completion.
	// The parent has its own attempt and its own fresh aggregate observation,
	// including the host/cluster releases and the full managed resource scope.
	nonce, nonceErr := freshNonce()
	if nonceErr != nil {
		return view, nonceErr
	}
	observation, observeErr := port.ObserveParent(ctx, clonePlan(plan), nonce)
	if observeErr != nil {
		return view, acceptedError(observeErr)
	}
	if view, err = loadParent(journal, plan); err != nil {
		return view, err
	}
	if !reflect.DeepEqual(observation.Current.Scope, plan.Scope) {
		return view, refusal(upgradetx.CodeUnsettled, "parent observation omits the sealed scope", true)
	}
	if err = completed(observation.Current, observation, view.Historical.Grant, nonce); err != nil {
		return view, err
	}
	if view.Historical.Forward.Phase == upgradetx.PhaseCompleted {
		// Historical execution stays immutable. Refresh only the returned view;
		// metadata/window changes belong to their separate daemon operations.
		view.Observation = observation
		return view, nil
	}
	for _, step := range plan.Steps {
		if !stepDurable(view.Historical, plan, step) {
			return view, refusal(upgradetx.CodeUnsettled, "parent step has no durable result", true)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return view, acceptedError(cause)
	}
	view, err = journal.CompletePlan(view.Historical.Forward.Handle, observation)
	if err != nil {
		return view, acceptedError(err)
	}
	if view.Historical.Forward.Phase != upgradetx.PhaseCompleted || view.Historical.Forward.Outcome != upgradetx.OutcomeSuccess || view.Historical.Forward.CutStage != upgradetx.CutReleased || !sameAttempt(view.Historical.Forward.Handle, plan.Parent) || !reflect.DeepEqual(view.Historical.Forward.Scope, plan.Scope) || view.Observation.Nonce != nonce {
		return view, refusal(upgradetx.CodeUnsettled, "parent completion was not durably recorded", true)
	}
	for _, step := range plan.Steps {
		if !stepDurable(view.Historical, plan, step) {
			return view, refusal(upgradetx.CodeUnsettled, "completed parent lost a durable step", true)
		}
	}
	return view, nil
}

// Copy every collection crossing an adapter boundary. A shallow copy leaves
// DependsOn and the scope slices aliased, allowing a later callback to change
// which original participant or resource an already persisted digest represents.
func clonePlan(plan upgradetx.Plan) upgradetx.Plan {
	plan.Scope.Participants = append([]upgradetx.Key(nil), plan.Scope.Participants...)
	plan.Scope.Domains = append([]upgradetx.Domain(nil), plan.Scope.Domains...)
	plan.Scope.Expected = append([]upgradetx.Resource(nil), plan.Scope.Expected...)
	plan.Steps = append([]upgradetx.Step(nil), plan.Steps...)
	for i := range plan.Steps {
		plan.Steps[i].DependsOn = append([]uint32(nil), plan.Steps[i].DependsOn...)
	}
	return plan
}

// Once the parent is accepted, a transport or child refusal is a settlement
// problem for this plan. In particular a retryable child defer must not invite
// the caller to start a new parent and dispatch the same child twice.
// origin: hot-update S8 plan §7.1 / P11
func acceptedError(err error) error {
	var transactionErr *upgradetx.Error
	if errors.As(err, &transactionErr) {
		code := transactionErr.Code
		if code == upgradetx.CodeDeferred {
			code = upgradetx.CodeUnsettled
		}
		return &upgradetx.Error{Code: code, Detail: transactionErr.Detail, Accepted: true}
	}
	return refusal(upgradetx.CodeUnsettled, "accepted parent cannot currently be settled", true)
}

// A fresh nonce binds the collection to this barrier rather than a cached
// release-version response. Passing a copy keeps collectors from shrinking the
// parent scope while they enumerate a changing fleet.
func observeFleet(ctx context.Context, lease Reservation, plan upgradetx.Plan, holderRequired bool) error {
	nonce, err := freshNonce()
	if err != nil {
		return err
	}
	proof, err := lease.Check(ctx, clonePlan(plan), nonce)
	if err != nil {
		return acceptedError(err)
	}
	return checkFleet(plan, proof, nonce, holderRequired)
}

// The release RPC's receipt and this holder readback are independent evidence.
// A receipt without a cleared expected holder cannot complete the parent; a
// missing holder without the receipt could instead be expiry or another owner.
func observeReleasedHolder(ctx context.Context, lease Reservation, plan upgradetx.Plan) error {
	nonce, err := freshNonce()
	if err != nil {
		return err
	}
	proof, err := lease.Check(ctx, clonePlan(plan), nonce)
	if err != nil {
		return acceptedError(err)
	}
	if err := checkFleet(plan, proof, nonce, false); err != nil {
		return err
	}
	if proof.HolderHeld {
		return refusal(upgradetx.CodeUnsettled, "cluster holder release was not independently observed", true)
	}
	return nil
}

// keepReservation joins synchronously on every exit, including renewal refusal
// while Query is blocked. The failure cancels the driver's upstream-derived
// context so no later step can run under a lost expected holder.
// origin: hot-update S8 plan §6.3 / §11.2 P11
func keepReservation(ctx context.Context, fail context.CancelCauseFunc, lease Reservation, parent upgradetx.Handle) func() {
	renewCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	interval := lease.RenewalInterval()
	if interval <= 0 {
		// No guessed fallback interval: an adapter without its lease budget has
		// not established that its reservation can safely survive this driver.
		fail(refusal(upgradetx.CodeUnsettled, "reservation renewal budget is missing", true))
		close(done)
		return func() { stop(); <-done }
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				// The exact original handle reaches every renew. Leader/route changes
				// must be resolved by the adapter with current holder proof, rather
				// than manufacturing a different holder for a restarted driver.
				if err := lease.Renew(renewCtx, parent); err != nil {
					if renewCtx.Err() == nil {
						fail(acceptedError(err))
					}
					return
				}
			}
		}
	}()
	return func() { stop(); <-done }
}

// Expired parents are queried exhaustively, even when an earlier child is
// missing. This gathers the remaining accepted identities without granting
// permission to dispatch, converge, or extend the old absolute deadline.
func queryExpired(ctx context.Context, endpoint Endpoint, plan upgradetx.Plan) {
	for _, step := range plan.Steps {
		if !isCut(step) {
			continue
		}
		nonce, err := freshNonce()
		if err != nil {
			return
		}
		_, _ = endpoint.Query(ctx, step.Participant, step.Grant.Handle.TxID, nonce)
	}
}

func driveChild(ctx context.Context, store PlanStore, endpoint Endpoint, lease Reservation, plan upgradetx.Plan, step upgradetx.Step) error {
	started := false
	for {
		if ctx.Err() != nil {
			return acceptedError(context.Cause(ctx))
		}
		nonce, err := freshNonce()
		if err != nil {
			return err
		}
		view, err := endpoint.Query(ctx, step.Participant, step.Grant.Handle.TxID, nonce)
		if err != nil {
			return acceptedError(err)
		}
		if view.Code != "" || !reflect.DeepEqual(view.Historical.Grant, step.Grant) || !sameAttempt(view.Historical.Forward.Handle, step.Grant.Handle) || view.Observation.Nonce != nonce || view.Observation.At <= 0 {
			return refusal(upgradetx.CodeUnsettled, "original child identity or query proof changed", true)
		}
		// Equal-byte grants retain observation-only participants in the sealed
		// scope. They confer no cut authority and never stage or send Start.
		// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
		if ObservationOnly(step.Grant) {
			return recordUnchangedChild(store, plan, step, view, nonce)
		}
		attempt := view.Historical.Forward
		switch attempt.Phase {
		case upgradetx.PhaseCompleted:
			return recordCompletedChild(store, plan, step, view, nonce)
		case upgradetx.PhasePrepared:
			if err := startOriginalChild(ctx, store, endpoint, lease, plan, step, view, nonce, started); err != nil {
				return err
			}
			started = true
		case upgradetx.PhaseStaged, upgradetx.PhaseReady, upgradetx.PhaseFreezing, upgradetx.PhaseActivating, upgradetx.PhaseServingNew, upgradetx.PhaseMigrating:
			// Accepted work is target-owned. Poll the same handle without changing
			// its cut, restarting Start, or making caller cancellation a child
			// cancellation. Past deadline, retain the pending plan for settlement.
			if time.Now().UnixNano() >= plan.Deadline {
				return refusal(upgradetx.CodeUnsettled, "accepted child exceeds the original parent deadline", true)
			}
			if err := waitObservation(ctx); err != nil {
				return err
			}
		case upgradetx.PhaseDeferred, upgradetx.PhaseRejected, upgradetx.PhaseRollbackPending, upgradetx.PhaseRollbackBlocked, upgradetx.PhaseRolledBack, upgradetx.PhaseRecoveryRequired, upgradetx.PhaseCancelled, upgradetx.PhaseAbandoned:
			return refusal(upgradetx.CodeUnsettled, "original child needs explicit settlement", true)
		default:
			return refusal(upgradetx.CodeUnsettled, "original child phase is unknown", true)
		}
	}
}

// Observation-only grants have no dispatch authority; only their fresh scope is recorded.
func recordUnchangedChild(store PlanStore, plan upgradetx.Plan, step upgradetx.Step, view upgradetx.View, nonce string) error {
	if err := ObserveUnchanged(view, step.Grant, nonce); err != nil {
		return err
	}
	if err := childScope(plan, step, view.Observation.Current.Scope); err != nil {
		return err
	}
	parent, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	if stepDurable(parent.Historical, plan, step) {
		return nil
	}
	return recordStep(store, plan, step, digest(view.Observation))
}

func recordCompletedChild(store PlanStore, plan upgradetx.Plan, step upgradetx.Step, view upgradetx.View, nonce string) error {
	// A target can already have completed the original child after the
	// previous driver's reply was lost. Observe it and record that result;
	// neither its release string nor a stale completed view permits Start.
	if err := completed(view.Observation.Current, view.Observation, step.Grant, nonce); err != nil {
		return err
	}
	if err := childScope(plan, step, view.Observation.Current.Scope); err != nil {
		return err
	}
	parent, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	if parent.Historical.Forward.Phase == upgradetx.PhaseCompleted {
		if !stepDurable(parent.Historical, plan, step) {
			return refusal(upgradetx.CodeStateCorrupt, "completed parent has an unrecorded child", true)
		}
		return nil
	}
	return recordStep(store, plan, step, digest(view.Observation))
}

// Dispatch has one boundary: fresh source proof, original publication and durable intent.
// origin: hot-update S8 plan §3.1 / §4.2 / §5.1
func startOriginalChild(ctx context.Context, store PlanStore, endpoint Endpoint, lease Reservation, plan upgradetx.Plan, step upgradetx.Step, view upgradetx.View, nonce string, started bool) error {
	attempt := view.Historical.Forward
	// An authoritative prepared reply proves no Start was accepted. A
	// lost reply/unknown is never treated as this negative evidence.
	if started {
		// A successful RPC without acceptance is itself unknown. Re-sending
		// in a tight loop would turn an untrustworthy reply into execution
		// authority; a later Run must first obtain authoritative state.
		return refusal(upgradetx.CodeUnsettled, "Start returned without authoritative acceptance", true)
	}
	if attempt.Outcome != upgradetx.OutcomePending || attempt.CutStage != upgradetx.CutNone || time.Now().UnixNano() >= step.Grant.ExpiresAt {
		return refusal(upgradetx.CodeGrantExpired, "original child grant cannot start", true)
	}
	if !reflect.DeepEqual(view.Observation.Current, attempt) || attempt.Direction != upgradetx.DirectionForward || attempt.SourceSHA != step.Grant.SourceSHA || attempt.TargetSHA != step.Grant.TargetSHA || !validScope(attempt.Scope) || !view.Observation.OwnerKnown || view.Observation.RunningSHA != step.Grant.SourceSHA || view.Observation.EffectiveSHA != step.Grant.SourceSHA || ScopeDigest(attempt.Scope) != step.Grant.ScopeDigest || !validCensus(view.Observation.Census, attempt.Handle, attempt.Scope, step.Grant.ScopeDigest, nonce) {
		return refusal(upgradetx.CodeUnsettled, "child source census is not fresh and complete", true)
	}
	if err := childScope(plan, step, attempt.Scope); err != nil {
		return err
	}
	parent, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	if stepDurable(parent.Historical, plan, step) || parent.Historical.Forward.Phase == upgradetx.PhaseCompleted {
		return refusal(upgradetx.CodeUnsettled, "recorded child execution disappeared", true)
	}
	receipt, err := endpoint.Artifact(ctx, step.Grant)
	if err != nil {
		return acceptedError(err)
	}
	if !reflect.DeepEqual(receipt.Grant, step.Grant) || receipt.SourceSHA != step.Grant.SourceSHA || receipt.TargetSHA != step.Grant.TargetSHA || !artifactBound(ctx, endpoint, plan, receipt) || receipt.CanonicalPath == "" || receipt.SourcePath == "" || receipt.TargetPath == "" {
		return refusal(upgradetx.CodeUnsettled, "artifact is not bound to the original grant", true)
	}
	if err := observeFleet(ctx, lease, plan, true); err != nil {
		return err
	}
	// Staging/readback can take time and can overlap a new registration.
	// Recheck both the fleet and the original absolute grant expiry at
	// the dispatch boundary; successful earlier preflight is insufficient.
	if err := writeStep(store, plan, step, upgradetx.ActionIntent, ""); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return acceptedError(context.Cause(ctx))
	}
	if time.Now().UnixNano() >= step.Grant.ExpiresAt {
		return refusal(upgradetx.CodeGrantExpired, "original grant expired before dispatch", true)
	}
	// The return may be lost after acceptance. Only another query of
	// this handle can settle it; Start is not automatically retried.
	if _, err := endpoint.Start(ctx, step.Grant, receipt); err != nil {
		// An armed cut answers accepted/unsettled before its exec.
		// Observe the same ID; this flag forbids a second Start even
		// if a later prepared answer contradicts that acceptance.
		// origin: S8 internal review cluster-roll F1
		var accepted *upgradetx.Error
		if !errors.As(err, &accepted) || !accepted.Accepted || accepted.Code != upgradetx.CodeUnsettled {
			return acceptedError(err)
		}
	}
	return nil
}

// Polling consumes the existing request budget's cadence. This wait carries no
// new execution authority and is joined by the caller's cancellation, unlike an
// independently rooted sleep that could keep an abandoned driver alive.
func waitObservation(ctx context.Context) error {
	timer := time.NewTimer(upgradebudget.RequestLatency / upgradebudget.ProbeRatePerSecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return acceptedError(context.Cause(ctx))
	case <-timer.C:
		return nil
	}
}

// A completed child can be observed before this driver ever dispatched it.
// Preserve the action's three durable stages even on that recovery/no-op path,
// so an absent dispatch reply does not become an absent parent journal entry.
func recordStep(store PlanStore, plan upgradetx.Plan, step upgradetx.Step, result string) error {
	if err := writeStep(store, plan, step, upgradetx.ActionIntent, ""); err != nil {
		return err
	}
	if err := writeStep(store, plan, step, upgradetx.ActionObserved, result); err != nil {
		return err
	}
	return writeStep(store, plan, step, upgradetx.ActionDurable, result)
}

// Convergence and each reservation release use their own step handle, receipt
// and CAS readback. Replaying one of these idempotent barriers is permitted only
// for this original step; it does not repeat a component Start or issue a grant.
func driveBarrier(ctx context.Context, store PlanStore, endpoint Endpoint, lease Reservation, plan upgradetx.Plan, step upgradetx.Step) error {
	view, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	settled := stepDurable(view.Historical, plan, step)
	if !settled {
		if time.Now().UnixNano() >= plan.Deadline {
			return refusal(upgradetx.CodeUnsettled, "parent execution window expired before the barrier", true)
		}
		if err := writeStep(store, plan, step, upgradetx.ActionIntent, ""); err != nil {
			return err
		}
	}
	nonce, err := freshNonce()
	if err != nil {
		return err
	}
	kind := upgradetx.ReceiptControlReady
	var receipt upgradetx.Receipt
	step.DependsOn = append([]uint32(nil), step.DependsOn...)
	if step.Kind == upgradetx.StepHomeConverge {
		receipt, err = endpoint.Converge(ctx, step, nonce)
	} else {
		kind = upgradetx.ReceiptOldGenerationReleased
		receipt, err = lease.Release(ctx, step, nonce)
	}
	if err != nil {
		return acceptedError(err)
	}
	if !validReceipt(receipt, kind, step.Grant.Handle, step.Grant.ScopeDigest, nonce) {
		return refusal(upgradetx.CodeUnsettled, "barrier has no independent fresh receipt", true)
	}
	if !settled {
		return recordStep(store, plan, step, digest(receipt))
	}
	return nil
}

// A fleet manifest cannot equal every grant-bound publication hash. Production
// adapters verify each receipt at its original publisher; older boundary ports
// retain the single-publication commitment until they supply that explicit port.
// origin: S8 internal review cluster-roll F2
func artifactBound(ctx context.Context, endpoint Endpoint, plan upgradetx.Plan, receipt upgradeartifact.Receipt) bool {
	if verifier, ok := endpoint.(interface {
		VerifyArtifact(context.Context, upgradetx.Plan, upgradeartifact.Receipt) error
	}); ok {
		return validHex(receipt.ManifestDigest, 64) && verifier.VerifyArtifact(ctx, plan, receipt) == nil
	}
	return receipt.ManifestDigest == plan.TargetManifestDigest
}
