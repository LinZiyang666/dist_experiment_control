package agentupgrade

import (
	"context"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// DeathObserver is the supplemental, read-only archive evidence port. The
// frozen RuntimePort's ordinary Observation cannot prove wait/reap or descriptor
// disappearance. Adapters must independently observe both rather than infer
// death from OwnerKnown=false or an empty resource count.
// origin: hot-update S8 plan §4.1 / §7.1 / §8.1
// AuthorityObserver supplies the verified current session incarnation for an
// automatic reverse. An absent observer keeps recovery unknown; the engine
// cannot invent an incarnation from PID, SHA, or a fixed placeholder.
// origin: S8 internal review rv-txn F05
type AuthorityObserver interface {
	Incarnation(upgradetx.Handle) (string, error)
}

type DeathObserver interface {
	ObserveDeath(upgradetx.Stamp) (upgradetx.DeathProof, error)
}

func (e *engine) Rollback(ctx context.Context, expected upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.View, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	return e.rollback(ctx, expected, req)
}

func (e *engine) rollback(ctx context.Context, expected upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.View, error) {
	view, err := e.query(expected.Current.Key, expected.Current.TxID, newID())
	if err != nil {
		return view, err
	}
	record, o := view.Historical, view.Observation
	a := current(record)
	// origin: hot-update S8 plan §8.1 / M12. Historical A's previous SHA is
	// not B's rollback target, even if A's completed journal remains readable.
	if !sameAttempt(a.Handle, expected.Current) || record.Revision != expected.Current.Revision || !sameAttempt(o.Current.Handle, expected.Current) || !o.OwnerKnown || o.Census.Stamp.Producer != expected.Owner || o.RunningSHA != expected.RunningSHA || record.RecordedPreviousSHA != expected.PreviousSHA || req.TargetSHA != record.RecordedPreviousSHA || req.HistoricalTxID != expected.Current.TxID {
		return view, refusal(upgradetx.CodeRejected, "reverse fence no longer owns the running transaction", true)
	}
	if view.Metadata.Window == upgradetx.WindowClosed || view.Metadata.Window == upgradetx.WindowClosing {
		return view, refusal(upgradetx.CodeWindowClosed, "historical rollback window is closed", true)
	}
	if a.CutStage == upgradetx.CutNone || a.CutStage == upgradetx.CutPristine {
		return e.cancel(expected.Current)
	}
	grant, err := e.store.PrepareReverse(expected, req)
	if err != nil {
		return view, publicError(err, true)
	}
	if !validGrant(grant) || grant.Handle.Key != expected.Current.Key || grant.Handle.CutID == expected.Current.CutID || grant.Handle.Generation != expected.Current.Generation+1 || grant.SourceSHA != o.RunningSHA || grant.TargetSHA != record.RecordedPreviousSHA || grant.Issuer != expected.Owner {
		return view, refusal(upgradetx.CodeUnsettled, "reverse grant does not describe a new current cut", true)
	}
	// Reverse has no caller-provided artifact receipt. Preflight must resolve
	// the original durable pins from this grant and verify the actual previous
	// reader. A zero receipt requests that lookup, never fabricates publication.
	proof, err := e.runtime.Preflight(ctx, grant, upgradeartifact.Receipt{})
	if err != nil {
		return e.blocked(record, "recorded previous reader is unavailable")
	}
	if err = checkPreflight(proof, grant, &req.Scope, false); err != nil {
		return e.blocked(record, "recorded previous reader or scope is incompatible")
	}
	reverse, err := e.store.Start(grant, expected)
	if err != nil {
		return view, publicError(err, true)
	}
	if !sameAttempt(current(reverse).Handle, grant.Handle) || current(reverse).Direction != upgradetx.DirectionReverse || current(reverse).Phase != upgradetx.PhasePrepared {
		return upgradetx.View{Historical: reverse}, refusal(upgradetx.CodeUnsettled, "accepted reverse attempt differs", true)
	}
	reverseAction, err := e.intent(&reverse, upgradetx.ActionReverse, "", "")
	if err == nil {
		err = e.result(&reverse, reverseAction, digest(grant), upgradetx.Event{})
	}
	if err == nil {
		reverse, err = e.run(ctx, reverse, grant, upgradeartifact.Receipt{}, expected.Current)
	}
	if err != nil {
		// Keep the local export/activation cause through this second recovery
		// projection too; the wire still exposes only the typed refusal.
		// origin: S8 internal review activation-and-route-migration F1 F10
		view, unresolved := e.markUnknown(reverse, "current reverse cut remains unresolved")
		return view, errors.Join(unresolved, err)
	}
	// P01/P02 preserve the forward/parent execution fact and reference the
	// completed reverse; a standalone reverse record remains completed. The
	// engine never writes rolled_back before its reverse receipts are durable.
	return e.query(reverse.Key, grant.Handle.TxID, newID())
}

func (e *engine) Cancel(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	return e.cancel(expected)
}

func (e *engine) cancel(expected upgradetx.Handle) (upgradetx.View, error) {
	view, err := e.query(expected.Key, expected.TxID, newID())
	if err != nil {
		return view, err
	}
	record, o := view.Historical, view.Observation
	a := current(record)
	if !sameAttempt(a.Handle, expected) || expected.Revision != record.Revision || !sameAttempt(o.Current.Handle, expected) || !o.OwnerKnown || o.RunningSHA != a.SourceSHA || !validCensus(o.Census, expected, record.Grant.Issuer, record.Grant.ScopeDigest) || !coversScope(o.Census, a.Scope) || a.CutStage != upgradetx.CutNone && a.CutStage != upgradetx.CutPristine {
		return view, refusal(upgradetx.CodeUnsettled, "cancellation cannot prove a pristine current owner", true)
	}
	if err = e.runtime.Thaw(expected); err != nil {
		return e.markUnknown(record, "pristine owner could not be thawed")
	}
	// origin: S8 internal review rv-fault F4
	// A successful Thaw call alone cannot certify the selector or FD custody.
	stamp := upgradetx.Stamp{Attempt: expected, Producer: record.Grant.Issuer, Nonce: newID(), At: time.Now().UnixNano(), ScopeDigest: record.Grant.ScopeDigest}
	o, err = e.runtime.Observe(stamp)
	proof := upgradetx.CancellationProof{Thawed: true, Observation: o}
	if err != nil || o.Nonce != stamp.Nonce || o.Census.Stamp.At < stamp.At || o.At < stamp.At || o.At > time.Now().UnixNano() || upgradetx.ValidateCancellation(a, record.Grant.Issuer, proof) != nil {
		return e.markUnknown(record, "thaw did not confirm the source selector and custody")
	}
	if err = e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseCancelled, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomeCancelled, Cancellation: &proof}); err != nil {
		return upgradetx.View{Historical: record}, err
	}
	return e.query(expected.Key, expected.TxID, newID())
}

func (e *engine) Archive(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	view, err := e.store.Load(expected.Key, expected.TxID)
	if err != nil {
		return view, err
	}
	if !sameAttempt(current(view.Historical).Handle, expected) || view.Historical.Revision != expected.Revision {
		return view, refusal(upgradetx.CodeRejected, "archive handle is stale", true)
	}
	a := current(view.Historical)
	if a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseCancelled || a.Phase == upgradetx.PhaseAbandoned {
		return view, refusal(upgradetx.CodeRejected, "terminal execution cannot be abandoned", true)
	}
	observer, ok := e.runtime.(DeathObserver)
	if !ok {
		return view, refusal(upgradetx.CodeUnsettled, "independent death and FD custody evidence is unavailable", true)
	}
	owner := view.Historical.Grant.Issuer
	if a.Control.Known && validOwner(a.Control.Stamp.Producer) {
		owner = a.Control.Stamp.Producer
	}
	stamp := upgradetx.Stamp{Attempt: expected, Producer: owner, Nonce: newID(), At: time.Now().UnixNano(), ScopeDigest: view.Historical.Grant.ScopeDigest}
	death, err := observer.ObserveDeath(stamp)
	if err != nil {
		return view, publicError(err, true)
	}
	// origin: hot-update S8 plan §8.1. A reaped owner cannot retain an active
	// resource just because a separate boolean says that its FDs are gone.
	// Each original responsibility is terminal or held by a known survivor.
	// origin: S8 internal review rv-txn F12
	if death.Stamp.Nonce != stamp.Nonce || death.Stamp.At < stamp.At || upgradetx.ValidateDeath(a, owner, death, time.Now().UnixNano(), int64(upgradebudget.RequestLatency)) != nil {
		return view, refusal(upgradetx.CodeUnsettled, "owner death or final custody is unproven", true)
	}
	return e.store.Archive(expected, death)
}

func (e *engine) CloseWindow(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	if !agentKey(expected.Key) {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "agent key is required", false)
	}
	metadata, err := e.store.CloseWindow(expected)
	if err != nil {
		return upgradetx.View{}, err
	}
	view, err := e.query(expected.Key, expected.TxID, newID())
	if err == nil {
		view.Metadata = metadata
	}
	return view, err
}

func (e *engine) blocked(record upgradetx.Record, reason string) (upgradetx.View, error) {
	// origin: S8 internal review rv3-leaves F8
	// A refused manual reverse reports its live reason without rewriting the
	// completed forward or concealing that reason behind a terminal-write error.
	if a := current(record); a.Phase != upgradetx.PhaseCompleted && a.Phase != upgradetx.PhaseRolledBack {
		if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseRollbackBlocked, CutStage: a.CutStage, Outcome: upgradetx.OutcomePending, Reason: reason}); err != nil {
			return upgradetx.View{Historical: record}, err
		}
	}
	return upgradetx.View{Historical: record, Code: upgradetx.CodeUnsettled, Detail: reason}, refusal(upgradetx.CodeUnsettled, reason, true)
}

func (e *engine) markUnknown(record upgradetx.Record, reason string) (upgradetx.View, error) {
	if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseRecoveryRequired, CutStage: current(record).CutStage, Outcome: upgradetx.OutcomeUnknown, Reason: reason}); err != nil {
		return upgradetx.View{Historical: record}, err
	}
	return upgradetx.View{Historical: record}, refusal(upgradetx.CodeUnsettled, reason, true)
}

func (e *engine) failed(ctx context.Context, record upgradetx.Record, grant upgradetx.Grant, cause error) (view upgradetx.View, err error) {
	// A secondary recovery refusal must not erase the initiating local error.
	// ErrorResponse still projects only the typed public recovery vocabulary.
	// origin: S8 internal review activation-and-route-migration F1
	defer func() {
		if err != nil {
			err = errors.Join(err, cause)
		}
	}()
	var txErr *upgradetx.Error
	var durableErr *durabilityError
	// A durable write failure or revocation is not a rollback authorization.
	// Keep the current intent/pins even if a later filesystem read looks good.
	if errors.As(cause, &durableErr) || errors.As(cause, &txErr) && (txErr.Code == upgradetx.CodeStateCorrupt || txErr.Code == upgradetx.CodeSessionIncarnationMismatch) {
		return e.markUnknown(record, "durability or authority prevents automatic recovery")
	}
	a := current(record)
	o, err := e.observe(a.Handle, upgradetx.Owner{}, grant.ScopeDigest, newID())
	if err != nil || !o.OwnerKnown || !sameAttempt(o.Current.Handle, a.Handle) || !validOwner(o.Census.Stamp.Producer) {
		return e.markUnknown(record, "failure has no known current recovery owner")
	}
	handle := a.Handle
	handle.Revision = record.Revision
	if a.CutStage == upgradetx.CutNone || a.CutStage == upgradetx.CutPristine {
		view, cancelErr := e.cancel(handle)
		if cancelErr != nil {
			return view, cancelErr
		}
		return view, publicError(cause, true)
	}
	if a.Direction == upgradetx.DirectionReverse {
		return e.markUnknown(record, "reverse failure requires current owner recovery")
	}
	authority, ok := e.runtime.(AuthorityObserver)
	if !ok {
		return e.markUnknown(record, "current authority observation unavailable")
	}
	incarnation, err := authority.Incarnation(handle)
	if err != nil || !hexValue(incarnation, 64) {
		return e.markUnknown(record, "current authority is unproven")
	}
	fence := upgradetx.Fence{Incarnation: incarnation, Current: handle, Owner: o.Census.Stamp.Producer, RunningSHA: o.RunningSHA, PreviousSHA: record.RecordedPreviousSHA}
	request := upgradetx.ReverseRequest{ClientNonce: newID(), ActorFP: grant.ActorFP, HistoricalTxID: a.Handle.TxID, TargetSHA: record.RecordedPreviousSHA, Scope: a.Scope, ExpiresAt: time.Now().Add(upgradebudget.ComponentHandover).UnixNano()}
	view, reverseErr := e.rollback(ctx, fence, request)
	if reverseErr != nil {
		return view, reverseErr
	}
	// Automatic reverse success means the requested forward target failed.
	// The rollback command itself can succeed, but Start cannot report rc0.
	return view, refusal(upgradetx.CodeRejected, "forward failed and current reverse completed", true)
}
