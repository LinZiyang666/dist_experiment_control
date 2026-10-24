package brokerupgrade

import (
	"context"
	"math"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Rollback starts a distinct attempt from the currently running broker. The
// source record and its parent keep their historical outcome; Store owns the
// atomic current/previous/window comparison and the new generation allocation.
// origin: hot-update S8 plan §8.1 / §8.3 M07, M12, M13
func (e *engine) Rollback(ctx context.Context, expected upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.View, error) {
	if err := e.enter(ctx, expected.Current.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	view, err := e.loadExpected(expected.Current)
	if err != nil {
		return view, err
	}
	record := view.Historical
	a := active(record)
	if expected.Current.Generation == math.MaxUint64 || req.HistoricalTxID != record.Grant.Handle.TxID ||
		req.TargetSHA != record.RecordedPreviousSHA || expected.PreviousSHA != record.RecordedPreviousSHA ||
		!validSHA(req.TargetSHA) || !knownOwner(expected.Owner) || !validSHA(expected.RunningSHA) || !upgradetx.CensusCovers(a.Scope, upgradetx.Census{Resources: req.Scope.Expected}) ||
		(view.Metadata.Window != upgradetx.WindowOpen) {
		return view, refusal(upgradetx.CodeRejected, "rollback history, window or previous image does not match")
	}
	stamp := makeStamp(expected.Current, expected.Owner, record.Grant.ScopeDigest, nonce())
	current, err := e.runtime.Observe(stamp)
	if err != nil || !fresh(current, stamp) || !sameAttempt(current.Current.Handle, expected.Current) ||
		current.Census.Stamp.Producer != expected.Owner || current.RunningSHA != expected.RunningSHA ||
		!covers(a.Scope, current.Census) || !covers(req.Scope, current.Census) {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "rollback requires known current broker ownership"))
	}
	// A return to old code is still a new generation. Retained source owners
	// must first be salvaged/reaped by the current adapter, under durable intent.
	// Never activate a third owner just because the target SHA existed before.
	if len(current.Census.OldOwners) != 0 || a.Released.Kind == "" && a.Phase != upgradetx.PhaseCompleted {
		if a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack {
			return view, accepted(refusal(upgradetx.CodeUnsettled, "completed broker has unproven retained owners"))
		}
		action, intentErr := e.intent(&record, upgradetx.ActionRetire, "", "")
		if intentErr != nil {
			return e.failed(record, intentErr)
		}
		released, retireErr := e.runtime.Retire(handle(record))
		if retireErr != nil || !validReceipt(released, upgradetx.ReceiptOldGenerationReleased, a, stamp, expected.Owner) || len(released.Census.OldOwners) != 0 {
			return e.blocked(record, "retained broker owners prevent a third generation")
		}
		if err = e.apply(&record, upgradetx.Event{Receipt: released}); err != nil {
			return e.failed(record, err)
		}
		if err = e.result(&record, action, digest(released)); err != nil {
			return e.failed(record, err)
		}
		expected.Current = handle(record)
		stamp = makeStamp(expected.Current, expected.Owner, record.Grant.ScopeDigest, nonce())
		current, err = e.runtime.Observe(stamp)
		if err != nil || !fresh(current, stamp) || len(current.Census.OldOwners) != 0 ||
			!sameAttempt(current.Current.Handle, expected.Current) || current.RunningSHA != expected.RunningSHA || current.Census.Stamp.Producer != expected.Owner {
			return e.blocked(record, "retained broker custody remains unknown")
		}
	}
	grant, err := e.store.PrepareReverse(expected, req)
	if err != nil {
		return upgradetx.View{Historical: record}, err
	}
	// origin: hot-update S8 plan §8.1
	// A store/adapter wiring error cannot turn historical A's reverse request
	// into authority over B, reuse A's cut, or copy forward completion receipts.
	if grant.Handle.Key != expected.Current.Key || grant.Handle.CutID == expected.Current.CutID ||
		grant.Handle.Generation != expected.Current.Generation+1 || grant.SourceSHA != expected.RunningSHA ||
		grant.TargetSHA != record.RecordedPreviousSHA || grant.Issuer != expected.Owner {
		return view, refusal(upgradetx.CodeRejected, "reverse grant does not bind the current broker fence")
	}
	// Reverse has no caller-supplied artifact receipt in the frozen endpoint.
	// The runtime resolves these exact, already pinned SHAs from the sole store
	// and performs the same reader/config/lease preflight as forward. Empty paths
	// are not permission to fall back to canonical/.prev or to publish a slot.
	artifact := upgradeartifact.Receipt{Grant: grant, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA}
	if _, err = e.preflight(ctx, grant, artifact); err != nil {
		return e.blocked(record, "recorded previous image cannot read the current broker cut")
	}
	reverse, err := e.store.Start(grant, expected)
	if err != nil {
		return view, err
	}
	if active(reverse).Direction != upgradetx.DirectionReverse || !sameAttempt(active(reverse).Handle, grant.Handle) ||
		active(reverse).Control.Kind != "" || active(reverse).Resources.Kind != "" || active(reverse).Released.Kind != "" {
		return e.failed(reverse, refusal(upgradetx.CodeUnsettled, "reverse must begin with independent completion receipts"))
	}
	return e.drive(ctx, reverse, grant, artifact)
}

func (e *engine) blocked(record upgradetx.Record, detail string) (upgradetx.View, error) {
	a := active(record)
	// Completed history is immutable. A failed manual reverse preflight must
	// leave its success and parent untouched, while returning the live refusal.
	if a.Phase != upgradetx.PhaseCompleted && a.Phase != upgradetx.PhaseRolledBack {
		if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseRollbackBlocked, CutStage: a.CutStage, Outcome: upgradetx.OutcomePending, Reason: detail}); err != nil {
			return e.failed(record, err)
		}
	}
	return upgradetx.View{Historical: record, Code: upgradetx.CodeUnsettled, Detail: detail}, accepted(refusal(upgradetx.CodeUnsettled, detail))
}

func (e *engine) Cancel(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx, expected.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	view, err := e.loadExpected(expected)
	if err != nil {
		return view, err
	}
	record := view.Historical
	a := active(record)
	if a.CutStage != upgradetx.CutNone && a.CutStage != upgradetx.CutPristine {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "an activated broker cut requires current-state rollback"))
	}
	stamp := makeStamp(expected, record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
	o, err := e.runtime.Observe(stamp)
	if err != nil || !fresh(o, stamp) || o.RunningSHA != a.SourceSHA || o.Census.Stamp.Producer != record.Grant.Issuer || !covers(a.Scope, o.Census) {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "cancellation cannot prove the original broker is serving"))
	}
	// Thaw includes selector readback/unflip in the adapter. Its failure keeps
	// the journal and both pins, never a cancelled/rolled_back success claim.
	action, err := e.intent(&record, upgradetx.ActionFreeze, "", "")
	if err != nil {
		return e.failed(record, err)
	}
	if err = e.runtime.Thaw(handle(record)); err != nil {
		return e.failed(record, err)
	}
	stamp = makeStamp(handle(record), record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
	o, err = e.runtime.Observe(stamp)
	if err != nil || !fresh(o, stamp) || o.RunningSHA != a.SourceSHA || o.EffectiveSHA != a.SourceSHA ||
		o.Census.Stamp.Producer != record.Grant.Issuer || !covers(a.Scope, o.Census) {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "thaw did not confirm the source selector and owner"))
	}
	if err = e.result(&record, action, digest(o.Census)); err != nil {
		return e.failed(record, err)
	}
	// origin: S8 internal review rv-fault F4
	// Action result writes advance CAS. Bind the final source census to that
	// exact revision before atomically publishing cancellation and its proof.
	stamp = makeStamp(handle(record), record.Grant.Issuer, record.Grant.ScopeDigest, nonce())
	o, err = e.runtime.Observe(stamp)
	proof := upgradetx.CancellationProof{Thawed: true, Observation: o}
	if err != nil || !fresh(o, stamp) || upgradetx.ValidateCancellation(active(record), record.Grant.Issuer, proof) != nil {
		return e.failed(record, refusal(upgradetx.CodeUnsettled, "thaw did not confirm the source custody"))
	}
	if err = e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseCancelled, CutStage: a.CutStage, Outcome: upgradetx.OutcomeCancelled, Cancellation: &proof}); err != nil {
		return e.failed(record, err)
	}
	return upgradetx.View{Historical: record, Observation: o}, nil
}
