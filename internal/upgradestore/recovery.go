package upgradestore

import (
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// PrepareReverse binds a fresh execution to both the historical previous image
// and the exact current cut. An old completed transaction can never select the
// previous image of a newer transaction merely by supplying its old TxID.
// origin: hot-update S8 plan §8.1 / M12
func (s *Store) PrepareReverse(expected upgradetx.Fence, req upgradetx.ReverseRequest) (out upgradetx.Grant, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := currentCAS(e, expected.Current); err != nil {
			return err
		}
		r := *e.Current
		a := attempt(r)
		// A broker carries its complete authority map in Scope; the scalar
		// incarnation fences only the agent session. Requiring that scalar for
		// a broker rejected every native CLI reverse before its map preflight.
		// origin: S8 internal review activation-and-route-migration F9
		if req.HistoricalTxID != r.Grant.Handle.TxID || expected.Owner != s.options.Owner || (r.Key.Component == upgradetx.ComponentAgent && !validText(expected.Incarnation)) || expected.RunningSHA != a.TargetSHA || expected.PreviousSHA != r.RecordedPreviousSHA || req.TargetSHA != r.RecordedPreviousSHA {
			return failure(upgradetx.CodeRejected, "reverse source or historical target mismatch", true)
		}
		if metadata(e, req.HistoricalTxID).Window != upgradetx.WindowOpen {
			return failure(upgradetx.CodeWindowClosed, "rollback window is closed", true)
		}
		if a.CutStage != upgradetx.CutPartialActive && a.CutStage != upgradetx.CutActive && a.CutStage != upgradetx.CutRetiring && a.CutStage != upgradetx.CutReleased {
			return failure(upgradetx.CodeRejected, "reverse requires a current active cut", true)
		}
		// Scope comes from the current exporter, not from the stored forward
		// bootstrap snapshot. Preflight/activation still own live-generation
		// and reader gates; a stored authorization cannot replace either.
		prepare := upgradetx.PrepareRequest{ClientNonce: req.ClientNonce, ActorFP: req.ActorFP,
			SourceRelease: r.Grant.TargetRelease, TargetRelease: r.Grant.SourceRelease,
			SourceSHA: expected.RunningSHA, TargetSHA: req.TargetSHA, Mode: upgradetx.ModeHot,
			Scope: req.Scope, CapabilityDigest: r.Grant.CapabilityDigest, ExpiresAt: req.ExpiresAt}
		var err error
		out, err = s.prepare(d, e, prepare, &reverseBinding{HistoricalTxID: req.HistoricalTxID, Fence: expected})
		return err
	})
	if err != nil {
		out = upgradetx.Grant{}
	}
	return
}

func (s *Store) CloseWindow(expected upgradetx.Handle) (out upgradetx.Metadata, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		var err error
		out, err = s.closeWindow(d, e, expected)
		return err
	})
	if err != nil {
		out = upgradetx.Metadata{}
	}
	return
}

func (s *Store) closeWindow(d *diskFile, e *diskEntry, expected upgradetx.Handle) (upgradetx.Metadata, error) {
	if err := currentCAS(e, expected); err != nil {
		return upgradetx.Metadata{}, err
	}
	if !settled(*e.Current) {
		return upgradetx.Metadata{}, failure(upgradetx.CodeUnsettled, "live execution still occupies current", true)
	}
	m := metadata(e, expected.TxID)
	if m.Window == upgradetx.WindowClosed {
		return *m, nil
	}
	// Invalidate all prepared requests against the predecessor before dropping
	// its rollback pins. A new Start still rechecks the source fence.
	grants := e.Grants[:0]
	for _, p := range e.Grants {
		if p.Reverse == nil || p.Reverse.HistoricalTxID != expected.TxID {
			grants = append(grants, p)
		}
	}
	e.Grants = grants
	m.Window = upgradetx.WindowClosed
	// Keep the current boot reference even after rollback authority closes.
	// Only an archived predecessor can release these references.
	if err := bump(e); err != nil {
		return upgradetx.Metadata{}, err
	}
	if err := s.write(d, true); err != nil {
		return upgradetx.Metadata{}, err
	}
	return *m, nil
}

// Archive is the only way to release an unresolved Current after owner death.
// It records unknown/abandoned, not an invented successful rollback. A trusted
// observer must establish process identity AND custody; PID absence is not it.
// origin: hot-update S8 plan §4.1 / §8.1
func (s *Store) Archive(expected upgradetx.Handle, death upgradetx.DeathProof) (out upgradetx.View, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := currentCAS(e, expected); err != nil {
			return err
		}
		r := *e.Current
		if !settled(r) {
			if !validDeath(r, grantForAttempt(r, e.AcceptedGrants).Issuer, death, s.now().UnixNano()) {
				return failure(upgradetx.CodeUnsettled, "owner death or custody is unproven", true)
			}
			a := attempt(r)
			a.Phase, a.Outcome = upgradetx.PhaseAbandoned, upgradetx.OutcomeUnknown
			if r.Reverse.Handle.TxID != "" {
				r.Reverse = a
			} else {
				r.Forward = a
			}
			r.Reason = "owner death and custody independently verified"
			r.Revision++
			if r.Revision == 0 {
				return failure(upgradetx.CodeUnsettled, "record sequence exhausted", true)
			}
			setRevision(&r, r.Revision)
		}
		m := metadata(e, expected.TxID)
		m.Window, m.ArchivedAt, m.Pins = upgradetx.WindowClosed, s.now().UnixNano(), nil
		out = upgradetx.View{Historical: r, Metadata: *m}
		e.Archives = append(e.Archives, r)
		e.Current = nil
		// No prepared grant against an abandoned source may later acquire it.
		e.Grants = nil
		if err := bump(e); err != nil {
			return err
		}
		collect(e, s.now().UnixNano())
		return s.write(d, true)
	})
	if err != nil {
		out = upgradetx.View{}
	}
	return
}

func validDeath(r upgradetx.Record, owner upgradetx.Owner, d upgradetx.DeathProof, now int64) bool {
	a := attempt(r)
	if a.Control.Known {
		owner = a.Control.Stamp.Producer
	}
	return upgradetx.ValidateDeath(a, owner, d, now, int64(upgradebudget.RequestLatency)) == nil
}

// A complete flag cannot overrule a member still held by the old owner. Each
// migrated/released resource is either terminal with no custody or belongs to
// this target generation; a nonempty but merely textual custody is not proof.
// origin: hot-update S8 plan §4.2 / M03
func receiptBound(receipt upgradetx.Receipt, a upgradetx.Attempt, kind upgradetx.ReceiptKind) bool {
	return upgradetx.ValidateReceipt(a, receipt, kind) == nil
}

func settled(r upgradetx.Record) bool {
	a := attempt(r)
	switch a.Phase {
	case upgradetx.PhaseCompleted, upgradetx.PhaseRolledBack:
		return validJoint(a) && receiptBound(a.Released, a, upgradetx.ReceiptOldGenerationReleased)
	case upgradetx.PhaseDeferred, upgradetx.PhaseRejected, upgradetx.PhaseCancelled:
		// A selector/dispatch intent can have taken effect even if its receipt
		// was lost. Only shared source evidence can release a thawed owner.
		// origin: S8 internal review rv-fault F4
		return validJoint(a) && upgradetx.CancellationSettled(r)
	case upgradetx.PhasePrepared, upgradetx.PhaseStaged, upgradetx.PhaseReady,
		upgradetx.PhaseFreezing, upgradetx.PhaseActivating, upgradetx.PhaseServingNew,
		upgradetx.PhaseMigrating, upgradetx.PhaseRollbackPending, upgradetx.PhaseRollbackBlocked,
		upgradetx.PhaseRecoveryRequired, upgradetx.PhaseAbandoned:
		// Unresolved ownership cannot authorize a successor or blind release.
		return false
	}
	return false
}

// RecoverActions only queries external evidence. It never executes, dispatches
// or supplies a callback to redo an effect. Even a durable action is checked
// after reopen: its serialized state alone is not an external receipt.
// Observers must return a digest bound to the supplied ID, cut and nonce.
// origin: hot-update S8 plan §3.2 / TestActionCrashRecovery
func (s *Store) RecoverActions(expected upgradetx.Handle, observe func(upgradetx.Action) (string, error)) (upgradetx.Record, error) {
	view, err := s.Load(expected.Key, expected.TxID)
	if err != nil {
		return upgradetx.Record{}, err
	}
	r := view.Historical
	if attempt(r).Handle != expected || observe == nil {
		return upgradetx.Record{}, failure(upgradetx.CodeRejected, "recovery requires current handle and observer", true)
	}
	for _, action := range r.Actions {
		if !sameAttempt(action.Handle, attempt(r).Handle) {
			continue
		}
		// Old dispatch has no causal query. Once intent exists, an absent
		// receipt cannot prove that sending the old request is still safe.
		if r.Grant.Mode == upgradetx.ModeLegacyInterrupting && action.Kind == upgradetx.ActionDispatch && action.State == upgradetx.ActionIntent {
			return upgradetx.Record{}, failure(upgradetx.CodeUnsettled, "legacy dispatch outcome unknown", true)
		}
		result, err := observe(action)
		if err != nil || !validHex(result, 64) || action.ResultDigest != "" && action.ResultDigest != result {
			return upgradetx.Record{}, failure(upgradetx.CodeUnsettled, "action outcome not independently verified", true)
		}
		for action.State != upgradetx.ActionDurable {
			if action.State == upgradetx.ActionIntent {
				action.State = upgradetx.ActionObserved
			} else {
				action.State = upgradetx.ActionDurable
			}
			action.ResultDigest = result
			r, err = s.Apply(attempt(r).Handle, upgradetx.Event{Action: action})
			if err != nil {
				return upgradetx.Record{}, err
			}
		}
	}
	// Queries can race a late writer. Recheck even if every action was already
	// durable, rather than returning a formerly current execution as authority.
	err = s.locked(func(_ *diskFile, e *diskEntry) error { return currentCAS(e, attempt(r).Handle) })
	if err != nil {
		return upgradetx.Record{}, err
	}
	return r, nil
}
