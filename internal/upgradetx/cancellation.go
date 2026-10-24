// Package upgradetx treats R0 cancellation as source ownership evidence, not a
// successful rollback. Durable publication alone is harmless. Freeze or select
// requires a successful thaw followed by a fresh source/selector/custody census.
// Unknown activation/dispatch cannot be erased by a cancellation phase. Legacy
// cancellations without this evidence retain Current until independently reaped.
// origin: S8 internal review rv-fault F4
package upgradetx

// CancellationProof records readback after the runtime restored its source
// fences. It is separate from target receipts, which certify a different owner.
// This additive evidence does not add an action kind or change Store signatures.
type CancellationProof struct {
	Thawed      bool        `json:"thawed"`
	Observation Observation `json:"observation"`
}

// ValidateCancellation is shared by both engines, Transition, and disk decode.
// Fresh nonce/time and the exact write revision are checked when submitting the
// event; persisted evidence retains that revision across subsequent reads.
func ValidateCancellation(a Attempt, owner Owner, p CancellationProof) error {
	o, c := p.Observation, p.Observation.Census
	if !p.Thawed || !o.OwnerKnown || !sameAttempt(o.Current, a) || o.RunningSHA != a.SourceSHA || o.EffectiveSHA != a.SourceSHA ||
		o.Nonce != c.Stamp.Nonce || o.At < c.Stamp.At || !validCensus(a, c, false) || c.Stamp.Producer != owner || c.Admitting != 0 {
		return unsettled("cancellation lacks source thaw evidence")
	}
	for _, old := range c.OldOwners {
		if old != owner {
			return unsettled("cancellation retains another owner")
		}
	}
	for _, row := range c.Resources {
		if ResourceNotApplicable(row) || ResourceReleased(row) {
			continue
		}
		if row.Owner != owner || row.Generation != a.SourceGeneration {
			return unsettled("cancellation retains foreign execution custody")
		}
	}
	return nil
}

// CancellationSettled distinguishes a safe R0 terminal from an unresolved
// legacy terminal. It never treats a result hash as independent thaw evidence.
func CancellationSettled(r Record) bool {
	a := r.Forward
	if !emptyAttempt(r.Reverse) {
		a = r.Reverse
	}
	if a.Phase != PhaseCancelled && a.Phase != PhaseDeferred && a.Phase != PhaseRejected {
		return false
	}
	if a.CutStage != CutNone && a.CutStage != CutPristine {
		return false
	}
	proven := r.Cancellation != nil && a.Phase == PhaseCancelled && ValidateCancellation(a, r.Grant.Issuer, *r.Cancellation) == nil
	if !proven && a.CutStage != CutNone {
		return false
	}
	for _, action := range r.Actions {
		if !sameCut(action.Handle, a.Handle) {
			continue
		}
		switch action.Kind {
		case ActionReverse:
			// origin: S8 internal review rv3-leaves F7
			// The reverse authorization is inert until freeze/dispatch. Only a
			// reverse-only record with fresh source proof can settle it; an
			// automatic reverse still owes the original forward retirement.
			if !proven || !emptyAttempt(r.Forward) || a.Direction != DirectionReverse || action.State != ActionDurable {
				return false
			}
		case ActionPublish:
			if action.State != ActionDurable {
				return false
			}
		case ActionFreeze, ActionSelect:
			if !proven {
				return false
			}
		case ActionDispatch:
			// Dispatch intent precedes activation intent in both engines. A
			// refused activation write leaves no receiver, but only a durable
			// pristine freeze and subsequent thaw can certify that boundary.
			if !proven || action.State != ActionIntent || r.Cancellation.Observation.Current.CutStage != CutPristine || !durableFreeze(r, a.Handle) {
				return false
			}
		case ActionActivate:
			// origin: S8 internal review roll-recovery-stall F5
			// A causal refusal before queueing can refine an activation intent
			// back to pristine after thaw. Observed target effects and generic
			// source-SHA guesses cannot use this R0 proof to release Current.
			if !proven || action.State != ActionIntent || r.Cancellation.Observation.Current.CutStage != CutPristine || !durableFreeze(r, a.Handle) ||
				a.Control.Kind != "" || a.Resources.Kind != "" || a.Released.Kind != "" {
				return false
			}
		case ActionControl, ActionMigrate, ActionRelease, ActionRetire:
			return false
		default:
			return false
		}
	}
	// Cancelling an automatic reverse does not retire the original forward.
	if !emptyAttempt(r.Reverse) && !emptyAttempt(r.Forward) {
		return false
	}
	return true
}

func durableFreeze(r Record, h Handle) bool {
	for _, action := range r.Actions {
		if sameCut(action.Handle, h) && action.Kind == ActionFreeze && action.State == ActionDurable {
			return true
		}
	}
	return false
}
