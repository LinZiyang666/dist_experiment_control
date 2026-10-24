package upgradetx

// ValidateRecovery permits a known accepted target cut to resume after a
// temporary observation failure. A phase string cannot supply this authority:
// the fresh control probe must close current running/selector and all custody,
// retain earlier identities, and name the exact expected journal revision.
// origin: S8 internal review rv3-leaves F9
func ValidateRecovery(record Record, o Observation) error {
	a := record.Forward
	if !emptyAttempt(record.Reverse) {
		a = record.Reverse
	}
	c := o.Census
	if a.Phase != PhaseRecoveryRequired || a.CutStage != CutPartialActive && a.CutStage != CutActive && a.CutStage != CutRetiring ||
		!sameAttempt(a, o.Current) || o.Current.Handle != a.Handle || !o.OwnerKnown || !observedActive(o.Current.CutStage) ||
		o.RunningSHA != a.TargetSHA || o.EffectiveSHA != a.TargetSHA ||
		o.Nonce != c.Stamp.Nonce || o.At < c.Stamp.At || c.Stamp.Attempt != a.Handle ||
		!validCensus(a, c, true) || len(c.OldOwners) != 0 ||
		!validReceipt(a, o.Current.Control, ReceiptControlReady) || !SameCensus(o.Current.Control.Census, c) {
		return unsettled("recovery lacks current target control and custody")
	}
	// The source issuer need not be the imported target's owner. Before the
	// first Control receipt, the fresh bound control/custody probe establishes
	// that target identity; later recovery must retain its recorded producer.
	owner := c.Stamp.Producer
	if a.Control.Kind != "" {
		owner = a.Control.Stamp.Producer
	}
	if c.Stamp.Producer != owner {
		return unsettled("recovery changed the accepted owner")
	}
	for _, earlier := range []Receipt{a.Control, a.Resources, a.Released} {
		if earlier.Kind != "" && (o.Nonce == earlier.Stamp.Nonce || c.Stamp.At <= earlier.Stamp.At ||
			c.Stamp.Sequence < earlier.Stamp.Sequence || !resourcesProgress(earlier.Census.Resources, c.Resources)) {
			return unsettled("recovery lost witnessed custody or freshness")
		}
	}
	return nil
}

func recoveryEdge(before, after Attempt) bool {
	if before.Phase != PhaseRecoveryRequired || after.Outcome != OutcomePending || before.Direction != after.Direction {
		return false
	}
	if before.CutStage == CutRetiring {
		return after.Phase == PhaseMigrating && after.CutStage == CutRetiring
	}
	return (before.CutStage == CutPartialActive || before.CutStage == CutActive) && after.Phase == PhaseServingNew && after.CutStage == CutActive
}

// ColdSelectionSafe separates publication from current execution authority.
// A pending select may have changed the disk, and partial ACTIVE may already
// own resources even before its result/control write. Normal cold boot must
// hand those cuts to trusted recovery, never select the old source by default.
// origin: S8 internal review rv3-leaves F10
func ColdSelectionSafe(record Record) bool {
	a := record.Forward
	if !emptyAttempt(record.Reverse) {
		a = record.Reverse
	}
	if a.Phase == PhaseCompleted || a.Phase == PhaseRolledBack || CancellationSettled(record) {
		return true
	}
	if a.Phase == PhaseRecoveryRequired || a.Phase == PhaseAbandoned {
		return false
	}
	for _, cut := range []Attempt{record.Forward, record.Reverse} {
		if cut.CutStage == CutPartialActive || cut.CutStage == CutActive || cut.CutStage == CutRetiring || cut.CutStage == CutReleased {
			return false
		}
	}
	for _, action := range record.Actions {
		if sameCut(action.Handle, a.Handle) && (action.Kind == ActionSelect || action.Kind == ActionDispatch || action.Kind == ActionActivate) {
			return false
		}
	}
	return true
}
