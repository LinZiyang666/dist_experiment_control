package upgradetx

// ValidateCompletionBarrier binds the final running observation to the census
// held under the admission gate. Control precedes Seal and may omit later
// requests; Resources and Released must close the latest admission sequence.
// Historical membership survives a resource's durable terminal transition.
// origin: S8 internal review rv3-leaves F4 F5
func ValidateCompletionBarrier(a Attempt, o Observation, snapshot Census) error {
	// The running observation is sampled before the gated snapshot, so a live
	// resource may settle between the two: they are compared as monotone
	// custody progress, not equality (CensusProgressed).
	// origin: S8 BROKER-EXPORTER F1
	if !sameAttempt(a, o.Current) || o.Current.Handle != a.Handle || !o.OwnerKnown ||
		!observedActive(o.Current.CutStage) ||
		o.RunningSHA != a.TargetSHA || o.EffectiveSHA != a.TargetSHA ||
		o.Nonce != snapshot.Stamp.Nonce || o.At < snapshot.Stamp.At ||
		!CensusProgressed(o.Census, snapshot) || !validCensus(a, snapshot, true) ||
		snapshot.Stamp.Attempt != a.Handle || len(snapshot.OldOwners) != 0 || !completionReceipts(a) {
		return unsettled("completion admission barrier is unproven")
	}
	for _, receipt := range []Receipt{a.Control, a.Resources, a.Released} {
		if receipt.Stamp.Producer != snapshot.Stamp.Producer || receipt.Census.ExpectedDigest != snapshot.ExpectedDigest ||
			receipt.Stamp.Sequence > snapshot.Stamp.Sequence || receipt.Stamp.At > snapshot.Stamp.At ||
			!resourcesProgress(receipt.Census.Resources, snapshot.Resources) {
			return unsettled("completion lost witnessed custody")
		}
	}
	if a.Resources.Stamp.Sequence != snapshot.Stamp.Sequence || a.Released.Stamp.Sequence != snapshot.Stamp.Sequence {
		return unsettled("resource seal predates the final admission sequence")
	}
	return nil
}

// CensusProgressed compares the census of the final running observation with
// the later snapshot the admission gate holds through the completion CAS. Both
// carry one stamp, so everything the stamp and the gate fix must be equal: the
// stamp itself, Known/Complete, the expected digest, the admission count and
// the retained owners. Admissions are fenced from Seal to Commit, so no
// identity (domain, ID, origin) may appear or disappear, and no row may change
// owner, generation or applicability. The only fact allowed to move between
// the two samples is an executing or retained row reaching terminal custody —
// a live transfer settling, a retained terminal being released — never the
// reverse: a released row is fixed and a terminal row cannot resume.
// Equality was required here before, so a transfer that settled between the
// two samples failed a correct cut as "boot completion evidence is
// unavailable" (S8 FIX-DESCRIBE F1: 5/220 boundary cells ended
// recovery_required with all three receipts true).
// origin: S8 BROKER-EXPORTER F1; hot-update S8 plan §4.2
func CensusProgressed(earlier, later Census) bool {
	if earlier.Stamp != later.Stamp || earlier.Known != later.Known || earlier.Complete != later.Complete ||
		earlier.ExpectedDigest != later.ExpectedDigest || earlier.Admitting != later.Admitting ||
		!SameCensus(Census{OldOwners: earlier.OldOwners}, Census{OldOwners: later.OldOwners}) {
		return false
	}
	rows := make(map[string]Resource, len(later.Resources))
	for _, row := range later.Resources {
		if _, duplicate := rows[resourceIdentity(row)]; duplicate {
			return false
		}
		rows[resourceIdentity(row)] = row
	}
	for _, old := range earlier.Resources {
		row, ok := rows[resourceIdentity(old)]
		if !ok || row.Owner != old.Owner || row.Generation != old.Generation {
			return false
		}
		delete(rows, resourceIdentity(old))
		if row != old && (ResourceNotApplicable(old) || ResourceNotApplicable(row) || ResourceReleased(old) || row.State != "terminal") {
			return false
		}
	}
	// A row the running sample never saw is a late or substituted identity.
	return len(rows) == 0
}

func observedActive(stage CutStage) bool {
	return stage == CutActive || stage == CutRetiring || stage == CutReleased
}

// Identity and origin never disappear. Released terminal custody cannot regain
// execution rights; active target custody may naturally become terminal while
// the admission gate is held. Validation of each current census independently
// rejects retained/foreign owners and fake exclusions.
func resourcesProgress(earlier, later []Resource) bool {
	rows := make(map[string]Resource, len(later))
	for _, row := range later {
		rows[resourceIdentity(row)] = row
	}
	for _, old := range earlier {
		row, ok := rows[resourceIdentity(old)]
		if !ok || ResourceNotApplicable(old) != ResourceNotApplicable(row) || ResourceReleased(old) && !ResourceReleased(row) {
			return false
		}
	}
	return true
}
