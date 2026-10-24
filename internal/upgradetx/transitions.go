package upgradetx

import (
	"reflect"
	"slices"
)

// ValidatePlan checks the saved execution plan, never a fresh roster. Expiry,
// issuer liveness and reservations are checked by the target/store immediately
// before Start; a pure structural check cannot renew execution authority.
// origin: hot-update S8 plan §3.1 / §5.1 / §9.1
func ValidatePlan(plan Plan) error {
	refuse := func() error { return &Error{Code: CodeRejected, Detail: "invalid fixed upgrade plan"} }
	if !validHandle(plan.Parent) || !validScope(plan.Scope) || !slices.Contains(plan.Scope.Participants, plan.Parent.Key) ||
		!hexString(plan.SourceManifestDigest, 64) || !hexString(plan.TargetManifestDigest, 64) || plan.Deadline <= 0 || len(plan.Steps) == 0 {
		return refuse()
	}
	seen := make(map[uint32]Step, len(plan.Steps))
	// Keep the original participant set, cut IDs and child transaction IDs
	// separately. Counting only steps would miss a duplicate that displaced
	// a different participant; comparing only SHAs would mint execution twice.
	cuts := make(map[Key]uint32, len(plan.Scope.Participants))
	transactions := make(map[string]bool, len(plan.Steps))
	cutIDs := make(map[string]bool, len(plan.Steps))
	slots := make(map[string]Step, len(plan.Steps))
	for i, step := range plan.Steps {
		g := step.Grant
		if step.Ordinal == 0 || (i > 0 && step.Ordinal <= plan.Steps[i-1].Ordinal) || !validGrant(g) ||
			step.Participant != g.Handle.Key || !slices.Contains(plan.Scope.Participants, step.Participant) ||
			g.ParentTxID != plan.Parent.TxID || !boundedText(g.ParentSlot, 128) || g.ExpiresAt > plan.Deadline ||
			g.Handle.TxID == plan.Parent.TxID || transactions[g.Handle.TxID] || cutIDs[g.Handle.CutID] {
			return refuse()
		}
		transactions[g.Handle.TxID], cutIDs[g.Handle.CutID] = true, true
		for j, dependency := range step.DependsOn {
			// A sorted backward-only DAG can be resumed in its saved order.
			// Missing, repeated and future dependencies are not interpreted as
			// already completed work, even when the current roster is smaller.
			if _, ok := seen[dependency]; !ok || (j > 0 && dependency <= step.DependsOn[j-1]) {
				return refuse()
			}
		}
		// Co-located children deliberately borrow one reservation. Their grants
		// remain distinct and their dependency chain must serialize that slot;
		// an unrelated installation cannot adopt it by reusing the slot string.
		if previous, shared := slots[g.ParentSlot]; shared && (previous.Participant.DomainID != step.Participant.DomainID || !dependsOn(step, previous.Ordinal, seen)) {
			return refuse()
		}
		slots[g.ParentSlot] = step
		// Release is its own grant/receipt and must follow the entire saved
		// plan. A component cut cannot stand in for the parent's last release.
		if step.Kind == StepRelease {
			if i != len(plan.Steps)-1 || i == 0 || !dependsOnAll(step, seen) {
				return refuse()
			}
		} else if step.Kind == StepHomeConverge {
			ordinal, ok := cuts[step.Participant]
			if !ok || !dependsOn(step, ordinal, seen) {
				return refuse()
			}
		} else if (step.Kind == StepAgentCut && step.Participant.Component == ComponentAgent) ||
			(step.Kind == StepBrokerCut && step.Participant.Component == ComponentBroker) {
			if _, duplicate := cuts[step.Participant]; duplicate {
				return refuse()
			}
			cuts[step.Participant] = step.Ordinal
		} else {
			return refuse()
		}
		seen[step.Ordinal] = step
	}
	if plan.Steps[len(plan.Steps)-1].Kind != StepRelease || len(cuts) != len(plan.Scope.Participants) {
		return refuse()
	}
	return nil
}

func dependsOnAll(step Step, previous map[uint32]Step) bool {
	for ordinal := range previous {
		if !dependsOn(step, ordinal, previous) {
			return false
		}
	}
	return true
}

func dependsOn(step Step, ordinal uint32, previous map[uint32]Step) bool {
	// Dependencies already point strictly backwards. A visited set bounds a
	// shared DAG without recursive expansion or a caller-controlled stack.
	queue := slices.Clone(step.DependsOn)
	visited := make(map[uint32]bool, len(previous))
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if id == ordinal {
			return true
		}
		if !visited[id] {
			visited[id] = true
			queue = append(queue, previous[id].DependsOn...)
		}
	}
	return false
}

func validGrant(g Grant) bool {
	// These are structural bindings, not issuer authentication. The store
	// still verifies the original target-issued grant, actor, boot identity,
	// expiry and reservation before it accepts any execution. No local clock
	// or random ID is consulted here, so validating a saved plan is repeatable.
	return validHandle(g.Handle) && validOwner(g.Issuer) && boundedText(g.ClientNonce, 256) &&
		boundedText(g.ActorFP, 256) && (g.Mode == ModeHot || g.Mode == ModeLegacyInterrupting) &&
		boundedText(g.SourceRelease, 128) && boundedText(g.TargetRelease, 128) &&
		hexString(g.SourceSHA, 64) && hexString(g.TargetSHA, 64) && hexString(g.ScopeDigest, 64) &&
		hexString(g.CapabilityDigest, 64) && g.ExpiresAt > 0 &&
		((g.ParentTxID == "" && g.ParentSlot == "") || (hexString(g.ParentTxID, 32) && boundedText(g.ParentSlot, 128)))
}

// Transition returns a detached value. Store.Apply owns expected-handle CAS,
// durable writes and authority; receipt stamps retain their original revisions
// even when later action writes advance the journal revision.
// A completed transition records the three historical proofs. Reporting live
// success additionally requires Completed with a fresh external observation.
// origin: hot-update S8 plan §3.2 / §3.3 / §4.1
func Transition(record Record, event Event) (Record, error) {
	if ValidateRecord(record) != nil {
		return Record{}, &Error{Code: CodeStateCorrupt, Detail: "invalid upgrade execution record", Accepted: true}
	}
	if reflect.DeepEqual(event, Event{}) || record.Revision == ^uint64(0) {
		return Record{}, unsettled("empty event or exhausted journal revision")
	}
	// Death and custody are not expressible in Event. Only Store.Archive can
	// validate DeathProof and publish abandoned; a phase string cannot reap.
	if event.Phase == PhaseAbandoned {
		return Record{}, unsettled("abandonment requires the store archive death proof")
	}
	next := cloneRecord(record)
	current := &next.Forward
	if !emptyAttempt(next.Reverse) {
		current = &next.Reverse
	}
	if terminalPhase(current.Phase) {
		return Record{}, unsettled("terminal execution is immutable")
	}
	before := *current
	// Omitted event fields retain their recorded values. Requiring the entire
	// resulting tuple to be legal catches a phase-only success update that
	// accidentally leaves partial_active or pending behind.
	if event.Phase != "" {
		current.Phase = event.Phase
	}
	if event.CutStage != "" {
		current.CutStage = event.CutStage
	}
	if event.Outcome != "" {
		current.Outcome = event.Outcome
	}
	// origin: S8 internal review rv3-leaves F9
	// Only this typed, revision-bound proof can leave accepted uncertainty.
	// Engines obtain it under the admission gate; disk uses the same validator.
	recovering := event.Recovery != nil
	if recovering && (ValidateRecovery(record, *event.Recovery) != nil || !recoveryEdge(before, *current)) {
		return Record{}, unsettled("invalid accepted-cut recovery proof")
	}
	// origin: S8 internal review roll-recovery-stall F5
	// Only a sealed pristine source-custody proof may refine the conservative
	// partial-active intent after a positively unqueued activation. The same
	// predicate must survive disk decode; a phase-only regression still fails.
	unqueued := false
	if event.Cancellation != nil && before.CutStage == CutPartialActive && current.CutStage == CutPristine {
		candidate := next
		candidate.Cancellation = event.Cancellation
		unqueued = CancellationSettled(candidate)
	}
	if !validJointState(*current) || !allowedEdge(before, *current) && !recovering && !unqueued {
		return Record{}, unsettled("illegal phase, cut, direction or outcome transition")
	}
	if event.Action != (Action{}) {
		if err := applyAction(&next, *current, event.Action); err != nil {
			return Record{}, err
		}
	}
	if !reflect.DeepEqual(event.Receipt, Receipt{}) {
		// A receipt describes effects that already happened. It cannot be
		// persisted in the same write that first declares those effects may
		// happen; partial_active must survive a crash before the effect starts.
		if before.CutStage == CutNone || before.CutStage == CutPristine {
			return Record{}, unsettled("receipt preceded the durable partial activation boundary")
		}
		if err := applyReceipt(current, event.Receipt); err != nil {
			return Record{}, err
		}
	}
	if (current.Phase == PhaseServingNew || current.Phase == PhaseMigrating) &&
		!validReceipt(*current, current.Control, ReceiptControlReady) {
		return Record{}, unsettled("serving requires an independent control receipt")
	}
	if current.Phase == PhaseCompleted && !completionReceipts(*current) {
		return Record{}, unsettled("completion requires all three sealed receipts")
	}
	// A failed selector recovery cannot be relabelled a harmless refusal. With
	// no unflip proof in Event, only recovery_required is safe after select.
	// origin: S8 internal review rv-fault F4
	if event.Cancellation != nil {
		if current.Phase != PhaseCancelled || event.Cancellation.Observation.Census.Stamp.Attempt != before.Handle ||
			ValidateCancellation(before, record.Grant.Issuer, *event.Cancellation) != nil {
			return Record{}, unsettled("cancellation does not bind the current source")
		}
		next.Cancellation = cloneCancellation(event.Cancellation)
		next.EffectiveSHA = current.SourceSHA
	}
	if before.Phase == PhaseRecoveryRequired && current.Phase == PhaseCancelled && next.Cancellation == nil {
		return Record{}, unsettled("pristine recovery requires durable thaw evidence")
	}
	if current.Phase == PhaseDeferred || current.Phase == PhaseRejected || current.Phase == PhaseCancelled {
		for _, action := range next.Actions {
			if sameCut(action.Handle, current.Handle) && (action.Kind == ActionActivate && !CancellationSettled(next) || action.Kind == ActionDispatch && !CancellationSettled(next) ||
				action.Kind == ActionSelect && next.Cancellation == nil) {
				return Record{}, unsettled("selected or activated execution needs recovery evidence")
			}
		}
	}
	// origin: S8 internal review rv-txn F06
	// A target control receipt includes actual running/selector readback.
	if event.Receipt.Kind == ReceiptControlReady {
		next.EffectiveSHA = current.TargetSHA
	}
	// origin: S8 internal review rv3-leaves F10
	// An observed selector/activation result follows actual runtime readback;
	// intent alone does not say which SHA was selected. Control is independent.
	if event.Action.State == ActionObserved || event.Action.State == ActionDurable {
		if event.Action.Kind == ActionSelect || event.Action.Kind == ActionActivate {
			next.EffectiveSHA = current.TargetSHA
		}
	}
	next.Revision++
	current.Handle.Revision = next.Revision
	if event.Reason != "" {
		if !boundedText(event.Reason, 4096) {
			return Record{}, unsettled("invalid transaction reason")
		}
		next.Reason = event.Reason
	}
	if current == &next.Reverse && current.Phase == PhaseCompleted && !emptyAttempt(next.Forward) {
		// Automatic reverse completes independently before projecting rolled_back
		// on its still-pending forward. Historical completed records never enter
		// this branch: manual reverse uses a new reverse-only current record.
		next.Forward.Phase, next.Forward.CutStage, next.Forward.Outcome = PhaseRolledBack, CutReleased, OutcomeRolledBack
	}
	if !validRecord(next) {
		return Record{}, unsettled("event conflicts with the sealed execution record")
	}
	return next, nil
}

func validRecord(record Record) bool {
	// Validate both the incoming and proposed record. This prevents a corrupt
	// journal from being repaired into apparent success by an otherwise legal
	// event, and makes failures return no replacement record at all.
	if record.Version != RecordVersion || !validKey(record.Key) || record.Revision == 0 || !validGrant(record.Grant) ||
		record.Grant.Handle.Key != record.Key || !hexString(record.EffectiveSHA, 64) || !hexString(record.RecordedPreviousSHA, 64) {
		return false
	}
	forward, reverse := record.Forward, record.Reverse
	root := forward
	if emptyAttempt(forward) {
		// A manual rollback starts a new reverse-only Current. Its historical
		// completed forward and parent are owned by the immutable archive.
		root = reverse
	}
	if !validAttempt(root) || !sameCut(root.Handle, record.Grant.Handle) || root.SourceSHA != record.Grant.SourceSHA ||
		root.TargetSHA != record.Grant.TargetSHA || root.Handle.Key != record.Key || root.Handle.Revision > record.Revision {
		return false
	}
	if !emptyAttempt(forward) && forward.Direction != DirectionForward {
		return false
	}
	if !emptyAttempt(reverse) {
		// Automatic rollback retains the failed forward but has a fresh cut,
		// next generation and current source SHA. Reusing old code bytes is
		// allowed; resurrecting that code's old execution generation is not.
		if !validAttempt(reverse) || reverse.Direction != DirectionReverse || reverse.Handle.Key != record.Key ||
			reverse.Handle.Revision > record.Revision || reverse.TargetSHA != record.RecordedPreviousSHA {
			return false
		}
		if !emptyAttempt(forward) && (forward.Phase != PhaseRollbackPending && forward.Phase != PhaseRollbackBlocked && forward.Phase != PhaseRolledBack ||
			reverse.SourceSHA != forward.TargetSHA || reverse.SourceGeneration != forward.Handle.Generation ||
			reverse.Handle.CutID == forward.Handle.CutID || !containsResources(reverse.Scope.Expected, forward.Scope.Expected)) {
			return false
		}
		// Preparing reverse may wait for salvage, but its first possible effect
		// would create G+2. G must already have an independent retirement proof;
		// returning to its code SHA does not make three live owners safe.
		// origin: hot-update S8 plan §8.1 / M01
		if !emptyAttempt(forward) && slices.Contains([]CutStage{CutPartialActive, CutActive, CutRetiring, CutReleased}, reverse.CutStage) &&
			!validReceipt(forward, forward.Released, ReceiptOldGenerationReleased) {
			return false
		}
	}
	current := root
	if !emptyAttempt(reverse) {
		current = reverse
	}
	if current.Handle.Revision != record.Revision || record.Grant.Handle.Revision > root.Handle.Revision {
		return false
	}
	if record.Cancellation != nil && (current.Phase != PhaseCancelled ||
		ValidateCancellation(current, record.Grant.Issuer, *record.Cancellation) != nil) {
		return false
	}
	for _, attempt := range []Attempt{forward, reverse} {
		if emptyAttempt(attempt) {
			continue
		}
		for _, pair := range []struct {
			receipt Receipt
			kind    ReceiptKind
		}{{attempt.Control, ReceiptControlReady}, {attempt.Resources, ReceiptResourcesMigrated}, {attempt.Released, ReceiptOldGenerationReleased}} {
			if !reflect.DeepEqual(pair.receipt, Receipt{}) && (!validReceipt(attempt, pair.receipt, pair.kind) ||
				(sameCut(attempt.Handle, root.Handle) && pair.receipt.Stamp.ScopeDigest != record.Grant.ScopeDigest) ||
				attempt.CutStage == CutNone || attempt.CutStage == CutPristine) {
				return false
			}
		}
		if (attempt.Phase == PhaseServingNew || attempt.Phase == PhaseMigrating) && !validReceipt(attempt, attempt.Control, ReceiptControlReady) {
			return false
		}
		if attempt.Phase == PhaseCompleted && (record.Grant.Mode != ModeHot || !completionReceipts(attempt)) {
			return false
		}
	}
	if forward.Phase == PhaseRolledBack && (emptyAttempt(reverse) || reverse.Phase != PhaseCompleted || !completionReceipts(reverse)) {
		return false
	}
	ids := make(map[string]bool, len(record.Actions))
	// Action IDs are unique across the entire execution record, not just the
	// currently selected direction. A reverse must not claim a forward effect.
	for _, action := range record.Actions {
		if ids[action.ID] || !validAction(forward, action) && !validAction(reverse, action) {
			return false
		}
		ids[action.ID] = true
	}
	return true
}

// The table is deliberately joint: partial_active without control is reachable
// and must remain distinguishable from pristine even when activation fails.
// origin: hot-update S8 plan §4.1 / P01
func validJointState(a Attempt) bool {
	if a.Direction != DirectionForward && a.Direction != DirectionReverse {
		return false
	}
	if !slices.Contains([]CutStage{CutNone, CutPristine, CutPartialActive, CutActive, CutRetiring, CutReleased}, a.CutStage) {
		return false
	}
	switch a.Phase {
	case PhasePrepared, PhaseStaged, PhaseReady:
		return a.CutStage == CutNone && a.Outcome == OutcomePending
	case PhaseFreezing:
		return a.CutStage == CutPristine && a.Outcome == OutcomePending
	case PhaseActivating:
		return slices.Contains([]CutStage{CutPristine, CutPartialActive, CutActive}, a.CutStage) && a.Outcome == OutcomePending
	case PhaseServingNew:
		return (a.CutStage == CutPartialActive || a.CutStage == CutActive) && a.Outcome == OutcomePending
	case PhaseMigrating:
		return (a.CutStage == CutActive || a.CutStage == CutRetiring) && a.Outcome == OutcomePending
	case PhaseCompleted:
		return a.CutStage == CutReleased && a.Outcome == OutcomeSuccess
	case PhaseDeferred:
		return a.CutStage == CutNone && a.Outcome == OutcomeDeferred
	case PhaseRejected:
		return a.CutStage == CutNone && a.Outcome == OutcomeRejected
	case PhaseRollbackPending, PhaseRollbackBlocked:
		return a.Direction == DirectionForward && slices.Contains([]CutStage{CutPartialActive, CutActive, CutRetiring}, a.CutStage) && a.Outcome == OutcomePending
	case PhaseRolledBack:
		return a.Direction == DirectionForward && a.CutStage == CutReleased && a.Outcome == OutcomeRolledBack
	case PhaseRecoveryRequired, PhaseAbandoned:
		return a.Outcome == OutcomeUnknown
	case PhaseCancelled:
		return (a.CutStage == CutNone || a.CutStage == CutPristine) && a.Outcome == OutcomeCancelled
	}
	return false
}

func terminalPhase(phase Phase) bool {
	return slices.Contains([]Phase{PhaseCompleted, PhaseDeferred, PhaseRejected, PhaseRolledBack, PhaseCancelled, PhaseAbandoned}, phase)
}

func allowedEdge(before, after Attempt) bool {
	if terminalPhase(before.Phase) || before.Direction != after.Direction {
		return false
	}
	if before.Phase == PhaseRecoveryRequired {
		// A phase-only Event has no owner-death/unflip/custody authority.
		// Recovery diagnostics may be appended; source recovery now requires
		// CancellationProof, while owner-death release remains Store.Archive.
		// origin: S8 internal review rv-fault F4
		return after.Phase == PhaseRecoveryRequired && after.CutStage == before.CutStage ||
			after.Phase == PhaseCancelled && (before.CutStage == CutNone || before.CutStage == CutPristine)
	}
	if after.Phase == PhaseRecoveryRequired {
		return after.CutStage == before.CutStage
	}
	if after.Phase == PhaseDeferred || after.Phase == PhaseRejected {
		return before.CutStage == CutNone
	}
	if after.Phase == PhaseCancelled {
		return before.CutStage == CutNone || before.CutStage == CutPristine
	}
	// Retiring/released and partial activation are one-way ownership boundaries.
	rank := map[CutStage]int{CutNone: 0, CutPristine: 1, CutPartialActive: 2, CutActive: 3, CutRetiring: 4, CutReleased: 5}
	if rank[after.CutStage] < rank[before.CutStage] {
		return false
	}
	// ACTIVE cannot be the first durable indication that effects were possible.
	// Persist partial_active before allowing the importer to open any pumps.
	if rank[before.CutStage] < rank[CutPartialActive] && rank[after.CutStage] > rank[CutPartialActive] {
		return false
	}
	if after.Phase == PhaseRollbackPending || after.Phase == PhaseRollbackBlocked {
		return before.CutStage == CutPartialActive || before.CutStage == CutActive || before.CutStage == CutRetiring
	}
	if before.Phase == after.Phase {
		return true
	}
	edges := map[Phase]Phase{
		PhasePrepared: PhaseStaged, PhaseStaged: PhaseReady, PhaseReady: PhaseFreezing,
		PhaseFreezing: PhaseActivating, PhaseActivating: PhaseServingNew,
		PhaseServingNew: PhaseMigrating, PhaseMigrating: PhaseCompleted,
	}
	return edges[before.Phase] == after.Phase
}

func applyReceipt(a *Attempt, receipt Receipt) error {
	var destination *Receipt
	switch receipt.Kind {
	case ReceiptControlReady:
		destination = &a.Control
	case ReceiptResourcesMigrated:
		destination = &a.Resources
	case ReceiptOldGenerationReleased:
		destination = &a.Released
	}
	if destination == nil || !validReceipt(*a, receipt, receipt.Kind) ||
		(a.CutStage != CutPartialActive && a.CutStage != CutActive && a.CutStage != CutRetiring && a.CutStage != CutReleased) {
		return unsettled("receipt does not prove the active attempt")
	}
	if !reflect.DeepEqual(*destination, Receipt{}) && !reflect.DeepEqual(*destination, receipt) {
		// Delivery is idempotent, evidence is immutable. A different receipt in
		// the same slot is not silently allowed to erase the first sealed set.
		return unsettled("a sealed receipt cannot be replaced")
	}
	*destination = cloneReceipt(receipt)
	return nil
}

func validAction(a Attempt, action Action) bool {
	// Intent never carries a result. Observed and durable rows both retain
	// the same result digest; fsync/readback truth is the store owner's job,
	// rather than a boolean inferred from this in-memory state machine.
	if !hexString(action.ID, 32) || !sameCut(action.Handle, a.Handle) || action.Handle.Revision == 0 ||
		action.Handle.Revision > a.Handle.Revision || !hexString(action.IntentDigest, 64) || !hexString(action.Nonce, 32) ||
		!slices.Contains([]ActionKind{ActionPublish, ActionSelect, ActionFreeze, ActionDispatch, ActionActivate, ActionControl,
			ActionMigrate, ActionRetire, ActionReverse, ActionRelease}, action.Kind) {
		return false
	}
	return (action.State == ActionIntent && action.ResultDigest == "") ||
		((action.State == ActionObserved || action.State == ActionDurable) && hexString(action.ResultDigest, 64))
}

func applyAction(record *Record, a Attempt, action Action) error {
	if !validAction(a, action) {
		return unsettled("invalid action identity or evidence")
	}
	for i, old := range record.Actions {
		if old.ID != action.ID {
			continue
		}
		// origin: S8 internal review rv-fault F1 F4
		// The write CAS may accompany a result, but stored operation identity
		// always retains the original intent revision for external readback.
		if action.Handle == a.Handle && sameCut(old.Handle, action.Handle) {
			action.Handle = old.Handle
		}
		if old.Handle != action.Handle || old.Kind != action.Kind || old.IntentDigest != action.IntentDigest || old.Nonce != action.Nonce ||
			(old.ResultDigest != "" && old.ResultDigest != action.ResultDigest) {
			return unsettled("action identity and result are immutable")
		}
		if old == action {
			// A retry observes the same operation; it does not append a second
			// action or reset the original intent/nonce for another dispatch.
			return nil
		}
		if (old.State != ActionIntent || action.State != ActionObserved) && (old.State != ActionObserved || action.State != ActionDurable) {
			return unsettled("action must advance from intent through observation to durable result")
		}
		record.Actions[i] = action
		return nil
	}
	if action.State != ActionIntent {
		return unsettled("action result has no durable intent")
	}
	record.Actions = append(record.Actions, action)
	return nil
}

func emptyAttempt(a Attempt) bool { return reflect.DeepEqual(a, Attempt{}) }

func cloneReceipt(receipt Receipt) Receipt {
	// Pure transitions must not lend mutable evidence back to the caller.
	// Clone only slices: every leaf value in the frozen DTOs is scalar.
	receipt.Census.Resources = slices.Clone(receipt.Census.Resources)
	receipt.Census.OldOwners = slices.Clone(receipt.Census.OldOwners)
	return receipt
}

func cloneAttempt(a Attempt) Attempt {
	a.Scope.Participants = slices.Clone(a.Scope.Participants)
	a.Scope.Domains = slices.Clone(a.Scope.Domains)
	a.Scope.Expected = slices.Clone(a.Scope.Expected)
	a.Control, a.Resources, a.Released = cloneReceipt(a.Control), cloneReceipt(a.Resources), cloneReceipt(a.Released)
	return a
}

func cloneRecord(record Record) Record {
	record.Forward, record.Reverse = cloneAttempt(record.Forward), cloneAttempt(record.Reverse)
	record.Actions = slices.Clone(record.Actions)
	record.Cancellation = cloneCancellation(record.Cancellation)
	return record
}

func cloneCancellation(p *CancellationProof) *CancellationProof {
	if p == nil {
		return nil
	}
	copy := *p
	copy.Observation.Current = cloneAttempt(p.Observation.Current)
	copy.Observation.Census.Resources = slices.Clone(p.Observation.Census.Resources)
	copy.Observation.Census.OldOwners = slices.Clone(p.Observation.Census.OldOwners)
	return &copy
}
