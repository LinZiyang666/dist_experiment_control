package upgraderoll

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"reflect"
	"slices"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func freshNonce() (string, error) {
	// Nonces are observation challenges, not execution IDs. Only target-issued
	// grants name executable children; a driver restart creates fresh challenges
	// while retaining every child TxID/CutID from the durable plan.
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", refusal(upgradetx.CodeUnsettled, "cannot obtain a fresh proof nonce", true)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func validStamp(stamp upgradetx.Stamp, handle upgradetx.Handle, scopeDigest, nonce string) bool {
	// Revisions can advance while an attempt keeps its cut/generation identity.
	// A receipt from a future revision is as invalid as one from a different cut.
	return sameAttempt(stamp.Attempt, handle) && stamp.Attempt.Revision > 0 && stamp.Attempt.Revision <= handle.Revision && validOwner(stamp.Producer) && stamp.Sequence > 0 && stamp.At > 0 && stamp.ScopeDigest == scopeDigest && validHex(stamp.Nonce, 32) && (nonce == "" || stamp.Nonce == nonce)
}

// Every expected ID remains visible through ownership transfer or durable
// termination. A collector cannot make a missing worker look released by
// deleting it from the census; extra admissions also prevent completion.
// origin: hot-update S8 plan §4.2 / §8.3 M06
func validCensus(census upgradetx.Census, handle upgradetx.Handle, scope upgradetx.Scope, scopeDigest, nonce string) bool {
	// Empty is legal only when it is explicitly observed and complete. The
	// expected-set digest seals original identities, while Resources may show
	// their new custody after migration; counters cannot represent that mapping.
	if !census.Known || !census.Complete || census.Admitting != 0 || len(census.OldOwners) != 0 || census.ExpectedDigest != digest(scope.Expected) || !validStamp(census.Stamp, handle, scopeDigest, nonce) || len(census.Resources) != len(scope.Expected) {
		return false
	}
	seen := make(map[string]bool)
	for _, resource := range census.Resources {
		id := resourceID(resource)
		if seen[id] || !validOwner(resource.Owner) || resource.Generation == 0 || resource.State == "" || resource.Custody == "" {
			return false
		}
		seen[id] = true
	}
	for _, resource := range scope.Expected {
		if !seen[resourceID(resource)] {
			return false
		}
	}
	return true
}

// Generic receipt validation deliberately does not infer kind from a phase.
// Each completion point has a different effect and must carry its own positive
// evidence, including a valid producer even when the resource list is empty.
func validReceipt(receipt upgradetx.Receipt, kind upgradetx.ReceiptKind, handle upgradetx.Handle, scopeDigest, nonce string) bool {
	return receipt.Kind == kind && receipt.Known && receipt.Complete && validHex(receipt.EvidenceDigest, 64) && validStamp(receipt.Stamp, handle, scopeDigest, nonce)
}

// Historical success is only a reference. Completion is checked against the
// fresh nonce, actual running SHA, exact scope and the admission sequence seen
// now. Forward and reverse never share receipt identities.
// origin: hot-update S8 plan §4.2 / §8.3 M07,M15
func completed(attempt upgradetx.Attempt, observation upgradetx.Observation, grant upgradetx.Grant, nonce string) error {
	bad := func() error { return refusal(upgradetx.CodeUnsettled, "fresh completion proof is incomplete", true) }
	if !sameAttempt(attempt.Handle, grant.Handle) || attempt.Direction != upgradetx.DirectionForward || attempt.Phase != upgradetx.PhaseCompleted || attempt.CutStage != upgradetx.CutReleased || attempt.Outcome != upgradetx.OutcomeSuccess || !validHex(grant.SourceSHA, 64) || !validHex(grant.TargetSHA, 64) || attempt.SourceGeneration == 0 || attempt.SourceGeneration >= attempt.Handle.Generation || attempt.SourceSHA != grant.SourceSHA || attempt.TargetSHA != grant.TargetSHA || !validScope(attempt.Scope) || ScopeDigest(attempt.Scope) != grant.ScopeDigest || !reflect.DeepEqual(attempt, observation.Current) || observation.Nonce != nonce || observation.At <= 0 || !observation.OwnerKnown || observation.RunningSHA != grant.TargetSHA || observation.EffectiveSHA != grant.TargetSHA {
		return bad()
	}
	// Components retain immutable execution receipts. A fresh runtime census
	// answers the query; its nonce must differ from the historical seal. Parent
	// collectors instead issue their aggregate Control at this observation.
	// origin: S8 internal review cluster-roll F5
	if attempt.Control.Stamp.Nonce != nonce {
		return upgradetx.Completed(attempt, observation)
	}
	if !validCensus(observation.Census, attempt.Handle, attempt.Scope, grant.ScopeDigest, nonce) {
		return bad()
	}
	receipts := []upgradetx.Receipt{attempt.Control, attempt.Resources, attempt.Released}
	kinds := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}
	for i, receipt := range receipts {
		// All three producers must be the observed current runtime owner. A
		// valid-looking boot/PID tuple from another process cannot sign this cut.
		// The admission gate must be the same sequence in the stamp, census and
		// fresh observation; a newly admitted object invalidates an older seal.
		if !validReceipt(receipt, kinds[i], attempt.Handle, grant.ScopeDigest, "") || receipt.Stamp.Producer != observation.Census.Stamp.Producer || receipt.Census.Stamp.Producer != receipt.Stamp.Producer || receipt.RunningSHA != grant.TargetSHA || !validCensus(receipt.Census, attempt.Handle, attempt.Scope, grant.ScopeDigest, "") || receipt.Stamp.Sequence != receipt.Census.Stamp.Sequence || receipt.Stamp.Sequence != observation.Census.Stamp.Sequence {
			return bad()
		}
	}
	// A live control probe must be causally tied to this query. Resource and
	// retirement receipts may describe durable earlier effects at the same gate.
	if attempt.Control.Stamp.Nonce != nonce {
		return bad()
	}
	return nil
}

func loadParent(store upgradetx.Store, plan upgradetx.Plan) (upgradetx.View, error) {
	// This lookup never asks for whatever happens to be Current. History A may
	// still exist when B runs, but that fact gives an old callback no authority
	// to change B; Apply below continues to CAS the captured A identity.
	view, err := store.Load(plan.Parent.Key, plan.Parent.TxID)
	if err != nil {
		return view, acceptedError(err)
	}
	record := view.Historical
	if record.Version != upgradetx.RecordVersion || record.Key != plan.Parent.Key || !sameAttempt(record.Forward.Handle, plan.Parent) || record.Revision < plan.Parent.Revision || record.Forward.Handle.Revision != record.Revision || !reflect.DeepEqual(record.Forward.Scope, plan.Scope) || !sameAttempt(record.Grant.Handle, plan.Parent) || record.Grant.ScopeDigest != ScopeDigest(plan.Scope) || record.Grant.Mode != upgradetx.ModeHot || !validOwner(record.Grant.Issuer) || !validHex(record.Grant.SourceSHA, 64) || !validHex(record.Grant.TargetSHA, 64) || record.Grant.ExpiresAt != plan.Deadline {
		return view, refusal(upgradetx.CodeUnsettled, "parent journal identity or scope changed", true)
	}
	if view.Code != "" {
		return view, refusal(upgradetx.CodeUnsettled, "parent journal is not actionable", true)
	}
	switch record.Forward.Phase {
	case upgradetx.PhasePrepared, upgradetx.PhaseStaged, upgradetx.PhaseReady, upgradetx.PhaseFreezing, upgradetx.PhaseActivating, upgradetx.PhaseServingNew, upgradetx.PhaseMigrating, upgradetx.PhaseCompleted:
	case upgradetx.PhaseDeferred, upgradetx.PhaseRejected, upgradetx.PhaseRollbackPending, upgradetx.PhaseRollbackBlocked, upgradetx.PhaseRolledBack, upgradetx.PhaseRecoveryRequired, upgradetx.PhaseCancelled, upgradetx.PhaseAbandoned:
		return view, refusal(upgradetx.CodeUnsettled, "parent needs explicit settlement", true)
	default:
		return view, refusal(upgradetx.CodeStateCorrupt, "parent phase is unknown", true)
	}
	return view, nil
}

func stepAction(plan upgradetx.Plan, step upgradetx.Step) upgradetx.Action {
	kind := upgradetx.ActionDispatch
	if step.Kind == upgradetx.StepHomeConverge {
		kind = upgradetx.ActionControl
	}
	if step.Kind == upgradetx.StepRelease {
		kind = upgradetx.ActionRelease
	}
	// Hashing the complete step also binds dependencies and participant identity.
	// Reusing a child SHA with a different grant/slot therefore cannot collide
	// with an observed action from a different accepted plan.
	// The action identity is deterministic from the sealed plan, while the
	// grant remains the target's ID. Neither depends on a restarted driver PID.
	return upgradetx.Action{ID: digest(step)[:32], Handle: plan.Parent, Kind: kind, IntentDigest: PlanDigest(plan), State: upgradetx.ActionIntent, Nonce: step.Grant.ClientNonce}
}

// writeStep uses the originally captured parent identity and current revision
// for every store CAS. A delayed driver cannot load Current B and claim it as A.
// The stored action's initial handle is preserved across intent/result updates.
// origin: hot-update S8 plan §3.2 / §3.3 / §8.3 M11
func writeStep(store upgradetx.Store, plan upgradetx.Plan, step upgradetx.Step, state upgradetx.ActionState, result string) error {
	view, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	record := view.Historical
	if record.Forward.Phase == upgradetx.PhaseCompleted {
		return refusal(upgradetx.CodeUnsettled, "completed parent execution is immutable", true)
	}
	action := stepAction(plan, step)
	for _, existing := range record.Actions {
		if existing.ID != action.ID {
			continue
		}
		if !sameAttempt(existing.Handle, plan.Parent) || existing.Kind != action.Kind || existing.IntentDigest != action.IntentDigest || existing.Nonce != action.Nonce {
			return refusal(upgradetx.CodeStateCorrupt, "parent step action identity changed", true)
		}
		if existing.State == upgradetx.ActionDurable {
			// Execution history is immutable. A newer fresh proof may update the
			// returned observation but never replace this already durable result.
			return nil
		}
		action = existing
	}
	if state == upgradetx.ActionIntent && action.State != upgradetx.ActionIntent {
		return nil
	}
	action.State = state
	action.ResultDigest = result
	_, err = store.Apply(record.Forward.Handle, upgradetx.Event{Action: action})
	if err != nil {
		return acceptedError(err)
	}
	// An acknowledged Apply must be visible in the sole journal before the next
	// effect. This does not replace the store's fsync obligation; it rejects an
	// adapter that returns success without actually recording the action/CAS.
	readback, err := loadParent(store, plan)
	if err != nil {
		return err
	}
	for _, saved := range readback.Historical.Actions {
		if reflect.DeepEqual(saved, action) && readback.Historical.Revision > record.Revision {
			return nil
		}
	}
	return refusal(upgradetx.CodeUnsettled, "parent action has no durable journal readback", true)
}

// Once final release intent is durable, a missing holder can mean its CAS
// succeeded before the reply/journal was lost. Only a plan with every earlier
// result durable may join that tombstone; it has no unexecuted child authority.
func finalReleaseStarted(record upgradetx.Record, plan upgradetx.Plan) bool {
	// A missing holder is not generally a release receipt. Permit tombstone
	// recovery only when every preceding action is durable and this exact final
	// release has already reached intent. No earlier child can start in this lane.
	for _, step := range plan.Steps[:len(plan.Steps)-1] {
		if !stepDurable(record, plan, step) {
			return false
		}
	}
	wanted := stepAction(plan, plan.Steps[len(plan.Steps)-1])
	for _, action := range record.Actions {
		if action.ID == wanted.ID && sameAttempt(action.Handle, plan.Parent) && action.IntentDigest == wanted.IntentDigest && action.Kind == wanted.Kind && action.Nonce == wanted.Nonce && (action.State == upgradetx.ActionIntent || action.State == upgradetx.ActionObserved || action.State == upgradetx.ActionDurable) {
			return true
		}
	}
	return false
}

func stepDurable(record upgradetx.Record, plan upgradetx.Plan, step upgradetx.Step) bool {
	// Counted results are insufficient: each ordinal has a distinct ID, kind,
	// parent binding and target nonce. A result for another step cannot discharge
	// this dependency just because it uses the same component or release string.
	wanted := stepAction(plan, step)
	for _, action := range record.Actions {
		if action.ID == wanted.ID && sameAttempt(action.Handle, plan.Parent) && action.Kind == wanted.Kind && action.IntentDigest == wanted.IntentDigest && action.Nonce == wanted.Nonce && action.State == upgradetx.ActionDurable && validHex(action.ResultDigest, 64) {
			return true
		}
	}
	return false
}

// proveChildren re-queries every original child at host/release barriers. This
// is also the no-op path: matching release strings or yesterday's completed
// record do not allow a disappeared participant to be skipped.
func proveChildren(ctx context.Context, endpoint Endpoint, plan upgradetx.Plan, host string) error {
	covered := make(map[string]bool)
	domains := make(map[upgradetx.Domain]bool)
	for _, step := range plan.Steps {
		if !isCut(step) || (host != "" && step.Participant.DomainID != host) {
			continue
		}
		nonce, err := freshNonce()
		if err != nil {
			return err
		}
		view, err := endpoint.Query(ctx, step.Participant, step.Grant.Handle.TxID, nonce)
		if err != nil {
			return refusal(upgradetx.CodeUnsettled, "original child cannot be observed", true)
		}
		if ObservationOnly(step.Grant) {
			if err := ObserveUnchanged(view, step.Grant, nonce); err != nil {
				return err
			}
		} else {
			if view.Code != "" || !reflect.DeepEqual(view.Historical.Grant, step.Grant) || !sameAttempt(view.Historical.Forward.Handle, step.Grant.Handle) || view.Historical.Forward.Phase != upgradetx.PhaseCompleted {
				return refusal(upgradetx.CodeUnsettled, "original child has not completed", true)
			}
			if err := completed(view.Observation.Current, view.Observation, step.Grant, nonce); err != nil {
				return err
			}
		}
		if err := childScope(plan, step, view.Observation.Current.Scope); err != nil {
			return err
		}
		for _, resource := range view.Observation.Current.Scope.Expected {
			covered[resourceID(parentResource(plan, step.Participant, resource))] = true
		}
		for _, domain := range view.Observation.Current.Scope.Domains {
			domains[domain] = true
		}
	}
	if host == "" {
		// The cluster barrier closes the full union, including helpers retained
		// by a child with a different runtime PID. A host barrier is narrower so
		// the first broker/agent host need not wait for a later unrelated host.
		for _, resource := range plan.Scope.Expected {
			if !covered[resourceID(resource)] {
				return refusal(upgradetx.CodeUnsettled, "child scopes omit an original resource", true)
			}
		}
		for _, domain := range plan.Scope.Domains {
			if !domains[domain] {
				return refusal(upgradetx.CodeUnsettled, "child scopes omit a required domain", true)
			}
		}
	}
	return nil
}

func childScope(plan upgradetx.Plan, step upgradetx.Step, scope upgradetx.Scope) error {
	// A child may own only its declared participant and original resources.
	// Owners/generations in the sealed expected list stay historical; their
	// actual current custody is supplied independently in the fresh census.
	if !reflect.DeepEqual(scope.Participants, []upgradetx.Key{step.Participant}) {
		return refusal(upgradetx.CodeUnsettled, "child scope participant changed", true)
	}
	for _, domain := range scope.Domains {
		if !slices.Contains(plan.Scope.Domains, domain) {
			return refusal(upgradetx.CodeUnsettled, "child scope has an unplanned domain", true)
		}
	}
	for _, resource := range scope.Expected {
		if !slices.Contains(plan.Scope.Expected, resource) && !slices.Contains(plan.Scope.Expected, ParentResource(step.Participant, resource)) {
			return refusal(upgradetx.CodeUnsettled, "child scope has an unplanned original resource", true)
		}
	}
	return nil
}

// The grants carry scope digests rather than full child scopes. Query their
// original prepared/accepted records before the first dispatch and close the
// union against the sealed parent. Otherwise a target-issued incomplete scope
// could finish a child and release its host before a missing helper is noticed.
// origin: hot-update S8 plan §3.1 / §4.2 / §8.3 M06,M09
func inspectPlanScopes(ctx context.Context, endpoint Endpoint, plan upgradetx.Plan) error {
	covered := make(map[string]bool)
	domains := make(map[upgradetx.Domain]bool)
	for _, step := range plan.Steps {
		if !isCut(step) {
			continue
		}
		nonce, err := freshNonce()
		if err != nil {
			return err
		}
		view, err := endpoint.Query(ctx, step.Participant, step.Grant.Handle.TxID, nonce)
		if err != nil {
			return acceptedError(err)
		}
		scope := view.Historical.Forward.Scope
		if view.Code != "" || !reflect.DeepEqual(view.Historical.Grant, step.Grant) || !sameAttempt(view.Historical.Forward.Handle, step.Grant.Handle) || view.Observation.Nonce != nonce || view.Observation.At <= 0 || !validScope(scope) || ScopeDigest(scope) != step.Grant.ScopeDigest {
			return refusal(upgradetx.CodeUnsettled, "original child scope cannot be observed", true)
		}
		if err := childScope(plan, step, scope); err != nil {
			return err
		}
		for _, resource := range scope.Expected {
			id := resourceID(parentResource(plan, step.Participant, resource))
			if covered[id] {
				// Two children claiming one original resource can create competing
				// custody even when each local receipt claims to be complete.
				return refusal(upgradetx.CodeUnsettled, "original resource belongs to two child scopes", true)
			}
			covered[id] = true
		}
		for _, domain := range scope.Domains {
			domains[domain] = true
		}
	}
	for _, resource := range plan.Scope.Expected {
		if !covered[resourceID(resource)] {
			return refusal(upgradetx.CodeUnsettled, "sealed child scopes omit an original resource", true)
		}
	}
	for _, domain := range plan.Scope.Domains {
		if !domains[domain] {
			return refusal(upgradetx.CodeUnsettled, "sealed child scopes omit a required domain", true)
		}
	}
	return nil
}

// ParentResource qualifies a local resource by its original participant. Two
// brokers may both own "tunnel:server" or the same session authority without
// either child claiming the other's live object. Child scopes stay unchanged.
// origin: hot-update S8 plan §4.2 / §5.1; S8 internal review cluster-roll F6
func ParentResource(key upgradetx.Key, resource upgradetx.Resource) upgradetx.Resource {
	resource.ID = digest(key)[:32] + ":" + resource.ID
	return resource
}
func parentResource(plan upgradetx.Plan, key upgradetx.Key, row upgradetx.Resource) upgradetx.Resource {
	qualified := ParentResource(key, row)
	if slices.Contains(plan.Scope.Expected, qualified) {
		return qualified
	}
	return row
}
