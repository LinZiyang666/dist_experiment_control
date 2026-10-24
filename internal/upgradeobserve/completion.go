// Package upgradeobserve holds the admission fence from Seal through durable
// completion. A failed commit keeps the fence; successful publication reopens
// target admissions only. Callbacks must not reenter the registry. Revision
// advances within a cut never discard its admissions or original commitment.
// origin: S8 internal review rv-txn F03 F08 F09
package upgradeobserve

import (
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// CompletionRegistry extends the frozen read/seal port with an owned commit
// barrier. The callback performs the final observation and durable store CAS
// under the admission gate. Older adapters must fail closed until wired.
type CompletionRegistry interface {
	Registry
	Commit(upgradetx.Stamp, func(upgradetx.Census) error) error
}

// Commit never falls back to Snapshot followed by an unguarded write.
func Commit(registry Registry, stamp upgradetx.Stamp, commit func(upgradetx.Census) error) error {
	r, ok := registry.(CompletionRegistry)
	if !ok {
		return unsettled("completion admission owner is unavailable")
	}
	return r.Commit(stamp, commit)
}

// Reconcile holds the same admission gate for a proof-authorized recovery CAS,
// without publishing completion or reopening admission. A failed proof/write
// leaves the seal in place for the original accepted cut.
// origin: S8 internal review rv3-leaves F9
func Reconcile(registry Registry, stamp upgradetx.Stamp, reconcile func(upgradetx.Census) error) error {
	r, ok := registry.(interface {
		Reconcile(upgradetx.Stamp, func(upgradetx.Census) error) error
	})
	if !ok {
		return unsettled("recovery admission owner is unavailable")
	}
	return r.Reconcile(stamp, reconcile)
}

func (r *sequencer) Reconcile(stamp upgradetx.Stamp, reconcile func(upgradetx.Census) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reconcile == nil || !r.sealed || r.pending != 0 || !upgradetx.SameCut(stamp.Attempt, r.attempt.Handle) ||
		stamp.Attempt.Revision < r.attempt.Handle.Revision || stamp.Producer != r.producer ||
		stamp.ScopeDigest != r.digest || stamp.Sequence != r.sequence || !validStamp(stamp) {
		return unsettled("recovery barrier is stale or unsealed")
	}
	census, err := r.snapshot(stamp)
	if err != nil {
		return err
	}
	return reconcile(census)
}

// Recover obtains a fresh accepted-cut proof and persists its narrow return to
// the completion path under the same gate used by Seal/Begin/Commit. The record
// is a CAS snapshot; the writer cannot reload Current to acquire another cut.
// origin: S8 internal review rv3-leaves F9
func Recover(registry Registry, record upgradetx.Record, stamp upgradetx.Stamp, observe func(upgradetx.Stamp) (upgradetx.Observation, error), apply func(upgradetx.Event) error) error {
	a := record.Forward
	if record.Reverse.Handle.TxID != "" {
		a = record.Reverse
	}
	if stamp.Attempt != a.Handle || observe == nil || apply == nil {
		return unsettled("recovery does not bind the expected revision")
	}
	if _, err := registry.Seal(a, stamp.Nonce); err != nil {
		return err
	}
	o, err := observe(stamp)
	if err != nil || o.Nonce != stamp.Nonce || o.At < stamp.At || o.At > time.Now().UnixNano() ||
		o.Census.Stamp.At < stamp.At || upgradetx.ValidateRecovery(record, o) != nil {
		return unsettled("fresh recovery owner or custody is unproven")
	}
	return Reconcile(registry, o.Census.Stamp, func(snapshot upgradetx.Census) error {
		// The proof was sampled before this gate: a live resource reaching
		// terminal custody in between is progress, not a changed custody set.
		// origin: S8 BROKER-EXPORTER F1
		if !upgradetx.CensusProgressed(o.Census, snapshot) {
			return unsettled("recovery census changed under admission")
		}
		event := upgradetx.Event{Recovery: &o, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive, Outcome: upgradetx.OutcomePending}
		if a.CutStage == upgradetx.CutRetiring {
			event.Phase, event.CutStage = upgradetx.PhaseMigrating, upgradetx.CutRetiring
		}
		if a.Control.Kind == "" {
			event.Receipt = o.Current.Control
		}
		return apply(event)
	})
}

func (r *sequencer) Commit(stamp upgradetx.Stamp, commit func(upgradetx.Census) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if commit == nil || !r.sealed || r.pending != 0 || !upgradetx.SameCut(stamp.Attempt, r.attempt.Handle) ||
		stamp.Attempt.Revision < r.attempt.Handle.Revision || stamp.Producer != r.producer || stamp.ScopeDigest != r.digest || stamp.Sequence != r.sequence || !validStamp(stamp) {
		return unsettled("completion barrier is stale or unsealed")
	}
	census, err := r.snapshot(stamp)
	if err != nil {
		return err
	}
	if err = commit(census); err != nil {
		return err
	}
	r.attempt.Handle.Revision = stamp.Attempt.Revision
	r.completed = true
	return nil
}

// Advance keeps one admission owner across an adjacent reverse cut. The new
// original commitment retains old scope identities. Late admissions remain
// tracked across the cut even if they arrived after Prepare; pending creates
// cannot be discarded by constructing a replacement registry.
// origin: S8 internal review rv-fault F5
// origin: S8 internal review rv-txn F05 F08 F09
func Advance(registry Registry, attempt upgradetx.Attempt, producer upgradetx.Owner) error {
	r, ok := registry.(*sequencer)
	if !ok {
		return nil
	} // Other registered adapters validate their own cut at Seal.
	r.mu.Lock()
	defer r.mu.Unlock()
	a, err := canonicalAttempt(attempt)
	if err != nil {
		return err
	}
	if sameCut(a, r.attempt) {
		return nil
	}
	if !validOwner(producer) || r.pending != 0 || a.Handle.Key != r.attempt.Handle.Key || a.SourceGeneration != r.attempt.Handle.Generation || a.Handle.Generation != a.SourceGeneration+1 ||
		a.Handle.CutID == r.attempt.Handle.CutID || !upgradetx.CensusCovers(r.attempt.Scope, upgradetx.Census{Resources: a.Scope.Expected}) {
		return unsettled("next cut lost current custody")
	}
	for _, entry := range r.admitted {
		for _, row := range a.Scope.Expected {
			if identityOf(row) == identityOf(entry.original) && row.Origin != entry.original.Origin {
				return unsettled("next cut changed admitted custody identity")
			}
		}
	}
	// Keep r.admitted and r.identities: the next census must still account for
	// every late identity, including those absent from the original grant.
	bindings, err := canonicalObservers(a.Scope, r.observers)
	if err != nil {
		return err
	}
	r.attempt, r.producer, r.observers = a, producer, bindings
	r.digest, r.expected = digestOf(a.Scope), digestOf(a.Scope.Expected)
	r.sealed, r.completed = false, false
	return nil
}
