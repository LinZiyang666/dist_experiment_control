// observe.go is the observing half of the broker runtime: a fresh
// local sample for query and completion, the importing target's Released
// receipt, and upgrade.describe. Control is signed only by the process that
// imported this exact cut, while serving its image, after one real request
// crossed its business subscriptions with this sample's nonce; the source
// never certifies the target's readiness.
// origin: hot-update S8 plan §2.1 / §4.2 / M05 / M06 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Observe is a fresh local sample: owner, running bytes, the journal's
// current attempt and an identity census. Unknown custody (no attached port,
// an unenumerable owner) is a truthful observation, never an empty broker.
//
// Two windows of an accepted cut answer a retryable deferral instead: the
// source between its armed final image and the exec (it can no longer sample
// what the target will hold), and the importer of the still unsettled cut
// before it can enumerate its owners (its transfer graph is not yet rebound).
// Neither is unknown health: an unknown view ends every waiting ctl as 74,
// while a deferral is asked again under the waiter's own deadline.
// origin: hot-update S8 plan §7.1; S8 BROKER-EXPORTER R-11 (real-cut finding)
func (p *brokerRuntime) Observe(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	if p.armed.Load() != nil {
		// origin: S8 internal review roll-recovery-stall F4
		// Arm can refuse before Run queues a reload, so Returned may never run.
		// Once the engine durably records uncertainty, this is accepted unknown,
		// not a transient exec window. Hiding it behind perpetual deferrals keeps
		// the parent polling until its entire rollout deadline expires.
		if view, started := p.record(stamp.Attempt); started && active(view.Historical).Phase == upgradetx.PhaseRecoveryRequired {
			return upgradetx.Observation{}, accepted(refusal(upgradetx.CodeUnsettled, "armed broker cut requires current-state recovery"))
		}
		return upgradetx.Observation{}, deferral(ReasonUpgradeInProgress)
	}
	o, err := p.observe(stamp, true)
	inherited := p.host.Inherited
	settled := o.Current.Phase == upgradetx.PhaseCompleted || o.Current.Phase == upgradetx.PhaseRolledBack
	if err == nil && !o.Census.Known && inherited != nil && upgradetx.SameCut(inherited.Attempt, o.Current.Handle) && !settled {
		return upgradetx.Observation{}, deferral(ReasonUpgradeInProgress)
	}
	return o, err
}

// observe samples once; control asks for the external control probe. The
// completion registry's domain observers and Retire read only the census and
// running bytes; the registry's own probe still runs the full Observe.
func (p *brokerRuntime) observe(stamp upgradetx.Stamp, control bool) (upgradetx.Observation, error) {
	h := p.host
	if h.Owner == nil || h.RunningSHA == nil || h.Journal == nil {
		return upgradetx.Observation{}, refusal(upgradetx.CodeNotWired, "broker runtime adapter is incomplete")
	}
	start := time.Now().UnixNano()
	owner, err := h.Owner()
	if err != nil {
		return upgradetx.Observation{}, err
	}
	running, err := h.RunningSHA()
	if err != nil {
		return upgradetx.Observation{}, refusal(upgradetx.CodeDeferred, ReasonRunningImageChanged)
	}
	view, started := p.record(stamp.Attempt)
	scope := stamp.ScopeDigest
	if !started {
		// A not-yet-accepted cut is observed against the journal's Current, and
		// stamped with that attempt's own scope (the engine's source fence).
		if latest, err := h.Journal.Load(h.Key, ""); err == nil {
			view = latest
			if active(latest.Historical).Handle.TxID != "" {
				scope = latest.Historical.Grant.ScopeDigest
			}
		}
	}
	a := active(view.Historical)
	o := upgradetx.Observation{Nonce: stamp.Nonce, Current: a, OwnerKnown: true, RunningSHA: running, EffectiveSHA: running}
	if view.Historical.EffectiveSHA != "" {
		o.EffectiveSHA = view.Historical.EffectiveSHA
	}
	s := p.stamp(stamp.Attempt, owner, stamp.Nonce, scope, stamp.Sequence)
	s.At = max(s.At, start)
	census := upgradetx.Census{Stamp: s}
	port := p.port.Load()
	facts, factsErr := p.facts(port)
	if factsErr == nil {
		generation, custody := p.custody(stamp.Attempt, view, started)
		if rows, err := censusRows(facts, owner, generation, custody); err == nil {
			if known, err := p.census(s, rows, view, started); err == nil {
				census = known
			}
		}
	}
	target := h.Inherited != nil && started && upgradetx.SameCut(h.Inherited.Attempt, a.Handle) && running == a.TargetSHA
	// The served image refines only an attempt still activating; once the
	// journal recorded retiring or released, the journal's stage stands.
	if target && factsErr == nil && facts.Serving && (a.CutStage == upgradetx.CutPartialActive || a.CutStage == upgradetx.CutActive) {
		o.Current.CutStage = upgradetx.CutActive
	}
	// One observation has one sampling time: the census stamp's.
	o.Census, o.At = census, s.At
	// A fresh Control receipt serves the completion still in progress. A
	// settled attempt is reported exactly as journaled.
	settled := a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack
	// A historical completion must still traverse this process's live
	// business subscriptions. Failure is unknown health, never cached ready.
	// origin: S8 internal review cluster-roll F5
	if control && target && census.Known && facts.Serving && (port.Control == nil || port.Control(stamp.Nonce) != nil) {
		if settled {
			o.Census.Known, o.Census.Complete = false, false
		}
		return o, nil
	}
	if control && target && !settled && census.Known && facts.Serving {
		o.Current.Control = upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: census.Stamp, Known: true, Complete: true,
			RunningSHA: running, Census: census, EvidenceDigest: digest(census)}
	}
	return o, nil
}

// Retire is signed only by the importing process: it proves that no row
// remains with the source generation. The source cannot certify itself, and
// a broker that imported no cut has none to release.
func (p *brokerRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	unproven := &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "old generation release is unproven", Accepted: true}
	view, started := p.record(h)
	if p.host.Inherited == nil || !started || !upgradetx.SameCut(p.host.Inherited.Attempt, h) {
		return upgradetx.Receipt{}, unproven
	}
	o, err := p.observe(upgradetx.Stamp{Attempt: h, Producer: p.host.Inherited.Owner, Nonce: nonce(), ScopeDigest: view.Historical.Grant.ScopeDigest}, false)
	if err != nil || !o.Census.Known || o.RunningSHA != active(view.Historical).TargetSHA {
		return upgradetx.Receipt{}, unproven
	}
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: o.Census.Stamp, Known: true, Complete: true,
		RunningSHA: o.RunningSHA, Census: o.Census, EvidenceDigest: digest(o.Census)}, nil
}

// DescribeRuntime reports the configured Key, the running /proc bytes with
// their release, the original scope — every domain and the identity of every
// object the census enumerates now — and the candidate staged at the
// registered canonical path. A reserved or armed cut, or an unsettled
// transaction, refuses (a new grant could not start); a census gap refuses
// rather than describing a partial scope a grant could hide behind (M06/M14).
// No not_applicable rows are described: the adjunct's closed codec requires an
// owner and generation on every expected row, and the census is a complete
// enumeration of what the broker holds, so an idle domain is observed empty.
func (p *brokerRuntime) DescribeRuntime(n string) (upgradeport.Description, error) {
	h, port := p.host, p.port.Load()
	if port == nil {
		return upgradeport.Description{}, deferral(ReasonDomainCensus)
	}
	if h.Owner == nil || h.RunningSHA == nil || h.Source == nil || h.Journal == nil {
		return upgradeport.Description{}, refusal(upgradetx.CodeNotWired, "broker runtime adapter is incomplete")
	}
	if !validNonce(n) {
		return upgradeport.Description{}, reject("describe_nonce_invalid")
	}
	owner, err := h.Owner()
	if err != nil {
		return upgradeport.Description{}, err
	}
	running, err := h.RunningSHA()
	if err != nil {
		return upgradeport.Description{}, deferral(ReasonRunningImageChanged)
	}
	source, err := h.Source(running)
	if err != nil || source.SHA256 != running || source.Release == "" {
		return upgradeport.Description{}, deferral(ReasonRunningImageChanged)
	}
	if p.busy(upgradetx.Handle{}) {
		return upgradeport.Description{}, deferral(ReasonUpgradeInProgress)
	}
	generation, err := p.describedGeneration()
	if err != nil {
		return upgradeport.Description{}, err
	}
	facts, err := p.facts(port)
	if err != nil {
		return upgradeport.Description{}, err
	}
	rows, err := censusRows(facts, owner, generation, "source")
	if err != nil {
		return upgradeport.Description{}, err
	}
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: nonce(), Participants: []upgradetx.Key{h.Key}, Domains: allDomains(), Expected: rows})
	if err != nil {
		return upgradeport.Description{}, deferral(ReasonDomainCensus)
	}
	d := upgradeport.Description{Key: h.Key, Nonce: n, At: time.Now().UnixNano(), RunningSHA: running, SourceRelease: source.Release, Scope: scope}
	if h.Canonical != "" && h.Candidate != nil {
		// No staged candidate is a description without one: the ctl refuses
		// with its own precise reason (candidate_declarations_unavailable).
		if candidate, err := h.Candidate(h.Canonical); err == nil {
			d.Candidate = candidate
		}
	}
	return d, nil
}

// describedGeneration is the generation that owns today's rows: the settled
// attempt's target after completion, its source after a refusal, and one for
// a runtime that never accepted a transaction. Any unsettled attempt refuses.
func (p *brokerRuntime) describedGeneration() (uint64, error) {
	view, err := p.host.Journal.Load(p.host.Key, "")
	var typed *upgradetx.Error
	if errors.As(err, &typed) && typed.Code == upgradetx.CodeGrantUnknown {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	a := active(view.Historical)
	switch a.Phase {
	case upgradetx.PhaseCompleted:
		return a.Handle.Generation, nil
	case upgradetx.PhaseCancelled, upgradetx.PhaseDeferred, upgradetx.PhaseRejected:
		return a.SourceGeneration, nil
	case upgradetx.PhaseAbandoned, upgradetx.PhaseActivating, upgradetx.PhaseFreezing, upgradetx.PhaseMigrating, upgradetx.PhasePrepared, upgradetx.PhaseReady, upgradetx.PhaseRecoveryRequired, upgradetx.PhaseRollbackBlocked, upgradetx.PhaseRollbackPending, upgradetx.PhaseRolledBack, upgradetx.PhaseServingNew, upgradetx.PhaseStaged:
		return 0, deferral(ReasonUpgradeInProgress)
	default:
		return 0, deferral(ReasonUpgradeInProgress)
	}
}

func allDomains() []upgradetx.Domain {
	return []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore,
		upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS,
		upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
}

func validNonce(n string) bool {
	if len(n) != 32 {
		return false
	}
	for _, c := range n {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
