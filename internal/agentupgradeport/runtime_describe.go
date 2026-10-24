// runtime_describe.go answers upgrade.describe for the agent: what a Prepare
// must name, stated by this process alone. A ctl knows only the session and
// node name; the registered Key, the running bytes and the original scope are
// facts of the target, and a ctl-invented scope could hide retained work.
// origin: hot-update S8 plan §2.1 / §3.1 / §4.2 / M06; S8 FIX-DESCRIBE (P19 request R1)
package agentupgradeport

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

var _ upgradeport.RuntimeDescriber = (*runtimePort)(nil)

// Describe is the engine's half of upgrade.describe: it relays only the
// runtime's own description of exactly the selected agent and this nonce.
// A runtime that cannot describe itself is not wired; any other answer
// (another node, a stale nonce, a key without its installation facts)
// refuses before the ctl downloads anything. The engine serializes the call.
// origin: hot-update S8 plan §2.1 / §3.1; S8 FIX-DESCRIBE (P19 request R1)
func Describe(runtime upgradeport.RuntimePort, selector upgradetx.Key, nonce string) (upgradeport.Description, error) {
	describer, ok := runtime.(upgradeport.RuntimeDescriber)
	if !ok {
		return upgradeport.Description{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent runtime cannot describe itself"}
	}
	d, err := describer.DescribeRuntime(nonce)
	if err != nil {
		return upgradeport.Description{}, err
	}
	k := d.Key
	if k.Component != upgradetx.ComponentAgent || k.DomainID == "" || k.SID == "" || k.ConfiguredNID == "" ||
		upgradewire.DescribeSelector(k) != selector || d.Nonce != nonce {
		return upgradeport.Description{}, deferral(upgradewire.DescriptionUnavailable)
	}
	return d, nil
}

// idleDomainImages reports whether the frozen bundle's flow and SS images
// carry no responsibility. An image the owner exports while idle (a tunnel
// client with no endpoint, flow or retirement; an SS image with no listener,
// connection or key) has zero identities, and zero is then its enumeration —
// the same predicate the live pre-freeze census applies. Anything retained
// still refuses without the typed identity producer (M06/M14). domainRows
// applies this guard only to hosts that have no such producer.
// The transfer image is not judged here: its entries are named by the
// worker's own census and checked against the sealed count (transfers).
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census)
func idleDomainImages(b *agentimage.Bundle) bool {
	if f := b.Flow; f != nil && (len(f.Endpoints) != 0 || len(f.Flows.Records) != 0 || len(f.Flows.Retirement) != 0 || f.Flows.ReservedBytes != 0) {
		return false
	}
	s := b.SS
	return s == nil || s.ConnectionCount == 0 && s.ListenerRef == "" && s.SecretBytes == 0
}

// DescribeRuntime reports the configured Key, the running /proc bytes with
// their release, and the original scope: every frozen domain and the identity
// of every retained object the census enumerates now. A held cut or an
// unsettled transaction refuses (a new grant could not start), and a domain
// the census cannot enumerate refuses rather than describing a partial scope
// (M06/M14).
//
// No not_applicable rows are described. The upgrade-cut-v1 adjunct carries
// the sealed scope across exec and its closed codec requires an owner and a
// generation on every expected row (upgradecut.validScope), so an ownerless
// exclusion would make every cut fail at Pack ("invalid upgrade adjunct").
// The agent's census is a complete enumeration of everything it holds, so its
// broker domains are observed — Known, Complete and empty — not uncounted.
// Carrying explicit exclusions needs the adjunct owner to admit them first.
func (p *runtimePort) DescribeRuntime(nonce string) (upgradeport.Description, error) {
	h := p.host
	if h.Owner == nil || h.RunningSHA == nil || h.Source == nil || h.Snapshot == nil || h.Journal == nil {
		return upgradeport.Description{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent runtime adapter is incomplete"}
	}
	if !hexValue(nonce, 32) {
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
	release, source, err := h.Source(running)
	if err != nil || source.SHA256 != running || release == "" {
		return upgradeport.Description{}, deferral(ReasonRunningImageChanged)
	}
	p.mu.Lock()
	held := p.held != nil
	p.mu.Unlock()
	if held {
		return upgradeport.Description{}, deferral(ReasonUpgradeInProgress)
	}
	generation, err := p.describedGeneration()
	if err != nil {
		return upgradeport.Description{}, err
	}
	rows, err := p.rows(nil, owner, generation, "source")
	if err != nil {
		return upgradeport.Description{}, err
	}
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: newID(), Participants: []upgradetx.Key{h.Key},
		Domains: append([]upgradetx.Domain(nil), registryDomains[:]...), Expected: rows})
	if err != nil {
		return upgradeport.Description{}, deferral(ReasonDomainCensus)
	}
	return upgradeport.Description{Key: h.Key, Nonce: nonce, At: time.Now().UnixNano(), RunningSHA: running,
		SourceRelease: release, Scope: scope}, nil
}

// describedGeneration is the generation that owns today's rows: the settled
// attempt's target after completion, its source after a refusal, and one for
// a runtime that never accepted a transaction. Any unsettled attempt refuses:
// a description is the input of a new grant, and Start would defer it anyway.
func (p *runtimePort) describedGeneration() (uint64, error) {
	view, err := p.host.Journal.Load(p.host.Key, "")
	var typed *upgradetx.Error
	if errors.As(err, &typed) && typed.Code == upgradetx.CodeGrantUnknown {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	a := current(view.Historical)
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
