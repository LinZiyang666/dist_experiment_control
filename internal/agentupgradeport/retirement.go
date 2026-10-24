// Current frozen exports remain the census authority while a reverse retires
// its inherited generation; the live runtime cannot be snapshotted twice.
// origin: hot-update S8 plan §8.1; S8 internal review upgrade-maketest F6
package agentupgradeport

import "github.com/LinZiyang666/tether/internal/upgradetx"

func heldImageMatches(p *runtimePort, held *heldCut, attempt upgradetx.Handle, running string) bool {
	if held == nil {
		return false
	}
	if upgradetx.SameCut(held.handle, attempt) {
		return true
	}
	inherited := p.host.Inherited
	if inherited == nil || !upgradetx.SameCut(inherited.Attempt, attempt) ||
		running != inherited.TargetSHA || held.handle.Key != attempt.Key ||
		attempt.Generation == ^uint64(0) || held.handle.Generation != attempt.Generation+1 {
		return false
	}
	// Only an accepted adjacent reverse of this imported image may lend its
	// current export. An unrelated forward, stale handle or older image cannot
	// certify retirement or resurrect the pristine input.
	view, ok := p.record(held.handle)
	a := current(view.Historical)
	return ok && a.Direction == upgradetx.DirectionReverse &&
		a.SourceGeneration == attempt.Generation && a.SourceSHA == running && a.TargetSHA == inherited.SourceSHA
}

// A continuing reverse shares the forward's Record; current(record) is already
// the reverse even though this process still executes the imported forward.
// Retire that inherited cut from the held CURRENT export, under its original
// scope commitment. Neither the reverse's scope nor its not-yet-running target
// can stand in for the source generation's independent release proof.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §8.1
func retireInheritedFromReverse(p *runtimePort, handle upgradetx.Handle) (upgradetx.Receipt, error) {
	fail := func() (upgradetx.Receipt, error) {
		return upgradetx.Receipt{}, refusal(upgradetx.CodeUnsettled, "old generation release is unproven", true)
	}
	h := p.host
	if h.Inherited == nil || h.Owner == nil || h.RunningSHA == nil || h.Journal == nil || !upgradetx.SameCut(h.Inherited.Attempt, handle) {
		return fail()
	}
	view, err := h.Journal.Load(h.Key, handle.TxID)
	if err != nil || !upgradetx.SameCut(view.Historical.Forward.Handle, handle) {
		return fail()
	}
	running, err := h.RunningSHA()
	if err != nil || running != h.Inherited.TargetSHA || running != view.Historical.Forward.TargetSHA {
		return fail()
	}
	owner, err := h.Owner()
	if err != nil || owner != h.Inherited.Owner {
		return fail()
	}
	p.mu.Lock()
	held := p.held
	p.mu.Unlock()
	if !heldImageMatches(p, held, handle, running) || !upgradetx.SameCut(held.handle, view.Historical.Reverse.Handle) {
		return fail()
	}
	// An export still activating, or a reverse the journal has since superseded,
	// cannot certify the source generation's release either.
	// origin: S8 internal review activation-and-route-migration F10 counterexamples
	if held.activating {
		return fail()
	}
	latest, err := h.Journal.Load(h.Key, "")
	if err != nil || !upgradetx.SameCut(current(latest.Historical).Handle, held.handle) {
		return fail()
	}
	bundle := held.image.Bundle()
	rows, err := p.rows(&bundle, owner, handle.Generation, "target")
	if err != nil {
		return fail()
	}
	scope := view.Historical.Forward.Scope
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		return fail()
	}
	expected, err := upgradetx.ExpectedDigest(scope)
	if err != nil {
		return fail()
	}
	stamp := p.stamp(handle, owner, newID(), digest, 0)
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: rows, ExpectedDigest: expected}
	if !upgradetx.CensusCovers(scope, census) {
		return fail()
	}
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: stamp, Known: true, Complete: true,
		RunningSHA: running, Census: census, EvidenceDigest: sha256Hex(census)}, nil
}
