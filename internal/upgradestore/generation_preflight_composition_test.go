package upgradestore_test

import (
	"context"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Runtime facts remain controlled DTOs, including decoder/config/reservation
// witnesses. The complete production compatibility validator runs at every
// engine gate against the real grant and actual source generation from Store.
// This does not certify production adapter collection or executable decoders.
type generationPreflightRuntime struct {
	*composedRuntime
	source uint64
	checks int
}

func (r *generationPreflightRuntime) Preflight(ctx context.Context, g upgradetx.Grant, receipt upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	proof, err := r.composedRuntime.Preflight(ctx, g, receipt)
	if err != nil {
		return proof, err
	}
	stage := []upgradetx.ActionKind{upgradetx.ActionSelect, upgradetx.ActionFreeze, upgradetx.ActionDispatch}[r.checks%3]
	r.checks++
	domains := []string{"exec-v1", "upgrade-cut-v1"}
	reader := func(sha, release string) upgradecompat.Reader {
		return upgradecompat.Reader{SHA256: sha, Release: release, Component: g.Handle.Key.Component, Epoch: 1, ReadEpochs: []uint64{1}, Domains: domains,
			Features: []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}}
	}
	c := proof.Census
	p := upgradecompat.Preflight{Grant: g, Stamp: c.Stamp, Stage: stage, Now: time.Now().UnixNano(), Started: r.current().Handle.TxID == g.Handle.TxID,
		Source: reader(g.SourceSHA, g.SourceRelease), Candidate: reader(g.TargetSHA, g.TargetRelease), Previous: reader(g.SourceSHA, g.SourceRelease),
		RecordedPreviousSHA: g.SourceSHA, RequiredDomains: domains, Census: c, ExpectedDigest: c.ExpectedDigest, SourceGeneration: r.source, TargetGeneration: g.Handle.Generation,
		Checks: make(map[string]upgradecompat.Evidence), Fleet: upgradecompat.FleetCensus{Stamp: c.Stamp, Known: true, Complete: true, Reserved: true,
			Expected: []string{"component"}, Participants: []upgradecompat.Participant{{ID: "component", Component: g.Handle.Key.Component, Known: true, Release: g.SourceRelease}}}}
	for _, name := range upgradecompat.RequiredChecks() {
		p.Checks[name] = upgradecompat.Evidence{Stamp: p.Stamp, Stage: p.Stage, Known: true, Complete: true}
	}
	return proof, upgradecompat.CheckPreflight(p)
}
