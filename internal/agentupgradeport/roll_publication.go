package agentupgradeport

import (
	"context"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// RollRegistration reads the agent's own configured installation and lineage.
// Neither a caller's pathname nor a broker's installation can publish its cut.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1 / §5.2
func RollRegistration(runtime upgradeport.RuntimePort) (upgradetx.Owner, upgradelock.Registration, error) {
	p, ok := runtime.(*runtimePort)
	if !ok || p.host.Owner == nil {
		return upgradetx.Owner{}, upgradelock.Registration{}, reject("agent roll runtime unavailable")
	}
	owner, err := p.host.Owner()
	if err != nil {
		return owner, upgradelock.Registration{}, err
	}
	reg, err := upgradelock.RegistrationFor(p.host.Key)
	return owner, reg, err
}

// DescribeRoll includes a real canonical-candidate reader probe for the fixed
// whole-host plan. Ordinary agent descriptions keep their established contract.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §6.1
func DescribeRoll(runtime upgradeport.RuntimePort, nonce string) (upgradeport.Description, error) {
	p, ok := runtime.(*runtimePort)
	if !ok || p.host.Probe == nil {
		return upgradeport.Description{}, reject("agent roll reader unavailable")
	}
	d, err := p.DescribeRuntime(nonce)
	if err != nil {
		return d, err
	}
	_, reg, err := RollRegistration(runtime)
	if err != nil {
		return d, err
	}
	release, caps, err := p.host.Probe(reg.CanonicalPath)
	if err != nil {
		return d, err
	}
	d.Candidate = &upgradetx.CandidateDescription{SHA: caps.SHA256, Release: release, CapabilityDigest: upgradestage.CapabilityDigest(upgradetx.ComponentAgent, caps)}
	return d, nil
}

// RollPublisher supplies the existing publication protocol with local agent
// facts. It writes no journal and borrows only the original parent's OFD.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.2
func RollPublisher(runtime upgradeport.RuntimePort) upgradeartifact.PublicationPort {
	p, _ := runtime.(*runtimePort)
	return &rollPublisher{runtime: p}
}

type rollPublisher struct {
	runtime *runtimePort
	grant   upgradetx.Grant
}

func (p *rollPublisher) Authorize(ctx context.Context, g upgradetx.Grant) (upgradeartifact.Location, error) {
	if err := ctx.Err(); err != nil {
		return upgradeartifact.Location{}, err
	}
	if p.runtime == nil || g.Handle.Key != p.runtime.host.Key || g.ExpiresAt <= time.Now().UnixNano() {
		return upgradeartifact.Location{}, reject("agent publication authorization unavailable")
	}
	owner, reg, err := RollRegistration(p.runtime)
	if err != nil {
		return upgradeartifact.Location{}, err
	}
	running, err := p.runtime.host.RunningSHA()
	if err != nil || owner != g.Issuer || running != g.SourceSHA {
		return upgradeartifact.Location{}, reject("agent publication source changed")
	}
	p.grant = g
	return upgradeartifact.Location{CanonicalPath: reg.CanonicalPath, Directory: reg.ControlDir + "/artifacts"}, nil
}
func (p *rollPublisher) Reserve(ctx context.Context, g upgradetx.Grant) (upgradelock.Lease, error) {
	return upgradelock.Acquire(ctx, g.Handle.Key, g.ParentTxID)
}
func (p *rollPublisher) CheckImage(ctx context.Context, path, sha, release, capsDigest string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if capsDigest != p.grant.CapabilityDigest {
		return "", reject("agent publication capability binding changed")
	}
	got, caps, err := p.runtime.host.Probe(path)
	if err != nil || got != release || caps.SHA256 != sha {
		return "", reject("agent published reader changed")
	}
	if sha == p.grant.TargetSHA && upgradestage.CapabilityDigest(upgradetx.ComponentAgent, caps) != capsDigest {
		return "", reject("agent candidate reader changed")
	}
	// Both current-source and candidate readers must cover the sealed domains.
	if err = p.runtime.host.Candidate(caps); err != nil {
		return "", err
	}
	return sha256Hex(struct{ SHA, Release, Caps string }{sha, got, upgradestage.CapabilityDigest(upgradetx.ComponentAgent, caps)}), nil
}
