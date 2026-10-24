package clusterroll

import (
	"context"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradehost"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

type rollParticipant struct {
	target    upgradecli.Target
	candidate upgradecli.Candidate
	host      string
}

// Agent calls use the original owner-admitted AgentForwarder route. The broker
// stamps the actor and signs fresh samples before the parent writer sees them.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §7.2
func (c *clusterClient) agentCall(ctx context.Context, key upgradetx.Key, r rollcontrol.Request) (rollcontrol.Response, error) {
	if c.ports.Agent == nil {
		return rollcontrol.Response{}, rejected("agent roll route unavailable")
	}
	r.Parent = c.parent
	if r.Parent.ActorFP == "" {
		r.Parent.ActorFP = c.ports.ActorFP
	}
	r.Key = key
	raw, err := json.Marshal(r)
	if err != nil {
		return rollcontrol.Response{}, err
	}
	raw, err = c.ports.Agent(ctx, key, raw)
	if err != nil {
		return rollcontrol.Response{}, unsettled("original agent roll reply unavailable")
	}
	var out rollcontrol.Response
	if err = upgradewire.StrictDecode(raw, &out); err != nil {
		return out, unsettled("invalid original agent roll reply")
	}
	if out.Code != "" {
		return out, &upgradetx.Error{Code: out.Code, Detail: out.Detail, Accepted: out.Accepted}
	}
	return out, nil
}
func (c *clusterClient) stepCall(ctx context.Context, s upgradetx.Step, r rollcontrol.Request) (rollcontrol.Response, error) {
	if s.Participant.Component == upgradetx.ComponentAgent {
		r.Route = route(s.Grant)
		return c.agentCall(ctx, s.Participant, r)
	}
	return c.call(ctx, route(s.Grant), r)
}
func (c *clusterClient) componentKey(key upgradetx.Key) upgradecli.Remote {
	if key.Component != upgradetx.ComponentAgent {
		return c.component(c.routeKey(key))
	}
	return upgradecli.Remote{Exchange: func(ctx context.Context, r upgradewire.Request) (upgradewire.Response, error) {
		raw, err := upgradewire.EncodeRequest(r)
		if err != nil {
			return upgradewire.Response{}, &upgradecli.NotSentError{Err: err}
		}
		raw, err = c.ports.Agent(ctx, key, raw)
		if err != nil {
			return upgradewire.Response{}, err
		}
		return upgradewire.DecodeResponse(raw)
	}}
}
func (c *clusterClient) participants(ctx context.Context, nodes []string, version, sha string) (out []rollParticipant, err error) {
	seen := map[string]bool{}
	var health map[string]proto.ClusterHealthResp
	for _, node := range nodes {
		if seen[node] {
			return nil, rejected("duplicate cluster participant")
		}
		seen[node] = true
		target, candidate, e := c.component(node).DescribeTarget(ctx, upgradetx.Key{Component: upgradetx.ComponentBroker})
		if e != nil {
			return nil, e
		}
		if candidate == nil || candidate.SHA != sha || !proto.SameRelease(candidate.Release, version) {
			return nil, rejected("staged candidate does not bind the requested host, SHA and release")
		}
		skip, agent := false, false
		if c.ports.Requested != nil {
			found := false
			for _, s := range c.ports.Requested.Steps {
				if s.NodeID == node {
					found = true
					skip = s.Kind == clusterupgrade.StepSkip
					agent = c.ports.Requested.Scope != clusterupgrade.ScopeBrokerDaemon && !s.BrokerOnly
					if s.Kind != clusterupgrade.StepSkip && s.Kind != clusterupgrade.StepUpgrade {
						return nil, rejected("unknown requested cluster step")
					}
					break
				}
			}
			if !found {
				return nil, rejected("requested host disappeared from original plan")
			}
		}
		// Preserve the requested execution scope. A stale release-label skip is a
		// refusal before parent acceptance, never permission for an extra cut.
		// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2 / IL14
		if skip && target.SourceSHA != sha {
			return nil, rejected("saved skip participant is not running the target image")
		}
		out = append(out, rollParticipant{target, upgradecli.Candidate{SHA: candidate.SHA, Release: candidate.Release, CapabilityDigest: candidate.CapabilityDigest}, node})
		if !agent {
			continue
		}
		if c.ports.Agent == nil || c.ports.SID == "" {
			return nil, rejected("whole-host roll requires its authenticated agent route")
		}
		if health == nil {
			rows, e := c.ports.Health(ctx)
			if e != nil {
				return nil, e
			}
			health = map[string]proto.ClusterHealthResp{}
			for _, row := range rows {
				health[row.NodeID] = row
			}
		}
		row, ok := health[node]
		if !ok {
			return nil, unsettled("original host configuration is unavailable")
		}
		nid := row.ColocatedAgentNID
		if nid == "" {
			nid = node
		}
		nonce, e := randomHex()
		if e != nil {
			return nil, e
		}
		selector := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: c.ports.SID, ConfiguredNID: nid}
		response, e := c.agentCall(ctx, selector, rollcontrol.Request{Op: "roll.describe", Nonce: nonce})
		if e != nil {
			return nil, e
		}
		d := response.Description
		if d == nil || d.Candidate == nil || d.Candidate.SHA != sha || !proto.SameRelease(d.Candidate.Release, version) {
			return nil, rejected("agent canonical candidate differs from requested image")
		}
		plain := *d
		plain.Candidate = nil
		if e = upgradewire.ValidateDescription(plain, selector, nonce); e != nil {
			return nil, e
		}
		if skip && d.RunningSHA != sha {
			return nil, rejected("saved agent skip participant is not running the target image")
		}
		out = append(out, rollParticipant{upgradecli.Target{Key: d.Key, SourceSHA: d.RunningSHA, SourceRelease: d.SourceRelease, Scope: d.Scope}, upgradecli.Candidate{SHA: d.Candidate.SHA, Release: d.Candidate.Release, CapabilityDigest: d.Candidate.CapabilityDigest}, node})
	}
	return out, nil
}

// Equal bytes grant observation authority only. Standalone BuildPrepare keeps
// its existing "already running" refusal, and both engines reject such Start.
func rollPrepare(p rollParticipant, actor string) (upgradetx.PrepareRequest, error) {
	if p.target.SourceSHA != p.candidate.SHA {
		return upgradecli.BuildPrepare(p.target, p.candidate, actor, time.Now())
	}
	nonce, err := randomHex()
	if err != nil {
		return upgradetx.PrepareRequest{}, err
	}
	return upgradetx.PrepareRequest{ClientNonce: nonce, ActorFP: actor, Mode: upgradetx.ModeHot, SourceRelease: p.target.SourceRelease, TargetRelease: p.candidate.Release, SourceSHA: p.target.SourceSHA, TargetSHA: p.candidate.SHA, Scope: p.target.Scope, CapabilityDigest: p.candidate.CapabilityDigest, ExpiresAt: time.Now().Add(upgradebudget.ComponentHandover).UnixNano()}, nil
}
func (c *clusterClient) prepareParticipants(ctx context.Context, parts []rollParticipant) error {
	var releases []uint32
	var previous uint32
	for i := 0; i < len(parts); {
		end := i + 1
		for end < len(parts) && parts[end].host == parts[i].host {
			end++
		}
		var cuts []uint32
		for j := i; j < end; j++ {
			p := parts[j]
			base, err := rollPrepare(p, c.ports.ActorFP)
			if err != nil {
				return err
			}
			base.ParentTxID, base.ParentSlot, base.ExpiresAt = c.parent.Handle.TxID, upgradehost.RollSlot(c.parent, p.target.Key, p.host), c.parent.ExpiresAt
			step := upgradetx.Step{Participant: p.target.Key, Grant: upgradetx.Grant{ParentSlot: base.ParentSlot}}
			if _, err = c.stepCall(ctx, step, rollcontrol.Request{Op: "roll.bind", Scope: c.plan.Scope, Route: p.host}); err != nil {
				return err
			}
			kind := upgradetx.StepBrokerCut
			if p.target.Key.Component == upgradetx.ComponentAgent {
				kind = upgradetx.StepAgentCut
			}
			deps := append([]uint32(nil), cuts...)
			if previous != 0 && len(deps) == 0 {
				deps = []uint32{previous}
			}
			if err = c.appendPrepared(ctx, step, base, kind, deps); err != nil {
				return err
			}
			cuts = append(cuts, uint32(len(c.plan.Steps)))
		}
		domains := map[string]bool{}
		// Agent/domain releases precede the broker host's final home/release, so
		// its reservation protects every original co-located child until settled.
		// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
		for j := end - 1; j >= i; j-- {
			p := parts[j]
			if domains[p.target.Key.DomainID] {
				continue
			}
			domains[p.target.Key.DomainID] = true
			base, err := rollPrepare(p, c.ports.ActorFP)
			if err != nil {
				return err
			}
			base.ParentTxID, base.ParentSlot, base.ExpiresAt = c.parent.Handle.TxID, upgradehost.RollSlot(c.parent, p.target.Key, p.host), c.parent.ExpiresAt
			step := upgradetx.Step{Participant: p.target.Key, Grant: upgradetx.Grant{ParentSlot: base.ParentSlot}}
			deps := append([]uint32(nil), cuts...)
			if previous > cuts[len(cuts)-1] {
				deps = append(deps, previous)
			}
			for _, kind := range []upgradetx.StepKind{upgradetx.StepHomeConverge, upgradetx.StepRelease} {
				if err = c.appendPrepared(ctx, step, base, kind, deps); err != nil {
					return err
				}
				deps = []uint32{uint32(len(c.plan.Steps))}
			}
			previous = uint32(len(c.plan.Steps))
			releases = append(releases, previous)
		}
		i = end
	}
	final, err := rollPrepare(parts[0], c.ports.ActorFP)
	if err != nil {
		return err
	}
	final.ParentTxID, final.ParentSlot, final.ExpiresAt = c.parent.Handle.TxID, upgradehost.RollSlot(c.parent, parts[0].target.Key, parts[0].host), c.parent.ExpiresAt
	reply, err := c.call(ctx, c.coordinator(), rollcontrol.Request{Op: "roll.prepare_step", Key: c.parent.Handle.Key, Prepare: final})
	if err != nil {
		return err
	}
	c.plan.Steps = append(c.plan.Steps, upgradetx.Step{Ordinal: uint32(len(c.plan.Steps) + 1), Participant: c.parent.Handle.Key, Grant: reply.Grant, Kind: upgradetx.StepRelease, DependsOn: releases})
	return upgraderoll.ValidatePlan(c.plan)
}
func (c *clusterClient) appendPrepared(ctx context.Context, step upgradetx.Step, base upgradetx.PrepareRequest, kind upgradetx.StepKind, deps []uint32) error {
	var err error
	base.ClientNonce, err = randomHex()
	if err != nil {
		return err
	}
	reply, err := c.stepCall(ctx, step, rollcontrol.Request{Op: "roll.prepare_step", Key: step.Participant, Prepare: base})
	if err != nil {
		return err
	}
	step.Ordinal, step.Grant, step.Kind, step.DependsOn = uint32(len(c.plan.Steps)+1), reply.Grant, kind, deps
	c.plan.Steps = append(c.plan.Steps, step)
	return nil
}
