package clusterroll

import (
	"context"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// PrepareCluster snapshots each requested broker from its authorized describe
// route, obtains every target-issued grant, then seals the complete ordered
// plan in the coordinator daemon. Cancellation during collection has issued
// only inert authorizations: no publication, lease or cut has run yet.
// origin: hot-update S8 plan §3.1 / §5.1 / §7.1; S8 internal review cluster-roll F4
func PrepareCluster(ctx context.Context, p ClusterPorts, nodes []string, version, sha string) (upgradetx.Grant, upgradetx.Plan, error) {
	if p.Trigger == nil || p.Health == nil || p.ActorFP == "" || len(nodes) == 0 || p.Home == "" {
		return upgradetx.Grant{}, upgradetx.Plan{}, rejected("cluster roll requires authenticated ports and a complete host list")
	}
	c := &clusterClient{ctx: ctx, ports: p}
	parts, err := c.participants(ctx, nodes, version, sha)
	if err != nil {
		return c.parent, c.plan, err
	}
	targets := make([]upgradecli.Target, len(parts))
	candidates := make([]upgradecli.Candidate, len(parts))
	scopeID, err := randomHex()
	if err != nil {
		return c.parent, c.plan, err
	}
	scope := upgradetx.Scope{ID: scopeID}
	for i, p := range parts {
		targets[i], candidates[i] = p.target, p.candidate
		scope.Participants = append(scope.Participants, p.target.Key)
		if i == 0 {
			scope.Domains = p.target.Scope.Domains
		}
		for _, row := range p.target.Scope.Expected {
			scope.Expected = append(scope.Expected, upgraderoll.ParentResource(p.target.Key, row))
		}
	}
	allSame := true
	for i, target := range targets {
		if target.SourceSHA != candidates[i].SHA {
			allSame = false
		}
	}
	if allSame {
		for _, node := range nodes {
			if _, err := c.call(ctx, node, rollcontrol.Request{Op: "roll.check_idle"}); err != nil {
				return c.parent, c.plan, err
			}
		}
		if p.Output != nil {
			_, _ = fmt.Fprintln(p.Output, "all requested broker images are running; fresh target descriptions contain no pending cut")
		}
		return upgradetx.Grant{}, upgradetx.Plan{}, nil
	}
	scope, err = upgradetx.CanonicalScope(scope)
	if err != nil {
		return c.parent, c.plan, err
	}
	req, err := rollPrepare(parts[0], p.ActorFP)
	if err != nil {
		return c.parent, c.plan, err
	}
	req.Scope = scope
	// This is the initial parent issuance, never a renewal of accepted rights.
	// Child grants retain exactly this same absolute deadline across resume.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1
	req.ExpiresAt = time.Now().Add(upgradebudget.FullStackRollout).UnixNano()
	reply, err := c.call(ctx, nodes[0], rollcontrol.Request{Op: "roll.prepare_parent", Prepare: req})
	if err != nil {
		return c.parent, c.plan, err
	}
	c.parent = reply.Grant
	if c.parent.Handle.Key != clusterParentKey(targets[0].Key, nodes[0]) || c.parent.ActorFP != p.ActorFP || c.parent.ScopeDigest != upgraderoll.ScopeDigest(scope) || c.parent.ExpiresAt != req.ExpiresAt {
		return c.parent, c.plan, unsettled("parent authorization differs from its snapshot")
	}
	if err := upgradecli.RememberGrant(p.Home, c.parent); err != nil {
		return c.parent, c.plan, err
	}
	if p.Output != nil {
		_, _ = fmt.Fprintf(p.Output, "parent transaction: %s (resume with --resume %s)\n", c.parent.Handle.TxID, c.parent.Handle.TxID)
	}
	reply, err = c.call(ctx, nodes[0], rollcontrol.Request{Op: "roll.accept_parent"})
	if err != nil {
		return c.parent, c.plan, err
	}
	c.plan = upgradetx.Plan{Parent: reply.View.Historical.Forward.Handle, Scope: scope, SourceManifestDigest: rollcontrol.Hash(targets), TargetManifestDigest: rollcontrol.Hash(candidates), Deadline: c.parent.ExpiresAt}
	if err = c.prepareParticipants(ctx, parts); err != nil {
		return c.parent, c.plan, err
	}
	if _, err = (rollJournal{client: c}).SealPlan(c.plan); err != nil {
		return c.parent, c.plan, err
	}
	return c.parent, c.plan, nil
}

func RunCluster(ctx context.Context, p ClusterPorts, parent upgradetx.Grant, plan upgradetx.Plan) (upgradetx.View, error) {
	var cancel context.CancelFunc
	if time.Now().UnixNano() < plan.Deadline {
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, plan.Deadline))
	} else {
		ctx, cancel = context.WithTimeout(ctx, upgradebudget.RequestLatency)
	}
	defer cancel()
	journal, endpoint, err := ClusterTransport(ctx, p, parent, plan)
	if err != nil {
		return upgradetx.View{}, err
	}
	return upgraderoll.Run(ctx, journal, endpoint, plan)
}

// ResumeCluster loads only the original daemon plan by the locally recorded
// parent grant. It does not describe a roster, prepare a child, or accept any
// new version, SHA, host, deadline or host-slot identity.
func ResumeCluster(ctx context.Context, p ClusterPorts, parentTx string) (upgradetx.View, error) {
	parent, err := upgradecli.RecallGrant(p.Home, parentTx)
	if err != nil {
		return upgradetx.View{}, err
	}
	if parent.ActorFP != p.ActorFP {
		return upgradetx.View{}, rejected("parent belongs to another actor")
	}
	c := &clusterClient{ctx: ctx, ports: p, parent: parent}
	plan, err := (rollJournal{client: c}).LoadPlan(parent.Handle)
	if err != nil {
		return upgradetx.View{}, err
	}
	return RunCluster(ctx, p, parent, plan)
}

func clusterParentKey(key upgradetx.Key, nodeID string) upgradetx.Key {
	key.ConfiguredNID = nodeID + "@roll"
	return key
}
