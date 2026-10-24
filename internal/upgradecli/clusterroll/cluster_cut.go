package clusterroll

import (
	"context"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The established S6 topology/maintenance loop surrounds the authorized S8
// Start. Its reload callback is an adapter to the original target-issued cut;
// no legacy reload reaches the wire. Both S6 serving-image evidence and the
// subsequent S8 three-receipt query must complete before the parent advances.
// origin: hot-update S8 plan §4.2 / §5.1; S8 internal review cluster-roll F1 F9
func (c *clusterClient) startBroker(ctx context.Context, g upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradetx.View, error) {
	// Warm the verified candidate reader while business still runs. The
	// fixed eight-second maintenance hold is obtained only afterwards; every
	// Start gate still rechecks source, artifacts, census and cluster role.
	// origin: S8 internal review cluster-roll F9
	for _, step := range c.plan.Steps {
		if step.Grant == g {
			if _, err := c.call(ctx, route(g), rollcontrol.Request{Op: "roll.preflight", Plan: c.plan, Step: step}); err != nil {
				return upgradetx.View{}, err
			}
			break
		}
	}
	health, err := c.brokerHealth(ctx)
	if err != nil {
		return upgradetx.View{}, err
	}
	var voters []string
	for _, row := range health {
		if row.IsVoter {
			voters = append(voters, row.NodeID)
		}
	}
	var startView upgradetx.View
	var startError error
	driver := cli.ClusterRollIO{Health: c.brokerHealth, Trigger: func(ctx context.Context, request *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		if request.Op != "reload" {
			return c.ports.Trigger(ctx, request)
		}
		var e error
		startView, e = c.component(route(g)).Start(ctx, g, artifact)
		var accepted *upgradetx.Error
		if e != nil && (!errors.As(e, &accepted) || !accepted.Accepted || accepted.Code != upgradetx.CodeUnsettled || accepted.Detail != "broker_activation_dispatched") {
			// The S6 refusal wrapper has no Accepted bit. Retain the actual
			// transaction error so the parent queries this same accepted child
			// after an arming/transport race instead of abandoning it as untyped.
			// origin: hot-update S8 plan §3.1; S8 internal review ROLL-SETTLEMENT F3
			if accepted != nil && accepted.Accepted {
				startError = e
			}
			if accepted != nil && (accepted.Code != upgradetx.CodeUnsettled || accepted.Detail == "broker store cut was not armed") {
				return &proto.ClusterUpgradeResp{Code: accepted.Code, Error: accepted.Detail}, nil
			}
			return nil, e
		}
		next := uint64(0)
		for _, row := range health {
			if row.NodeID == route(g) && row.BrokerImage != nil {
				next = row.BrokerImage.Generation + 1
			}
		}
		return &proto.ClusterUpgradeResp{OK: true, Reloading: true, NextGeneration: next}, nil
	}}
	err = cli.RollBrokerStep(ctx, driver, cli.ClusterRollStep{Step: clusterupgrade.Step{NodeID: route(g)}, Scope: clusterupgrade.ScopeBrokerDaemon, Voters: voters, ToVersion: g.TargetRelease, ExpectSHA256: g.TargetSHA, Holder: c.parent.Handle.TxID, CutID: g.Handle.CutID, PollInterval: 20 * time.Millisecond})
	if err != nil && startError != nil {
		return startView, startError
	}
	return startView, err
}

// A completed image can answer health before its follower role has converged.
// Re-read that observation inside the original parent deadline before the next
// child; repeating a read grants no new authority. Positive membership drift,
// ambiguous identities and transport/refusal errors still reach the S6 driver.
// origin: hot-update S8 plan §3.1 / §5.1; S8 internal review ROLL-SETTLEMENT F3
func (c *clusterClient) brokerHealth(ctx context.Context) ([]proto.ClusterHealthResp, error) {
	for {
		rows, err := c.ports.Health(ctx)
		pending := errors.Is(err, cli.ErrIncompleteBrokerHealth)
		if err == nil {
			seen, leaders := make(map[string]bool), 0
			for _, row := range rows {
				if row.NodeID == "" || seen[row.NodeID] {
					return rows, nil // The topology validator owns malformed identities.
				}
				seen[row.NodeID] = true
				if row.WritableLeaderConfirmed {
					leaders++
				}
				pending = pending || row.IsVoter && row.LeaderContactStale
			}
			pending = pending || leaders == 0
		}
		if !pending {
			return rows, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
