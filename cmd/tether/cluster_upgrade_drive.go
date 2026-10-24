package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecli/clusterroll"
	"github.com/LinZiyang666/tether/internal/upgradecli/legacy"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_upgrade_drive.go — G5 #13/#14 W4: the execute loop. One host at a time, transfer-leader-first,
// signed reload, converge-barrier wait, canary command-version skew check. HALTS (never de-clusters) on
// any refusal; the roll is idempotent-resumable (re-run skips hosts already at target).
// S8 (plan §4.2 / IL14): "already at target" needs running-image proof — a release string never skips a
// broker or an agent leg. The executor helpers moved verbatim to internal/upgradecli/legacy/roll_execute.go; the
// S8 parent driver (--resume) runs only an original sealed plan through upgraderoll.Run.

// cli.RollBrokerStep owns these broker-step guarantees.
// Each broker step uses the roll's deadline and cut identity while it retains the lock.
// origin: hot-update S6 plan §5 / T38 / T45
// origin: hot-update S6 plan §5 / T38 / T45
// TransferTo is a plan-time hint. The CLI helper reads the live leader
// for this step and observes running bytes after a single unsafe reload.

// Whole-host scope includes the agent leg without re-deriving plan-time presence.
// origin: hot-update S6 plan §5 / T45
func driveAgentUpgradeStep(ctx context.Context, nc *nats.Conn, actor, sid string, seed []byte, s clusterupgrade.Step, scope clusterupgrade.PlanScope, signedScope, toVersion, expectSHA, holder string, out io.Writer) error {
	// Stage-C B1: a WHOLE-HOST upgrade needs the co-located AGENT re-exec'd too (the broker daemon is
	// only half the host). Do this even when the broker was AlreadyAtVersion (its agent may be stale);
	// skip only when the agent is ALREADY at target (idempotent — re-run does not re-restart agents).
	//
	// P3: unless this host HAS no agent. The decision is the PLAN's (Step.BrokerOnly), not re-derived
	// here, so the executor can never disagree with what the operator was shown. Re-deriving it live
	// would also let a mid-roll registration flap silently flip a host between whole-host and
	// broker-only halfway through its own step.
	if scope == clusterupgrade.ScopeBrokerDaemon || s.BrokerOnly {
		_, _ = fmt.Fprintf(out, "  (broker-only host %s — agent leg skipped)\n", s.NodeID)
		// SameRelease — origin: prerelease audit round 2, K-F3. waitAgentVersion 200
		// lines below was converted and this was not, so the comment above claiming the
		// re-run is idempotent was false for a v-prefixed --to-version: the skip never
		// fired and every re-run re-execed every agent.
		//
		// origin: hot-update S8 plan §4.2 / IL14 / §11.2 P19 ("skip by release" mutant).
		// The release-equality skip that stood here is deleted: an agent reporting the
		// target release has proven nothing about its running bytes, control readiness
		// or retained resources, so the agent leg always runs and proves itself (the
		// shared legacy policy decides whether this old route may act at all).
	} else {
		_, _ = fmt.Fprintf(out, "→ re-exec %s's co-located agent into %s\n", s.NodeID, toVersion)
		aresp, aerr := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "reexec-agent", TargetNode: s.NodeID, SID: sid, ExpectSHA256: expectSHA, Holder: holder, Scope: signedScope})
		if aerr != nil {
			return aerr
		}
		if !aresp.OK {
			return fmt.Errorf("agent re-exec refused: %s %s", aresp.Code, aresp.Error)
		}
		if err := waitAgentVersion(ctx, nc, actor, sid, s.NodeID, toVersion); err != nil {
			return err
		}
	}
	return nil
}

// upgradeLeaseRenewInterval / growLeaseRenewInterval are vars, not consts, ONLY so hermetic tests can
// compress the 5-minute production cadence into microseconds. Production never reassigns them; both are
// cluster.LockLeaseRenewInterval, whose derivation lives in internal/cluster/lock_lease.go.
var (
	upgradeLeaseRenewInterval = cluster.LockLeaseRenewInterval
	growLeaseRenewInterval    = cluster.LockLeaseRenewInterval
)

// renewUpgradeLease / currentLeader keep their names for cluster add/unlock and
// their tests; the bodies and rationale moved verbatim to internal/upgradecli/legacy
// (RenewRollLease, RollLeader). ReleaseRollLock is owned directly by that leaf.
func renewUpgradeLease(ctx context.Context, nc *nats.Conn, actor string, seed []byte, holders ...string) (bool, error) {
	return legacy.RenewRollLease(ctx, upgradeRollIO(nc, actor, seed), holders...)
}

func currentLeader(ctx context.Context, nc *nats.Conn, actor string) (string, error) {
	return legacy.RollLeader(ctx, upgradeRollIO(nc, actor, nil))
}

// upgradeRollIO binds the signed trigger, the health probes and the exit
// constructors the moved executor helpers need.
func upgradeRollIO(nc *nats.Conn, actor string, seed []byte) legacy.RollIO {
	return legacy.RollIO{
		Trigger: func(ctx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			return sendUpgradeTrigger(ctx, nc, actor, seed, req)
		},
		TriggerOnce: func(ctx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
			return sendUpgradeTriggerOnce(ctx, nc, actor, seed, req)
		},
		Health: func(ctx context.Context) ([]proto.ClusterHealthResp, error) {
			return cli.QueryBrokerRollHealth(ctx, nc, actor)
		},
		Probe:       func() ([]proto.ClusterHealthResp, error) { return probeClusterHealth(nc, actor) },
		Request:     natsRequest(nc),
		Unavailable: unavailErr, LockNotHeldCode: clusterLockNotHeldCode, Poll: upgradeConvergePoll,
		Transient: func(err error) error { return &ExitError{Class: exitTransient, Err: err} },
	}
}

// "fix and re-run to resume" is a PROMISE, and round 2 (H-2) found it had been
// false since the acquire started refusing a marker it does not own. It is
// true again — driveUpgrade hands the lock's lease back on every exit path
// that did not release it outright — so the wording stays.
// Preserve a transfer-busy deferral's exit class through the HALT wrapper.
// A store/census read error must remain distinct from a retry-later refusal.
// origin: hot-update S6 plan §9.1 / T45

// agentVersionOf resolves brokerNode's co-located agent's running release (legacy.ColocatedAgentVersion).
func agentVersionOf(ctx context.Context, nc *nats.Conn, actor, sid, brokerNode string) (string, bool) {
	return legacy.ColocatedAgentVersion(ctx, upgradeRollIO(nc, actor, nil), actor, sid, brokerNode)
}

// waitAgentVersion polls until brokerNode's co-located agent re-registers at the target version.
func waitAgentVersion(ctx context.Context, nc *nats.Conn, actor, sid, brokerNode, version string) error {
	return pollUntil(ctx, upgradeConvergeTimeout, func() bool {
		v, ok := agentVersionOf(ctx, nc, actor, sid, brokerNode)
		// proto.SameRelease: the two ends disagree on the leading "v" (goreleaser stamps it, a source build does not), and this is a CONFIRMATION check — a false negative reports a successful upgrade as unconfirmed (prerelease audit DRD-F2/F3).
		return ok && proto.SameRelease(v, version)
	}, fmt.Sprintf("%s's co-located agent did not re-register at %s within %s", brokerNode, version, upgradeConvergeTimeout))
}

// Home convergence uses the single wire literal in legacy.HomesConvergedOp,
// mirroring internal/broker's upgradeOpHomesConverged for the final roll barrier.

// notifyUpgrade POSTs a best-effort JSON milestone to the webhook (no-op if empty). Failures are ignored
// — the roll's correctness never depends on the webhook.
func notifyUpgrade(webhook, event string, fields map[string]any) {
	notifyMilestone(webhook, "cluster_upgrade_"+event, fields)
}

// notifyMilestone is shared by the upgrade and grow orchestrators (body in legacy.NotifyMilestone).
var notifyMilestone = legacy.NotifyMilestone

// Execution and --resume share the same signed ports. The home holds only
// target-issued grants; the original plan and every result live at the daemon.
// origin: hot-update S8 plan §3.1 / §7.1 / P15 / P19
func clusterTransactionPorts(cmd *cobra.Command, nc *nats.Conn, actor string, seed []byte, home string) (clusterroll.ClusterPorts, error) {
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return clusterroll.ClusterPorts{}, err
	}
	return clusterroll.ClusterPorts{Trigger: func(ctx context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		return sendUpgradeTriggerOnce(ctx, nc, actor, seed, r)
	}, Health: func(ctx context.Context) ([]proto.ClusterHealthResp, error) {
		return cli.QueryBrokerRollHealth(ctx, nc, actor)
	}, Agent: clusterroll.AgentExchange(nc, actor, seed), SID: cli.ReadCurrentSession(home), ActorFP: fp, Home: home, Output: cmd.OutOrStdout()}, nil
}
func driveUpgrade(cmd *cobra.Command, nc *nats.Conn, actor string, seed []byte, plan clusterupgrade.Plan, toVersion, expectSHA, webhook string) error {
	home, err := cmd.Flags().GetString("home")
	if err != nil {
		return usageErr("cluster roll requires its command-owned home")
	}
	ports, err := clusterTransactionPorts(cmd, nc, actor, seed, home)
	if err != nil {
		return err
	}
	ports.Requested = &plan
	var nodes []string
	for _, step := range plan.Steps {
		if plan.Scope != clusterupgrade.ScopeBrokerDaemon && step.BrokerOnly {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  (broker-only host %s — agent leg skipped)\n", step.NodeID)
		}
		nodes = append(nodes, step.NodeID)
	}
	// The immutable plan already names every broker. Finish a health sample when
	// all of them answer; rediscovery's full timeout on every keeper/topology
	// probe otherwise consumes the original parent's deadline between cuts.
	// origin: hot-update S8 plan §3.1 / §6.3; S8 internal review upgrade-maketest-2 F2 F4
	ports.Health = func(ctx context.Context) ([]proto.ClusterHealthResp, error) {
		return cli.QueryBrokerRollHealthForVoters(ctx, nc, actor, nodes)
	}
	parent, sealed, err := clusterroll.PrepareCluster(cmd.Context(), ports, nodes, toVersion, expectSHA)
	if err == nil && parent.Handle.TxID != "" {
		_, err = clusterroll.RunCluster(cmd.Context(), ports, parent, sealed)
	}
	if err != nil {
		notifyUpgrade(webhook, "halt", map[string]any{"parent_tx_id": parent.Handle.TxID})
		return upgradeExit("cluster upgrade", err)
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "rolling upgrade complete.")
	notifyUpgrade(webhook, "complete", map[string]any{"parent_tx_id": parent.Handle.TxID})
	return nil
}
func resumeClusterUpgrade(cmd *cobra.Command, home, url, seedPath, parent string) (upgradetx.View, error) {
	if seedPath == "" {
		return upgradetx.View{}, usageErr("--account-seed is required to resume the original signed cluster roll")
	}
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		return upgradetx.View{}, err
	}
	if _, err = auth.PublicKeyFromSeed(seed); err != nil {
		return upgradetx.View{}, err
	}
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return upgradetx.View{}, err
	}
	url = cli.ResolveNATSURLFromHome(url, cmd.Flags().Changed("nats-url"), home)
	sid := cli.ReadCurrentSession(home)
	nc, err := connectCtl(cmd, "cluster upgrade --resume", home, url, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return upgradetx.View{}, err
	}
	defer nc.Close()
	ports, err := clusterTransactionPorts(cmd, nc, id.PublicKey, seed, home)
	if err != nil {
		return upgradetx.View{}, err
	}
	return clusterroll.ResumeCluster(cmd.Context(), ports, parent)
}
