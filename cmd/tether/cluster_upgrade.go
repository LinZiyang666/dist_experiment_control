package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/infracli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradecli/legacy"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_upgrade.go — G5 #13/#14 W4: the ctl-driven rolling broker-upgrade orchestrator. It runs
// EXTERNAL to all brokers (so it survives every restart including the leader's) and drives each host over
// the account-signed remote-trigger subject (W2b): plan the roll (followers-first / leader-last), then per
// host transfer-leader-if-needed → reload → wait for it to re-register at the target. Binary STAGING is
// the privileged precondition (§0.1) — the reload verifies the on-disk sha and refuses a stale image.
//
// v1 scope: the ordered roll + signed triggers + per-host version-converge wait + N=2 write-fence ack +
// dry-run + a canary command-version skew check. The mandatory pre-roll backup + single-active-op lock are
// documented follow-ups (the roll HALTS on any refusal, never de-clusters).

const (
	upgradeTriggerTimeout  = upgradebudget.RequestLatency
	upgradeConvergeTimeout = 3 * time.Minute
	upgradeConvergePoll    = 3 * time.Second
)

func newClusterUpgradeCmd() *cobra.Command {
	var infra infracli.Options
	var (
		natsURL, home, seedPath, toVersion, expectSHA, webhook, resume string
		dryRun, ackWriteFence, backupTaken, brokerOnly                 bool
	)
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Rolling host upgrade or NATS/Caddy component management",
		Example: "  tether cluster upgrade --to-version v0.5.0 --dry-run                          # preview the ordered roll\n" +
			"  tether cluster upgrade --to-version v0.5.0 --expect-sha256 <hex> \\\n" +
			"    --account-seed /etc/tether/secrets/account.nk --backup-taken               # execute (staging + backup done first)",
		Long: "Select --component nats|caddy for infrastructure upgrade, rollback or Caddy config reload.\n" +
			"Use --status <tx> for JSON status or --recover <tx> for the original journal; these need no target version.\n" +
			"Without --component, the existing whole-host roll and --broker-only scope apply.\n\n" +
			"Rolls every broker host to --to-version ONE AT A TIME, transferring leadership off a host\n" +
			"before restarting it, so an N>=3 cluster keeps quorum throughout. The target binary must be\n" +
			"STAGED on each host first (a privileged step — /usr/local/bin/tether is root-owned); the reload\n" +
			"verifies the on-disk sha and refuses a stale image. Requests are signed with the cluster account\n" +
			"seed (--account-seed) and verified by each broker. N=2 fences writes during each restart (inherent\n" +
			"F=0) — pass --ack-writefence to proceed; production should run N>=3.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			infra.ToVersion, infra.ExpectSHA256, infra.Resume = toVersion, expectSHA, resume
			infra.BrokerOnly, infra.AckWriteFence, infra.BackupTaken, infra.DryRun = brokerOnly, ackWriteFence, backupTaken, dryRun
			// origin: hot-update S9 plan §7.1 / T52 / M26
			if infracli.Selected(infra) || cmd.Flags().Changed("status") || cmd.Flags().Changed("recover") {
				return infracli.RunCluster(cmd, infra, home, natsURL, seedPath, infracli.ClusterPorts{Connect: connectCtl, Health: probeClusterHealth, Manifest: fetchManifestOverNATS, Unavailable: unavailErr})
			}
			// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19 — a resume names only the
			// original parent; it accepts no new version, SHA or scope to re-plan with.
			if resume != "" {
				if toVersion != "" || expectSHA != "" || dryRun || brokerOnly {
					return usageErr("--resume continues the original sealed plan and accepts no new target or scope")
				}
				_, err := resumeClusterUpgrade(cmd, home, natsURL, seedPath, resume)
				return upgradeExit("cluster upgrade --resume", err)
			}
			if toVersion == "" {
				return usageErr("--to-version is required (the release every broker host should end on)")
			}
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "cluster upgrade", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			// External-review round2 B3: read the account seed EARLY (when provided) so the upgrade planner can
			// VERIFY the signed roster (signature + expiry) against the account pub, rather than trusting a
			// discovery-cache manifest blindly. It also lets the no-op path (below) send a signed release-lock to
			// self-heal a stale lock. On execute the seed is required (checked later); dry-run may omit it.
			var accountSeed []byte
			var accountPub string
			if seedPath != "" {
				var rerr error
				if accountSeed, rerr = os.ReadFile(seedPath); rerr != nil {
					return unavailErr("read account seed: %v", rerr)
				}
				if accountPub, rerr = auth.PublicKeyFromSeed(accountSeed); rerr != nil {
					return unavailErr("derive account pub from seed: %v", rerr)
				}
			}

			out := cmd.OutOrStdout()
			nodes, err := buildUpgradeNodes(cmd.Context(), nc, id.PublicKey, sid, accountPub, out)
			if err != nil {
				return err
			}
			scope := clusterupgrade.ScopeWholeHost
			if brokerOnly {
				scope = clusterupgrade.ScopeBrokerDaemon
			}
			// origin: hot-update S6 plan §5 / T45
			// Version is not a running-image witness for a same-release rebuild.
			health, err := cli.QueryBrokerRollHealth(cmd.Context(), nc, id.PublicKey)
			if err != nil {
				return err
			}
			plan := cli.PlanClusterRoll(nodes, toVersion, scope, expectSHA, health)
			renderUpgradePlan(out, plan, toVersion)
			if len(plan.Refused) > 0 {
				return unavailErr("cluster upgrade refused (restore HA first): %v", plan.Refused)
			}
			if plan.N2WriteFence && !ackWriteFence {
				return usageErr("N=2: each broker restart fences writes for its restart duration (inherent F=0). " +
					"Pass --ack-writefence to proceed, or grow to N>=3 first.")
			}
			if dryRun {
				_, _ = fmt.Fprintln(out, "(dry-run — no host was touched)")
				return nil
			}
			if seedPath == "" {
				return usageErr("--account-seed <path> is required to execute (the cluster account seed signs each broker trigger)")
			}
			// Stage-C M6: the reload verifies the on-disk binary against ExpectSHA256 — require it so a
			// default roll NEVER re-execs an unverified/unstaged image (inv: no-restart-into-a-bad-binary).
			if expectSHA == "" {
				return usageErr("--expect-sha256 <hex> is required to execute — each broker reload verifies the staged on-disk binary against it and refuses a mismatch")
			}
			// Stage-C M5: a target's first-boot may run a ONE-WAY forward migration; the only recovery is
			// restore-not-reinstall. The orchestrator (over NATS) cannot take the socket-local `cluster
			// backup`, so require the operator to confirm they took one first (documented in the runbook).
			if !backupTaken {
				return usageErr("take a verified `tether cluster backup` on the leader FIRST (a target's first-boot migration is one-way; restore is the only recovery), then re-run with --backup-taken")
			}
			return driveUpgrade(cmd, nc, id.PublicKey, accountSeed, plan, toVersion, expectSHA, webhook)
		},
	}
	cmd.Flags().StringVar(&toVersion, "to-version", "", "the release every broker host should end on (required)")
	cmd.Flags().StringVar(&expectSHA, "expect-sha256", "", "hex sha256 of the STAGED on-disk binary; each reload refuses a mismatch")
	cmd.Flags().StringVar(&seedPath, "account-seed", "", "path to the cluster account nkey seed (signs each broker trigger; required to execute)")
	cmd.Flags().BoolVar(&brokerOnly, "broker-only", false, "upgrade only broker daemons; leave co-located agents running")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the ordered roll plan and exit without touching any host")
	cmd.Flags().BoolVar(&ackWriteFence, "ack-writefence", false, "acknowledge the inherent N=2 write-fence and proceed")
	cmd.Flags().BoolVar(&backupTaken, "backup-taken", false, "confirm a verified `cluster backup` was taken first (a target's first-boot migration is one-way — restore is the only recovery)")
	cmd.Flags().StringVar(&webhook, "notify-webhook", "", "POST a JSON milestone to this URL on start / per-host / complete / HALT")
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	infracli.Flags(cmd, &infra, true)
	cmd.Flags().StringVar(&resume, "resume", "", "continue the original sealed S8 parent plan by its parent transaction ID")
	return cmd
}

// buildUpgradeNodes folds the live cluster-health probe with the node list into
// the planner's []Node; the body and its A7/B1/B3 rationale moved verbatim to
// legacy.BuildLegacyRollNodes (S8 P19 slim move).
func buildUpgradeNodes(ctx context.Context, nc *nats.Conn, actor, sid, accountPub string, out io.Writer) ([]clusterupgrade.Node, error) {
	return legacy.BuildLegacyRollNodes(ctx, legacy.RollPlanIO{Request: natsRequest(nc), Unavailable: unavailErr,
		Probe:    func() ([]proto.ClusterHealthResp, error) { return probeClusterHealth(nc, actor) },
		Manifest: func(ctx context.Context) *proto.ClusterManifest { return fetchManifestOverNATS(ctx, nc, actor) },
	}, actor, sid, accountPub, out)
}

// resolveColocatedAgent is the P3 three-state presence verdict (upgradecli).
var resolveColocatedAgent = legacy.ResolveColocatedAgent

// renderUpgradePlan prints the ordered S6 roll (body in legacy.RenderLegacyRollPlan).
var renderUpgradePlan = legacy.RenderLegacyRollPlan

// signUpgradeTrigger stamps IssuedAt + the account signature onto a request and marshals it.
var signUpgradeTrigger = infracli.SignUpgradeTrigger

// upgradeTriggerCodeNotReady mirrors the broker's retriable "admin backend not wired yet" code.
const upgradeTriggerCodeNotReady = "cluster_not_ready"

// sendUpgradeTrigger signs + publishes a trigger and awaits the target broker's reply, RETRYING the
// retriable cluster_not_ready (External-review M1: a broker that just re-exec'd answers health before its
// admin backend wires — wait it out rather than HALT the roll). Bounded by upgradeConvergeTimeout.
//
// origin: line-2 review M14. cluster_add.go's sendGrowTrigger carries the argument for this pair.
//
//nolint:dupl // paired with sendGrowTrigger in cluster_add.go; coincidental send skeleton.
func sendUpgradeTrigger(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	deadline := time.Now().Add(upgradeConvergeTimeout)
	for {
		resp, err := sendUpgradeTriggerOnce(ctx, nc, actor, seed, req)
		if err != nil || resp.Code != upgradeTriggerCodeNotReady || time.Now().After(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(upgradeConvergePoll):
		}
	}
}

// origin: line-2 review M14. sendGrowTriggerOnce in cluster_add.go is this one's pair.
//
//nolint:dupl // paired with sendGrowTriggerOnce in cluster_add.go; single-attempt half.
func sendUpgradeTriggerOnce(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	data, err := signUpgradeTrigger(seed, req, time.Now())
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, upgradeTriggerTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, proto.SubjCtrlClusterUpgrade(actor), data)
	if err != nil {
		return nil, unavailErr("trigger %s/%s: %v (broker unreachable, or too old to have the responder)", req.Op, req.TargetNode, err)
	}
	var resp proto.ClusterUpgradeResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("decode trigger reply: %w", err)
	}
	return &resp, nil
}

// newBrokerCmd is a local daemon surface, independent of node upgrade's agent
// identity and the NATS session. Installed candidate/prev checks belong to the
// broker's reload admission; this command reports only observed running status.
// origin: hot-update S6 plan §5 / T45 / lead L3
func newBrokerCmd() *cobra.Command {
	var socketPath string
	root := &cobra.Command{Use: "broker", Short: "Manage the local broker daemon"}
	root.PersistentFlags().StringVar(&socketPath, "socket", defaultAdminSocket, "path to the broker admin Unix socket")
	upgrade := &cobra.Command{Use: "upgrade", Short: "Reload or inspect the broker image"}
	var version, sha, home, txID string
	var tx upgradecli.UpgradeOptions
	var asJSON bool
	ports := upgradecli.BrokerPorts{Call: cli.CallBrokerAdmin, Exit: upgradeExit, DefaultHome: cli.DefaultHome(),
		Socket: func(*cobra.Command) string { return socketPath }}
	// origin: S8 FIX-DESCRIBE (P19 request R1) — the broker describes itself and
	// its staged candidate; a prepared grant is attributed to the local identity.
	ports.Describe, ports.Actor = upgradecli.DescribeBroker(ports), upgradecli.IdentityActor(cli.EnsureIdentity, func(id *cli.Identity) string { return id.Fingerprint })
	reload := &cobra.Command{Use: "reload", Short: "Reload the staged broker image and verify its running SHA", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P19
			if tx.TxID != "" || tx.PrepareOnly || tx.GrantOut != "" || tx.ArtifactReceipt != "" {
				tx.ToVersion, tx.ExpectSHA256 = version, sha
				return upgradecli.RunBrokerReload(cmd, ports, home, tx, upgradeConvergeTimeout)
			}
			if version == "" || sha == "" {
				return usageErr("--to-version and --expect-sha256 are required")
			}
			status, err := cli.ReloadLocalBroker(cmd.Context(), socketPath, version, sha)
			if err != nil {
				return brokerUpgradeError(err)
			}
			return emitJSON(cmd.OutOrStdout(), status)
		},
	}
	reload.Flags().StringVar(&version, "to-version", "", "release of the staged image")
	reload.Flags().StringVar(&sha, "expect-sha256", "", "SHA256 of the staged image")
	reload.Flags().StringVar(&tx.TxID, "tx-id", "", "resume only the original transaction (with --artifact-receipt: Start it)")
	reload.Flags().BoolVar(&tx.PrepareOnly, "prepare-only", false, "obtain the broker-issued inert grant only; requires --grant-out")
	reload.Flags().StringVar(&tx.GrantOut, "grant-out", "", "write the prepared grant here for `tether upgrade stage`")
	reload.Flags().StringVar(&tx.ArtifactReceipt, "artifact-receipt", "", "artifact receipt from `tether upgrade stage` for --tx-id")
	status := &cobra.Command{Use: "status", Short: "Read the running broker image", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// origin: hot-update S8 plan §7.1 — additive: one transaction's private View.
			if txID != "" {
				return ports.Exit("broker upgrade status", upgradecli.RunBrokerVerb(cmd, ports, upgradecli.VerbStatus, home, txID, upgradeTriggerTimeout, asJSON))
			}
			resp, err := cli.CallBrokerAdmin(cmd.Context(), socketPath, adminsock.Request{Op: adminsock.OpRuntime})
			if err != nil {
				return unavailErr("broker status: %v", err)
			}
			if resp.Code != "" || resp.Error != "" || resp.Runtime == nil || resp.Runtime.BrokerImage == nil {
				return unavailErr("broker runtime image is unavailable")
			}
			return emitJSON(cmd.OutOrStdout(), resp.Runtime.BrokerImage)
		},
	}
	status.Flags().StringVar(&txID, "tx-id", "", "query one upgrade transaction instead of the running image")
	status.Flags().BoolVar(&asJSON, "json", false, "with --tx-id: emit the private transaction View as JSON")
	for _, c := range []*cobra.Command{reload, status} {
		c.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir (holds the grants this ctl prepared)")
	}
	upgrade.AddCommand(reload, status)
	upgrade.AddCommand(upgradecli.BrokerTransactionCommands(ports)...)
	root.AddCommand(upgrade)
	return root
}

// origin: hot-update S6 plan §9.1 / T45
func brokerUpgradeError(err error) error {
	var refusal *cli.BrokerReloadError
	if errors.As(err, &refusal) {
		return &ExitError{Class: brokerCodeExitClass(refusal.Code), Code: refusal.Code, Err: err}
	}
	var unknown *cli.BrokerReloadUnknownError
	if errors.As(err, &unknown) {
		return &ExitError{Class: exitUnsettled, Code: proto.CodeAgentNoResponders, Err: err}
	}
	return unavailErr("broker upgrade: %v", err)
}
