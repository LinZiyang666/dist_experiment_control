package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_add.go (G4 §B) — the grow orchestrator `tether cluster add`. It runs ON THE JOINER HOST (the offline
// `cluster init` + `join prepare` are inherently local) and drives every cross-host step (lock, approve-join,
// former-N1 cutover, rebalance) over the account-signed grow-trigger subject, mirroring the G5 upgrade
// orchestrator. It NEVER orchestrates systemctl (cluster.go:782-784): the joiner's cold daemon start is
// provisioning's job, so `cluster add` HALTs at that boundary with the exact command + "re-run to resume";
// every phase is a re-observable postcondition, so a re-run seamlessly resumes. It composes the shipped
// leader-driven OpKindJoin/driveJoin ladder for the raft membership (never AddNonvoter/AddVoter itself), so
// R3 is inherited. See docs/reviews/g4-plan.md.

const (
	growTriggerTimeout  = 10 * time.Second
	growConvergeTimeout = 3 * time.Minute
	growConvergePoll    = 3 * time.Second
	// The former-N1 cutover restarts/reloads NATS and its auth_callout responder. A valid, already
	// activated grow identity can pass one probe and hit a short Authorization Violation window on
	// the very next connect. The grow is explicitly resumable, so retry this ambiguous transport edge
	// inside the product rather than exiting and stranding the membership op/grow lock.
	growConnectAuthRetryWindow = 30 * time.Second
	growConnectAuthRetryPause  = time.Second
)

func newClusterAddCmd(socketPath *string) *cobra.Command {
	var (
		natsURL, home, seedPath, confirmNodeID, webhook     string
		selfID, name, nodeIdentPub, raftAddr, natsRoute     string
		tunnelAddr, publicHost, secretsDir, dataDir, dbPath string
		configPath                                          string
		resetFormerJS, preserveJSData, dryRun               bool
		autoConfirmCatchup                                  int
		timeout                                             time.Duration
	)
	cmd := &cobra.Command{
		Use:   "add <joiner-node-id>",
		Short: "Grow the cluster: orchestrate a new broker to VOTER in one idempotent, resumable command (run on the joiner)",
		Long: "Runs on the NEW broker host. Orchestrates the whole grow — offline init, join, route-mesh cutover,\n" +
			"former-N1 JetStream reset, catch-up, promote to VOTER, seed, rebalance — composing the shipped\n" +
			"leader-driven join ladder (never touching raft membership directly). It NEVER runs systemctl: the\n" +
			"joiner's daemon start is provisioning's job, so `add` HALTs at that boundary and RESUMES on re-run.\n" +
			"Cross-host steps are signed with the cluster account seed (--account-seed) and verified by each broker.",
		Example: "  # on the joiner host (bootstrap-privileged), with $TETHER_CONFIRM_NODE_ID=brk2:\n" +
			"  tether cluster add brk2 --raft-addr brk2:7400 --nats-route nats://brk2:6222 \\\n" +
			"    --tunnel-addr brk2:7000 --public-host brk2 --node-ident-pub <pub> \\\n" +
			"    --account-seed /etc/tether/secrets/account.nk --confirm-node-id brk2",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			joiner := args[0]
			if selfID == "" {
				selfID = joiner
			}
			if selfID != joiner {
				return usageErr("--self-id %q must equal the <joiner-node-id> %q", selfID, joiner)
			}
			if name == "" {
				name = joiner
			}
			for flag, val := range map[string]string{
				"--raft-addr": raftAddr, "--nats-route": natsRoute, "--tunnel-addr": tunnelAddr,
				"--public-host": publicHost, "--node-ident-pub": nodeIdentPub,
			} {
				if val == "" {
					return usageErr("%s is required (the joiner's own %s)", flag, flag[2:])
				}
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
			nc, err := retryClusterAddConnect(cmd.Context(), growConnectAuthRetryWindow, growConnectAuthRetryPause, func() (*nats.Conn, error) {
				return connectCtl(cmd, "cluster add", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			})
			if err != nil {
				return err
			}
			defer nc.Close()

			// The account seed signs every cross-host trigger. Read it early; required unless a pure dry-run.
			var accountSeed []byte
			if seedPath != "" {
				if accountSeed, err = os.ReadFile(seedPath); err != nil {
					return unavailErr("read account seed: %v", err)
				}
			}

			jp := joinerParams{
				Joiner: joiner, Name: name, NodeIdentPub: nodeIdentPub, RaftAddr: raftAddr, NatsRoute: natsRoute,
				TunnelAddr: tunnelAddr, PublicHost: publicHost, SecretsDir: secretsDir, DataDir: dataDir, DBPath: dbPath,
				ConfigPath:    configPath,
				ConfirmNodeID: confirmNodeID, ResetFormerJS: resetFormerJS, PreserveJSData: preserveJSData,
				AutoConfirmCatchup: autoConfirmCatchup,
			}
			return driveAdd(cmd, nc, id.PublicKey, sid, accountSeed, seedPath, jp, *socketPath, dryRun, timeout, webhook)
		},
	}
	cmd.Flags().StringVar(&seedPath, "account-seed", "", "path to the cluster account nkey seed (signs each broker trigger; required to execute)")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "unattended init confirm (#5): must equal <joiner-node-id> AND match $"+"TETHER_CONFIRM_NODE_ID")
	cmd.Flags().StringVar(&selfID, "self-id", "", "the joiner's node_id (defaults to the positional arg)")
	cmd.Flags().StringVar(&name, "name", "", "human broker name (UNIQUE in the cluster; defaults to the node_id)")
	cmd.Flags().StringVar(&nodeIdentPub, "node-ident-pub", "", "the joiner's node-identity public key (required)")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "the joiner's raft transport addr host:7400 (required)")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "the joiner's NATS route URL nats://host:6222 (required)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "the joiner's public tunnel control addr (required)")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "the joiner's public DNS host (required)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/tether", "the joiner's broker data dir (raft/ bootstrapped here)")
	cmd.Flags().StringVar(&dbPath, "db", "/var/lib/tether/tether.db", "the joiner's tether.db")
	cmd.Flags().StringVar(&configPath, "config", "/etc/tether/broker.yaml", "the joiner's broker.yaml (the init step applies + this verifies the broker.cluster seam here)")
	cmd.Flags().BoolVar(&resetFormerJS, "reset-former-js", false, "acknowledge resetting a NON-EMPTY former-N1 JetStream store (history/audit loss; the store is MOVED aside, not deleted)")
	cmd.Flags().BoolVar(&preserveJSData, "preserve-js-data", false, "acknowledge the former-N1 JS reset, keeping the moved-aside store (jetstream.grow-bak.<epoch>) to restore by hand; auto backup→restore is not implemented in v1")
	cmd.Flags().IntVar(&autoConfirmCatchup, "auto-confirm-catchup", 0, "for unattended runs, auto-confirm a BLOCKED catch-up op up to N times (default 0 = surface it to the operator)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "resolve + print the grow plan and exit, touching nothing")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "overall bound on the wait-to-SERVING")
	cmd.Flags().StringVar(&webhook, "notify-webhook", "", "POST a JSON milestone on each phase / complete / HALT")
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "a live broker NATS URL (the trigger transport)")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	registerYesRejector(cmd)
	return cmd
}

// retryClusterAddConnect retries only the ambiguous auth-callout-unavailable signature produced
// during the grow cutover. A wrong URL/TLS/network error is returned immediately; a persistently bad
// session/PIN still returns the original permission error after the bounded window. The caller's
// context cancels the wait. Kept as a small injected-attempt helper so its exact retry boundary is
// independently testable without weakening the deploy-tier harness.
func retryClusterAddConnect(ctx context.Context, window, pause time.Duration, attempt func() (*nats.Conn, error)) (*nats.Conn, error) {
	deadline := time.Now().Add(window)
	for {
		nc, err := attempt()
		if err == nil {
			return nc, nil
		}
		if !strings.Contains(err.Error(), "Authorization Violation") || window <= 0 || !time.Now().Before(deadline) {
			return nil, err
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// joinerParams bundles the joiner-local provisioning identity the orchestrator threads into the local
// init/prepare steps and the signed triggers.
type joinerParams struct {
	Joiner, Name, NodeIdentPub, RaftAddr, NatsRoute     string
	TunnelAddr, PublicHost, SecretsDir, DataDir, DBPath string
	ConfigPath                                          string
	ConfirmNodeID                                       string
	ResetFormerJS                                       bool
	PreserveJSData                                      bool
	AutoConfirmCatchup                                  int
}

// signGrowTrigger stamps IssuedAt + the account signature onto a grow request and marshals it.
func signGrowTrigger(seed []byte, req *proto.ClusterGrowReq, now time.Time) ([]byte, error) {
	req.IssuedAt = now.UTC().Format(time.RFC3339)
	sig, err := auth.SignWithSeed(seed, proto.CanonicalGrowReqBytes(req))
	if err != nil {
		return nil, err
	}
	req.Sig = hex.EncodeToString(sig)
	return json.Marshal(req)
}

// sendGrowTrigger signs + publishes a grow trigger and awaits the target broker's reply, RETRYING the
// retriable cluster_not_ready (a broker that just restarted answers health before its admin backend wires).
func sendGrowTrigger(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
	deadline := time.Now().Add(growConvergeTimeout)
	for {
		resp, err := sendGrowTriggerOnce(ctx, nc, actor, seed, req)
		if err != nil || resp.Code != growTriggerCodeNotReadyCLI || time.Now().After(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(growConvergePoll):
		}
	}
}

const growTriggerCodeNotReadyCLI = "cluster_not_ready"

func sendGrowTriggerOnce(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
	data, err := signGrowTrigger(seed, req, time.Now())
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, growTriggerTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, proto.SubjCtrlClusterGrow(actor), data)
	if err != nil {
		return nil, unavailErr("grow trigger %s/%s: %v (broker unreachable, or too old to have the responder)", req.Op, req.TargetNode, err)
	}
	var resp proto.ClusterGrowResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("decode grow trigger reply: %w", err)
	}
	return &resp, nil
}

// notifyGrow POSTs a best-effort JSON milestone to the webhook (no-op if empty); mirrors notifyUpgrade.
func notifyGrow(webhook, event string, fields map[string]any) {
	notifyMilestone(webhook, "cluster_grow_"+event, fields)
}
