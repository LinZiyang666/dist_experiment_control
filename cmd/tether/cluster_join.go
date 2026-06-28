package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

// cluster_join.go (C4) — the ergonomic ONE-prepare-ONE-approve join. `prepare` runs on the JOINING
// machine (not yet a cluster member, no leader call): it self-mints a nonce, signs the PoP over the
// unchanged bytes, and emits a base64 bundle. `approve` runs on the leader: it hands the bundle to the
// operation controller, which drives the recoverable join to SERVING.

func newClusterJoinCmd(socketPath *string) *cobra.Command {
	parent := &cobra.Command{
		Use:   "join",
		Short: "Recoverable cluster join: `prepare` (on the joiner) then `approve` (on the leader)",
		Long: "Two steps, ONE each: `cluster join prepare` on the NEW broker emits a self-signed bundle;\n" +
			"`cluster join approve <bundle>` on the leader admits + drives the join to SERVING (resumable).",
		Example: "  joiner$ tether cluster join prepare --raft-addr 10.0.0.2:7400 --nats-route nats://10.0.0.2:6222 --tunnel-addr brk-b:7000 --public-host brk-b\n" +
			"  leader$ tether cluster join approve tether-join:v1:… --wait",
	}
	parent.AddCommand(newClusterJoinPrepareCmd(), newClusterJoinApproveCmd(socketPath))
	return parent
}

func newClusterJoinPrepareCmd() *cobra.Command {
	var seedPath, nodeID, name, natsServerID, raftAddr, natsRoute, tunnelAddr, publicHost, certFP, secretsDir string
	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "On the JOINING broker: mint a self-signed join bundle (no leader call needed)",
		Example: "  tether cluster join prepare --node-id brk-b --raft-addr 10.0.0.2:7400 \\\n" +
			"      --nats-route nats://10.0.0.2:6222 --tunnel-addr brk-b:7000 --public-host brk-b",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			rawSeed, err := os.ReadFile(seedPath)
			if err != nil {
				return fmt.Errorf("read node-ident seed %s: %w", seedPath, err)
			}
			// C8 (D12): tolerate a trailing newline on a hand-edited node-ident.nk (the deleted
			// sign-join/node-pub trimmed; join prepare must too — keygen writes none, but operators edit).
			seed := []byte(strings.TrimSpace(string(rawSeed)))
			pub, err := auth.PublicKeyFromSeed(seed)
			if err != nil {
				return fmt.Errorf("derive node-ident pub: %w", err)
			}
			if nodeID == "" {
				nodeID = natsServerID // fall back to the nats server_name if not given
			}
			if err := proto.ValidateClusterNodeID(nodeID); err != nil {
				return usageErr("cluster join prepare: --node-id %q invalid: %v", nodeID, err)
			}
			if raftAddr == "" {
				return usageErr("cluster join prepare: --raft-addr is required (host:7400)")
			}
			// C8 (D10): require the expose-home/NATS-peer identity that the deleted `cluster add`
			// enforced — else the admitted voter can never serve as an expose home or join the mesh.
			// cert_fp stays optional (D6 backfills). JoinBundle.Validate re-enforces leader-side.
			if tunnelAddr == "" || natsRoute == "" {
				return usageErr("cluster join prepare: --tunnel-addr (host:7000) and --nats-route (nats://host:6222) are required (a voter must be able to serve exposes + join the NATS mesh)")
			}
			// Self-mint a nonce (a consistency property, not a secret — §18.2.4): 16 random bytes.
			nb := make([]byte, 16)
			if _, err := rand.Read(nb); err != nil {
				return fmt.Errorf("mint nonce: %w", err)
			}
			nonce := hex.EncodeToString(nb)
			sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
			if err != nil {
				return fmt.Errorf("sign join PoP: %w", err)
			}
			// audit A: derive the bus nkey (from broker.nk) + cert_fp (from the tunnel cert) at PREPARE so
			// the leader bakes cluster_nodes.bus_nkey_pub + cert_fp AT ADMISSION. This breaks the learner
			// bus_nkey self-backfill deadlock (the write needs a mesh that needs the value) and the empty-
			// cert_fp wireClusterEarly crash-loop, and keeps the new voter home-eligible + topology-renderable.
			if natsServerID == "" {
				natsServerID = nodeID // §6.5 SSOT: server_name == node_id
			}
			busNkey := ""
			if raw, e := os.ReadFile(filepath.Join(secretsDir, "broker.nk")); e == nil {
				if bp, e2 := auth.PublicKeyFromSeed([]byte(strings.TrimSpace(string(raw)))); e2 == nil {
					busNkey = bp
				} else {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "WARN: could not derive bus nkey from %s/broker.nk: %v (the learner bus_nkey backfill may deadlock)\n", secretsDir, e2)
				}
			} else {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "WARN: could not read %s/broker.nk: %v — pass --secrets-dir (the leader needs the bus nkey at admission)\n", secretsDir, e)
			}
			if certFP == "" {
				if fp, e := clusteroffline.TunnelCertFingerprint(secretsDir); e == nil {
					certFP = fp
				} else {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "WARN: could not derive cert_fp from %s tunnel cert: %v (an empty cert_fp can crash-loop the joiner on restart)\n", secretsDir, e)
				}
			}
			bundle, err := cluster.EncodeJoinBundle(cluster.JoinBundle{
				NodeID: nodeID, Name: name, NodeIdentPub: pub, NatsServerID: natsServerID,
				RaftAddr: raftAddr, NatsRoute: natsRoute, TunnelAddr: tunnelAddr, PublicHost: publicHost,
				CertFP: certFP, BusNkey: busNkey, JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig),
			})
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintln(out, bundle)
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nrun on the LEADER: tether cluster join approve <the bundle above> --wait")
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "this broker's node_id (== nats server_name; defaults to --nats-server-id)")
	cmd.Flags().StringVar(&name, "name", "", "human name (defaults to node_id)")
	cmd.Flags().StringVar(&natsServerID, "nats-server-id", "", "this broker's deterministic nats server_name")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "this joiner's raft address (host:7400) — required")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "this joiner's NATS route URL (nats://host:6222)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "this joiner's public tunnel addr (host:7000)")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "this joiner's public host")
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "this joiner's tunnel-cert fingerprint pin (default: auto-derived from <secrets-dir>/tunnel-cert.pem)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "§15 secrets dir — read broker.nk (bus nkey) + tunnel-cert.pem (cert_fp) to carry at admission (audit A)")
	return cmd
}

func newClusterJoinApproveCmd(socketPath *string) *cobra.Command {
	var wait bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:     "approve <bundle>",
		Short:   "On the LEADER: admit + drive a join bundle to SERVING (recoverable)",
		Args:    cobra.ExactArgs(1),
		Example: "  tether cluster join approve tether-join:v1:… --wait",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterJoinApprove, JoinBundle: args[0]})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil { // m7: a follower names the leader host + the right exit class
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("join approve", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "join operation %s created (driving to SERVING).\n", resp.OpID)
			if wait {
				return waitForOp(cmd, *socketPath, resp.OpID, timeout)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  watch: tether cluster ops show %s\n", resp.OpID)
			return nil
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "block until the join reaches SERVING (or --timeout), naming the laggard state")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "with --wait: give up after this long")
	return cmd
}

// waitForOp polls `cluster ops` for the op_id until it is terminal (done = exit 0; failed/aborted =
// loud non-zero) or the timeout (exit 75 transient). Reuses the cluster status socket round-trip.
func waitForOp(cmd *cobra.Command, socketPath, opID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterOps, OpsNode: opID})
		if err != nil {
			return err
		}
		var op *adminsock.ClusterOpEntry
		for i := range resp.Ops {
			if resp.Ops[i].OpID == opID {
				op = &resp.Ops[i]
			}
		}
		if op == nil {
			return fmt.Errorf("operation %s not found", opID)
		}
		if op.Terminal {
			if op.State == "done" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "operation %s reached %s.\n", opID, op.OpState)
				return nil
			}
			return &ExitError{Class: exitInternal, Err: fmt.Errorf("operation %s ended %s: %s", opID, op.OpState, op.LastError)}
		}
		// C4-M7: a BLOCKED op needs operator action — do NOT hang to the timeout; surface it promptly.
		if op.State == "stalled" {
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("operation %s is BLOCKED (%s): %s — `cluster ops confirm %s` to proceed, or `cluster ops abort %s`", opID, op.OpState, op.LastError, opID, opID)}
		}
		if time.Now().After(deadline) {
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("operation %s still in flight (%s) after %s — `cluster ops show %s`", opID, op.OpState, timeout, opID)}
		}
		time.Sleep(2 * time.Second)
	}
}
