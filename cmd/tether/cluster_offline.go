package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// cluster_offline.go — D7 §8.4 OFFLINE + local cluster verbs. force-single / recover
// run with the daemon STOPPED and touch disk directly via internal/clusteroffline
// (which calls cluster.RecoverSingleNode; raft never enters cmd/tether). They NEVER
// honor --yes — a typed node_id is mandatory (§8.1).

const (
	defaultDataDir      = "/var/lib/tether"
	defaultDBPath       = "/var/lib/tether/tether.db"
	defaultSeed         = "/etc/tether/node-ident.nk"
	defaultNatsConfPath = "/etc/tether/nats.conf"
)

func newClusterForceSingleCmd() *cobra.Command {
	var dataDir, dbPath, selfID, selfAddr string
	var confirmDead []string
	var guided bool
	cmd := &cobra.Command{
		Use:   "force-single",
		Short: "DANGER (split-brain risk): force this node to a lone single-voter cluster — quorum-loss escape hatch only; daemon STOPPED; type node_id to confirm",
		Long: `force-single is the quorum-loss escape hatch — and the single most dangerous
cluster command. It rewrites this node into a lone single-voter cluster: if ANY listed
peer is actually alive (merely partitioned, not dead) it SPLITS THE BRAIN into two
divergent timelines, and there is NO HA / NO integrity until you recover.

STOP the daemon first (systemctl mask tether-broker && systemctl stop tether-broker). It
HARD-REFUSES if any peer is still reachable on :7400, if there is no existing raft state, or
if a daemon still holds the store. It NEVER accepts --yes; you must type this node's id to
confirm (and the split-brain consequence is shown at the prompt).`,
		Example: "  # quorum-loss drill (the majority is PERMANENTLY dead):\n" +
			"  systemctl mask tether-broker && systemctl stop tether-broker\n" +
			"  tether cluster force-single --self-id brk-a --self-addr 10.0.0.1:7400 --confirm-peers-dead brk-b,brk-c\n" +
			"  systemctl unmask tether-broker && systemctl start tether-broker   # then recover each returning node",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// B7 DOC#7: --guided diagnoses (read-only) and PRINTS the exact command — it auto-derives
			// the --confirm-peers-dead list + TCP-probes each peer, and BLOCKS if any is still alive.
			// It executes nothing (the unchanged hard gates stay the only mutating path).
			if guided {
				return forceSingleGuided(cmd, dbPath, selfID, selfAddr)
			}
			if err := rejectedUnattendedYes(cmd, "force-single", selfID); err != nil {
				return err
			}
			if selfID == "" || selfAddr == "" {
				return usageErr("force-single requires --self-id and --self-addr")
			}
			if len(confirmDead) == 0 {
				return usageErr("force-single requires --confirm-peers-dead listing EVERY other node_id (a missed peer would split-brain)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"force-single will rewrite %q to a single-voter cluster (NO HA / NO integrity until recover).\n"+
					"Confirm the peers %v are TRULY dead (powered off / unreachable to agents), not merely partitioned.\n",
				selfID, confirmDead)
			if !confirmTypedNodeID(cmd, selfID,
				"CONSEQUENCE: no HA + no integrity until you recover; if ANY listed peer is alive (merely partitioned) this SPLITS THE BRAIN into two divergent timelines.",
				false, "") { // never-escapable: an env var is not "attended" for a brain-split-capable op
				return fmt.Errorf("aborted (type this node's id to confirm; --yes is never accepted)")
			}
			abandoned, err := clusteroffline.ForceSingle(clusteroffline.ForceSingleOptions{
				DataDir: dataDir, DBPath: dbPath, SelfID: selfID, SelfRaftAddr: selfAddr, ConfirmedDead: confirmDead,
			})
			if err != nil {
				return fmt.Errorf("force-single: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"force-single complete: %q is now a single-voter cluster (%d nodes abandoned).\n"+
					"systemctl unmask tether-broker && systemctl start tether-broker, then recover the others.\n", selfID, len(abandoned))
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (holds raft/ + tether.db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&selfID, "self-id", "", "this node's cluster node_id")
	cmd.Flags().StringVar(&selfAddr, "self-addr", "", "this node's raft address (host:7400)")
	cmd.Flags().StringSliceVar(&confirmDead, "confirm-peers-dead", nil, "node_ids of EVERY other roster node (comma-separated)")
	cmd.Flags().BoolVar(&guided, "guided", false, "diagnose + print the exact force-single command (auto-derives --confirm-peers-dead, probes peers); executes nothing (B7 DOC#7)")
	registerYesRejector(cmd)
	return cmd
}

func newClusterRecoverCmd() *cobra.Command {
	var dataDir, dbPath, dumpPath, selfID, emitManifest, secretsDir string
	var guided bool
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Wipe a returning node after a forensic divergence dump, so it can rejoin clean (daemon STOPPED)",
		Example: "  systemctl mask tether-broker && systemctl stop tether-broker\n" +
			"  tether cluster recover --self-id brk-b --dump-divergent /root/divergent-brk-b.json\n" +
			"  # then `cluster init --from-existing` on this node, and `cluster add` on the leader to rejoin",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// B7 DOC#7: --guided prints the sequenced recover→init→add commands (read-only); executes nothing.
			if guided {
				return recoverGuided(cmd, selfID, dumpPath, emitManifest, secretsDir)
			}
			if err := rejectedUnattendedYes(cmd, "recover", selfID); err != nil {
				return err
			}
			if dumpPath == "" {
				return usageErr("recover requires --dump-divergent <file> (the forensic dump path)")
			}
			if selfID == "" {
				return usageErr("recover requires --self-id (the node_id being wiped, to confirm you target the right node)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"recover will DUMP node %q's divergent rows to %s then WIPE raft/ + tether.db.\n"+
					"The dump is forensic-only and NOT auto-mergeable.\n", selfID, dumpPath)
			// Confirm on the node_id (B3): typing a fixed word gives zero protection
			// against running recover against the wrong --data-dir/--db.
			if !confirmTypedNodeID(cmd, selfID, "", false, "") { // never-escapable
				return fmt.Errorf("aborted (type the node_id to confirm; --yes is never accepted)")
			}
			n, err := clusteroffline.Recover(clusteroffline.RecoverOptions{
				DataDir: dataDir, DBPath: dbPath, DumpPath: dumpPath,
				SelfID: selfID, ManifestPath: emitManifest, SecretsDir: secretsDir,
			})
			if err != nil {
				return fmt.Errorf("recover: %w", err)
			}
			reinit := "`cluster init --from-existing`"
			if emitManifest != "" {
				reinit = fmt.Sprintf("`cluster init --from-manifest %s`", emitManifest)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"recover complete: %d rows dumped to %s, node %q wiped. Re-run %s on this node before starting tether-broker, then run `cluster add` on the leader to rejoin.\n",
				n, dumpPath, selfID, reinit)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&dumpPath, "dump-divergent", "", "forensic dump output path (0600, must not pre-exist)")
	cmd.Flags().StringVar(&selfID, "self-id", "", "the node_id being wiped (typed to confirm)")
	cmd.Flags().StringVar(&emitManifest, "emit-manifest", "", "also capture this node's identity to a manifest (0600) for `cluster init --from-manifest`")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "", "secrets dir for the advisory account_fp in the emitted manifest (optional)")
	cmd.Flags().BoolVar(&guided, "guided", false, "print the sequenced recover→init→add commands with real values; executes nothing (B7 DOC#7)")
	registerYesRejector(cmd)
	return cmd
}

func newClusterSignJoinCmd() *cobra.Command {
	var seedPath, secretsDir, raftAddr, tunnelAddr, natsRoute, publicHost, releaseVersion string
	var protoVer int
	cmd := &cobra.Command{
		Use:   "sign-join <node-id> <nonce>",
		Short: "Sign a leader-issued join nonce; with --raft-addr it prints the COMPLETE `cluster add` re-run line (run on the JOINING node)",
		Example: "  # bare token (paste as --join-token on the leader):\n" +
			"  tether cluster sign-join brk-b 7f3a…\n\n" +
			"  # complete pasteable re-run line (stdout is the SOLE line; 2>/dev/null for just it):\n" +
			"  tether cluster sign-join brk-b 7f3a… --raft-addr 10.0.0.2:7400 \\\n" +
			"      --tunnel-addr brk-b.example:7000 --nats-route nats://10.0.0.2:6222 --public-host brk-b.example",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID, nonce := args[0], args[1]
			seed, err := os.ReadFile(seedPath)
			if err != nil {
				return fmt.Errorf("read node-ident seed %s: %w", seedPath, err)
			}
			seed = []byte(strings.TrimSpace(string(seed)))
			pub, err := auth.PublicKeyFromSeed(seed)
			if err != nil {
				return fmt.Errorf("derive pubkey: %w", err)
			}
			sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
			if err != nil {
				return fmt.Errorf("sign: %w", err)
			}
			token := nonce + ":" + hex.EncodeToString(sig)
			out, errw := cmd.OutOrStdout(), cmd.ErrOrStderr()

			// Backward-compatible: without --raft-addr, emit the bare token (the pre-B3 contract)
			// + a hint. The token NEVER depends on cert/addr reads — it derives only from the
			// node-ident seed (which already gates the command).
			if raftAddr == "" {
				_, _ = fmt.Fprintln(out, token)
				_, _ = fmt.Fprintf(errw, "node-pub: %s\n", pub)
				_, _ = fmt.Fprintln(errw, "(paste the token above as --join-token on the leader; or pass --raft-addr (+ --tunnel-addr/--nats-route/--public-host) to print the COMPLETE `cluster add` line)")
				return nil
			}

			// cert-fp is INDEPENDENT of the token: a cert read failure must not lose the token.
			certFP, certErr := clusteroffline.TunnelCertFingerprint(secretsDir)
			if certErr != nil {
				_, _ = fmt.Fprintf(errw, "# WARN: could not read tunnel cert-fp (%v); the line below is incomplete — fill --cert-fp\n", certErr)
			}
			// B3 review n3: warn (to stderr) about any addr flag left as a placeholder, so an
			// operator piping `2>/dev/null` isn't surprised by an unfilled <…> the leader rejects.
			var missingAddrs []string
			if tunnelAddr == "" {
				missingAddrs = append(missingAddrs, "--tunnel-addr")
			}
			if natsRoute == "" {
				missingAddrs = append(missingAddrs, "--nats-route")
			}
			if publicHost == "" {
				missingAddrs = append(missingAddrs, "--public-host")
			}
			if len(missingAddrs) > 0 {
				_, _ = fmt.Fprintf(errw, "# WARN: the line below has <…> placeholders for %s — fill them before running it on the leader.\n", strings.Join(missingAddrs, ", "))
			}
			_, _ = fmt.Fprintf(errw, "Re-run on the LEADER (node-pub %s):\n", pub)
			// stdout is the SOLE pasteable line (so `2>/dev/null` yields exactly one line). The
			// --joiner-proto/--joiner-release are this JOINER binary's versions (B6 A3): the leader
			// hard-rejects a proto mismatch, advisory-warns on a release skew.
			_, _ = fmt.Fprintf(out, "tether cluster add %s %s %s --join-token %s --tunnel-addr %s --cert-fp %s --nats-route %s --public-host %s --joiner-proto %d --joiner-release %s\n",
				nodeID, raftAddr, pub, token,
				orPlaceholder(tunnelAddr, "<tunnel-host:7000>"), orPlaceholder(certFP, "<sha256:…>"),
				orPlaceholder(natsRoute, "<nats://host:6222>"), orPlaceholder(publicHost, "<public-dns>"),
				protoVer, releaseVersion)
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir (for the tunnel cert-fp)")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "this joiner's raft address (host:7400) — required to print the complete add line")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "this joiner's public tunnel addr (host:7000)")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "this joiner's NATS route URL (nats://host:6222)")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "this joiner's public DNS host")
	cmd.Flags().IntVar(&protoVer, "proto-ver", proto.ProtoVersion, "this joiner's proto version (default: this binary's)")
	cmd.Flags().StringVar(&releaseVersion, "release-version", proto.ReleaseVersion, "this joiner's release tag (default: this binary's)")
	return cmd
}

func newClusterNodePubCmd() *cobra.Command {
	var seedPath string
	cmd := &cobra.Command{
		Use:     "node-pub",
		Short:   "Print this node's identity public key (for `cluster add`)",
		Example: "  tether cluster node-pub   # -> U…  (the 3rd positional arg to `cluster add` on the leader)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			seed, err := os.ReadFile(seedPath)
			if err != nil {
				return fmt.Errorf("read node-ident seed %s: %w", seedPath, err)
			}
			pub, err := auth.PublicKeyFromSeed([]byte(strings.TrimSpace(string(seed))))
			if err != nil {
				return fmt.Errorf("derive pubkey: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), pub)
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	return cmd
}

func newClusterKeygenCmd() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:     "keygen",
		Short:   "Generate a new node-identity key (prints the pubkey; writes the seed 0600)",
		Example: "  tether cluster keygen --out /etc/tether/node-ident.nk   # writes the seed, prints the pubkey",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			seed, err := auth.GenerateUserSeed()
			if err != nil {
				return err
			}
			pub, err := auth.PublicKeyFromSeed(seed)
			if err != nil {
				return err
			}
			if out != "" {
				if err := os.WriteFile(out, seed, 0o600); err != nil {
					return fmt.Errorf("write seed %s: %w", out, err)
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), pub)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "write the seed to this path (0600); empty = print pubkey only")
	return cmd
}

// machineConfirmEnv is the node-id-valued env var that, TOGETHER WITH a matching
// --confirm-node-id flag, lets a machine-confirm-ESCAPABLE op (only non-F==0 `cluster remove`)
// proceed unattended. It is deliberately NOT a boolean "yes-to-everything": it must EXACTLY
// equal the node_id being confirmed, so a stray exported env cannot auto-confirm an arbitrary op.
const machineConfirmEnv = "TETHER_CONFIRM_NODE_ID"

// confirmTypedNodeID requires the operator to TYPE want exactly (no --yes accepted). consequence,
// when non-empty, is printed immediately before the prompt (so the danger is at the moment of
// typing). It reads cmd.InOrStdin() so a unit test can inject input via cmd.SetIn(...); in
// production (stdin) it still HARD-REFUSES a non-interactive terminal — there is no unattended
// path for a quorum-affecting op (B3 item 4).
//
// allowMachineEscape is a PER-CALL-SITE capability (default false), NEVER a property of the
// shared funnel: the never-escapable ops (force-single / recover / F==0-drain / init / restore)
// pass false, so even a correct flag+env STILL falls through to the TTY refuse — an env var in a
// systemd unit or CI is not "attended" for an irreversible / quorum-destructive op. Only non-F==0
// `cluster remove` passes true. When escape IS allowed, it fires ONLY if BOTH the --confirm-node-id
// flag (flagNodeID) AND the env exactly equal want; either alone refuses (a stray env or a stray
// history flag must not be enough). --yes is never the escape (rejectedUnattendedYes is unchanged).
func confirmTypedNodeID(cmd *cobra.Command, want, consequence string, allowMachineEscape bool, flagNodeID string) bool {
	if allowMachineEscape {
		envID := os.Getenv(machineConfirmEnv)
		if flagNodeID != "" && envID != "" && flagNodeID == want && envID == want {
			return true
		}
		// A partial or mismatched escape attempt is NOT an error here — it just falls through to
		// the interactive path (which HARD-REFUSES a non-interactive terminal below).
	}
	in := cmd.InOrStdin()
	if in == os.Stdin && !term.IsTerminal(int(os.Stdin.Fd())) {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "refusing: this confirmation requires an interactive terminal (no --yes for destructive cluster ops)")
		return false
	}
	if consequence != "" {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), consequence)
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "type %q to confirm: ", want)
	var got string
	_, _ = fmt.Fscanln(in, &got)
	return got == want
}
