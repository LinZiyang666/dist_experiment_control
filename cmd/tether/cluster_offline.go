package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// cluster_offline.go — D7 §8.4 OFFLINE + local cluster verbs. force-single / recover
// run with the daemon STOPPED and touch disk directly via internal/clusteroffline
// (which calls cluster.RecoverSingleNode; raft never enters cmd/tether). They NEVER
// honor --yes — a typed node_id is mandatory (§8.1).

const (
	defaultDataDir = "/var/lib/tether"
	defaultDBPath  = "/var/lib/tether/tether.db"
	defaultSeed    = "/etc/tether/node-ident.nk"
)

func newClusterForceSingleCmd() *cobra.Command {
	var dataDir, dbPath, selfID, selfAddr string
	var confirmDead []string
	cmd := &cobra.Command{
		Use:   "force-single",
		Short: "ESCAPE HATCH: rewrite this node to a single-voter cluster (daemon must be STOPPED; runbook in docs/)",
		Long: `force-single is the quorum-loss escape hatch. STOP the daemon first
(systemctl mask tether && systemctl stop tether). It HARD-REFUSES if any peer is
still reachable, if there is no existing raft state, or if a daemon still holds the
store. It NEVER accepts --yes; you must type this node's id to confirm.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if selfID == "" || selfAddr == "" {
				return fmt.Errorf("force-single requires --self-id and --self-addr")
			}
			if len(confirmDead) == 0 {
				return fmt.Errorf("force-single requires --confirm-peers-dead listing EVERY other node_id (a missed peer would split-brain)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"force-single will rewrite %q to a single-voter cluster (NO HA / NO integrity until recover).\n"+
					"Confirm the peers %v are TRULY dead (powered off / unreachable to agents), not merely partitioned.\n",
				selfID, confirmDead)
			if !confirmTypedNodeID(cmd, selfID) {
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
					"systemctl unmask tether && systemctl start tether, then recover the others.\n", selfID, len(abandoned))
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (holds raft/ + tether.db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&selfID, "self-id", "", "this node's cluster node_id")
	cmd.Flags().StringVar(&selfAddr, "self-addr", "", "this node's raft address (host:7400)")
	cmd.Flags().StringSliceVar(&confirmDead, "confirm-peers-dead", nil, "node_ids of EVERY other roster node (comma-separated)")
	return cmd
}

func newClusterRecoverCmd() *cobra.Command {
	var dataDir, dbPath, dumpPath, selfID string
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Wipe a returning node after a forensic divergence dump, so it can rejoin clean (daemon STOPPED)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dumpPath == "" {
				return fmt.Errorf("recover requires --dump-divergent <file> (the forensic dump path)")
			}
			if selfID == "" {
				return fmt.Errorf("recover requires --self-id (the node_id being wiped, to confirm you target the right node)")
			}
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"recover will DUMP node %q's divergent rows to %s then WIPE raft/ + tether.db.\n"+
					"The dump is forensic-only and NOT auto-mergeable.\n", selfID, dumpPath)
			// Confirm on the node_id (B3): typing a fixed word gives zero protection
			// against running recover against the wrong --data-dir/--db.
			if !confirmTypedNodeID(cmd, selfID) {
				return fmt.Errorf("aborted (type the node_id to confirm; --yes is never accepted)")
			}
			n, err := clusteroffline.Recover(clusteroffline.RecoverOptions{DataDir: dataDir, DBPath: dbPath, DumpPath: dumpPath})
			if err != nil {
				return fmt.Errorf("recover: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "recover complete: %d rows dumped to %s, node %q wiped. Run `cluster add` to rejoin.\n", n, dumpPath, selfID)
			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&dumpPath, "dump-divergent", "", "forensic dump output path (0600, must not pre-exist)")
	cmd.Flags().StringVar(&selfID, "self-id", "", "the node_id being wiped (typed to confirm)")
	return cmd
}

func newClusterSignJoinCmd() *cobra.Command {
	var seedPath string
	cmd := &cobra.Command{
		Use:   "sign-join <node-id> <nonce>",
		Short: "Sign a leader-issued join nonce with this node's identity key (run on the JOINING node)",
		Args:  cobra.ExactArgs(2),
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
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s:%s\n", nonce, hex.EncodeToString(sig))
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "node-pub: %s\n(paste the line above as --join-token on the leader)\n", pub)
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	return cmd
}

func newClusterNodePubCmd() *cobra.Command {
	var seedPath string
	cmd := &cobra.Command{
		Use:   "node-pub",
		Short: "Print this node's identity public key (for `cluster add`)",
		Args:  cobra.NoArgs,
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
		Use:   "keygen",
		Short: "Generate a new node-identity key (prints the pubkey; writes the seed 0600)",
		Args:  cobra.NoArgs,
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

// confirmTypedNodeID requires the operator to TYPE want exactly (no --yes accepted).
// Refuses on a non-interactive stdin.
func confirmTypedNodeID(cmd *cobra.Command, want string) bool {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "refusing: this confirmation requires an interactive terminal (no --yes for destructive cluster ops)")
		return false
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "type %q to confirm: ", want)
	var got string
	_, _ = fmt.Fscanln(os.Stdin, &got)
	return got == want
}
