package main

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/spf13/cobra"
)

// cluster.go — D7 §8.1 `tether cluster *` command plane. ONLINE verbs talk to the
// broker's local admin socket (admin strictly local, no network bypass); a
// non-leader broker replies NotLeader+LeaderHost so we tell the operator where to
// re-run (§8.1 fail-fast, NO forwarding). OFFLINE verbs (force-single / recover /
// sign-join / node-pub / keygen) live in cluster_offline.go and touch disk directly.

func newClusterCmd() *cobra.Command {
	var socketPath string
	root := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster lifecycle: membership, drain/retire, status, and the force-single escape hatch",
		Long: `Cluster admin commands. ONLINE verbs (add/remove/drain/retire/transfer-leader/
status/rotate-tunnel-cert) talk to the broker's local admin socket and must run on a
broker host. OFFLINE verbs (force-single/recover) run with the daemon STOPPED and
operate directly on disk (see the runbook in docs/).`,
	}
	root.PersistentFlags().StringVar(&socketPath, "socket", defaultAdminSocket, "broker admin Unix socket")

	root.AddCommand(newClusterStatusCmd(&socketPath))
	root.AddCommand(newClusterAddCmd(&socketPath))
	root.AddCommand(newClusterDrainCmd(&socketPath))
	root.AddCommand(newClusterRemoveCmd(&socketPath))
	root.AddCommand(newClusterTransferCmd(&socketPath))
	root.AddCommand(newClusterRotateCertCmd(&socketPath))
	root.AddCommand(newClusterInitCmd())
	// Offline + local (cluster_offline.go).
	root.AddCommand(newClusterForceSingleCmd())
	root.AddCommand(newClusterRecoverCmd())
	root.AddCommand(newClusterSignJoinCmd())
	root.AddCommand(newClusterNodePubCmd())
	root.AddCommand(newClusterKeygenCmd())
	return root
}

func newClusterStatusCmd(socketPath *string) *cobra.Command {
	var asJSON, offline bool
	var dbPath string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster health (exit 0=HEALTHY-HA 1=DEGRADED 2=read-only/quorum-lost 3=force-single)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if offline {
				return clusterStatusOffline(cmd, dbPath, asJSON)
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterStatus})
			if err != nil {
				return err
			}
			if resp.Error != "" && resp.Cluster == nil {
				return fmt.Errorf("cluster status: %s", resp.Error)
			}
			rep := resp.Cluster
			if asJSON {
				b, _ := json.MarshalIndent(rep, "", "  ")
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
			} else {
				renderClusterStatus(cmd, rep)
			}
			os.Exit(rep.ExitCode) // §17 exit-code contract for cron/monitoring
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable JSON schema")
	cmd.Flags().BoolVar(&offline, "offline", false, "broker-local offline view (reads the on-disk roster; pass --db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (for --offline)")
	// doctor is an alias for status (the diagnostic framing).
	return cmd
}

// clusterStatusOffline reads the on-disk cluster_nodes roster directly (daemon may
// be down / quorum lost), so an operator can inspect membership without NATS. It
// does NOT probe peers over :7400 in this build (the force-single --confirm-peers-dead
// TCP-liveness gate is the authoritative live-peer check); it is a roster snapshot.
func clusterStatusOffline(cmd *cobra.Command, dbPath string, asJSON bool) error {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return fmt.Errorf("offline status: open %s: %w", dbPath, err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT node_id, name, phase, cert_fp FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return fmt.Errorf("offline status: read roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type row struct{ NodeID, Name, Phase, CertFP string }
	var roster []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.NodeID, &r.Name, &r.Phase, &r.CertFP); err != nil {
			return err
		}
		roster = append(roster, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var forceSingle string
	_ = db.QueryRow(`SELECT value FROM cluster_meta WHERE key='force_single_active'`).Scan(&forceSingle)

	// Emit the SAME versioned ClusterStatusReport schema as the online view (review
	// F4). The offline view is a DISK-ONLY roster snapshot: it does NOT probe peer
	// liveness over :7400 in this build (the force-single --confirm-peers-dead
	// TCP-liveness gate is the authoritative live-peer check; the offline raft-ping
	// view is deferred to D9). It therefore carries NO health/exit semantics —
	// Health is empty and ExitCode is 0, with a banner saying so.
	rep := &adminsock.ClusterStatusReport{
		SchemaVersion: 1, View: "offline", ExitCode: 0,
		Banner:   "disk-only roster snapshot — peer liveness NOT probed; for quorum loss use `cluster force-single --confirm-peers-dead ...` (it TCP-probes + HARD-REFUSES any live peer)",
		NextStep: "cluster force-single --confirm-peers-dead <ids...>",
	}
	if forceSingle != "" {
		rep.Health = "FORCE_SINGLE"
		rep.ExitCode = 3
		rep.Banner = "force_single_active set at " + forceSingle + " — " + rep.Banner
	}
	for _, r := range roster {
		rep.Nodes = append(rep.Nodes, adminsock.ClusterNodeStatus{
			NodeID: r.NodeID, Name: r.Name, Phase: r.Phase,
			Reachable: false, ReachSource: "disk-snapshot",
		})
	}
	if asJSON {
		b, _ := json.MarshalIndent(rep, "", "  ")
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
		return nil
	}
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE_ID\tNAME\tPHASE")
	for _, r := range roster {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", r.NodeID, r.Name, r.Phase)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "\n** offline roster view (disk-only; peer liveness NOT probed) **\n%s\n", rep.Banner)
	return nil
}

func renderClusterStatus(cmd *cobra.Command, rep *adminsock.ClusterStatusReport) {
	w := cmd.OutOrStdout()
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE_ID\tNAME\tPHASE\tROLE\tLAG\tACCT.NK\tSTREAMS\tREACH")
	for _, n := range rep.Nodes {
		acct := "N"
		if n.AccountNkMatch {
			acct = "Y"
		}
		reach := n.ReachSource
		if !n.Reachable {
			reach = "UNREACHABLE(" + n.ReachSource + ")"
		}
		flag := ""
		if n.Inconsistent {
			flag = " INCONSISTENT"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%d/%d\t%s%s\n",
			n.NodeID, n.Name, n.Phase, n.Role, n.AppliedLag, acct, n.StreamActual, n.StreamTarget, reach, flag)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintf(w, "\n** %s (exit %d) **\n", rep.Health, rep.ExitCode)
	if rep.Banner != "" {
		_, _ = fmt.Fprintf(w, "%s\n", rep.Banner)
	}
	if rep.NextStep != "" {
		_, _ = fmt.Fprintf(w, "next: %s\n", rep.NextStep)
	}
}

func newClusterAddCmd(socketPath *string) *cobra.Command {
	var joinToken string
	cmd := &cobra.Command{
		Use:   "add <node-id> <host> <node-pub>",
		Short: "Admit a new voter (two-phase: run without --join-token to get a nonce, sign it on the joiner, then re-run)",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op: adminsock.OpClusterAdd, NodeID: args[0], Host: args[1], NodePub: args[2], JoinToken: joinToken,
			})
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			if resp.Nonce != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"challenge nonce: %s\non the joining node run:  tether cluster sign-join %s %s\nthen re-run:  tether cluster add %s %s %s --join-token <nonce>:<sig>\n",
					resp.Nonce, args[0], resp.Nonce, args[0], args[1], args[2])
				return nil
			}
			if resp.Error != "" {
				return fmt.Errorf("cluster add: %s", resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "added %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&joinToken, "join-token", "", "<nonce>:<sigHex> from `cluster sign-join` on the joiner")
	return cmd
}

func newClusterDrainCmd(socketPath *string) *cobra.Command {
	var retire, now, abort, confirmed bool
	cmd := &cobra.Command{
		Use:   "drain <node-id>",
		Short: "Drain (migrate exposes off) and optionally --retire a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			node := args[0]
			req := adminsock.Request{Op: adminsock.OpClusterDrain, NodeID: node, Retire: retire, Now: now, Abort: abort, Confirmed: confirmed}
			resp, err := callAdmin(*socketPath, req)
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			// F==0 confirm gate: NEVER honored by --yes; require a typed node_id.
			if resp.QuorumProj != nil {
				p := resp.QuorumProj
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"WARNING: after this op the cluster has %d voters, quorum=%d, tolerates %d failures.\n",
					p.Voters, p.Quorum, p.FaultTolerance)
				if !confirmTypedNodeID(cmd, node) {
					return fmt.Errorf("aborted (type the node_id to confirm an F==0 drain; --yes is not accepted)")
				}
				req.Confirmed = true
				resp, err = callAdmin(*socketPath, req)
				if err != nil {
					return err
				}
			}
			if resp.Error != "" {
				return fmt.Errorf("cluster drain: %s", resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "drain %s ok\n", node)
			return nil
		},
	}
	cmd.Flags().BoolVar(&retire, "retire", false, "remove the node from the cluster after draining")
	cmd.Flags().BoolVar(&now, "now", false, "skip the drain notice period")
	cmd.Flags().BoolVar(&abort, "abort", false, "abort an in-progress drain (return the node to VOTER)")
	return cmd
}

func newClusterRemoveCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "remove <node-id>",
		Short: "Remove a node from the raft configuration + roster (prefer `drain --retire`)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !confirmTypedNodeID(cmd, args[0]) {
				return fmt.Errorf("aborted (type the node_id to confirm)")
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRemove, NodeID: args[0]})
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			if resp.Error != "" {
				return fmt.Errorf("cluster remove: %s", resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", args[0])
			return nil
		},
	}
}

func newClusterTransferCmd(socketPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "transfer-leader <node-id>",
		Short: "Hand raft leadership to a caught-up voter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterTransfer, NodeID: args[0]})
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			if resp.Error != "" {
				return fmt.Errorf("cluster transfer-leader: %s", resp.Error)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "leadership transfer requested")
			return nil
		},
	}
}

func newClusterRotateCertCmd(socketPath *string) *cobra.Command {
	var certFP string
	cmd := &cobra.Command{
		Use:   "rotate-tunnel-cert <node-id>",
		Short: "Rotate a node's stable tunnel cert (cert_pins {current,previous,valid_until})",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRotateCrt, NodeID: args[0], CertFP: certFP})
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			if resp.Error != "" {
				return fmt.Errorf("cluster rotate-tunnel-cert: %s", resp.Error)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cert rotation requested")
			return nil
		},
	}
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "new cert fingerprint (sha256:...)")
	return cmd
}

func newClusterInitCmd() *cobra.Command {
	var fromExisting bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a cluster (the live single-broker --from-existing migration is D9)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromExisting {
				return fmt.Errorf("cluster init --from-existing is the D9 one-time migration path; not available in this build")
			}
			return fmt.Errorf("cluster init: a node bootstraps as a single-voter cluster automatically; use `cluster add` to grow it")
		},
	}
	cmd.Flags().BoolVar(&fromExisting, "from-existing", false, "migrate a live single-broker into a cluster (D9)")
	return cmd
}

// errNonLeader is a sentinel so RunE exits non-zero after the leader-redirect hint
// was already printed (avoid a duplicate error line).
var errNonLeader = fmt.Errorf("not leader")

// leaderRedirect prints the §8.1 fail-fast hint (where to re-run) and reports whether
// the response was a non-leader bounce.
func leaderRedirect(cmd *cobra.Command, resp *adminsock.Response) bool {
	if !resp.NotLeader {
		return false
	}
	if resp.LeaderHost != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "not the leader — re-run on the leader host: %s\n", resp.LeaderHost)
	} else {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "no leader (election in progress); retry shortly")
	}
	return true
}
