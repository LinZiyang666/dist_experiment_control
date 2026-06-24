package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
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
		Long: `Cluster admin commands. ONLINE verbs (add/remove/drain --retire/transfer-leader/
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
	root.AddCommand(newClusterTakeoverNatsconfCmd())
	root.AddCommand(newClusterDoctorCmd())
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
	rows, err := db.Query(`SELECT node_id, name, phase, cert_fp, raft_addr FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return fmt.Errorf("offline status: read roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type row struct{ NodeID, Name, Phase, CertFP, RaftAddr string }
	var roster []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.NodeID, &r.Name, &r.Phase, &r.CertFP, &r.RaftAddr); err != nil {
			return err
		}
		roster = append(roster, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var forceSingle string
	_ = db.QueryRow(`SELECT value FROM cluster_meta WHERE key='force_single_active'`).Scan(&forceSingle)

	// Emit the SAME versioned ClusterStatusReport schema as the online view (review F4).
	// §17 row 4: the offline view is a disk roster snapshot that ALSO TCP-pings each peer's
	// raft port (raft_addr) so the operator sees real per-peer liveness without a running
	// daemon. This is an advisory liveness hint; the authoritative live-peer gate is still
	// `cluster force-single --confirm-peers-dead` (which HARD-REFUSES any live peer).
	rep := &adminsock.ClusterStatusReport{
		SchemaVersion: 1, View: "offline", ExitCode: 0,
		Banner:   "disk roster snapshot with a raft-port TCP liveness probe (advisory); for quorum loss use `cluster force-single --confirm-peers-dead ...` (it re-probes + HARD-REFUSES any live peer)",
		NextStep: "cluster force-single --confirm-peers-dead <ids...>",
	}
	if forceSingle != "" {
		rep.Health = "FORCE_SINGLE"
		rep.ExitCode = 3
		rep.Banner = "force_single_active set at " + forceSingle + " — " + rep.Banner
	}
	for _, r := range roster {
		reachable := false
		source := "disk-snapshot"
		if r.RaftAddr != "" {
			if c, derr := net.DialTimeout("tcp", r.RaftAddr, 2*time.Second); derr == nil {
				_ = c.Close()
				reachable = true
			}
			source = "raft-ping"
		}
		rep.Nodes = append(rep.Nodes, adminsock.ClusterNodeStatus{
			NodeID: r.NodeID, Name: r.Name, Phase: r.Phase,
			Reachable: reachable, ReachSource: source,
		})
	}
	if asJSON {
		b, _ := json.MarshalIndent(rep, "", "  ")
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(b))
		if rep.ExitCode != 0 {
			os.Exit(rep.ExitCode)
		}
		return nil
	}
	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE_ID\tNAME\tPHASE\tRAFT_PING")
	for _, n := range rep.Nodes {
		ping := "DOWN"
		if n.Reachable {
			ping = "UP"
		}
		if n.ReachSource != "raft-ping" {
			ping = "?"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.NodeID, n.Name, n.Phase, ping)
	}
	_ = tw.Flush()
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "\n** offline roster view (disk snapshot + advisory raft-port ping) **\n%s\n", rep.Banner)
	if rep.ExitCode != 0 {
		os.Exit(rep.ExitCode)
	}
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
	var joinToken, tunnelAddr, publicHost, natsRoute, certFP string
	cmd := &cobra.Command{
		Use:   "add <node-id> <host> <node-pub>",
		Short: "Admit a new voter (two-phase: run without --join-token to get a nonce, sign it on the joiner, then re-run)",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// D9 round-1 BLOCKER + external-review Q2: the second (token) call MUST carry the
			// joiner's full expose-home + NATS identity (tunnel-addr + cert-fp + nats-route),
			// else the added voter can hold raft votes but never serve as an expose home nor be
			// rendered into a correct NATS topology.
			if joinToken != "" && (tunnelAddr == "" || certFP == "" || natsRoute == "") {
				return fmt.Errorf("cluster add: --tunnel-addr, --cert-fp and --nats-route are required on the " +
					"token call (an added voter without them can never serve as an expose home or NATS peer)")
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op: adminsock.OpClusterAdd, NodeID: args[0], Host: args[1], NodePub: args[2], JoinToken: joinToken,
				TunnelAddr: tunnelAddr, PublicHost: publicHost, NatsRoute: natsRoute, CertFP: certFP,
			})
			if err != nil {
				return err
			}
			if leaderRedirect(cmd, resp) {
				return errNonLeader
			}
			if resp.Nonce != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"challenge nonce: %s\non the joining node run:  tether cluster sign-join %s %s\nthen re-run:  tether cluster add %s %s %s --join-token <nonce>:<sig> --tunnel-addr <host:7000> --cert-fp <sha256:...> --public-host <dns> --nats-route nats://<host>:6222\n",
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
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "joiner's public tunnel addr host:port (required on the token call)")
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "joiner's stable tunnel cert fingerprint sha256:… (required on the token call)")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "joiner's public host (defaults to <host>)")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "joiner's NATS route URL, e.g. nats://10.0.0.2:6222")
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
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cert rotation committed; target broker hot-swapped its live tunnel certificate")
			return nil
		},
	}
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "new cert fingerprint (sha256:...)")
	return cmd
}

func newClusterInitCmd() *cobra.Command {
	var (
		fromExisting bool
		dataDir      string
		dbPath       string
		secretsDir   string
		selfID       string
		name         string
		nodeIdentPub string
		raftAddr     string
		natsRoute    string
		tunnelAddr   string
		publicHost   string
	)
	cmd := &cobra.Command{
		Use:   "init --from-existing",
		Short: "Migrate THIS live single broker into a single-voter cluster (the D9 one-time migration)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !fromExisting {
				return fmt.Errorf("cluster init: pass --from-existing to migrate this broker's DB into a cluster " +
					"(there is no fresh-bootstrap path; a node always migrates an existing DB)")
			}
			if missing := missingClusterInitFields(map[string]string{
				"--self-id":        selfID,
				"--name":           name,
				"--node-ident-pub": nodeIdentPub,
				"--raft-addr":      raftAddr,
				"--nats-route":     natsRoute,
				"--tunnel-addr":    tunnelAddr,
				"--public-host":    publicHost,
			}); len(missing) > 0 {
				return fmt.Errorf("cluster init --from-existing requires %s", strings.Join(missing, ", "))
			}
			// Loud v1→v2 wire-break warning: a one-way, flag-day migration. The daemon
			// MUST be stopped, and ALL agents reinstalled on v2 afterward (a v1 agent
			// cannot connect to a v2 broker). Rollback = restore tether.db.bak (to the v2
			// single broker, NOT the v1 fleet).
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(),
				"WARNING: cluster init --from-existing is a one-time, one-way migration.\n"+
					"  - Stop the broker first (systemctl stop tether-broker).\n"+
					"  - proto v2 breaks the wire: EVERY agent must be reinstalled on v2 afterward.\n"+
					"  - Rollback = restore tether.db.bak (to the v2 SINGLE broker; no path back to a v1 fleet).")
			if !confirmTypedNodeID(cmd, selfID) {
				return fmt.Errorf("cluster init: aborted (node_id not confirmed)")
			}
			if err := clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{
				DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
				SelfID: selfID, Name: name, NodeIdentPub: nodeIdentPub,
				RaftAddr: raftAddr, NatsRoute: natsRoute, TunnelAddr: tunnelAddr, PublicHost: publicHost,
			}); err != nil {
				return err
			}
			// Halt-and-print the restart sequence (tether does NOT orchestrate systemctl;
			// d9-plan OQ-3). cluster mode needs the new nats.conf authorization{} ACL live
			// before the broker connects.
			routeURL := natsRoute
			if routeURL != "" && !strings.Contains(routeURL, "://") {
				routeURL = "nats://" + routeURL
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"cluster init --from-existing complete — %s is now a single-voter cluster (data_dir %s).\n"+
					"NEXT (run in order):\n"+
					"  1. tether cluster takeover-natsconf --secrets-dir %s --server-name %s --route-url %s --account-issuer <account-public-nkey> --broker-nkey <broker-public-nkey>\n"+
					"     # --account-issuer may be read from existing auth_callout; --broker-nkey is auto-read only from a single-user authorization block\n"+
					"  2. systemctl restart nats-server                       # bring up the new conf\n"+
					"  3. set broker.cluster.{data_dir,raft_addr,secrets_dir} in broker.yaml\n"+
					"  4. systemctl start tether-broker                       # starts in cluster mode (N=1)\n"+
					"  5. reinstall ALL agents on v2, then `tether cluster add` to grow to N>=3\n",
				selfID, dataDir, secretsDir, selfID, routeURL)
			return nil
		},
	}
	cmd.Flags().BoolVar(&fromExisting, "from-existing", false, "migrate this live single broker into a cluster")
	cmd.Flags().StringVar(&dataDir, "data-dir", defaultDataDir, "broker data dir (raft/ is created here)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", "/etc/tether/secrets", "§15 secrets dir (cluster-ca, route leaf, tunnel cert)")
	cmd.Flags().StringVar(&selfID, "self-id", "", "this node's cluster node_id (== deterministic nats server_name)")
	cmd.Flags().StringVar(&name, "name", "", "human-facing broker name (UNIQUE in the cluster)")
	cmd.Flags().StringVar(&nodeIdentPub, "node-ident-pub", "", "this node's node-identity public key")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "this node's raft transport address (host:7400, private net)")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "this node's NATS route address (host:6222)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "this node's public tunnel control address")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "this node's public DNS host")
	return cmd
}

func missingClusterInitFields(fields map[string]string) []string {
	order := []string{"--self-id", "--name", "--node-ident-pub", "--raft-addr", "--nats-route", "--tunnel-addr", "--public-host"}
	var missing []string
	for _, name := range order {
		if fields[name] == "" {
			missing = append(missing, name)
		}
	}
	return missing
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
