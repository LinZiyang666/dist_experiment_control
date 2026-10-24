package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/spf13/cobra"
)

// cluster.go — D7 §8.1 / C8 `tether cluster *` command plane. ONLINE verbs talk to the
// broker's local admin socket (admin strictly local, no network bypass); a
// non-leader broker replies NotLeader+LeaderHost so we tell the operator where to
// re-run (§8.1 fail-fast, NO forwarding). The OFFLINE escape hatches (force-single /
// rejoin prepare / restore) live under the `recovery` group + cluster_offline.go and
// touch disk directly. C8: `add`/`sign-join`/`wait` were deleted; node-pub/keygen are
// hidden debug; the former top-level escapes are hidden one-cycle deprecated aliases.

func newClusterCmd() *cobra.Command {
	var socketPath string
	root := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster lifecycle: join/drain/retire, status, and the recovery escape hatches",
		Long: `Cluster admin commands. ONLINE verbs (join/drain/retire/transfer-leader/
status/rotate-tunnel-cert) talk to the broker's local admin socket and must run on a
broker host. The recovery group holds the escape hatches (force-single / rejoin prepare
/ restore / incident export / raw node remove); its OFFLINE members run with the daemon
STOPPED and operate directly on disk (see the runbook in docs/).`,
	}
	root.PersistentFlags().StringVar(&socketPath, "socket", defaultAdminSocket, "broker admin Unix socket")

	// B3 item 5 / C8: group the visible subcommands by context + danger tier so `tether cluster --help`
	// makes where-do-I-run-this / how-dangerous obvious. C8 dropped the `local` group (sign-join deleted;
	// node-pub/keygen demoted to hidden debug) → 3 groups.
	root.AddGroup(
		&cobra.Group{ID: "online", Title: "Online (leader, daemon running):"},
		&cobra.Group{ID: "client", Title: "Client discovery (laptop): broker auto-failover pin/invite:"},
		&cobra.Group{ID: "migrate", Title: "Migration / nats.conf takeover (one-time):"},
		&cobra.Group{ID: "escape", Title: "DANGER -- recovery + raw escape hatches (runbook section 3):"},
	)
	addGrouped := func(c *cobra.Command, group string) { c.GroupID = group; root.AddCommand(c) }
	addGrouped(newClusterStatusCmd(&socketPath), "online")
	addGrouped(newClusterDrainCmd(&socketPath), "online")
	addGrouped(newClusterTransferCmd(&socketPath), "online")
	addGrouped(newClusterRotateCertCmd(&socketPath), "online")
	addGrouped(newClusterSetRaftAddrCmd(&socketPath), "online")
	addGrouped(newClusterBackupCmd(&socketPath), "online")
	addGrouped(newClusterOpsCmd(&socketPath), "online")
	addGrouped(newClusterApplyCmd(&socketPath), "online")
	addGrouped(newClusterSeedsCmd(&socketPath), "online")     // C2: seeds publish/show
	addGrouped(newClusterReconcileCmd(&socketPath), "online") // C3: reconcile nats --all --wait / --manual
	addGrouped(newClusterJoinCmd(&socketPath), "online")      // C4: join prepare/approve (recoverable; replaces `add`/`sign-join`)
	addGrouped(newClusterRetireCmd(&socketPath), "online")    // C4: retire (recoverable; replaces `drain --retire`/`remove`)
	addGrouped(newClusterRebalanceCmd(&socketPath), "online") // C-rebalance: rebalance proxy (spread __proxy__ homes)
	addGrouped(newClusterUpgradeCmd(), "online")              // G5 #13/#14: rolling broker-daemon upgrade
	addGrouped(newClusterAddCmd(&socketPath), "online")       // G4 §B: grow orchestration (joiner-local + leader-over-NATS)
	addGrouped(newClusterUnlockCmd(), "online")               // R7b: clear a stale roll/grow lock without waiting out its lease TTL
	addGrouped(newClusterPinCmd(), "client")                  // cli-failover: pin a cluster from an OOB discovery invite
	addGrouped(newClusterInviteCmd(), "client")               // cli-failover: mint an OOB discovery invite
	addGrouped(newClusterInitCmd(), "migrate")
	addGrouped(newClusterTakeoverNatsconfCmd(), "migrate") // hidden deprecated alias for reconcile nats --manual
	addGrouped(newClusterDoctorCmd(), "migrate")
	// C8: `recovery` is now the PRIMARY home of the escape verbs (force-single/rejoin prepare/restore/
	// incident export/node remove) + diagnose.
	addGrouped(newClusterRecoveryCmd(&socketPath), "escape")

	// C8 hidden one-cycle DEPRECATED aliases — warn to stderr + delegate to the same constructor (so
	// every gate is byte-preserved), then deleted next release. node-pub/keygen are hidden DEBUG (kept;
	// keygen is the only node-ident.nk minter). The deleted `add`/`sign-join`/`wait` are simply gone.
	root.AddCommand(
		deprecatedClusterAlias(newClusterForceSingleCmd(), "recovery force-single"),
		deprecatedClusterAlias(newClusterRecoverCmd(), "recovery rejoin prepare"),
		deprecatedClusterAlias(newClusterRestoreCmd(), "recovery restore"),
		deprecatedClusterAlias(newClusterExportIncidentCmd(&socketPath), "recovery incident export"),
		deprecatedClusterAlias(newClusterRemoveCmd(&socketPath), "recovery node remove --manual"),
		hiddenDebugCmd(newClusterNodePubCmd()),
		hiddenDebugCmd(newClusterKeygenCmd()),
	)
	return root
}

// deprecatedClusterAlias (C8) wraps a relocated command as a HIDDEN one-release deprecated alias: it
// warns to STDERR (never stdout — D2: cobra's own .Deprecated prints via OutOrStderr which rides the
// SetOut writer, polluting `export-incident`'s JSON) then delegates to the SAME RunE (so every safety
// gate is byte-preserved). GroupID="" so dropping a group can't trip checkCommandGroups. Deleted next
// release.
func deprecatedClusterAlias(c *cobra.Command, replacement string) *cobra.Command {
	c.Hidden, c.GroupID = true, ""
	inner := c.RunE
	c.RunE = func(cmd *cobra.Command, a []string) error {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
			"deprecated: `tether cluster %s` moved to `tether cluster %s`; this alias is removed next release.\n",
			c.Name(), replacement)
		if inner == nil {
			return cmd.Help()
		}
		return inner(cmd, a)
	}
	return c
}

// hiddenDebugCmd (C8) demotes a now-internal helper (node-pub / keygen) to a hidden debug command —
// still runnable (keygen is the only in-binary minter of node-ident.nk) but off the documented surface.
func hiddenDebugCmd(c *cobra.Command) *cobra.Command {
	c.Hidden, c.GroupID = true, ""
	return c
}

func newClusterDrainCmd(socketPath *string) *cobra.Command {
	var retire, now, abort, confirmed bool
	cmd := &cobra.Command{
		Use:     "drain <node-id>",
		Short:   "Drain (migrate exposes off, keep it a voter)",
		Example: "  tether cluster drain brk-b            # migrate rebuild-ON exposes off, keep it a voter",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			node := args[0]
			// C8 (D4): `drain --retire` is removed — redirect to the resumable `cluster retire` operation
			// (NOT a deprecated flag, which would still parse + honor → a live second mutation path). A
			// MarkHidden retire flag is kept only so this redirect can fire instead of "unknown flag".
			if retire {
				return usageErr("`cluster drain --retire` is removed; run `tether cluster retire %s` (resumable, same F==0 gate, migrates exposes + waits for stream redundancy)", node)
			}
			req := adminsock.Request{Op: adminsock.OpClusterDrain, NodeID: node, Retire: false, Now: now, Abort: abort, Confirmed: confirmed}
			resp, err := callAdmin(*socketPath, req)
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			// F==0 confirm gate: NEVER honored by --yes; require a typed node_id.
			if resp.QuorumProj != nil {
				p := resp.QuorumProj
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"WARNING: after this op the cluster has %d voters, quorum=%d, tolerates %d failures.\n",
					p.Voters, p.Quorum, p.FaultTolerance)
				if !confirmTypedNodeID(cmd, node, "", false, "") { // never-escapable: F==0 destroys fault tolerance
					return usageErr("aborted (type the node_id to confirm an F==0 drain; --yes is not accepted)")
				}
				req.Confirmed = true
				resp, err = callAdmin(*socketPath, req)
				if err != nil {
					return err
				}
			}
			if resp.Error != "" {
				return clusterAdminError("drain", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "drain %s ok\n", node)
			return nil
		},
	}
	cmd.Flags().BoolVar(&retire, "retire", false, "REMOVED: use `cluster retire` instead")
	cmd.Flags().BoolVar(&now, "now", false, "skip the drain notice period")
	cmd.Flags().BoolVar(&abort, "abort", false, "abort an in-progress drain (return the node to VOTER)")
	_ = cmd.Flags().MarkHidden("retire") // C8 (D4): kept only so --retire redirects, not "unknown flag"
	return cmd
}

func newClusterRemoveCmd(socketPath *string) *cobra.Command {
	var force, manual bool
	var confirmNodeID string
	cmd := &cobra.Command{
		Use:   "remove <node-id>",
		Short: "RAW raft-config + roster removal (escape; prefer `cluster retire`)",
		Example: "  # PREFER cluster retire (migrates exposes off + waits for stream redundancy first, resumable):\n" +
			"  tether cluster retire brk-b --wait\n" +
			"  # last resort — raw removal of a node that can't retire (e.g. VOTER_ADD_FAILED); --manual is REQUIRED:\n" +
			"  tether cluster recovery node remove brk-b --manual --force\n" +
			"  # unattended (CI/automation): BOTH the flag AND the env must equal the node_id:\n" +
			"  TETHER_CONFIRM_NODE_ID=brk-b tether cluster recovery node remove brk-b --manual --confirm-node-id brk-b",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := rejectedUnattendedYes(cmd, "remove", args[0]); err != nil {
				return err
			}
			// C8 (D3): raw remove is fail-closed behind --manual — the routine path is `cluster retire`
			// (a resumable operation). Without --manual, refuse + redirect. Ordered AFTER the
			// unattended-yes rejection so `--yes` still yields "cannot run unattended".
			if !manual {
				return usageErr("raw `remove` requires --manual (this is the last-resort escape); the routine path is `tether cluster retire %s` (resumable, migrates exposes + waits for stream redundancy)", args[0])
			}
			// remove is the ONLY machine-confirm-escapable typed-confirm (reversible: re-add with
			// `cluster join`). The escape needs BOTH --confirm-node-id AND the env to equal the id.
			if !confirmTypedNodeID(cmd, args[0], "", true, confirmNodeID) {
				return usageErr("aborted (type the node_id to confirm, or pass --confirm-node-id + %s for unattended use)", machineConfirmEnv)
			}
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRemove, NodeID: args[0], Force: force})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("remove", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&manual, "manual", false, "REQUIRED: acknowledge this is the last-resort raw removal (the routine path is `cluster retire`)")
	cmd.Flags().BoolVar(&force, "force", false, "remove even if a VOTER_ADD_FAILED node still homes exposes (orphans them; prefer `cluster retire`)")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "unattended confirm: must equal the node-id AND match $"+machineConfirmEnv+" (no TTY)")
	registerYesRejector(cmd)
	return cmd
}

func newClusterTransferCmd(socketPath *string) *cobra.Command {
	var wait bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:     "transfer-leader <node-id>",
		Short:   "Hand raft leadership to a caught-up voter",
		Example: "  tether cluster transfer-leader brk-b --wait   # block until brk-b is leader",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterTransfer, NodeID: args[0]})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("transfer-leader", resp)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "leadership transfer requested")
			if wait {
				node := args[0]
				// Converged once leadership lands on the target. LeaderID=="" is mid-election → keep
				// waiting (not a failure).
				return waitForConverge(cmd, *socketPath, node, func(_ *adminsock.ClusterNodeStatus, rep *adminsock.ClusterStatusReport) (bool, string) {
					return rep.LeaderID == node, ""
				}, timeout, minWatchInterval)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "block until leadership lands on the target (B5 OPS#13)")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "give up waiting after this long (exit 75)")
	return cmd
}

func newClusterRotateCertCmd(socketPath *string) *cobra.Command {
	var certFP string
	cmd := &cobra.Command{
		Use:     "rotate-tunnel-cert <node-id>",
		Short:   "Rotate a node's stable tunnel cert (cert_pins {current,previous,valid_until})",
		Example: "  tether cluster rotate-tunnel-cert brk-a --cert-fp sha256:…   # run on the target broker (it must be leader)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRotateCrt, NodeID: args[0], CertFP: certFP})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("rotate-tunnel-cert", resp)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "cert rotation committed; target broker hot-swapped its live tunnel certificate")
			return nil
		},
	}
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "new cert fingerprint (sha256:...)")
	return cmd
}

// newClusterSetRaftAddrCmd is the v0.4.2 ONLINE raft/NATS advertise-address rebind — the routine
// replacement for force-single when preparing a single broker to grow cross-network. It rewrites
// the raft advertise address in place (roster column + raft Configuration via AddVoter, NOT a
// wipe); --route also rebinds the NATS route-mesh advertise (the twin defect). Leader-only
// (fails fast naming the leader). Defaults to self.
func newClusterSetRaftAddrCmd(socketPath *string) *cobra.Command {
	var route string
	var allowLoopback bool
	cmd := &cobra.Command{
		Use:   "set-raft-addr <host:port>",
		Short: "Rebind THIS broker's raft advertise address ONLINE (routine replacement for force-single)",
		Long: "Rewrite the LEADER's OWN raft advertise address IN PLACE — both the cluster_nodes.raft_addr\n" +
			"roster column and the committed raft Configuration (via AddVoter on the existing voter, an online\n" +
			"address update, NOT the force-single wipe). This is the routine localhost->public rebind that\n" +
			"prepares a single broker to grow cross-network; force-single is reserved for genuine quorum loss.\n" +
			"SELF-ONLY by design — rebinding a peer's address would wedge the cluster, so to readdress a\n" +
			"follower `cluster transfer-leader` to it first, then run this there. --route also rebinds the\n" +
			"NATS route-mesh advertise (the twin). Run on the raft leader.",
		Example: "  tether cluster set-raft-addr 155.98.36.32:7400 --route nats://155.98.36.32:6222   # on the leader",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{
				Op: adminsock.OpClusterSetRaftAddr, Host: args[0], AllowLoopback: allowLoopback,
			})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("set-raft-addr", resp)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "raft advertise address rebound (roster column + raft config updated in place)")
			if route != "" {
				rresp, err := callAdmin(*socketPath, adminsock.Request{
					Op: adminsock.OpClusterSetRoute, NatsRoute: route, AllowLoopback: allowLoopback,
				})
				if err != nil {
					return err
				}
				if err := leaderRedirect(cmd, rresp); err != nil {
					return err
				}
				if rresp.Error != "" {
					return clusterAdminError("set-route", rresp)
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "NATS route advertise rebound; run `tether cluster reconcile nats` to re-render the mesh")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&route, "route", "", "also rebind the NATS route-mesh advertise (nats://host:port)")
	cmd.Flags().BoolVar(&allowLoopback, "allow-loopback", false, "allow a loopback/unspecified advertise (single-host dev only)")
	return cmd
}

func newClusterInitCmd() *cobra.Command {
	var (
		fromExisting  bool
		dataDir       string
		dbPath        string
		secretsDir    string
		selfID        string
		name          string
		nodeIdentPub  string
		raftAddr      string
		natsRoute     string
		tunnelAddr    string
		publicHost    string
		fromManifest  string
		check         bool
		dryRun        bool
		confirmNodeID string // G4 #5: machine-escape confirm (with $TETHER_CONFIRM_NODE_ID) for unattended grow
		configPath    string // G4 #5: broker.yaml to auto-apply the cluster seam into (was print-only)
		natsConfPath  string // N-4c: the nats.conf path recorded into the seam + used for the doctor/identity cross-check
	)
	cmd := &cobra.Command{
		Use:     "init --from-existing",
		Aliases: []string{"migrate-to-cluster"}, // B3 item 3: clearer verb, same RunE; `init` stays primary (zero D9-doc divergence)
		Short:   "ONE-TIME migration of THIS live single broker into a single-voter cluster (step 2 of a 6-step migration — run `cluster doctor` first)",
		Example: "  tether cluster doctor                                    # 0. preflight first\n" +
			"  systemctl stop tether-broker\n" +
			"  tether cluster init --from-existing --check --self-id brk-a …   # dry-run (no mutation)\n" +
			"  tether cluster init --from-existing --self-id brk-a …          # the real migration; prints the NEXT steps",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := rejectedUnattendedYes(cmd, "init", selfID); err != nil {
				return err
			}
			if !fromExisting && fromManifest == "" {
				return usageErr("cluster init: pass --from-existing to migrate this broker's DB into a cluster " +
					"(there is no fresh-bootstrap path; a node always migrates an existing DB)")
			}
			// B3 item 3 (review m1): --check / --dry-run runs the read-only doctor preflight and
			// exits WITHOUT mutating. It runs BEFORE the full identity gate — the dry-run only
			// consumes doctor-relevant fields (secrets/db/conf + optional raft/nats addrs), so it
			// must not demand --name/--node-ident-pub/--tunnel-addr/--public-host the real run needs.
			if check || dryRun {
				checks := clusteroffline.Doctor(clusteroffline.DoctorOptions{
					SecretsDir: secretsDir, DBPath: dbPath, ConfPath: natsConfPath, RaftAddr: raftAddr, NatsRoute: natsRoute,
				})
				return renderDoctor(cmd, checks, false)
			}
			// --from-manifest (B6 OPS#11): read the identity from a recover/backup manifest instead
			// of the 9 flags. Populate the locals so the typed-confirm + the post-success NEXT-steps
			// print use the manifest's identity; cert_fp is still re-derived LIVE by the seed.
			if fromManifest != "" {
				m, err := clusteroffline.ReadManifest(fromManifest)
				if err != nil {
					return err
				}
				selfID, name, nodeIdentPub = m.SelfID, m.Name, m.NodeIdentPub
				raftAddr, natsRoute, tunnelAddr, publicHost = m.RaftAddr, m.NatsRoute, m.TunnelAddr, m.PublicHost
			} else if missing := missingClusterInitFields(map[string]string{
				"--self-id":        selfID,
				"--name":           name,
				"--node-ident-pub": nodeIdentPub,
				"--raft-addr":      raftAddr,
				"--nats-route":     natsRoute,
				"--tunnel-addr":    tunnelAddr,
				"--public-host":    publicHost,
			}); len(missing) > 0 {
				return usageErr("cluster init --from-existing requires %s", strings.Join(missing, ", "))
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
			// G4 #5: machine-ESCAPABLE confirm. init is irreversible but NOT quorum-destructive (it migrates
			// this node's own/empty DB and cannot fork an existing quorum), so it joins resnapshot/remove's
			// escapable tier — `cluster add` can drive it unattended via --confirm-node-id + $TETHER_CONFIRM_NODE_ID.
			// The TTY path still HARD-REFUSES a non-interactive terminal unless BOTH flag AND env equal self-id.
			if !confirmTypedNodeID(cmd, selfID,
				"CONSEQUENCE: one-way v1→v2 migration of this broker's DB in place; rollback = restore tether.db.bak.",
				true, confirmNodeID) {
				return usageErr("cluster init: aborted (type this node's id to confirm, or pass --confirm-node-id + $%s for unattended use)", machineConfirmEnv)
			}
			var initErr error
			if fromManifest != "" {
				initErr = clusteroffline.InitFromManifest(fromManifest, dataDir, dbPath, secretsDir, nil, nil)
			} else {
				initErr = clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{
					DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
					SelfID: selfID, Name: name, NodeIdentPub: nodeIdentPub,
					RaftAddr: raftAddr, NatsRoute: natsRoute, TunnelAddr: tunnelAddr, PublicHost: publicHost,
				})
			}
			if initErr != nil {
				return initErr
			}
			// G4 #5: APPLY the broker.yaml cluster seam (it was previously only PRINTED as NEXT-step 3, so an
			// operator/automation had to hand-append it). Idempotent: skips if a cluster.raft_addr seam is
			// already present. This lets `cluster add` (and a manual init) leave a serve-ready broker.yaml.
			seamApplied := false
			if configPath != "" {
				applied, aerr := applyClusterSeam(configPath, dataDir, raftAddr, secretsDir, natsConfPath)
				if aerr != nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "note: could not auto-apply the broker.yaml cluster seam (%v) — set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path} by hand\n", aerr)
				}
				seamApplied = applied
			}
			// Halt-and-print the restart sequence (tether does NOT orchestrate systemctl;
			// d9-plan OQ-3). cluster mode needs the new nats.conf authorization{} ACL live
			// before the broker connects.
			routeURL := natsRoute
			if routeURL != "" && !strings.Contains(routeURL, "://") {
				routeURL = "nats://" + routeURL
			}
			// B3 item 1: substitute the REAL account/broker PUBLIC nkeys (cross-checked against
			// nats.conf) so step 1 is copy-paste-ready. Fail-closed: an unresolved/mismatched value
			// stays a <placeholder> + a NOTE — this print is POST-success and never fails the cmd.
			ids := readClusterPublicIdentities(secretsDir, natsConfPath)
			acctTok := orPlaceholder(ids.AccountIssuer, "<account-public-nkey>")
			brkTok := orPlaceholder(ids.BrokerNkey, "<broker-public-nkey>")
			out := cmd.OutOrStdout()
			fillHint := ""
			if ids.AccountIssuer == "" || ids.BrokerNkey == "" {
				fillHint = " (fill the <…> placeholder(s) in step 1 below)"
			}
			_, _ = fmt.Fprintf(out, "cluster init --from-existing complete — %s is now a single-voter cluster (data_dir %s).%s\n", selfID, dataDir, fillHint)
			_, _ = fmt.Fprintln(out, "NEXT (run in order):")
			_, _ = fmt.Fprintf(out, "  1. tether cluster reconcile nats --manual --conf %s --secrets-dir %s --server-name %s --route-url %s --account-issuer %s --broker-nkey %s\n",
				natsConfPath, secretsDir, selfID, routeURL, acctTok, brkTok)
			// N-3: only claim "verified against nats.conf" when the cross-check actually RAN. On a stock
			// pre-cutover conf (no auth_callout issuer yet) the issuer is UNVERIFIED — printing "verified"
			// there was an outright false claim; the ids.Note (UNVERIFIED sentence) is printed instead.
			if ids.AccountIssuer != "" && ids.BrokerNkey != "" && !ids.IssuerUnverified {
				_, _ = fmt.Fprintln(out, "     # account-issuer auto-read from this host's secrets + verified against nats.conf")
				_, _ = fmt.Fprintln(out, "     # (broker-nkey verified too on a single-user conf; otherwise read from broker.nk)")
			}
			if ids.Note != "" {
				_, _ = fmt.Fprintf(out, "     %s\n", ids.Note)
			}
			_, _ = fmt.Fprintln(out, "  2. systemctl restart nats-server                       # bring up the new conf")
			if seamApplied {
				_, _ = fmt.Fprintf(out, "  3. ✓ broker.cluster seam auto-applied to %s (#5)\n", configPath)
			} else {
				_, _ = fmt.Fprintln(out, "  3. set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path} in broker.yaml")
				_, _ = fmt.Fprintf(out, "     # nats_conf_path: %s  (the reconciler's conf; #22 — set it explicitly so a future binary upgrade can't repoint it)\n", natsConfPath)
			}
			_, _ = fmt.Fprintln(out, "  4. systemctl start tether-broker                       # starts in cluster mode (N=1)")
			_, _ = fmt.Fprintln(out, "  5. reinstall ALL agents on v2, then `tether cluster join prepare`/`approve` to grow to N>=3")
			return nil
		},
	}
	cmd.Flags().BoolVar(&fromExisting, "from-existing", false, "migrate this live single broker into a cluster")
	cmd.Flags().StringVar(&fromManifest, "from-manifest", "", "read node identity from a recover/backup manifest instead of the 9 identity flags (cert_fp re-derived live)")
	// Stage-C M-e: --from-existing (9 flags) and --from-manifest are two ways to supply the SAME
	// identity — passing both silently preferred the manifest. Make it a loud usage error.
	cmd.MarkFlagsMutuallyExclusive("from-existing", "from-manifest")
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
	cmd.Flags().BoolVar(&check, "check", false, "dry-run: run the read-only doctor preflight and exit WITHOUT mutating")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "alias of --check")
	cmd.Flags().StringVar(&confirmNodeID, "confirm-node-id", "", "unattended confirm (#5): must equal --self-id AND match $"+machineConfirmEnv+" (no TTY; for `cluster add`)")
	cmd.Flags().StringVar(&configPath, "config", defaultBrokerConfigPath, "broker.yaml to auto-apply the cluster seam into (#5; empty = only print it)")
	cmd.Flags().StringVar(&natsConfPath, "nats-conf", defaultNatsConfPath, "nats.conf path recorded into the broker.cluster seam + used for the --check doctor preflight and the printed-identity cross-check (N-4c)")
	registerYesRejector(cmd)
	return cmd
}

// applyClusterSeam (G4 #5) writes the broker.cluster seam into broker.yaml so a freshly-init'd broker is
// serve-ready without a hand edit. It is idempotent: a config that already carries a cluster.raft_addr is
// left untouched (returns applied=false). A missing config file is not an error (returns applied=false).
//
// External review B1: the seam is INSERTED as the first child of the top-level `broker:` block, NOT appended at
// EOF. An EOF append only lands under broker.cluster when `broker:` happens to be the LAST top-level key — a
// broker.yaml with any block after it (a `storage:` section, a trailing comment) would attach the seam to the
// wrong parent, leaving broker.cluster unset. The broker then boots SINGLE mode, which `cluster add` used to
// misjudge as clustered and stall. Inserting right after the `broker:` line is placement-independent, and the
// write is FAIL-CLOSED VERIFIED: the result must decode back with broker.cluster.raft_addr set, else this
// returns an error so the caller never proceeds on a config that will boot single-mode.
func applyClusterSeam(configPath, dataDir, raftAddr, secretsDir, natsConfPath string) (bool, error) {
	// N-4c: the seam records the ACTUAL nats.conf path this deploy uses (the reconciler reads it), not
	// a hardcoded default. An empty path is a caller bug — fail loud rather than silently substituting a
	// default that would drift from a --nats-conf-customized deployment.
	if natsConfPath == "" {
		return false, fmt.Errorf("applyClusterSeam: natsConfPath must not be empty (pass --nats-conf or the default)")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	// External re-review R2-B1 / R3-B1: decide idempotency by DECODING broker.cluster, not a `raft_addr:`
	// substring match, and require the seam to be COMPLETE + matching. An existing broker.cluster block is
	// idempotent ONLY if every field matches this node (raft_addr, data_dir, secrets_dir, nats_conf_path);
	// anything else — a stale/wrong raft_addr OR a partial seam missing the cluster-mode-trigger data_dir — is a
	// HARD error, never left in place (serve keys cluster mode on a non-empty data_dir, so a partial seam boots
	// SINGLE mode) and never inserted over (a second `cluster:` block would be malformed).
	// #75: a Load error MUST propagate, not fall through as "no existing seam".
	// With the strict serveconf decoder, an unparseable/typo'd broker.yaml
	// used to be silently treated as seam-less here — the seam got inserted,
	// and only the fail-closed verify at the tail blew up, with the cause
	// mis-attributed to the freshly-written seam instead of the pre-existing
	// typo. Refuse up front with the real parse error.
	existing, lerr := serveconf.Load(configPath)
	if lerr != nil {
		return false, fmt.Errorf("applyClusterSeam: %s does not parse (fix it before clustering): %w", configPath, lerr)
	}
	ec := existing.Broker.Cluster
	if ec.RaftAddr != "" || ec.DataDir != "" || ec.SecretsDir != "" || ec.NatsConfPath != "" {
		if ec.RaftAddr == raftAddr && ec.DataDir == dataDir && ec.SecretsDir == secretsDir && ec.NatsConfPath == natsConfPath {
			return false, nil // complete + matching (idempotent)
		}
		return false, fmt.Errorf("broker.yaml %s already has a broker.cluster seam that is stale/incomplete for this node "+
			"(have raft_addr=%q data_dir=%q secrets_dir=%q nats_conf_path=%q; want raft_addr=%q data_dir=%q secrets_dir=%q nats_conf_path=%q) — "+
			"fix or remove it, then re-run", configPath, ec.RaftAddr, ec.DataDir, ec.SecretsDir, ec.NatsConfPath, raftAddr, dataDir, secretsDir, natsConfPath)
	}
	seam := fmt.Sprintf("  cluster:\n    data_dir: %s\n    raft_addr: %s\n    secrets_dir: %s\n    nats_conf_path: %s\n    nats_server_bin: nats-server",
		dataDir, raftAddr, secretsDir, natsConfPath)
	lines := strings.Split(string(data), "\n")
	out := make([]string, 0, len(lines)+7)
	inserted := false
	for _, ln := range lines {
		out = append(out, ln)
		if !inserted && strings.TrimRight(ln, " \t\r") == "broker:" {
			out = append(out, seam)
			inserted = true
		}
	}
	if !inserted {
		return false, fmt.Errorf("no top-level `broker:` block in %s to attach the cluster seam to", configPath)
	}
	if werr := os.WriteFile(configPath, []byte(strings.Join(out, "\n")), 0o644); werr != nil {
		return false, werr
	}
	// Fail-closed: the write must actually decode into broker.cluster (catches wrong placement / a partial write).
	c, lerr := serveconf.Load(configPath)
	if lerr != nil {
		return false, fmt.Errorf("cluster seam written to %s but it no longer decodes: %w", configPath, lerr)
	}
	if c.Broker.Cluster.RaftAddr != raftAddr {
		return false, fmt.Errorf("cluster seam written to %s but broker.cluster.raft_addr did not take effect (got %q) — check the file structure", configPath, c.Broker.Cluster.RaftAddr)
	}
	return true, nil
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
// was already printed (avoid a duplicate error line). Classed as a permission error (77):
// the operator targeted the wrong host, not a transport/usage fault.
var errNonLeader = permErr("not leader")

// clusterAdminError wraps a cluster-admin reply error with the exit class derived from its machine
// Code (B2 item 4); the human message format is unchanged ("cluster <verb>: <error>").
func clusterAdminError(verb string, resp *adminsock.Response) error {
	return &ExitError{Class: brokerCodeExitClass(resp.Code), Err: fmt.Errorf("cluster %s: %s", verb, resp.Error)}
}

// registerYesRejector adds a HIDDEN, accepted `--yes` flag to a Tier-2 (irreversible /
// quorum-affecting) cluster command so rejectedUnattendedYes can give a helpful error instead of
// cobra's bare "unknown flag --yes" (B3 item 4). Reversible ops (drain/transfer-leader) have no
// --yes at all.
func registerYesRejector(cmd *cobra.Command) {
	cmd.Flags().Bool("yes", false, "")
	_ = cmd.Flags().MarkHidden("yes")
}

// rejectedUnattendedYes returns a helpful usage error when --yes is passed to a Tier-2 command;
// these require a human to TYPE the id at a TTY — there is NO unattended override by design.
func rejectedUnattendedYes(cmd *cobra.Command, verb, want string) error {
	if yes, _ := cmd.Flags().GetBool("yes"); yes {
		idHint := "the id"
		if want != "" {
			idHint = fmt.Sprintf("the id (%q)", want)
		}
		return usageErr("cluster %s cannot run unattended: this is an irreversible / quorum-affecting op, so it requires a human to TYPE %s at a TTY. There is NO --yes override for quorum-affecting ops by design. (Reversible ops like `drain`/`transfer-leader` need no confirm; see `tether cluster` help, \"how confirmations work\".)", verb, idHint)
	}
	return nil
}

// leaderRedirect prints the §8.1 fail-fast hint (where to re-run) and returns the right-CLASS error for
// a non-leader bounce, or nil if the response is not a bounce. Mega-audit MAJ-8: a bounce with NO
// LeaderHost is an election-in-progress (a routine failover) — a TRANSIENT (exit 75, "retry"), NOT a
// terminal permission error (77). A bounce that names a leader host IS terminal-for-this-host (77): the
// operator must re-run elsewhere.
func leaderRedirect(cmd *cobra.Command, resp *adminsock.Response) error {
	if !resp.NotLeader {
		return nil
	}
	if resp.LeaderHost != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "not the leader — re-run on the leader host: %s\n", resp.LeaderHost)
		return errNonLeader
	}
	_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "no leader (election in progress); retry shortly")
	return &ExitError{Class: exitTransient, Err: fmt.Errorf("no leader (election in progress)")}
}
