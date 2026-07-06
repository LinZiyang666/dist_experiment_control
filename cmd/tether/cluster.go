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
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
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

func newClusterStatusCmd(socketPath *string) *cobra.Command {
	var asJSON, offline, remote, homes bool
	var dbPath, natsURL, home string
	var watch time.Duration
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster health (exit 0=HEALTHY-HA 1=DEGRADED 2=read-only/quorum-lost 3=force-single)",
		Example: "  tether cluster status                 # operator table (on a broker host)\n" +
			"  tether cluster status --watch 5s      # repaint every 5s (Ctrl-C to exit)\n" +
			"  tether cluster status --remote        # user summary over NATS (from a laptop, no socket)\n" +
			"  tether cluster status --offline --db /var/lib/tether/tether.db   # disk roster (daemon stopped)",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if homes {
				// C6 建议5: the homes aggregate (its own view; no exit-code contract — a descriptive read).
				return clusterStatusHomes(cmd, *socketPath, asJSON)
			}
			if offline {
				return clusterStatusOffline(cmd, dbPath, asJSON)
			}
			// B1: ctl-over-NATS user view (no broker admin socket). Explicit --remote only —
			// never auto-detect, so the operator socket path is never silently downgraded.
			if remote {
				return clusterStatusRemote(cmd, home, natsURL, asJSON)
			}
			// B5 OPS#5: --watch repaints on an interval (≥2s, since each socket frame can take
			// up to ~2s via the blocking StatusReport). One-shot (watch==0) is byte-identical.
			if watch > 0 {
				return watchClusterStatus(cmd, *socketPath, asJSON, watch)
			}
			rep, perr := fetchClusterStatusReport(*socketPath)
			if perr != nil {
				return perr
			}
			renderClusterStatusReport(cmd, rep, asJSON)
			os.Exit(rep.ExitCode) // §17 exit-code contract for cron/monitoring
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable JSON schema")
	cmd.Flags().BoolVar(&statusCardMode, "card", false, "glance card render (headline + what's wrong + what to do); default is the full table (B7 DOC#6)")
	cmd.Flags().BoolVar(&offline, "offline", false, "broker-local offline view (reads the on-disk roster; pass --db)")
	cmd.Flags().StringVar(&dbPath, "db", defaultDBPath, "tether.db path (for --offline)")
	cmd.Flags().DurationVar(&watch, "watch", 0, "repaint the status every interval (≥2s; Ctrl-C to exit); 0 = one-shot (B5 OPS#5)")
	// B1: ctl-over-NATS user view — no broker host / admin socket needed. Returns a user
	// summary (reachability + leader pointer), NOT the operator table or escape hatches.
	cmd.Flags().BoolVar(&remote, "remote", false, "query cluster health over NATS as a ctl (no broker admin socket; returns a user summary; exit 0/2/3 only, never 1/DEGRADED)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL (with --remote)")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir (with --remote)")
	// --offline (disk roster) and --remote (NATS user view) are different transports; forbid both
	// so a typo can't silently fall through to the offline path (B1 review F7).
	cmd.MarkFlagsMutuallyExclusive("offline", "remote")
	// --watch repaints the socket view; it makes no sense for the one-shot offline snapshot, and a
	// remote laptop watch-looping the member broadcast amplifies ACL traffic for little value.
	cmd.MarkFlagsMutuallyExclusive("offline", "watch")
	cmd.MarkFlagsMutuallyExclusive("remote", "watch")
	cmd.Flags().BoolVar(&homes, "homes", false, "C6: aggregate every expose + proxy home/epoch/ready_reason (one descriptive view; no exit-code contract)")
	cmd.MarkFlagsMutuallyExclusive("homes", "offline")
	cmd.MarkFlagsMutuallyExclusive("homes", "remote")
	cmd.MarkFlagsMutuallyExclusive("homes", "watch")
	// doctor is an alias for status (the diagnostic framing).
	return cmd
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// clusterStatusHomes fetches + renders the C6 homes aggregate (OpClusterHomes). No os.Exit / exit-code
// contract — it is a descriptive read (unlike `cluster status`'s 0/1/2/3).
func clusterStatusHomes(cmd *cobra.Command, socketPath string, asJSON bool) error {
	resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterHomes})
	if err != nil {
		return err
	}
	if resp.Homes == nil {
		msg := resp.Error
		if msg == "" {
			msg = "no homes report (single-broker mode?)"
		}
		return unavailErr("cluster status --homes: %s", msg)
	}
	rep := resp.Homes
	if asJSON {
		return emitJSON(cmd.OutOrStdout(), rep)
	}
	w := cmd.OutOrStdout()
	if !rep.IsLeaderView {
		_, _ = fmt.Fprintln(w, "(non-leader view — reachability is best-effort; run on the leader for authoritative tunnel-down)")
	}
	_, _ = fmt.Fprintf(w, "%-7s %-14s %-12s %-10s %-6s %-6s %-13s %-7s %-20s %s\n",
		"KIND", "SID", "NAME", "NID", "PORT", "EPOCH", "REASON", "READY", "HOME", "PUBLIC_URL")
	for _, h := range rep.Homes {
		nid := dashIfEmpty(h.NID)
		url := dashIfEmpty(h.PublicURL)
		_, _ = fmt.Fprintf(w, "%-7s %-14s %-12s %-10s %-6d %-6d %-13s %-7v %-20s %s\n",
			h.Kind, h.SID, h.Name, nid, h.Port, h.Epoch, h.ReadyReason, h.Ready, h.HomeBroker, url)
	}
	if len(rep.Homes) == 0 {
		_, _ = fmt.Fprintln(w, "(no allocated exposes or proxies)")
	}
	if rep.Partial {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "** PARTIAL ** %v\n", rep.Errors)
	}
	return nil
}

// fetchClusterStatusReport does the online callAdmin(OpClusterStatus) + the B2-item-5 error-fold,
// returning the report (no os.Exit) so both the one-shot and --watch paths share it (B5 §0 refactor).
// A var seam so the wait/watch tests can stub the socket round-trip.
var fetchClusterStatusReport = func(socketPath string) (*adminsock.ClusterStatusReport, error) {
	resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterStatus})
	if err != nil {
		return nil, err
	}
	rep := resp.Cluster
	if rep == nil {
		// No report at all → unavailable (69); never a silent half-report.
		msg := resp.Error
		if msg == "" {
			msg = "no report"
		}
		return nil, unavailErr("cluster status: %s", msg)
	}
	// B2 item 5: a report present AND a broker-flagged error → FOLD it, never drop.
	if resp.Error != "" {
		rep.Errors = append(rep.Errors, resp.Error)
		rep.Partial = true
	}
	rep.Errors = normSlice(rep.Errors) // errors:[] not null
	return rep, nil
}

// renderClusterStatusReport renders one report (JSON or table), WITHOUT os.Exit — the caller owns
// the exit-code contract. The PARTIAL stderr note rides the human view only.
func renderClusterStatusReport(cmd *cobra.Command, rep *adminsock.ClusterStatusReport, asJSON bool) {
	if asJSON {
		_ = emitJSON(cmd.OutOrStdout(), rep)
		return
	}
	// B7 DOC#6(a): --card is a glance reformat of the SAME report (JSON unchanged); default = table.
	if statusCardMode {
		renderClusterStatusCard(cmd.OutOrStdout(), rep)
	} else {
		renderClusterStatus(cmd, rep)
	}
	if rep.Partial {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "** PARTIAL **\n")
	}
}

// statusCardMode is set by the `cluster status --card` flag (the renderClusterStatusReport seam is
// shared by the one-shot/watch/offline/remote paths, so a package var threads --card without
// reworking every call site). One-shot CLI, never concurrent.
var statusCardMode bool

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
		Banner:   "disk roster snapshot with a raft-port TCP liveness probe (advisory); for quorum loss use `cluster recovery force-single --confirm-peers-dead ...` (it re-probes + HARD-REFUSES any live peer)",
		NextStep: "cluster recovery force-single --confirm-peers-dead <ids...>",
	}
	if forceSingle != "" {
		rep.Health = "FORCE_SINGLE"
		rep.HealthLabel = "FORCE-SINGLE" // C6 建议6 (offline can't read the voter count → label set inline)
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
	// audit cli F5: derive a coarse exit code from the probe so a monitoring gate
	// (`cluster status --offline --json || alert`) is not silently exit-0 "OK" during a total
	// outage. With MORE THAN ONE roster node and NONE answering the raft-port probe, report
	// DEGRADED (exit 2). The len>1 guard avoids a false positive for an N=1 cluster whose single
	// (stopped) daemon is unreachable by design when offline status is run. FORCE_SINGLE (exit 3,
	// set above) wins.
	if rep.ExitCode == 0 && len(rep.Nodes) > 1 {
		reachable := 0
		for _, n := range rep.Nodes {
			if n.Reachable {
				reachable++
			}
		}
		if reachable == 0 {
			// B1/B2 coordination: use a DISTINCT health string from the online DEGRADED (which
			// is exit 1). The offline "no peer answered the raft-port probe" condition is exit 2,
			// so reusing "DEGRADED" would make (health -> exit) ambiguous across views. A monitor
			// branches on `view` first; this keeps (view, health, exit) self-consistent.
			rep.Health = "ROSTER_UNREACHABLE"
			rep.HealthLabel = "READ-ONLY" // C6 建议6: leaderless/unreachable maps to the READ-ONLY operator state
			rep.ExitCode = 2
			rep.Banner = "no roster node answered the raft-port probe (possible quorum loss / total outage) — " + rep.Banner
		}
	}
	rep.Errors = normSlice(rep.Errors) // errors:[] not null (schema parity with the online view)
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

// topoCell renders the C3 topology column `applied/observed→desired` with a convergence marker: "✓"
// converged, "…" still catching up, "STUCK" when the reconcile is wedged (bad render / unknown
// directive). "-" when the node does not report topology (older broker / nothing desired yet).
func topoCell(n adminsock.ClusterNodeStatus, desired uint64) string {
	if !n.TopoReported || desired == 0 {
		return "-"
	}
	s := fmt.Sprintf("%d/%d→%d", n.TopoApplied, n.TopoObserved, desired)
	switch {
	case strings.Contains(n.TopoReconcileReason, "unrecognized directive") || strings.Contains(n.TopoReconcileReason, "nats-server -t"):
		return s + " STUCK"
	case n.TopoObserved >= desired:
		return s + " ✓"
	default:
		return s + " …"
	}
}

func renderClusterStatus(cmd *cobra.Command, rep *adminsock.ClusterStatusReport) {
	w := cmd.OutOrStdout()
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE_ID\tNAME\tPHASE\tROLE\tVER\tLAG\tACCT.NK\tSTREAMS\tTOPO\tREACH")
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
		// B6 OPS#4: VER = the node's live self-reported release ("?" if it did not report).
		ver := n.ReleaseVersion
		if ver == "" {
			ver = "?"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%d/%d\t%s\t%s%s\n",
			n.NodeID, n.Name, n.Phase, n.Role, ver, n.AppliedLag, acct, n.StreamActual, n.StreamTarget,
			topoCell(n, rep.TopoDesired), reach, flag)
	}
	_ = tw.Flush()
	// B1 item 2: column legend (broker-host operator view). ACCT.NK is honest that per-node
	// account-key verification is not yet wired (the column is currently always Y).
	_, _ = fmt.Fprintln(w, "\ncolumns: LAG=raft entries behind the leader (0=caught up) · ACCT.NK=Y if this node's account key")
	_, _ = fmt.Fprintln(w, "  matches (currently always Y — per-node verification not yet wired) · STREAMS=JetStream replicas")
	_, _ = fmt.Fprintln(w, "  actual/target (actual<target = degraded) · VER=running release (live self-report) · REACH=NATS/raft reachability")
	_, _ = fmt.Fprintln(w, "  · TOPO=NATS topology applied/observed→desired gen (✓ converged · … catching up · STUCK fix conf or `cluster reconcile nats --manual`)")
	// C6 建议6: show the hyphenated 5-state operator label; fall back to the legacy Health when unset
	// (an old broker that predates health_label) so legacy fixtures stay green.
	healthHeadline := rep.HealthLabel
	if healthHeadline == "" {
		healthHeadline = rep.Health
	}
	_, _ = fmt.Fprintf(w, "\n** %s (exit %d) **\n", healthHeadline, rep.ExitCode)
	if rep.Banner != "" {
		_, _ = fmt.Fprintf(w, "%s\n", rep.Banner)
	}
	// B1 item 3A: plain-language voter-count verdict (the authoritative broker-host view).
	if rep.Verdict != "" {
		_, _ = fmt.Fprintf(w, "verdict: %s\n", rep.Verdict)
	}
	// B1 item 4A: view-source footer — which broker self-reported, and whether it is the
	// authoritative leader view (a follower's view must say "re-run on the leader").
	if rep.IsLeaderView {
		_, _ = fmt.Fprintf(w, "view: authoritative (leader %s).\n", rep.ViewHost)
	} else if rep.ViewHost != "" {
		leader := rep.LeaderID
		if leader == "" {
			leader = "unknown"
		}
		_, _ = fmt.Fprintf(w, "view: this is %s's local view; re-run on the leader (%s) for the authoritative verdict.\n", rep.ViewHost, leader)
	}
	if rep.NextStep != "" {
		_, _ = fmt.Fprintf(w, "next: %s\n", rep.NextStep)
	}
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
					return fmt.Errorf("aborted (type the node_id to confirm an F==0 drain; --yes is not accepted)")
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
				return fmt.Errorf("aborted (type the node_id to confirm, or pass --confirm-node-id + %s for unattended use)", machineConfirmEnv)
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
		fromManifest string
		check        bool
		dryRun       bool
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
					SecretsDir: secretsDir, DBPath: dbPath, ConfPath: defaultNatsConfPath, RaftAddr: raftAddr, NatsRoute: natsRoute,
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
			if !confirmTypedNodeID(cmd, selfID, "", false, "") { // never-escapable: irreversible one-way migration
				return fmt.Errorf("cluster init: aborted (node_id not confirmed)")
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
			ids := readClusterPublicIdentities(secretsDir, defaultNatsConfPath)
			acctTok := orPlaceholder(ids.AccountIssuer, "<account-public-nkey>")
			brkTok := orPlaceholder(ids.BrokerNkey, "<broker-public-nkey>")
			out := cmd.OutOrStdout()
			fillHint := ""
			if ids.AccountIssuer == "" || ids.BrokerNkey == "" {
				fillHint = " (fill the <…> placeholder(s) in step 1 below)"
			}
			_, _ = fmt.Fprintf(out, "cluster init --from-existing complete — %s is now a single-voter cluster (data_dir %s).%s\n", selfID, dataDir, fillHint)
			_, _ = fmt.Fprintln(out, "NEXT (run in order):")
			_, _ = fmt.Fprintf(out, "  1. tether cluster reconcile nats --manual --secrets-dir %s --server-name %s --route-url %s --account-issuer %s --broker-nkey %s\n",
				secretsDir, selfID, routeURL, acctTok, brkTok)
			if ids.AccountIssuer != "" && ids.BrokerNkey != "" {
				_, _ = fmt.Fprintln(out, "     # account-issuer auto-read from this host's secrets + verified against nats.conf")
				_, _ = fmt.Fprintln(out, "     # (broker-nkey verified too on a single-user conf; otherwise read from broker.nk)")
			}
			if ids.Note != "" {
				_, _ = fmt.Fprintf(out, "     %s\n", ids.Note)
			}
			_, _ = fmt.Fprintln(out, "  2. systemctl restart nats-server                       # bring up the new conf")
			_, _ = fmt.Fprintln(out, "  3. set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path} in broker.yaml")
			_, _ = fmt.Fprintln(out, "     # nats_conf_path: /etc/tether/nats.d/nats.conf  (the reconciler's conf; #22 — set it explicitly so a future binary upgrade can't repoint it)")
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
	registerYesRejector(cmd)
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
