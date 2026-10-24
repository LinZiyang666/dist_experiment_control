package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/spf13/cobra"
)

// origin: hot-update S5 plan §10.2 / V-18. The CLI only chooses the output
// channel; the imported renderer owns the report's presentation.
func renderClusterStatusCard(w io.Writer, rep *adminsock.ClusterStatusReport) {
	cli.RenderClusterStatusCard(w, rep)
}

// Cluster status owns the socket and offline views alongside their CLI rendering.
// origin: hot-update S6 plan §9.3 / GATES-S6-C
func newClusterStatusCmd(socketPath *string) *cobra.Command {
	var asJSON, offline, remote, homes bool
	var dbPath, natsURL, home string
	var watch, settle time.Duration
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster health (exit 0=HEALTHY-HA 1=DEGRADED 2=read-only/quorum-lost 3=force-single)",
		Long: "Show cluster health with the §17 exit-code contract (0=HEALTHY-HA, 1=DEGRADED, 2=read-only/\n" +
			"quorum-lost, 3=force-single). The exit code is INSTANTANEOUS: a voter restart legitimately\n" +
			"produces a brief DEGRADED (exit 1) window of a few seconds while that voter is unreachable /\n" +
			"catching up, so a monitor polling continuously will see 0→1→0. That transient is REAL (reduced\n" +
			"redundancy), not a bug — see docs/cluster-runbook.md 'cluster status exit-code semantics'. Use\n" +
			"--settle to debounce a benign restart: it waits up to the given duration for a DEGRADED verdict\n" +
			"to clear before trusting it (a genuinely quorum-lost/force-single cluster still exits at once;\n" +
			"a sustained DEGRADED still exits 1 after the window).",
		Example: "  tether cluster status                 # operator table (on a broker host)\n" +
			"  tether cluster status --settle 30s    # debounce a benign voter-restart transient (cron/monitor)\n" +
			"  tether cluster status --watch 5s      # repaint every 5s (Ctrl-C to exit)\n" +
			"  tether cluster status --remote        # user summary over NATS (from a laptop, no socket)\n" +
			"  tether cluster status --offline --db /var/lib/tether/tether.db   # disk roster (daemon stopped)",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if homes && remote {
				// G7b #16: the over-NATS homes aggregate (per-broker __proxy__ home counts, no socket).
				return clusterStatusHomesRemote(cmd, home, natsURL, asJSON)
			}
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
			// D3: --settle debounces a benign voter-restart DEGRADED transient (opt-in; default off ⇒
			// the instantaneous verdict below). It renders the SETTLED report and exits with its code.
			if settle > 0 {
				rep, perr := settleClusterStatus(cmd, *socketPath, settle, 0)
				if perr != nil {
					return perr
				}
				renderClusterStatusReport(cmd, rep, asJSON)
				os.Exit(rep.ExitCode) // §17 exit-code contract for cron/monitoring
				return nil
			}
			if settle < 0 {
				return usageErr("--settle must be non-negative")
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
	cmd.Flags().DurationVar(&settle, "settle", 0, "D3: debounce a benign voter-restart DEGRADED transient — wait up to this long for a DEGRADED(exit 1) verdict to clear before trusting it (0 = off, report the instantaneous verdict; quorum-lost/force-single still exit immediately)")
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
	// --settle is a debounced ONE-SHOT socket probe: it makes no sense with the disk snapshot
	// (--offline), the interactive repaint (--watch), the descriptive homes view (no exit contract),
	// or the ctl-over-NATS user view (which never returns DEGRADED/exit 1, so there is nothing to
	// debounce).
	cmd.MarkFlagsMutuallyExclusive("offline", "settle")
	cmd.MarkFlagsMutuallyExclusive("watch", "settle")
	cmd.MarkFlagsMutuallyExclusive("remote", "settle")
	cmd.Flags().BoolVar(&homes, "homes", false, "C6: aggregate every expose + proxy home/epoch/ready_reason (one descriptive view; no exit-code contract)")
	cmd.MarkFlagsMutuallyExclusive("homes", "offline")
	// G7b #16: --homes + --remote is now ALLOWED (the over-NATS home-count aggregate). The other
	// homes combinations stay mutually exclusive (offline = disk snapshot, watch = socket repaint).
	cmd.MarkFlagsMutuallyExclusive("homes", "watch")
	cmd.MarkFlagsMutuallyExclusive("homes", "settle") // homes is a descriptive view with no exit-code to debounce
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
		// external review B2-2: was a hardcoded 1 while the socket view bumped to 2 — one struct, two
		// versions. Both producers now stamp the single constant next to the struct.
		SchemaVersion: adminsock.ClusterStatusSchemaVersion, View: "offline", ExitCode: 0,
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
			// os.Exit skips the `defer rows.Close()` above; Close is idempotent, so calling it here
			// makes the cleanup happen on BOTH exits from this function rather than only the one that
			// returns normally.
			_ = rows.Close()
			//nolint:gocritic // exitAfterDefer: rows.Close() runs explicitly on the line above, so the
			// deferred close is not the thing being skipped. The shape stays because this command's
			// contract is an exit CODE and cobra cannot return one.
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

// topoCell renders the C3 topology column `applied/observed→desired` plus a marker from the SHARED
// classifier: "✓" converged, "…" catching up, "HOLD" a deliberately withheld standalone→clustered
// cutover, "STUCK" wedged, "?" an action this binary does not know. "-" when the node does not
// report topology (older broker / nothing desired yet).
//
// batch C: this used to substring-match TopoReconcileReason with its OWN set, which was missing the
// render-failure marker the broker side had. The two ends disagreed, so a render failure rendered
// here as "still catching up" while the broker correctly ruled the cluster DEGRADED — telling the
// operator to wait for a self-heal that was never coming. Both ends now call ClassifyTopo, so they
// cannot word the same state differently again.
func topoCell(n adminsock.ClusterNodeStatus, desired uint64) string {
	st := natsconf.ClassifyTopo(n.TopoAction, n.TopoReconcileReason, n.TopoObserved, desired, n.TopoReported)
	if st == natsconf.TopoUnreported {
		return "-"
	}
	return fmt.Sprintf("%d/%d→%d %s", n.TopoApplied, n.TopoObserved, desired, st.Cell())
}

// acctCell renders the ACCT.NK column as one of three states (batch B / B4).
//
// It used to be two states derived from a bool that the producer hardcoded true on the online path
// and left false everywhere else — so the online view printed Y for nodes it had never contacted and
// the offline view printed N for every node in the roster without ever comparing a key. Both were
// fabricated, and N is the worse of the two: an operator running `cluster status --offline` during an
// outage read "every broker's account key is wrong" as a finding.
//
// "?" is therefore not a cosmetic third state. It is the only truthful rendering for a row carrying
// no answer: an orphan raft voter, the offline disk snapshot, a peer that did not reply to the health
// scatter, or a broker predating proto.ClusterHealthResp.AccountNkPub.
func acctCell(n adminsock.ClusterNodeStatus) string {
	if !n.AccountNkReported {
		return "?"
	}
	if n.AccountNkMatch {
		return "Y"
	}
	return "N"
}

func renderClusterStatus(cmd *cobra.Command, rep *adminsock.ClusterStatusReport) {
	w := cmd.OutOrStdout()
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NODE_ID\tNAME\tPHASE\tROLE\tVER\tLAG\tACCT.NK\tSTREAMS\tTOPO\tREACH")
	for _, n := range rep.Nodes {
		acct := acctCell(n)
		reach := n.ReachSource
		if !n.Reachable {
			reach = "UNREACHABLE(" + n.ReachSource + ")"
		}
		flag := ""
		if n.Inconsistent {
			// EXTERNAL review m1: the bare word carried one meaning for years and now carries two.
			// Qualify it so the table row itself says which.
			flag = " INCONSISTENT"
			if n.InconsistentReason == adminsock.InconsistentDrainingWithoutMarker {
				flag = " INCONSISTENT(drain)"
			}
		}
		// B6 OPS#4: VER = the node's live self-reported release ("?" if it did not report).
		ver := n.ReleaseVersion
		if ver == "" {
			ver = "?"
		}
		// STREAMS is three-state, like VER and ACCT.NK above it: "n/t" when the replica observation
		// completed, "?/t" when it did not. A negative actual means UNOBSERVED (B7 / review B7-02) — it
		// used to be rendered as a measured 0, so a timed-out probe on a busy broker printed "0/3" and
		// an operator read a total replication failure that had not happened. It still counts as
		// not-at-target everywhere that gates on replica health; it just no longer claims a measurement.
		streams := fmt.Sprintf("%d/%d", n.StreamActual, n.StreamTarget)
		if n.StreamActual < 0 {
			streams = fmt.Sprintf("?/%d", n.StreamTarget)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s%s\n",
			n.NodeID, n.Name, n.Phase, n.Role, ver, n.AppliedLag, acct, streams,
			topoCell(n, rep.TopoDesired), reach, flag)
	}
	_ = tw.Flush()
	// B1 item 2: column legend (broker-host operator view). batch B / B4: ACCT.NK is now a real
	// per-node comparison against the viewing broker's own auth_callout account key, so the legend
	// no longer has to admit the column is always Y — it documents the three states instead.
	_, _ = fmt.Fprintln(w, "\ncolumns: LAG=raft entries behind the leader (0=caught up) · ACCT.NK=this node's auth_callout account")
	_, _ = fmt.Fprintln(w, "  key vs this view's (Y=same · N=DIFFERENT, agents will fail auth at random depending on which")
	_, _ = fmt.Fprintln(w, "  broker answers the callout · ?=no answer from this node, not verified) · STREAMS=JetStream replicas")
	_, _ = fmt.Fprintln(w, "  actual/target (actual<target = degraded) · VER=running release (live self-report) · REACH=NATS/raft reachability")
	// origin: batch-c internal review C3-F1 — derived from the classifier, not hand-written, so a new
	// TOPO token can never ship without its key. The hand-written version was missing HOLD, and its
	// only non-converged remedy pointed HOLD operators at the one command that damages their data.
	_, _ = fmt.Fprintln(w, "  · "+natsconf.TopoCellLegend())
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
