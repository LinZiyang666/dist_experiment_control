package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newNodeCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "node",
		Short: "Per-node operations (architecture J.4 / G)",
	}
	root.AddCommand(newNodeUpgradeCmd())
	root.AddCommand(newNodeLsCmd())
	return root
}

// newNodeLsCmd implements `tether node ls` — list every agent in the
// active session with its current liveness status, last heartbeat
// age, proto version, and release version. Companion to `tether ps`
// (which is process-centric); this command is node-centric so a
// freshly-registered agent that hasn't exec'd anything still shows
// up.
//
// Underlying RPC is the same node.list.req that `node upgrade --all`
// uses to enumerate ONLINE targets.
func newNodeLsCmd() *cobra.Command {
	var (
		natsURL     string
		home        string
		showAll     bool
		asJSON      bool
		showBrokers bool
	)
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List agents (nodes) in the active session",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "node ls", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			body, _ := json.Marshal(proto.NodeListReq{})
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
			defer cancel()
			respMsg, err := nc.RequestWithContext(ctx,
				proto.SubjCtrlNodeList(id.PublicKey, sid), body)
			if err != nil {
				return fmt.Errorf("node ls: %w (broker unreachable on NATS)", err)
			}
			var resp proto.NodeListResp
			if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
				return fmt.Errorf("node ls: malformed reply: %w", err)
			}
			if resp.Code != "" {
				return brokerErrorMessage("node ls", resp.Code, resp.Error)
			}

			// G5 #19: the broker-host dual-version view — correlate each broker daemon's version (from the
			// cluster-health probe) with its co-located agent's RELEASE (from this node list), flagging
			// same-host skew so "whole-host upgraded" has one trusted criterion.
			if showBrokers {
				return renderNodeLsBrokers(cmd, nc, id.PublicKey, resp.Nodes, asJSON)
			}

			out := cmd.OutOrStdout()
			// Filter once (honor -a/showAll); --json changes RENDERING, not selection.
			shown := make([]proto.NodeListEntry, 0, len(resp.Nodes))
			for _, n := range resp.Nodes {
				if n.Status == "ONLINE" || showAll {
					shown = append(shown, n)
				}
			}
			if asJSON {
				if err := emitJSON(out, nodeLsJSON{Schema: "node_ls", SchemaVersion: 1, Nodes: normSlice(shown)}); err != nil {
					return err
				}
				withBanner(nc, id.PublicKey, true) // suppressed under --json
				return nil
			}
			now := time.Now()
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			// KIND is what tells an operator which of these rows is a device and
			// which is a transient instance the broker named (external review
			// F13). The JSON has carried `leased` from the start; the table an
			// operator actually reads did not, so the one visible difference
			// between "your machine" and "a clone that will be gone by
			// lunchtime" was a suffix they had to know to look for.
			_, _ = fmt.Fprintln(tw, "NODE\tKIND\tSTATUS\tHEARTBEAT\tPROTO\tRELEASE")
			for _, n := range shown {
				age := "-"
				if !n.LastHeartbeatAt.IsZero() {
					age = humanizeAgo(now, n.LastHeartbeatAt)
				}
				release := n.ReleaseVersion
				if release == "" {
					release = "-"
				}
				kind := "device"
				if n.Leased {
					kind = "leased"
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
					n.NID, kind, n.Status, age, n.ProtoVersion, release)
			}
			if len(shown) == 0 {
				_, _ = fmt.Fprintln(tw, "(no nodes)")
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			withBanner(nc, id.PublicKey, false)
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "include OFFLINE / STALE nodes")
	cmd.Flags().BoolVar(&showBrokers, "brokers", false, "G5 #19: show each broker HOST's daemon + co-located agent version (skew ⇒ whole-host upgrade incomplete)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit the stable machine JSON schema (default: human text)")
	return cmd
}

func newNodeUpgradeCmd() *cobra.Command {
	var (
		urlFlag    string
		shaFlag    string
		natsURL    string
		home       string
		timeoutSec int
		all        bool
		wait       bool
	)
	cmd := &cobra.Command{
		Use:   "upgrade <nid>|--all",
		Short: "Trigger an owner-only agent binary upgrade (architecture J.4)",
		Long: fmt.Sprintf(`Sends a %s.s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req
to the broker. The broker enforces owner-only + URL allowlist +
proto match before forwarding to the agent. The agent downloads
the tarball, verifies SHA256 + URL allowlist locally, smoke-tests
the staged binary (`+"`version`"+` must answer), keeps the old
binary in a .prev slot, atomically replaces itself, then re-execs
in place (PID preserved under systemd / setsid). The new binary's
first successful register COMMITS the upgrade; if it cannot
register within its deadline (~2 min) the agent rolls itself back
to .prev. See architecture §21.3.

URL must be one the broker has whitelisted (broker.yaml or
--upgrade-url-allow). SHA256 is the hex digest of the tarball,
NOT of the extracted binary.

--wait (default on for a single <nid>) polls until the node
re-registers with the new release (COMMITTED) or reports that the
deadline passed (likely rolled back).

--all upgrades every ONLINE node in the active session. The first
node (lexicographic) is the CANARY: it is always fully confirmed
(staged + re-registered) before the rest are touched; if it fails
or rolls back, the remaining nodes are left untouched.`, proto.SubjectPrefix),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if urlFlag == "" || shaFlag == "" {
				return usageErr("--url and --sha256 are required")
			}
			if all && len(args) > 0 {
				return usageErr("--all and explicit <nid> are mutually exclusive")
			}
			if !all && len(args) == 0 {
				return usageErr("either <nid> or --all is required")
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
			nc, err := connectCtl(cmd, "upgrade", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			targets := args
			if all {
				var skippedLeased int
				targets, skippedLeased, err = listOnlineNIDs(cmd.Context(), nc, sid, id.PublicKey)
				if err != nil {
					return err
				}
				if skippedLeased > 0 {
					// Say it out loud: a silent exclusion reads as "the fleet
					// was upgraded" when part of it deliberately was not.
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
						"skipping %d instance(s) running under an assigned lease name "+
							"(remote upgrade is disabled for clone families; rebuild the shared image instead)\n",
						skippedLeased)
				}
				if len(targets) == 0 {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(no ONLINE nodes in session)")
					return nil
				}
				// Deterministic canary selection (upgrade-safety plan §5):
				// lexicographic order, first node is the canary.
				sort.Strings(targets)
			}
			timeout := time.Duration(timeoutSec) * time.Second

			// Single explicit <nid>: dispatch, then (default) wait for the
			// commit/rollback verdict. Baseline BEFORE dispatch (S11), and a
			// baseline failure refuses the dispatch outright (F4).
			if !all {
				nid := targets[0]
				startRelease := ""
				if wait {
					startRelease, err = captureReleaseBaseline(cmd, nc, sid, id.PublicKey, nid)
					if err != nil {
						return err
					}
				}
				newVersion, err := dispatchUpgrade(cmd, nc, sid, id.PublicKey, nid, urlFlag, shaFlag, timeout)
				if err != nil {
					return err
				}
				if !wait {
					return nil
				}
				return waitForUpgradeCommit(cmd, nc, sid, id.PublicKey, nid, newVersion, startRelease)
			}

			return runUpgradeAll(cmd, nc, sid, id.PublicKey, targets, urlFlag, shaFlag, timeout)
		},
	}
	cmd.Flags().StringVar(&urlFlag, "url", "", "absolute https:// URL of the new release tarball (required)")
	cmd.Flags().StringVar(&shaFlag, "sha256", "", "hex SHA256 of the tarball (required)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVar(&timeoutSec, "timeout", 60,
		"seconds to wait for broker reply (must exceed agent download time)")
	cmd.Flags().BoolVar(&all, "all", false,
		"upgrade every ONLINE node in the active session (canary-first, then sequential)")
	cmd.Flags().BoolVar(&wait, "wait", true,
		"single-<nid> mode: poll until the node re-registers with the new release (COMMITTED) or the deadline passes; --all always confirms its canary regardless")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 || all {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
		t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
		defer t.Close()
		return cli.CompleteOnlineNodes(t, cctx, toComplete)
	}
	return cmd
}

// runUpgradeAll is the `--all` body (extracted from RunE for testability).
// The CANARY (first node of the sorted target list) is always fully
// confirmed — staged AND re-registered as the new release — before any other
// node is touched; a canary failure leaves the rest of the fleet untouched
// (upgrade-safety plan §5). The remainder is NOT per-node waited (N×2min of
// serial waiting buys nothing after a confirmed canary) — the closing hint
// points at `tether node ls`.
func runUpgradeAll(cmd *cobra.Command, nc *nats.Conn,
	sid, actor string, targets []string, urlFlag, shaFlag string, timeout time.Duration,
) error {
	canary, rest := targets[0], targets[1:]
	if len(rest) > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"canary: %s (%d more node(s) held until it commits)\n", canary, len(rest))
	}
	// Baseline before dispatch (S11); failure holds the WHOLE fleet (F4) —
	// a canary whose confirmation would be blind is no canary.
	startRelease, err := captureReleaseBaseline(cmd, nc, sid, actor, canary)
	if err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}
	newVersion, err := dispatchUpgrade(cmd, nc, sid, actor, canary, urlFlag, shaFlag, timeout)
	if err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}
	if err := waitForUpgradeCommit(cmd, nc, sid, actor, canary, newVersion, startRelease); err != nil {
		return fmt.Errorf("canary %s failed, %d node(s) untouched: %w", canary, len(rest), err)
	}

	// Fleet rollouts must distinguish transient from configuration errors:
	//   - node_offline / agent_no_responders / upgrade_in_progress /
	//     timeout: keep going; the operator will retry these later.
	//   - everything else (not_owner / url_not_allowed / proto_bump /
	//     sha256_invalid / smoke_failed): config bug, abort the rest of the
	//     fleet so we don't fan-out a known-bad request.
	var skipped, failed int
	for _, nid := range rest {
		_, err := dispatchUpgrade(cmd, nc, sid, actor, nid, urlFlag, shaFlag, timeout)
		if err == nil {
			continue
		}
		if isConfigError(err) {
			return err
		}
		if isTransientError(err) {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"⚠ %s/%s skipped (transient): %v\n", sid, nid, err)
			skipped++
			continue
		}
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
			"✗ %s/%s failed: %v\n", sid, nid, err)
		failed++
	}
	if failed > 0 {
		return fmt.Errorf("upgrade --all: %d failed (%d transiently skipped)", failed, skipped)
	}
	if skipped > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"(%d node(s) skipped due to transient errors — retry later)\n", skipped)
	}
	if len(rest) > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"fleet dispatched; verify releases with `tether node ls`\n")
	}
	return nil
}

// dispatchUpgrade fires one upgrade.req at (sid, nid) and reports the
// broker's reply. Returns the smoke-gate-normalized release tag the agent
// staged (empty from a pre-upgrade-safety agent) so callers can wait for the
// matching re-register. Extracted so the --all loop can reuse it.
func dispatchUpgrade(cmd *cobra.Command, nc *nats.Conn,
	sid, actor, nid, url, sha256 string, timeout time.Duration,
) (string, error) {
	body, _ := json.Marshal(proto.UpgradeReq{
		URL:          url,
		SHA256:       sha256,
		ProtoVersion: proto.ProtoVersion,
	})
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()
	respMsg, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy(sid, actor, nid, "upgrade"), body)
	if err != nil {
		return "", fmt.Errorf("upgrade %s/%s: %w (broker or agent unreachable on NATS)", sid, nid, err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return "", fmt.Errorf("decode reply for %s: %w (broker version skew?)", nid, err)
	}
	if !resp.OK {
		return "", brokerErrorMessage(fmt.Sprintf("upgrade %s/%s", sid, nid), resp.Code, resp.Error)
	}
	// origin: internal review S3. A PRE-upgrade-safety agent (every deployed
	// release up to and including the one before this shipped) fills
	// NewVersion with the FULL first line of `tether version` — e.g.
	// "tether v0.4.7 (proto v2)" — not the bare tag: its readVersionString
	// was best-effort whole-line. Treating that as a comparable tag makes
	// --wait's equality test unsatisfiable → every successful first-hop
	// upgrade reads as ROLLED BACK and the canary aborts the whole fleet.
	// Normalize defensively: anything with whitespace or without a leading
	// "v" is legacy — drop to "" so --wait uses the release-changed fallback.
	newVersion := resp.NewVersion
	if newVersion != "" && (strings.ContainsAny(newVersion, " \t") || !strings.HasPrefix(newVersion, "v")) {
		newVersion = ""
	}
	if newVersion != "" {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"✔ %s/%s: staged (→ %s, smoke ok); agent re-exec in progress\n", sid, nid, newVersion)
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"✔ upgrade dispatched to %s/%s (agent re-exec in progress)\n", sid, nid)
	}
	return newVersion, nil
}

// upgradeWaitBudget bounds waitForUpgradeCommit's polling: the agent's own
// register deadline is 120s (internal/agent upgradeRegisterDeadline — the
// number in the "deadline 120s" line below), plus slack for the re-exec, the
// reconnect backoff, and our 3s poll grain. A var, not a const, so the
// timeout-path test does not have to sit through the real budget.
var upgradeWaitBudget = 150 * time.Second

// upgradeWaitPoll is the node.list polling grain. A var (internal review
// S30) so tests can tighten it instead of sleeping in 3s steps.
var upgradeWaitPoll = 3 * time.Second

// waitForUpgradeCommit polls node.list (the same replicated view `node ls`
// reads — zero new schema, zero new subscriptions) until nid re-registers
// ONLINE with the upgraded release, or the wait budget runs out (by then the
// agent's own deadline has passed, so a still-old release almost certainly
// means it ROLLED BACK). newVersion=="" (pre-upgrade-safety agent: no
// smoke-normalized tag to compare) falls back to "any release change from
// startRelease" — the baseline the CALLER captured BEFORE dispatching
// (internal review S11: capturing it after dispatch races a fast re-register
// into a false ROLLED BACK). origin: upgrade-safety plan §5.
func waitForUpgradeCommit(cmd *cobra.Command, nc *nats.Conn, sid, actor, nid, newVersion, startRelease string) error {
	// origin: internal review S4, HARDENED by external review F3. A same-tag
	// re-push (re-installing the release the node already runs — this
	// fleet's standard recovery move for a corrupted binary) makes the
	// release-equality criterion satisfiable by the STALE pre-upgrade row:
	// the very first poll would report a hollow COMMITTED. There is no
	// remote signal that separates "old row" from "new register" without
	// new wire (deliberately out of scope this increment). The internal
	// round downgraded to a warning and returned success; the external
	// review is right that a canary is a HEALTH gate, not an artifact gate —
	// `version` passing says nothing about Agent.Run/register/watchdog, so
	// "unconfirmable" must FAIL CLOSED: a loud UNCONFIRMED error. Single-
	// node operators read the message and check the logs; `--all` treats it
	// as a canary failure and leaves the fleet untouched. Same-tag fleet
	// reinstalls are done node-by-node until a wire-level registration
	// generation exists (recorded as the long-term fix).
	if newVersion != "" && newVersion == startRelease {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"⚠ %s: node already reports %s — a same-tag re-push cannot be confirmed via release change.\n"+
				"  UNCONFIRMED: staged + smoke passed, but re-register proof is unavailable; verify via the\n"+
				"  agent/broker logs (upgrade_state line) before trusting this node.\n",
			nid, newVersion)
		return &ExitError{Class: exitUsage, Err: fmt.Errorf(
			"upgrade %s/%s: UNCONFIRMED — same-tag re-push has no re-register proof (verify via agent/broker logs)",
			sid, nid)}
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  waiting for re-register (deadline 120s) …\n")

	deadline := time.Now().Add(upgradeWaitBudget)
	lastSeen := startRelease
	for {
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"✗ %s: still %s after deadline — likely ROLLED BACK (check agent log / broker log)\n",
				nid, orUnknown(lastSeen))
			// exitUsage, not transient (internal review S6): a retry hits
			// the same artifact and the same rollback — automation told
			// "retry" would push a broken binary in a loop, each round
			// crash-booting the target three times. A human has to change
			// the --url (or read the logs); that is the 64 class.
			return &ExitError{Class: exitUsage, Err: fmt.Errorf(
				"upgrade %s/%s: node did not re-register with the new release within %s (likely rolled back)",
				sid, nid, upgradeWaitBudget)}
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-time.After(upgradeWaitPoll):
		}
		entry, err := lookupNodeEntry(cmd.Context(), nc, sid, actor, nid)
		if err != nil {
			continue // transient lookup failure — the budget bounds us
		}
		if entry.ReleaseVersion != "" {
			lastSeen = entry.ReleaseVersion
		}
		if entry.Status != "ONLINE" {
			continue
		}
		if newVersion != "" {
			if entry.ReleaseVersion == newVersion {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"✔ %s: registered as %s — upgrade COMMITTED\n", nid, entry.ReleaseVersion)
				return nil
			}
		} else if startRelease != "" && entry.ReleaseVersion != "" && entry.ReleaseVersion != startRelease {
			// The legacy fallback is meaningful ONLY against a known non-empty
			// baseline (external review F4): with startRelease=="", every
			// ordinary stale ONLINE row is "different" on the first poll and
			// would mint a hollow COMMITTED. Baseline capture is mandatory
			// before dispatch, so "" here means the pre-upgrade row itself had
			// no release — fail toward the timeout verdict, never toward a
			// fabricated commit.
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"✔ %s: registered as %s (release changed) — upgrade COMMITTED\n", nid, entry.ReleaseVersion)
			return nil
		}
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "(unknown release)"
	}
	return s
}

// captureReleaseBaseline reads nid's currently-registered release BEFORE the
// upgrade is dispatched — the comparison base --wait's verdicts stand on.
// HARD failure (external review F4, superseding internal S11's warn-and-
// continue): with an unknown baseline, the legacy fallback would read ANY
// stale ONLINE row as "release changed" on its very first poll and print a
// hollow COMMITTED, and the same-tag guard would be silently bypassed. When
// confirmation is requested, no baseline ⇒ no dispatch.
func captureReleaseBaseline(cmd *cobra.Command, nc *nats.Conn, sid, actor, nid string) (string, error) {
	entry, err := lookupNodeEntry(cmd.Context(), nc, sid, actor, nid)
	if err != nil {
		return "", fmt.Errorf("upgrade %s/%s: cannot read the pre-upgrade release baseline (%w) — refusing to dispatch with --wait confirmation blind; retry, or use --wait=false for a fire-and-forget dispatch", sid, nid, err)
	}
	return entry.ReleaseVersion, nil
}

// lookupNodeEntry fetches one node's row from the node.list RPC.
func lookupNodeEntry(ctx context.Context, nc *nats.Conn, sid, actor, nid string) (proto.NodeListEntry, error) {
	body, _ := json.Marshal(proto.NodeListReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respMsg, err := nc.RequestWithContext(respCtx, proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return proto.NodeListEntry{}, err
	}
	var nl proto.NodeListResp
	if err := json.Unmarshal(respMsg.Data, &nl); err != nil {
		return proto.NodeListEntry{}, err
	}
	if nl.Code != "" {
		return proto.NodeListEntry{}, fmt.Errorf("node.list rejected: %s %s", nl.Code, nl.Error)
	}
	for _, n := range nl.Nodes {
		if n.NID == nid {
			return n, nil
		}
	}
	return proto.NodeListEntry{}, fmt.Errorf("node %s not in node.list", nid)
}

// HOW `node upgrade --all` CLASSIFIES A PER-NODE FAILURE
//
// Two disjoint sets of WIRE CODES, matched EXACTLY against the code the error carries structurally
// (ExitError.Code, set at the source by brokerErrorMessage), plus one narrow prose fallback for
// errors that have no wire code at all.
//
// origin: line-2 external review 疑惑 #2. Both functions used to be
// `strings.Contains(err.Error(), needle)` over the same code names — exactly what exitcode.go's own
// comment forbids ("The classifier never string-sniffs prose for a class — that would make a
// reworded message silently change a script's exit code"). The prose these matched is the
// OPERATOR-FACING hint text from brokerCodeHints, which exists to be reworded; a hint that came to
// mention "download_http_status" while explaining something else would silently turn a skip into a
// fleet abort. Neither direction is safe: a false transient keeps rolling out a broken artifact, a
// false config error halts a fleet over one bad box.
//
// The fallback is deliberately NOT the old full list. It holds only needles that can never arrive as
// a wire code — Go's own transport/context prose — because for those there is nothing structural to
// match and today's behaviour is all there is. Every code-bearing needle moved to exact matching.
var (
	// transientUpgradeCodes: "this specific node is unreachable right now". `--all` logs + skips these
	// so a single OFFLINE box doesn't abort a fleet-wide rollout; the operator gets a final summary
	// and can retry just the skipped subset.
	transientUpgradeCodes = map[string]bool{
		"node_offline":         true,
		"node_not_found":       true,
		"agent_no_responders":  true,
		"agent_malformed_resp": true,
		// origin: line-2 closure verification §6 B1. The other half of the Y2 split. These two really do
		// clear on their own — a transport blip mid-download, and a PTY/fd/pty-count limit that frees as
		// sessions close — so a fleet rollout should SKIP the node and keep going rather than counting it
		// as a hard failure the operator has to interpret.
		//
		// pty_alloc_failed is here rather than in the config set for the same reason its exit class is 75:
		// see internal/agent/run.go's ptyTransientErrnos. Its sibling pty_unavailable is deliberately in
		// NEITHER set — a host with no /dev/ptmx is not transient, but it is also not a bad CALL, so
		// aborting the whole fleet over one misconfigured box would be wrong. It stays a per-node failure.
		"download_failed": true,
		// origin: external review M1 — a mirror that is down or rate-limiting is exactly the case for
		// skipping this node and continuing: the artifact is fine and the next node may well succeed.
		"download_http_retryable": true,
		"pty_alloc_failed":        true,
		// origin: upgrade-safety plan §4 — a prior upgrade on THIS node is inside its register
		// deadline; it self-resolves (commit or rollback) within that bound. Skip and retry later.
		"upgrade_in_progress": true,
	}
	// configUpgradeCodes: the CALL itself is bad (not the target). Aborts `--all` because no other node
	// will accept it either.
	configUpgradeCodes = map[string]bool{
		"not_owner":                     true,
		"url_not_allowed":               true,
		"sha256_invalid":                true,
		"proto_bump_requires_reinstall": true,
		"actor_invalid":                 true,
		"session_not_found_or_deleting": true,
		// origin: line-2 closure verification §6 B1, the review's self-declared most substantive item.
		//
		// The line-2 §12 Y2 split created these two codes precisely because `download_failed` was
		// telling automation to retry a permanently-broken download forever. It fixed the EXIT CLASS
		// (both → 64) and the hint text, and missed this list — a THIRD classification of the same
		// codes, in the same package, reached by the one command Y2 was written to rescue.
		//
		// The consequence was worse than a wrong exit code. Neither code matched here nor in the
		// transient set, so `node upgrade --all` counted the node as "✗ failed" and CARRIED ON —
		// fanning a typo'd --url out to every remaining node in the fleet. Aborting on exactly that is
		// what the comment above isConfigError's call site says this function is for.
		//
		// download_too_large is the same shape: the artifact is over the agent's ceiling, so it will be
		// over the ceiling on node 2 through node N as well.
		"download_http_status": true,
		"download_too_large":   true,
		// origin: upgrade-safety plan §4 — the sha-verified artifact failed the agent's pre-install
		// smoke gate (cannot exec / no version line): wrong arch or truncated build, which is wrong for
		// EVERY node. `--all` presumes a homogeneous fleet (one URL, one sha for all), so fanning it out
		// after the first smoke failure would push a known-bad artifact at the remaining N-1 nodes.
		"smoke_failed": true,
	}
	// transientCodelessNeedles: prose that can only come from Go's transport/context layer, which never
	// carries a wire code. Consulted ONLY when the error carries no code — never as an override.
	transientCodelessNeedles = []string{
		"deadline exceeded",
		"context canceled",
	}
)

// isTransientError reports whether `--all` should skip this node and keep going.
func isTransientError(err error) bool {
	if code := wireCodeOf(err); code != "" {
		return transientUpgradeCodes[code]
	}
	msg := err.Error()
	for _, needle := range transientCodelessNeedles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// isConfigError reports whether `--all` should abort the rest of the fleet.
func isConfigError(err error) bool {
	code := wireCodeOf(err)
	return code != "" && configUpgradeCodes[code]
}

// listOnlineNIDs round-trips a node.list.req (architecture B.1
// line 129) and returns the nids whose `nodes.status` is ONLINE.
// Uses the dedicated node-enum RPC instead of process-derived
// inference: a fresh agent that registered but never exec'd
// shows up here AND can be upgrade target, which the older
// ps-based heuristic missed.
// It also reports how many ONLINE rows were EXCLUDED for running under an
// assigned lease name, so the caller can say so out loud rather than silently
// upgrading a smaller fleet than the operator expects.
func listOnlineNIDs(ctx context.Context, nc *nats.Conn, sid, actor string) ([]string, int, error) {
	body, _ := json.Marshal(proto.NodeListReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respMsg, err := nc.RequestWithContext(respCtx,
		proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return nil, 0, fmt.Errorf("upgrade --all: node.list lookup: %w", err)
	}
	var nl proto.NodeListResp
	if err := json.Unmarshal(respMsg.Data, &nl); err != nil {
		return nil, 0, fmt.Errorf("upgrade --all: node.list decode: %w", err)
	}
	if nl.Code != "" {
		return nil, 0, fmt.Errorf("upgrade --all: node.list rejected: %s %s", nl.Code, nl.Error)
	}
	var out []string
	var skippedLeased int
	for _, n := range nl.Nodes {
		if n.Status != "ONLINE" {
			continue
		}
		// Q4: a LEASED instance is excluded from the fleet fan-out.
		//
		// Two concrete hazards, not a preference. (1) Upgrading one is
		// meaningless: it runs the binary baked into its image and reverts on
		// its next restart. (2) Targets are sorted lexicographically and
		// targets[0] is the canary that gates the entire fleet — a leased
		// instance can sort first, and if its pod recycles inside the wait
		// budget the whole rollout aborts having upgraded nothing.
		//
		// Explicit upgrades of the leased row and its basename are refused too:
		// shared-home clones may share one binary and rollback marker, so there is
		// no honest per-instance remote-upgrade boundary.
		if n.Leased {
			skippedLeased++
			continue
		}
		out = append(out, n.NID)
	}
	return out, skippedLeased, nil
}
