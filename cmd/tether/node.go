package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradecli/legacy"
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
	// origin: hot-update S8 plan §7.1 / DC06 — sibling verbs, never children of
	// `node upgrade <nid>`, so a NID named status/wait/rollback stays addressable.
	root.AddCommand(upgradecli.NodeTransactionCommands(nodeTxPorts())...)
	return root
}

// nodeTxPorts supplies the cmd-owned edges of the S8 node transaction verbs:
// the private-inbox ctl connection (a no-responder is provably not sent) and
// the exit taxonomy. The target describes itself over upgrade.describe.
// origin: hot-update S8 plan §7.1 / §11.2 P19; S8 FIX-DESCRIBE (request R1)
func nodeTxPorts() upgradecli.NodePorts {
	open := func(cmd *cobra.Command, verb, home, natsURL string) (*upgradecli.NodeSession, error) {
		sid := cli.ReadCurrentSession(home)
		if sid == "" {
			return nil, fmt.Errorf("no active session — run `tether login -s <sid>` first")
		}
		id, err := cli.EnsureIdentity(home)
		if err != nil {
			return nil, err
		}
		nc, err := connectCtl(cmd, verb, home, cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home), id, nats.Name(cli.CtlNameForSession(sid)))
		if err != nil {
			return nil, err
		}
		return upgradecli.NewNodeSession(sid, id.PublicKey, id.Fingerprint, natsRequest(nc), nc.Close), nil
	}
	return upgradecli.NodePorts{Open: open, Exit: upgradeExit, DefaultHome: cli.DefaultHome(), Describe: upgradecli.DescribeNode, Fetch: binaryupgrade.FetchURL, Probe: probeUpgradeImage}
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
		RunE: func(cmd *cobra.Command, args []string) (resultErr error) {
			admission, err := beginCtlAdmission(cmd)
			if err != nil {
				return err
			}
			defer finishCtlAdmission(admission, &resultErr)
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
			respMsg, err := requestCtlWithin(cmd, nc, proto.SubjCtrlNodeList(id.PublicKey, sid), body, upgradebudget.RequestLatency)
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
				if err := withBannerWithin(cmd, nc, id.PublicKey, true); err != nil {
					return err
				} // suppressed under --json
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
			if err := withBannerWithin(cmd, nc, id.PublicKey, false); err != nil {
				return err
			}
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
		tx         upgradecli.UpgradeOptions
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
			// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P19 — the transaction
			// forms address exactly one NID; the default form stays on the old wire.
			if tx.TxID != "" || tx.PrepareOnly || tx.GrantOut != "" || tx.ArtifactReceipt != "" || tx.Interrupting {
				if all || len(args) != 1 {
					return usageErr("transaction upgrades address exactly one <nid> and exclude --all")
				}
				tx.URL, tx.SHA256 = urlFlag, shaFlag
				return upgradecli.RunNodeUpgrade(cmd, nodeTxPorts(), args[0], home, natsURL, tx, time.Duration(timeoutSec)*time.Second)
			}
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
	cmd.Flags().StringVar(&tx.TxID, "tx-id", "", "resume only the original transaction (with --artifact-receipt: Start it)")
	cmd.Flags().BoolVar(&tx.PrepareOnly, "prepare-only", false, "obtain the target-issued inert grant only; requires --grant-out")
	cmd.Flags().StringVar(&tx.GrantOut, "grant-out", "", "write the prepared grant here for `tether upgrade stage`")
	cmd.Flags().StringVar(&tx.ArtifactReceipt, "artifact-receipt", "", "artifact receipt from `tether upgrade stage` for --tx-id")
	cmd.Flags().BoolVar(&tx.Interrupting, "interrupting", false, "explicit non-hot first migration of a legacy agent (never counted hot)")
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
	// The canary/fleet loop moved verbatim to legacy.RunLegacyFleet (S8 P19 slim move).
	return legacy.RunLegacyFleet(cmd.OutOrStdout(), cmd.ErrOrStderr(), legacy.LegacyFleet{
		Baseline: func(nid string) (string, error) { return captureReleaseBaseline(cmd, nc, sid, actor, nid) },
		Dispatch: func(nid string) (string, error) {
			return dispatchUpgrade(cmd, nc, sid, actor, nid, urlFlag, shaFlag, timeout)
		},
		Wait: func(nid, newVersion, start string) error {
			return waitForUpgradeCommit(cmd, nc, sid, actor, nid, newVersion, start)
		},
		Transient: isTransientError, Config: isConfigError}, sid, targets)
}

// dispatchUpgrade fires one upgrade.req at (sid, nid); the body and its
// legacy-NewVersion normalization moved verbatim to
// legacy.DispatchLegacyUpgrade (S8 P19 slim move).
func dispatchUpgrade(cmd *cobra.Command, nc *nats.Conn,
	sid, actor, nid, url, sha256 string, timeout time.Duration,
) (string, error) {
	return legacy.DispatchLegacyUpgrade(cmd.Context(), natsRequest(nc), cmd.OutOrStdout(), brokerErrorMessage, sid, actor, nid, url, sha256, timeout)
}

// natsRequest adapts a ctl connection to the upgrade leaves' request port.
func natsRequest(nc *nats.Conn) upgradecli.RequestFunc {
	return func(ctx context.Context, subject string, body []byte) ([]byte, error) {
		if nc == nil {
			return nil, nats.ErrInvalidConnection
		}
		msg, err := nc.RequestWithContext(ctx, subject, body)
		if err != nil {
			return nil, err
		}
		return msg.Data, nil
	}
}

// upgradeWaitBudget bounds waitForUpgradeCommit's polling: the agent's own
// register deadline is 120s (upgradebudget.ComponentHandover, which
// internal/agent's upgradeRegisterDeadline reads — the number in the
// "deadline 120s" line below), plus slack for the re-exec, the reconnect
// backoff, and our 3s poll grain. A var, not a const, so the timeout-path
// test does not have to sit through the real budget.
var upgradeWaitBudget = upgradebudget.ComponentHandover + 30*time.Second

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
	// proto.SameRelease, and the POLARITY HERE IS THE OPPOSITE of the sites that comment
	// was written for — origin: prerelease audit round 2, K-F12. Everywhere else
	// SameRelease widens an "already at target, skip" test, where a false NEGATIVE means
	// redundant work. At this one it widens a REFUSAL, so widening produces MORE
	// UNCONFIRMED verdicts, and the pasted claim that it "prevents reporting a successful
	// upgrade as unconfirmed" reads exactly backwards.
	//
	// It is still right, for a reason that has to be stated rather than inherited:
	// `v0.5.1` and `0.5.1` ARE the same release, so a node reporting one when the ctl
	// asked for the other really is a same-tag re-push with no re-register proof — the
	// case this branch exists to refuse. A `==` comparison would have called them
	// different releases and gone on to wait for a re-register that cannot be
	// distinguished from the old row, which is the hollow COMMITTED the paragraph above
	// describes. So SameRelease turns a WRONG confirmation into an honest refusal; it does
	// not turn a refusal into a confirmation. (DRD-F2/F3 converted the family.)
	if newVersion != "" && proto.SameRelease(newVersion, startRelease) {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(),
			"⚠ %s: node already reports %s — a same-tag re-push cannot be confirmed via release change.\n"+
				"  UNCONFIRMED: staged + smoke passed, but re-register proof is unavailable; verify via the\n"+
				"  agent/broker logs (upgrade_state line) before trusting this node.\n",
			nid, newVersion)
		return &ExitError{Class: exitUsage, Err: fmt.Errorf(
			"upgrade %s/%s: UNCONFIRMED — same-tag re-push has no re-register proof (verify via agent/broker logs)",
			sid, nid)}
	}
	// The rendered bytes must stay "deadline 120s": drill 33's A1 arm greps
	// this exact line, and the number is upgradebudget.ComponentHandover.
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  waiting for re-register (deadline %ds) …\n", int(upgradebudget.ComponentHandover/time.Second))

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
			if proto.SameRelease(entry.ReleaseVersion, newVersion) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"✔ %s: registered as %s — upgrade COMMITTED\n", nid, entry.ReleaseVersion)
				return nil
			}
			// SameRelease — origin: prerelease audit round 2, the K-F* sweep's gate. A
			// v-prefix difference alone is NOT a release change, and reading it as one
			// mints a hollow COMMITTED for a host that never moved.
		} else if startRelease != "" && entry.ReleaseVersion != "" && !proto.SameRelease(entry.ReleaseVersion, startRelease) {
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

// lookupNodeEntry fetches one node's row from the node.list RPC (body in upgradecli).
func lookupNodeEntry(ctx context.Context, nc *nats.Conn, sid, actor, nid string) (proto.NodeListEntry, error) {
	return legacy.LookupNodeEntry(ctx, natsRequest(nc), sid, actor, nid)
}

// HOW `node upgrade --all` CLASSIFIES A PER-NODE FAILURE: two disjoint wire-code sets plus a
// codeless transport-prose fallback. The tables and their full rationale moved verbatim to
// internal/upgradecli/legacy_node.go (S8 P19 slim move); these names keep the classifiers below.
var (
	transientUpgradeCodes    = legacy.LegacyTransientCodes
	configUpgradeCodes       = legacy.LegacyConfigCodes
	transientCodelessNeedles = legacy.LegacyCodelessNeedles
)

// isTransientError reports whether `--all` should skip this node and keep going.
func isTransientError(err error) bool {
	// origin: hot-update S6 plan §9.1 / T36
	var classified *ExitError
	if errors.As(err, &classified) && classified.Class == exitUnsettled {
		return false
	}
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

// listOnlineNIDs returns the ONLINE, non-leased nids and the leased count;
// the body and its Q4 lease-exclusion rationale live in legacy.ListOnlineNIDs.
func listOnlineNIDs(ctx context.Context, nc *nats.Conn, sid, actor string) ([]string, int, error) {
	return legacy.ListOnlineNIDs(ctx, natsRequest(nc), sid, actor)
}
