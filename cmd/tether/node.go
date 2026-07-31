package main

import (
	"context"
	"encoding/json"
	"fmt"
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
			_, _ = fmt.Fprintln(tw, "NODE\tSTATUS\tHEARTBEAT\tPROTO\tRELEASE")
			for _, n := range shown {
				age := "-"
				if !n.LastHeartbeatAt.IsZero() {
					age = humanizeAgo(now, n.LastHeartbeatAt)
				}
				release := n.ReleaseVersion
				if release == "" {
					release = "-"
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
					n.NID, n.Status, age, n.ProtoVersion, release)
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
	)
	cmd := &cobra.Command{
		Use:   "upgrade <nid>|--all",
		Short: "Trigger an owner-only agent binary upgrade (architecture J.4)",
		Long: fmt.Sprintf(`Sends a %s.s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req
to the broker. The broker enforces owner-only + URL allowlist +
proto match before forwarding to the agent. The agent downloads
the tarball, verifies SHA256 + URL allowlist locally, atomically
replaces its own binary, then re-execs in place (PID preserved
under systemd / setsid). G.1 reconcile runs as the new binary
re-registers.

URL must be one the broker has whitelisted (broker.yaml or
--upgrade-url-allow). SHA256 is the hex digest of the tarball,
NOT of the extracted binary.

--all upgrades every ONLINE node in the active session (sequential,
fail-fast). Use it for fleet-wide patch rollouts; for canary
testing run a single <nid> first.`, proto.SubjectPrefix),
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
				targets, err = listOnlineNIDs(cmd.Context(), nc, sid, id.PublicKey)
				if err != nil {
					return err
				}
				if len(targets) == 0 {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "(no ONLINE nodes in session)")
					return nil
				}
			}

			// Fleet rollouts must distinguish transient from
			// configuration errors:
			//   - node_offline / agent_no_responders / timeout: keep
			//     going; the operator will retry these later.
			//   - everything else (not_owner / url_not_allowed /
			//     proto_bump / sha256_invalid): config bug, abort
			//     the rest of the fleet so we don't fan-out a known-
			//     bad request.
			// Single-nid mode (len(targets) == 1) keeps the original
			// strict behavior: one explicit target, one yes/no answer.
			var skipped, failed int
			strict := !all
			for _, nid := range targets {
				err := dispatchUpgrade(cmd, nc, sid, id.PublicKey, nid,
					urlFlag, shaFlag, time.Duration(timeoutSec)*time.Second)
				if err == nil {
					continue
				}
				if strict || isConfigError(err) {
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
			if all && skipped > 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"(%d node(s) skipped due to transient errors — retry later)\n", skipped)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&urlFlag, "url", "", "absolute https:// URL of the new release tarball (required)")
	cmd.Flags().StringVar(&shaFlag, "sha256", "", "hex SHA256 of the tarball (required)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVar(&timeoutSec, "timeout", 60,
		"seconds to wait for broker reply (must exceed agent download time)")
	cmd.Flags().BoolVar(&all, "all", false,
		"upgrade every ONLINE node in the active session (sequential, fail-fast)")
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

// dispatchUpgrade fires one upgrade.req at (sid, nid) and reports
// the broker's reply. Extracted so the --all loop can reuse it.
func dispatchUpgrade(cmd *cobra.Command, nc *nats.Conn,
	sid, actor, nid, url, sha256 string, timeout time.Duration,
) error {
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
		return fmt.Errorf("upgrade %s/%s: %w (broker or agent unreachable on NATS)", sid, nid, err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return fmt.Errorf("decode reply for %s: %w (broker version skew?)", nid, err)
	}
	if !resp.OK {
		return brokerErrorMessage(fmt.Sprintf("upgrade %s/%s", sid, nid), resp.Code, resp.Error)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"✔ upgrade dispatched to %s/%s (agent re-exec in progress)\n", sid, nid)
	return nil
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
func listOnlineNIDs(ctx context.Context, nc *nats.Conn, sid, actor string) ([]string, error) {
	body, _ := json.Marshal(proto.NodeListReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respMsg, err := nc.RequestWithContext(respCtx,
		proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return nil, fmt.Errorf("upgrade --all: node.list lookup: %w", err)
	}
	var nl proto.NodeListResp
	if err := json.Unmarshal(respMsg.Data, &nl); err != nil {
		return nil, fmt.Errorf("upgrade --all: node.list decode: %w", err)
	}
	if nl.Code != "" {
		return nil, fmt.Errorf("upgrade --all: node.list rejected: %s %s", nl.Code, nl.Error)
	}
	var out []string
	for _, n := range nl.Nodes {
		if n.Status == "ONLINE" {
			out = append(out, n.NID)
		}
	}
	return out, nil
}
