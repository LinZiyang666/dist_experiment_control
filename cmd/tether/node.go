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

// isTransientError returns true for broker reply codes that signal
// "this specific node is unreachable right now" — `--all` should
// log + skip these so a single OFFLINE box doesn't abort a
// fleet-wide rollout. The operator gets a final summary and can
// retry just the skipped subset.
func isTransientError(err error) bool {
	msg := err.Error()
	for _, needle := range []string{
		"node_offline",
		"node_not_found",
		"agent_no_responders",
		"agent_malformed_resp",
		"deadline exceeded",
		"context canceled",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// isConfigError returns true for broker reply codes that mean the
// CALL itself is bad (not the target). Aborts `--all` because no
// other node will accept it either.
func isConfigError(err error) bool {
	msg := err.Error()
	for _, needle := range []string{
		"not_owner",
		"url_not_allowed",
		"sha256_invalid",
		"proto_bump_requires_reinstall",
		"actor_invalid",
		"session_not_found_or_deleting",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
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
