package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	return root
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
		Long: `Sends a tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req
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
testing run a single <nid> first.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if urlFlag == "" || shaFlag == "" {
				return fmt.Errorf("--url and --sha256 are required")
			}
			if all && len(args) > 0 {
				return fmt.Errorf("--all and explicit <nid> are mutually exclusive")
			}
			if !all && len(args) == 0 {
				return fmt.Errorf("either <nid> or --all is required")
			}

			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return fmt.Errorf("upgrade: connect: %w", err)
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

			for _, nid := range targets {
				if err := dispatchUpgrade(cmd, nc, sid, id.PublicKey, nid,
					urlFlag, shaFlag, time.Duration(timeoutSec)*time.Second); err != nil {
					return err
				}
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
		return fmt.Errorf("upgrade %s/%s: %w", sid, nid, err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return fmt.Errorf("decode reply for %s: %w", nid, err)
	}
	if !resp.OK {
		return fmt.Errorf("broker rejected upgrade for %s/%s: %s %s",
			sid, nid, resp.Code, resp.Error)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"✔ upgrade dispatched to %s/%s (agent re-exec in progress)\n", sid, nid)
	return nil
}

// listOnlineNIDs round-trips a ps.req and returns the unique nids
// whose owning node is currently ONLINE — exactly the set a
// fleet-wide --all upgrade should target. Skips OFFLINE / STALE
// (broker would reject those anyway).
func listOnlineNIDs(ctx context.Context, nc *nats.Conn, sid, actor string) ([]string, error) {
	body, _ := json.Marshal(proto.PsReq{})
	respCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	respMsg, err := nc.RequestWithContext(respCtx, proto.SubjCtrlPs(actor, sid), body)
	if err != nil {
		return nil, fmt.Errorf("upgrade --all: ps lookup: %w", err)
	}
	var ps proto.PsResp
	if err := json.Unmarshal(respMsg.Data, &ps); err != nil {
		return nil, fmt.Errorf("upgrade --all: ps decode: %w", err)
	}
	if ps.Code != "" {
		return nil, fmt.Errorf("upgrade --all: ps rejected: %s %s", ps.Code, ps.Error)
	}
	seen := map[string]bool{}
	var out []string
	// PsResp.Processes carries one row per process; we want unique
	// node ids. The status filter avoids dispatching to OFFLINE /
	// LOST nodes (broker would reject them at the node_offline
	// gate, but failing one of those mid-loop would short-circuit
	// the whole --all run).
	for _, p := range ps.Processes {
		if seen[p.NID] {
			continue
		}
		if p.Status != "RUNNING" && p.Status != "EXITED" {
			// LOST / unknown statuses signal the node is OFFLINE;
			// skip to avoid abort-on-first-failure mid-fleet.
			continue
		}
		seen[p.NID] = true
		out = append(out, p.NID)
	}
	// Some sessions have nodes with NO processes yet — they show up
	// in ps.Processes only after their first exec. Future work:
	// expose a dedicated nodes RPC; for v1 the process-derived list
	// covers the common upgrade target (a node that's been used).
	return out, nil
}
