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
	)
	cmd := &cobra.Command{
		Use:   "upgrade <nid>",
		Short: "Trigger an owner-only agent binary upgrade (architecture J.4)",
		Long: `Sends a tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req
to the broker. The broker enforces owner-only + URL allowlist +
proto match before forwarding to the agent. The agent downloads
the tarball, verifies SHA256 + URL allowlist locally, atomically
replaces its own binary, then exits so the supervisor (systemd /
setsid wrapper) can launch the new binary.

URL must be one the broker has whitelisted (broker.yaml or
--upgrade-url-allow). SHA256 is the hex digest of the tarball,
NOT of the extracted binary.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nid := args[0]
			if urlFlag == "" || shaFlag == "" {
				return fmt.Errorf("--url and --sha256 are required")
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

			body, _ := json.Marshal(proto.UpgradeReq{
				URL:          urlFlag,
				SHA256:       shaFlag,
				ProtoVersion: proto.ProtoVersion,
			})
			ctx, cancel := context.WithTimeout(cmd.Context(), time.Duration(timeoutSec)*time.Second)
			defer cancel()
			respMsg, err := nc.RequestWithContext(ctx,
				proto.SubjCmdBy(sid, id.PublicKey, nid, "upgrade"), body)
			if err != nil {
				return fmt.Errorf("upgrade request: %w", err)
			}
			var resp proto.UpgradeResp
			if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
				return fmt.Errorf("decode reply: %w", err)
			}
			if !resp.OK {
				return fmt.Errorf("broker rejected upgrade: %s %s", resp.Code, resp.Error)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"✔ upgrade dispatched to %s/%s (agent will exit and restart)\n", sid, nid)
			return nil
		},
	}
	cmd.Flags().StringVar(&urlFlag, "url", "", "absolute https:// URL of the new release tarball (required)")
	cmd.Flags().StringVar(&shaFlag, "sha256", "", "hex SHA256 of the tarball (required)")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().IntVar(&timeoutSec, "timeout", 60,
		"seconds to wait for broker reply (must exceed agent download time)")
	return cmd
}
