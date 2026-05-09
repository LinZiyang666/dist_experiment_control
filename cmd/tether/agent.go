package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/spf13/cobra"
)

func newAgentCmd() *cobra.Command {
	var (
		natsURL string
		sid     string
		nid     string
		pin     string
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run agent daemon (register + heartbeat)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := agent.Config{
				NATSURL: natsURL,
				SID:     sid,
				NID:     nid,
				PIN:     pin,
				Logger:  slog.New(slog.NewTextHandler(os.Stderr, nil)),
			}

			// TETHER_DEV_NO_AUTH (CLI-side env, see internal/cli.DevNoAuthEnv):
			// when set, skip loading the agent nkey and connect anonymously.
			// Only safe against a broker without auth_callout enabled. The
			// agent package handles Identity==nil as the anonymous path.
			if os.Getenv(cli.DevNoAuthEnv) == "" {
				home := cli.DefaultHome()
				id, err := cli.EnsureAgentIdentity(home, sid)
				if err != nil {
					return fmt.Errorf("agent identity: %w", err)
				}
				cfg.Identity = id
			}

			a, err := agent.New(cfg)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			fpDisplay := "anonymous"
			if cfg.Identity != nil {
				fpDisplay = cfg.Identity.Fingerprint
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether agent: NATS=%s sid=%s nid=%s identity=%s\n(press Ctrl-C to quit)\n",
				natsURL, sid, nid, fpDisplay)

			if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&sid, "session", "", "session id (required)")
	cmd.Flags().StringVar(&nid, "nid", "", "node id (required)")
	cmd.Flags().StringVar(&pin, "pin", "", "session PIN, required only on first connect (binds (sid,nid) to this agent's nkey)")
	_ = cmd.MarkFlagRequired("session")
	_ = cmd.MarkFlagRequired("nid")
	return cmd
}
