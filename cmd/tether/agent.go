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
	"github.com/spf13/cobra"
)

func newAgentCmd() *cobra.Command {
	var (
		natsURL string
		sid     string
		nid     string
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run agent daemon (register + heartbeat)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			a, err := agent.New(agent.Config{
				NATSURL: natsURL,
				SID:     sid,
				NID:     nid,
				Logger:  slog.New(slog.NewTextHandler(os.Stderr, nil)),
			})
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether agent: NATS=%s sid=%s nid=%s\n(press Ctrl-C to quit)\n",
				natsURL, sid, nid)

			if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&sid, "session", "", "session id (required)")
	cmd.Flags().StringVar(&nid, "nid", "", "node id (required)")
	_ = cmd.MarkFlagRequired("session")
	_ = cmd.MarkFlagRequired("nid")
	return cmd
}
