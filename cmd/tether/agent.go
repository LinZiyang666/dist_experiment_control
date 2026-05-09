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
		natsURL    string
		sid        string
		nid        string
		pin        string
		tunnelAddr string
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run agent daemon (register + heartbeat + expose tunnel client)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			home := cli.DefaultHome()
			cfg := agent.Config{
				NATSURL: natsURL,
				SID:     sid,
				NID:     nid,
				PIN:     pin,
				Home:    home,
				Logger:  slog.New(slog.NewTextHandler(os.Stderr, nil)),
			}

			// TETHER_DEV_NO_AUTH (CLI-side env, see internal/cli.DevNoAuthEnv):
			// when set, skip loading the agent nkey and connect anonymously.
			// Only safe against a broker without auth_callout enabled. The
			// agent package handles Identity==nil as the anonymous path.
			if os.Getenv(cli.DevNoAuthEnv) == "" {
				id, err := cli.EnsureAgentIdentity(home, sid)
				if err != nil {
					return fmt.Errorf("agent identity: %w", err)
				}
				cfg.Identity = id
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			// Wire the P6 tunnel adapter so `tether expose` actually
			// forwards TCP traffic. tunnelAddr is the broker side's
			// reverse-tunnel control listener (default :7000); leave
			// the flag at "" to disable the data plane (control plane
			// still works — useful for debugging).
			if tunnelAddr != "" {
				adapter := agent.NewTunnelExposeAdapter(tunnelAddr, sid, nid, cfg.Logger)
				adapter.Start(ctx)
				cfg.ExposeAdapter = adapter
			}

			a, err := agent.New(cfg)
			if err != nil {
				return err
			}

			fpDisplay := "anonymous"
			if cfg.Identity != nil {
				fpDisplay = cfg.Identity.Fingerprint
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether agent: NATS=%s sid=%s nid=%s identity=%s tunnel=%s\n(press Ctrl-C to quit)\n",
				natsURL, sid, nid, fpDisplay, displayOrOff(tunnelAddr))

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
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "127.0.0.1:7000",
		"broker reverse-TCP tunnel control address (host:port); empty to disable data plane")
	_ = cmd.MarkFlagRequired("session")
	_ = cmd.MarkFlagRequired("nid")
	return cmd
}

func displayOrOff(s string) string {
	if s == "" {
		return "off"
	}
	return s
}
