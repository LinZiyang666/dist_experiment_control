package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	var (
		configPath       string
		natsURL          string
		dbPath           string
		authSeedsDir     string
		tunnelCtrlAddr   string
		tunnelPublicHost string
		publicHost       string
		storeDir         string
		adminSocket      string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run broker daemon (NATS subscriber + node state machine)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			fileCfg, err := serveconf.Load(configPath)
			if err != nil {
				return err
			}
			// CLI flag wins iff supplied; otherwise broker.yaml value;
			// otherwise the cobra default. cmd.Flags().Changed lets us
			// distinguish "user passed --foo" from "cobra applied the
			// default value", which is the standard precedence rule for
			// "config file + flag override" configs.
			natsURL = pickFlagOrYaml(cmd, "nats-url", natsURL, fileCfg.Broker.NATS.URL)
			dbPath = pickFlagOrYaml(cmd, "db", dbPath, fileCfg.Broker.Storage.DB)
			tunnelCtrlAddr = pickFlagOrYaml(cmd, "tunnel-addr", tunnelCtrlAddr, fileCfg.Broker.Frp.ControlListen)
			tunnelPublicHost = pickFlagOrYaml(cmd, "tunnel-public-host", tunnelPublicHost, fileCfg.Broker.Frp.BindAddr)
			publicHost = pickFlagOrYaml(cmd, "public-host", publicHost, fileCfg.Broker.PublicHost)
			storeDir = pickFlagOrYaml(cmd, "store-dir", storeDir, fileCfg.Broker.Storage.JSStore)
			adminSocket = pickFlagOrYaml(cmd, "admin-socket", adminSocket, fileCfg.Broker.Admin.Socket)

			db, err := storage.Open(dbPath)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			cfg := broker.Config{
				NATSURL:           natsURL,
				DB:                db,
				Logger:            slog.New(slog.NewTextHandler(os.Stderr, nil)),
				PublicHost:        publicHost,
				TunnelControlAddr: tunnelCtrlAddr,
				TunnelPublicHost:  tunnelPublicHost,
				StoreDir:          storeDir,
				AdminSocketPath:   adminSocket,
			}

			// auth_callout: enabled iff --auth-callout-seeds-dir is supplied
			// and contains both broker.nk and account.nk. The matching
			// nats-server.conf must list the broker pubkey under
			// `nkeys` + `authorization.auth_callout.auth_users`, and the
			// account pubkey under `authorization.auth_callout.issuer`.
			// (See architecture E.2 / K.3 for the full deployment shape.)
			if authSeedsDir != "" {
				ac, err := loadAuthCalloutSeeds(authSeedsDir)
				if err != nil {
					return fmt.Errorf("auth_callout seeds: %w", err)
				}
				cfg.AuthCallout = ac
			}

			b, err := broker.New(cfg)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			authMode := "off (dev / P2-style)"
			if cfg.AuthCallout != nil {
				authMode = "on (seeds=" + authSeedsDir + ")"
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether serve: NATS=%s DB=%s auth_callout=%s\n(press Ctrl-C to quit)\n",
				natsURL, dbPath, authMode)

			if err := b.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "",
		"path to broker.yaml (architecture A.3); empty = no file, every value comes from flags")
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&dbPath, "db", "./tether.db", "SQLite database file (use \":memory:\" for ephemeral)")
	cmd.Flags().StringVar(&authSeedsDir, "auth-callout-seeds-dir", "",
		"directory containing broker.nk + account.nk for auth_callout (P3+ secure mode); empty = dev/P2 mode")
	cmd.Flags().StringVar(&publicHost, "public-host", "localhost",
		"DNS name printed in expose URLs (operator-facing)")
	cmd.Flags().StringVar(&tunnelCtrlAddr, "tunnel-addr", "0.0.0.0:7000",
		"reverse-TCP tunnel control listener (host:port); empty disables P6 data plane")
	cmd.Flags().StringVar(&tunnelPublicHost, "tunnel-public-host", "0.0.0.0",
		"bind address for the public per-port tunnel listeners (default 0.0.0.0)")
	cmd.Flags().StringVar(&storeDir, "store-dir", "",
		"JetStream store dir to monitor for disk pressure (P7/H.4); empty = monitor disabled")
	cmd.Flags().StringVar(&adminSocket, "admin-socket", "",
		"local Unix socket for `tether admin *` (architecture I.2b); empty = admin endpoint disabled")
	return cmd
}

// pickFlagOrYaml returns flagVal when the user explicitly passed the
// CLI flag (cobra's Changed reports true), and yamlVal otherwise.
// Yaml-empty falls back to the cobra default already loaded into
// flagVal, so the precedence is: explicit flag > yaml > default.
func pickFlagOrYaml(cmd *cobra.Command, flag, flagVal, yamlVal string) string {
	if cmd.Flags().Changed(flag) {
		return flagVal
	}
	if yamlVal != "" {
		return yamlVal
	}
	return flagVal
}

// loadAuthCalloutSeeds reads broker.nk and account.nk (both 0600 files
// containing nkey seeds) from dir. Both must exist.
func loadAuthCalloutSeeds(dir string) (*broker.AuthCalloutConfig, error) {
	brokerSeed, err := os.ReadFile(filepath.Join(dir, "broker.nk"))
	if err != nil {
		return nil, fmt.Errorf("read broker.nk: %w", err)
	}
	accountSeed, err := os.ReadFile(filepath.Join(dir, "account.nk"))
	if err != nil {
		return nil, fmt.Errorf("read account.nk: %w", err)
	}
	return &broker.AuthCalloutConfig{
		BrokerNkeySeed: brokerSeed,
		AccountSeed:    accountSeed,
	}, nil
}
