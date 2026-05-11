package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/spf13/cobra"
)

// defaultUpgradeURLPrefix matches the GitHub release URL that
// install.sh and goreleaser produce; keeping it here (rather than in
// serveconf) so a self-hosted distribution can override via yaml or
// flag without recompiling. Architecture J.4: "默认
// https://github.com/<org>/tether/releases/".
const defaultUpgradeURLPrefix = "https://github.com/LinZiyang666/dist_experiment_control/releases/"

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
		upgradeURLAllow  []string
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
			// public_host precedence: explicit --public-host > yaml
			// broker.public_host > yaml broker.domain (the architecture
			// A.3 skeleton key) > cobra default "localhost". Falling
			// back to broker.domain means a config that only sets
			// `domain: tether.example.com` Just Works for `tether
			// expose` URLs without requiring the operator to also
			// duplicate the value into a non-skeleton public_host key.
			publicHost = pickPublicHost(cmd, publicHost, fileCfg.Broker.PublicHost, fileCfg.Broker.Domain)
			storeDir = pickFlagOrYaml(cmd, "store-dir", storeDir, fileCfg.Broker.Storage.JSStore)
			adminSocket = pickFlagOrYaml(cmd, "admin-socket", adminSocket, fileCfg.Broker.Admin.Socket)

			// frp.port_range "low-high" → PortBandLow/High. Empty stays
			// 0/0 so broker.PortAllocCfg falls back to the
			// 14000-14999 default. Bad input is fatal — silently
			// landing on the default would surprise an operator who
			// configured a custom firewall range.
			bandLow, bandHigh, err := parsePortBand(fileCfg.Broker.Frp.PortRange)
			if err != nil {
				return fmt.Errorf("broker.yaml frp.port_range: %w", err)
			}

			db, err := storage.Open(dbPath)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			// Upgrade allowlist precedence: explicit
			// --upgrade-url-allow > yaml > built-in default. The
			// built-in default points at this binary's own GitHub
			// release prefix so `tether node upgrade` works out of
			// the box; operators who self-host artifacts override
			// via yaml or flag.
			allow := upgradeURLAllow
			if !cmd.Flags().Changed("upgrade-url-allow") {
				switch {
				case len(fileCfg.Broker.Upgrade.URLAllow) > 0:
					allow = fileCfg.Broker.Upgrade.URLAllow
				case len(allow) == 0:
					allow = []string{defaultUpgradeURLPrefix}
				}
			}

			cfg := broker.Config{
				NATSURL:             natsURL,
				DB:                  db,
				Logger:              slog.New(slog.NewTextHandler(os.Stderr, nil)),
				PublicHost:          publicHost,
				TunnelControlAddr:   tunnelCtrlAddr,
				TunnelPublicHost:    tunnelPublicHost,
				StoreDir:            storeDir,
				AdminSocketPath:     adminSocket,
				PortBandLow:         bandLow,
				PortBandHigh:        bandHigh,
				UpgradeURLAllowlist: allow,
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
	cmd.Flags().StringVar(&adminSocket, "admin-socket", defaultAdminSocket,
		"local Unix socket for `tether admin *` (architecture I.2b); set to empty to disable")
	cmd.Flags().StringSliceVar(&upgradeURLAllow, "upgrade-url-allow", nil,
		"URL prefixes accepted by `tether node upgrade` (architecture J.4); empty = upgrades disabled")
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

// pickPublicHost is pickFlagOrYaml with one extra fallback: if both
// the CLI flag default and yaml broker.public_host are empty, use
// yaml broker.domain (the architecture A.3 skeleton primary key).
// Lets a minimal broker.yaml that only sets `domain:` cover the
// expose-URL use case without requiring a duplicated key.
func pickPublicHost(cmd *cobra.Command, flagVal, yamlPublicHost, yamlDomain string) string {
	if cmd.Flags().Changed("public-host") {
		return flagVal
	}
	if yamlPublicHost != "" {
		return yamlPublicHost
	}
	if yamlDomain != "" {
		return yamlDomain
	}
	return flagVal
}

// parsePortBand parses an architecture A.3 frp.port_range string of
// the form "low-high" (e.g. "14000-14999") into two ints. Empty
// input returns (0, 0, nil) so broker.PortAllocCfg falls back to
// the 14000-14999 default. Inverted ranges, non-numeric tokens, and
// out-of-range values are rejected with a precise error so the
// operator's typo doesn't silently bind a valid-but-wrong band.
//
// The valid TCP-port range 1..65535 is accepted in full — this
// helper does not enforce a "must be within 14000-14999" policy.
// Operators with custom firewall ranges (e.g. an institutional
// allow-list of 30000-30099) can configure them here; the
// architecture's 14000-14999 default is just the default, not a
// hard cap. Documented so future readers don't mistake the absent
// upper-bound check for a missing validation.
func parsePortBand(s string) (low, high int, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, nil
	}
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("expected \"low-high\", got %q", s)
	}
	low, err = strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("low: %w", err)
	}
	high, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("high: %w", err)
	}
	if low <= 0 || high <= 0 || low > 65535 || high > 65535 {
		return 0, 0, fmt.Errorf("ports out of range: %d-%d", low, high)
	}
	if low > high {
		return 0, 0, fmt.Errorf("low %d > high %d", low, high)
	}
	return low, high, nil
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
