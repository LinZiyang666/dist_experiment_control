package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradecli/legacy"
	"github.com/spf13/cobra"
)

// The default upgrade URL prefix and its precedence moved verbatim to
// internal/upgradecli/allowlist.go (hot-update S8 plan §11.2 P19 slim move).

func newServeCmd() *cobra.Command {
	var (
		configPath        string
		natsURL           string
		dbPath            string
		authSeedsDir      string
		tunnelCtrlAddr    string
		tunnelPublicHost  string
		publicHost        string
		storeDir          string
		adminSocket       string
		upgradeURLAllow   []string
		subHTTPListen     string
		clusterDataDir    string
		clusterRaftAddr   string
		clusterSecrets    string
		colocatedAgentNID string
		logLevel          string
		logJSON           bool
		logFile           string
		metricsListen     string
		manifestListen    string
		natsConfPath      string
		natsServerBin     string
		alertWebhookURL   string
		diskCheckInterval time.Duration
		configCheck       bool
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run broker daemon (NATS subscriber + node state machine)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (runErr error) {
			// origin: hot-update S6 plan §4.1 / T03
			// Keep S3 W1 cleanup and ordinary agent recovery, but never open
			// a production broker store after losing an inherited dispatch.
			if img := agentboot.Current(); img != nil && img.Phase() == agentboot.PhaseLost {
				return fmt.Errorf("broker: refusing cold start after unreadable dispatch")
			}
			if brokerBoot != nil {
				data, err := brokerimage.BootConfigBytes(os.Args)
				if err != nil || brokerBoot.CheckConfig(data, os.Args) != nil {
					return brokerimage.ErrImage
				}
			}
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
			subHTTPListen = pickFlagOrYaml(cmd, "sub-http-listen", subHTTPListen, fileCfg.Broker.Sub.Listen)
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
			// D9 cluster cutover surface — paths only; all empty ⇒ single mode (no
			// behavior change). The authoritative cluster trigger is the on-disk raft/
			// probe (broker.clusterModeEnabled), cross-checked against ClusterDataDir.
			clusterDataDir = pickFlagOrYaml(cmd, "cluster-data-dir", clusterDataDir, fileCfg.Broker.Cluster.DataDir)
			clusterRaftAddr = pickFlagOrYaml(cmd, "cluster-raft-addr", clusterRaftAddr, fileCfg.Broker.Cluster.RaftAddr)
			clusterSecrets = pickFlagOrYaml(cmd, "cluster-secrets-dir", clusterSecrets, fileCfg.Broker.Cluster.SecretsDir)
			colocatedAgentNID = pickFlagOrYaml(cmd, "colocated-agent-nid", colocatedAgentNID, fileCfg.Broker.Cluster.ColocatedAgentNID)
			// B5/B6 observability knobs: flag wins, else broker.yaml, else the cobra default.
			logLevel = pickFlagOrYaml(cmd, "log-level", logLevel, fileCfg.Broker.Obs.LogLevel)
			metricsListen = pickFlagOrYaml(cmd, "metrics-listen", metricsListen, fileCfg.Broker.Obs.MetricsListen)
			manifestListen = pickFlagOrYaml(cmd, "cluster-manifest-listen", manifestListen, fileCfg.Broker.Cluster.ManifestListen)
			natsConfPath = pickFlagOrYaml(cmd, "nats-conf-path", natsConfPath, fileCfg.Broker.Cluster.NatsConfPath)
			natsServerBin = pickFlagOrYaml(cmd, "nats-server-bin", natsServerBin, fileCfg.Broker.Cluster.NatsServerBin)
			alertWebhookURL = pickFlagOrYaml(cmd, "alert-webhook-url", alertWebhookURL, fileCfg.Broker.Obs.AlertWebhookURL)
			if !cmd.Flags().Changed("log-json") && fileCfg.Broker.Obs.LogJSON {
				logJSON = true
			}

			// frp.port_range "low-high" → PortBandLow/High. Empty stays
			// 0/0 so broker.PortAllocCfg falls back to the
			// 14000-14999 default. Bad input is fatal — silently
			// landing on the default would surprise an operator who
			// configured a custom firewall range.
			bandLow, bandHigh, err := parsePortBand(fileCfg.Broker.Frp.PortRange)
			if err != nil {
				return fmt.Errorf("broker.yaml frp.port_range: %w", err)
			}

			// proc_retention / proc_gc_interval — yaml-only knobs.
			// Empty values stay 0 so broker.New applies its
			// built-in defaults (1h / 5m). Parsing happens here,
			// BEFORE storage.Open, so a misconfigured value
			// (e.g. `proc_retention: -1h`) fails the launch
			// instead of leaving behind a half-migrated tether.db
			// on disk.
			procRetention, err := fileCfg.ProcRetentionDuration()
			if err != nil {
				return err
			}
			procGCInterval, err := fileCfg.ProcGCIntervalDuration()
			if err != nil {
				return err
			}
			// #58/P10: home-authoritative orphan xfer-object reap cadence (yaml-only knob, cluster
			// section). Empty ⇒ 0 ⇒ broker.New's 5m default. Parsed here so a bad value fails the
			// launch before storage.Open, same as the proc knobs above.
			xferReapInterval, err := fileCfg.XferReapIntervalDuration()
			if err != nil {
				return err
			}
			// R16 #58 Lane C: the leader cross-home GC age floor (empty ⇒ 0 ⇒ broker.New's derived 3×tier-B
			// default). A deploy-tier drill compresses it to observe the GC; production leaves it unset.
			xferCrossHomeReapAge, err := fileCfg.XferCrossHomeReapAgeDuration()
			if err != nil {
				return err
			}

			// #39: disk-pressure monitor interval. Precedence flag > yaml > built-in default:
			// an explicit --disk-check-interval wins; else broker.observability.disk_check_interval;
			// else 0, which startDiskMonitor reads as "use the 5m default". Parsing the yaml here
			// (before storage.Open) means a bad value aborts launch instead of half-creating tether.db.
			if !cmd.Flags().Changed("disk-check-interval") {
				yamlDisk, derr := fileCfg.DiskCheckIntervalDuration()
				if derr != nil {
					return derr
				}
				if yamlDisk != 0 {
					diskCheckInterval = yamlDisk
				}
			}

			// D9 C-1 (single WAL owner): in CLUSTER mode the cluster.Node opens the merged
			// WAL DB and is the sole writable pool — serve.go must NOT call storage.Open (a
			// second pool that also runs migrations is a locking/corruption hazard). Decide
			// the mode from the on-disk raft state BEFORE touching the DB; only single mode
			// opens it here (broker.Run re-points b.cfg.DB to node.RODB() in cluster mode).
			// Configuration probes derive defaults from the requested mode, without
			// opening the live Bolt store. Recoverable-state checks belong to real
			// startup, after the reader probe has exited; only a daemon checks
			// initialized state.
			// origin: hot-update S6 plan §3.1 / T06; internal review rv-writer-5 / rv-fd-7
			clusterMode := clusterDataDir != ""
			if !configCheck {
				if brokerBoot == nil {
					clusterMode, err = broker.DetectClusterMode(clusterDataDir)
					if err != nil {
						return err
					}
				}
			}
			// C3: in cluster mode the per-broker topology reconciler manages the live nats.conf. Default
			// the path so a standard install converges automatically; an operator can override or set it
			// empty to opt out (then the broker reports no topology and is not held out of HEALTHY-HA).
			// G1 #22: the reconciler (User=tether) rewrites this conf atomically, so it lives in the
			// tether-owned /etc/tether/nats.d/ subdir ($ETC_DIR itself stays root-owned; see install.sh).
			// A pre-G1 host whose conf is still /etc/tether/nats.conf MUST migrate it (or set an explicit
			// nats_conf_path) BEFORE this binary upgrade repoints the default — see broker-ops.md.
			if clusterMode && natsConfPath == "" && !cmd.Flags().Changed("nats-conf-path") {
				natsConfPath = defaultNatsConfPath
			}
			// #27: default the C2 well-known manifest listener in cluster mode so a cluster-init'd broker
			// is discovery serve-ready WITHOUT an explicit manifest_listen (the `cluster init` seam
			// intentionally omits it — keying discovery on a config field the seam never writes left the
			// listener un-bound and curl connection-REFUSED). Loopback-only + Caddy-fronted. An operator
			// overrides the addr, or passes an explicit empty --cluster-manifest-listen to opt out.
			manifestListen = resolveManifestListen(manifestListen, clusterMode, cmd.Flags().Changed("cluster-manifest-listen"))

			// #75 review M6 / external review F1: --config-check validates the
			// FULLY-RESOLVED config and exits before any side effect. It runs the
			// SAME validators a real start does — strict decode + durations
			// (above), the log-level check (newLoggerTo below, with a discard
			// sink so no log file is created), the auth-seed read (below), and
			// broker.ValidateConfig (webhook URL / listen addresses). The point
			// (broker-ops §8.4): an operator can validate a strict broker.yaml
			// with the NEW binary during an upgrade WITHOUT that binary opening +
			// migrating the live tether.db. So storage.Open is SKIPPED in
			// config-check (as it is in cluster mode); the exit is after the pure
			// validators, before broker.New / listeners.
			var db *sql.DB

			// Upgrade allowlist precedence: explicit --upgrade-url-allow > yaml >
			// built-in default (upgradecli.BrokerUpgradeAllow documents why).
			allow := upgradecli.BrokerUpgradeAllow(cmd.Flags().Changed("upgrade-url-allow"), upgradeURLAllow, fileCfg.Broker.Upgrade.URLAllow)

			// h1 F: --log-file (or broker.yaml observability.log_file) points
			// slog at an in-process size-capped rotating file. Binary default
			// stays stderr — dev runs and the embedded-NATS test harness are
			// byte-unchanged; the DEPLOYED default lives in install.sh's
			// broker.yaml. Panics/stacktraces keep going to raw stderr, which
			// the unit routes to journald.
			logFile = pickFlagOrYaml(cmd, "log-file", logFile, fileCfg.Broker.Obs.LogFile)
			// F1: config-check validates the log LEVEL (newLoggerTo's job) but
			// must not create the log FILE — use a discard sink then. A real
			// serve resolves the capped rotating sink.
			logSink := io.Discard
			if !configCheck {
				logSink = resolveLogSink(logFile, fileCfg.Broker.Obs.LogMaxSizeMB, fileCfg.Broker.Obs.LogMaxBackups)
			}
			logger, err := newLoggerTo(logLevel, logJSON, logSink)
			if err != nil {
				return err
			}
			// origin: hot-update S8 plan §5.2 / §11.2 P19 — cold broker boot follows
			// its registered selector; the config path and data dir are its identity.
			// S8 FIX-DESCRIBE R4: a registered broker binds its journal (Config.Upgrade).
			// S8 BROKER-EXPORTER R-11: a restored image's S8 adjunct is verified here.
			plan, err := upgradecli.BrokerBoot(clusterDataDir, dbPath, configPath, brokerBoot != nil || agentboot.Current() != nil, configCheck, logger)
			upgradeDeps, depsErr := upgradecli.BrokerDependencies(plan, brokerBoot)
			if err = errors.Join(err, depsErr); err != nil {
				return upgradeExit("serve canonical boot", err)
			}

			cfg := broker.Config{
				TransferFactory:      broker.ServeTransferFactory,
				NATSURL:              natsURL,
				DB:                   db,
				Logger:               logger,
				PublicHost:           publicHost,
				TunnelControlAddr:    tunnelCtrlAddr,
				TunnelPublicHost:     tunnelPublicHost,
				StoreDir:             storeDir,
				AdminSocketPath:      adminSocket,
				PortBandLow:          bandLow,
				PortBandHigh:         bandHigh,
				UpgradeURLAllowlist:  allow,
				ProcRetention:        procRetention,
				ProcGCInterval:       procGCInterval,
				XferReapInterval:     xferReapInterval,
				XferCrossHomeReapAge: xferCrossHomeReapAge,
				SubHTTPAddr:          subHTTPListen,
				SubURLBase:           subURLBase(publicHost),
				ClusterDataDir:       clusterDataDir,
				ClusterRaftAddr:      clusterRaftAddr,
				ClusterSecretsDir:    clusterSecrets,
				ColocatedAgentNID:    colocatedAgentNID,
				DBPath:               dbPath,
				MetricsAddr:          metricsListen,
				ManifestAddr:         manifestListen,
				NatsConfPath:         natsConfPath,
				NatsServerBin:        natsServerBin,
				AlertWebhookURL:      alertWebhookURL,
				DiskCheckInterval:    diskCheckInterval,
				Upgrade:              upgradeDeps,
			}

			authSeedsSource := effectiveAuthSeedsDir(authSeedsDir, clusterMode, clusterSecrets)
			cfg.StableTunnelCertDir = authSeedsSource
			// auth_callout: enabled iff an explicit --auth-callout-seeds-dir is supplied
			// or cluster mode defaults it to broker.cluster.secrets_dir. The directory
			// must contain both private 0600 seed files: broker.nk and account.nk. The matching
			// nats-server.conf must list the broker pubkey under
			// `nkeys` + `authorization.auth_callout.auth_users`, and the
			// account pubkey under `authorization.auth_callout.issuer`.
			// (See architecture E.2 / K.3 for the full deployment shape.)
			if authSeedsSource != "" {
				ac, err := loadAuthCalloutSeeds(authSeedsSource)
				if err != nil {
					return fmt.Errorf("auth_callout seeds: %w", err)
				}
				cfg.AuthCallout = ac
			}

			// F1: config-check exits HERE — after every pure validator (strict
			// decode, durations, log level, auth seeds) and broker.ValidateConfig
			// (webhook URL / listen addresses, the same call broker.New makes),
			// before broker.New opens listeners / the DB was never opened. So a
			// config a real start rejects can no longer earn a "config OK".
			if configCheck {
				if err := broker.ValidateConfig(cfg); err != nil {
					return err
				}
				mode := "single"
				if clusterMode {
					mode = "cluster"
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "config OK: %s (mode=%s)\n", configPath, mode)
				return nil
			}

			// origin: hot-update S6 plan §1.2 / §2.1 / T03
			// Only a cold single launch opens here, after all pure validation.
			// Image failure must retain salvage, never migrate or cold-open.
			if err := broker.ValidateConfig(cfg); err != nil {
				return err
			}
			if runtime.GOOS == "linux" && dbPath != ":memory:" {
				cfg.ImageRun, err = broker.NewImageRun(os.Args, brokerBoot)
				if err != nil {
					return err
				}
			}
			var store brokerimage.StoreOwner
			if !clusterMode && brokerBoot == nil && dbPath == ":memory:" {
				// In-memory stores remain cold-capable and explicitly image-ineligible.
				// origin: hot-update S6 plan §3.1 / lead D3
				cfg.DB, err = storage.Open(dbPath)
				if err != nil {
					return err
				}
				defer func() { runErr = errors.Join(runErr, cfg.DB.Close()) }()
			} else if !clusterMode && brokerBoot == nil {
				if err := brokerimage.InstallColdSingleStoreSupport(); err != nil {
					return err
				}
				// Single mode has no independent DataDir flag. Let the store
				// owner derive it from the canonical DB path, including file:
				// URIs; filepath.Dir on the raw DSN would split the lock domain.
				store, db, err = brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{
					DBPath: dbPath, Registry: storage.DefaultDriverRegistry(),
				})
				// A partial opener's nonnil owner also needs terminal cleanup.
				defer func() {
					if store != nil {
						runErr = errors.Join(runErr, brokerimage.CloseStoreOwner(store))
					}
				}()
				if err != nil {
					return err
				}
				cfg.DB, cfg.SingleStore = db, store
			}
			b, err := broker.New(cfg)
			if err != nil {
				return err
			}
			// Run owns terminal cleanup from entry; InstallCurrent transfers it
			// to the current slot before any image replacement.
			// This defer is constructor-failure cleanup only, never a DB owner
			// captured across exec-error reopen or ACTIVE rollback.

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			authMode := "off (dev / P2-style)"
			if cfg.AuthCallout != nil {
				authMode = "on (seeds=" + authSeedsSource + ")"
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(),
				"tether serve: NATS=%s DB=%s auth_callout=%s\n(press Ctrl-C to quit)\n",
				natsURL, dbPath, authMode)

			store = nil // Run owns terminal cleanup from entry, including startup errors.
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
		"directory containing broker.nk + account.nk for auth_callout; empty = off in single mode, broker.cluster.secrets_dir in cluster mode")
	cmd.Flags().StringVar(&publicHost, "public-host", "localhost",
		"DNS name printed in expose URLs (operator-facing)")
	cmd.Flags().StringVar(&tunnelCtrlAddr, "tunnel-addr", "0.0.0.0:7000",
		"reverse-TCP tunnel control listener (host:port); empty disables P6 data plane")
	cmd.Flags().StringVar(&tunnelPublicHost, "tunnel-public-host", "0.0.0.0",
		"bind address for the public per-port tunnel listeners (default 0.0.0.0)")
	cmd.Flags().StringVar(&storeDir, "store-dir", "",
		"JetStream store dir to monitor for disk pressure (P7/H.4); empty = monitor disabled")
	cmd.Flags().StringVar(&subHTTPListen, "sub-http-listen", "",
		"loopback address for the P13 proxy subscription HTTP endpoint (e.g. 127.0.0.1:8090); empty disables it")
	cmd.Flags().StringVar(&adminSocket, "admin-socket", defaultAdminSocket,
		"local Unix socket for `tether admin *` (architecture I.2b); set to empty to disable")
	cmd.Flags().StringSliceVar(&upgradeURLAllow, "upgrade-url-allow", nil,
		"URL prefixes accepted by `tether node upgrade` (architecture J.4); empty = upgrades disabled")
	cmd.Flags().StringVar(&clusterDataDir, "cluster-data-dir", "",
		"D9 cluster: raft sub-tree parent (raft/ lives here); empty = single mode. Cluster mode requires `tether cluster init` to have created raft/ first")
	cmd.Flags().StringVar(&clusterRaftAddr, "cluster-raft-addr", "",
		"D9 cluster: raft transport bind host:port (private net only, e.g. 0.0.0.0:7400)")
	cmd.Flags().StringVar(&colocatedAgentNID, "colocated-agent-nid", "",
		"G5 #19: the nid of the tether agent co-located on this broker host (self-reported so `node ls --brokers` correlates broker+agent versions)")
	cmd.Flags().StringVar(&clusterSecrets, "cluster-secrets-dir", "",
		"D9 cluster: secrets dir (cluster-ca, route leaf, tunnel-cert, broker.nk, node-ident, account.nk); required in cluster mode")
	cmd.Flags().StringVar(&logFile, "log-file", "", "write logs to this size-capped rotating file instead of stderr ('-' = stderr; size/backups from broker.yaml observability.*, default 50MB x 2)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level: debug | info | warn | error (B5 OPS#8)")
	cmd.Flags().BoolVar(&logJSON, "log-json", false, "emit structured JSON logs instead of text (B5 OPS#8)")
	cmd.Flags().StringVar(&metricsListen, "metrics-listen", "", "address for the Prometheus /metrics + /healthz + /readyz HTTP endpoint (e.g. 127.0.0.1:9090); empty disables it (B5 OPS#1)")
	cmd.Flags().StringVar(&manifestListen, "cluster-manifest-listen", "", "LOOPBACK address for the well-known cluster discovery manifest /.well-known/tether/cluster.json, Caddy-fronted; bound only in cluster mode where it DEFAULTS to "+defaultManifestListen+" (#27); pass an explicit empty value to opt out (C2)")
	cmd.Flags().StringVar(&natsConfPath, "nats-conf-path", "", "live nats.conf the topology reconciler manages in cluster mode (default /etc/tether/nats.d/nats.conf; empty opts out) (C3)")
	cmd.Flags().StringVar(&natsServerBin, "nats-server-bin", "", "nats-server binary for the topology reconciler's `-t` dry-run + `--signal reload` (default: nats-server on PATH) (C3)")
	cmd.Flags().StringVar(&alertWebhookURL, "alert-webhook-url", "", "POST every committed alert raise/clear to this http/https endpoint (cluster mode; B6 OPS#2); empty disables it")
	cmd.Flags().DurationVar(&diskCheckInterval, "disk-check-interval", 0, "how often the disk-pressure monitor samples --store-dir (H.4/#39; e.g. 5m, 30s); 0 = built-in default (5m). Overrides broker.observability.disk_check_interval")
	cmd.Flags().BoolVar(&configCheck, "config-check", false, "validate --config (strict parse + all validators) and exit 0 before any side effect (no DB open, no migration, no listeners); nonzero on a bad config. Safe to run with a NEW binary during an upgrade")
	return cmd
}

// resolveManifestListen applies the #27 cluster-mode default for the C2 well-known discovery manifest
// listener. In cluster mode, an operator who left --cluster-manifest-listen unset (empty AND flag not
// changed — which is exactly what the `cluster init` broker.yaml seam leaves, since it never writes a
// manifest_listen key) gets the default loopback bind so discovery is serve-ready. A single-mode broker,
// an operator-supplied addr, or an explicit empty --cluster-manifest-listen (opt out) is left as-is.
func resolveManifestListen(cur string, clusterMode, flagChanged bool) string {
	if clusterMode && cur == "" && !flagChanged {
		return defaultManifestListen
	}
	return cur
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

func effectiveAuthSeedsDir(explicit string, clusterMode bool, clusterSecretsDir string) string {
	if explicit != "" {
		return explicit
	}
	if clusterMode {
		return clusterSecretsDir
	}
	return ""
}

// loadAuthCalloutSeeds reads broker.nk and account.nk (both private regular files
// containing nkey seeds) from dir. Both must exist.
func loadAuthCalloutSeeds(dir string) (*broker.AuthCalloutConfig, error) {
	brokerSeed, err := readPrivateSeedFile(dir, "broker.nk")
	if err != nil {
		return nil, err
	}
	accountSeed, err := readPrivateSeedFile(dir, "account.nk")
	if err != nil {
		return nil, err
	}
	// Batch-A A6: reject a wrong-KIND seed here, at startup.
	//
	// A user seed (SU…) parses fine through nkeys.FromSeed, so
	// broker.installAuthCallout accepted it and the broker came up healthy —
	// then signed every user JWT with a wrong-kind issuer and nats-server
	// rejected every client at auth_callout. The failure presented as "the
	// service is up but nobody can connect", with nothing in the broker log
	// pointing at account.nk.
	//
	// auth.LoadAccountSigner is exactly this check (it derives the public key
	// and requires the A prefix) and has existed since P1 — test/p1's risk test
	// even asserts the guard exists. It was simply never wired into the
	// production path. We discard the signer: this is a validation call only,
	// and the signing path is deliberately left byte-for-byte unchanged
	// (routing it through IssueUserJWT would drop uc.Audience, which
	// nats-server uses to place the connection in an account).
	if _, err := auth.LoadAccountSigner(accountSeed); err != nil {
		return nil, fmt.Errorf("%s/account.nk: %w — auth_callout needs an ACCOUNT seed (SA…); "+
			"a user seed parses but makes every client fail authentication with a wrong-kind issuer", dir, err)
	}
	return &broker.AuthCalloutConfig{
		BrokerNkeySeed: brokerSeed,
		AccountSeed:    accountSeed,
	}, nil
}

func readPrivateSeedFile(dir, name string) ([]byte, error) {
	path := filepath.Join(dir, name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s: seed file must be a regular file, not a symlink", name)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s: seed file must be a regular file", name)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s: permissions %04o too open; want 0600 or stricter", name, info.Mode().Perm())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return body, nil
}

func subURLBase(publicHost string) string {
	return legacy.BrokerSubURLBase(publicHost)
}

// The reader protocol is offline and shares serve's real pure validators. It
// never installs store adapters or reaches broker.New; config-check uses a
// discard logger and performs only configuration/auth seed reads.
// origin: hot-update S6 plan §3.1 / T06 / §12.1 P22
func newBrokerImagePreflightCmd() *cobra.Command {
	return &cobra.Command{
		Use: brokerimage.PreflightProbeCommand, Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			request, err := brokerimage.DecodeProbeRequest(cmd.InOrStdin())
			if err != nil {
				return err
			}
			supported, err := storage.SupportedMigrations()
			if err != nil {
				return brokerimage.ErrImage
			}
			reply, err := brokerimage.ReaderProbe(request, supported, validateBrokerProbeConfig)
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(reply)
		},
	}
}

func validateBrokerProbeConfig(config []byte, args []string) error {
	if len(args) < 2 || args[1] != "serve" {
		return brokerimage.ErrImage
	}
	bytesOnDisk, err := brokerimage.BootConfigBytes(args)
	if err != nil || !bytes.Equal(bytesOnDisk, config) {
		return brokerimage.ErrImage
	}
	serve := newServeCmd()
	serve.SetOut(io.Discard)
	serve.SetErr(io.Discard)
	serve.SetArgs(append(append([]string(nil), args[2:]...), "--config-check=true"))
	return serve.Execute()
}
