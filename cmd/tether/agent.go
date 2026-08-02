package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
)

// agentYAML mirrors the architecture K.1 agent.yaml shape written by
// `install.sh --role agent`. Field names match install.sh exactly so
// new operators can hand-edit the file without surprises.
type agentYAML struct {
	BrokerURL string `yaml:"broker_url"`
	Session   string `yaml:"session"`
	NID       string `yaml:"nid"`
	// AccountPub (C2) is the OOB-pinned cluster account public key (written by `agent join`). When
	// set it disables roster TOFU and is enforced against every roster/seed bundle. BootstrapURL is
	// the well-known HTTPS manifest URL for cold-start discovery. Both additive; a pre-C2 agent.yaml
	// (no keys) still round-trips through KnownFields(true) → "" → C1 TOFU behavior.
	AccountPub   string             `yaml:"account_pub,omitempty"`
	BootstrapURL string             `yaml:"bootstrap_url,omitempty"`
	TunnelAddr   string             `yaml:"tunnel_addr"`
	FileTransfer fileTransferConfig `yaml:"file_transfer"`
	Proxy        proxyConfig        `yaml:"proxy"`
	RemoteFS     remoteFSConfig     `yaml:"remote_fs"`
	// Upgrade (#28) is the agent's own upgrade policy. The agent has ALWAYS re-checked the download
	// URL against a local allowlist (architecture J.4 § 安全约束 — belt and braces behind the broker's
	// gate), but until now that allowlist was a hardcoded constant with no operator input: an operator
	// self-hosting release artifacts could open the broker's allowlist and still be refused
	// url_not_allowed_local by every agent, with nothing to change. Both the error hint and the manual
	// pointed at "the agent's --upgrade-url-allow flag", which did not exist.
	Upgrade agentUpgradeConfig `yaml:"upgrade,omitempty"`
}

// agentUpgradeConfig is the agent.yaml `upgrade:` block. It mirrors the broker's
// `broker.upgrade.url_allow` (cmd/tether/serve.go) so the two roles are configured the same way.
type agentUpgradeConfig struct {
	// URLAllow narrows/replaces the built-in agent allowlist. Absent/empty ⇒ the agent keeps its
	// built-in default (this project's GitHub releases prefix), preserving the pre-#28 behaviour
	// exactly for every existing install.
	URLAllow []string `yaml:"url_allow,omitempty"`
}

// remoteFSConfig controls hung-network-filesystem-safe spawn for exec/run
// (docs/reviews/remote-fs-resilience-plan.md). Absent block ⇒ mode "auto",
// which is inert on machines with no network mounts.
type remoteFSConfig struct {
	Mode         string `yaml:"mode"`          // "auto" (default) | "off"
	SafeDir      string `yaml:"safe_dir"`      // optional local substitute cwd during an outage
	ProbeTimeout string `yaml:"probe_timeout"` // bounded mount-liveness probe deadline, e.g. "2s" (empty ⇒ default)
	SpawnTimeout string `yaml:"spawn_timeout"` // execve start-window deadline, e.g. "30s" (empty ⇒ default)
	WedgeCeiling int    `yaml:"wedge_ceiling"` // max concurrent abandoned spawns (0 ⇒ default)
}

// parseOptDuration parses an optional Go duration string (e.g. "2s"). Empty ⇒ 0
// (the agent then applies its built-in default). A malformed value fails loud so
// a typo doesn't silently fall back to the default.
func parseOptDuration(s, field string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q: %w", field, s, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s: must not be negative: %q", field, s)
	}
	return d, nil
}

// resolveAgentUpgradeAllow applies #28's precedence for the agent's local upgrade-URL allowlist:
//
//	explicit --upgrade-url-allow  >  agent.yaml `upgrade.url_allow`  >  the agent's built-in default
//
// This is the same shape `tether serve` already uses for the broker's copy of the same setting. An empty
// result is NOT "deny everything": internal/agent falls back to defaultAgentURLAllowlist, so every
// pre-#28 install keeps its exact behaviour.
//
// Entries are validated, because the failure mode of a typo here is silent and confusing: urlAllowed is
// a plain strings.HasPrefix test, so "githib.com/…" or a bare hostname simply never matches, and the
// operator sees url_not_allowed_local for a URL they believe they allowed. Fail at startup instead.
func resolveAgentUpgradeAllow(cmd *cobra.Command, flagVal, yamlVal []string) ([]string, error) {
	allow := flagVal
	if !cmd.Flags().Changed("upgrade-url-allow") {
		allow = yamlVal
	}
	for _, p := range allow {
		if !strings.HasPrefix(p, "https://") && !strings.HasPrefix(p, "http://") {
			return nil, fmt.Errorf("upgrade URL allowlist entry %q is not a URL prefix — entries must begin with "+
				"https:// (or http://). The allowlist is matched by prefix, so a non-URL entry can never match a "+
				"download URL: it would silently DISABLE upgrades rather than permit them. "+
				"Set it via --upgrade-url-allow or the `upgrade.url_allow` list in agent.yaml", p)
		}
	}
	return allow, nil
}

// agentDaemonInputs is everything the daemon's config fold needs that is not on the cobra command.
type agentDaemonInputs struct {
	Home, SID, NID, PIN, NATSURL string
	YAML                         agentYAML
	UpgradeURLAllowFlag          []string
	Logger                       *slog.Logger
}

// agentDaemonConfig folds flags + agent.yaml into the daemon's agent.Config. Every value-resolution rule
// for `tether agent` lives here so it can be asserted directly; RunE does IO and lifecycle only.
//
// It runs BEFORE any identity/NATS work, so a malformed setting is a startup error the operator sees at
// once rather than a mysterious refusal much later (e.g. url_not_allowed_local at upgrade time).
func agentDaemonConfig(cmd *cobra.Command, in agentDaemonInputs) (agent.Config, error) {
	probeTO, err := parseOptDuration(in.YAML.RemoteFS.ProbeTimeout, "remote_fs.probe_timeout")
	if err != nil {
		return agent.Config{}, err
	}
	spawnTO, err := parseOptDuration(in.YAML.RemoteFS.SpawnTimeout, "remote_fs.spawn_timeout")
	if err != nil {
		return agent.Config{}, err
	}
	upgradeAllow, err := resolveAgentUpgradeAllow(cmd, in.UpgradeURLAllowFlag, in.YAML.Upgrade.URLAllow)
	if err != nil {
		return agent.Config{}, usageErr("%v", err)
	}
	return agent.Config{
		NATSURL:      in.NATSURL,
		SID:          in.SID,
		NID:          in.NID,
		PIN:          in.PIN,
		Home:         in.Home,
		Logger:       in.Logger,
		AccountPub:   in.YAML.AccountPub,   // C2: OOB account-pub pin (disables TOFU)
		BootstrapURL: in.YAML.BootstrapURL, // C2: cold-start manifest URL

		AllowRoots:                    in.YAML.FileTransfer.AllowRoots,
		RootsConfigured:               in.YAML.FileTransfer.AllowRoots != nil,
		ProxyAllowPrivateDestinations: in.YAML.Proxy.AllowPrivateDestinations,
		RemoteFSMode:                  in.YAML.RemoteFS.Mode,
		RemoteFSSafeDir:               in.YAML.RemoteFS.SafeDir,
		RemoteFSProbeTimeout:          probeTO,
		RemoteFSSpawnTimeout:          spawnTO,
		RemoteFSWedgeCeiling:          in.YAML.RemoteFS.WedgeCeiling,
		UpgradeURLAllowlist:           upgradeAllow, // #28
	}, nil
}

// proxyConfig is the agent-side P13 proxy block (round-7 F5). The documented
// allow_private_destinations opt-in must actually reach agent.Config.
type proxyConfig struct {
	AllowPrivateDestinations bool `yaml:"allow_private_destinations"`
}

// fileTransferConfig controls `tether push` / `tether pull` containment.
// AllowRoots is an OPTIONAL narrowing, resolved into a transferMode by the
// agent (internal/agent/transfer.go resolveTransferMode):
//
//   - key absent/null → OPEN: whole-FS reach, equal to run/exec (default).
//   - non-empty list  → NARROW: confined to those absolute roots.
//   - explicit `[]`   → DISABLED: every push/pull → transfer_disabled.
//
// yaml.v3 leaves AllowRoots nil when the key is absent and a non-nil len-0
// slice for `allow_roots: []`, which is exactly the open-vs-disabled
// discriminator (carried to the agent as Config.RootsConfigured).
type fileTransferConfig struct {
	AllowRoots []string `yaml:"allow_roots"`
}

// loadAgentYAML reads ~/.tether/agent/<sid>/agent.yaml when present.
// Missing file → returns zero-value config without error so callers
// can treat "no install" the same as "no overrides". A malformed
// file IS reported so the operator notices a typo instead of
// silently falling back to flag defaults.
//
// sid is validated via proto.ValidateSID before being concatenated
// into the path. Without this, a malicious sid like "../../../etc/passwd"
// would let filepath.Clean escape <home> and read arbitrary files —
// caught by test/security TestAgentYAMLPathTraversalContract +
// TestLoadAgentYAMLRejectsTraversalSID below. Defense in depth: the
// CLI flag parsing and broker-side bootstrap also validate, but a
// path-handling helper that touches the filesystem must not trust
// its caller.
func loadAgentYAML(home, sid string) (agentYAML, error) {
	if err := proto.ValidateSID(sid); err != nil {
		return agentYAML{}, fmt.Errorf("loadAgentYAML: %w", err)
	}
	path := filepath.Join(home, "agent", sid, "agent.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return agentYAML{}, nil
		}
		return agentYAML{}, fmt.Errorf("read %s: %w", path, err)
	}
	var ay agentYAML
	dec := yaml.NewDecoder(bytes.NewReader(body))
	dec.KnownFields(true)
	if err := dec.Decode(&ay); err != nil {
		// An empty / comment-only / whitespace-only file has no document;
		// tolerate it (zero struct → caller falls through to CLI flags),
		// matching the historical yaml.Unmarshal behavior. KnownFields
		// strictness still applies to any real document.
		if errors.Is(err, io.EOF) {
			return agentYAML{}, nil
		}
		return agentYAML{}, fmt.Errorf("parse %s: %w", path, err)
	}
	// Reject any SECOND-or-later NON-EMPTY document: it could hide a
	// narrowing/disable the first does not show. Benign empty trailing
	// documents (e.g. a lone `---`) decode to nil and are tolerated. The
	// scan MUST loop to io.EOF: an empty *middle* document must not let a
	// later non-empty document slip through unchecked (F11 fail-open).
	for {
		var extra any
		err := dec.Decode(&extra)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return agentYAML{}, fmt.Errorf("parse %s: %w", path, err)
		}
		if extra != nil {
			return agentYAML{}, fmt.Errorf("parse %s: multiple YAML documents are not supported", path)
		}
		// extra == nil: an empty document — keep scanning for a hidden one.
	}
	return ay, nil
}

func newAgentCmd() *cobra.Command {
	var (
		natsURL            string
		sid                string
		nid                string
		pin                string
		tunnelAddr         string
		upgradeURLAllow    []string
		installUserService bool
		uninstall          bool
		logLevel           string
		logJSON            bool
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run agent daemon (register + heartbeat + expose tunnel client)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Architecture K.1: install-user-service writes a
			// systemd template unit and exits without starting
			// anything. Runs first so the rest of the agent setup
			// (identity, tunnel adapter, etc.) is skipped — install
			// callers don't have a broker to talk to yet.
			if installUserService {
				return runInstallUserService(cmd, sid, nid, natsURL, tunnelAddr)
			}
			if uninstall {
				return runUninstallUserService(cmd, sid)
			}

			home := cli.DefaultHome()
			// agent.yaml fills in the install-time values so the
			// systemd unit (and the K.1 manual `setsid nohup`
			// command) can stay short — `tether agent --session
			// <sid> --nid <nid>` Just Works after install.sh
			// dropped the yaml. Precedence: explicit flag > yaml >
			// cobra default. We need sid resolved first because
			// the yaml is keyed by it.
			if sid == "" {
				return fmt.Errorf("--session is required to run the agent")
			}
			ay, err := loadAgentYAML(home, sid)
			if err != nil {
				return err
			}
			natsURL = pickFlagOrYaml(cmd, "nats-url", natsURL, ay.BrokerURL)
			tunnelAddr = pickFlagOrYaml(cmd, "tunnel-addr", tunnelAddr, ay.TunnelAddr)
			if nid == "" {
				nid = ay.NID
			}
			if nid == "" {
				return fmt.Errorf("--nid is required (set on CLI or in agent.yaml)")
			}

			logger, err := newLogger(logLevel, logJSON)
			if err != nil {
				return err
			}
			// The boot half of the upgrade state machine runs in main(),
			// BEFORE Cobra parsing — see isAgentDaemonInvocation (external
			// review F2: running it here, after flag/YAML/logger steps, left
			// the boot budget unconsumed exactly when the staged binary
			// broke on those steps). Deliberately NOT called again here — a
			// second call would double-count the boot.

			// The FLAGS+YAML → agent.Config fold lives in one testable function (see #28's wiring
			// tests): assembling it inline here made it impossible to assert that a resolved setting
			// actually reaches the daemon, which is precisely how a flag ends up parsed-then-discarded.
			cfg, err := agentDaemonConfig(cmd, agentDaemonInputs{
				Home: home, SID: sid, NID: nid, PIN: pin, NATSURL: natsURL,
				YAML: ay, UpgradeURLAllowFlag: upgradeURLAllow, Logger: logger,
			})
			if err != nil {
				return err
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
	cmd.Flags().StringVar(&sid, "session", "", "session id (required for run + install)")
	cmd.Flags().StringVar(&nid, "nid", "", "node id (required for run + install)")
	cmd.Flags().StringVar(&pin, "pin", "", "session PIN, required only on first connect (binds (sid,nid) to this agent's nkey)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "127.0.0.1:7000",
		"broker reverse-TCP tunnel control address (host:port); empty to disable data plane")
	cmd.Flags().StringSliceVar(&upgradeURLAllow, "upgrade-url-allow", nil,
		"URL prefixes this agent will accept for `tether node upgrade` downloads (architecture J.4; the agent re-checks "+
			"independently of the broker). Overrides `upgrade.url_allow` in agent.yaml; unset on both = the built-in "+
			"tether releases prefix")
	cmd.Flags().BoolVar(&installUserService, "install-user-service", false,
		"write ~/.config/systemd/user/tether-agent@<sid>.service and exit (does NOT start)")
	cmd.Flags().BoolVar(&uninstall, "uninstall", false,
		"remove the user systemd unit for this session and exit (does NOT stop running agents)")
	cmd.Flags().StringVar(&logLevel, "log-level", "info", "log level: debug | info | warn | error (B5 OPS#8)")
	cmd.Flags().BoolVar(&logJSON, "log-json", false, "emit structured JSON logs instead of text (B5 OPS#8)")
	// C2: agent join / config refresh / doctor (the daemon RunE still runs when no subcommand is given).
	cmd.AddCommand(newAgentJoinCmd(), newAgentConfigCmd(), newAgentDoctorCmd())
	return cmd
}

// runInstallUserService writes the architecture K.1 systemd --user
// template unit at ~/.config/systemd/user/tether-agent@<sid>.service.
// Per K.0 §2 install ≠ start: this writes the file, prints next-step
// commands, and exits. The caller does `systemctl --user enable
// --now tether-agent@<sid>` separately.
func runInstallUserService(cmd *cobra.Command, sid, nid, natsURL, tunnelAddr string) error {
	if sid == "" || nid == "" {
		return fmt.Errorf("--session and --nid are required for --install-user-service")
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("agent install: cannot resolve own path: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("agent install: $HOME: %w", err)
	}
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return fmt.Errorf("agent install: mkdir %s: %w", unitDir, err)
	}
	unitPath := filepath.Join(unitDir, fmt.Sprintf("tether-agent@%s.service", sid))
	// Audit shard 04 F2: when the operator typed explicit flags
	// (`--nats-url`, `--tunnel-addr`) on the install line, embed
	// them in the unit body so the systemd-launched agent uses
	// them too. Defaults are dropped (agent.yaml fills those in).
	// $TETHER_HOME passes through via Environment= for the same
	// reason. PIN is intentionally NOT persisted (architecture K.1).
	extraArgs := ""
	if cmd.Flags().Changed("nats-url") {
		extraArgs += " --nats-url " + shellQuote(natsURL)
	}
	if cmd.Flags().Changed("tunnel-addr") {
		extraArgs += " --tunnel-addr " + shellQuote(tunnelAddr)
	}
	envLines := ""
	if v := os.Getenv("TETHER_HOME"); v != "" {
		envLines = "Environment=TETHER_HOME=" + v + "\n"
	}
	body := agentUserUnitBody(exe, sid, nid, extraArgs, envLines)
	if err := os.WriteFile(unitPath, []byte(body), 0o644); err != nil {
		return fmt.Errorf("agent install: write unit: %w", err)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "✔ wrote %s\n", unitPath)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Next:\n")
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "    systemctl --user daemon-reload\n")
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "    systemctl --user enable --now tether-agent@%s.service\n", sid)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "    loginctl enable-linger %s   # keep agent up across logouts\n", os.Getenv("USER"))
	return nil
}

func runUninstallUserService(cmd *cobra.Command, sid string) error {
	if sid == "" {
		return fmt.Errorf("--session is required for --uninstall")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("agent uninstall: $HOME: %w", err)
	}
	unitPath := filepath.Join(home, ".config", "systemd", "user",
		fmt.Sprintf("tether-agent@%s.service", sid))
	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("agent uninstall: remove unit: %w", err)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "✔ removed %s\n", unitPath)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Note: any running tether agent processes are NOT stopped automatically.\n")
	return nil
}

// agentUserUnitBody returns the rendered systemd template unit body.
// Used by runInstallUserService and exposed for the install-time
// test. exe is the absolute path of the running tether binary so a
// caller invoking from a non-PATH location still gets a working
// unit; sid + nid pin the daemon's identity. extraArgs is appended
// to ExecStart verbatim (already shell-quoted by the caller);
// envLines is zero or more `Environment=KEY=val\n` lines injected
// before [Install].
func agentUserUnitBody(exe, sid, nid, extraArgs, envLines string) string {
	return fmt.Sprintf(`[Unit]
Description=tether agent (session=%s nid=%s)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%s agent --session %s --nid %s%s
Restart=on-failure
RestartSec=5
StandardOutput=append:%%h/.tether/agent/%s/agent.log
StandardError=append:%%h/.tether/agent/%s/agent.log
%s
[Install]
WantedBy=default.target
`, sid, nid, exe, sid, nid, extraArgs, sid, sid, envLines)
}

// shellQuote wraps a value in single quotes, escaping embedded
// single quotes via the standard '\” trick. Suitable for systemd
// ExecStart= values which use shell-style argument splitting.
func shellQuote(v string) string {
	if v == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(v, "'", `'\''`) + "'"
}

func displayOrOff(s string) string {
	if s == "" {
		return "off"
	}
	return s
}
