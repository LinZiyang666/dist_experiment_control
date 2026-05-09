package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
)

// agentYAML mirrors the architecture K.1 agent.yaml shape written by
// `install.sh --role agent`. Field names match install.sh exactly so
// new operators can hand-edit the file without surprises.
type agentYAML struct {
	BrokerURL  string `yaml:"broker_url"`
	Session    string `yaml:"session"`
	NID        string `yaml:"nid"`
	TunnelAddr string `yaml:"tunnel_addr"`
}

// loadAgentYAML reads ~/.tether/agent/<sid>/agent.yaml when present.
// Missing file → returns zero-value config without error so callers
// can treat "no install" the same as "no overrides". A malformed
// file IS reported so the operator notices a typo instead of
// silently falling back to flag defaults.
func loadAgentYAML(home, sid string) (agentYAML, error) {
	path := filepath.Join(home, "agent", sid, "agent.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return agentYAML{}, nil
		}
		return agentYAML{}, fmt.Errorf("read %s: %w", path, err)
	}
	var ay agentYAML
	if err := yaml.Unmarshal(body, &ay); err != nil {
		return agentYAML{}, fmt.Errorf("parse %s: %w", path, err)
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
		installUserService bool
		uninstall          bool
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
	cmd.Flags().StringVar(&sid, "session", "", "session id (required for run + install)")
	cmd.Flags().StringVar(&nid, "nid", "", "node id (required for run + install)")
	cmd.Flags().StringVar(&pin, "pin", "", "session PIN, required only on first connect (binds (sid,nid) to this agent's nkey)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "127.0.0.1:7000",
		"broker reverse-TCP tunnel control address (host:port); empty to disable data plane")
	cmd.Flags().BoolVar(&installUserService, "install-user-service", false,
		"write ~/.config/systemd/user/tether-agent@<sid>.service and exit (does NOT start)")
	cmd.Flags().BoolVar(&uninstall, "uninstall", false,
		"remove the user systemd unit for this session and exit (does NOT stop running agents)")
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
// single quotes via the standard '\'' trick. Suitable for systemd
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
