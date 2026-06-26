package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// agent_join.go (C2) — `tether agent join <invite> [--start]`. Ingests an OOB invite (account_pub
// pin + bootstrap URL + sid + optional inline seed), writes agent.yaml, pre-warms the discovery
// cache, prints the pinned account_pub LOUDLY, and optionally starts the daemon in-process (PIN in
// memory, never in argv). It FAILS LOUD and persists NOTHING when it cannot derive a dialable client
// URL (no inline seed= and no published seed endpoints).

func newAgentJoinCmd() *cobra.Command {
	var nid, pin, expectPub string
	var start bool
	cmd := &cobra.Command{
		Use:   "join <invite>",
		Short: "Join a cluster from an OOB invite (writes agent.yaml + pins the account); --start runs the daemon",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inv, err := clusterroster.ParseInvite(args[0])
			if err != nil {
				return err
			}
			if expectPub != "" && expectPub != inv.Pin {
				return fmt.Errorf("agent join: --expect-account-pub %q does not match the invite's pin %q (refusing)", expectPub, inv.Pin)
			}
			if nid == "" {
				return fmt.Errorf("agent join: --nid is required (the per-machine node id)")
			}
			home := cli.DefaultHome()

			// Best-effort fetch (also derives endpoints when the invite has no inline seed).
			fetchCtx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			m, fetchErr := clusterroster.FetchManifest(fetchCtx, inv.BootstrapURL, 0)
			cancel()
			now := time.Now()
			var seedEndpoints []string
			if fetchErr == nil && m.Seeds != nil {
				if verr := clusterroster.VerifySeedsAt(m.Seeds, inv.Pin, now); verr == nil {
					seedEndpoints = clusterroster.SeedDialURLs(m.Seeds)
				}
			}
			brokerURL := inv.Seed
			if brokerURL == "" {
				brokerURL = strings.Join(seedEndpoints, ",")
			}
			if brokerURL == "" {
				hint := ""
				if fetchErr != nil {
					hint = " (manifest fetch failed: " + fetchErr.Error() + ")"
				}
				return fmt.Errorf("agent join: no dialable client URL — the invite carries no inline seed= and no published seed endpoints were retrievable from %s%s; run `tether cluster seeds publish` first, then re-mint the invite", inv.BootstrapURL, hint)
			}

			if err := writeAgentConfig(home, inv.SID, nid, brokerURL, inv.Pin, inv.BootstrapURL); err != nil {
				return fmt.Errorf("agent join: write config: %w", err)
			}
			// Pre-warm the discovery cache through the SINGLE adopt authority (best-effort).
			if fetchErr == nil {
				next, ok := agent.AdoptDecision(agent.RosterState{Pin: inv.Pin}, m.Roster, m.Seeds, brokerURL, now)
				if ok {
					_ = agent.WriteRosterCache(home, inv.SID, &agent.RosterCache{
						PinAccountPub: next.Pin, Generation: next.RosterGen, Roster: next.Roster,
						SeedGen: next.SeedGen, Seeds: next.Seeds,
					})
				}
			}

			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "joined session %q as nid %q\n", inv.SID, nid)
			_, _ = fmt.Fprintf(out, "PINNED cluster account: %s\n", inv.Pin)
			_, _ = fmt.Fprintf(out, "  (every roster/seed update is verified against this key — confirm it is the cluster's real account pubkey)\n")
			if fetchErr != nil {
				_, _ = fmt.Fprintf(out, "note: manifest pre-warm skipped (%v) — the agent will learn the roster on its first NATS connect\n", fetchErr)
			}
			if !start {
				_, _ = fmt.Fprintf(out, "\nstart the agent now with:\n  tether agent --session %s --nid %s", inv.SID, nid)
				if pin != "" {
					_, _ = fmt.Fprintf(out, " --pin <pin>")
				}
				_, _ = fmt.Fprintf(out, "\nor enable the user service:\n  tether agent --session %s --nid %s --install-user-service && systemctl --user enable --now tether-agent@%s\n", inv.SID, nid, inv.SID)
				return nil
			}
			return runJoinedAgent(cmd, home, inv.SID, nid, brokerURL, pin)
		},
	}
	cmd.Flags().StringVar(&nid, "nid", "", "this machine's node id (required)")
	cmd.Flags().StringVar(&pin, "pin", "", "session PIN, in memory only (required only on first connect; never persisted)")
	cmd.Flags().StringVar(&expectPub, "expect-account-pub", "", "hard-fail unless the invite pins this exact account public key (defense-in-depth)")
	cmd.Flags().BoolVar(&start, "start", false, "after joining, run the agent daemon in-process (foreground; PIN kept in memory, never in argv)")
	return cmd
}

// writeAgentConfig rewrites agent.yaml from the typed struct (preserving existing file_transfer /
// proxy / remote_fs / tunnel_addr blocks by reading them first), setting the C2 join fields. Comments
// are NOT preserved (typed round-trip). Atomic write, 0600 (parity with install.sh).
func writeAgentConfig(home, sid, nid, brokerURL, accountPub, bootstrapURL string) error {
	if err := proto.ValidateSID(sid); err != nil {
		return err
	}
	ay, err := loadAgentYAML(home, sid) // preserve any existing blocks
	if err != nil {
		return err
	}
	ay.BrokerURL = brokerURL
	ay.AccountPub = accountPub
	ay.BootstrapURL = bootstrapURL
	ay.Session = sid
	ay.NID = nid
	body, err := yaml.Marshal(&ay)
	if err != nil {
		return fmt.Errorf("marshal agent.yaml: %w", err)
	}
	dir := filepath.Join(home, "agent", sid)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "agent.yaml")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// runJoinedAgent runs the daemon in-process after a join (minimal bring-up; PIN in memory, never
// argv). Mirrors the daemon RunE's post-resolution path (identity → adapter → New → Run).
func runJoinedAgent(cmd *cobra.Command, home, sid, nid, brokerURL, pin string) error {
	logger, err := newLogger("info", false)
	if err != nil {
		return err
	}
	ay, _ := loadAgentYAML(home, sid) // just-written config: account_pub/bootstrap pin the cache + cold-start
	cfg := agent.Config{
		NATSURL:      brokerURL,
		SID:          sid,
		NID:          nid,
		PIN:          pin,
		Home:         home,
		Logger:       logger,
		AccountPub:   ay.AccountPub,
		BootstrapURL: ay.BootstrapURL,
	}
	if os.Getenv(cli.DevNoAuthEnv) == "" {
		id, ierr := cli.EnsureAgentIdentity(home, sid)
		if ierr != nil {
			return fmt.Errorf("agent identity: %w", ierr)
		}
		cfg.Identity = id
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if ay.TunnelAddr != "" {
		adapter := agent.NewTunnelExposeAdapter(ay.TunnelAddr, sid, nid, logger)
		adapter.Start(ctx)
		cfg.ExposeAdapter = adapter
	}
	ag, err := agent.New(cfg)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "tether agent: NATS=%s sid=%s nid=%s (press Ctrl-C to quit)\n", brokerURL, sid, nid)
	if err := ag.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
