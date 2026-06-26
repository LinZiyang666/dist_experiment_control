package main

import (
	"context"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/spf13/cobra"
)

// agent_config.go (C2) — `tether agent config refresh --once`: pull a fresh signed roster/seed bundle
// from the well-known manifest and update the discovery cache, WITHOUT a running daemon (e.g. a cron
// job). It verifies against the ALREADY-PERSISTED pin and REFUSES to create a first-ever pin from
// HTTP (that is only ever established by `agent join` with an OOB account_pub).

func newAgentConfigCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Agent config helpers (refresh the discovery cache)"}
	cmd.AddCommand(newAgentConfigRefreshCmd())
	return cmd
}

func newAgentConfigRefreshCmd() *cobra.Command {
	var sid string
	var once bool
	cmd := &cobra.Command{
		Use:   "refresh --once",
		Short: "Fetch a fresh signed roster/seed bundle from the well-known manifest and update the cache (no daemon)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !once {
				return fmt.Errorf("agent config refresh: pass --once (the only supported mode)")
			}
			if sid == "" {
				return fmt.Errorf("agent config refresh: --session is required")
			}
			home := cli.DefaultHome()
			ay, err := loadAgentYAML(home, sid)
			if err != nil {
				return err
			}
			if ay.BootstrapURL == "" {
				return fmt.Errorf("agent config refresh: no bootstrap_url in agent.yaml (run `tether agent join`)")
			}
			rc, _ := agent.ReadRosterCache(home, sid)
			pin := ay.AccountPub
			if pin == "" && rc != nil {
				pin = rc.PinAccountPub
			}
			if pin == "" {
				return fmt.Errorf("agent config refresh: no pinned account — refusing to TOFU-pin from HTTP; run `tether agent join <invite>` first")
			}
			fctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			m, ferr := clusterroster.FetchManifest(fctx, ay.BootstrapURL, 0)
			cancel()
			if ferr != nil {
				return fmt.Errorf("agent config refresh: fetch: %w", ferr)
			}
			prev := agent.RosterState{Pin: pin}
			if rc != nil {
				prev = agent.RosterState{Pin: pin, RosterGen: rc.Generation, SeedGen: rc.SeedGen, Roster: rc.Roster, Seeds: rc.Seeds}
			}
			next, ok := agent.AdoptDecision(prev, m.Roster, m.Seeds, ay.BrokerURL, time.Now())
			if !ok {
				return fmt.Errorf("agent config refresh: manifest rejected (sig/account/schema/expiry/generation) — cache unchanged")
			}
			if err := agent.WriteRosterCache(home, sid, &agent.RosterCache{
				PinAccountPub: next.Pin, Generation: next.RosterGen, Roster: next.Roster,
				SeedGen: next.SeedGen, Seeds: next.Seeds,
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "roster refreshed (roster_gen=%d seed_gen=%d)\n", next.RosterGen, next.SeedGen)
			return nil
		},
	}
	cmd.Flags().StringVar(&sid, "session", "", "session id (required)")
	cmd.Flags().BoolVar(&once, "once", false, "perform a single refresh and exit")
	return cmd
}
