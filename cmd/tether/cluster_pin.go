package main

import (
	"fmt"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/spf13/cobra"
)

// cluster_pin.go — client-side cluster discovery: `cluster pin` writes the OOB-trusted account_pub + failover
// endpoints into the ctl's discovery cache (enabling broker auto-failover on the persisted broker_url path),
// and `cluster invite` mints the OOB discovery token an operator hands to a teammate. Both are laptop/ctl
// commands (no admin socket); the pin is the ONLY trust-establishment path (never HTTP/NATS-TOFU).

func shortPub(p string) string {
	if len(p) <= 14 {
		return p
	}
	return p[:14] + "..."
}

func newClusterPinCmd() *cobra.Command {
	var home string
	var force bool
	cmd := &cobra.Command{
		Use:     "pin <invite>",
		GroupID: "client",
		Short:   "Pin a cluster from an out-of-band discovery invite (enables ctl broker auto-failover)",
		Example: "  tether login --broker wss://broker1:443 -s lab     # set your primary first\n" +
			"  tether cluster pin 'tether-invite:v1?pin=AB...&seed=wss://broker2:443'   # then learn the survivor",
		Long: `Record a cluster's account_pub (trust anchor) + failover endpoints from an OOB-delivered
discovery invite, so this ctl fails over to a surviving broker when its configured broker_url is down.
The invite is OOB-trusted (paste it from a broker operator / ` + "`tether cluster invite`" + `); it is the
ONLY way the pin is set (never trust-on-first-use over HTTP/NATS). First-write-wins: re-pinning a DIFFERENT
account_pub requires --force (a cluster switch, which resets the cache). Run ` + "`tether login --broker`" + `
first so failover knows your primary broker_url.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inv, err := clusterroster.ParseDiscoveryInvite(args[0])
			if err != nil {
				return fmt.Errorf("cluster pin: %w", err)
			}
			base, src := cli.ResolveBrokerURLSource("", false, home)
			if src != cli.SourceFile {
				return unavailErr("cluster pin: no persisted broker_url; run `tether login --broker <url>` first so failover knows your primary broker")
			}
			ce, _ := cli.ReadClusterEndpoints(home)
			if ce != nil && ce.PinAccountPub != "" && ce.PinAccountPub != inv.Pin && !force {
				return unavailErr("cluster pin: already pinned to a different cluster account (%s); pass --force to switch clusters (resets the cache)", shortPub(ce.PinAccountPub))
			}
			if ce == nil || ce.PinAccountPub != inv.Pin { // first pin or --force cluster switch → fresh cache
				ce = &cli.ClusterEndpoints{}
			}
			ce.PinAccountPub = inv.Pin
			ce.FloorURL = base
			ce.BootstrapURL = inv.BootstrapURL
			if inv.Seed != "" {
				seen := false
				for _, s := range ce.InviteSeeds {
					if s == inv.Seed {
						seen = true
						break
					}
				}
				if !seen {
					ce.InviteSeeds = append(ce.InviteSeeds, inv.Seed)
				}
			}
			if err := cli.WriteClusterEndpoints(home, ce); err != nil {
				return fmt.Errorf("cluster pin: %w", err)
			}
			// Eager best-effort warm so a bootstrap-only invite (no inline seed) is useful immediately
			// instead of only after the next File-source connect. No-op without a bootstrap URL. Pin is OOB
			// (no broker conn yet) → nil nc + empty actor → the NATS roster-pull is skipped, HTTP bootstrap
			// serves the warm (G3 #17).
			if ce.BootstrapURL != "" {
				refreshCtlEndpoints(cmd.Context(), nil, home, base, "")
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "pinned cluster %s\n", shortPub(inv.Pin))
			_, _ = fmt.Fprintf(out, "  failover floor: %s\n", base)
			if len(ce.InviteSeeds) > 0 {
				_, _ = fmt.Fprintf(out, "  invite seeds:   %v\n", ce.InviteSeeds)
			}
			if ce.BootstrapURL != "" {
				_, _ = fmt.Fprintf(out, "  manifest url:   %s (tier-2 auto-discovery)\n", ce.BootstrapURL)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVar(&force, "force", false, "switch to a different cluster account_pub (resets the cache)")
	return cmd
}

func newClusterInviteCmd() *cobra.Command {
	var accountPub, seed, bootstrap string
	cmd := &cobra.Command{
		Use:     "invite",
		GroupID: "client",
		Short:   "Mint an out-of-band ctl discovery invite (account pin + failover endpoints) for a teammate",
		Example: "  tether cluster invite --account-pub AB... --seed wss://broker2:443   # hand the printed token to a teammate's `cluster pin`",
		Long: `Print a discovery invite token a teammate pastes into ` + "`tether cluster pin`" + ` to learn this
cluster's account_pub (trust pin) + failover endpoints. Provide at least one of --seed (tier-1 cold-start
floor) or --bootstrap (tier-2 HTTPS manifest). Deliver the token over a trusted OOB channel — it is
unsigned (its integrity rides the delivery channel, same as handing over broker_url + account_pub today).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			tok, err := clusterroster.MintDiscoveryInvite(clusterroster.Invite{
				Pin: accountPub, BootstrapURL: bootstrap, Seed: seed,
			})
			if err != nil {
				return fmt.Errorf("cluster invite: %w", err)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), tok)
			return nil
		},
	}
	cmd.Flags().StringVar(&accountPub, "account-pub", "", "the cluster account public key (trust pin); required")
	cmd.Flags().StringVar(&seed, "seed", "", "a client-dialable broker endpoint (nats://|tls://|wss://); tier-1 failover floor")
	cmd.Flags().StringVar(&bootstrap, "bootstrap", "", "the well-known HTTPS manifest URL (optional; tier-2 auto-discovery)")
	_ = cmd.MarkFlagRequired("account-pub")
	return cmd
}
