package main

import (
	"context"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// ctl_connect.go — the ONE ctl/cli connect path with cluster broker auto-failover (v0.4.5 阶段4 fix).
// Every ctl-over-NATS subcommand connects through connectCtl instead of the bare
// ResolveNATSURLFromHome+ConnectNATSWithNkey+connectError triple, so failover, the operator-override
// invariant, and the opportunistic discovery refresh live in exactly one place.

const (
	// ctlRefreshTTL bounds how often a File-source command re-fetches the signed HTTP manifest (tier-2).
	ctlRefreshTTL = 10 * time.Minute
	// ctlManifestFetchTimeout bounds the post-connect manifest fetch so a slow/absent route adds at most
	// this to a command, once per TTL.
	ctlManifestFetchTimeout = 2500 * time.Millisecond
)

// connectCtl resolves the dial set, connects with nats.Connect failover, and (only on the expandable,
// OOB-pinned broker_url path) opportunistically refreshes the discovery cache from the signed manifest.
// base is the human single URL (error text); dial is the comma-separated failover list (== base on every
// non-expandable path → byte-equivalent to today). The two extra opts are added ONLY when dial != base.
func connectCtl(cmd *cobra.Command, verb, home, natsURL string, id *cli.Identity, name nats.Option) (*nats.Conn, error) {
	return connectCtlOpts(cmd, verb, home, natsURL, id, name)
}

// connectCtlOpts is connectCtl with caller-supplied NATS options (e.g. nats.Token(pin) for a first-time
// login PIN join). Same source-aware failover: expand ONLY on the persisted broker_url default (never an
// explicit --nats-url/--broker flag nor $TETHER_NATS_URL), and opportunistically refresh the discovery
// cache from the signed manifest on every eligible connect — including a bootstrap-only pin whose dial has
// not expanded yet (the refresh is gated on source eligibility, NOT on dial!=base, so tier-2 can warm).
func connectCtlOpts(cmd *cobra.Command, verb, home, natsURL string, id *cli.Identity, extra ...nats.Option) (*nats.Conn, error) {
	base := natsURL // already resolved by the caller's ResolveNATSURLFromHome (single human URL)
	// --broker is login's alias of --nats-url; both pin single. An explicit flag or $TETHER_NATS_URL means
	// the source is NOT the persisted broker_url default → no expansion, no refresh.
	flagChanged := cmd.Flags().Changed("nats-url") || cmd.Flags().Changed("broker")
	expandable := !flagChanged && os.Getenv(cli.DefaultBrokerURLEnv) == ""
	dial := cli.DialFor(flagChanged, base, home, time.Now())
	opts := append([]nats.Option(nil), extra...)
	if dial != base {
		// expanded path: DontRandomize honors the floor-last order; Timeout(3s) bounds the per-endpoint
		// stall on a filtered/hung broker (proxydial is 10s/endpoint). No-ops for a single URL → kept off
		// the byte-equivalent path.
		opts = append(opts, nats.DontRandomize(), nats.Timeout(3*time.Second))
	}
	nc, err := cli.ConnectNATSWithNkey(dial, id, opts...)
	if err != nil {
		return nil, connectError(verb, base, err)
	}
	if expandable {
		refreshCtlEndpoints(cmd.Context(), home, base)
	}
	return nc, nil
}

// refreshCtlEndpoints is the tier-2 opportunistic discovery refresh: best-effort, TTL-gated, and
// PIN-gated (no pin ⇒ return ⇒ NEVER HTTP-TOFUs, the tested agent invariant). It fetches the signed
// manifest and funnels it through the agent's single adopt authority against the EXISTING pin — a
// forged/foreign/expired/rollback manifest is rejected and leaves the cache bytes UNCHANGED (no poison).
// No bootstrap URL configured (tier-1 invite-seed only) ⇒ no fetch ⇒ zero cost.
func refreshCtlEndpoints(ctx context.Context, home, base string) {
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil || ce == nil || ce.PinAccountPub == "" || ce.FloorURL != base || ce.BootstrapURL == "" {
		return
	}
	if ce.FetchedAt != "" {
		if t, perr := time.Parse(time.RFC3339, ce.FetchedAt); perr == nil && time.Since(t) < ctlRefreshTTL {
			return
		}
	}
	fctx, cancel := context.WithTimeout(ctx, ctlManifestFetchTimeout)
	defer cancel()
	m, err := clusterroster.FetchManifest(fctx, ce.BootstrapURL, 0)
	if err != nil || m == nil {
		return
	}
	prev := agent.RosterState{
		Pin: ce.PinAccountPub, RosterGen: ce.RosterGen, SeedGen: ce.SeedGen,
		Roster: ce.Roster, Seeds: ce.Seeds,
	}
	next, accepted := agent.AdoptDecision(prev, m.Roster, m.Seeds, base, time.Now())
	if !accepted {
		return // reject leaves the cache untouched (no poison)
	}
	ce.RosterGen = next.RosterGen
	ce.Roster = next.Roster
	ce.SeedGen = next.SeedGen
	ce.Seeds = next.Seeds
	ce.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	_ = cli.WriteClusterEndpoints(home, ce)
}
