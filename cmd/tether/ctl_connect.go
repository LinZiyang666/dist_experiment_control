package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
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
		refreshCtlEndpoints(cmd.Context(), nc, home, base, id.PublicKey)
	}
	return nc, nil
}

// refreshCtlEndpoints is the tier-2 opportunistic discovery refresh: best-effort, TTL-gated, and
// PIN-gated. It funnels a signed manifest through the agent's single adopt authority (AdoptDecision)
// against the EXISTING pin — a forged/foreign/expired/rollback manifest is rejected and leaves the cache
// bytes UNCHANGED (no poison).
//
// G3 #17 — two changes to escape the "floor single-point" of the shipped version:
//   - 改法二 (primary): pull the signed manifest from the ACTUALLY-CONNECTED broker over the live conn
//     (SubjCtrlClusterRoster), so discovery converges from whichever survivor a transparent failover
//     landed on — not the one pinned floor/bootstrap host. HTTP bootstrap is kept as a secondary fallback.
//   - 改法一: the FloorURL==base gate B is REMOVED. A signed manifest self-authenticates against the OOB
//     pin, so WHICH broker delivered it is cryptographically irrelevant. On the accepted path FloorURL is
//     migrated to base so DialFor's FloorURL==base expansion gate self-heals after an operator re-point.
//
// 门A (PIN) stays ABSOLUTE and structural: a nil/empty pin ⇒ return BEFORE any consume — never TOFU-pin
// off a network responder's self-claimed AccountPub (AdoptDecision would otherwise TOFU the roster path).
func refreshCtlEndpoints(ctx context.Context, nc *nats.Conn, home, base, actor string) {
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil || ce == nil || ce.PinAccountPub == "" {
		return
	}
	if ce.FetchedAt != "" {
		if t, perr := time.Parse(time.RFC3339, ce.FetchedAt); perr == nil && time.Since(t) < ctlRefreshTTL {
			return
		}
	}
	// templateURL = the STABLE cached FloorURL (fallback base) so DialURLs scheme/port/path templating is
	// consistent regardless of which survivor answered (homogeneous-port assumption; only host varies).
	tmpl := ce.FloorURL
	if tmpl == "" {
		tmpl = base
	}
	// Nothing to refresh FROM (no live conn AND no bootstrap URL) → return WITHOUT touching FetchedAt: we
	// never attempted a fetch, so we must not throttle the next eligible connect.
	if nc == nil && ce.BootstrapURL == "" {
		return
	}
	prev := agent.RosterState{
		Pin: ce.PinAccountPub, RosterGen: ce.RosterGen, SeedGen: ce.SeedGen,
		Roster: ce.Roster, Seeds: ce.Seeds,
	}
	next := prev
	accepted := false
	adopt := func(m *proto.ClusterManifest) {
		if m == nil {
			return
		}
		if n2, ok := agent.AdoptDecision(next, m.Roster, m.Seeds, tmpl, time.Now()); ok {
			next = n2 // accumulate — AdoptDecision's monotone gate keeps the higher generation
			accepted = true
		}
	}
	// 改法二 primary: pull from the actually-connected survivor (escapes the floor/bootstrap single point).
	adopt(fetchManifestOverNATS(ctx, nc, actor))
	// External-review F1: if the NATS pull did not advance EITHER generation (a cached-but-stale roster-pull
	// in the ≥30s manifest window, or nil / rejected — note the roster and seed gens advance independently,
	// so a manifest can bump seed but not roster), consult the HTTP bootstrap too so a FRESHER manifest is
	// not masked by the stale one. Both candidates flow through the SAME AdoptDecision monotone gate — the
	// higher generation wins, a rollback/foreign one is still rejected (no poison). Only when NATS advanced
	// BOTH gens (fully fresh) do we skip HTTP, preserving 改法二's "no bootstrap dependency" property.
	if (next.RosterGen <= prev.RosterGen || next.SeedGen <= prev.SeedGen) && ce.BootstrapURL != "" {
		fctx, cancel := context.WithTimeout(ctx, ctlManifestFetchTimeout)
		defer cancel()
		httpM, _ := clusterroster.FetchManifest(fctx, ce.BootstrapURL, 0)
		adopt(httpM)
	}
	// Stage-C M2: we ATTEMPTED a refresh — advance FetchedAt to throttle it to at most once per ctlRefreshTTL,
	// so a FAILING/non-advancing refresh does NOT re-incur the (possibly multi-second) fetch on every ctl
	// command; it retries once per TTL, like a successful one.
	ce.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	if accepted {
		ce.RosterGen = next.RosterGen
		ce.Roster = next.Roster
		ce.SeedGen = next.SeedGen
		ce.Seeds = next.Seeds
		ce.FloorURL = base // 改法一: self-heal the DialFor expansion gate after a re-point (accepted path only)
	}
	// reject/no-progress → roster/seed/FloorURL fields untouched (no poison); only FetchedAt advanced.
	_ = cli.WriteClusterEndpoints(home, ce)
}

// fetchManifestOverNATS pulls the signed cluster manifest from the connected broker over the live conn
// (G3 #17 改法二). Returns nil on ANY request failure — ErrNoResponders (an old broker that never wired
// the responder), a TIMEOUT (a cluster-mode-but-unsigned broker that is subscribed but silent, or a new
// ctl whose JWT was minted by an OLD broker lacking the cluster-roster.req pub-allow so the publish is
// refused), or a bad reply — so the caller falls back to the HTTP bootstrap. nil is NOT an error, it is
// "this broker cannot answer over NATS". The reply is verified downstream by AdoptDecision against the
// OOB pin (this never trusts it blindly).
func fetchManifestOverNATS(ctx context.Context, nc *nats.Conn, actor string) *proto.ClusterManifest {
	if nc == nil || actor == "" {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, ctlManifestFetchTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, proto.SubjCtrlClusterRoster(actor), nil)
	if err != nil || msg == nil || len(msg.Data) == 0 {
		return nil
	}
	var m proto.ClusterManifest
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		return nil
	}
	return &m
}
