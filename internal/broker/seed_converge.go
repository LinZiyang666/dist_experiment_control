package broker

import (
	"database/sql"
	"net"
	"net/url"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// seed_converge.go — G3 #1: after a membership commit (join->VOTER / retire / online force-single
// prune) the leader auto-derives the cluster's client-dialable seed endpoints from the replicated
// roster and re-publishes them, so grow/shrink converge the account-signed SeedBundle WITHOUT a manual
// `cluster seeds publish`. It reuses the SAME PlanClusterSeedsPublish (all-literal, no new op → mixed
// version safe) and the SAME homogeneous-port assumption as the shipped client DialURLs — the derived
// endpoints ARE broker public_hosts (already in the signed roster), never an un-vetted self-reported
// field, so it adds no new poison surface (OQ-A option ①).

// DeriveSeedEndpoints turns the current roster into a deterministic, client-dialable seed endpoint set
// by inheriting the scheme+port+path of the already-published prev[0] template and templating it onto
// each dialable broker's public_host — the exact mechanism (and homogeneous-port invariant) of the
// shipped DialURLs (roster.go), only broker-side and WITHOUT the client's rand.Shuffle so the stored
// set is deterministic (a non-deterministic set would churn the non-change-gated seed_generation).
//
//   - prev empty            → nil (never bootstrap the FIRST publish; the operator must establish the
//     scheme/port/path template so we do not guess wss://host:443).
//   - template scheme nats  → nil (refuse to auto-propagate a cleartext-PIN-carrying nats:// template to
//     every broker; an operator-TYPED nats:// seed is their choice, a machine fan-out is not — security).
//   - template not tls/wss  → nil (only the account-PIN-safe schemes are auto-derivable).
//   - all brokers undialable → nil (loopback/empty public_host; the caller keeps the stored set, INV-2).
//
// Ordering: VOTER first, then transient (CATCHING_UP/PENDING/unknown), then DRAINING/RETIRING/
// VOTER_ADD_FAILED last — each tier sorted (deterministic). Capped to cluster.MaxSeedEndpoints (the single
// SSOT, Stage-C n23) keeping VOTERs preferred.
func DeriveSeedEndpoints(prev []string, brokers []proto.RosterBroker) []string {
	// Stage-C m18: use the FIRST parseable tls/wss entry in prev as the template (robust to a garbage /
	// nats:// prev[0] that would otherwise wedge convergence forever via the empty-set floor). nats:// and
	// any other scheme are SKIPPED, not auto-derived — never auto-propagate a cleartext-PIN-carrying
	// template (security); an operator-TYPED nats:// seed is their choice, a machine fan-out is not.
	var scheme, port, path string
	for _, p := range prev {
		tu, err := url.Parse(strings.TrimSpace(p))
		if err != nil || tu.Host == "" {
			continue
		}
		if tu.Scheme == "tls" || tu.Scheme == "wss" {
			scheme, port, path = tu.Scheme, tu.Port(), tu.EscapedPath()
			break
		}
	}
	if scheme == "" {
		return nil // empty prev / all-garbage / only nats:// → do not derive (first publish stays manual)
	}

	var voters, transient, draining []string
	for _, b := range brokers {
		host := b.PublicHost
		if host == "" || clusterroster.IsUndialableHost(host) {
			continue
		}
		if b.Phase == proto.RosterPhaseAddFailed {
			continue // Stage-C m11: never advertise a failed-join broker's endpoint (it may never have served)
		}
		hostport := host
		if port != "" {
			hostport = net.JoinHostPort(host, port)
		} else if strings.ContainsRune(host, ':') {
			hostport = "[" + host + "]" // Stage-C n21: bracket a bare IPv6 literal so the URL is well-formed
		}
		u := scheme + "://" + hostport + path
		switch b.Phase {
		case proto.RosterPhaseVoter:
			voters = append(voters, u)
		case proto.RosterPhaseDraining, proto.RosterPhaseRetiring:
			draining = append(draining, u)
		default: // CATCHING_UP, JOIN_VERIFIED_PENDING_VOTER, or any unknown/future phase
			transient = append(transient, u)
		}
	}
	sort.Strings(voters)
	sort.Strings(transient)
	sort.Strings(draining)
	out := make([]string, 0, len(voters)+len(transient)+len(draining))
	out = append(out, voters...)
	out = append(out, transient...)
	out = append(out, draining...)
	out = dedupeSeeds(out)
	if len(out) > cluster.MaxSeedEndpoints { // Stage-C n23: single SSOT with the plan-side ceiling
		out = out[:cluster.MaxSeedEndpoints]
	}
	return out
}

// dedupeSeeds removes duplicate URLs preserving first-seen order (guards a malformed roster where two
// brokers collapse to the same client URL). VOTER-first order is preserved so the cap keeps VOTERs.
func dedupeSeeds(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0:0]
	for _, u := range in {
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

// seedSetEqual reports whether two endpoint sets are equal as SETS (order-independent) — the change
// gate: if the derived set equals the stored set we do NOT re-publish, so the non-change-gated
// seed_generation never bumps and the fleet never needlessly re-adopts.
func seedSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// seedHostsMatchAnyBroker reports whether any stored endpoint's hostname equals some broker's
// public_host — the provenance-free clobber guard (OQ-B, replacing a seed_source KV): a stored set
// that matches SOME broker is a broker-endpoint set the leader owns (rebuild it, so grow/shrink
// converge existing deployments with zero opt-in); a set matching NO broker is an operator-curated
// VIP/LB floor (leave it alone — durable custom endpoints belong in ctl InviteSeeds).
func seedHostsMatchAnyBroker(endpoints []string, brokers []proto.RosterBroker) bool {
	hosts := make(map[string]struct{}, len(brokers))
	for _, b := range brokers {
		if b.PublicHost != "" {
			hosts[b.PublicHost] = struct{}{}
		}
	}
	for _, e := range endpoints {
		if u, err := url.Parse(e); err == nil {
			if _, ok := hosts[u.Hostname()]; ok {
				return true
			}
		}
	}
	return false
}

// deriveAndConvergeSeedsFromRoster is the shared leader helper called at the tail of each membership
// commit (and the leadership backstop). BEST-EFFORT: the caller logs and never fails the membership op.
// Invariants: read-back bootstrap (INV-3, PlanClusterSeedsPublish would otherwise blank it); host-match
// clobber guard (protect operator VIP sets); empty-set floor (INV-2, never Propose an empty set);
// deterministic change-gate (INV-1, no churn). Leader-only (Propose); RODB read-after-Propose is
// reliable on the leader's own SetMaxOpenConns(1) pool, exactly as PublishSeeds already relies on.
func (a *ClusterAdmin) deriveAndConvergeSeedsFromRoster() error {
	var eps []string
	var bootstrap string
	var brokers []proto.RosterBroker
	// Stage-C m10: read seed_endpoints + cluster_nodes in ONE bounded-stale snapshot so host-match and
	// derive see a consistent generation (a concurrent membership Apply between two separate RODB reads
	// could otherwise yield a spurious hands-off on stale seeds).
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		var e error
		if eps, bootstrap, _, e = cluster.Seeds(db); e != nil {
			return e
		}
		brokers, _, e = readRosterBrokers(db)
		return e
	}); err != nil {
		return err
	}
	if len(eps) > 0 && !seedHostsMatchAnyBroker(eps, brokers) {
		return nil // operator-curated VIP/LB set — hands-off (clobber guard)
	}
	next := DeriveSeedEndpoints(eps, brokers)
	if len(next) == 0 {
		if len(eps) > 0 {
			a.logger.Warn("seed auto-converge: derived empty endpoint set (roster all-loopback/undialable or non-tls/wss template); keeping stored seeds",
				"stored_count", len(eps))
		}
		return nil // empty-set floor (INV-2): never Propose empty / wipe
	}
	if seedSetEqual(next, eps) {
		return nil // change-gate: unchanged → no bump, no fleet re-adopt
	}
	// Stage-C m8: a takeover REBUILDS from the roster, so any stored endpoint whose host is not a current
	// broker (a departed broker OR an operator VIP mixed into a broker-endpoint set) is dropped. We do NOT
	// loud-WARN here: the same code path fires on every routine shrink (retire / force-single drops a
	// departed broker's endpoint — expected, not a clobber), and from the current roster we cannot tell a
	// departed broker from a VIP, so a warning would be noise + a wrong "move to InviteSeeds" hint. The
	// mixed-VIP clobber tradeoff is documented instead (docs/cluster.md §5.6.9: durable custom endpoints
	// belong in ctl InviteSeeds, never-clobber) and pinned by TestDeriveSeedEndpoints (the m8 table case).
	return a.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterSeedsPublish(next, bootstrap, a.now())
	})
}

// convergeSeedsAfterRemoval is the best-effort seed-convergence tail for the `cluster recovery node
// remove` paths (RemoveNode / removeGhost). G3 #1 Stage-C M1: deleting a force-single ghost's roster row
// must ALSO drop its dead client endpoint from the published seeds — matching the inline retire +
// online-force-single + offline drop-only paths (online/offline parity). Critically, on a single-voter
// force-single cluster the sole voter holds leadership indefinitely, so NO later leadership edge fires the
// ReconcileMembershipOnLeadership backstop; this operator-finalizer tail is then the ONLY trigger that
// converges seeds after the ghost is cleared. Best-effort (never fail a completed removal).
func (a *ClusterAdmin) convergeSeedsAfterRemoval(nodeID string) {
	if err := a.deriveAndConvergeSeedsFromRoster(); err != nil {
		a.logger.Warn("cluster recovery node remove: seed auto-converge failed (removed node's endpoint may linger in published seeds until a later op / leadership backstop)",
			"node_id", nodeID, "err", err)
	}
}
