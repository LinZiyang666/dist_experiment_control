// seed_converge.go — G3 #1: after a membership commit (join->VOTER / retire / online force-single
// prune) the leader auto-derives the cluster's client-dialable seed endpoints from the replicated
// roster and re-publishes them, so grow/shrink converge the account-signed SeedBundle WITHOUT a manual
// `cluster seeds publish`. It reuses the SAME PlanClusterSeedsPublish (all-literal, no new op → mixed
// version safe) and the SAME homogeneous-port assumption as the shipped client DialURLs — the derived
// endpoints ARE broker public_hosts (already in the signed roster), never an un-vetted self-reported
// field, so it adds no new poison surface (OQ-A option ①).
package brokercontrol

import (
	"database/sql"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

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
	out = DedupeSeeds(out)
	if len(out) > cluster.MaxSeedEndpoints { // Stage-C n23: single SSOT with the plan-side ceiling
		out = out[:cluster.MaxSeedEndpoints]
	}
	return out
}

// dedupeSeeds removes duplicate URLs preserving first-seen order (guards a malformed roster where two
// brokers collapse to the same client URL). VOTER-first order is preserved so the cap keeps VOTERs.
func DedupeSeeds(in []string) []string {
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
func SeedSetEqual(a, b []string) bool {
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
func SeedHostsMatchAnyBroker(endpoints []string, brokers []proto.RosterBroker) bool {
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
func (a *ClusterAdmin) DeriveAndConvergeSeedsFromRoster() error {
	var eps []string
	var bootstrap string
	var brokers []proto.RosterBroker
	// Stage-C m10: read seed_endpoints + cluster_nodes in ONE bounded-stale snapshot so host-match and
	// derive see a consistent generation (a concurrent membership Apply between two separate RODB reads
	// could otherwise yield a spurious hands-off on stale seeds).
	if err := a.Node.BoundedStaleRead(func(db *sql.DB) error {
		var e error
		if eps, bootstrap, _, e = cluster.Seeds(db); e != nil {
			return e
		}
		brokers, _, e = ReadRosterBrokers(db)
		return e
	}); err != nil {
		return err
	}
	if len(eps) > 0 && !SeedHostsMatchAnyBroker(eps, brokers) {
		return nil // operator-curated VIP/LB set — hands-off (clobber guard)
	}
	next := DeriveSeedEndpoints(eps, brokers)
	if len(next) == 0 {
		if len(eps) > 0 {
			a.Logger.Warn("seed auto-converge: derived empty endpoint set (roster all-loopback/undialable or non-tls/wss template); keeping stored seeds",
				"stored_count", len(eps))
		}
		return nil // empty-set floor (INV-2): never Propose empty / wipe
	}
	if SeedSetEqual(next, eps) {
		return nil // change-gate: unchanged → no bump, no fleet re-adopt
	}
	// Stage-C m8: a takeover REBUILDS from the roster, so any stored endpoint whose host is not a current
	// broker (a departed broker OR an operator VIP mixed into a broker-endpoint set) is dropped. We do NOT
	// loud-WARN here: the same code path fires on every routine shrink (retire / force-single drops a
	// departed broker's endpoint — expected, not a clobber), and from the current roster we cannot tell a
	// departed broker from a VIP, so a warning would be noise + a wrong "move to InviteSeeds" hint. The
	// mixed-VIP clobber tradeoff is documented instead (docs/cluster.md §5.6.9: durable custom endpoints
	// belong in ctl InviteSeeds, never-clobber) and pinned by TestDeriveSeedEndpoints (the m8 table case).
	return a.Node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterSeedsPublish(next, bootstrap, a.Now())
	})
}

// convergeSeedsAfterRemoval is the best-effort seed-convergence tail for the `cluster recovery node
// remove` paths (RemoveNode / removeGhost). G3 #1 Stage-C M1: deleting a force-single ghost's roster row
// must ALSO drop its dead client endpoint from the published seeds — matching the inline retire +
// online-force-single + offline drop-only paths (online/offline parity). Critically, on a single-voter
// force-single cluster the sole voter holds leadership indefinitely, so NO later leadership edge fires the
// ReconcileMembershipOnLeadership backstop; this operator-finalizer tail is then the ONLY trigger that
// converges seeds after the ghost is cleared. Best-effort (never fail a completed removal).
func (a *ClusterAdmin) ConvergeSeedsAfterRemoval(nodeID string) {
	if err := a.DeriveAndConvergeSeedsFromRoster(); err != nil {
		a.Logger.Warn("cluster recovery node remove: seed auto-converge failed (removed node's endpoint may linger in published seeds until a later op / leadership backstop)",
			"node_id", nodeID, "err", err)
	}
}

// readRosterBrokers projects the public broker fields off cluster_nodes + derives an ADVISORY
// generation (the max of the leader-baked added_at/phase_changed_at times, as unix-nano). It
// advances on most membership changes but is NOT strictly monotone (Stage-C M1: it can regress when
// the max-stamp node is pruned/retired) — so delivery never gates on it (rosterForRegister always
// emits); it is only an advisory tie-breaker. NOT the raft commit index (which resets on recover).
func ReadRosterBrokers(ro *sql.DB) ([]proto.RosterBroker, uint64, error) {
	rows, err := ro.Query(`SELECT node_id, nats_route, public_host, tunnel_addr, cert_fp, phase,
	    COALESCE(added_at,''), COALESCE(phase_changed_at,'') FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var out []proto.RosterBroker
	var gen int64
	for rows.Next() {
		var br proto.RosterBroker
		var addedAt, changedAt string
		if err := rows.Scan(&br.NodeID, &br.NatsRoute, &br.PublicHost, &br.TunnelAddr, &br.CertFP, &br.Phase, &addedAt, &changedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, br)
		if v := TsUnixNano(addedAt); v > gen {
			gen = v
		}
		if v := TsUnixNano(changedAt); v > gen {
			gen = v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if gen < 0 {
		gen = 0
	}
	return out, uint64(gen), nil
}

// tsUnixNano parses a leader-baked timestamp (RFC3339Nano OR Go's time.String() form) to unix
// nanos; 0 on any parse failure (the signature is the real anchor; generation is rollback-detection
// and degrades safely to 0).
func TsUnixNano(s string) int64 {
	if s == "" {
		return 0
	}
	// Stage-C m5: a Go time.String() with a monotonic-clock reading carries a trailing " m=+…"
	// token the layouts below don't cover — strip it so such a stamp still parses (else it would
	// silently yield 0).
	if i := strings.Index(s, " m="); i >= 0 {
		s = s[:i]
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixNano()
		}
	}
	return 0
}

// buildSignedRoster reads the replicated roster off RODB() and account-signs it. Returns nil if
// the account seed is unavailable (signing impossible) or no brokers are present.
func BuildSignedRoster(db *sql.DB, seed []byte, clock func() time.Time, ttl time.Duration) *proto.ClusterRoster {
	if len(seed) == 0 {
		return nil
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil
	}
	brokers, derived, err := ReadRosterBrokers(db)
	if err != nil || len(brokers) == 0 {
		return nil
	}
	// C1 §D-2: stamp the TRULY-MONOTONE roster_generation counter (cluster_meta), NOT the
	// derived max(added_at,phase_changed_at) — which regresses on retire-of-newest/recover and
	// would make a strict-monotone agent reject the corrected roster.
	gen, err := cluster.RosterGeneration(db)
	if err != nil {
		return nil
	}
	// Stage-C BLOCKER-1 (rolling-upgrade floor): the counter row is ABSENT (reads 0) until the
	// first membership/phase/cert op bumps it. Shipped v0.4.0 stamped the DERIVED max (~now.UnixNano)
	// as the generation, so an agent that cached that high hwm from a v0.4.0 broker would reject a
	// freshly-upgraded C1 broker's gen=0 roster as a rollback. FLOOR the wire gen with the derived
	// max (which readRosterBrokers already computes): the floor only ever applies before the counter
	// overtakes it (the first real op bumps it to >now > any prior hwm), after which the counter's
	// strict monotonicity governs and the derived max's own regressions are irrelevant. Net: the wire
	// gen is monotone AND never presents below what v0.4.0 served — closing the mixed-version wedge.
	if derived > gen {
		gen = derived
	}
	// External-review re-review: STAMP a TTL so the anti-replay seam (clusterroster.VerifyAt) is
	// fully armed when the deferred agent consumer lands — a captured old roster cannot then be
	// replayed indefinitely. The roster is re-pushed on every register (frequent), so a short TTL is
	// safe; the agent treats a cached roster only as a short-lived fallback.
	now := clock().UTC()
	expires := now.Add(ttl)
	r, err := clusterroster.Build(seed, accountPub, gen, brokers, now.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano))
	if err != nil {
		return nil
	}
	return r
}

// buildSeedBundle reads the replicated seed config + account-signs it. nil until `cluster seeds
// publish` has stored at least one endpoint (or when no account seed).
func BuildSeedBundle(seed []byte, now time.Time, endpoints []string, bootstrap string, gen uint64, ttl time.Duration) *proto.SeedBundle {
	if len(endpoints) == 0 {
		return nil
	}
	if len(seed) == 0 {
		return nil
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil
	}
	issued := now.UTC()
	expires := issued.Add(ttl)
	s, err := clusterroster.BuildSeeds(seed, accountPub, gen, endpoints, bootstrap, issued.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano))
	if err != nil {
		return nil
	}
	return s
}
