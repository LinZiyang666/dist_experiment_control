// Package clusterroster owns the B7 DOC#3 account-signed broker roster: its canonicalization,
// signing (with the cluster account seed), and verification (against the agent's pinned
// account_pub). It is a pure leaf — it imports internal/auth + internal/proto only, never raft or
// nats — so both the broker (Build/sign) and the agent (Verify/Select) share one implementation.
//
// SCOPE (audit SIMP-MAJOR-1 / b7-review.md): only Build + the broker register-push ship in v2.
// Verify/Select are the TESTED seam for the DEFERRED post-v2 agent-discovery consumer (the agent
// has no account_pub pin today, so consuming the roster touches the fragile reconnect path the B7
// plan explicitly defers). They are intentionally present-but-unconsumed — NOT dead code to remove;
// removing them would force re-deriving + re-testing the verifier when the consumer lands.
//
// SECURITY MODEL (security-pragmatic v1): the roster is signed by the ACCOUNT seed (the same
// anchor agents already pin) and verified against the pinned account_pub — this reuses the
// existing trust anchor, no new PKI. The roster is DISCOVERY-ONLY: it confers ZERO membership
// authority. A node becomes a voter only via the two-phase join-PoP over a fresh leader-issued
// nonce; this verifier must NEVER be mistaken for a join credential. Generation is a
// MEMBERSHIP-DOMAIN monotone value (NOT the raft commit index, which restarts lower after
// force-single/recover and would make agents reject the legitimate post-recovery roster).
package clusterroster

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// sep is the NUL field separator for the canonical bytes (impossible in the textual fields,
// mirroring cluster.JoinSignBytes — so distinct field sets cannot collide).
const sep = "\x00"

// CanonicalRosterBytes is the exact byte sequence signed/verified. It is deterministic: brokers
// in input order (the builder sorts by node_id), every field NUL-separated, the Sig field itself
// EXCLUDED. Any change to the topology/generation/account/expiry changes these bytes.
func CanonicalRosterBytes(r *proto.ClusterRoster) []byte {
	var b strings.Builder
	b.WriteString("tether.roster.v")
	b.WriteString(strconv.Itoa(r.SchemaVersion))
	b.WriteString(sep)
	b.WriteString(strconv.FormatUint(r.Generation, 10))
	b.WriteString(sep)
	b.WriteString(r.AccountPub)
	b.WriteString(sep)
	b.WriteString(r.IssuedAt)
	b.WriteString(sep)
	b.WriteString(r.ExpiresAt)
	for _, br := range r.Brokers {
		b.WriteString(sep)
		b.WriteString(strings.Join([]string{br.NodeID, br.NatsRoute, br.PublicHost, br.TunnelAddr, br.CertFP, br.Phase}, sep))
	}
	return []byte(b.String())
}

// Build assembles + signs a roster with the account seed. accountPub MUST be the public form of
// accountSeed (the builder records it so a verifier can pin against it). brokers are sorted by
// node_id for determinism.
func Build(accountSeed []byte, accountPub string, generation uint64, brokers []proto.RosterBroker, issuedAt, expiresAt string) (*proto.ClusterRoster, error) {
	sorted := append([]proto.RosterBroker(nil), brokers...)
	for i := 1; i < len(sorted); i++ { // tiny insertion sort by node_id (small N)
		for j := i; j > 0 && sorted[j-1].NodeID > sorted[j].NodeID; j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	r := &proto.ClusterRoster{
		SchemaVersion: proto.ClusterRosterSchemaVersion,
		Generation:    generation,
		AccountPub:    accountPub,
		Brokers:       sorted,
		IssuedAt:      issuedAt,
		ExpiresAt:     expiresAt,
	}
	sig, err := auth.SignWithSeed(accountSeed, CanonicalRosterBytes(r))
	if err != nil {
		return nil, fmt.Errorf("clusterroster: sign: %w", err)
	}
	r.Sig = hex.EncodeToString(sig)
	return r, nil
}

// ErrAccountMismatch is returned when a roster's embedded account_pub != the agent's pinned one
// (a foreign-cluster / cross-account roster).
var ErrAccountMismatch = errors.New("clusterroster: roster account_pub does not match the pinned account")

// Verify checks the roster against the agent's PINNED account_pub: (1) the embedded account_pub
// equals the pin (anti cross-cluster), and (2) the signature verifies against that pin. A roster
// whose account_pub matches the pin but whose signature was made by a different key fails (2) —
// so a forged roster cannot vouch for itself.
func Verify(r *proto.ClusterRoster, pinnedAccountPub string) error {
	if r == nil {
		return errors.New("clusterroster: nil roster")
	}
	if r.AccountPub != pinnedAccountPub {
		return ErrAccountMismatch
	}
	sig, err := hex.DecodeString(r.Sig)
	if err != nil {
		return fmt.Errorf("clusterroster: sig not hex: %w", err)
	}
	if err := auth.VerifySignature(pinnedAccountPub, CanonicalRosterBytes(r), sig); err != nil {
		return fmt.Errorf("clusterroster: signature invalid: %w", err)
	}
	return nil
}

// ErrRosterExpired / ErrRosterSchema are the freshness/compat rejections VerifyAt adds over Verify.
var (
	ErrRosterExpired = errors.New("clusterroster: roster expired")
	ErrRosterSchema  = errors.New("clusterroster: unknown roster schema_version")
)

// VerifyAt is the HARDENED verifier the deferred agent-discovery consumer MUST use (External-review
// F14): it is Verify PLUS (1) a schema_version compatibility gate — an unknown (future) schema is
// rejected rather than mis-parsed — and (2) an expiry gate: a roster carrying an expires_at that has
// passed `now` is refused, so a captured/cached old roster cannot be replayed indefinitely to pin an
// agent onto retired broker routes. A roster with NO expires_at (the current broker output) is NOT
// rejected on freshness — the producer must start stamping a TTL before the consumer trusts it as
// more than a short-lived fallback; VerifyAt makes the seam ready for that.
// origin: line-2 review M14 (moved here from .golangci.yml, where the exemption was pinned to line
// ranges a comment edit could invalidate). VerifyAt and seeds.go's VerifySeedsAt run parallel
// verify+schema+expiry checks over two DIFFERENT signed artifacts. Unifying them needs a shared
// interface over unrelated types, which buys less than it costs at two call sites.
//
//nolint:dupl // paired with VerifySeedsAt in seeds.go; argument above.
func VerifyAt(r *proto.ClusterRoster, pinnedAccountPub string, now time.Time) error {
	if err := Verify(r, pinnedAccountPub); err != nil {
		return err
	}
	if r.SchemaVersion > proto.ClusterRosterSchemaVersion {
		return fmt.Errorf("%w: %d (this binary supports <= %d)", ErrRosterSchema, r.SchemaVersion, proto.ClusterRosterSchemaVersion)
	}
	if r.ExpiresAt != "" {
		exp, err := time.Parse(time.RFC3339Nano, r.ExpiresAt)
		if err != nil {
			return fmt.Errorf("clusterroster: bad expires_at %q: %w", r.ExpiresAt, err)
		}
		if now.After(exp) {
			return fmt.Errorf("%w: expired_at %s", ErrRosterExpired, r.ExpiresAt)
		}
	}
	return nil
}

// Select returns the broker NATS routes an agent should dial, in order: VOTER brokers first (by
// node_id), then non-voters. A caller building a nats.Connect URL list uses this. (Superseded for
// the C1 agent consumer by DialURLs, which templates the CLIENT scheme/port/path; Select is kept as
// the shared route-level selector the broker tests use.)
func Select(r *proto.ClusterRoster) []string {
	var voters, others []string
	for _, b := range r.Brokers {
		if b.NatsRoute == "" {
			continue
		}
		if b.Phase == proto.RosterPhaseVoter {
			voters = append(voters, b.NatsRoute)
		} else {
			others = append(others, b.NatsRoute)
		}
	}
	return append(voters, others...)
}

// DialURLs returns the CLIENT-dialable NATS URLs an agent should (re)connect through, derived from
// the signed roster + the agent's own configured URL as the template for the client scheme/port/path
// (C1 §D-3). RosterBroker.NatsRoute is the cluster ROUTE addr (port :6222), NOT dialable by a client
// through auth_callout — so we re-template each broker's PublicHost onto the template's
// scheme+port+path instead of trusting an un-signed client-URL field (which would force a roster
// schema bump / break signature byte-stability against shipped v0.4.0 brokers).
//
// Ordering is 3-tier (c1-plan.md §D-7): VOTER (shuffled to spread fleet reconnect load) → transient
// (CATCHING_UP / PENDING / unknown) → DRAINING / RETIRING / VOTER_ADD_FAILED LAST (de-preferred,
// never dropped — "停止偏好" not "drop", so an all-draining roster still yields URLs). A broker with
// an empty / loopback / unspecified PublicHost is SKIPPED (un-dialable, would strand). The caller
// MUST union the result with its configured seed URL as a PERMANENT floor so an all-loopback roster
// never empties the live pool. Returns (nil, err) only on an unparseable templateURL.
func DialURLs(r *proto.ClusterRoster, templateURL string) ([]string, error) {
	if r == nil {
		return nil, nil
	}
	first := templateURL
	if i := strings.IndexByte(first, ','); i >= 0 { // tolerate a comma-list seed; template off the first
		first = first[:i]
	}
	tu, err := url.Parse(strings.TrimSpace(first))
	if err != nil {
		return nil, fmt.Errorf("clusterroster: bad template url %q: %w", templateURL, err)
	}
	scheme := tu.Scheme
	if scheme == "" {
		scheme = "nats"
	}
	port := tu.Port()        // "" when the template carries no explicit port
	path := tu.EscapedPath() // wss/Caddy deployments carry a path the client must keep (§D-3 caveat)

	var voters, transient, draining []string
	for _, b := range r.Brokers {
		host := b.PublicHost
		if host == "" || IsUndialableHost(host) {
			continue
		}
		hostport := host
		if port != "" {
			hostport = net.JoinHostPort(host, port)
		}
		u := scheme + "://" + hostport + path
		switch b.Phase {
		case proto.RosterPhaseVoter:
			voters = append(voters, u)
		case proto.RosterPhaseDraining, proto.RosterPhaseRetiring, proto.RosterPhaseAddFailed:
			draining = append(draining, u)
		default: // CATCHING_UP, JOIN_VERIFIED_PENDING_VOTER, or any unknown/future phase
			transient = append(transient, u)
		}
	}
	rand.Shuffle(len(voters), func(i, j int) { voters[i], voters[j] = voters[j], voters[i] })
	out := make([]string, 0, len(voters)+len(transient)+len(draining))
	out = append(out, voters...)
	out = append(out, transient...)
	out = append(out, draining...)
	return dedupeStable(out), nil
}

// IsUndialableHost reports whether a roster PublicHost can never be reached from another machine
// (loopback / unspecified) and so must be skipped — a broker that advertises "localhost" / "0.0.0.0"
// (publicHostFor can fall back to "localhost") must not strand an agent onto an un-dialable target.
func IsUndialableHost(host string) bool {
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsUnspecified()
	}
	return false
}

// dedupeStable removes duplicate URLs preserving first-seen order (a node could appear once; this
// guards against a malformed roster or a host that collapses to the same client URL across tiers).
func dedupeStable(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
