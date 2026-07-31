package clusterroster

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// seeds.go (C2) — the account-signed SeedBundle: its canonicalization, signing (cluster account
// seed), and verification (against the agent's pinned account_pub). Pure leaf (auth + proto only),
// mirroring roster.go so the broker (Build) and the agent (Verify) share one implementation. The
// SeedBundle carries the CLIENT-DIALABLE endpoints a brand-new agent needs for its first connection;
// it is DISCOVERY-ONLY (public endpoint URLs, zero secrets).

// CanonicalSeedBytes is the exact byte sequence signed/verified for a SeedBundle. Deterministic:
// every field NUL-separated, endpoints in input order, the Sig field EXCLUDED. Any change to
// generation/account/endpoints/bootstrap/expiry changes these bytes.
func CanonicalSeedBytes(s *proto.SeedBundle) []byte {
	var b strings.Builder
	b.WriteString("tether.seeds.v")
	b.WriteString(strconv.Itoa(s.SchemaVersion))
	b.WriteString(sep)
	b.WriteString(strconv.FormatUint(s.Generation, 10))
	b.WriteString(sep)
	b.WriteString(s.AccountPub)
	b.WriteString(sep)
	b.WriteString(s.IssuedAt)
	b.WriteString(sep)
	b.WriteString(s.ExpiresAt)
	b.WriteString(sep)
	b.WriteString(s.BootstrapURL)
	for _, e := range s.Endpoints {
		b.WriteString(sep)
		b.WriteString(e)
	}
	return []byte(b.String())
}

// BuildSeeds assembles + signs a SeedBundle with the account seed. accountPub MUST be the public form
// of accountSeed. endpoints are kept in input order (the leader's published order).
func BuildSeeds(accountSeed []byte, accountPub string, generation uint64, endpoints []string, bootstrapURL, issuedAt, expiresAt string) (*proto.SeedBundle, error) {
	s := &proto.SeedBundle{
		SchemaVersion: proto.SeedBundleSchemaVersion,
		Generation:    generation,
		AccountPub:    accountPub,
		Endpoints:     append([]string(nil), endpoints...),
		BootstrapURL:  bootstrapURL,
		IssuedAt:      issuedAt,
		ExpiresAt:     expiresAt,
	}
	sig, err := auth.SignWithSeed(accountSeed, CanonicalSeedBytes(s))
	if err != nil {
		return nil, fmt.Errorf("clusterroster: sign seeds: %w", err)
	}
	s.Sig = hex.EncodeToString(sig)
	return s, nil
}

// ErrSeedsExpired / ErrSeedsSchema are the freshness/compat rejections VerifySeedsAt adds.
var (
	ErrSeedsExpired = errors.New("clusterroster: seed bundle expired")
	ErrSeedsSchema  = errors.New("clusterroster: unknown seed bundle schema_version")
)

// VerifySeeds checks a SeedBundle against the agent's PINNED account_pub: embedded account_pub equals
// the pin (anti cross-cluster) AND the signature verifies against that pin. Mirrors Verify.
func VerifySeeds(s *proto.SeedBundle, pinnedAccountPub string) error {
	if s == nil {
		return errors.New("clusterroster: nil seed bundle")
	}
	if s.AccountPub != pinnedAccountPub {
		return ErrAccountMismatch
	}
	sig, err := hex.DecodeString(s.Sig)
	if err != nil {
		return fmt.Errorf("clusterroster: seed sig not hex: %w", err)
	}
	if err := auth.VerifySignature(pinnedAccountPub, CanonicalSeedBytes(s), sig); err != nil {
		return fmt.Errorf("clusterroster: seed signature invalid: %w", err)
	}
	return nil
}

// VerifySeedsAt is VerifySeeds PLUS a schema_version compatibility gate (an unknown future schema is
// rejected, not mis-parsed) and an expiry gate (a captured/cached old bundle past expires_at is
// refused). The HARDENED verifier the C2 consumer MUST use — mirrors VerifyAt.
// origin: line-2 review M14. roster.go's VerifyAt carries the argument for this pair.
//
//nolint:dupl // paired with VerifyAt in roster.go; two unrelated signed types, one sequence.
func VerifySeedsAt(s *proto.SeedBundle, pinnedAccountPub string, now time.Time) error {
	if err := VerifySeeds(s, pinnedAccountPub); err != nil {
		return err
	}
	if s.SchemaVersion > proto.SeedBundleSchemaVersion {
		return fmt.Errorf("%w: %d (this binary supports <= %d)", ErrSeedsSchema, s.SchemaVersion, proto.SeedBundleSchemaVersion)
	}
	if s.ExpiresAt != "" {
		exp, err := time.Parse(time.RFC3339Nano, s.ExpiresAt)
		if err != nil {
			return fmt.Errorf("clusterroster: bad seed expires_at %q: %w", s.ExpiresAt, err)
		}
		if now.After(exp) {
			return fmt.Errorf("%w: expired_at %s", ErrSeedsExpired, s.ExpiresAt)
		}
	}
	return nil
}

// SeedDialURLs returns the client-dialable endpoints from a verified SeedBundle, filtered to non-empty
// entries. These are ALREADY full client URLs (unlike roster NatsRoute) so no templating is needed.
func SeedDialURLs(s *proto.SeedBundle) []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.Endpoints))
	for _, e := range s.Endpoints {
		if strings.TrimSpace(e) != "" {
			out = append(out, e)
		}
	}
	return out
}
