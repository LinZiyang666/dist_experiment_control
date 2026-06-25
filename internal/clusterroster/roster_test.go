package clusterroster

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nkeys"
)

// roster_test.go — B7 DOC#3 verifier core (the load-bearing security): sign/verify round-trip,
// tamper rejection, foreign-account rejection, the self-vouch attack, and the Select ordering.

func mustSeed(t *testing.T) ([]byte, string) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return seed, pub
}

func sampleBrokers() []proto.RosterBroker {
	return []proto.RosterBroker{
		{NodeID: "brk-b", NatsRoute: "nats://b:6222", PublicHost: "b", TunnelAddr: "b:7000", CertFP: "sha256:b", Phase: "VOTER"},
		{NodeID: "brk-a", NatsRoute: "nats://a:6222", PublicHost: "a", TunnelAddr: "a:7000", CertFP: "sha256:a", Phase: "VOTER"},
		{NodeID: "brk-c", NatsRoute: "nats://c:6222", PublicHost: "c", TunnelAddr: "c:7000", CertFP: "sha256:c", Phase: "CATCHING_UP"},
	}
}

func TestRosterSignVerifyRoundTrip(t *testing.T) {
	seed, pub := mustSeed(t)
	r, err := Build(seed, pub, 7, sampleBrokers(), "2026-06-24T00:00:00Z", "")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := Verify(r, pub); err != nil {
		t.Fatalf("Verify must pass for a genuine roster: %v", err)
	}
	// Brokers are sorted by node_id for determinism.
	if r.Brokers[0].NodeID != "brk-a" || r.Brokers[2].NodeID != "brk-c" {
		t.Fatalf("brokers not sorted: %+v", r.Brokers)
	}
}

func TestRosterTamperRejected(t *testing.T) {
	seed, pub := mustSeed(t)
	r, _ := Build(seed, pub, 7, sampleBrokers(), "2026-06-24T00:00:00Z", "")
	// Mutate a broker's route AFTER signing — Verify must fail (the canonical bytes changed).
	r.Brokers[0].NatsRoute = "nats://evil:6222"
	if err := Verify(r, pub); err == nil {
		t.Fatal("a tampered roster must fail verification")
	}
	// Mutate the generation — also must fail.
	r2, _ := Build(seed, pub, 7, sampleBrokers(), "2026-06-24T00:00:00Z", "")
	r2.Generation = 999
	if err := Verify(r2, pub); err == nil {
		t.Fatal("a mutated generation must fail verification")
	}
}

func TestRosterForeignAccountRejected(t *testing.T) {
	seed, pub := mustSeed(t)
	r, _ := Build(seed, pub, 1, sampleBrokers(), "t", "")
	_, otherPub := mustSeed(t)
	if err := Verify(r, otherPub); err != ErrAccountMismatch {
		t.Fatalf("a roster for a different account must be ErrAccountMismatch, got %v", err)
	}
}

func TestRosterSelfVouchAttackRejected(t *testing.T) {
	// An attacker forges a roster CLAIMING victimPub but signs with their OWN key. Verify(victimPub)
	// passes the account_pub equality check but MUST fail the signature check.
	victimSeed, victimPub := mustSeed(t)
	_ = victimSeed
	attackerSeed, _ := mustSeed(t)
	forged, _ := Build(attackerSeed, victimPub, 1, sampleBrokers(), "t", "") // account_pub=victim, signed by attacker
	if err := Verify(forged, victimPub); err == nil {
		t.Fatal("a forged roster signed by a different key must fail verification (no self-vouch)")
	}
}

func TestRosterSelectVotersFirst(t *testing.T) {
	seed, pub := mustSeed(t)
	r, _ := Build(seed, pub, 1, sampleBrokers(), "t", "")
	routes := Select(r)
	if len(routes) != 3 || routes[0] != "nats://a:6222" || routes[1] != "nats://b:6222" || routes[2] != "nats://c:6222" {
		t.Fatalf("Select must list VOTERs (a,b) before non-voters (c): %v", routes)
	}
}

// TestRosterSignsWithAccountSeed — Stage-C m8: production signs with an ACCOUNT seed (SA…), not the
// user seed the other tests use. Round-trip with a real account key.
func TestRosterSignsWithAccountSeed(t *testing.T) {
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Build(seed, pub, 3, sampleBrokers(), "t", "")
	if err != nil {
		t.Fatalf("Build with account seed: %v", err)
	}
	if err := Verify(r, pub); err != nil {
		t.Fatalf("Verify must pass for an account-signed roster: %v", err)
	}
}

// TestRosterVerifyVsVerifyAt — Stage-C m7 / External-review F14: the lenient Verify checks only the
// signature (no freshness), but the hardened VerifyAt (which the deferred consumer MUST use) rejects
// an expired roster and an unknown future schema_version — closing the indefinite-replay seam.
func TestRosterVerifyVsVerifyAt(t *testing.T) {
	seed, pub := mustSeed(t)
	now := time.Date(2026, 6, 25, 0, 0, 0, 0, time.UTC)

	// expired roster: Verify passes (sig-only), VerifyAt rejects on freshness.
	expired, _ := Build(seed, pub, 1, sampleBrokers(), "t", "2000-01-01T00:00:00Z")
	if err := Verify(expired, pub); err != nil {
		t.Fatalf("lenient Verify is sig-only — must still pass an expired roster: %v", err)
	}
	if err := VerifyAt(expired, pub, now); !errors.Is(err, ErrRosterExpired) {
		t.Fatalf("VerifyAt must reject an expired roster, got %v", err)
	}

	// no expiry (current broker output): VerifyAt does NOT reject on freshness.
	fresh, _ := Build(seed, pub, 1, sampleBrokers(), "t", "")
	if err := VerifyAt(fresh, pub, now); err != nil {
		t.Fatalf("VerifyAt must accept a no-expiry roster (the producer hasn't started stamping TTLs): %v", err)
	}

	// a not-yet-expired roster passes.
	future, _ := Build(seed, pub, 1, sampleBrokers(), "t", "2099-01-01T00:00:00Z")
	if err := VerifyAt(future, pub, now); err != nil {
		t.Fatalf("VerifyAt must accept a not-yet-expired roster: %v", err)
	}

	// an unknown FUTURE schema_version is rejected by VerifyAt (re-sign so the sig itself is valid —
	// proving the rejection is the schema gate, not a signature failure).
	resigned, _ := Build(seed, pub, 1, sampleBrokers(), "t", "2099-01-01T00:00:00Z")
	resigned.SchemaVersion = proto.ClusterRosterSchemaVersion + 1
	sig, err := auth.SignWithSeed(seed, CanonicalRosterBytes(resigned))
	if err != nil {
		t.Fatal(err)
	}
	resigned.Sig = hex.EncodeToString(sig)
	if err := VerifyAt(resigned, pub, now); !errors.Is(err, ErrRosterSchema) {
		t.Fatalf("VerifyAt must reject an unknown future schema_version, got %v", err)
	}
}
