package clusterroster

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

func TestSeedBundleBuildVerify(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	now := time.Unix(1700000000, 0).UTC()
	s, err := BuildSeeds(seed, pub, 7, []string{"wss://b1:443", "wss://b2:443"}, "https://c/c.json",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySeedsAt(s, pub, now.Add(time.Minute)); err != nil {
		t.Fatalf("fresh seed bundle must verify: %v", err)
	}
	// foreign account pin → reject
	otherSeed, _ := auth.GenerateUserSeed()
	otherPub, _ := auth.PublicKeyFromSeed(otherSeed)
	if err := VerifySeedsAt(s, otherPub, now); err == nil {
		t.Fatal("foreign-account pin must reject")
	}
	// tampered endpoints → sig fail
	s2 := *s
	s2.Endpoints = []string{"wss://evil:443"}
	if err := VerifySeedsAt(&s2, pub, now); err == nil {
		t.Fatal("tampered endpoints must reject (sig)")
	}
	// expired
	if err := VerifySeedsAt(s, pub, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired bundle must reject")
	}
	// future schema (validly re-signed) → schema reject, not a sig reject
	fut := &proto.SeedBundle{
		SchemaVersion: proto.SeedBundleSchemaVersion + 1, Generation: 7, AccountPub: pub,
		Endpoints: s.Endpoints, BootstrapURL: s.BootstrapURL, IssuedAt: s.IssuedAt, ExpiresAt: s.ExpiresAt,
	}
	sig, _ := auth.SignWithSeed(seed, CanonicalSeedBytes(fut))
	fut.Sig = hex.EncodeToString(sig)
	if err := VerifySeedsAt(fut, pub, now); err == nil {
		t.Fatal("future schema must reject")
	}
}

// TestCanonicalRosterBytesStable pins the EXACT roster canonical bytes: C2 must not touch the roster
// canon (signature byte-stability vs shipped v0.4.0 brokers — a change would break rolling upgrade).
func TestCanonicalRosterBytesStable(t *testing.T) {
	r := &proto.ClusterRoster{
		SchemaVersion: 1, Generation: 42, AccountPub: "ACC",
		Brokers:  []proto.RosterBroker{{NodeID: "b1", NatsRoute: "nats://10.0.0.1:6222", PublicHost: "b1", TunnelAddr: "t", CertFP: "fp", Phase: "VOTER"}},
		IssuedAt: "2026-01-01T00:00:00Z", ExpiresAt: "2026-01-02T00:00:00Z",
	}
	got := string(CanonicalRosterBytes(r))
	want := "tether.roster.v1\x0042\x00ACC\x002026-01-01T00:00:00Z\x002026-01-02T00:00:00Z\x00b1\x00nats://10.0.0.1:6222\x00b1\x00t\x00fp\x00VOTER"
	if got != want {
		t.Fatalf("roster canon changed (would break v0.4.0 sig compat):\n got: %q\nwant: %q", got, want)
	}
}
