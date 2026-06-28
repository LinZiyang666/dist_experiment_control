package clusterroster

import (
	"testing"
)

func TestDiscoveryInviteRoundTrip(t *testing.T) {
	_, pub := mustAccountPub(t)
	tok, err := MintDiscoveryInvite(Invite{Pin: pub, BootstrapURL: "https://c.example/m.json", Seed: "wss://b:443"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	got, err := ParseDiscoveryInvite(tok)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Pin != pub || got.Seed != "wss://b:443" || got.BootstrapURL != "https://c.example/m.json" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestDiscoveryInviteSeedOnly(t *testing.T) {
	_, pub := mustAccountPub(t)
	tok, err := MintDiscoveryInvite(Invite{Pin: pub, Seed: "wss://b:443"})
	if err != nil {
		t.Fatalf("seed-only mint must succeed: %v", err)
	}
	got, err := ParseDiscoveryInvite(tok)
	if err != nil || got.Seed != "wss://b:443" || got.BootstrapURL != "" {
		t.Fatalf("seed-only parse: %+v err=%v", got, err)
	}
}

func TestDiscoveryInviteRequiresEndpoint(t *testing.T) {
	_, pub := mustAccountPub(t)
	if _, err := MintDiscoveryInvite(Invite{Pin: pub}); err == nil {
		t.Fatal("a discovery invite with neither bootstrap nor seed must be refused")
	}
}

func TestDiscoveryInviteRejectsNonAccountPin(t *testing.T) {
	if _, err := MintDiscoveryInvite(Invite{Pin: "not-an-account-key", Seed: "wss://b:443"}); err == nil {
		t.Fatal("non-account pin must be refused")
	}
	// parse side too (forged token)
	if _, err := ParseDiscoveryInvite("tether-invite:v1?pin=bogus&seed=wss://b:443"); err == nil {
		t.Fatal("parse must reject a non-account pin")
	}
}

func TestDiscoveryInviteRejectsBadSeedScheme(t *testing.T) {
	_, pub := mustAccountPub(t)
	if _, err := MintDiscoveryInvite(Invite{Pin: pub, Seed: "http://evil:80"}); err == nil {
		t.Fatal("seed scheme must be nats/tls/wss")
	}
}

// A SID-less discovery invite must NOT parse as an agent-join token (and vice-versa stays intact): the two
// token roles are kept separable so a ctl discovery invite can never be mistaken for a session-carrying
// agent-join token.
func TestDiscoveryInviteNotAcceptedByAgentJoinParser(t *testing.T) {
	_, pub := mustAccountPub(t)
	tok, err := MintDiscoveryInvite(Invite{Pin: pub, Seed: "wss://b:443"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if _, err := ParseInvite(tok); err == nil {
		t.Fatal("a SID-less discovery invite must be rejected by the agent-join ParseInvite (sid required)")
	}
}
