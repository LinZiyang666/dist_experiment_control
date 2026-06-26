package clusterroster

import (
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
)

// mustAccountPub returns a real ACCOUNT keypair (prefix A) so the invite's account_pub passes
// nkeys.IsValidPublicAccountKey (the production account.nk is an account key, not a user key).
func mustAccountPub(t *testing.T) (seed []byte, pub string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	s, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	p, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}

func TestMintParseInviteRoundTrip(t *testing.T) {
	_, pub := mustAccountPub(t)
	in := Invite{Pin: pub, BootstrapURL: "https://c.example.com/.well-known/tether/cluster.json", SID: "lab", Seed: "wss://b1.example.com:443"}
	tok, err := MintInvite(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseInvite(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got != in {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, in)
	}
}

func TestParseInviteRejectsNonAccountPub(t *testing.T) {
	// A user key / garbage pin must be rejected.
	for _, bad := range []string{"", "not-a-key", "UABC123"} {
		tok := "tether-invite:v1?pin=" + bad + "&url=https%3A%2F%2Fx.example.com%2Fc.json&sid=lab"
		if _, err := ParseInvite(tok); err == nil {
			t.Fatalf("pin %q must be rejected as a non-account-key", bad)
		}
	}
}

func TestParseInviteRejectsUnknownParamsAndScheme(t *testing.T) {
	_, pub := mustAccountPub(t)
	base := "tether-invite:v1?pin=" + pub + "&url=https%3A%2F%2Fx.example.com%2Fc.json&sid=lab"
	if _, err := ParseInvite(base + "&evil=1"); err == nil {
		t.Fatal("unknown param must be rejected")
	}
	if _, err := ParseInvite(strings.Replace(base, "tether-invite:v1", "tether-invite:v2", 1)); err == nil {
		t.Fatal("wrong version must be rejected")
	}
	if _, err := ParseInvite(strings.Replace(base, "tether-invite:v1", "https:", 1)); err == nil {
		t.Fatal("wrong scheme must be rejected")
	}
}

func TestParseInviteRejectsNonHTTPSBootstrap(t *testing.T) {
	_, pub := mustAccountPub(t)
	tok := "tether-invite:v1?pin=" + pub + "&url=http%3A%2F%2Fx.example.com%2Fc.json&sid=lab"
	if _, err := ParseInvite(tok); err == nil {
		t.Fatal("a plaintext-http bootstrap url must be rejected without TETHER_DEV_NO_AUTH")
	}
}

// TestInviteHasNoPinOrSeedMaterial is the grep-guard: a minted invite must NEVER contain a session
// PIN field or any seed/private-key material — only the account_pub + public URLs (拒绝表 #2).
func TestInviteHasNoPinOrSeedMaterial(t *testing.T) {
	_, pub := mustAccountPub(t)
	tok, err := MintInvite(Invite{Pin: pub, BootstrapURL: "https://c.example.com/c.json", SID: "lab", Seed: "wss://b1.example.com:443"})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"session_pin", "&secret=", "nkey", "SUAB", "private", "account.nk"} {
		if strings.Contains(strings.ToLower(tok), strings.ToLower(forbidden)) {
			t.Fatalf("invite must not contain %q: %s", forbidden, tok)
		}
	}
	// The only "pin" is the account pubkey query key, never a session credential.
	if strings.Count(tok, "pin=") != 1 {
		t.Fatalf("invite should carry exactly one pin= (the account pubkey): %s", tok)
	}
}
