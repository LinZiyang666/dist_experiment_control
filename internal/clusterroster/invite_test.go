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

// FuzzParseInvite drives both invite parsers with arbitrary tokens.
//
// An invite is pasted by an operator from wherever it was handed to them — the least trusted input
// in the cluster join path, and a hand-written strict parser (scheme, opaque version, allow-listed
// query keys, account-key pin). Properties: never panic; accepted ⇒ Mint(Parse(x)) parses back to
// the same Invite (the canonical form is a fixpoint); and a foreign query parameter appended to an
// accepted token is rejected (the allow-list is load-bearing — an unknown key is how a future field
// would be smuggled past an older parser).
// origin: docs/reviews/test-system-overhaul-plan.md B2 (infra I7).
func FuzzParseInvite(f *testing.F) {
	// The accept set must not depend on the developer's shell: with TETHER_DEV_NO_AUTH=1 the URL
	// validators admit http/ws, so a crasher found in one environment would not replay in another
	// (test-system overhaul internal review L2-F12).
	f.Setenv(devNoAuthEnv, "")
	kp, err := nkeys.CreateAccount()
	if err != nil {
		f.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		f.Fatal(err)
	}
	good, err := MintInvite(Invite{Pin: pub, BootstrapURL: "https://b.example/.well-known/tether", SID: "lab"})
	if err != nil {
		f.Fatal(err)
	}
	disc, err := MintDiscoveryInvite(Invite{Pin: pub, Seed: "nats://h.example:4222"})
	if err != nil {
		f.Fatal(err)
	}
	for _, s := range []string{
		good, good + "&x=1", disc,
		"tether-invite:v1?pin=" + pub + "&url=https://b.example/x&sid=lab&seed=nats://h.example:4222",
		"tether-invite:v0?pin=" + pub, "tether-invite:v1?pin=UNOTANACCOUNTKEY&url=https://x&sid=lab",
		"tether-invite:v1?pin=" + pub + "&url=http://plain&sid=lab",
		"", "http://x", "tether-invite:", "tether-invite:v1", "tether-invite:v1?",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, token string) {
		if in, err := ParseInvite(token); err == nil {
			re, merr := MintInvite(in)
			if merr != nil {
				t.Fatalf("ParseInvite accepted %q but MintInvite refuses the result: %v", token, merr)
			}
			in2, perr := ParseInvite(re)
			if perr != nil || in2 != in {
				t.Fatalf("invite fixpoint: %q -> %+v -> %q -> %+v (err=%v)", token, in, re, in2, perr)
			}
			// The allow-list is asserted on the CANONICAL token, not the raw one: a raw token may carry
			// a `#fragment` or trailing whitespace that url.Parse tolerates, and "&zz=1" appended after a
			// fragment lands in the fragment, not the query — the first version of this oracle asserted
			// on the raw token and the fuzzer found that in 0.1s (a test-oracle bug, not a parser bug).
			if _, err := ParseInvite(re + "&zz=1"); err == nil {
				t.Fatalf("ParseInvite accepted an unknown parameter appended to canonical %q", re)
			}
		}
		if in, err := ParseDiscoveryInvite(token); err == nil {
			re, merr := MintDiscoveryInvite(in)
			if merr != nil {
				t.Fatalf("ParseDiscoveryInvite accepted %q but MintDiscoveryInvite refuses the result: %v", token, merr)
			}
			in2, perr := ParseDiscoveryInvite(re)
			// REGISTERED ASYMMETRY, found by this fuzzer on its first -fuzz run (2026-09-01, on a SEED):
			// ParseDiscoveryInvite's allow-list accepts `sid=` and fills Invite.SID, but
			// MintDiscoveryInvite never emits sid, so Parse→Mint→Parse drops it. The fixpoint is
			// therefore asserted on {Pin, BootstrapURL, Seed} only. Whether Mint should emit sid (the
			// older parser already allows it, so it would be additive) is a product decision left to
			// review, not something a test batch changes. origin: docs/reviews/test-system-overhaul-plan.md B2.
			want := Invite{Pin: in.Pin, BootstrapURL: in.BootstrapURL, Seed: in.Seed}
			if perr != nil || in2 != want {
				t.Fatalf("discovery invite fixpoint: %q -> %+v -> %q -> %+v (err=%v)", token, in, re, in2, perr)
			}
			if _, err := ParseDiscoveryInvite(re + "&zz=1"); err == nil {
				t.Fatalf("ParseDiscoveryInvite accepted an unknown parameter appended to canonical %q", re)
			}
		}
	})
}
