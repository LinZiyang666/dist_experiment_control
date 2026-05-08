package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// freshAccountSeed creates a brand-new account nkey and returns its seed.
// nkeys' Seed() aliases the keypair's internal buffer and Wipe() overwrites
// it, so we copy before wiping.
func freshAccountSeed(t *testing.T) []byte {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		kp.Wipe()
		t.Fatalf("Seed: %v", err)
	}
	out := append([]byte(nil), seed...)
	kp.Wipe()
	return out
}

// freshUserPub creates a brand-new user nkey and returns just its public key.
// PublicKey returns a string (independent of kp internal state), so Wipe is safe.
func freshUserPub(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	defer kp.Wipe()
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatalf("PublicKey: %v", err)
	}
	return pub
}

func TestIssueAndDecodeUserJWT(t *testing.T) {
	signer, err := LoadAccountSigner(freshAccountSeed(t))
	if err != nil {
		t.Fatal(err)
	}
	userPub := freshUserPub(t)

	pubAllow := "tether.v1.ctrl.by." + userPub + ".session.create.req"
	perms := jwt.Permissions{
		Pub: jwt.Permission{Allow: []string{pubAllow}},
		Sub: jwt.Permission{Allow: []string{"_INBOX.>"}},
	}

	token, err := signer.IssueUserJWT(userPub, perms, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, "eyJ") {
		t.Fatalf("expected JWT compact form starting with 'eyJ', got %q", token[:5])
	}

	uc, err := DecodeUserJWT(token)
	if err != nil {
		t.Fatalf("DecodeUserJWT: %v", err)
	}

	if uc.Subject != userPub {
		t.Errorf("subject: got %q want %q", uc.Subject, userPub)
	}

	wantIssuer, err := signer.AccountPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if uc.Issuer != wantIssuer {
		t.Errorf("issuer: got %q want %q", uc.Issuer, wantIssuer)
	}

	if got := uc.Permissions.Pub.Allow; len(got) != 1 || got[0] != pubAllow {
		t.Errorf("pub.allow: got %v want [%q]", got, pubAllow)
	}
	if got := uc.Permissions.Sub.Allow; len(got) != 1 || got[0] != "_INBOX.>" {
		t.Errorf("sub.allow: got %v want [_INBOX.>]", got)
	}
}

func TestDecodeRejectsTamperedJWT(t *testing.T) {
	signer, err := LoadAccountSigner(freshAccountSeed(t))
	if err != nil {
		t.Fatal(err)
	}
	userPub := freshUserPub(t)
	token, err := signer.IssueUserJWT(userPub, jwt.Permissions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a character inside the payload segment of the JWS (3 dot-separated parts).
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3-part JWS, got %d", len(parts))
	}
	bad := []byte(parts[1])
	if bad[0] == 'A' {
		bad[0] = 'B'
	} else {
		bad[0] = 'A'
	}
	tampered := parts[0] + "." + string(bad) + "." + parts[2]
	if _, err := DecodeUserJWT(tampered); err == nil {
		t.Fatal("DecodeUserJWT must reject a JWT with tampered payload")
	}
}

func TestLoadAccountSignerRejectsBadSeed(t *testing.T) {
	if _, err := LoadAccountSigner([]byte("garbage")); err == nil {
		t.Fatal("LoadAccountSigner must reject malformed seed")
	}
}

func TestLoadAccountSignerAcceptsAccountSeed(t *testing.T) {
	seed := freshAccountSeed(t)
	signer, err := LoadAccountSigner(seed)
	if err != nil {
		t.Fatalf("LoadAccountSigner with account seed: %v", err)
	}
	pub, err := signer.AccountPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(pub, "A") {
		t.Fatalf("account public key must start with 'A', got %q", pub)
	}
}

func TestLoadAccountSignerRejectsUserSeed(t *testing.T) {
	seed, err := GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAccountSigner(seed); err == nil {
		t.Fatal("LoadAccountSigner must reject a user seed (would silently produce wrong-kind issuer)")
	}
}

// IssueUserJWT must reject malformed user public keys with an ordinary error,
// not a nil-deref panic. jwt.NewUserClaims("") returns nil and would crash
// at the first field write (round-2 review F1).
func TestIssueUserJWTRejectsBadUserPub(t *testing.T) {
	signer, err := LoadAccountSigner(freshAccountSeed(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("IssueUserJWT must not panic; recovered %v", r)
		}
	}()

	bad := []string{
		"",                                    // empty
		"hello",                               // garbage
		strings.Repeat("U", 56),               // looks like a user pub but invalid checksum
		"A" + strings.Repeat("A", 55),         // account pub, not user
	}
	for _, p := range bad {
		if _, err := signer.IssueUserJWT(p, jwt.Permissions{}, 0); err == nil {
			t.Errorf("IssueUserJWT(%q) must return an error, got nil", p)
		}
	}
}
