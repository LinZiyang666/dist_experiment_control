package auth

import (
	"strings"
	"testing"
)

func TestNkeyRoundtrip(t *testing.T) {
	seed, err := GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(seed), "SU") {
		// User nkey seeds are base32-encoded with prefix bytes 'S' (seed) + 'U' (user).
		t.Fatalf("expected user seed to start with 'SU', got %q", seed[:2])
	}

	pub, err := PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(pub, "U") || len(pub) != 56 {
		t.Fatalf("public key shape unexpected: %q (len=%d)", pub, len(pub))
	}

	// Same seed → same public key (deterministic).
	pub2, err := PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	if pub != pub2 {
		t.Fatal("PublicKeyFromSeed must be deterministic")
	}
}

func TestSignVerifyRoundtrip(t *testing.T) {
	seed, err := GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	msg := []byte("nonce-2026-05-08:lab")
	sig, err := SignWithSeed(seed, msg)
	if err != nil {
		t.Fatal(err)
	}
	if len(sig) != 64 {
		t.Fatalf("ed25519 signature is 64 bytes, got %d", len(sig))
	}

	if err := VerifySignature(pub, msg, sig); err != nil {
		t.Fatalf("Verify must succeed for own signature: %v", err)
	}
}

func TestVerifyRejectsTamperedMessage(t *testing.T) {
	seed, _ := GenerateUserSeed()
	pub, _ := PublicKeyFromSeed(seed)
	sig, _ := SignWithSeed(seed, []byte("original"))
	if err := VerifySignature(pub, []byte("modified"), sig); err == nil {
		t.Fatal("Verify must reject signature on tampered message")
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	seedA, _ := GenerateUserSeed()
	_, _ = PublicKeyFromSeed(seedA)
	seedB, _ := GenerateUserSeed()
	pubB, _ := PublicKeyFromSeed(seedB)

	msg := []byte("hello")
	sig, _ := SignWithSeed(seedA, msg)
	if err := VerifySignature(pubB, msg, sig); err == nil {
		t.Fatal("Verify must reject signature with wrong public key")
	}
}

func TestPublicKeyFromBadSeed(t *testing.T) {
	if _, err := PublicKeyFromSeed([]byte("not-a-seed")); err == nil {
		t.Fatal("PublicKeyFromSeed should reject a malformed seed")
	}
}
