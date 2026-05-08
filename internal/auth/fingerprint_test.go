package auth

import (
	"strings"
	"testing"
)

func TestFingerprintRoundtrip(t *testing.T) {
	seed, err := GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	fpFromActor, err := FingerprintFromActor(pub)
	if err != nil {
		t.Fatalf("FingerprintFromActor: %v", err)
	}
	fpFromSeed, err := FingerprintFromSeed(seed)
	if err != nil {
		t.Fatalf("FingerprintFromSeed: %v", err)
	}
	if fpFromActor != fpFromSeed {
		t.Fatalf("actor-derived fp %q != seed-derived fp %q", fpFromActor, fpFromSeed)
	}

	if !strings.HasPrefix(fpFromActor, "SHA256:") {
		t.Errorf("fingerprint must start with SHA256:, got %q", fpFromActor)
	}
	// Body must be base64-no-pad of 32 bytes → 43 chars exactly.
	body := strings.TrimPrefix(fpFromActor, "SHA256:")
	if len(body) != 43 {
		t.Errorf("fingerprint body length = %d, want 43", len(body))
	}
	if strings.Contains(body, "=") {
		t.Errorf("fingerprint body must be padding-free, got %q", body)
	}
}

func TestFingerprintRejectsNonUserKey(t *testing.T) {
	if _, err := FingerprintFromActor("not-a-key"); err == nil {
		t.Fatal("expected error for malformed actor")
	}
	if _, err := FingerprintFromActor(""); err == nil {
		t.Fatal("expected error for empty actor")
	}
}

func TestDifferentSeedsDifferentFingerprints(t *testing.T) {
	s1, _ := GenerateUserSeed()
	s2, _ := GenerateUserSeed()
	fp1, _ := FingerprintFromSeed(s1)
	fp2, _ := FingerprintFromSeed(s2)
	if fp1 == fp2 {
		t.Fatal("two distinct seeds yielded identical fingerprints (cosmic ray?)")
	}
}
