package auth

import (
	"strings"
	"testing"
)

func TestPinHashVerifyRoundtrip(t *testing.T) {
	cases := []string{"123456", "p@ssw0rd!", "12-AB-cd-XY", strings.Repeat("a", 32)}
	for _, pin := range cases {
		phc, err := HashPIN(pin)
		if err != nil {
			t.Fatalf("HashPIN(%q): %v", pin, err)
		}
		if !strings.HasPrefix(phc, "$argon2id$v=19$m=65536,t=3,p=2$") {
			t.Fatalf("PHC prefix unexpected: %q", phc)
		}
		if err := VerifyPIN(pin, phc); err != nil {
			t.Errorf("VerifyPIN(%q, ...) failed: %v", pin, err)
		}
	}
}

func TestHashPINProducesDistinctHashes(t *testing.T) {
	// Same PIN, two calls → different PHCs (different salts).
	a, err := HashPIN("pinpin")
	if err != nil {
		t.Fatal(err)
	}
	b, err := HashPIN("pinpin")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("two HashPIN calls of same pin must yield different PHCs (different salts), got identical %q", a)
	}
}

func TestVerifyPINRejectsWrong(t *testing.T) {
	phc, err := HashPIN("right")
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyPIN("wrong", phc); err == nil {
		t.Fatal("VerifyPIN must reject wrong PIN")
	}
}

func TestVerifyPINRejectsTamperedPHC(t *testing.T) {
	phc, err := HashPIN("hello")
	if err != nil {
		t.Fatal(err)
	}
	tampered := []string{
		"",
		"not-a-phc",
		strings.Replace(phc, "argon2id", "argon2i", 1),
		strings.Replace(phc, "v=19", "v=18", 1),
		strings.Replace(phc, "t=3", "t=4", 1),
		strings.Replace(phc, "m=65536", "m=32768", 1),
		strings.ReplaceAll(phc, "$", "@"),
	}
	for _, p := range tampered {
		if err := VerifyPIN("hello", p); err == nil {
			t.Errorf("VerifyPIN should fail on tampered PHC: %q", p)
		}
	}
}

func TestPINCharsetEnforced(t *testing.T) {
	// Empty / non-ASCII are rejected by HashPIN per requirements §6.3.
	bad := []string{"", "中文", "emoji😀", "tab\there", "ctrl\x01"}
	for _, pin := range bad {
		if _, err := HashPIN(pin); err == nil {
			t.Errorf("HashPIN(%q) should be rejected (charset)", pin)
		}
	}
}
