package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.5 — the receipt
// is per session AND per key: a rotated key does not inherit the previous
// key's standing, and the unactivated connection name carries no session.
func TestAuthReceiptIsPerSessionAndPerKey(t *testing.T) {
	home := t.TempDir()
	if HasAuthReceipt(home, "lab", "SHA256:a") {
		t.Fatal("fresh home has a receipt")
	}
	if err := WriteAuthReceipt(home, "lab", "SHA256:a"); err != nil {
		t.Fatal(err)
	}
	if !HasAuthReceipt(home, "lab", "SHA256:a") || HasAuthReceipt(home, "lab", "SHA256:b") || HasAuthReceipt(home, "other", "SHA256:a") {
		t.Fatal("receipt scope wrong")
	}
	info, err := os.Stat(AuthReceiptPath(home, "lab"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("receipt perms: %v %v", info, err)
	}
	if dir, err := os.Stat(filepath.Dir(AuthReceiptPath(home, "lab"))); err != nil || dir.Mode().Perm() != 0o700 {
		t.Fatalf("receipt dir perms: %v %v", dir, err)
	}
	// Rotating the key replaces the receipt; idempotent rewrite is a no-op.
	if err := WriteAuthReceipt(home, "lab", "SHA256:b"); err != nil {
		t.Fatal(err)
	}
	if HasAuthReceipt(home, "lab", "SHA256:a") || !HasAuthReceipt(home, "lab", "SHA256:b") {
		t.Fatal("rotation did not replace the receipt")
	}
	if err := WriteAuthReceipt("", "lab", "SHA256:b"); err == nil {
		t.Fatal("empty home accepted")
	}
	for name, want := range map[string]string{"tether-cli:lab": "lab", "tether-cli": "", "tether-cli:": "", "tether-agent:lab:n": "", "": ""} {
		sid, ok := SessionFromCtlName(name)
		if sid != want || ok != (want != "") {
			t.Fatalf("SessionFromCtlName(%q) = %q,%v", name, sid, ok)
		}
	}
}
