package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureIdentityCreatesAndReuses(t *testing.T) {
	home := t.TempDir()

	id1, err := EnsureIdentity(home)
	if err != nil {
		t.Fatalf("first EnsureIdentity: %v", err)
	}
	if !strings.HasPrefix(id1.PublicKey, "U") || len(id1.PublicKey) != 56 {
		t.Errorf("public key shape unexpected: %q (len=%d)", id1.PublicKey, len(id1.PublicKey))
	}
	if !strings.HasPrefix(id1.Fingerprint, "SHA256:") {
		t.Errorf("fingerprint shape unexpected: %q", id1.Fingerprint)
	}

	keyPath := filepath.Join(home, "keys", "default.nk")
	stat, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file missing: %v", err)
	}
	if mode := stat.Mode().Perm(); mode != 0o600 {
		t.Errorf("key file mode = %o, want 0600", mode)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(keyPath), ".default.nk.tmp-*")); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("identity temp files remain after atomic replace: %v", matches)
	}

	// Second call must reuse the same key.
	id2, err := EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	if id2.PublicKey != id1.PublicKey || id2.Fingerprint != id1.Fingerprint {
		t.Fatal("second EnsureIdentity returned a different key")
	}
}

func TestEnsureIdentityDoesNotFollowPredictableTempSymlink(t *testing.T) {
	home := t.TempDir()
	keyPath := filepath.Join(home, "keys", "default.nk")
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(home, "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, keyPath+".tmp"); err != nil {
		t.Fatal(err)
	}

	if _, err := EnsureIdentity(home); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "keep" {
		t.Fatalf("predictable temp symlink target was overwritten: %q", body)
	}
}

func TestCurrentSessionEnvOverridesFile(t *testing.T) {
	home := t.TempDir()
	if err := WriteCurrentSession(home, "lab"); err != nil {
		t.Fatal(err)
	}
	if got := ReadCurrentSession(home); got != "lab" {
		t.Errorf("file-only read: got %q, want lab", got)
	}

	t.Setenv("TETHER_SESSION", "prod")
	if got := ReadCurrentSession(home); got != "prod" {
		t.Errorf("env override: got %q, want prod", got)
	}

	t.Setenv("TETHER_SESSION", "")
	if got := ReadCurrentSession(home); got != "lab" {
		t.Errorf("env empty falls back to file: got %q, want lab", got)
	}
}

func TestWriteCurrentSessionEmptyClears(t *testing.T) {
	home := t.TempDir()
	_ = WriteCurrentSession(home, "lab")
	if err := WriteCurrentSession(home, ""); err != nil {
		t.Fatal(err)
	}
	if got := ReadCurrentSession(home); got != "" {
		t.Errorf("after clear: got %q, want empty", got)
	}
}
