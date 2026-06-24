package clusteroffline_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

func writeAllSecrets(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range []struct {
		name string
		mode os.FileMode
	}{
		{"cluster-ca.pem", 0o644}, {"route-cert.pem", 0o644}, {"route-key.pem", 0o600},
		{"tunnel-cert.pem", 0o644}, {"tunnel-key.pem", 0o600},
		{"node-ident.nk", 0o600},
	} {
		if err := os.WriteFile(filepath.Join(dir, f.name), []byte("x"), f.mode); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestSecretsPreflightCleanBillIsAdvisoryOnly(t *testing.T) {
	adv, fatal := clusteroffline.SecretsPreflight(writeAllSecrets(t))
	if fatal != nil {
		t.Fatalf("complete 0600 secrets must pass (no fatal), got: %v", fatal)
	}
	// FDE is advisory, never fatal — there is exactly the psk_at_rest_unprotected note.
	if len(adv) == 0 || !strings.Contains(adv[0], "psk_at_rest_unprotected") {
		t.Fatalf("expected an FDE advisory, got %v", adv)
	}
}

func TestSecretsPreflightMissingIsFatal(t *testing.T) {
	dir := writeAllSecrets(t)
	if err := os.Remove(filepath.Join(dir, "node-ident.nk")); err != nil {
		t.Fatal(err)
	}
	_, fatal := clusteroffline.SecretsPreflight(dir)
	if fatal == nil || !strings.Contains(fatal.Error(), "missing") {
		t.Fatalf("a missing secret must be FATAL, got: %v", fatal)
	}
}

func TestSecretsPreflightTooOpenPrivateKeyIsFatal(t *testing.T) {
	dir := writeAllSecrets(t)
	// Loosen a private key to group/world-readable.
	if err := os.Chmod(filepath.Join(dir, "node-ident.nk"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, fatal := clusteroffline.SecretsPreflight(dir)
	if fatal == nil || !strings.Contains(fatal.Error(), "too-permissive") {
		t.Fatalf("a group/world-readable private key must be FATAL, got: %v", fatal)
	}
}

func TestSecretsPreflightEmptyDirIsFatal(t *testing.T) {
	if _, fatal := clusteroffline.SecretsPreflight(""); fatal == nil {
		t.Fatal("empty secrets_dir must be FATAL")
	}
}
