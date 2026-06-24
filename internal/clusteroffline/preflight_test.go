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
		{"node-ident.nk", 0o600}, {"broker.nk", 0o600}, {"account.nk", 0o600},
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

func TestSecretsPreflightAuthSeedsAreRequiredPrivateKeys(t *testing.T) {
	dir := writeAllSecrets(t)
	if err := os.Chmod(filepath.Join(dir, "account.nk"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, fatal := clusteroffline.SecretsPreflight(dir)
	if fatal == nil || !strings.Contains(fatal.Error(), "account.nk") || !strings.Contains(fatal.Error(), "too-permissive") {
		t.Fatalf("account.nk must be checked as private auth seed, got: %v", fatal)
	}
}

func TestSecretsPreflightCanSkipAuthSeedsForExplicitAuthDir(t *testing.T) {
	dir := writeAllSecrets(t)
	for _, name := range []string{"broker.nk", "account.nk"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	_, fatal := clusteroffline.SecretsPreflightWithOptions(dir, clusteroffline.SecretsPreflightOptions{
		RequireAuthSeeds: false,
	})
	if fatal != nil {
		t.Fatalf("explicit auth seed dir path should not require broker/account in secrets_dir, got: %v", fatal)
	}
}

func TestSecretsPreflightRejectsSymlinkSecret(t *testing.T) {
	dir := writeAllSecrets(t)
	if err := os.Remove(filepath.Join(dir, "node-ident.nk")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "broker.nk"), filepath.Join(dir, "node-ident.nk")); err != nil {
		t.Fatal(err)
	}
	_, fatal := clusteroffline.SecretsPreflight(dir)
	if fatal == nil || !strings.Contains(fatal.Error(), "not regular") || !strings.Contains(fatal.Error(), "node-ident.nk") {
		t.Fatalf("symlinked private key must be FATAL non-regular, got: %v", fatal)
	}
}

func TestSecretsPreflightEmptyDirIsFatal(t *testing.T) {
	if _, fatal := clusteroffline.SecretsPreflight(""); fatal == nil {
		t.Fatal("empty secrets_dir must be FATAL")
	}
}
