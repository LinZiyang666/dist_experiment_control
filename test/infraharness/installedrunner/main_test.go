//go:build linux && amd64

package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// These are actual product-entry and custody checks. They cannot certify the
// unavailable multi-UID/systemd/nft installed success scenarios.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func TestInstalledRunnerRefusesMissingAuthority(t *testing.T) {
	for _, scenario := range []string{"authority", "rotation", "release-runtime", "support-cycle", "active-installer"} {
		root := t.TempDir()
		if err := os.Chmod(root, 0700); err != nil {
			t.Fatal(err)
		}
		var diagnostic bytes.Buffer
		rc := run([]string{"--scenario", scenario, "--root", root, "--supply", filepath.Join(root, "missing.json")}, &diagnostic)
		if rc != 2 || !strings.HasPrefix(diagnostic.String(), "SETUP-RED:") {
			t.Fatal("missing installed authority was reported as success", rc)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatal("failed setup published an installed effect or receipt")
		}
	}
}

// origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / hot-update S9 plan §9.5
func TestInstalledEntryWitnessRejectsUnboundBytes(t *testing.T) {
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "go", "build", "-race", "-trimpath", "-o", filepath.Join(root, "helper"), "./cmd/tether-infra-helper")
	cmd.Dir = repo
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build actual helper: %v: %s", err, output)
	}
	cmd = exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", filepath.Join(root, "plain"), "./cmd/tether-infra-helper")
	cmd.Dir = repo
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build plain helper control: %v: %s", err, output)
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseEntryWitness(t, root)
	}
	testharness.AssertNoGoroutineLeak(t, "installed entry custody", g)
	testharness.AssertNoFDLeak(t, "installed entry custody", fd, 0)
}

func exerciseEntryWitness(t *testing.T, root string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "helper"))
	if err != nil {
		t.Fatal(err)
	}
	valid := suppliedFile{"helper", fmt.Sprintf("%x", sha256.Sum256(data))}
	path, err := verifiedEntry(root, valid, "tether-infra-helper")
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"version", "worker", "bootstrap"} {
		check, err := checkEntry(root, path, binary{Name: "tether-infra-helper", Race: valid}, command)
		if err != nil || check.SHA256 != valid.SHA256 || check.OutputSHA256 == "" {
			t.Fatal("actual product entry did not preserve its unprivileged refusal", command, err)
		}
	}
	if _, err := checkEntry(root, path, binary{Name: "tether-infra-helper", Race: valid}, "untrusted-command"); err == nil {
		t.Fatal("entry witness accepted a different native refusal")
	}
	plain, err := os.ReadFile(filepath.Join(root, "plain"))
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(filepath.Join(root, "helper"), alias); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(alias) }()
	for _, row := range []struct {
		file suppliedFile
		name string
	}{
		{suppliedFile{"helper", strings.Repeat("f", 64)}, "tether-infra-helper"},
		{suppliedFile{"alias", valid.SHA256}, "tether-infra-helper"},
		{suppliedFile{"../helper", valid.SHA256}, "tether-infra-helper"},
		{valid, "tether-infra-launch"},
		{suppliedFile{"plain", fmt.Sprintf("%x", sha256.Sum256(plain))}, "tether-infra-helper"},
	} {
		if _, err := verifiedEntry(root, row.file, row.name); err == nil {
			t.Fatal("installed entry witness accepted changed, foreign or uninstrumented bytes", row.file.Path, row.name)
		}
	}
}
