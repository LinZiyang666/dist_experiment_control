//go:build linux

package cli_e2e_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S10 lead decision D-A
func buildReleaseUpgradeArtifact(t *testing.T, version string) upgradeArtifact {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "tether-release")
	testharness.BuildReleaseCandidate(t, root, bin, version, os.Getenv("GOFLAGS"))
	body, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	return packUpgradeArtifact(t, body)
}
