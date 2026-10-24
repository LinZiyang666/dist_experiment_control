//go:build linux || darwin

package cli_e2e_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Both the exporting and importing images must match the release profile:
// instrumentation in the exporting image also consumes the freeze window.
// origin: hot-update S10 lead decision D-A
func buildReleaseTether(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "tether-release")
	testharness.BuildReleaseCandidate(t, root, bin, "v0.0.0-dev", os.Getenv("GOFLAGS"))
	return bin
}
