//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real historical binaries are supplied outside go test by S1/P22. This test
// never builds old source and cannot turn missing previous support into N-1.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func TestInfraPreviousImageInputsKeepReleaseProvenance(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	previous := os.Getenv("S9_PREVIOUS_VENDOR")
	if previous == "" {
		t.Fatal("SETUP-RED pending S1/P22: S9_PREVIOUS_VENDOR must name genuine prebuilt release inputs")
	}
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", filepath.Join(repo, "test/infraharness/release_inputs_controls.py"), repo, previous, t.TempDir())
	output, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("historical producer: %v: %s", err, output)
	}
	if !bytes.Contains(output, []byte("historical custody controls passed")) {
		t.Fatalf("historical producer did not close its controls: %s", output)
	}
}
