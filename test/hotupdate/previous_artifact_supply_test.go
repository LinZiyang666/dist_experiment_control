//go:build linux

package hotupdate_test

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// A standalone test has the same genuine release prerequisite as a drill.
// Use P22's archive/build/cache verifier rather than relabeling the candidate
// or inventing a second provenance format. The caller still verifies the
// receipt, archive and embedded buildinfo before executing the old program.
// origin: hot-update S8 plan §7.3; S8 internal review upgrade-maketest-2 F1
func supplyUpgradePrevious(t *testing.T) string {
	t.Helper()
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	script := `
set -eu
SIM_REPO=$1
SIM_VENDOR=$2/vendor
. "$SIM_REPO/test/simcluster/lib/supervisor.sh"
sim_supervisor_provision "$2"
. "$SIM_REPO/test/simcluster/lib/stage.sh"
sim_stage_prev
`
	cmd := exec.CommandContext(t.Context(), "sh", "-c", script, "previous-supply", repo, root)
	if out, err := testsupervisor.CombinedOutput(cmd); err != nil {
		t.Fatalf("genuine v0.6.0 supply failed: %v\n%s", err, out)
	}
	return filepath.Join(root, "vendor", "tether-prev")
}
