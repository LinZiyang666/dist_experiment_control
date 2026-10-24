//go:build linux

package hotupdate_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// Re-execute the acceptance tests: missing live supply must be a visible
// SETUP-RED, while an explicit broken supply must still fail its real reader.
// These controls never produce compatibility receipts or execute a release.
// origin: S8 internal review s8-n1-evidence F1-F4
// origin: S10 internal review NO-SKIP-AS-PASS F1
func TestUpgradeCompatibilityRequiresSuppliedEvidence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeCompatibilityEvidenceAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "compatibility evidence admission x5", g)
	testharness.AssertNoFDLeak(t, "compatibility evidence admission x5", fd, 0)
}

func exerciseUpgradeCompatibilityEvidenceAdmission(t *testing.T) {
	t.Helper()
	for _, test := range []struct {
		name, flag, receipt, pending string
	}{
		{"TestUpgradeWireEightQuadrants", "upgrade-compatibility-evidence", "wire-supply.json", "S8 P22/P24/P26"},
		{"TestUpgradeConfigurationParsersCurrentAndPrevious", "upgrade-reader-catalog", "catalog.json", "S8 P22/P24/P26"},
		{"TestUpgradeGoCheckpointDecoderContinuation", "upgrade-reader-catalog", "catalog.json", "S7 P28/P29 and S8 P22/P24/P26"},
		{"TestLegacyIssuerRouteChange", "upgrade-compatibility-evidence", "round-0/mixed-consumer.json", "S8 P13/P15/P19/P24/P26"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, supply := range []string{"absent", "missing", "malformed"} {
				t.Run(supply, func(t *testing.T) {
					args := []string{"-test.run=^" + test.name + "$", "-test.v", "-test.count=1"}
					if supply != "absent" {
						root := t.TempDir()
						path := filepath.Join(root, test.receipt)
						if supply == "malformed" {
							if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
								t.Fatal(err)
							}
						}
						if test.flag == "upgrade-reader-catalog" {
							root = path
						}
						args = append(args, "-"+test.flag+"="+root)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, os.Args[0], args...)
					cmd.Env = hotUpdateEnvironment(t)
					out, err := testsupervisor.CombinedOutput(cmd)
					if supply == "absent" {
						if err == nil || !bytes.Contains(out, []byte("SETUP-RED: NOT_RUN pending "+test.pending+":")) || !bytes.Contains(out, []byte("--- FAIL: "+test.name+" ")) || bytes.Contains(out, []byte("--- PASS:")) || bytes.Contains(out, []byte("--- SKIP:")) {
							t.Fatalf("absent compatibility supply must fail SETUP-RED with its exact owner: %v\n%s", err, out)
						}
						return
					}
					want := "no such file or directory"
					if supply == "malformed" {
						want = "unexpected EOF"
					}
					if err == nil || !bytes.Contains(out, []byte("--- FAIL: "+test.name+" ")) || !bytes.Contains(out, []byte(want)) || bytes.Contains(out, []byte("--- SKIP:")) || bytes.Contains(out, []byte("--- PASS:")) {
						t.Fatalf("explicit broken compatibility supply must fail its reader: %v\n%s", err, out)
					}
				})
			}
		})
	}
}
