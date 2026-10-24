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
)

// The default hermetic run has no P26 live receipt supplier. Its result must
// fail SETUP-RED, while an explicitly supplied, broken receipt remains
// a failure. Re-exec the real test so a log line alone cannot fake either state.
// origin: S7 internal review TRANSFER-MAKETEST-2 F4; hot-update S8 plan §6.2 / P24 / P26
// origin: S10 internal review NO-SKIP-AS-PASS F2
func TestAuthorityParallelTerminationRequiresSuppliedEvidence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAuthorityEvidenceAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "authority evidence admission", g)
	testharness.AssertNoFDLeak(t, "authority evidence admission", fd, 0)
}

func exerciseAuthorityEvidenceAdmission(t *testing.T) {
	t.Helper()
	for _, supply := range []string{"absent", "missing", "malformed"} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		args := []string{"-test.run=^TestAuthorityParallelFlowTransferTermination$", "-test.v", "-test.count=1"}
		if supply != "absent" {
			root := t.TempDir()
			if supply == "malformed" {
				if err := os.MkdirAll(filepath.Join(root, "round-0"), 0o700); err != nil {
					cancel()
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "round-0", "authority-parallel.json"), []byte("{"), 0o600); err != nil {
					cancel()
					t.Fatal(err)
				}
			}
			args = append(args, "-upgrade-compatibility-evidence="+root)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], args...)
		cmd.Env = hotUpdateEnvironment(t)
		out, err := testharness.CombinedOutput(cmd)
		cancel()
		if supply != "absent" {
			want := "no such file or directory"
			if supply == "malformed" {
				want = "unexpected EOF"
			}
			if err == nil || !bytes.Contains(out, []byte(want)) || !bytes.Contains(out, []byte("--- FAIL: TestAuthorityParallelFlowTransferTermination ")) || bytes.Contains(out, []byte("--- SKIP:")) || bytes.Contains(out, []byte("--- PASS:")) {
				t.Fatalf("supplied %s evidence did not fail its reader: %v\n%s", supply, err, out)
			}
		} else if err == nil || !bytes.Contains(out, []byte("SETUP-RED: NOT_RUN pending P26:")) || !bytes.Contains(out, []byte("--- FAIL: TestAuthorityParallelFlowTransferTermination ")) || bytes.Contains(out, []byte("--- PASS:")) || bytes.Contains(out, []byte("--- SKIP:")) {
			t.Fatalf("absent live supplier did not fail SETUP-RED with its exact owner: %v\n%s", err, out)
		}
	}
}
