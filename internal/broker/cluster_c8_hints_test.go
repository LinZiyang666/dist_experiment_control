package broker

import (
	"os"
	"strings"
	"testing"
)

// cluster_c8_hints_test.go (C8 review M2) — the LIVE operator-hint strings that `cluster ops` /
// `alert ls` / the RemoveNode error render to operators must name post-C8 commands. A source scan over
// the reachable hint sites guards against re-introducing a deleted/erroring verb (`cluster add`,
// `cluster remove … --force` without `--manual`, `drain … --retire`). The D9-accepted latent
// handleAdd/OpClusterAdd version-skew path is CLI-unreachable and intentionally excluded.
func TestC8BrokerHintsNameLiveVerbs(t *testing.T) {
	// (file, forbidden-substring) pairs on the reachable resume/alert/error strings.
	for _, f := range []string{"clusterops.go", "alert_reconcile.go", "clusterdrain.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		// Scan only the lines that build an operator-facing string (Resume / message / Errorf / Sprintf),
		// so we don't trip on the latent handleAdd code or comments naming the old phases.
		for i, line := range strings.Split(string(src), "\n") {
			isHint := strings.Contains(line, "e.Resume =") || strings.Contains(line, "message:") ||
				strings.Contains(line, "Errorf(") || strings.Contains(line, "Sprintf(")
			if !isHint {
				continue
			}
			for _, bad := range []string{"`tether cluster add`", "`cluster add`", "cluster add ", "cluster drain %s --retire", "`--retire`", "cluster remove " + "%s --force"} {
				if strings.Contains(line, bad) {
					t.Errorf("%s:%d operator hint names a deleted/erroring verb %q:\n  %s", f, i+1, bad, strings.TrimSpace(line))
				}
			}
			// `cluster remove <id> --force` without --manual is now a hard error — the hint must say --manual.
			if strings.Contains(line, "cluster remove ") && strings.Contains(line, "--force") && !strings.Contains(line, "--manual") && !strings.Contains(line, "recovery node remove") {
				t.Errorf("%s:%d hint uses raw `cluster remove … --force` without --manual:\n  %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}
