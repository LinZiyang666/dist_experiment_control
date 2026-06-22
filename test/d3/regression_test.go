package d3_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestD3ProductionWiresNoClusterNode locks D3-R1 as a standing invariant: the
// live broker is NOT cut over to a clustered FSM in D3 (cutover is D9). The
// production wiring must construct no cluster.Node and must not set the
// auth_callout fail-closed / PIN-write seams — so production behavior is
// byte-identical to today (the seams default to no-op when nil).
//
// A source scan is deliberate: it fails loudly the moment a future change wires
// the cluster into production ahead of D9, instead of relying on "some test still
// passes".
func TestD3ProductionWiresNoClusterNode(t *testing.T) {
	root := moduleRoot(t)
	for _, rel := range []string{
		"cmd/tether/serve.go",
		"internal/broker/authcallout.go",
		"internal/broker/broker.go",
	} {
		src := readFile(t, filepath.Join(root, rel))
		for _, banned := range []string{
			"cluster.New(",         // no cluster.Node constructed in production
			"LeaderContactStale:",  // no fail-closed seam wired
			"ProvisionAgentWrite:", // no PIN-write seam wired
			"JoinMemberWrite:",     // no PIN-write seam wired
		} {
			if strings.Contains(src, banned) {
				t.Errorf("%s wires %q — D3 must NOT cut production over to the cluster FSM (that is D9)", rel, banned)
			}
		}
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not locate module root (go.mod) above the test dir")
	return ""
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
