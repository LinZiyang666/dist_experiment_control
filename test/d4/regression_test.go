package d4_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestD4ProductionWiresNoClusterNode extends the D3 standing guard with the D4
// forwarding surface: D4 is build-and-prove (cutover=D9), so the production wiring
// must construct no cluster.Node, set no auth_callout seam, AND wire no forwarder /
// cluster.apply responder / PIN-forward seam. A source scan fails loudly the moment a
// future change cuts the cluster into production ahead of D9.
func TestD4ProductionWiresNoClusterNode(t *testing.T) {
	root := moduleRoot(t)
	for _, rel := range []string{
		"cmd/tether/serve.go",
		"internal/broker/authcallout.go",
		"internal/broker/broker.go",
	} {
		src := readFile(t, filepath.Join(root, rel))
		for _, banned := range d4BannedTokens {
			if strings.Contains(src, banned) {
				t.Errorf("%s wires %q — D4 must NOT cut production over to the cluster FSM / forwarding (that is D9)", rel, banned)
			}
		}
	}
}

// d4BannedTokens is the SSOT banned-token set the production-wiring guard scans for
// (shared with the self-check so they cannot drift).
var d4BannedTokens = []string{
	"cluster.New(", "LeaderContactStale:", "ProvisionAgentWrite:", "JoinMemberWrite:",
	"SubscribeClusterApply(", "NewForwarder(", "NewProvisionSeam(", "NewJoinSeam(",
}

// TestD4ProductionGuardSelfCheck (review m7) proves the guard's detection logic has
// discriminating power (mirrors the repo's TestRaftConfinementSelfCheck precedent): a
// synthetic source containing a banned token is flagged; a clean source is not.
func TestD4ProductionGuardSelfCheck(t *testing.T) {
	flagged := func(src string) bool {
		for _, b := range d4BannedTokens {
			if strings.Contains(src, b) {
				return true
			}
		}
		return false
	}
	if !flagged("x := cluster.New(cfg)") {
		t.Error("self-check: a banned token must be detected")
	}
	if flagged("// production wires no cluster node here\nb.Run(ctx)") {
		t.Error("self-check: a clean source must NOT be flagged")
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
