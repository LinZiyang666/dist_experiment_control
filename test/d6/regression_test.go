package d6_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// D9 cutover (step 7): the build-and-prove production-wiring guard
// (TestD6ProductionWiresNoClusterNode + its self-check + the d6BannedTokens scan) was
// REMOVED — D9 is the authored cutover, so production now legitimately attaches the D6
// home seam + stable tunnel cert (cutover.go / clusterwrite.go). Its proof obligation is
// replaced by the two-mode invariant in test/d9 (TestD9ClusterMode{Off,On}*). The L-2
// import guards below are ORTHOGONAL (clusternodes/cluster layering) and stay verbatim.

// TestD6ClusternodesNoNATSNoCluster (R-3 / L-2): internal/clusternodes is a pure-SQL
// leaf — it must import NEITHER nats.go NOR internal/cluster (raft).
func TestD6ClusternodesNoNATSNoCluster(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/clusternodes")
	for _, banned := range []string{
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		"github.com/LinZiyang666/tether/internal/cluster",
		"github.com/LinZiyang666/tether/internal/broker",
	} {
		if deps[banned] {
			t.Errorf("internal/clusternodes transitively imports %q — L-2 violated (it must stay a pure-SQL leaf)", banned)
		}
	}
	if !deps["database/sql"] {
		t.Fatal("self-check: internal/clusternodes deps must include database/sql (query failed?)")
	}
}

// TestD6ClusterStillNoNATS re-asserts the D5 L-2 boundary survives D6.
func TestD6ClusterStillNoNATS(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/cluster")
	for _, banned := range []string{
		"github.com/nats-io/nats.go",
		"github.com/LinZiyang666/tether/internal/broker",
		"github.com/LinZiyang666/tether/internal/clusternodes",
	} {
		if deps[banned] {
			t.Errorf("internal/cluster transitively imports %q — L-2 violated", banned)
		}
	}
	if !deps["github.com/hashicorp/raft"] {
		t.Fatal("self-check: internal/cluster deps must include hashicorp/raft (query failed?)")
	}
}

func goListDeps(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", pkg)
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps %s: %v", pkg, err)
	}
	deps := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		deps[strings.TrimSpace(line)] = true
	}
	return deps
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
