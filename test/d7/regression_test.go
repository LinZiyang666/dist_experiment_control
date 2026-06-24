package d7_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// D9 cutover (step 7): the build-and-prove production-wiring guard
// (TestD7ProductionWiresNoCluster + its self-check + d7BannedTokens/d7ExcludedFiles) was
// REMOVED — D9 is the authored cutover, so production now legitimately constructs the
// ClusterAdmin orchestrator + registers the adminsock cluster backend (broker.go /
// cutover.go). Its proof obligation is replaced by the two-mode invariant in test/d9
// (TestD9ClusterMode{Off,On}*). The L-2 import guards below are ORTHOGONAL (raft/auth/
// clusternodes layering) and stay verbatim — note TestD7RaftConfinedToCluster still
// asserts internal/broker never DIRECTLY imports hashicorp/raft (cutover.go imports
// internal/cluster, not raft; the mTLS transport + Node live in cluster.NewProduction).

// TestD7ClusternodesStaysLeaf: internal/clusternodes (the pure-SQL home leaf) must
// still import NEITHER nats.go NOR internal/cluster, even though D7 adds membership.
func TestD7ClusternodesStaysLeaf(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/clusternodes")
	for _, banned := range []string{
		"github.com/nats-io/nats.go",
		"github.com/LinZiyang666/tether/internal/cluster",
		"github.com/LinZiyang666/tether/internal/broker",
	} {
		if deps[banned] {
			t.Errorf("internal/clusternodes transitively imports %q — L-2 violated", banned)
		}
	}
}

// TestD7ClusterStaysNATSFreeAfterAuthEdge: internal/cluster imports internal/auth (the
// join-PoP verify); auth pulls in nats-io/nkeys (crypto), NOT the nats.go client.
func TestD7ClusterStaysNATSFreeAfterAuthEdge(t *testing.T) {
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
	if !deps["github.com/LinZiyang666/tether/internal/auth"] {
		t.Fatal("self-check: internal/cluster must import internal/auth (the join-PoP verify edge)")
	}
	if !deps["github.com/hashicorp/raft"] {
		t.Fatal("self-check: internal/cluster must import hashicorp/raft")
	}
}

// TestD7RaftConfinedToCluster: internal/clusteroffline and internal/broker must NOT
// DIRECTLY import hashicorp/raft — raft stays confined to internal/cluster (the offline
// tool goes through cluster.RecoverSingleNode; the daemon through cluster.NewProduction).
func TestD7RaftConfinedToCluster(t *testing.T) {
	for _, pkg := range []string{
		"github.com/LinZiyang666/tether/internal/clusteroffline",
		"github.com/LinZiyang666/tether/internal/broker",
	} {
		for _, imp := range goListDirectImports(t, pkg) {
			if imp == "github.com/hashicorp/raft" {
				t.Errorf("%s DIRECTLY imports hashicorp/raft — L-2 confines raft to internal/cluster", pkg)
			}
		}
	}
}

// --- helpers (also used by external_review_test.go) ---

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

func goListDirectImports(t *testing.T, pkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "list", "-f", "{{range .Imports}}{{println .}}{{end}}", pkg)
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list imports %s: %v", pkg, err)
	}
	var imps []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l := strings.TrimSpace(line); l != "" {
			imps = append(imps, l)
		}
	}
	return imps
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
