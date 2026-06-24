package d5_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// D9 cutover (step 7): the build-and-prove production-wiring guard
// (TestD5ProductionWiresNoClusterNode + its self-check + the d5BannedTokens scan) was
// REMOVED — D9 is the authored cutover, so production now legitimately constructs the
// AuditPublisher + cluster.Node. Its proof obligation is replaced by the two-mode
// invariant in test/d9 (TestD9ClusterMode{Off,On}*). The L-2 import guards below are
// ORTHOGONAL (raft/jsstream layering) and stay verbatim.

// TestD5ClusterNoNATSImport (E-G2 / L-2 / R-23): internal/cluster must import NO NATS and
// NOT the broker/jsstream packages — raft stays confined; the audit publisher (which needs
// NATS) lives in internal/broker and reads the cluster via the raft-free primitives.
func TestD5ClusterNoNATSImport(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/cluster")
	for _, banned := range []string{
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		"github.com/nats-io/nats-server",
		"github.com/LinZiyang666/tether/internal/broker",
		"github.com/LinZiyang666/tether/internal/jsstream",
	} {
		if deps[banned] {
			t.Errorf("internal/cluster transitively imports %q — L-2 violated (raft must stay nats-free)", banned)
		}
	}
	// Self-check: the dep set is non-empty (the query worked) and includes a known dep.
	if !deps["github.com/hashicorp/raft"] {
		t.Fatal("self-check: internal/cluster deps must include hashicorp/raft (query failed?)")
	}
}

// TestD5JsstreamNoClusterImport (E-G3 / R-23): internal/jsstream must NOT import
// internal/cluster — ReplicasFor takes nVoters as a plain int, keeping the stream layer
// decoupled from raft.
func TestD5JsstreamNoClusterImport(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/jsstream")
	if deps["github.com/LinZiyang666/tether/internal/cluster"] {
		t.Error("internal/jsstream imports internal/cluster — ReplicasFor must stay an int param (R-23)")
	}
	if !deps["github.com/nats-io/nats.go/jetstream"] {
		t.Fatal("self-check: internal/jsstream deps must include nats.go/jetstream (query failed?)")
	}
}

// goListDeps returns the transitive import set of pkg as a set.
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

// moduleRoot walks up to the module root (go.mod).
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
