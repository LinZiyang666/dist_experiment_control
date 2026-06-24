package d8_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// D9 cutover (step 7): the build-and-prove production-wiring guard
// (TestD8ProductionWiresNoCluster + its self-check + TestD8GuardExclusionsJustified +
// the d8BannedTokens scan) was REMOVED — D9 is the authored cutover, so production now
// legitimately attaches the transfer-audit / xfer-replica / alert sinks and subscribes
// the cluster-health / alert responders (cutover.go / clusterwrite.go). Its proof
// obligation is replaced by the two-mode invariant in test/d9
// (TestD9ClusterMode{Off,On}*). The L-2 import guards below are ORTHOGONAL and stay.

// TestD8ClusterStaysNATSFree: internal/cluster (alert_ops.go + alert_read.go) must
// import only database/sql + std — never the nats.go client (the D5 L-2 line).
func TestD8ClusterStaysNATSFree(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/cluster")
	if deps["github.com/nats-io/nats.go"] {
		t.Error("internal/cluster transitively imports nats.go — L-2 violated (alert store must be NATS-free)")
	}
}

// TestD8JsstreamStaysClusterFree: internal/jsstream (ListXferStreams) must not import
// internal/cluster (the D5 line).
func TestD8JsstreamStaysClusterFree(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/jsstream")
	if deps["github.com/LinZiyang666/tether/internal/cluster"] {
		t.Error("internal/jsstream imports internal/cluster — L-2 violated")
	}
}

// TestD8XferauditIsLeaf: internal/xferaudit imports internal/cluster + internal/schema
// but NEVER nats.go — a pure render/replay leaf keeping schema out of internal/cluster.
func TestD8XferauditIsLeaf(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/xferaudit")
	if deps["github.com/nats-io/nats.go"] {
		t.Error("internal/xferaudit imports nats.go — it must be a pure leaf")
	}
	if !deps["github.com/LinZiyang666/tether/internal/cluster"] {
		t.Fatal("self-check: internal/xferaudit must import internal/cluster (it returns *cluster.Command)")
	}
	if !deps["github.com/LinZiyang666/tether/internal/schema"] {
		t.Fatal("self-check: internal/xferaudit must import internal/schema (it owns the AuditTransfer Aux)")
	}
}

// --- helpers ---

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
