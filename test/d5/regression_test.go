package d5_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// d5BannedTokens is the SSOT set the build-and-prove production-wiring guard scans for
// (E-G1 / R-23): D5 is build-and-prove (cutover=D9), so the production wiring must
// construct no cluster.Node, start no audit publisher loop, and call none of the D5
// committed-log readers / reconfig primitives. The live publishAudit is byte-unchanged
// because D5 publishes via a SEPARATE AuditPublisher (never wired here).
//
// Internal review M4: the token is the EXPORTED type name "AuditPublisher" (NOT the stale
// lowercase sketch name) so it catches the constructor `NewAuditPublisher(`, a struct
// literal `&AuditPublisher{...}`, AND a `var x AuditPublisher` — closing the
// struct-literal cutover path that a constructor-only ban missed. `.Run(` is deliberately
// NOT banned (the production Broker already has a Run method).
var d5BannedTokens = []string{
	"cluster.New(",
	"AuditPublisher",       // type name: catches NewAuditPublisher(, &AuditPublisher{, var _ AuditPublisher
	"AuditPublisherConfig", // the config literal of a struct-literal cutover
	"AdvanceAuditPublished(",
	"AuditPublishedIndex(",
	"CommittedCommandAt(",
	".CommitIndex(",
	".NumVoters(",
	// NOTE: XferReplicaState is NOT banned — it is a D5 reconfig HELPER DEFINED in the
	// production file transfer.go (the widened scan would flag its own definition). It is
	// inert in production (only the test-constructed AuditPublisher's XferState field calls
	// it; ensureXferBucket reaches its raiseXferReplicas sibling with target=1 = no-op). The
	// real cutover indicator is constructing the publisher, caught by "AuditPublisher".
}

// productionBrokerFiles returns every production .go file in internal/broker EXCEPT the D5
// publisher implementation itself and any _test.go — widened from the original 3-file set
// (internal review M4) so a cutover authored in run.go/sessions.go/reconcile.go/etc. cannot
// hide from the scan.
func productionBrokerFiles(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "internal/broker"))
	if err != nil {
		t.Fatalf("read internal/broker: %v", err)
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") || n == "audit_publisher.go" {
			continue
		}
		// D7 build-and-prove mechanism files: they legitimately consume the D5
		// raft-free read primitives (NumVoters/CommitIndex/AppliedIndex) to build the
		// cluster status report + drain projection — like audit_publisher.go consumes
		// the publisher primitives. Production wiring (serve.go) is still scanned.
		if n == "clusteradmin.go" || n == "clusterdrain.go" || n == "clusterstatus.go" {
			continue
		}
		// D8 build-and-prove mechanism files: they consume the D5 read primitives
		// (NumVoters / ReplicaReport / ObserveReplicas) to drive the alert reconcile loop
		// + tier-B replica seam + cluster-health responder. Production (serve.go) is scanned.
		if n == "transfer_home.go" || n == "transfer_audit_forward.go" ||
			n == "alert_reconcile.go" || n == "alert_forward.go" || n == "cluster_health.go" {
			continue
		}
		out = append(out, filepath.Join("internal/broker", n))
	}
	return out
}

// TestD5ProductionWiresNoClusterNode (E-G1): a source scan fails loudly the moment a future
// change cuts the cluster FSM / the D5 publisher loop into production ahead of D9. Scans
// serve.go + ALL production internal/broker files (minus the publisher itself).
func TestD5ProductionWiresNoClusterNode(t *testing.T) {
	root := moduleRoot(t)
	files := append([]string{"cmd/tether/serve.go"}, productionBrokerFiles(t, root)...)
	for _, rel := range files {
		src := readFile(t, filepath.Join(root, rel))
		for _, banned := range d5BannedTokens {
			if strings.Contains(src, banned) {
				t.Errorf("%s wires %q — D5 is build-and-prove; production cutover is D9", rel, banned)
			}
		}
	}
}

// TestD5GuardSelfCheck proves the scan has discriminating power AND closes the
// struct-literal evasion the original constructor-only ban missed (internal review M4).
func TestD5GuardSelfCheck(t *testing.T) {
	flagged := func(src string) bool {
		for _, b := range d5BannedTokens {
			if strings.Contains(src, b) {
				return true
			}
		}
		return false
	}
	// The constructor path.
	if !flagged("x := broker.NewAuditPublisher(cfg)") {
		t.Error("self-check: the constructor must be detected")
	}
	// The struct-literal cutover path (the one the dead lowercase token missed).
	if !flagged("pub := &AuditPublisher{cfg: AuditPublisherConfig{Node: n, JS: js}}\n\tgo pub.Run(ctx)") {
		t.Error("self-check: a struct-literal cutover must be detected")
	}
	if flagged("// production wires no publisher here\nb.Run(ctx)") {
		t.Error("self-check: a clean source must NOT be flagged")
	}
}

// TestD5ClusterNoNATSImport (E-G2 / L-2 / R-23): internal/cluster must import NO NATS and
// NOT the broker/jsstream packages — raft stays confined; the audit publisher (which needs
// NATS) lives in internal/broker and reads the cluster via the raft-free primitives. No
// such import-graph guard existed before D5.
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

// ---- shared source-scan helpers (mirror test/d4/regression_test.go) ----

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
