package d7_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// regression_test.go — D7 build-and-prove guard (NON-gated, runs in make test).
// D7 is build-and-prove (cutover=D9): production cmd/tether/serve.go + internal/
// broker + internal/agent must never ACTIVATE the cluster admin/membership/offline
// machinery. The orchestrator/membership/offline code is real and unit/integration
// proven, but production must not construct a ClusterAdmin, call a raft config
// change, open the raft bolt store, run RecoverCluster, or write cluster_nodes
// directly — those activations are the D9 cutover.

// d7BannedTokens is the SSOT of production-cutover indicators. Every token is verified
// ABSENT from the scanned production files (the mechanism file clusteradmin.go is
// excluded — it legitimately constructs/drives the orchestrator).
var d7BannedTokens = []string{
	"AttachClusterAdminSeam(", // the (future) admin seam setter
	"NewClusterAdmin(",        // constructing the orchestrator in production
	".clusterAdmin =",         // direct field-write cutover (b.clusterAdmin = ...)
	"clusterAdmin:",           // struct-literal cutover (&Broker{clusterAdmin: ...})
	"AddVoter(",               // a raft config change wired into a scanned file
	"RemoveServer(",           // a raft config change wired into a scanned file
	"LeadershipTransferToServer(",
	"RecoverCluster(",           // the raw raft recovery (must go through clusteroffline -> cluster.RecoverSingleNode)
	"raftboltdb.New(",           // opening the raft bolt store directly from production
	"INSERT INTO cluster_nodes", // the roster writer is the FSM (internal/cluster), never production
	"UPDATE cluster_nodes",
	"DELETE FROM cluster_nodes",
}

// d7ExcludedFiles are the build-and-prove mechanism files that legitimately hold
// the banned tokens (the D7 analogues of home.go / audit_publisher.go).
var d7ExcludedFiles = map[string]bool{
	"home.go":            true, // D6 mechanism
	"audit_publisher.go": true, // D5 mechanism
	// The D7 ClusterAdmin orchestrator/mechanism: these legitimately construct the
	// orchestrator + drive raft config changes (AddVoter/RemoveServer/...). Production
	// wiring (serve.go, broker.go, the handlers) is still scanned, so a cutover that
	// CONSTRUCTS a ClusterAdmin or wires the seam there is still caught.
	"clusteradmin.go":  true, // two-phase orchestrator + reconciliation + nonce store
	"clusterdrain.go":  true, // drain/retire (RemoveServer, PlanClusterNodeRemove)
	"clusterstatus.go": true, // status report + adminsock adapter (RemoveServer, TransferLeadership)
	// D8 mechanism files (no D7 banned tokens today, but excluded for forward-consistency
	// so a future D8 edge that touches a D7 token is not double-counted here).
	"transfer_home.go":          true,
	"transfer_audit_forward.go": true,
	"alert_reconcile.go":        true,
	"alert_forward.go":          true,
	"cluster_health.go":         true,
}

func stripComment(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		return line[:i]
	}
	return line
}

func productionScanFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, dir := range []string{"cmd/tether", "internal/broker", "internal/agent"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			n := e.Name()
			if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
				continue
			}
			if d7ExcludedFiles[n] {
				continue
			}
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}

func TestD7ProductionWiresNoCluster(t *testing.T) {
	root := moduleRoot(t)
	for _, rel := range productionScanFiles(t, root) {
		src := readFile(t, filepath.Join(root, rel))
		for _, line := range strings.Split(src, "\n") {
			code := stripComment(line)
			for _, banned := range d7BannedTokens {
				if strings.Contains(code, banned) {
					t.Errorf("%s wires %q — D7 is build-and-prove; production cutover is D9\n  line: %s",
						rel, banned, strings.TrimSpace(line))
				}
			}
		}
	}
}

// TestD7GuardSelfCheck proves the scan discriminates (the D5 fake-reader vacuity
// lesson): every cutover form is CAUGHT, every benign line is NOT.
func TestD7GuardSelfCheck(t *testing.T) {
	flagged := func(src string) bool {
		code := stripComment(src)
		for _, b := range d7BannedTokens {
			if strings.Contains(code, b) {
				return true
			}
		}
		return false
	}
	// Cutover forms that MUST be caught.
	for _, planted := range []string{
		"b.clusterAdmin = broker.NewClusterAdmin(node, log)",
		"p := &Broker{clusterAdmin: a}",
		"b.AttachClusterAdminSeam(node)",
		"if err := node.AddVoter(id, addr); err != nil {",
		"node.RemoveServer(id)",
		"node.LeadershipTransferToServer(id, addr)",
		"raft.RecoverCluster(rc, f, s, s, snaps, tr, cfg)",
		"store, _ := raftboltdb.New(raftboltdb.Options{Path: p})",
		"db.Exec(`INSERT INTO cluster_nodes(node_id) VALUES(?)`, id)",
		"db.Exec(`UPDATE cluster_nodes SET phase=? WHERE node_id=?`, p, id)",
		"db.Exec(`DELETE FROM cluster_nodes WHERE node_id=?`, id)",
	} {
		if !flagged(planted) {
			t.Errorf("self-check: a cutover line must be detected: %q", planted)
		}
	}
	// Benign lines that must NOT be flagged.
	for _, benign := range []string{
		"\tclusterAdmin *ClusterAdmin // nil in production until D9", // struct field decl + comment
		"rows, _ := db.Query(`SELECT node_id FROM cluster_nodes`)",   // a READ is fine
		"_ = clusteroffline.ForceSingle(opts) // goes through the seam",
		"resp.Cluster = b.clusterStatusReport() // inert when clusterAdmin==nil",
	} {
		if flagged(benign) {
			t.Errorf("self-check: a benign line must NOT be flagged: %q", benign)
		}
	}
}

// --- import-cycle re-assertions (L-2 survives D7) ---

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

// TestD7ClusterStaysNATSFreeAfterAuthEdge: internal/cluster now imports internal/auth
// (the join-PoP verify). auth pulls in nats-io/nkeys (crypto), NOT the nats.go
// client — assert the messaging client did not sneak in transitively.
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

// TestD7RaftConfinedToCluster: internal/clusteroffline (offline surgery) and
// internal/broker must NOT DIRECTLY import hashicorp/raft — raft stays confined to
// internal/cluster (the offline tool goes through cluster.RecoverSingleNode; the
// orchestrator through cluster.Node's string wrappers).
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

// --- helpers (mirrors test/d6/regression_test.go) ---

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
