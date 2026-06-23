package d6_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// d6BannedTokens is the SSOT the D6 build-and-prove production-wiring guard scans
// for (R-25). D6 is build-and-prove (cutover=D9): the production wiring must
// never ACTIVATE the cluster home seam (AttachClusterSeam sets b.selfID +
// b.tunnelCert) nor load/serve a stable tunnel cert. With those absent,
// b.selfID=="" keeps every home path inert (homeForRegister/homeForExpose return
// nil → byte-identical responses; the tunnelTokenLookup ladder is skipped) and
// the tunnel server stays on its ephemeral self-signed N=1 cert.
//
// NOTE: the GATED home call sites (homeForRegister( / homeForExpose( /
// selfNodeID() / newTunnelServer() in handleRegister/handleExposeReq/Run) are
// deliberately NOT banned — they live in the scanned production files but are
// inert when b.selfID=="" (the seam was never attached). The real activation
// indicator is AttachClusterSeam, banned here. cluster.New( is already banned by
// the D5 guard over the same files.
// Review A1 MAJOR: the seam is two UNEXPORTED fields (b.selfID, b.tunnelCert), so
// the method-name ban alone misses a struct-literal / direct-field-write / renamed
// setter cutover (the D5 M4 lesson — ban the field, not just the constructor).
// Every token below is verified ABSENT from the scanned production files (home.go
// + audit_publisher.go are excluded; they legitimately hold the seam). The GATED
// call sites (homeForRegister(/homeForExpose(/selfNodeID()/newTunnelServer()) are
// deliberately NOT banned — they appear in scanned files but are inert when
// selfID=="" (R-25 was over-broad on those).
var d6BannedTokens = []string{
	"AttachClusterSeam(", // the seam setter
	"NewServerWithCert(",  // stable-cert tunnel server (harness-only; production uses NewServer)
	"LoadServerCert(",     // loads a stable tunnel cert from PEM (harness/D9 only)
	".selfID =",           // direct field write of the cluster identity (b.selfID = ...)
	".tunnelCert =",       // direct field write of the stable cert (b.tunnelCert = ...)
	"selfID:",             // struct-literal cutover (&Broker{selfID: ...})
	"tunnelCert:",         // struct-literal cutover (&Broker{tunnelCert: ...})
	// The home-resolution producers: building a directive in a SCANNED file means
	// emitting the cluster path (the directive struct literal itself is NOT banned —
	// `proto.HomeDirective{}` appears as a benign map-init type — but its inputs are).
	"resolveHomeForAgent(", "certPinsFor(", "LookupByNatsServer(", "PlanReassignHome(",
}

// stripComment removes a trailing // line comment so a banned token mentioned in a
// comment (e.g. the field doc `b.tunnelCert == nil`) is not a false positive. It
// is a heuristic (does not parse string literals), which is fine: the scanned
// production files contain no banned token inside a string literal, and the
// self-check pins the discrimination.
func stripComment(line string) string {
	if i := strings.Index(line, "//"); i >= 0 {
		return line[:i]
	}
	return line
}

// productionScanFiles returns serve.go + every production .go in internal/broker
// AND internal/agent EXCEPT the build-and-prove mechanism file (home.go), the D5
// publisher, and any _test.go. The agent is scanned too (Critique 4 V2) so a
// clustered activation cannot hide there either.
func productionScanFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	// Review A1 MINOR: scan ALL of cmd/tether (not just serve.go) + the broker +
	// agent packages, so a cutover authored in any of them is caught.
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
			if n == "home.go" || n == "audit_publisher.go" {
				continue // build-and-prove mechanism files (intentionally hold the seam tokens)
			}
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}

// TestD6ProductionWiresNoClusterNode (R-25): a source scan fails loudly the
// moment a change wires the D6 home seam / stable cert into production ahead of
// the D9 cutover.
func TestD6ProductionWiresNoClusterNode(t *testing.T) {
	root := moduleRoot(t)
	for _, rel := range productionScanFiles(t, root) {
		src := readFile(t, filepath.Join(root, rel))
		for _, line := range strings.Split(src, "\n") {
			code := stripComment(line)
			for _, banned := range d6BannedTokens {
				if strings.Contains(code, banned) {
					t.Errorf("%s wires %q — D6 is build-and-prove; production cutover is D9\n  line: %s", rel, banned, strings.TrimSpace(line))
				}
			}
		}
	}
}

// TestD6GuardSelfCheck proves the scan discriminates: an attach / stable-cert
// cutover is caught, a clean source is not.
func TestD6GuardSelfCheck(t *testing.T) {
	flagged := func(src string) bool {
		code := stripComment(src)
		for _, b := range d6BannedTokens {
			if strings.Contains(code, b) {
				return true
			}
		}
		return false
	}
	if !flagged("b.AttachClusterSeam(node.SelfID(), cert)") {
		t.Error("self-check: an AttachClusterSeam cutover must be detected")
	}
	if !flagged("srv := tunnel.NewServerWithCert(addr, host, lookup, cert, log)") {
		t.Error("self-check: a stable-cert tunnel server must be detected")
	}
	// Review A1 MAJOR: the field-based cutover evasions must all be caught.
	if !flagged("b.selfID = string(n.LocalID)") {
		t.Error("self-check: a direct selfID field-write cutover must be detected")
	}
	if !flagged("b.tunnelCert = cert") {
		t.Error("self-check: a direct tunnelCert field-write cutover must be detected")
	}
	if !flagged("p := &Broker{cfg: c, selfID: id, tunnelCert: cert}") {
		t.Error("self-check: a struct-literal cutover must be detected")
	}
	// The benign lines (gated call site, struct field decl, the ==\"\" comment) must NOT be flagged.
	if flagged("resp.Home = b.homeForRegister(sid, nid, req) // inert when selfID==\"\"") {
		t.Error("self-check: a GATED home call site must NOT be flagged")
	}
	if flagged("\tselfID     string") || flagged("\ttunnelCert *tls.Certificate") {
		t.Error("self-check: the struct field declarations must NOT be flagged")
	}
}

// TestD6ClusternodesNoNATSNoCluster (R-3 / L-2): internal/clusternodes is a
// pure-SQL leaf — it must import NEITHER nats.go NOR internal/cluster (raft), so
// the home-resolution read path drags in neither the messaging nor the consensus
// layer.
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
	// Self-check: the query worked and the package depends on database/sql.
	if !deps["database/sql"] {
		t.Fatal("self-check: internal/clusternodes deps must include database/sql (query failed?)")
	}
}

// TestD6ClusterStillNoNATS re-asserts the D5 L-2 boundary survives D6 (the home
// logic that needs nats lives in internal/broker, not internal/cluster).
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

// ---- shared source-scan helpers (mirror test/d5/regression_test.go) ----

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

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
