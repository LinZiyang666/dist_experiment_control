package d8_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// regression_test.go — D8 build-and-prove guard (NON-gated, runs in make test).
// D8 (distributed transfer ‖ replicated alerts) is build-and-prove (cutover=D9):
// production cmd/tether/serve.go + internal/broker + internal/agent must never ACTIVATE
// the transfer-distribution or alert-store machinery. The mechanism code is real and
// unit/integration proven, but production must not attach the transfer-audit / alert /
// xfer-replica seams nor subscribe the cluster-health / alert responders — those are D9.
//
// The ctl-side gate/banner (cmd/tether/d8_alerts.go, alert.go) are genuinely LIVE but inert
// at N=1 (no broker answers the member-reachable RPCs → ErrNoResponders → no gate/banner),
// so they publish/request (allowed) and never appear in the banned-token set.

var d8BannedTokens = []string{
	// transfer-audit forward seam
	"AttachTransferAuditSink(",
	"b.transferAuditSink =",
	"transferAuditSink:",
	// tier-B replica seam
	"AttachXferReplicas(",
	"b.xferReplicasFn =",
	"xferReplicasFn:",
	// alert disk-forward seam
	"AttachAlertSink(",
	"b.alertSink =",
	"alertSink:",
	// alert reconcile loop + cluster-health / alert responders (harness-wired only)
	"NewAlertReconciler(",
	"SubscribeClusterHealth(",
	"SubscribeAlertLs(",
	"SubscribeAlertAck(",
}

// d8ExcludedFiles are EXACTLY the D8 build-and-prove mechanism files that legitimately hold a
// banned token (the seam setters / responder constructors). It is kept MINIMAL (review m4):
// the inherited D4–D7 mechanism files (home.go / audit_publisher.go / cluster_forward.go /
// clusteradmin.go / clusterdrain.go / clusterstatus.go) hold ZERO D8 tokens, so excluding them
// would only widen the blind spot — they stay SCANNED. TestD8GuardExclusionsJustified pins that
// every entry here holds >=1 banned token, so the list can never quietly over-broaden.
var d8ExcludedFiles = map[string]bool{
	"transfer_home.go":          true, // xfer replica seam + home gate + orphan filter
	"transfer_audit_forward.go": true, // AttachTransferAuditSink + forward closure
	"alert_reconcile.go":        true, // NewAlertReconciler + leader-gated loop
	"alert_forward.go":          true, // AttachAlertSink + planAlertSignal
	"cluster_health.go":         true, // SubscribeClusterHealth/AlertLs/AlertAck responders
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
			if d8ExcludedFiles[n] {
				continue
			}
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}

func TestD8ProductionWiresNoCluster(t *testing.T) {
	root := moduleRoot(t)
	for _, rel := range productionScanFiles(t, root) {
		src := readFile(t, filepath.Join(root, rel))
		for _, line := range strings.Split(src, "\n") {
			code := stripComment(line)
			for _, banned := range d8BannedTokens {
				if strings.Contains(code, banned) {
					t.Errorf("%s wires %q — D8 is build-and-prove; production cutover is D9\n  line: %s",
						rel, banned, strings.TrimSpace(line))
				}
			}
		}
	}
}

// TestD8GuardSelfCheck proves the scan discriminates: every cutover form is CAUGHT, every
// benign line (the inert seam READS that legitimately live in scanned production files) is NOT.
func TestD8GuardSelfCheck(t *testing.T) {
	flagged := func(src string) bool {
		code := stripComment(src)
		for _, b := range d8BannedTokens {
			if strings.Contains(code, b) {
				return true
			}
		}
		return false
	}
	for _, planted := range []string{
		"b.AttachTransferAuditSink(fwd)",
		"b.transferAuditSink = func(rec schema.AuditTransfer) {}",
		"x := &Broker{transferAuditSink: f}",
		"b.AttachXferReplicas(func() int { return 3 })",
		"b.xferReplicasFn = fn",
		"b.AttachAlertSink(fwd)",
		"b.alertSink = func(active bool) {}",
		"r := NewAlertReconciler(cfg)",
		"sub, _ := SubscribeClusterHealth(nc, node, db, now)",
		"sub, _ := SubscribeAlertLs(nc, db)",
		"sub, _ := SubscribeAlertAck(nc, fwd)",
	} {
		if !flagged(planted) {
			t.Errorf("self-check: a cutover line must be detected: %q", planted)
		}
	}
	for _, benign := range []string{
		"\ttransferAuditSink func(schema.AuditTransfer) // nil in production",       // field decl + comment
		"\tif b.transferAuditSink != nil { b.transferAuditSink(rec); return }",      // the inert READ + call
		"\tif b.alertSink != nil { b.alertSink(frac >= threshold) }",               // disk monitor READ + call
		"\treturn b.xferTargetReplicas() // ReplicasSingle when xferReplicasFn==nil", // the read method
		"\tmsg, _ := nc.Request(proto.SubjCtrlAlertLs(actor), nil, t) // ctl LIVE",  // ctl-side request (allowed)
	} {
		if flagged(benign) {
			t.Errorf("self-check: a benign line must NOT be flagged: %q", benign)
		}
	}
}

// TestD8GuardExclusionsJustified (review m4): every excluded file must hold >=1 banned token —
// an exclusion that doesn't is unjustified (it would only widen the scan's blind spot). This is
// the converse of the self-check: it keeps d8ExcludedFiles from quietly over-broadening.
func TestD8GuardExclusionsJustified(t *testing.T) {
	root := moduleRoot(t)
	for name := range d8ExcludedFiles {
		path := filepath.Join(root, "internal/broker", name)
		src := readFile(t, path)
		held := false
		for _, line := range strings.Split(src, "\n") {
			code := stripComment(line)
			for _, banned := range d8BannedTokens {
				if strings.Contains(code, banned) {
					held = true
				}
			}
		}
		if !held {
			t.Errorf("excluded file %s holds NO banned token — the exclusion is unjustified (scan it instead)", name)
		}
	}
}

// --- layering re-assertions (L-2 survives D8) ---

// TestD8ClusterStaysNATSFree: internal/cluster gained alert_ops.go + alert_read.go (the
// replicated alert store Plan/read helpers). They must import only database/sql + std — never
// the nats.go client (the L-2 line D5 established).
func TestD8ClusterStaysNATSFree(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/cluster")
	if deps["github.com/nats-io/nats.go"] {
		t.Error("internal/cluster transitively imports nats.go — L-2 violated (alert store must be NATS-free)")
	}
}

// TestD8JsstreamStaysClusterFree: internal/jsstream gained ListXferStreams (used by the
// read-only retire-gate observer). jsstream must not import internal/cluster (the D5 line).
func TestD8JsstreamStaysClusterFree(t *testing.T) {
	deps := goListDeps(t, "github.com/LinZiyang666/tether/internal/jsstream")
	if deps["github.com/LinZiyang666/tether/internal/cluster"] {
		t.Error("internal/jsstream imports internal/cluster — L-2 violated")
	}
}

// TestD8XferauditIsLeaf: the new internal/xferaudit leaf (transfer-audit Plan/Replay + the
// schema.AuditTransfer Aux) imports internal/cluster + internal/schema but NEVER nats.go —
// it is a pure render/replay leaf like internal/proc, keeping schema out of internal/cluster.
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

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
