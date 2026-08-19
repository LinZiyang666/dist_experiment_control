package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// cfgdb_ratchet_test.go (batch B, B3) — the SHRINK-ONLY ratchet on direct b.cfg.DB access.
//
// WHY A RATCHET AND NOT A TYPE
//
// `Config.DB` is one field with two runtime meanings: broker.go assigns
// `b.cfg.DB = cl.node.RODB()` when clusterMode is true, so it is a writable pool in single mode
// and a READ-ONLY pool in cluster mode. internal/broker/dbrole.go adds three named accessors
// (read / liveness / singleWriter) whose types carry the distinction, and readDB genuinely makes
// `session.Create(b.read(), …)` a compile error.
//
// But the compiler cannot express "and nobody reaches around them". Config.DB must stay a
// *sql.DB (it is a constructor input; changing its type breaks test/concurrency's direct
// `broker.Config{DB: db}` and takes the goroutine-leak gate, the fd gate and -race down with it),
// so a caller can always write `b.cfg.DB` again. That half is enforced here.
//
// Neither half substitutes for the other — docs/testing-standards.md §S1 records the general
// case: 结构检查与行为检查互相替代不了.
//
// HOW IT WORKS
//
// Keyed by `file:func` with an EXACT count, because the alternative shapes both fail:
//
//   - a total-only ceiling lets one function gain a site while another loses one;
//   - a per-file ceiling does the same within a file;
//   - a ceiling (rather than an exact count) lets the recorded numbers rot upward, so the table
//     stops describing the code and a reviewer reading it is misled.
//
// EXACT means a DECREASE fails too. That is deliberate and is the ratchet's whole working
// mechanism: converting a site is progress, and the same commit lowers the number here. The
// failure message says so. A frozen table nobody has to touch is a table that stops being true.
//
// The baseline below is the state at the moment B3's accessors landed. It is expected to shrink
// to (near) zero as read sites move to b.read(); it must never grow.

// cfgDBBaseline is the per-function count of direct `b.cfg.DB` references in non-test files of
// internal/broker.
//
// Notable entries and why they are legitimate today:
//
//	dbrole.go:*            — the accessors themselves; these are the ONE place that may touch it
//	clusterwrite.go:livenessDB — derives the liveness handle
//	broker.go:Run          — the assignment that creates the whole problem, plus boot-time reads
//	proxy*.go              — the largest block (P13); single-mode reads and writes, unconverted
var cfgDBBaseline = map[string]int{
	"admit.go:admitACL":                      4,
	"audit.go:reconcileHistoryStreamsOnBoot": 2,
	"authcallout.go:installAuthCallout":      1,
	"broker.go:Run":                          5,
	"broker.go:handleHeartbeat":              1,
	"broker.go:handleRegister":               1,
	"cluster_manifest.go:manifestBytes":      2,
	"cluster_roster.go:buildSignedRoster":    2,
	"clusterwrite.go:allocatePort":           3,
	"clusterwrite.go:createSession":          1,
	// refileProc: single-mode-only re-file of an adopted process row. Cluster
	// mode returns early (see the function) rather than growing a raft verb for
	// a display-level fix, so this site is unreachable there.
	"clusterwrite.go:refileProc":           1,
	"clusterwrite.go:dropSession":          1,
	"clusterwrite.go:freePortAllocation":   1,
	"clusterwrite.go:livenessDB":           1,
	"clusterwrite.go:markProcExited":       1,
	"clusterwrite.go:readCommittedSession": 1,
	"clusterwrite.go:recordProc":           1,
	"clusterwrite.go:registerNode":         1,
	"clusterwrite.go:revokePortAllocation": 1,
	"clusterwrite.go:tombstoneSession":     1,
	"clusterwrite.go:wireClusterLate":      6,
	"dbrole.go:read":                       1,
	"dbrole.go:singleWriter":               1,
	// Lowered from 3 / 5 when ps and node.list moved onto admitCtrl (plan §15.2). The ratchet
	// requires this in the SAME change as the conversion — that is its working mechanism.
	"exec.go:handleNodeListReq":                          1,
	"exec.go:handlePsReq":                                2, // -1: h1 A1 ports read → b.read() (counts/list additions all ride b.read too)
	"expose.go:handleExposeRmReq":                        2,
	"expose.go:reconcilePorts":                           1,
	"expose.go:tunnelTokenLookup":                        2,
	"home.go:homeForExpose":                              1,
	"home.go:homeForRegister":                            1, // -1: inline read → b.read()
	"home.go:resolveHomeForAgent":                        1, // -1: inline read → b.read()
	"metrics_wire.go:metricsSnapshot":                    2,
	"observability.go:observeOnce":                       1,
	"proxy.go:activeProxyKeys":                           1,
	"proxy.go:createSubAndBump":                          1,
	"proxy.go:disableProxy":                              6,
	"proxy.go:enableProxy":                               4,
	"proxy.go:escalateProxyGen":                          1,
	"proxy.go:handleProxyReadyEvent":                     1,
	"proxy.go:handleProxyStatus":                         3, // lowered from 5: now gated by admitACL
	"proxy.go:handleProxySub":                            1,
	"proxy.go:proxyActiveOwnerGate":                      2,
	"proxy.go:proxyDirectiveForRegister":                 7,
	"proxy.go:proxyStatusNodes":                          1, // -1: inline read → b.read()
	"proxy.go:proxyStatusNodesCluster":                   3, // -1: inline read → b.read()
	"proxy.go:pushCurrentKeyset":                         1,
	"proxy.go:repairProxy":                               2,
	"proxy.go:revokeSubAndBump":                          1,
	"proxy.go:sessionNIDs":                               1,
	"proxy_auto_rebalance.go:driveAutoRebalanceOnReturn": 1,
	"proxy_auto_rebalance.go:noInflightOps":              1,
	"proxy_cluster_wire.go:handleProxySetCluster":        1,
	"proxy_cluster_wire.go:handleProxySubCreateCluster":  1,
	"proxy_cluster_wire.go:handleProxySubRevokeCluster":  2,
	"proxy_cluster_wire.go:proxyClusterStatusFor":        2,
	"proxy_rebalance.go:eligibleProxyHomes":              1, // -1: inline read → b.read()
	"proxy_rebalance.go:moveProxyHomeTo":                 1,
	"proxy_rebalance.go:rebalanceProxyHomes":             1,
	"proxy_reconcile.go:pickProxyRehomeTarget":           1, // -2: inline reads → b.read()
	"proxy_reconcile.go:proxyHomeHealthy":                1,
	"proxy_reconcile.go:reconcileProxySession":           2,
	"proxy_reconcile.go:reconcileProxyTeardown":          1, // -1: inline read → b.read()
	"proxy_reconcile.go:rehomeProxy":                     1,
	// -1: the port half moved out to reconcilePortsOnRegister (below), which
	// took its port.ListBySession read with it.
	"reconcile.go:reconcileOnRegister":            1,
	"reconcile.go:reconcilePortsOnRegister":       1,
	"roster_stale.go:maybeEmitRosterStale":        1,
	"sessions.go:handleSessionList":               2,
	"sessions.go:handleSessionRm":                 1,
	"topology_reconcile.go:buildTopologyInputs":   1,
	"topology_reconcile.go:reconcileTopologyOnce": 1,
	"transfer.go:transferGate":                    3,
	"tunnel_reconcile.go:reconcileTunnelSessions": 1,
}

// cfgDBBaselineTotal is asserted separately so a wholesale table rewrite still has to move a
// number a human will look at.
// 150 at the moment the accessors landed; 144 after ps / node.list / proxy.status moved onto the
// shared gate; 119 after the INLINE read sweep. It only ever goes down.
//
// The inline sweep is the correction to a conclusion this batch recorded and got half wrong. The
// plan's §16.2 said the read-point sweep was not worth doing, because internal/session and friends
// take a concrete *sql.DB, so a converted site has to hand back readDB.SQL() and gains nothing —
// that part is true and still is. What it then generalised to "there is nothing to convert" was
// false: 25 sites consumed the handle INLINE (b.cfg.DB.Query(`SELECT …`) right there, never passing
// it on), and those become structurally unable to write, because readDB has no Exec/Begin at all.
// Those 25 are what moved 144 → 119. The remaining 119 are the domain-function calls, and they stay.
const cfgDBBaselineTotal = 119 // +1: refileProc (single-mode re-file of an adopted proc row); markNodeOfflineOnRelease now goes through livenessDB in both modes

func TestCfgDBDirectAccessRatchet(t *testing.T) {
	got, files := scanCfgDBSites(t)

	if files < 30 {
		t.Fatalf("scanned only %d non-test file(s) in internal/broker — the traversal is broken, "+
			"and a broken traversal reports zero direct access everywhere", files)
	}

	total := 0
	for _, n := range got {
		total += n
	}
	if total != cfgDBBaselineTotal {
		t.Errorf("total direct b.cfg.DB sites = %d, baseline says %d.\n"+
			"If you CONVERTED sites, lower cfgDBBaselineTotal in the same commit — that is how this "+
			"ratchet works. If you ADDED one, use b.read() / b.livenessDB() / b.singleWriter() instead; "+
			"see internal/broker/dbrole.go for which.", total, cfgDBBaselineTotal)
	}

	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range cfgDBBaseline {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	for _, k := range sorted {
		want, listed := cfgDBBaseline[k]
		have := got[k]
		switch {
		case !listed:
			t.Errorf("%s touches b.cfg.DB directly (%d site(s)) and is NOT in the baseline.\n"+
				"In cluster mode that handle is READ-ONLY (broker.go re-points it to node.RODB()), so a "+
				"write here fails at runtime and ONLY in cluster mode — which this package's ~126 "+
				"zero-value &Broker{} test literals do not exercise. Use b.read() for reads, "+
				"b.livenessDB() for the three liveness columns, or route the write through raft.", k, have)
		case have == 0:
			t.Errorf("baseline lists %s with %d site(s) but it now has none — the function was renamed, "+
				"deleted, or fully converted. Remove the entry in the same commit, or the table stops "+
				"describing the code.", k, want)
		case have > want:
			t.Errorf("%s went from %d to %d direct b.cfg.DB site(s). The ratchet only turns one way: "+
				"use one of the accessors in internal/broker/dbrole.go.", k, want, have)
		case have < want:
			t.Errorf("%s went from %d to %d — that is PROGRESS, and the ratchet needs the number "+
				"lowered in the same commit so the table keeps describing the code.", k, want, have)
		}
	}
}

// scanCfgDBSites counts `b.cfg.DB` selector expressions per enclosing top-level declaration.
func scanCfgDBSites(t *testing.T) (map[string]int, int) {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "internal", "broker")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := map[string]int{}
	files := 0
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		files++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		for _, d := range f.Decls {
			name := "<file-level>"
			if fd, ok := d.(*ast.FuncDecl); ok {
				name = fd.Name.Name
			}
			if n := countCfgDB(d); n > 0 {
				out[e.Name()+":"+name] += n
			}
		}
	}
	return out, files
}

// countCfgDB is the predicate, split out so the self-check exercises the SAME code the live scan
// uses rather than a re-implementation of it.
func countCfgDB(n ast.Node) int {
	count := 0
	ast.Inspect(n, func(x ast.Node) bool {
		sel, ok := x.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "DB" {
			return true
		}
		inner, ok := sel.X.(*ast.SelectorExpr)
		if !ok || inner.Sel.Name != "cfg" {
			return true
		}
		if id, ok := inner.X.(*ast.Ident); ok && id.Name == "b" {
			count++
		}
		return true
	})
	return count
}

// TestCfgDBRatchetSelfCheck proves the counter matches what it claims and nothing else. Without
// it, a predicate that silently stopped matching would report every function clean and the
// ratchet would be a table of numbers guarding nothing.
func TestCfgDBRatchetSelfCheck(t *testing.T) {
	const src = `package broker
func hit() { _ = b.cfg.DB; _ = b.cfg.DB }
func viaAccessor() { _ = b.read(); _ = b.livenessDB() }
func otherReceiver() { _ = other.cfg.DB }
func otherField() { _ = b.other.DB }
func otherSel() { _ = b.cfg.Other }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic: %v", err)
	}
	want := map[string]int{"hit": 2}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		got := countCfgDB(d)
		if exp := want[fd.Name.Name]; got != exp {
			t.Errorf("countCfgDB(%s) = %d, want %d — the predicate %s", fd.Name.Name, got, exp,
				map[bool]string{true: "over-matches", false: "under-matches"}[got > exp])
		}
	}
}
