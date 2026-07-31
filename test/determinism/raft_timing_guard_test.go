package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestRaftTimingsUseProductionConstants stops a test harness from inventing its
// own raft timings.
//
// # WHY THIS EXISTS
//
// Every raft-driving suite in this repo used to hardcode its own heartbeat,
// election and leader-lease timeouts: 50ms, 60ms, 80ms, 100ms, 150ms — against a
// production MultinodeHeartbeatTimeout of 1000ms and a leader lease of 500ms.
// One of them used a 25ms lease, meaning a leader stepped down after 25ms without
// a majority ack.
//
// Those numbers are not merely "tighter". They are wrong in two independent ways:
//
//   - They do not test what production does. A harness tuned to 50ms exercises an
//     election cadence no deployment will ever run.
//   - They break under the conditions these suites are REQUIRED to run in. Most
//     carry -race (5-10x slower on every memory access, per CLAUDE.md §5) and
//     `make e2e-parallel` runs 20 of them at once. A single GC pause then unseats
//     a leader mid-AddNode.
//
// That produced three load-only flakes (test/d3 follower PIN write,
// internal/broker mutating-verb redirect, test/d7 FollowerStatusViewSource) that
// all passed in isolation — the shape that is hardest to diagnose and easiest to
// "fix" by making the harness more patient, which is how a runner's pressure
// symptoms get written into a product regression suite.
//
// Referencing the production constants also removes the drift: change
// MultinodeHeartbeatTimeout and every harness follows, instead of continuing to
// verify a configuration that no longer exists.
//
// See docs/testing-standards.md T1/T2.
func TestRaftTimingsUseProductionConstants(t *testing.T) {
	root := repoRoot(t)

	// Deliberate deviations. A suite whose ASSERTION depends on specific timings
	// belongs here WITH its reason, not silently inline. Any value chosen here
	// must still survive -race under full parallel load, which is exactly what
	// the original hardcoded numbers did not.
	//
	// KEYED file:FUNCTION#ordinal, NOT file:line — the FOURTH map in this repo to make that move, and
	// the last one. origin: p-b2 internal review m2.
	//
	// The other three were re-keyed because a line key goes stale on any insertion above the site and
	// its maintenance edit (renumber) is indistinguishable in a diff from its subversion (silence a
	// genuinely new site). This one was the weakest of the four: the keys were `:509`/`:510`, one reason
	// cross-referenced the other BY LINE, and there was no stale-entry check at all — `exempt` was only
	// ever READ, so a dead entry could sit here forever and a live one could quietly start covering a
	// different site. Plan §4 row 6 had registered it with the mitigation "do not add or remove any line
	// in transport_test.go", which is a real mitigation and also an admission that the key was the
	// problem. The ordinal is the site's 1-based index among the NON-CONFORMING timing fields of its
	// enclosing function, so nothing above the function can move it.
	// THE RE-KEY FOUND A THIRD SYNTACTIC SITE. The old map had two entries, `:509` and `:510`, while line
	// 509 carried HeartbeatTimeout AND ElectionTimeout. External review then checked whether all three
	// were legitimate exceptions: heartbeat/election were literal 1s values equal to their production
	// constants, so they now reference those constants. Only the 2s leader lease creates the deliberate
	// invalid ordering and remains exempt.
	exempt := map[string]string{
		"internal/cluster/transport_test.go:TestD3FailingNewReapsTransportNoLeak#1": "LeaderLeaseTimeout " +
			"(2s) deliberately exceeds the production HeartbeatTimeout (1s), so raft.ValidateConfig " +
			"rejects inside BootstrapCluster AFTER the transport exists — the failure path this test " +
			"must reap. HeartbeatTimeout and ElectionTimeout still use production constants; only this " +
			"field needs to differ.",
	}
	// Every exemption must still name a live non-conforming site. Without this the map is write-only:
	// the two entries below survived a rename and a file move purely because nothing ever checked them.
	seen := map[string]bool{}

	fields := map[string]string{
		"HeartbeatTimeout":   "MultinodeHeartbeatTimeout",
		"ElectionTimeout":    "MultinodeElectionTimeout",
		"LeaderLeaseTimeout": "MultinodeLeaderLeaseTimeout",
	}

	var offenders []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if n := info.Name(); n == ".git" || n == "vendor" || n == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		ord := map[string]int{}
		var walk func(fnName string, n ast.Node)
		walk = func(fnName string, n ast.Node) {
			ast.Inspect(n, func(m ast.Node) bool {
				if fd, ok := m.(*ast.FuncDecl); ok {
					if fd.Body != nil {
						walk(guardFuncName(fd), fd.Body)
					}
					return false
				}
				kv, ok := m.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					return true
				}
				expected, guarded := fields[key.Name]
				if !guarded {
					return true
				}
				if isExactTimingConstant(kv.Value, expected) {
					return true
				}
				fn := fnName
				if fn == "" {
					fn = "<file-scope>"
				}
				siteKey := rel + ":" + fn
				ord[siteKey]++
				site := siteKey + "#" + strconv.Itoa(ord[siteKey])
				if _, ok := exempt[site]; ok {
					seen[site] = true
					return true
				}
				offenders = append(offenders, site+"  ("+key.Name+")")
				return true
			})
		}
		walk("", f)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Non-vacuity: the scan must be finding these fields SOMEWHERE, or a rename
	// would turn this gate green while every harness went back to inventing
	// numbers. See docs/testing-standards.md G2.
	if n := countProductionConstantUses(t, root); n < 10 {
		t.Fatalf("only %d test site(s) reference the production raft timing constants; "+
			"this gate has probably gone blind (a rename, a moved constant, or a walk that "+
			"stopped reaching the suites)", n)
	}

	var stale []string
	for site := range exempt {
		if !seen[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d raft-timing exemption(s) no longer name a non-conforming site; re-read and remove "+
			"or update them:\n  %s\n"+
			"An exemption nothing checks is write-only: it survives the deletion of the code it excused "+
			"and is then free to start covering something else.", len(stale), strings.Join(stale, "\n  "))
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d raft timing(s) hardcoded in tests instead of referencing "+
			"cluster.Multinode{Heartbeat,Election,LeaderLease}Timeout:\n  %s\n\n"+
			"A harness with its own timings tests a cadence production never runs, and breaks "+
			"under the -race + parallel load these suites are required to survive. If a suite "+
			"genuinely needs different values, add it to the exempt map WITH a reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// guardFuncName renders a FuncDecl the way an exemption key names it: `(*T).Method` for a method,
// `TestFoo` for a plain function. Same shape as cmd/tether's qualifiedFuncName and internal/auth's
// aclQualifiedFuncName — four site-keyed exemption maps in this repo, one key scheme between them.
func guardFuncName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + guardRecvName(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

func guardRecvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + guardRecvName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return guardRecvName(t.X)
	case *ast.IndexListExpr:
		return guardRecvName(t.X)
	}
	return "<unknown-recv>"
}

// countProductionConstantUses counts test sites that reference the production
// timing constants, so the gate can prove it is still looking at live code.
func countProductionConstantUses(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(f, func(node ast.Node) bool {
			switch v := node.(type) {
			case *ast.Ident:
				if strings.HasPrefix(v.Name, "Multinode") && strings.HasSuffix(v.Name, "Timeout") {
					n++
				}
			case *ast.SelectorExpr:
				if strings.HasPrefix(v.Sel.Name, "Multinode") && strings.HasSuffix(v.Sel.Name, "Timeout") {
					n++
					return false // do not count Sel again as an Ident
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("count production timing constants: %v", err)
	}
	return n
}

func isExactTimingConstant(e ast.Expr, expected string) bool {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name == expected
	case *ast.SelectorExpr:
		return v.Sel.Name == expected
	}
	return false
}

func TestRaftTimingGuardRejectsAliasesAndArithmetic(t *testing.T) {
	parse := func(src string) ast.Expr {
		t.Helper()
		e, err := parser.ParseExpr(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		return e
	}
	if !isExactTimingConstant(parse("cluster.MultinodeHeartbeatTimeout"), "MultinodeHeartbeatTimeout") {
		t.Fatal("production selector was rejected")
	}
	for _, src := range []string{
		"testHeartbeat",
		"2 * cluster.MultinodeHeartbeatTimeout",
		"time.Second",
		"100 * time.Millisecond",
	} {
		if isExactTimingConstant(parse(src), "MultinodeHeartbeatTimeout") {
			t.Errorf("%s changes or hides the production timing but was accepted", src)
		}
	}
}
