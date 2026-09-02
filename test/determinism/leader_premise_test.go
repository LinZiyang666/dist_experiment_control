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

// leader_premise_test.go — a test may read leadership only where the reading cannot go stale
// before it is used: inside a polling predicate, or inside clusterharness.WithLeader.
//
// WHY THIS EXISTS
// ---------------
// docs/testing-standards.md T3 and parallel-flake-rootcause.md root cause 2: "observe leadership,
// then assume it" produced two load-only flakes and the second was fixed wrong twice before the
// premise itself was recognised as the defect. The standard has stood since 2026-07-26 with the
// column "门禁 —". This is the gate: a bare leadership read — `.IsLeader()` or `.State() ==
// raft.Leader` — is a premise that will be assumed one statement later, and is red unless it sits
// in the site-keyed draining ledger.
//
// THE SAFE SHAPES, AND WHY EACH IS SAFE
//   - a func literal passed to a polling helper whose IDENTITY is established — an external
//     primitive named by import path (pollingHelperMethods), or a package-local helper that
//     verifiedPollingHelpers has proven from its own body to loop over the predicate or forward it
//     to such a primitive (external rereview R1: a bare name, even an exact one, can be shadowed
//     by a one-shot local function): the predicate is re-evaluated by its caller until it holds,
//     so the reading is as fresh as the last call and the caller, not the test, decides what to do
//     with it;
//   - the CONDITION of a for statement whose body only WAITS (`for !n.IsLeader() { time.Sleep(…) }`):
//     the language re-takes the condition every iteration and nothing acts on the reading in
//     between.
//
// NOT safe, and reported (internal review L1-F7 / L6-F9 caught the first three; external review F2
// the last two):
//   - a read inside a loop BODY: `for attempt := …; { if n.IsLeader() { … } }` is the exact shape
//     of the d3 flake — the loop retries, but each iteration reads once and then acts on it;
//   - a func literal that is NOT a known polling predicate: an IIFE, a `go func`, a t.Run body, a
//     t.Cleanup — evaluated once, straight-line code wearing a closure;
//   - a read inside a select case;
//   - a for CONDITION whose body ACTS (`for n.IsLeader() { n.Mutate(); break }`): from the
//     condition to the action is the same stale window as any other observe-then-act;
//   - a func literal handed to a helper that merely SOUNDS like a poller (`retryOnce(func() …)`):
//     the first version trusted any callee starting with wait/poll/retry/…, which let any future
//     helper opt out of the gate by choosing a good name.
//
// The ledger froze the sites this stricter scan found on 2026-09-01 — most of them hand-written
// observe→act→re-observe loops that are correct today; the ledger does not judge, it freezes.
// Drain an entry by acting through clusterharness.WithLeader, which does the re-observe for you.
//
// gate-control: TestLeaderPremiseScannerSeesTheShapes

// Package-qualified helpers are trusted by identity. Bare helpers are trusted only after
// verifiedPollingHelpers has inspected their declaration in the same Go package and proved that
// the callback is invoked from a loop or forwarded to one of these primitives. The distinction
// matters: comparing only the final callee name lets a one-shot local WaitFor shadow the real
// helper and bypass this gate (external rereview R1).
var pollingHelperMethods = map[string]map[string]bool{
	"github.com/LinZiyang666/tether/test/clusterharness": {
		"WaitForCond": true,
		"WithLeader":  true,
	},
	"github.com/LinZiyang666/tether/internal/testharness": {
		"WaitFor": true,
	},
}

// isBareLeaderRead recognises the two spellings of a leadership read: `x.IsLeader()` and
// `x.State() == raft.Leader` (either operand order).
func isBareLeaderRead(n ast.Node) bool {
	switch v := n.(type) {
	case *ast.CallExpr:
		sel, ok := v.Fun.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "IsLeader" && len(v.Args) == 0
	case *ast.BinaryExpr:
		if v.Op != token.EQL && v.Op != token.NEQ {
			return false
		}
		isStateCall := func(e ast.Expr) bool {
			call, ok := e.(*ast.CallExpr)
			if !ok || len(call.Args) != 0 {
				return false
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			return ok && sel.Sel.Name == "State"
		}
		isRaftLeader := func(e ast.Expr) bool {
			sel, ok := e.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Leader" {
				return false
			}
			pkg, ok := sel.X.(*ast.Ident)
			return ok && pkg.Name == "raft"
		}
		return (isStateCall(v.X) && isRaftLeader(v.Y)) || (isStateCall(v.Y) && isRaftLeader(v.X))
	}
	return false
}

// containsLeaderRead reports whether a leadership read occurs anywhere under n.
func containsLeaderRead(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if found {
			return false
		}
		if isBareLeaderRead(m) {
			found = true
		}
		return !found
	})
	return found
}

// loopBodyActs reports whether a loop body does anything beyond WAITING. The wait vocabulary is
// exact and small: the time package, time.Now()-rooted comparisons, fmt/errors, testing.TB methods
// on conventional receivers and runtime.Gosched. Selector spelling alone is not identity:
// nodes.Add is an action even though time.Time.Add is not (external rereview R1). Assignment,
// send, inc/dec, go/defer and non-continue branches are actions even when they contain no call.
func loopBodyActs(body *ast.BlockStmt, imports map[string]string) bool {
	testingRecv := map[string]bool{"t": true, "b": true, "tb": true}
	testingMethods := map[string]bool{
		"Fatal": true, "Fatalf": true, "Error": true, "Errorf": true,
		"Log": true, "Logf": true, "Helper": true,
		"Skip": true, "Skipf": true, "SkipNow": true,
	}
	isTimeValue := func(e ast.Expr) bool { return false }
	isTimeValue = func(e ast.Expr) bool {
		call, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		if id, ok := sel.X.(*ast.Ident); ok && imports[id.Name] == "time" && sel.Sel.Name == "Now" {
			return true
		}
		return (sel.Sel.Name == "Add" || sel.Sel.Name == "Round" || sel.Sel.Name == "Truncate") && isTimeValue(sel.X)
	}
	isWaitCall := func(call *ast.CallExpr) bool {
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			if id, ok := call.Fun.(*ast.Ident); ok {
				return id.Name == "len" || id.Name == "cap"
			}
			return false
		}
		if recv, ok := sel.X.(*ast.Ident); ok {
			switch imports[recv.Name] {
			case "time", "fmt", "errors":
				return true
			case "runtime":
				return sel.Sel.Name == "Gosched"
			}
			if testingRecv[recv.Name] && testingMethods[sel.Sel.Name] {
				return true
			}
		}
		return (sel.Sel.Name == "After" || sel.Sel.Name == "Before" || sel.Sel.Name == "Equal") && isTimeValue(sel.X)
	}
	acts := false
	ast.Inspect(body, func(m ast.Node) bool {
		if acts {
			return false
		}
		switch v := m.(type) {
		case *ast.FuncLit:
			return false // a deferred callback is not the loop body's action unless an outer call invokes it
		case *ast.AssignStmt, *ast.IncDecStmt, *ast.SendStmt, *ast.GoStmt, *ast.DeferStmt:
			acts = true
			return false
		case *ast.BranchStmt:
			if v.Tok != token.CONTINUE {
				acts = true
				return false
			}
		case *ast.CallExpr:
			if isWaitCall(v) {
				return false
			}
			acts = true
			return false
		default:
			return true
		}
		return true
	})
	return acts
}

func importNames(f *ast.File) map[string]string {
	names := map[string]string{}
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		local := filepath.Base(path)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		if local != "." && local != "_" {
			names[local] = path
		}
	}
	return names
}

func trustedPollingCall(call *ast.CallExpr, imports map[string]string, local map[string]bool) bool {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return local[fun.Name]
	case *ast.SelectorExpr:
		id, ok := fun.X.(*ast.Ident)
		if !ok {
			return false
		}
		return pollingHelperMethods[imports[id.Name]][fun.Sel.Name]
	}
	return false
}

// verifiedPollingHelpers proves package-local helper identity from its implementation. Merely
// being named waitFor is insufficient. A func-typed parameter must either be invoked beneath a
// loop or forwarded to an already-proven local/external polling primitive.
func verifiedPollingHelpers(files []*ast.File) map[string]bool {
	decls := map[string]*ast.FuncDecl{}
	owners := map[string]*ast.File{}
	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Body != nil {
				decls[fd.Name.Name] = fd
				owners[fd.Name.Name] = f
			}
		}
	}
	trusted := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for name, fd := range decls {
			if trusted[name] {
				continue
			}
			preds := map[string]bool{}
			if fd.Type.Params != nil {
				for _, field := range fd.Type.Params.List {
					if _, ok := field.Type.(*ast.FuncType); !ok {
						continue
					}
					for _, id := range field.Names {
						preds[id.Name] = true
					}
				}
			}
			if len(preds) == 0 {
				continue
			}
			proved := false
			// Direct repetition: the callback is called from a deadline-controlled loop with
			// no break. Merely putting pred() in `for i := 0; i < 1; i++` is still one-shot
			// code and is not proof of polling.
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if proved {
					return false
				}
				loop, ok := n.(*ast.ForStmt)
				if !ok || loop.Cond == nil {
					return true
				}
				deadlineControlled := false
				ast.Inspect(loop.Cond, func(m ast.Node) bool {
					call, ok := m.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					id, isID := sel.X.(*ast.Ident)
					if isID && importNames(owners[name])[id.Name] == "time" && (sel.Sel.Name == "Now" || sel.Sel.Name == "Since" || sel.Sel.Name == "Until") {
						deadlineControlled = true
						return false
					}
					return true
				})
				if !deadlineControlled {
					return true
				}
				hasBreak := false
				ast.Inspect(loop.Body, func(m ast.Node) bool {
					if branch, ok := m.(*ast.BranchStmt); ok && branch.Tok == token.BREAK {
						hasBreak = true
						return false
					}
					return !hasBreak
				})
				if hasBreak {
					return true
				}
				ast.Inspect(loop.Body, func(m ast.Node) bool {
					call, ok := m.(*ast.CallExpr)
					if !ok {
						return true
					}
					id, ok := call.Fun.(*ast.Ident)
					if ok && preds[id.Name] {
						proved = true
						return false
					}
					return true
				})
				return !proved
			})
			// Forwarders are proven only when the exact predicate parameter is handed to a
			// primitive whose identity has already been established.
			imports := importNames(owners[name])
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if proved {
					return false
				}
				call, ok := n.(*ast.CallExpr)
				if !ok || !trustedPollingCall(call, imports, trusted) {
					return true
				}
				for _, arg := range call.Args {
					if id, ok := arg.(*ast.Ident); ok && preds[id.Name] {
						proved = true
						return false
					}
				}
				return true
			})
			if proved {
				trusted[name] = true
				changed = true
			}
		}
	}
	return trusted
}

// leaderPremiseSites returns `rel: func` for every function containing a bare leadership read.
// Pure; shared with the self-check.
func leaderPremiseSites(f *ast.File, rel string) []string {
	return leaderPremiseSitesWithHelpers(f, rel, verifiedPollingHelpers([]*ast.File{f}))
}

func leaderPremiseSitesWithHelpers(f *ast.File, rel string, localHelpers map[string]bool) []string {
	var sites []string
	imports := importNames(f)
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		found := false
		// pollingLits are func literals that are arguments of a KNOWN polling helper call: safe,
		// skipped. Any other callee — whatever it is called — gets its literal treated as this body.
		pollingLits := map[*ast.FuncLit]bool{}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !trustedPollingCall(call, imports, localHelpers) {
				return true
			}
			for _, a := range call.Args {
				if lit, ok := a.(*ast.FuncLit); ok {
					pollingLits[lit] = true
				}
			}
			return true
		})
		var visit func(n ast.Node)
		visit = func(n ast.Node) {
			ast.Inspect(n, func(m ast.Node) bool {
				if found {
					return false
				}
				switch v := m.(type) {
				case *ast.FuncLit:
					return !pollingLits[v] // a known polling predicate is safe; any other literal is this body
				case *ast.ForStmt:
					// The condition is re-taken by the language — which only helps if nothing ACTS on
					// it before the next iteration. `for !n.IsLeader() { time.Sleep(d) }` waits;
					// `for n.IsLeader() { n.Mutate(); break }` observes then acts, and is reported
					// (external review F2). Init, post and body are never re-taken.
					if v.Cond != nil && containsLeaderRead(v.Cond) && loopBodyActs(v.Body, imports) {
						found = true
						return false
					}
					if v.Init != nil {
						visit(v.Init)
					}
					if v.Post != nil {
						visit(v.Post)
					}
					visit(v.Body)
					return false
				default:
					if isBareLeaderRead(m) {
						found = true
						return false
					}
				}
				return true
			})
		}
		visit(fd.Body)
		if found {
			sites = append(sites, rel+": "+guardFuncName(fd))
		}
	}
	return sites
}

func TestLeadershipIsNeverAssumedFromAStaleRead(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	seen := map[string]int{}
	type parsedTestFile struct {
		f   *ast.File
		rel string
	}
	var parsed []parsedTestFile
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if n := info.Name(); n == ".git" || n == "vendor" || n == "node_modules" || n == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		parsed = append(parsed, parsedTestFile{f: f, rel: filepath.ToSlash(rel)})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	packages := map[string][]*ast.File{}
	for _, pf := range parsed {
		key := filepath.Dir(pf.rel) + ":" + pf.f.Name.Name
		packages[key] = append(packages[key], pf.f)
	}
	trustedByPackage := map[string]map[string]bool{}
	for key, files := range packages {
		trustedByPackage[key] = verifiedPollingHelpers(files)
	}
	for _, pf := range parsed {
		key := filepath.Dir(pf.rel) + ":" + pf.f.Name.Name
		for _, s := range leaderPremiseSitesWithHelpers(pf.f, pf.rel, trustedByPackage[key]) {
			seen[s]++
			if !legacyLeaderPremiseSites[s] {
				offenders = append(offenders, s)
			}
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d test function(s) read leadership bare — outside a polling predicate or a for-condition — "+
			"and are not in the ledger:\n  %s\n\n"+
			"A leadership reading is stale one statement later (docs/testing-standards.md T3). Poll it in a "+
			"WaitForCond predicate, or act through clusterharness.WithLeader, which re-checks after the act.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	var stale []string
	for s := range legacyLeaderPremiseSites {
		if seen[s] == 0 {
			stale = append(stale, s)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d ledger entr(y/ies) in legacyLeaderPremiseSites no longer name a bare read:\n  %s", len(stale), strings.Join(stale, "\n  "))
	}
	if n := len(legacyLeaderPremiseSites); n > legacyLeaderPremiseSitesCap {
		t.Errorf("legacyLeaderPremiseSites has %d entries, cap is %d — this ledger only drains", n, legacyLeaderPremiseSitesCap)
	} else if n < legacyLeaderPremiseSitesCap {
		t.Errorf("legacyLeaderPremiseSites is down to %d entries but the cap says %d — lower the cap in the same commit", n, legacyLeaderPremiseSitesCap)
	}
}

// TestWithLeaderHasARealSuiteCaller: a helper with no caller guards nothing. plan B5 named the d3
// follower-PIN test as WithLeader's first adopter; the first landing shipped the helper, the gate
// and zero callers (internal review L6-F2). This keeps at least one real suite on it.
func TestWithLeaderHasARealSuiteCaller(t *testing.T) {
	root := repoRoot(t)
	callers := 0
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, "_test.go") {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "test/clusterharness/") || strings.HasPrefix(rel, "test/determinism/") {
			return nil // the helper's own tests and this gate's synthetic sample do not count
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(src), "clusterharness.WithLeader(") {
			callers++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if callers == 0 {
		t.Fatal("clusterharness.WithLeader has no caller outside its own package: the T3 primitive is dead code")
	}
}

// TestLeaderPremiseScannerSeesTheShapes is the G2 self-check: every safe shape, every reported one.
func TestLeaderPremiseScannerSeesTheShapes(t *testing.T) {
	src := `package synth

import (
	"time"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

func waitForCond(d time.Duration, pred func() bool) bool {
	for time.Now().Before(time.Now().Add(d)) {
		if pred() { return true }
	}
	return false
}

func bareRead(nodes []probe) int {
	for i, n := range nodes { _ = i; _ = n }
	if nodes[0].IsLeader() { return 0 } // reported: bare, outside any loop
	return -1
}

func polled(nodes []probe) {
	waitForCond(time.Second, func() bool { return nodes[0].IsLeader() }) // polling predicate: fine
	testharness.WaitFor(t, time.Second, 10*time.Millisecond, func() bool { return nodes[0].State() == raft.Leader })
}

func loopCondition(nodes []probe) {
	for !nodes[0].IsLeader() { time.Sleep(time.Millisecond) } // the condition itself: fine
}

// origin: docs/reviews/test-system-overhaul-external-review.md F2
func loopConditionThenActs(nodes []probe) {
	for nodes[0].IsLeader() { // reported: the observation is stale before the body acts
		nodes[0].Mutate()
		break
	}
}

// origin: docs/reviews/test-system-overhaul-external-rereview.md R1
func loopConditionAssigns(nodes []probe) {
	for nodes[0].IsLeader() { // reported: assignment is an action even without a call
		chosen = nodes[0]
		break
	}
}

func loopConditionMethodDisguise(nodes []probe) {
	for nodes[0].IsLeader() { // reported: a business Add method is not time.Time.Add
		nodes[0].Add()
		break
	}
}

func WaitFor(pred func() bool) { _ = pred() } // one-shot local shadow, not a polling primitive
func shadowedPollingHelper(nodes []probe) {
	WaitFor(func() bool { // reported: callee spelling alone is not helper identity
		if nodes[0].IsLeader() { nodes[0].Mutate(); return true }
		return false
	})
}

func waitUntil(pred func() bool) {
	for i := 0; i < 1; i++ { _ = pred() } // a loop shell does not prove repeated evaluation
}
func oneShotLoopHelper(nodes []probe) {
	waitUntil(func() bool { // reported: verified helper analysis rejects the one-shot loop
		return nodes[0].IsLeader()
	})
}

func misleadingHelperName(nodes []probe) {
	retryOnce(func() bool { // reported: a name prefix does not prove that this is a polling helper
		if nodes[0].IsLeader() { nodes[0].Mutate(); return true }
		return false
	})
}

func loopBody(nodes []probe) int {
	for attempt := 0; attempt < 5; attempt++ {
		if nodes[1].IsLeader() { continue } // reported: read once per iteration, then acted on
	}
	return 0
}

func rangeBody(nodes []probe) int {
	for i, n := range nodes {
		if n.IsLeader() { return i } // reported: the d7 adminForLeader shape
	}
	return -1
}

func viaHelper(nodes []LeaderProbe) {
	_, _ = clusterharness.WithLeader(t, nodes, time.Second, func(i int) error {
		_ = nodes[i].IsLeader() // polling helper argument: fine
		return nil
	})
}

func iife(nodes []probe) {
	if func() bool { return nodes[0].IsLeader() }() { return } // reported: evaluated once
}

func stateSpelling(n *raft.Raft) bool {
	return n.State() == raft.Leader // reported: the other spelling
}

func selectCase(nodes []probe) {
	select {
	case <-time.After(time.Second):
		_ = nodes[0].IsLeader() // reported: a select arm reads once
	}
}

func (c *cluster) methodBare() bool {
	return c.nodes[0].IsLeader() // reported, keyed (*cluster).methodBare
}

func notLeaderMethod(x other) {
	_ = x.IsLeader(1) // wrong arity: not the probe method
	_ = x.State() == other.Leader // not raft.Leader
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "synth_test.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := leaderPremiseSites(f, "synth_test.go")
	want := []string{
		"synth_test.go: bareRead",
		"synth_test.go: loopConditionThenActs",
		"synth_test.go: loopConditionAssigns",
		"synth_test.go: loopConditionMethodDisguise",
		"synth_test.go: shadowedPollingHelper",
		"synth_test.go: oneShotLoopHelper",
		"synth_test.go: misleadingHelperName",
		"synth_test.go: loopBody",
		"synth_test.go: rangeBody",
		"synth_test.go: iife",
		"synth_test.go: stateSpelling",
		"synth_test.go: selectCase",
		"synth_test.go: (*cluster).methodBare",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("leaderPremiseSites drifted:\n got  %v\n want %v", got, want)
	}
}
