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
//
// # TWO SYNTACTIC FORMS, ONE GATE
//
// The first version only matched composite-literal fields (`raft.Config{HeartbeatTimeout: 50ms}`,
// an *ast.KeyValueExpr). `c.HeartbeatTimeout = 50 * time.Millisecond` — an *ast.AssignStmt on a
// field selector — was invisible to it, and internal/cluster/prevote_test.go had carried three of
// those since it was written: 50ms heartbeat, election AND leader lease, neither reported nor
// exempted. That is the G2 shape ("matches nothing, reports perfect") in miniature: the gate was
// green because it could not see the form the offender used. Both forms now feed the same ordinal
// counter; the self-check below (gate-control) synthesises each form and asserts it is seen.
// origin: docs/reviews/test-system-overhaul-plan.md B0 (D8).
//
// gate-control: TestRaftTimingGuardSeesBothSyntacticForms
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
		// prevote_test.go exercises hashicorp/raft's PRE-VOTE semantics in isolation (partitioned
		// candidate must not disturb a stable cluster), not tether's production cadence. It is T2
		// exception 1 (an assertion that depends on fast failure detection): its waitForLeader budget
		// is 3s and the whole file runs in a few seconds at 50ms; at the production 1s heartbeat the
		// same assertions need a >=10s leader wait per case under -race. These three sites were never
		// reported before 2026-09-01 because the gate could not see assignment-form timings (see the
		// function comment); they are registered here rather than aligned because the value is the
		// fixture. Revisit if the file ever grows a step that races the production cadence.
		"internal/cluster/prevote_test.go:prevoteConfig#1": "HeartbeatTimeout 50ms — prevote semantics " +
			"test; the sub-second cadence IS the fixture (T2 exception 1). See comment above.",
		"internal/cluster/prevote_test.go:prevoteConfig#2": "ElectionTimeout 50ms — same fixture as #1.",
		"internal/cluster/prevote_test.go:prevoteConfig#3": "LeaderLeaseTimeout 50ms — same fixture as #1.",
		// transport_test.go's newRealTransportCluster is the same pre-vote family over the REAL mTLS
		// transport: TestD3PreVoteRealTransportDisabledControl asserts an isolated node inflates its
		// term by >=3 within a fixed 2s window, which only holds at a sub-second election cadence
		// (T2 exception 1). Aligning to the 1s production heartbeat would need a >=8s window in both
		// tests and would still be testing raft's pre-vote, not tether's cadence. These three were the
		// first NEW sites the assignment-form scan found (2026-09-01: 0 offenders before the scan
		// could see assignments, 6 after — prevote_test.go 3 + these 3 — all registered here).
		"internal/cluster/transport_test.go:newRealTransportCluster#1": "HeartbeatTimeout 200ms — real-" +
			"transport pre-vote fixture (T2 exception 1). See comment above.",
		"internal/cluster/transport_test.go:newRealTransportCluster#2": "ElectionTimeout 200ms — same fixture as #1.",
		"internal/cluster/transport_test.go:newRealTransportCluster#3": "LeaderLeaseTimeout 100ms — same fixture as #1.",
	}
	// Both ways: the stale check below stops entries from outliving their sites; the cap stops the
	// table from growing without a reason in the same change (internal review L1-F8).
	const raftTimingExemptionsCap = 7
	if n := len(exempt); n != raftTimingExemptionsCap {
		t.Errorf("raft timing exemptions has %d entries, cap says %d — move the cap in the same change, with the reason", n, raftTimingExemptionsCap)
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
		offenders = append(offenders, scanRaftTimingSites(f, rel, fields, exempt, seen)...)
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

// scanRaftTimingSites returns every non-conforming guarded-field site in one file, as
// `rel:func#ordinal  (Field)`, consulting `exempt` and marking `seen`. Two syntactic forms feed one
// ordinal counter per enclosing function:
//
//	raft.Config{HeartbeatTimeout: 50 * time.Millisecond}   // *ast.KeyValueExpr
//	c.HeartbeatTimeout = 50 * time.Millisecond             // *ast.AssignStmt on a selector
//
// Ordinals count NON-CONFORMING sites only, in source order, so an existing exemption key keeps its
// meaning when a conforming line is added above it. Shared by the gate and its self-check.
func scanRaftTimingSites(f *ast.File, rel string, fields, exempt map[string]string, seen map[string]bool) []string {
	var offenders []string
	ord := map[string]int{}
	record := func(fnName, field string) {
		fn := fnName
		if fn == "" {
			fn = "<file-scope>"
		}
		siteKey := rel + ":" + fn
		ord[siteKey]++
		site := siteKey + "#" + strconv.Itoa(ord[siteKey])
		if _, ok := exempt[site]; ok {
			seen[site] = true
			return
		}
		offenders = append(offenders, site+"  ("+field+")")
	}
	var walk func(fnName string, n ast.Node)
	walk = func(fnName string, n ast.Node) {
		ast.Inspect(n, func(m ast.Node) bool {
			switch v := m.(type) {
			case *ast.FuncDecl:
				if v.Body != nil {
					walk(guardFuncName(v), v.Body)
				}
				return false
			case *ast.KeyValueExpr:
				key, ok := v.Key.(*ast.Ident)
				if !ok {
					return true
				}
				expected, guarded := fields[key.Name]
				if !guarded || isExactTimingConstant(v.Value, expected) {
					return true
				}
				record(fnName, key.Name)
				return true
			case *ast.AssignStmt:
				// Positional pairing. A multi-assign with a single call RHS (`a.X, a.Y = f()`) has no
				// per-field expression to inspect, and a call result is never the exact constant, so
				// every guarded LHS in that shape is recorded — the conservative direction.
				paired := len(v.Lhs) == len(v.Rhs)
				for i, lhs := range v.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					expected, guarded := fields[sel.Sel.Name]
					if !guarded {
						continue
					}
					if paired && isExactTimingConstant(v.Rhs[i], expected) {
						continue
					}
					record(fnName, sel.Sel.Name)
				}
				return true
			}
			return true
		})
	}
	walk("", f)
	return offenders
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

// productTimingConstants are the broker's lease / probe / liveness time constants. A test that
// REFERENCES one of these and ALSO sleeps a literal duration in the same function is calibrating
// against the constant by hand — the shape that went stale twice when leaseGrantWindow moved from 1s
// to 5s (docs/testing-standards.md T7). Same-package tests only in this first version: a test in
// another package cannot reference an unexported constant, so it cannot trip this; the cross-package
// form is what LeaseGrantWindow() exists for.
var productTimingConstants = map[string]bool{
	"claimProbeBudget": true, "probeTTL": true, "backgroundProbeBudget": true,
	"leaseSubscribeSettle": true, "leaseGrantWindow": true, "inlineProbeGrace": true,
	"DefaultLeaseGrace": true, "DefaultStaleAfter": true,
	// The exported accessor is how a CROSS-PACKAGE test references leaseGrantWindow, so a test
	// that calls broker.LeaseGrantWindow() and also sleeps a literal is the same hand calibration
	// one package over (internal review N-2; the p2 restart test carried a 1500ms literal beside it).
	"LeaseGrantWindow": true,
}

// legacyProductTimingSleeps is the draining ledger for TestProductTimingSleepsReferenceTheConstant,
// keyed `path: func`. Empty on the day the gate landed (2026-09-01): the two stale sites it was
// written for were comments, not sleeps, and were corrected in B0/B3 instead of being registered.
var legacyProductTimingSleeps = map[string]bool{}

// legacyProductTimingSleepsCap pins the ledger both ways (internal review L1-F8): the stale check
// alone only says an entry must still match a site, not that the ledger may not grow.
const legacyProductTimingSleepsCap = 0

// scanProductTimingSleeps returns `rel: func` for every test function that both references a
// product timing constant and calls time.Sleep with a literal-only duration argument. Pure; shared
// with the self-check.
func scanProductTimingSleeps(f *ast.File, rel string) []string {
	var sites []string
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		refs, literalSleep := false, false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.Ident:
				if productTimingConstants[v.Name] {
					refs = true
				}
			case *ast.CallExpr:
				// time.Sleep(d) and time.After(d) — the second covers `<-time.After(1100 *
				// time.Millisecond)` and `case <-time.After(...)`, the same hand calibration in a
				// different spelling (internal review L1-F6).
				sel, ok := v.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Sleep" && sel.Sel.Name != "After") || len(v.Args) != 1 {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "time" {
					return true
				}
				if isLiteralOnlyDuration(v.Args[0]) {
					literalSleep = true
				}
			}
			return true
		})
		if refs && literalSleep {
			sites = append(sites, rel+": "+guardFuncName(fd))
		}
	}
	return sites
}

// isLiteralOnlyDuration is true for `1100 * time.Millisecond`, `2 * time.Second`, `time.Second` —
// an expression built from numeric literals and time unit selectors only. `probeTTL / 10` or
// `leaseGrantWindow + time.Second` reference the constant and are exactly what the rule wants.
func isLiteralOnlyDuration(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return true
	case *ast.SelectorExpr:
		pkg, ok := v.X.(*ast.Ident)
		return ok && pkg.Name == "time"
	case *ast.BinaryExpr:
		return isLiteralOnlyDuration(v.X) && isLiteralOnlyDuration(v.Y)
	case *ast.ParenExpr:
		return isLiteralOnlyDuration(v.X)
	}
	return false
}

// gate-control: TestProductTimingSleepGateSeesTheShapes
func TestProductTimingSleepsReferenceTheConstant(t *testing.T) {
	root := repoRoot(t)
	if n := len(legacyProductTimingSleeps); n != legacyProductTimingSleepsCap {
		t.Errorf("legacyProductTimingSleeps has %d entries, cap says %d — move the cap in the same change, with the reason", n, legacyProductTimingSleepsCap)
	}
	var offenders []string
	seen := map[string]bool{}
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
		for _, s := range scanProductTimingSleeps(f, filepath.ToSlash(rel)) {
			seen[s] = true
			if !legacyProductTimingSleeps[s] {
				offenders = append(offenders, s)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d test function(s) reference a product timing constant AND sleep a literal duration:\n  %s\n\n"+
			"Sleep in terms of the constant (leaseGrantWindow+time.Second, probeTTL/10) or drive the "+
			"injected clock; a literal calibrated by hand goes silently stale when the constant moves "+
			"(docs/testing-standards.md T7).", len(offenders), strings.Join(offenders, "\n  "))
	}
	var stale []string
	for s := range legacyProductTimingSleeps {
		if !seen[s] {
			stale = append(stale, s)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d stale entr(y/ies) in legacyProductTimingSleeps:\n  %s", len(stale), strings.Join(stale, "\n  "))
	}
}

// TestProductTimingSleepGateSeesTheShapes is the G2 self-check for scanProductTimingSleeps (G2b:
// the live tree's success state is ZERO sites, so the shapes are synthetic).
func TestProductTimingSleepGateSeesTheShapes(t *testing.T) {
	src := `package synth

import "time"

func TestLiteralNextToConstant(t *testing.T) {
	_ = probeTTL
	time.Sleep(1100 * time.Millisecond) // reported
}

func TestConstantDerived(t *testing.T) {
	time.Sleep(probeTTL / 10) // fine: expressed in the constant
	time.Sleep(leaseGrantWindow + time.Second)
}

func TestLiteralWithoutConstant(t *testing.T) {
	time.Sleep(2 * time.Second) // fine: no product constant in sight
}

func TestSelectorConstant(t *testing.T) {
	_ = broker.DefaultLeaseGrace
	time.Sleep(time.Second) // reported: DefaultLeaseGrace referenced via selector
}

func TestAfterSpelling(t *testing.T) {
	_ = probeTTL
	<-time.After(1100 * time.Millisecond) // reported: the same calibration spelled with time.After
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "synth_test.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := scanProductTimingSleeps(f, "synth_test.go")
	want := []string{"synth_test.go: TestLiteralNextToConstant", "synth_test.go: TestSelectorConstant", "synth_test.go: TestAfterSpelling"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("scanProductTimingSleeps drifted:\n got  %v\n want %v", got, want)
	}
}

// TestRaftTimingGuardSeesBothSyntacticForms is the G2 self-check for scanRaftTimingSites: one
// synthetic file carrying every form the gate must report and every form it must ignore, run through
// the SAME function the gate uses. Removing the AssignStmt arm reddens this before it re-blinds the
// gate to prevote_test.go's shape.
func TestRaftTimingGuardSeesBothSyntacticForms(t *testing.T) {
	src := `package synth

import "time"

func literalForm() {
	_ = raft.Config{
		HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout, // conforming: not counted
		ElectionTimeout:    80 * time.Millisecond,             // #1
		LeaderLeaseTimeout: testLease,                         // #2 (alias, not the constant)
		CommitTimeout:      5 * time.Millisecond,              // unguarded field
	}
}

func assignForm(c *raft.Config) {
	c.HeartbeatTimeout = 60 * time.Millisecond              // #1
	c.ElectionTimeout = cluster.MultinodeElectionTimeout    // conforming
	c.CommitTimeout = 5 * time.Millisecond                  // unguarded
	c.LeaderLeaseTimeout, c.HeartbeatTimeout = pick(), pick() // #2, #3 (call results are never the constant)
	x := 50 * time.Millisecond                               // := on an ident, not a field
	_ = x
}

func mixedForms(c *raft.Config) {
	_ = raft.Config{HeartbeatTimeout: 10 * time.Millisecond} // #1
	c.ElectionTimeout = 10 * time.Millisecond               // #2 — same counter as the literal above
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "synth_test.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic source: %v", err)
	}
	fields := map[string]string{
		"HeartbeatTimeout":   "MultinodeHeartbeatTimeout",
		"ElectionTimeout":    "MultinodeElectionTimeout",
		"LeaderLeaseTimeout": "MultinodeLeaderLeaseTimeout",
	}
	exempt := map[string]string{"synth_test.go:assignForm#3": "exercise the exempt path"}
	seen := map[string]bool{}
	got := scanRaftTimingSites(f, "synth_test.go", fields, exempt, seen)
	sort.Strings(got)
	want := []string{
		"synth_test.go:assignForm#1  (HeartbeatTimeout)",
		"synth_test.go:assignForm#2  (LeaderLeaseTimeout)",
		"synth_test.go:literalForm#1  (ElectionTimeout)",
		"synth_test.go:literalForm#2  (LeaderLeaseTimeout)",
		"synth_test.go:mixedForms#1  (HeartbeatTimeout)",
		"synth_test.go:mixedForms#2  (ElectionTimeout)",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("scanRaftTimingSites drifted:\n got:\n  %s\n want:\n  %s",
			strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	if !seen["synth_test.go:assignForm#3"] {
		t.Fatal("the exempt assignment-form site was not marked seen — exemptions would go stale for that form")
	}
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
