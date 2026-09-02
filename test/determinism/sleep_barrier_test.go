package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// sleep_barrier_test.go — readiness helpers (startBroker / startAgent / seedSession) must not use a
// bare time.Sleep as a barrier.
//
// WHY THIS EXISTS
// ---------------
// docs/testing-standards.md T1 says timeouts must be written for the environment the test really
// runs in — `-race` plus twenty parallel workers — and docs/reviews/parallel-flake-rootcause.md
// root cause 3 is what a bare `time.Sleep(50 * time.Millisecond) // let subscribes settle` turns
// into under that load. The 16 hand-copied startBroker/startAgent helpers each carry their own
// number; internal/testharness.WaitNodeOnline was written to replace "the time.Sleep(150ms) then
// time.Sleep(300ms) stack that dominates phase test helpers" and 21 of those stacks are still
// there. This gate freezes the count: existing sites sit in a site-keyed draining ledger; a new
// bare sleep in a readiness helper is red; `// sleep-fixture: <why>` on the line (or the one above)
// declares that the sleep IS the fixture (a deliberate restart gap, a debounce being measured)
// rather than a barrier.
//
// SCOPE IS DELIBERATELY NARROW: helper functions whose name starts with startBroker / startAgent /
// seedSession, not the 315 sleeps across the whole test tree. Root-cause analysis found no flake
// caused by a sleep OUTSIDE a readiness helper; widening the scan would spend calibration on a
// class with no incident behind it (docs/reviews/test-system-overhaul-plan.md §0 A12).
//
// gate-control: TestSleepBarrierScannerSeesTheShapes

var readinessHelperRe = regexp.MustCompile(`^(startBroker|startAgent|seedSession)`)

const sleepFixtureMarker = "sleep-fixture:"

// sleepBarrierSites returns `rel: func` for every bare time.Sleep / `<-time.After` statement in the
// body of a readiness helper — including inside a loop or select that OBSERVES nothing (a loop that
// only counts and sleeps, a select whose every arm is a timer, are barriers wearing control flow;
// external review F3 / rereview R2 define "observes" as control-flow evidence: the loop or range
// source, an if/switch condition, a receive, a non-timer select arm — never a bare call). Not
// inside a func literal (that is somebody else's body, unless it is an IIFE), and not marked
// sleep-fixture. Pure; shared with the self-check.
func sleepBarrierSites(fset *token.FileSet, f *ast.File, src string, rel string) []string {
	// A marker on a comment-only line covers the NEXT line; a trailing marker covers ITS line only.
	// Without the distinction, `time.Sleep(a) // sleep-fixture: x` on line N also exempted an
	// unmarked sleep on line N+1 — the self-check caught that on the first run.
	sameLine, lineAbove := map[int]bool{}, map[int]bool{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if !strings.Contains(c.Text, sleepFixtureMarker) {
				continue
			}
			pos := fset.Position(c.Pos())
			if commentOnlyLine(src, pos.Offset) {
				lineAbove[pos.Line] = true
			} else {
				sameLine[pos.Line] = true
			}
		}
	}
	var sites []string
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil || !readinessHelperRe.MatchString(fd.Name.Name) {
			continue
		}
		report := func(pos token.Pos) {
			line := fset.Position(pos).Line
			if sameLine[line] || lineAbove[line-1] {
				return
			}
			sites = append(sites, rel+": "+fd.Name.Name)
		}
		var visit func(n ast.Node)
		visit = func(n ast.Node) {
			ast.Inspect(n, func(m ast.Node) bool {
				switch v := m.(type) {
				case *ast.ForStmt:
					// A loop is polling only if it OBSERVES something between sleeps (a call, a
					// receive, a condition with a call in it). A loop that only counts and sleeps —
					// `for i := 0; i < 1; i++ { time.Sleep(d) }` — is a barrier wearing a loop, and the
					// first version skipped every loop on sight (external review F3).
					if loopObserves(v.Cond, v.Body) {
						return false
					}
					visit(v.Body)
					return false
				case *ast.RangeStmt:
					// The range source controls whether the body runs. A call/receive there is
					// observation evidence; ignoring it misclassifies channel-producing helpers.
					if loopObserves(v.X, v.Body) {
						return false
					}
					visit(v.Body)
					return false
				case *ast.SelectStmt:
					// A select whose every arm is a timer is `time.Sleep` with extra steps
					// (`select { case <-time.After(d): }`); a select with a non-timer arm waits for
					// something and the timer is its bound (external review F3).
					if pos, ok := timerOnlySelectPos(v); ok {
						report(pos)
					}
					return false
				case *ast.FuncLit:
					// Somebody else's body — unless it is called on the spot. An IIFE
					// `func() { time.Sleep(d) }()` is straight-line code wearing a closure, and the
					// first version let it through (internal review L1-F6). The IIFE case is handled
					// at the ExprStmt below by descending into the literal explicitly.
					return false
				case *ast.ExprStmt:
					if call, ok := v.X.(*ast.CallExpr); ok {
						if lit, ok := call.Fun.(*ast.FuncLit); ok {
							visit(lit.Body) // IIFE: its body is this body
							return false
						}
					}
					if isBareSleepStmt(v) {
						report(v.Pos())
					}
					return true
				}
				return true
			})
		}
		visit(fd.Body)
	}
	return sites
}

// isBareSleepStmt recognises both spellings of "block for a duration": `time.Sleep(d)` and
// `<-time.After(d)` as a statement. The second was invisible to the first version (its ExprStmt.X is
// a UnaryExpr, not a CallExpr) and is the one-token rewrite that would have taken any ledgered
// site out of the ledger without changing what it does (internal review L1-F6).
func isBareSleepStmt(s *ast.ExprStmt) bool {
	x := s.X
	if u, ok := x.(*ast.UnaryExpr); ok && u.Op == token.ARROW {
		x = u.X
	}
	call, ok := x.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Sleep" && sel.Sel.Name != "After") {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "time"
}

// isTimerCall reports whether e is `time.Sleep(…)` or `time.After(…)`.
func isTimerCall(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Sleep" && sel.Sel.Name != "After") {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "time"
}

// isClockOnlyCall recognises calls whose result is only wall-clock/timer state. A deadline check
// does not observe readiness: `for time.Now().Before(deadline) { time.Sleep(d) }` is still a fixed
// barrier. Ambiguous time.Time-style method names are treated as clock-only too: without type
// information, fail-closed means they cannot make a fixed sleep disappear; a genuine readiness
// method with one of these names can use the explicit ledger.
func isClockOnlyCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "time" {
		return true
	}
	switch sel.Sel.Name {
	case "After", "Before", "Equal", "Add", "Sub", "Round", "Truncate", "Since", "Until":
		return true
	}
	return false
}

// loopObserves reports whether control flow is driven by an observation: the loop/range source,
// an if/switch/nested-loop condition, a channel receive, or a select with a non-timer arm. An
// arbitrary expression call in the body is NOT evidence — `doWork(); time.Sleep(d)` remains a
// fixed barrier (external rereview R2).
func loopObserves(cond ast.Expr, body *ast.BlockStmt) bool {
	controlObserves := func(e ast.Node) bool {
		observes := false
		if e == nil {
			return false
		}
		ast.Inspect(e, func(n ast.Node) bool {
			if observes {
				return false
			}
			switch v := n.(type) {
			case *ast.CallExpr:
				if !isClockOnlyCall(v) {
					observes = true
				}
			case *ast.UnaryExpr:
				if v.Op == token.ARROW && !isTimerCall(v.X) {
					observes = true
				}
			}
			return !observes
		})
		return observes
	}
	containsReceive := func(n ast.Node) bool {
		observes := false
		ast.Inspect(n, func(m ast.Node) bool {
			if observes {
				return false
			}
			if v, ok := m.(*ast.UnaryExpr); ok && v.Op == token.ARROW && !isTimerCall(v.X) {
				observes = true
				return false
			}
			return true
		})
		return observes
	}
	containsProbeCall := func(n ast.Node) bool {
		observes := false
		ast.Inspect(n, func(m ast.Node) bool {
			if observes {
				return false
			}
			if call, ok := m.(*ast.CallExpr); ok && !isClockOnlyCall(call) {
				observes = true
				return false
			}
			return true
		})
		return observes
	}
	usesProbeResult := func(n ast.Node, results map[string]bool) bool {
		uses := false
		ast.Inspect(n, func(m ast.Node) bool {
			if uses {
				return false
			}
			if id, ok := m.(*ast.Ident); ok && results[id.Name] {
				uses = true
				return false
			}
			return true
		})
		return uses
	}
	if controlObserves(cond) {
		return true
	}
	observes := false
	var inspectBlock func(*ast.BlockStmt)
	inspectBlock = func(block *ast.BlockStmt) {
		if block == nil || observes {
			return
		}
		probeResults := map[string]bool{}
		for _, stmt := range block.List {
			if observes {
				return
			}
			switch v := stmt.(type) {
			case *ast.IfStmt:
				// Calls in an init statement feed the immediately following condition:
				// `if nc, err := nats.Connect(url); err == nil` is a real probe.
				observes = controlObserves(v.Init) || controlObserves(v.Cond) || usesProbeResult(v.Cond, probeResults)
				inspectBlock(v.Body)
				if elseBlock, ok := v.Else.(*ast.BlockStmt); ok {
					inspectBlock(elseBlock)
				}
			case *ast.SwitchStmt:
				observes = controlObserves(v.Init) || controlObserves(v.Tag) || usesProbeResult(v.Tag, probeResults)
				for _, clause := range v.Body.List {
					if cc, ok := clause.(*ast.CaseClause); ok {
						inspectBlock(&ast.BlockStmt{List: cc.Body})
					}
				}
			case *ast.ForStmt:
				observes = controlObserves(v.Init) || controlObserves(v.Cond) || usesProbeResult(v.Cond, probeResults)
				inspectBlock(v.Body)
			case *ast.RangeStmt:
				observes = controlObserves(v.X)
				inspectBlock(v.Body)
			case *ast.SelectStmt:
				observes = !timerOnlySelect(v)
			case *ast.AssignStmt:
				probeCall := false
				for _, rhs := range v.Rhs {
					if containsReceive(rhs) {
						observes = true
						break
					}
					probeCall = probeCall || containsProbeCall(rhs)
				}
				if probeCall {
					for _, lhs := range v.Lhs {
						if id, ok := lhs.(*ast.Ident); ok {
							probeResults[id.Name] = true
						}
					}
				}
			case *ast.ExprStmt:
				observes = containsReceive(v.X)
			case *ast.BlockStmt:
				inspectBlock(v)
			}
		}
	}
	inspectBlock(body)
	return observes
}

// timerOnlySelect reports whether every arm of a select is a receive from time.After (no default,
// no other channel): `select { case <-time.After(d): }` is a sleep.
func timerOnlySelect(s *ast.SelectStmt) bool {
	_, ok := timerOnlySelectPos(s)
	return ok
}

// timerOnlySelectPos returns the first timer receive, so a trailing sleep-fixture marker on the
// line that actually blocks exempts the select. Reporting s.Pos() would look at the `select` line
// instead and make the documented same-line marker ineffective.
func timerOnlySelectPos(s *ast.SelectStmt) (token.Pos, bool) {
	if len(s.Body.List) == 0 {
		return token.NoPos, false
	}
	var first token.Pos
	for _, c := range s.Body.List {
		cc, ok := c.(*ast.CommClause)
		if !ok || cc.Comm == nil {
			return token.NoPos, false // default: not a timer
		}
		var recv ast.Expr
		switch st := cc.Comm.(type) {
		case *ast.ExprStmt:
			recv = st.X
		case *ast.AssignStmt:
			if len(st.Rhs) == 1 {
				recv = st.Rhs[0]
			}
		}
		u, ok := recv.(*ast.UnaryExpr)
		if !ok || u.Op != token.ARROW || !isTimerCall(u.X) {
			return token.NoPos, false
		}
		if first == token.NoPos {
			first = u.Pos()
		}
	}
	return first, true
}

// commentOnlyLine reports whether the text before byte offset `off` on its line is blank.
func commentOnlyLine(src string, off int) bool {
	if off > len(src) {
		off = len(src)
	}
	start := strings.LastIndex(src[:off], "\n") + 1
	return strings.TrimSpace(src[start:off]) == ""
}

func TestReadinessHelpersDoNotSleepAsABarrier(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	seen := map[string]int{}
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
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, src, parser.ParseComments)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		for _, s := range sleepBarrierSites(fset, f, string(src), filepath.ToSlash(rel)) {
			seen[s]++
			if !legacySleepBarriers[s] {
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
		t.Errorf("%d bare time.Sleep barrier(s) in readiness helpers outside the ledger:\n  %s\n\n"+
			"Wait on the thing the test depends on (testharness.WaitNodeOnline for an agent's row; a short "+
			"nc.Request against the broker's subject, or a DB predicate, for the broker — NOT WaitConnect, "+
			"which only proves NATS accepts connections), or mark the sleep `// sleep-fixture: <why>` when "+
			"the sleep itself is what the test measures.", len(offenders), strings.Join(offenders, "\n  "))
	}
	var stale []string
	for s := range legacySleepBarriers {
		if seen[s] == 0 {
			stale = append(stale, s)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d ledger entr(y/ies) in legacySleepBarriers no longer name a bare sleep:\n  %s\n\n"+
			"Delete them — this ledger only drains.", len(stale), strings.Join(stale, "\n  "))
	}
	if n := len(legacySleepBarriers); n > legacySleepBarriersCap {
		t.Errorf("legacySleepBarriers has %d entries, cap is %d — this ledger only drains", n, legacySleepBarriersCap)
	} else if n < legacySleepBarriersCap {
		t.Errorf("legacySleepBarriers is down to %d entries but the cap says %d — lower the cap in the same commit", n, legacySleepBarriersCap)
	}
}

// TestSleepBarrierScannerSeesTheShapes is the G2 self-check: positive and negative shapes through the
// same scanner the gate uses.
func TestSleepBarrierScannerSeesTheShapes(t *testing.T) {
	src := `package synth

import "time"

func startBroker(t *testing.T) {
	time.Sleep(50 * time.Millisecond) // reported: bare barrier
}

func startAgentWithAuth(t *testing.T) {
	// sleep-fixture: the restart gap is what this fixture models
	time.Sleep(1200 * time.Millisecond)
}

func seedSession(t *testing.T) {
	// A polling loop OBSERVES between sleeps (ready() here); the sample's first version counted
	// and slept only, which under external review F3's rule is exactly the one-shot-loop barrier
	// with a bigger count.
	for i := 0; i < 10 && !ready(); i++ {
		time.Sleep(10 * time.Millisecond) // polling loop, not a barrier
	}
	go func() { time.Sleep(time.Second) }() // somebody else's body
	select {
	case <-done: // waiting for a signal, bounded by the timer arm: not a barrier
	case <-time.After(time.Second):
	}
}

func startBrokerManual(t *testing.T) {
	time.Sleep(time.Second) // sleep-fixture: same-line marker
	time.Sleep(time.Second) // reported: the marker above does not reach here
}

func startBrokerAfter(t *testing.T) {
	<-time.After(50 * time.Millisecond) // reported: the other spelling of the same barrier
}

func startBrokerIIFE(t *testing.T) {
	func() { time.Sleep(50 * time.Millisecond) }() // reported: an IIFE is this body
}

// origin: docs/reviews/test-system-overhaul-external-review.md F3
func startBrokerOneShotLoop(t *testing.T) {
	for i := 0; i < 1; i++ {
		time.Sleep(50 * time.Millisecond) // reported: a one-shot loop is still a barrier
	}
}

// origin: docs/reviews/test-system-overhaul-external-rereview.md R2
func startBrokerCallThenOneShotSleep(t *testing.T) {
	for i := 0; i < 1; i++ {
		doWork() // an arbitrary call is not a readiness observation
		time.Sleep(50 * time.Millisecond) // reported: still a fixed barrier
	}
}

func startBrokerDeadlineOnly(t *testing.T) {
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond) // reported: a clock is not a readiness probe
	}
}

func startBrokerChannelRange(t *testing.T) {
	for range readyEvents() { // source call controls iteration: this is an observation
		time.Sleep(10 * time.Millisecond)
	}
}

func startBrokerTimerSelect(t *testing.T) {
	select {
	case <-time.After(50 * time.Millisecond): // reported: a one-case timer select is still a barrier
	}
}

func startBrokerTimerSelectFixture(t *testing.T) {
	select {
	case <-time.After(50 * time.Millisecond): // sleep-fixture: the timer itself is the fixture
	}
}

func helperNotReadiness(t *testing.T) {
	time.Sleep(time.Second) // out of scope by name
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synth_test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	got := sleepBarrierSites(fset, f, src, "synth_test.go")
	want := []string{"synth_test.go: startBroker", "synth_test.go: startBrokerManual",
		"synth_test.go: startBrokerAfter", "synth_test.go: startBrokerIIFE",
		"synth_test.go: startBrokerOneShotLoop", "synth_test.go: startBrokerCallThenOneShotSleep",
		"synth_test.go: startBrokerDeadlineOnly", "synth_test.go: startBrokerTimerSelect"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sleepBarrierSites drifted:\n got  %v\n want %v", got, want)
	}
}
