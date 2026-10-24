package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/test/transferharness"
)

// inventory_test.go — the matrix's unit list is pinned in a golden that refuses to shrink, every
// `go test` fork in the tree lives in test/e2e, and every leak-risky package is run under -race by
// some matrix.
//
// WHY THIS EXISTS
// ---------------
// all_phases_test.go's package lists are hand-typed, and its own B5 note says deleting a glob there
// is SILENT: the runner's coverage self-check and its reconcile both derive "what should run" from
// the same file, so they can only prove "everything the file declares ran", never "the file still
// declares what it declared yesterday". The golden here is the cross-commit half — the same shape as
// the test-function inventory (test/determinism/test_inventory_test.go), one level up: a unit is
// (matrix, pkg, tags, race, hasRun, runFilter, extra). It lives in THIS package because
// splitMatrices is the runner's own parser and this is `package main`: nothing else can import it,
// and a second parser would be the fifth "silently ran less" (docs/reviews/e2e-parallel-plan.md §4).
//
// WHOLE MATRICES ARE NOT OPAQUE. Five matrices have a shape the splitter cannot parse (a `run`
// helper, a range over allPhases) and are scheduled whole. The first version keyed them as one line
// each — `TestRemoteFSMatrix|<whole matrix: unparsed shape>` — so a `run("pty", "./internal/pty/...")`
// deleted from inside one of them changed nothing anywhere (internal review L4-F1: the golden, the
// reconcile and the -race attribution were all blind to 9 inline commands and the 11-entry allPhases
// list). Now each whole matrix also contributes one `|<whole>|lit=…` line per package literal, per
// `-run` value and per allPhases element found in its body and the same-file functions it calls.
//
// -update-matrix-inventory appends; it refuses to remove a line. Removing a unit is a decision
// made by hand in the golden with the reason in the commit message.
//
// gate-control: TestMatrixInventoryKeySeesEveryField

const matrixUnitsGolden = "testdata/matrix_units_golden.txt"

var updateMatrixInventory = flag.Bool("update-matrix-inventory", false,
	"append newly declared matrix units to the golden (never removes a line)")

// Direct and parallel entry points cannot share a Go package. Compare their
// actual listing loops so either reader losing package custody is independently
// red; both readers changing together still face the executable shape controls.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func partitionListingLoop(root, path, name string) (string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, 0)
	if err != nil {
		return "", err
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Body == nil {
			continue
		}
		for _, stmt := range fn.Body.List {
			if loop, ok := stmt.(*ast.RangeStmt); ok {
				var out bytes.Buffer
				err := format.Node(&out, token.NewFileSet(), loop)
				return out.String(), err
			}
		}
	}
	return "", fmt.Errorf("%s: missing package listing loop %s", path, name)
}

// Package-qualified identities retain generic round-robin sharding as well as
// mandatory per-test owners. Equal names in two packages remain separate work.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func checkPartitionGenericBuckets(t *testing.T) {
	t.Helper()
	var names []string
	for _, pkg := range []string{"./internal/cluster", "./internal/cluster/child"} {
		for i := 0; i < 16; i++ {
			names = append(names, fmt.Sprintf("%s:TestCase%d", pkg, i))
		}
	}
	u := unit{matrix: "TestD4Matrix", pkg: "./internal/cluster/...", name: "cluster", race: true, timeo: "600s"}
	units, err := shardPackagesWithList([]unit{u}, 8, func(unit) ([]string, error) { return names, nil })
	if err != nil || len(units) != 16 {
		t.Fatalf("generic package shards lost their buckets: %+v %v", units, err)
	}
	for _, entry := range names {
		pkg, name := partitionIdentity(u.pkg, entry)
		count := 0
		for _, unit := range units {
			selector, err := regexp.Compile(unit.runFilter)
			if err != nil || unit.tags != u.tags || !unit.race || unit.timeo != u.timeo {
				t.Fatalf("generic shard changed its contract: %+v %v", unit, err)
			}
			if unit.pkg == pkg && selector.MatchString(name) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%s: covered by %d generic shards", entry, count)
		}
	}
}

func unitKey(u unit) string {
	if u.isWhole() {
		return u.matrix + "|<whole matrix: unparsed shape>"
	}
	return fmt.Sprintf("%s|%s|tags=%s|race=%v|run=%q|extra=%q", u.matrix, u.pkg, u.tags, u.race, u.runFilter, u.extra)
}

// wholeMatrixLiterals returns, per unparsed matrix, the sorted literals that describe what it runs:
// every string literal starting with `./` (a package pattern), every `-run` value that follows a
// `"-run"` literal in a call, and every element of a package-level string-slice var the matrix
// ranges over (allPhases), as `phase <x>`. Same-file functions the matrix calls (runPhase) are
// walked too. Pure over the parsed file; shared with the self-check.
func wholeMatrixLiterals(f *ast.File, unparsed []string) map[string][]string {
	funcs := map[string]*ast.FuncDecl{}
	vars := map[string]*ast.CompositeLit{}
	for _, d := range f.Decls {
		switch x := d.(type) {
		case *ast.FuncDecl:
			if x.Body != nil && x.Recv == nil {
				funcs[x.Name.Name] = x
			}
		case *ast.GenDecl:
			for _, s := range x.Specs {
				vs, ok := s.(*ast.ValueSpec)
				if !ok || len(vs.Names) != len(vs.Values) {
					continue
				}
				for i := range vs.Names {
					if cl, ok := vs.Values[i].(*ast.CompositeLit); ok {
						vars[vs.Names[i].Name] = cl
					}
				}
			}
		}
	}
	unquote := func(l *ast.BasicLit) (string, bool) {
		if l == nil || l.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(l.Value)
		return s, err == nil
	}
	out := map[string][]string{}
	for _, m := range unparsed {
		set := map[string]bool{}
		seen := map[string]bool{}
		var walk func(name string)
		walk = func(name string) {
			fd := funcs[name]
			if fd == nil || seen[name] {
				return
			}
			seen[name] = true
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok {
						walk(id.Name)
					}
					for i := 0; i+1 < len(x.Args); i++ {
						if flag, ok := unquote(litOf(x.Args[i])); ok && flag == "-run" {
							if v, ok := unquote(litOf(x.Args[i+1])); ok {
								set["-run "+v] = true
							}
						}
					}
				case *ast.BasicLit:
					if s, ok := unquote(x); ok && strings.HasPrefix(s, "./") {
						set[s] = true
					}
				case *ast.Ident:
					if cl, ok := vars[x.Name]; ok {
						for _, e := range cl.Elts {
							if s, ok := unquote(litOf(e)); ok {
								set["phase "+s] = true
							}
						}
					}
				}
				return true
			})
		}
		walk(m)
		var lits []string
		for l := range set {
			lits = append(lits, l)
		}
		sort.Strings(lits)
		out[m] = lits
	}
	return out
}

func litOf(e ast.Expr) *ast.BasicLit {
	l, _ := e.(*ast.BasicLit)
	return l
}

func parseMatrixFile(t *testing.T, root string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "test", "e2e", "all_phases_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func currentMatrixUnits(t *testing.T) (keys map[string]bool, matrices int) {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, unparsed, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	keys = map[string]bool{}
	seen := map[string]bool{}
	for _, u := range units {
		keys[unitKey(u)] = true
		// The whole declaration remains the recursive discovery owner; these
		// additional rows preserve each measured boundary's receipt identity.
		// origin: hot-update S7 plan lead L17 / TRANSFER-OVERLAY-REPAIR F5
		if u.pkg == "./test/hotupdate/..." && u.tags == "hotupdate_integration" && !u.hasRun && u.runFilter == "" {
			for _, name := range []string{"TestTransferAgentExecAcrossEveryBoundary", "TestTransferBrokerExecKeepsTrackerBeforeReaper", "TestTransferCarrierAndJSRestartReceiptsAreDistinct"} {
				for _, selector := range transferharness.BoundarySelectors(u.pkg, name, u.tags) {
					cell := u
					cell.pkg = "./test/hotupdate"
					cell.runFilter = selector
					keys[unitKey(cell)] = true
				}
			}
		}
		seen[u.matrix] = true
	}
	lits := wholeMatrixLiterals(parseMatrixFile(t, root), unparsed)
	for _, m := range unparsed {
		keys[unitKey(unit{matrix: m, whole: true})] = true
		seen[m] = true
		// Non-vacuity: a whole matrix that runs nothing the scan can see is a scan that has gone
		// blind, not a matrix with nothing in it.
		if len(lits[m]) == 0 {
			t.Fatalf("whole matrix %s: the literal scan found no package pattern, -run value or phase — it is blind", m)
		}
		for _, l := range lits[m] {
			keys[m+"|<whole>|lit="+l] = true
		}
	}
	return keys, len(seen)
}

func readGolden(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v\n\nFirst run? go test ./test/e2e/parallel/ -run TestMatrixUnitInventoryOnlyGrows -update-matrix-inventory", path, err)
	}
	out := map[string]bool{}
	for _, l := range strings.Split(string(raw), "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		out[l] = true
	}
	return out
}

func writeGolden(t *testing.T, path string, keys map[string]bool) {
	t.Helper()
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var b strings.Builder
	b.WriteString("# matrix_units_golden.txt — every unit test/e2e/all_phases_test.go declares, as the parallel runner\n")
	b.WriteString("# parses it: matrix|pkg|tags|race|run|extra, plus one `<whole>|lit=` line per package pattern / -run value /\n")
	b.WriteString("# phase inside each unparsed (whole) matrix. Maintained by test/e2e/parallel/inventory_test.go. Only GROWS:\n")
	b.WriteString("#   new unit          -> go test ./test/e2e/parallel/ -run TestMatrixUnitInventoryOnlyGrows -update-matrix-inventory\n")
	b.WriteString("#   removed / changed -> hand-edit the line and say why in the commit message. Deleting a glob in\n")
	b.WriteString("#                        all_phases_test.go is silent everywhere else (its own B5 note); here it is red.\n")
	for _, k := range sorted {
		b.WriteString(k)
		b.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMatrixUnitInventoryOnlyGrows(t *testing.T) {
	current, matrices := currentMatrixUnits(t)
	if matrices < 15 {
		t.Fatalf("parsed only %d matrices from all_phases_test.go — the parser or the file moved", matrices)
	}
	goldenPath := filepath.Join("testdata", "matrix_units_golden.txt")
	if _, err := os.Stat(goldenPath); os.IsNotExist(err) && *updateMatrixInventory {
		writeGolden(t, goldenPath, current)
		t.Logf("wrote initial matrix inventory: %d units", len(current))
		return
	}
	golden := readGolden(t, goldenPath)
	var missing, extra []string
	for k := range golden {
		if !current[k] {
			missing = append(missing, k)
		}
	}
	for k := range current {
		if !golden[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("%d matrix unit(s) recorded in the golden are no longer declared by all_phases_test.go:\n  %s\n\n"+
			"A package dropped from a matrix, a -race removed, a -tags changed, a run(...) line deleted from a whole matrix. "+
			"If it is deliberate, remove the line from %s by hand and say why in the commit message; -update-matrix-inventory will not.",
			len(missing), strings.Join(missing, "\n  "), matrixUnitsGolden)
		if *updateMatrixInventory {
			t.Fatalf("-update-matrix-inventory refused: %d recorded unit(s) are missing", len(missing))
		}
	}
	if len(extra) > 0 {
		if *updateMatrixInventory && len(missing) == 0 {
			for k := range current {
				golden[k] = true
			}
			writeGolden(t, goldenPath, golden)
			t.Logf("appended %d unit(s)", len(extra))
			return
		}
		t.Errorf("%d new matrix unit(s) are not in the golden:\n  %s\n\nRecord them (append-only):\n"+
			"  go test ./test/e2e/parallel/ -run TestMatrixUnitInventoryOnlyGrows -update-matrix-inventory",
			len(extra), strings.Join(extra, "\n  "))
	}
}

// TestMatrixInventoryKeySeesEveryField is the G2 control: every field that changes what a unit RUNS
// must change its key, or the golden would not notice that change.
func TestMatrixInventoryKeySeesEveryField(t *testing.T) {
	base := unit{matrix: "TestXMatrix", pkg: "./p/...", tags: "", race: true, runFilter: ""}
	variants := map[string]unit{
		"pkg":   {matrix: "TestXMatrix", pkg: "./q/...", race: true},
		"tags":  {matrix: "TestXMatrix", pkg: "./p/...", tags: "x_integration", race: true},
		"race":  {matrix: "TestXMatrix", pkg: "./p/...", race: false},
		"run":   {matrix: "TestXMatrix", pkg: "./p/...", race: true, runFilter: "^TestA$", hasRun: true},
		"extra": {matrix: "TestXMatrix", pkg: "./p/...", race: true, extra: "-short"},
		"whole": {matrix: "TestXMatrix", whole: true},
	}
	for field, v := range variants {
		if unitKey(v) == unitKey(base) {
			t.Errorf("changing %s does not change the key", field)
		}
	}
}

// TestWholeMatrixLiteralScanSeesTheShapes is the G2 control for wholeMatrixLiterals: a synthetic
// matrix file with a run helper, a -run pair, a package-level phase list ranged by a called
// function, and a parsed matrix that must contribute nothing.
// origin: test-system-overhaul internal review L4-F1
func TestWholeMatrixLiteralScanSeesTheShapes(t *testing.T) {
	src := "package e2e\n\n" +
		"var allPhases = []string{\"p1\", \"p2\"}\n\n" +
		"func runPhase(t *testing.T, phase string) { exec.Command(\"go\", \"test\", \"./test/\"+phase+\"/...\") }\n\n" +
		"func TestAllPhases(t *testing.T) { for _, phase := range allPhases { runPhase(t, phase) } }\n\n" +
		"func TestHelperMatrix(t *testing.T) {\n" +
		"\trun := func(label string, args ...string) {}\n" +
		"\trun(\"pty\", \"./internal/pty/...\")\n" +
		"\trun(\"wiring\", \"-run\", \"RemoteFS|SafeField\", \"./internal/agent/...\", \"./test/p4/...\")\n" +
		"}\n\n" +
		"func TestParsedMatrix(t *testing.T) { exec.Command(\"go\", \"test\", \"-race\", \"./internal/cluster/...\") }\n"
	f, err := parser.ParseFile(token.NewFileSet(), "all_phases_test.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := wholeMatrixLiterals(f, []string{"TestAllPhases", "TestHelperMatrix"})
	want := map[string]string{
		// `./test/` is the literal half of runPhase's "./test/"+phase+"/..." — pinned as a prefix,
		// which is exactly as much as the source says.
		"TestAllPhases":    "./test/,phase p1,phase p2",
		"TestHelperMatrix": "-run RemoteFS|SafeField,./internal/agent/...,./internal/pty/...,./test/p4/...",
	}
	for m, w := range want {
		if g := strings.Join(got[m], ","); g != w {
			t.Errorf("%s: literals drifted:\n got  %s\n want %s", m, g, w)
		}
	}
	if _, ok := got["TestParsedMatrix"]; ok {
		t.Error("a matrix not in the unparsed list must contribute nothing")
	}
}

// TestOnlyTheMatrixForksGoTest: no Go file outside test/e2e may exec `go test`. The runner parses
// ONE file for its plan; a fork anywhere else is outside the coverage self-check, the reconcile and
// this golden — a second, invisible matrix. Scan surface: every `_test.go` in the tree, plus every
// non-test Go file under test/ and internal/testharness (a helper compiled into a test binary can
// fork just as well as the test that calls it — internal review L4-F3).
// goForkExceptions: files the fail-closed predicate flags that provably do not run tests, each with
// the reason. File-keyed and capped: adding one means editing the cap in the same change, and an
// entry whose file the predicate no longer flags is red (the same drain-only discipline as every
// other exception table — internal review L1-F8).
var goForkExceptions = map[string]string{
	"test/hotupdate/harness_test.go": "buildHotUpdateArtifact passes BuildArgs beginning with literal build to go; " +
		"optional race/ldflags and cmd/tether follow. It only builds sandbox artifacts, never forks go test.",
	"test/architecture/layering_test.go": "exec.Command(\"go\", args...) where args is built two lines above " +
		"starting with the literal \"list\" — it reads `go list -deps`, never runs a test; the predicate cannot " +
		"see through the variable and fail-closed is the right default for everything it cannot see through.",
}

// 1 → 2: the S5 artifact harness is a build-only variable-argv command.
const goForkExceptionsCap = 2

func TestOnlyTheMatrixForksGoTest(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if n := len(goForkExceptions); n != goForkExceptionsCap {
		t.Fatalf("goForkExceptions has %d entries, cap says %d — move the cap in the same change, with the reason", n, goForkExceptionsCap)
	}
	var offenders []string
	flagged := map[string]bool{}
	scanned := 0
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "test/e2e/") {
			return nil
		}
		isTest := strings.HasSuffix(p, "_test.go")
		isTestHelper := strings.HasPrefix(rel, "test/") || strings.HasPrefix(rel, "internal/testharness/")
		if !isTest && !isTestHelper {
			return nil
		}
		scanned++
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if perr != nil {
			return perr
		}
		if forksGoTest(f) {
			flagged[rel] = true
			if _, excepted := goForkExceptions[rel]; !excepted {
				offenders = append(offenders, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 500 {
		t.Fatalf("scanned only %d files — walk is blind", scanned)
	}
	if len(offenders) > 0 {
		t.Errorf("%d file(s) outside test/e2e fork `go test` (or a `go` command the predicate cannot prove is not `test`):\n  %s\n\n"+
			"Move the matrix into all_phases_test.go so the runner, its self-check and the golden can see it — or, if the "+
			"command provably never runs tests, add the file to goForkExceptions WITH the reason and move the cap.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	for rel := range goForkExceptions {
		if !flagged[rel] {
			t.Errorf("goForkExceptions names %s, which the predicate no longer flags — delete the entry and lower the cap", rel)
		}
	}
}

// forksGoTest reports whether f contains an exec.Command / exec.CommandContext whose argv[0] is the
// literal "go" and whose argv[1] is the literal "test" OR is not a literal at all. The non-literal
// case is FAIL-CLOSED on purpose: `exec.Command("go", append(base, args...)...)` is the shape the
// matrices themselves use, and a predicate that only recognised two literals could not see its own
// subject (internal review L4-F3 / L1-F5). The os/exec import's local name is read from the file,
// so an alias does not hide the call.
func forksGoTest(f *ast.File) bool {
	execName := ""
	dotExec := false
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == "os/exec" {
			execName = "exec"
			if imp.Name != nil {
				if imp.Name.Name == "." {
					dotExec = true
					execName = ""
				} else {
					execName = imp.Name.Name
				}
			}
		}
	}
	isCtor := func(e ast.Expr) (name string, ok bool) {
		if id, isID := e.(*ast.Ident); isID && dotExec && (id.Name == "Command" || id.Name == "CommandContext") {
			return id.Name, true
		}
		sel, isSel := e.(*ast.SelectorExpr)
		if !isSel || (sel.Sel.Name != "Command" && sel.Sel.Name != "CommandContext") {
			return "", false
		}
		if id, isID := sel.X.(*ast.Ident); !isID || execName == "" || id.Name != execName {
			return "", false
		}
		return sel.Sel.Name, true
	}
	// Function-value aliases: `var command = exec.Command` / `command := exec.CommandContext`. A call
	// through one is a constructor call (external review F4's alias shape, mirrored here).
	aliases := map[string]string{}
	derived := func(e ast.Expr) (string, bool) {
		if name, ok := isCtor(e); ok {
			return name, true
		}
		id, ok := e.(*ast.Ident)
		if !ok || aliases[id.Name] == "" {
			return "", false
		}
		return aliases[id.Name], true
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.AssignStmt:
				for i, rhs := range v.Rhs {
					name, ok := derived(rhs)
					if !ok || i >= len(v.Lhs) {
						continue
					}
					if id, ok := v.Lhs[i].(*ast.Ident); ok && id.Name != "_" && aliases[id.Name] != name {
						aliases[id.Name] = name
						changed = true
					}
				}
			case *ast.ValueSpec:
				for i, val := range v.Values {
					name, ok := derived(val)
					if ok && i < len(v.Names) && v.Names[i].Name != "_" && aliases[v.Names[i].Name] != name {
						aliases[v.Names[i].Name] = name
						changed = true
					}
				}
			}
			return true
		})
	}
	// A constructor value passed, returned or stored somewhere this predicate cannot follow may
	// fork go test. Flag the file instead of silently assuming it does not (external rereview R3).
	escapes := false
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.AssignStmt:
			for i, rhs := range v.Rhs {
				if _, ok := derived(rhs); !ok {
					continue
				}
				if i >= len(v.Lhs) {
					escapes = true
					continue
				}
				if id, ok := v.Lhs[i].(*ast.Ident); !ok || (id.Name != "_" && aliases[id.Name] == "") {
					escapes = true
				}
			}
		case *ast.ValueSpec:
			for i, val := range v.Values {
				if _, ok := derived(val); ok && (i >= len(v.Names) || (v.Names[i].Name != "_" && aliases[v.Names[i].Name] == "")) {
					escapes = true
				}
			}
		case *ast.CallExpr:
			for _, arg := range v.Args {
				if _, ok := derived(arg); ok {
					escapes = true
				}
			}
		case *ast.CompositeLit:
			for _, elt := range v.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					elt = kv.Value
				}
				if _, ok := derived(elt); ok {
					escapes = true
				}
			}
		case *ast.ReturnStmt:
			for _, result := range v.Results {
				if _, ok := derived(result); ok {
					escapes = true
				}
			}
		}
		return !escapes
	})
	if escapes {
		return true
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ctor, ok := isCtor(call.Fun)
		if !ok {
			if id, isID := call.Fun.(*ast.Ident); isID && aliases[id.Name] != "" {
				ctor = aliases[id.Name]
			} else {
				return true
			}
		}
		args := call.Args
		if ctor == "CommandContext" && len(args) > 0 {
			args = args[1:]
		}
		if len(args) == 0 {
			return true
		}
		a0, ok := args[0].(*ast.BasicLit)
		if !ok || a0.Value != `"go"` {
			return true
		}
		if len(args) < 2 {
			// exec.Command("go") with cmd.Args edited later: cannot prove it is not `test`.
			found = true
			return false
		}
		a1, ok := args[1].(*ast.BasicLit)
		if !ok || a1.Value == `"test"` {
			found = true
			return false
		}
		return true
	})
	return found
}

func TestForksGoTestPredicateSeesTheShapes(t *testing.T) {
	cases := map[string]bool{
		"package x\nimport \"os/exec\"\nfunc f() { exec.Command(\"go\", \"test\", \"./x\") }\n":                                        true,
		"package x\nimport \"os/exec\"\nfunc f(base, a []string) { exec.Command(\"go\", append(base, a...)...) }\n":                    true, // all_phases_test.go's own run helper shape
		"package x\nimport (\"context\";\"os/exec\")\nfunc f(c context.Context) { exec.CommandContext(c, \"go\", \"test\") }\n":        true,
		"package x\nimport goexec \"os/exec\"\nfunc f() { goexec.Command(\"go\", \"test\") }\n":                                        true,
		"package x\nimport \"os/exec\"\nfunc f() { exec.Command(\"go\") }\n":                                                           true,  // args edited later: fail-closed
		"package x\nimport \"os/exec\"\nvar command = exec.Command\nfunc f(a []string) { command(\"go\", a...) }\n":                    true,  // function-value alias (external review F4)
		"package x\nimport \"os/exec\"\nvar command = exec.Command\nvar command2 = command\nfunc f() { command2(\"go\", \"test\") }\n": true,  // transitive alias (external rereview R3)
		"package x\nimport \"os/exec\"\nvar command = exec.Command\nfunc apply(any) {}\nfunc f() { apply(command) }\n":                 true,  // escaped constructor may fork go test: fail-closed
		"package x\nimport . \"os/exec\"\nfunc f() { Command(\"go\", \"test\") }\n":                                                    true,  // dot import constructor
		"package x\nimport \"os/exec\"\nfunc f() { run := exec.CommandContext; _ = run }\n":                                            false, // an alias that is never called
		"package x\nimport \"os/exec\"\nfunc f() { exec.Command(\"go\", \"build\", \"./x\"); exec.Command(\"gofmt\") }\n":              false,
		"package x\nimport \"os/exec\"\nfunc f(bin string) { exec.Command(bin, \"serve\") }\n":                                         false,
		"package x\nimport goexec \"os/exec\"\nfunc f() { exec.Command(\"go\", \"test\") }\n":                                          false, // `exec` is not os/exec here
	}
	for src, want := range cases {
		f, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := forksGoTest(f); got != want {
			t.Errorf("forksGoTest = %v, want %v for %q", got, want, src)
		}
	}
}

// TestEveryRiskyPackageRunsUnderRaceInSomeMatrix: CLAUDE.md §5 says touching tunnel/PTY/reconcile/
// transport/raft requires -race; this checks that the full matrix actually applies it to the WHOLE
// of every package that imports those things — the -race ATTRIBUTION half of the leak gate
// (test/determinism/leak_assert_shape_test.go holds the coverage half).
//
// A -run SUBSET DOES NOT COUNT. The first version carried a hand-written map that said
// internal/agent was covered by TestRemoteFSMatrix and internal/tunnel by
// TestProxyTunnelReconnectMatrix; both matrices run those packages through a `-run` filter that
// selects 14 of agent's 257 tests and 12 of tunnel's 43 — and agent's one leak-gate test was
// outside the filter. The map was a receipt for something that had never happened (internal
// review L1-F2 / L4-F2 / L6-F3). Now the only thing that satisfies this gate is a PARSED unit with
// -race and no -run: the package, whole. TestLeakRiskMatrix exists for exactly this.
func TestEveryRiskyPackageRunsUnderRaceInSomeMatrix(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	risky := riskyPackages(t, root)
	if len(risky) < 4 {
		t.Fatalf("only %d risky packages found: %v", len(risky), risky)
	}
	units, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	var uncovered []string
	for _, pkg := range risky {
		covered := false
		for _, u := range units {
			if u.race && !u.hasRun && u.runFilter == "" && racePackagePatternCovers(u.pkg, pkg) {
				covered = true
				break
			}
		}
		if !covered {
			uncovered = append(uncovered, pkg)
		}
	}
	sort.Strings(uncovered)
	if len(uncovered) > 0 {
		t.Errorf("%d leak-risky package(s) are run WHOLE by NO parsed -race matrix unit (a -run subset does not count):\n  %s\n\n"+
			"Add the package, unfiltered, to a -race matrix in all_phases_test.go (TestLeakRiskMatrix is the home for "+
			"exactly this), then -update-matrix-inventory.",
			len(uncovered), strings.Join(uncovered, "\n  "))
	}
}

// A recursive Go package pattern covers extracted owners below its root; an
// exact package path does not. Prefix boundaries prevent tunnel/... granting
// race coverage to a similarly named but unrelated package.
// origin: hot-update S6 FIX-MATRIX / matrix inventory integration seam.
func racePackagePatternCovers(pattern, pkg string) bool {
	path := strings.TrimPrefix(pattern, "./")
	if !strings.HasSuffix(path, "/...") {
		return path == pkg
	}
	path = strings.TrimSuffix(path, "/...")
	return path == pkg || strings.HasPrefix(pkg, path+"/")
}

// origin: hot-update S6 FIX-MATRIX / matrix inventory integration seam.
func TestRacePackagePatternCoversOnlyItsClosure(t *testing.T) {
	for _, tc := range []struct {
		pattern, pkg string
		want         bool
	}{
		{"./internal/tunnel/...", "internal/tunnel", true},
		{"./internal/tunnel/...", "internal/tunnel/transport", true},
		{"./internal/tunnel/...", "internal/tunnel/flowcarrier", true},
		{"./internal/tunnel/...", "internal/tunnelother", false},
		{"./internal/tunnel/...", "internal/flow", false},
		{"./internal/tunnel", "internal/tunnel/transport", false},
		{"./internal/tunnel", "internal/tunnel", true},
	} {
		if got := racePackagePatternCovers(tc.pattern, tc.pkg); got != tc.want {
			t.Errorf("%s covers %s = %v, want %v", tc.pattern, tc.pkg, got, tc.want)
		}
	}
}

// origin: hot-update S5 plan §8 / T50 / §10.2 — exact parsed tuples protect
// owners with no risky import and include the watchdog's real wall budget.
var flowSSRacePackages = []string{
	"internal/rawsocket", "internal/sealedstate", "internal/flow", "internal/tunnel",
	"internal/agent/ssproxy", "internal/agentimage", "internal/agentboot", "test/hotupdate",
	"internal/agent", "internal/broker", "internal/execsession",
}

func flowSSMatrixProblems(units []unit) []string {
	var problems []string
	for _, pkg := range flowSSRacePackages {
		count := 0
		for _, u := range units {
			if u.matrix != "TestHotUpdateFlowMatrix" || u.pkg != "./"+pkg+"/..." {
				continue
			}
			count++
			if pkg == "test/hotupdate" {
				problems = append(problems, partitionTupleProblems(u, "", "600s")...)
				continue
			}
			budget, err := time.ParseDuration(u.timeo)
			if u.whole || !u.race || u.hasRun || u.runFilter != "" || u.tags != "" || u.extra != "" || err != nil || budget < 240*time.Second {
				problems = append(problems, pkg+": requires parsed whole-package -race, no tags/filter/extra, timeout >=240s")
			}
		}
		if (pkg == "test/hotupdate" && count == 0) || (pkg != "test/hotupdate" && count != 1) {
			problems = append(problems, fmt.Sprintf("%s: expected complete flow matrix ownership, got %d tuples", pkg, count))
		}
	}
	// The tagged whole-package declaration owns every heavy test. Runtime
	// sharding is exhaustive and independent of the original reference tuple.
	count := 0
	for _, u := range units {
		if u.matrix != "TestHotUpdateIntegrationMatrix" || u.pkg != "./test/hotupdate/..." {
			continue
		}
		count++
		budget, err := time.ParseDuration(u.timeo)
		if u.whole || !u.race || u.hasRun || u.runFilter != "" || u.tags != "hotupdate_integration" || u.extra != "" || err != nil || budget != 600*time.Second {
			problems = append(problems, "hotupdate integration requires whole-package race, its local tag, no filter/extra, timeout 600s")
		}
	}
	if count != 1 {
		problems = append(problems, fmt.Sprintf("expected one hotupdate integration tuple, got %d", count))
	}
	return problems
}

// origin: hot-update S5 plan §10.1 / T49 / T50 — live source uses the runner's
// parser; synthetic defects exercise the exact predicate used for attribution.
func TestFlowSSMatrixControls(t *testing.T) {
	want := strings.Fields("internal/rawsocket internal/sealedstate internal/flow internal/tunnel internal/agent/ssproxy internal/agentimage internal/agentboot test/hotupdate internal/agent internal/broker internal/execsession")
	got := append([]string(nil), flowSSRacePackages...)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("flow/SS race registration = %v, want %v", got, want)
	}
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, unparsed, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range unparsed {
		if name == "TestHotUpdateFlowMatrix" {
			t.Fatal("flow matrix must consist of parsed tuples")
		}
	}
	if problems := flowSSMatrixProblems(units); len(problems) != 0 {
		t.Fatalf("flow matrix coverage: %s", strings.Join(problems, "; "))
	}
	var good []unit
	for _, pkg := range flowSSRacePackages {
		u := unit{matrix: "TestHotUpdateFlowMatrix", pkg: "./" + pkg + "/...", race: true, timeo: "600s"}
		if pkg == "test/hotupdate" {
			u.hasRun, u.runFilter = true, "^TestExample$"
		}
		good = append(good, u)
	}
	good = append(good, unit{matrix: "TestHotUpdateIntegrationMatrix", pkg: "./test/hotupdate/...", tags: "hotupdate_integration", race: true, timeo: "600s"})
	for _, defect := range []string{"integration delete", "integration tag", "integration race", "integration filter", "integration budget", "delete", "race", "filter", "empty filter", "tags", "short", "watchdog", "opaque"} {
		t.Run(defect, func(t *testing.T) {
			bad := append([]unit(nil), good...)
			switch defect {
			case "integration delete":
				bad = bad[:len(bad)-1]
			case "integration tag":
				bad[len(bad)-1].tags = ""
			case "integration race":
				bad[len(bad)-1].race = false
			case "integration filter":
				bad[len(bad)-1].hasRun = true
			case "integration budget":
				bad[len(bad)-1].timeo = "601s"
			case "delete":
				bad = bad[1:]
			case "race":
				bad[0].race = false
			case "filter":
				bad[0].runFilter = "^TestHappy$"
				bad[0].hasRun = true
			case "empty filter":
				bad[0].hasRun = true
			case "tags":
				bad[0].tags = "integration"
			case "short":
				bad[0].extra = "-short"
			case "watchdog":
				bad[0].timeo = "90s"
			case "opaque":
				bad[0].whole = true
			}
			if len(flowSSMatrixProblems(bad)) == 0 {
				t.Fatal("defective flow matrix tuple was accepted")
			}
		})
	}
}

func riskyPackages(t *testing.T, root string) []string {
	t.Helper()
	imports := []string{
		`"github.com/hashicorp/raft"`, `"github.com/hashicorp/yamux"`,
		`"github.com/LinZiyang666/tether/internal/tunnel"`, `"github.com/LinZiyang666/tether/internal/pty"`,
	}
	set := map[string]bool{}
	// origin: hot-update S5 plan §8 / §10.2 — layering deliberately makes
	// several resource owners invisible to import-based risk discovery.
	for _, pkg := range append(append([]string(nil), flowSSRacePackages...), transferRacePackages...) {
		set[pkg] = true
	}
	// Source layering hides these S10 resource owners from import discovery;
	// their explicit declarations retain the complete race/compile closure.
	// origin: hot-update S10 plan §10.2 P30 / §11.1
	for _, pkg := range []string{"internal/drillobserve", "internal/storewitness", "test/fullstackharness"} {
		set[pkg] = true
	}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if n := d.Name(); n == "testdata" || n == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			src, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			for _, imp := range imports {
				if strings.Contains(string(src), imp) {
					rel, _ := filepath.Rel(root, filepath.Dir(p))
					set[filepath.ToSlash(rel)] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var out []string
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// Compile-list the serial gate and prove every top-level test has a unit. This
// runs no matrix or real-binary suite; it guards the same startup predicate.
// origin: hot-update S5 plan §10.1 / T49 / T50.
func TestMatrixPlanCoversSerialTests(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, unparsed, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range unparsed {
		units = append(units, unit{matrix: name, whole: true})
	}
	declared, err := listTests("e2e_matrix", "")
	if err != nil {
		t.Fatal(err)
	}
	covered := coveredMatrices(units, nil)
	for _, name := range declared {
		if !covered[name] {
			t.Errorf("serial test %s has no matrix unit", name)
		}
	}
	for _, name := range []string{"TestFlowSSDrillRegistration", "TestFlowSSManifestControls"} {
		found := false
		for _, u := range units {
			if u.matrix == name && !u.whole && u.race && u.tags == "e2e_matrix" && u.pkg == "./test/e2e" && u.hasRun && u.runFilter == "^"+name+"$" {
				found = true
			}
		}
		if !found {
			t.Errorf("serial guard %s requires a direct race unit", name)
		}
	}
}

// Every tagged top-level test gets an independent process even with optional
// generic sharding disabled. The watchdog cannot share its five-round budget.
// origin: hot-update S5 plan §8 / §10.2 / T45 / T50.
func TestHotUpdateIntegrationPlanSeparatesEveryTest(t *testing.T) {
	// origin: hot-update S7 plan lead L17 / T52; default fuzz replay shares race ownership.
	names := []string{"TestFlow", "TestReadyPendingWatchdogTakesSecondCurrentCut", "TestRollback", "FuzzPrivateReply"}
	tagged := unit{matrix: "TestHotUpdateIntegrationMatrix", pkg: "./test/hotupdate/...", tags: "hotupdate_integration", race: true, timeo: "600s", name: "integration"}
	reference := unit{matrix: "TestHotUpdateFlowMatrix", pkg: "./test/hotupdate/...", race: true, timeo: "600s", name: "reference"}
	// origin: hot-update S9 plan §12 P28/P36/P40; S10 plan §10.2 P27/P30.
	for _, subject := range []unit{
		tagged,
		{matrix: "TestHotUpdateInfraMatrix", pkg: "./internal/infraupgrade/...", race: true, timeo: "600s", name: "infra"},
		{matrix: "TestHotUpdateInfraNATSMatrix", pkg: "./test/infranats/...", tags: "infranats_integration", race: true, timeo: "600s", name: "nats"},
		// origin: S9 internal review rv-business F7 — retain flags and reject empty discovery for every suite.
		{matrix: "TestHotUpdateInfraCaddyMatrix", pkg: "./test/infracaddy/...", tags: "infracaddy_integration", race: true, timeo: "600s", name: "caddy"},
		{matrix: "TestHotUpdateInfraBusinessMatrix", pkg: "./test/infrabusiness/...", tags: "infrabusiness_integration", race: true, timeo: "600s", name: "business"},
		{matrix: "TestHotUpdateInfraPrivilegeMatrix", pkg: "./test/infraprivilege/...", tags: "infraprivilege_integration", race: true, timeo: "600s", name: "privilege"},
		{matrix: "TestFullstackAcceptanceMatrix", pkg: "./test/hotupdate/acceptance/...", race: true, timeo: "600s", name: "acceptance"},
		// origin: hot-update S7 plan lead L17 / T52; S10 plan §11.1.
		{matrix: "TestHotUpdateBrokerMatrix", pkg: "./internal/broker/...", race: true, timeo: "600s", name: "broker"},
		{matrix: "TestHotUpdateBrokerMatrix", pkg: "./cmd/tether/...", race: true, timeo: "600s", name: "command"},
		{matrix: "TestFullstackHarnessMatrix", pkg: "./test/fullstackharness/...", race: true, timeo: "600s", name: "harness"},
	} {
		budget := "600s"
		for _, shards := range []int{0, 1, 8} {
			calls := 0
			got, err := shardPackagesWithList([]unit{reference, subject}, shards, func(u unit) ([]string, error) {
				calls++
				if u.tags != subject.tags || !u.race {
					t.Fatal("listing lost tagged race ownership")
				}
				return names, nil
			})
			if err != nil || len(got) != len(names)+1 || calls != 1 {
				t.Fatalf("shards=%d units=%v calls=%d err=%v", shards, got, calls, err)
			}
			if got[0].runFilter != "" || got[0].tags != "" {
				t.Fatal("reference tuple was changed")
			}
			for i, name := range names {
				u := got[i+1]
				if u.runFilter != "^"+name+"$" || u.tags != subject.tags || !u.race || u.timeo != budget || u.name != subject.name+"["+name+"]" {
					t.Fatalf("test %s lost its independent unit: %+v", name, u)
				}
			}
		}
		for _, defect := range []string{"empty", "listing error"} {
			_, err := shardPackagesWithList([]unit{subject}, 8, func(unit) ([]string, error) {
				if defect == "listing error" {
					return nil, fmt.Errorf("listing failed")
				}
				return nil, nil
			})
			if err == nil {
				t.Fatalf("%s fell back to cumulative suite", defect)
			}
		}
	}

	// The policy leaf is exercised by infrahost; it has no Test declarations
	// of its own. Keep its whole-package race tuple without inventing a selector.
	// origin: hot-update S9 plan §12 / S9-SLIM policy extraction.
	leaf := unit{matrix: "TestHotUpdateInfraMatrix", pkg: "./internal/infraunit/...", race: true, timeo: "600s", name: "policy"}
	got, err := shardPackagesWithList([]unit{leaf}, 0, func(unit) ([]string, error) {
		t.Fatal("policy leaf must keep its whole-package declaration")
		return nil, nil
	})
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], leaf) {
		t.Fatalf("policy leaf tuple changed: units=%v err=%v", got, err)
	}
}

// The process supervisor owns leases, pidfds and adopted descendants and
// needs the same whole-package race coverage as the fixtures that consume it.
// origin: hot-update S6 plan §9.3 / T54 / FIX-ORPHAN matrix request.
var brokerRacePackages = strings.Fields("internal/brokercontrol/maintenance internal/brokerimage internal/brokerimage/contractstub internal/brokermaint internal/controlbudget internal/listenerimage internal/clusterio internal/storage internal/cluster internal/tunnel internal/adminsock internal/httplisten internal/subhttp internal/brokermetrics internal/clustermanifest internal/broker internal/brokercontrol internal/agent internal/agentboot internal/cli internal/natsinbox internal/proxydial internal/execclient internal/clusterupgrade cmd/tether test/clusterharness test/hotupdate test/p13 test/cli_e2e internal/testharness")

func brokerMatrixProblems(units []unit) []string {
	var problems []string
	want := map[string]bool{}
	for _, pkg := range brokerRacePackages {
		want["./"+pkg+"/..."] = true
	}
	seen := map[string]int{}
	partitionSelectors := map[string]bool{}
	for _, u := range units {
		if u.matrix != "TestHotUpdateBrokerMatrix" {
			continue
		}
		// The CLI's dynamic owner discovers the remainder after its separately
		// declared roll tests. Count that owner once; each explicit declaration
		// must retain the same race, tag and budget contract with one exact Test.
		// The companion source-discovery gate checks the resulting union.
		// origin: S10 internal review ARCH-REDS-3 F3; hot-update S10 plan §11.1
		if u.pkg == "./cmd/tether/..." && (u.hasRun || u.runFilter != "") {
			problems = append(problems, partitionTupleProblems(u, "", "600s")...)
			key := u.pkg + "|" + u.runFilter
			if partitionSelectors[key] {
				problems = append(problems, "duplicate broker partition: "+key)
			}
			partitionSelectors[key] = true
			continue
		}
		seen[u.pkg]++
		if u.pkg == "./test/hotupdate/..." || u.pkg == "./test/p13/..." {
			// Exact coverage and duplicate selectors are independently checked by
			// TestPartitionedPackagesCoverEveryTestExactlyOnce against source.
			// origin: hot-update S6 FIX-MATRIX / lead decision / T54.
			tags := ""
			if u.pkg == "./test/p13/..." {
				tags = "p13_integration"
			}
			problems = append(problems, partitionTupleProblems(u, tags, "600s")...)
			key := u.pkg + "|" + u.runFilter
			if partitionSelectors[key] {
				problems = append(problems, "duplicate broker partition: "+key)
			}
			partitionSelectors[key] = true
			continue
		}
		if !want[u.pkg] || u.whole || !u.race || u.hasRun || u.runFilter != "" || u.tags != "" || u.extra != "" || u.timeo != "600s" {
			problems = append(problems, u.pkg+": requires exact whole-package race tuple with timeout 600s")
		}
	}
	for pkg := range want {
		partitioned := pkg == "./test/hotupdate/..." || pkg == "./test/p13/..."
		if (partitioned && seen[pkg] == 0) || (!partitioned && seen[pkg] != 1) {
			problems = append(problems, fmt.Sprintf("%s: expected complete broker ownership, got %d tuples", pkg, seen[pkg]))
		}
	}
	sort.Strings(problems)
	return problems
}

// The runner's real parser is the oracle; no source regex can certify a
// package that the splitter actually discards. This test does not run matrices.
// origin: hot-update S6 plan §9.3 / T54
// gate-control: TestBrokerRaceCoverageControls
func TestBrokerWholePackageRaceCoverage(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	if problems := brokerMatrixProblems(units); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

// origin: hot-update S6 plan §11 / T54
func TestBrokerRaceCoverageControls(t *testing.T) {
	// origin: S7 internal review GATES-LEAD F6 / moved maintenance race owner
	const frozen = "internal/brokercontrol/maintenance internal/brokerimage internal/brokerimage/contractstub internal/brokermaint internal/controlbudget internal/listenerimage internal/clusterio internal/storage internal/cluster internal/tunnel internal/adminsock internal/httplisten internal/subhttp internal/brokermetrics internal/clustermanifest internal/broker internal/brokercontrol internal/agent internal/agentboot internal/cli internal/natsinbox internal/proxydial internal/execclient internal/clusterupgrade cmd/tether test/clusterharness test/hotupdate test/p13 test/cli_e2e internal/testharness"
	if strings.Join(brokerRacePackages, " ") != frozen {
		t.Fatal("S6 race owner registry changed")
	}
	var good []unit
	for _, pkg := range strings.Fields(frozen) {
		args := []string{"go", "test", "-race", "-count=1", "-timeout", "600s"}
		if pkg == "test/p13" {
			args = append(args, "-tags", "p13_integration")
		}
		if pkg == "test/hotupdate" || pkg == "test/p13" {
			args = append(args, "-run", "^TestExample$")
		}
		good = append(good, parseGoTestArgs("TestHotUpdateBrokerMatrix", append(args, "./"+pkg+"/..."))...)
	}
	if problems := brokerMatrixProblems(good); len(problems) != 0 {
		t.Fatal(problems)
	}
	// The dynamic CLI owner retains the discovered remainder while exact roll
	// partitions run separately; all six declarations still own one suite.
	// origin: S10 internal review ARCH-REDS-3 F3; hot-update S10 plan §11.1 / T54
	cliPartition := parseGoTestArgs("TestHotUpdateBrokerMatrix", []string{"go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRoll$", "./cmd/tether/..."})[0]
	withCLI := append(append([]unit(nil), good...), cliPartition)
	if problems := brokerMatrixProblems(withCLI); len(problems) != 0 {
		t.Fatalf("dynamic CLI owner with an exact partition rejected: %v", problems)
	}
	for _, defect := range []string{"missing owner", "duplicate owner", "duplicate partition", "broad filter", "empty filter", "race", "tags", "timeout", "extra", "whole"} {
		t.Run("CLI/"+defect, func(t *testing.T) {
			broken := append([]unit(nil), withCLI...)
			last := len(broken) - 1
			switch defect {
			case "missing owner":
				for i, u := range broken {
					if u.pkg == cliPartition.pkg && !u.hasRun {
						broken = append(broken[:i], broken[i+1:]...)
						break
					}
				}
			case "duplicate owner":
				for _, u := range good {
					if u.pkg == cliPartition.pkg {
						broken = append(broken, u)
						break
					}
				}
			case "duplicate partition":
				broken = append(broken, cliPartition)
			case "broad filter":
				broken[last].runFilter = "^Test.*$"
			case "empty filter":
				broken[last].runFilter = ""
			case "race":
				broken[last].race = false
			case "tags":
				broken[last].tags = "hidden"
			case "timeout":
				broken[last].timeo = "601s"
			case "extra":
				broken[last].extra = "-short"
			case "whole":
				broken[last].whole = true
			}
			if len(brokerMatrixProblems(broken)) == 0 {
				t.Fatalf("defective CLI ownership accepted: %s", defect)
			}
		})
	}
	for i, u := range good {
		t.Run(u.pkg, func(t *testing.T) {
			missing := append(append([]unit(nil), good[:i]...), good[i+1:]...)
			if len(brokerMatrixProblems(missing)) == 0 {
				t.Fatal("omitted package accepted")
			}
			for _, defect := range []string{"race", "filter", "tags", "timeout", "whole", "duplicate", "extra", "unknown"} {
				broken := append([]unit(nil), good...)
				switch defect {
				case "race":
					broken[i].race = false
				case "filter":
					broken[i].hasRun = true
					broken[i].runFilter = "^TestOne.*$"
				case "tags":
					broken[i].tags = "hidden"
				case "timeout":
					broken[i].timeo = "601s"
				case "whole":
					broken[i].whole = true
				case "duplicate":
					broken = append(broken, u)
				case "extra":
					broken[i].extra = "-short"
				case "unknown":
					broken[i].pkg = "./internal/unowned/..."
				}
				if len(brokerMatrixProblems(broken)) == 0 {
					t.Fatalf("%s accepted", defect)
				}
			}
		})
	}
}

// Explicit ownership survives dependency slimming. Pending packages have no
// invented tuples or leak anchors; T52 names each missing producer receipt.
// origin: hot-update S7 plan §11.3 / T52
var transferRacePackages = []string{
	"internal/jsstream",
	"internal/transferpath",
	"internal/filetransfer",
	"internal/drillobserve",
	"internal/xferworker",
	"internal/xfersend",
	"internal/xferreceive",
	"internal/xfercourier",
	"internal/xfertrack",
	"internal/xferfinal",
	"internal/xferledger",
	"internal/xferhistory",
	"internal/xferbucket",
	"internal/xfergc",
	"internal/xfercontrol",
	"internal/xferclient",
	"internal/xferauth",
	"internal/xferbridge",
	"internal/xferagent",
	"internal/xferserve",
	"internal/agent",
	"internal/broker",
	"internal/brokerimage",
	"internal/brokercontrol",
	"internal/authcallout",
	"internal/cli",
	"test/transferharness",
}

// These owners declare contracts or encode sealed records without runtime IO.
// Their tests still require race ownership; this is not a leak exemption.
// origin: hot-update S7 plan §11.3 / T52
var transferPurePackages = map[string]string{
	"internal/xfercontract":              "declarations: no IO or lifecycle root",
	"internal/xfercontract/contractstub": "inert witnesses: no IO or lifecycle root",
	"internal/xferimage":                 "sealed codec: no IO or lifecycle root",
}

// Coverage uses the runner's real tuples and the shape gate's site-scoped
// anchors. Detailed loop/baseline semantics remain guarded by the shape gate;
// this join prevents a valid assertion in another package from signing S7.
// origin: hot-update S7 plan §11.3 / T52
func transferCoverageProblems(units []unit, race, leak map[string]string, cycles map[string]bool) []string {
	problems := transferRiskRegistryProblems(race, leak)
	for pkg := range race {
		if _, ok := leak[pkg]; !ok {
			problems = append(problems, pkg+": missing explicit leak risk row")
		}
		if !cycles[pkg] {
			problems = append(problems, pkg+": missing registered direct transfer lifecycle test (pending runtime owner)")
		}
	}
	for pkg := range leak {
		if _, ok := race[pkg]; !ok {
			problems = append(problems, pkg+": missing explicit race risk row")
		}
	}
	want := map[string]string{}
	for pkg := range race {
		want["./"+pkg+"/..."] = ""
	}
	for pkg := range transferPurePackages {
		want["./"+pkg+"/..."] = ""
	}
	want["./test/hotupdate/..."] = "hotupdate_integration"
	want["./test/cli_e2e/..."] = ""
	problems = append(problems, transferMatrixProblems(units, want)...)
	sort.Strings(problems)
	return problems
}

func transferMatrixProblems(units []unit, want map[string]string) []string {
	var problems []string
	seen := map[string]int{}
	for _, u := range units {
		if u.matrix != "TestTransferContinuationMatrix" {
			continue
		}
		seen[u.pkg]++
		tags, owned := want[u.pkg]
		if !owned || u.whole || !u.race || u.hasRun || u.runFilter != "" || u.tags != tags || u.extra != "" || u.timeo != "600s" {
			problems = append(problems, u.pkg+": requires exact whole-package transfer race tuple, timeout 600s and local tag")
		}
	}
	for pkg := range want {
		if seen[pkg] != 1 {
			problems = append(problems, fmt.Sprintf("%s: expected one transfer race tuple, got %d (pending test owner)", pkg, seen[pkg]))
		}
	}
	sort.Strings(problems)
	return problems
}

func transferLedgerVariable(t *testing.T, root, path, name string) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			if len(vs.Names) != 1 || vs.Names[0].Name != name || len(vs.Values) != 1 {
				continue
			}
			cl, ok := vs.Values[0].(*ast.CompositeLit)
			if !ok {
				t.Fatalf("%s: %s must remain an explicit ledger", path, name)
			}
			rows := map[string]string{}
			for _, elt := range cl.Elts {
				key, value := elt, ast.Expr(nil)
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					key, value = kv.Key, kv.Value
				}
				keyLit, ok := key.(*ast.BasicLit)
				if !ok || keyLit.Kind != token.STRING {
					t.Fatalf("%s: nonliteral ledger key", name)
				}
				k, err := strconv.Unquote(keyLit.Value)
				if err != nil {
					t.Fatal(err)
				}
				v := ""
				if value != nil {
					lit, ok := value.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Fatalf("%s: nonliteral ledger value", name)
					}
					v, err = strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
				}
				if _, duplicate := rows[k]; duplicate {
					t.Fatalf("%s: duplicate ledger key %s", name, k)
				}
				rows[k] = v
			}
			return rows
		}
	}
	t.Fatalf("%s: missing ledger %s", path, name)
	return nil
}

// The assertion owner must be the shared package, and both calls must be
// direct Test-body statements. Dead helpers and same-named local shims cannot
// supply the package's coverage receipt. The full shape gate checks >=5 rounds.
// origin: hot-update S7 plan §11.3 / T52
func transferDirectCycle(file *ast.File, fn *ast.FuncDecl, anchor string) bool {
	if fn.Recv != nil || fn.Body == nil || anchor == "" {
		return false
	}
	shared := ""
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err == nil && path == "github.com/LinZiyang666/tether/internal/testharness" {
			shared = "testharness"
			if imp.Name != nil {
				shared = imp.Name.Name
			}
		}
	}
	if shared == "" || shared == "." || shared == "_" {
		return false
	}
	exercised := false
	asserts := map[string]int{}
	for _, stmt := range fn.Body.List {
		switch stmt.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			var visit func(ast.Node) bool
			visit = func(n ast.Node) bool {
				if _, literal := n.(*ast.FuncLit); literal {
					return false
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == anchor {
						exercised = true
					}
					// t.Run executes its body; an uncalled literal does not.
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Run" && len(call.Args) == 2 {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == "t" {
							if body, ok := call.Args[1].(*ast.FuncLit); ok {
								ast.Inspect(body.Body, visit)
							}
						}
					}
				}
				return true
			}
			ast.Inspect(stmt, visit)
		}
		expr, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		id, ok := sel.X.(*ast.Ident)
		if ok && id.Name == shared && (sel.Sel.Name == "AssertNoGoroutineLeak" || sel.Sel.Name == "AssertNoFDLeak") {
			if !exercised {
				return false
			}
			asserts[sel.Sel.Name]++
		}
	}
	return asserts["AssertNoGoroutineLeak"] == 1 && asserts["AssertNoFDLeak"] == 1
}

// These assembly owners already exercise their real retained-worker and
// reconnect lifetimes under responsibility-specific names. Require every
// exact site: one inline happy path cannot sign a cut or a joined lookup.
// origin: S7 internal review GATES-LEAD F7 / hot-update S7 plan §11.3 / T52
var transferMovedLifecycleSubjects = map[string]string{
	"internal/xferagent/inline_pull_test.go:TestTransferInlinePullSurvivesCutReplayAndSettlement":      "exerciseInlinePullCut",
	"internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverLandsRetainedPayload":        "exerciseInlinePushReceiver",
	"internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverSurvivesImageCutAndRollback": "exerciseInlinePushReceiverCut",
	"internal/xferserve/inline_link_test.go:TestTransferInlinePrepareCarriesRetainedCopyAndSeal":       "exerciseInlinePrepareLink",
	"internal/xferserve/assemble_test.go:TestTransferConfigWriterSucceedsAcrossGraphReinstall":         "exerciseConfigWriterReinstall",
	"internal/xferserve/settlement_lifetime_test.go:TestTransferSettlementReconnectJoinsRemoteLookup":  "exerciseRemoteSettlementJoin",
}

func transferLifecycleCandidate(key, name, anchor string) bool {
	if name == "TestTransferLifecycleNoLeaks" || anchor == "exerciseTransferLifecycle" || anchor == "exerciseTransferCustody" || transferMovedLifecycleSubjects[key] != "" {
		return true
	}
	for _, moved := range transferMovedLifecycleSubjects {
		if anchor == moved {
			return true
		}
	}
	return false
}

func transferLiveCycles(t *testing.T, root string, risks, anchors map[string]string, inventory map[string]bool) (map[string]bool, []string) {
	t.Helper()
	cycles, seen, qualified := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var problems []string
	for pkg := range risks {
		paths, err := filepath.Glob(filepath.Join(root, pkg, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		functions := map[string]bool{}
		files := map[string]*ast.File{}
		for _, path := range paths {
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files[path] = file
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil && len(fn.Body.List) > 0 {
					functions[fn.Name.Name] = true
				}
			}
		}
		for path, file := range files {
			if !strings.HasSuffix(path, "_test.go") {
				continue
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				rel, err := filepath.Rel(root, path)
				if err != nil {
					t.Fatal(err)
				}
				key := filepath.ToSlash(rel) + ":" + fn.Name.Name
				anchor := anchors[key]
				if !transferLifecycleCandidate(key, fn.Name.Name, anchor) {
					continue
				}
				seen[key] = true
				want := "exerciseTransferLifecycle"
				if pkg == "internal/drillobserve" {
					want = "exerciseTransferCustody"
				}
				if moved := transferMovedLifecycleSubjects[key]; moved != "" {
					want = moved
				}
				if anchor != want || !functions[anchor] || !transferDirectCycle(file, fn, anchor) || !inventory[filepath.ToSlash(rel)+": "+fn.Name.Name] {
					problems = append(problems, key+": requires live exercise, registered anchor/inventory and two direct shared assertions")
					continue
				}
				cycles[pkg], qualified[key] = true, true
			}
		}
	}
	for key, anchor := range anchors {
		_, name, _ := strings.Cut(key, ":")
		if transferLifecycleCandidate(key, name, anchor) {
			path, _, _ := strings.Cut(key, ":")
			pkg := filepath.ToSlash(filepath.Dir(path))
			if _, owned := risks[pkg]; !owned || !seen[key] {
				problems = append(problems, key+": dead or unowned transfer leak anchor")
			}
		}
	}
	for key := range transferMovedLifecycleSubjects {
		path, _, _ := strings.Cut(key, ":")
		if _, owned := risks[filepath.ToSlash(filepath.Dir(path))]; owned && !qualified[key] {
			problems = append(problems, key+": missing measured assembly lifecycle")
		}
	}
	return cycles, problems
}

// Missing producer tests are failures, never a skip or an exemption. Controls
// below verify the join independently while this test waits for actual owners.
// origin: hot-update S7 plan §11.3 / T52
// gate-control: TestTransferRiskCoverageControls
func TestTransferRiskPackagesHaveWholeRaceAndDirectLeaks(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	race := transferLedgerVariable(t, root, "test/e2e/parallel/inventory_test.go", "transferRacePackages")
	leak := transferLedgerVariable(t, root, "test/determinism/leak_assert_shape_test.go", "transferLeakRiskPackages")
	anchors := transferLedgerVariable(t, root, "test/determinism/leak_assert_shape_test.go", "leakExerciseAnchors")
	inventory := readGolden(t, filepath.Join(root, "test/determinism/testdata/test_function_inventory.txt"))
	cycles, problems := transferLiveCycles(t, root, leak, anchors, inventory)
	problems = append(problems, transferCoverageProblems(units, race, leak, cycles)...)
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatal(strings.Join(problems, "\n"))
	}
}

// A second frozen denominator makes deleting matching rows in both registries
// fail too. These are obligations, not claims that the missing tests exist.
// origin: hot-update S7 plan §11.3 / T52
const transferFrozenRisks = "internal/jsstream internal/transferpath internal/filetransfer internal/drillobserve internal/xferworker internal/xfersend internal/xferreceive internal/xfercourier internal/xfertrack internal/xferfinal internal/xferledger internal/xferhistory internal/xferbucket internal/xfergc internal/xfercontrol internal/xferclient internal/xferauth internal/xferbridge internal/xferagent internal/xferserve internal/agent internal/broker internal/brokerimage internal/brokercontrol internal/authcallout internal/cli test/transferharness"

func transferRiskRegistryProblems(race, leak map[string]string) []string {
	var problems []string
	for _, pkg := range []string{"internal/xfercontract", "internal/xfercontract/contractstub", "internal/xferimage"} {
		if transferPurePackages[pkg] == "" {
			problems = append(problems, pkg+": missing pure owner IO/root fact")
		}
	}
	if len(transferPurePackages) != 3 {
		problems = append(problems, "pure owner registry must contain the three frozen contract/codec owners")
	}
	want := map[string]bool{}
	for _, pkg := range strings.Fields(transferFrozenRisks) {
		want[pkg] = true
		for name, rows := range map[string]map[string]string{"race": race, "leak": leak} {
			if _, ok := rows[pkg]; !ok {
				problems = append(problems, pkg+": missing frozen "+name+" risk row")
			}
		}
	}
	for name, rows := range map[string]map[string]string{"race": race, "leak": leak} {
		for pkg := range rows {
			if !want[pkg] {
				problems = append(problems, pkg+": unowned "+name+" risk row")
			}
		}
	}
	return problems
}

// The same predicates judge live tuples and synthetic controls. Every risk
// row is removed independently, including filetransfer/transferpath/drillobserve.
// origin: hot-update S7 plan §11.3 / T52
func TestTransferRiskCoverageControls(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	liveRace := transferLedgerVariable(t, root, "test/e2e/parallel/inventory_test.go", "transferRacePackages")
	liveLeak := transferLedgerVariable(t, root, "test/determinism/leak_assert_shape_test.go", "transferLeakRiskPackages")
	if problems := transferRiskRegistryProblems(liveRace, liveLeak); len(problems) > 0 {
		t.Fatal(problems)
	}
	// Register only tuples whose package tests exist. This does not certify
	// their S7 behavior: the live T52 join still requires transfer exercises.
	present := map[string]string{}
	for _, pkg := range append(append([]string(nil), transferRacePackages...), "internal/xfercontract", "internal/xfercontract/contractstub", "internal/xferimage", "test/cli_e2e") {
		if _, err := os.Stat(filepath.Join(root, pkg)); os.IsNotExist(err) {
			continue
		}
		names, err := partitionPackageTests(root, "./"+pkg+"/...", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(names) > 0 {
			present["./"+pkg+"/..."] = ""
		}
	}
	present["./test/hotupdate/..."] = "hotupdate_integration"
	liveUnits, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	if problems := transferMatrixProblems(liveUnits, present); len(problems) > 0 {
		t.Fatal(problems)
	}
	// Exercise L17 with independent source discovery, without running any
	// integration test or starting a process.
	nativeNames, err := partitionPackageTests(root, "./test/hotupdate/...", "hotupdate_integration")
	if err != nil {
		t.Fatal(err)
	}
	plannedLive, err := shardPackagesWithList(liveUnits, 0, func(u unit) ([]string, error) { return partitionPackageTests(root, u.pkg, u.tags) })
	if err != nil {
		t.Fatal(err)
	}
	if problems := partitionCoverageProblems(plannedLive, "TestTransferContinuationMatrix", "./test/hotupdate/...", "hotupdate_integration", "600s", nativeNames); len(problems) > 0 {
		t.Fatal(problems)
	}

	race, leak, cycles := map[string]string{}, map[string]string{}, map[string]bool{}
	var good []unit
	for _, pkg := range strings.Fields(transferFrozenRisks) {
		race[pkg], leak[pkg], cycles[pkg] = "", "", true
	}
	for pkg := range race {
		good = append(good, unit{matrix: "TestTransferContinuationMatrix", pkg: "./" + pkg + "/...", race: true, timeo: "600s"})
	}
	for pkg := range transferPurePackages {
		good = append(good, unit{matrix: "TestTransferContinuationMatrix", pkg: "./" + pkg + "/...", race: true, timeo: "600s"})
	}
	good = append(good, unit{matrix: "TestTransferContinuationMatrix", pkg: "./test/hotupdate/...", tags: "hotupdate_integration", race: true, timeo: "600s"}, unit{matrix: "TestTransferContinuationMatrix", pkg: "./test/cli_e2e/...", race: true, timeo: "600s"})
	if problems := transferCoverageProblems(good, race, leak, cycles); len(problems) != 0 {
		t.Fatal(problems)
	}
	if problems := transferRiskRegistryProblems(race, leak); len(problems) != 0 {
		t.Fatal(problems)
	}
	for pkg := range race {
		t.Run(pkg, func(t *testing.T) {
			delete(race, pkg)
			if len(transferRiskRegistryProblems(race, leak)) == 0 || len(transferCoverageProblems(good, race, leak, cycles)) == 0 {
				t.Fatal("deleted race risk accepted")
			}
			race[pkg] = ""
			delete(leak, pkg)
			if len(transferRiskRegistryProblems(race, leak)) == 0 || len(transferCoverageProblems(good, race, leak, cycles)) == 0 {
				t.Fatal("deleted leak risk accepted")
			}
			delete(race, pkg)
			if len(transferRiskRegistryProblems(race, leak)) == 0 {
				t.Fatal("both risk rows deleted without detection")
			}
			race[pkg], leak[pkg] = "", ""
			cycles[pkg] = false
			if len(transferCoverageProblems(good, race, leak, cycles)) == 0 {
				t.Fatal("missing direct lifecycle accepted")
			}
			cycles[pkg] = true
		})
	}
	for i, u := range good {
		t.Run(u.pkg, func(t *testing.T) {
			for _, defect := range []string{"delete", "race", "filter", "empty filter", "tag", "budget", "extra", "opaque", "duplicate", "unknown"} {
				bad := append([]unit(nil), good...)
				switch defect {
				case "delete":
					bad = append(bad[:i], bad[i+1:]...)
				case "race":
					bad[i].race = false
				case "filter":
					bad[i].hasRun, bad[i].runFilter = true, "^TestHappy$"
				case "empty filter":
					bad[i].hasRun = true
				case "tag":
					bad[i].tags = "foreign_integration"
				case "budget":
					bad[i].timeo = "601s"
				case "extra":
					bad[i].extra = "-short"
				case "opaque":
					bad[i].whole = true
				case "duplicate":
					bad = append(bad, u)
				case "unknown":
					bad[i].pkg = "./internal/unowned/..."
				}
				if len(transferCoverageProblems(bad, race, leak, cycles)) == 0 {
					t.Fatalf("%s accepted", defect)
				}
			}
		})
	}
	// Synthetic source controls do not sign any absent runtime owner. The
	// live T52 test remains red until the real five-round exercises land.
	const source = `package transferpath
import th "github.com/LinZiyang666/tether/internal/testharness"
func exerciseTransferLifecycle(t *testing.T) { exerciseAtomic(t) }
func TestTransferLifecycleNoLeaks(t *testing.T) {
 beforeG := runtime.NumGoroutine()
 beforeFD := th.FDCount()
 for round := 0; round < 5; round++ { exerciseTransferLifecycle(t) }
 th.AssertNoGoroutineLeak(t, "transfer", beforeG)
 th.AssertNoFDLeak(t, "transfer", beforeFD, 0)
}`
	const site = "internal/transferpath/lifecycle_test.go:TestTransferLifecycleNoLeaks"
	for _, defect := range []string{"positive", "subtest", "anchor", "inventory", "dead ledger", "goroutine helper", "fd helper", "function value", "dead helper", "dead literal", "wrong import", "dead exercise"} {
		t.Run(defect, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "internal/transferpath/lifecycle_test.go")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			text := source
			anchors := map[string]string{site: "exerciseTransferLifecycle"}
			inventory := map[string]bool{strings.Replace(site, ":", ": ", 1): true}
			switch defect {
			case "subtest":
				text = strings.Replace(text, "{ exerciseTransferLifecycle(t) }", `{ t.Run("round", func(t *testing.T) { exerciseTransferLifecycle(t) }) }`, 1)
			case "dead literal":
				text = strings.Replace(text, "{ exerciseTransferLifecycle(t) }", "{ _ = func() { exerciseTransferLifecycle(t) } }", 1)
			case "anchor":
				delete(anchors, site)
			case "inventory":
				clear(inventory)
			case "dead ledger":
				anchors["internal/transferpath/absent_test.go:TestTransferLifecycleNoLeaks"] = "exerciseTransferLifecycle"
			case "goroutine helper":
				text = strings.Replace(text, "th.AssertNoGoroutineLeak", "local.AssertNoGoroutineLeak", 1)
			case "fd helper":
				text = strings.Replace(text, "th.AssertNoFDLeak", "local.AssertNoFDLeak", 1)
			case "function value":
				text = strings.Replace(text, "{ exerciseTransferLifecycle(t) }", "{ _ = exerciseTransferLifecycle }", 1)
			case "dead helper":
				text = strings.Replace(text, " th.AssertNoGoroutineLeak", " func() { th.AssertNoGoroutineLeak", 1)
				text = strings.Replace(text, "beforeFD, 0)\n}", "beforeFD, 0) }\n}", 1)
			case "wrong import":
				text = strings.Replace(text, "internal/testharness", "internal/notshared", 1)
			case "dead exercise":
				text = strings.Replace(text, "func exerciseTransferLifecycle(t *testing.T) { exerciseAtomic(t) }", "", 1)
			}
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			measured, problems := transferLiveCycles(t, root, map[string]string{"internal/transferpath": ""}, anchors, inventory)
			accepted := measured["internal/transferpath"] && len(problems) == 0
			if accepted != (defect == "positive" || defect == "subtest") {
				t.Fatalf("%s: accepted=%v problems=%v", defect, accepted, problems)
			}
		})
	}
	// Freeze the adaptation independently, then inject each missing half at
	// each exact moved site. Matching names elsewhere cannot supply a receipt.
	// origin: S7 internal review GATES-LEAD F7 / T52
	const movedSubjects = `internal/xferagent/inline_pull_test.go:TestTransferInlinePullSurvivesCutReplayAndSettlement exerciseInlinePullCut
internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverLandsRetainedPayload exerciseInlinePushReceiver
internal/xferagent/inline_push_test.go:TestTransferInlinePushReceiverSurvivesImageCutAndRollback exerciseInlinePushReceiverCut
internal/xferserve/inline_link_test.go:TestTransferInlinePrepareCarriesRetainedCopyAndSeal exerciseInlinePrepareLink
internal/xferserve/assemble_test.go:TestTransferConfigWriterSucceedsAcrossGraphReinstall exerciseConfigWriterReinstall
internal/xferserve/settlement_lifetime_test.go:TestTransferSettlementReconnectJoinsRemoteLookup exerciseRemoteSettlementJoin`
	frozenMoved := map[string]string{}
	for _, line := range strings.Split(movedSubjects, "\n") {
		fields := strings.Fields(line)
		frozenMoved[fields[0]] = fields[1]
	}
	if !reflect.DeepEqual(transferMovedLifecycleSubjects, frozenMoved) {
		t.Fatal("assembly lifecycle denominator changed")
	}
	for site, exercise := range frozenMoved {
		for _, defect := range []string{"positive", "anchor", "inventory", "missing test", "missing subject", "dead ledger", "goroutine helper", "fd helper", "function value", "dead exercise"} {
			t.Run(site+"/"+defect, func(t *testing.T) {
				root := t.TempDir()
				anchors, inventory := map[string]string{}, map[string]bool{}
				path, name, _ := strings.Cut(site, ":")
				pkg := filepath.ToSlash(filepath.Dir(path))
				// Every required subject for this owner must be present, even
				// when another valid lifecycle has already signed the package.
				for key, anchor := range frozenMoved {
					file, test, _ := strings.Cut(key, ":")
					if filepath.ToSlash(filepath.Dir(file)) != pkg {
						continue
					}
					text := strings.ReplaceAll(source, "exerciseTransferLifecycle", anchor)
					text = strings.ReplaceAll(text, "TestTransferLifecycleNoLeaks", test)
					if key == site {
						switch defect {
						case "missing test", "missing subject":
							text = strings.ReplaceAll(text, name, "TestUnrelated")
						case "goroutine helper":
							text = strings.Replace(text, "th.AssertNoGoroutineLeak", "local.AssertNoGoroutineLeak", 1)
						case "fd helper":
							text = strings.Replace(text, "th.AssertNoFDLeak", "local.AssertNoFDLeak", 1)
						case "function value":
							text = strings.Replace(text, "{ "+exercise+"(t) }", "{ _ = "+exercise+" }", 1)
						case "dead exercise":
							text = strings.Replace(text, "func "+exercise+"(t *testing.T) { exerciseAtomic(t) }", "", 1)
						}
					}
					// Two push tests share a production test file. Fixture
					// declarations can live in separate files in the same package.
					if err := os.MkdirAll(filepath.Join(root, pkg), 0o700); err != nil {
						t.Fatal(err)
					}
					fixture := filepath.Join(root, file)
					if raw, err := os.ReadFile(fixture); err == nil {
						text = string(raw) + "\nfunc " + strings.SplitN(text, "func ", 2)[1]
					}
					if err := os.WriteFile(fixture, []byte(text), 0o600); err != nil {
						t.Fatal(err)
					}
					anchors[key] = anchor
					inventory[file+": "+test] = true
				}
				switch defect {
				case "anchor":
					delete(anchors, site)
				case "inventory":
					delete(inventory, path+": "+name)
				case "missing subject":
					delete(anchors, site)
					delete(inventory, path+": "+name)
				case "dead ledger":
					anchors[pkg+"/absent_test.go:"+name] = exercise
				}
				measured, problems := transferLiveCycles(t, root, map[string]string{pkg: ""}, anchors, inventory)
				if accepted := measured[pkg] && len(problems) == 0; accepted != (defect == "positive") {
					t.Fatalf("%s: accepted=%v problems=%v", defect, accepted, problems)
				}
			})
		}
	}
	// Tagged whole declarations are expanded one Test per process by the
	// existing FIX-MATRIX sharder, independent of requested shard count.
	var tagged unit
	for _, u := range good {
		if u.tags != "" {
			tagged = u
		}
	}
	tagged.name = "integration"
	names := []string{"TestTransferOne", "TestTransferTwo"}
	planned, err := shardPackagesWithList([]unit{tagged}, 1, func(unit) ([]string, error) { return names, nil })
	if err != nil {
		t.Fatal(err)
	}
	if problems := partitionCoverageProblems(planned, tagged.matrix, tagged.pkg, tagged.tags, "600s", names); len(problems) > 0 {
		t.Fatal(problems)
	}
}
