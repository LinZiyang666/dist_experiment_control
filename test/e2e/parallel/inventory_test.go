package main

import (
	"flag"
	"fmt"
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
	"test/architecture/layering_test.go": "exec.Command(\"go\", args...) where args is built two lines above " +
		"starting with the literal \"list\" — it reads `go list -deps`, never runs a test; the predicate cannot " +
		"see through the variable and fail-closed is the right default for everything it cannot see through.",
}

const goForkExceptionsCap = 1

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
			if u.race && u.runFilter == "" && strings.TrimSuffix(strings.TrimPrefix(u.pkg, "./"), "/...") == pkg {
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

func riskyPackages(t *testing.T, root string) []string {
	t.Helper()
	imports := []string{
		`"github.com/hashicorp/raft"`, `"github.com/hashicorp/yamux"`,
		`"github.com/LinZiyang666/tether/internal/tunnel"`, `"github.com/LinZiyang666/tether/internal/pty"`,
	}
	set := map[string]bool{}
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
