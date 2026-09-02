package determinism

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// test_inventory_test.go — every Test/Fuzz/Benchmark function in the tree, keyed `path: Name`, pinned
// in a golden that only ever GROWS.
//
// WHY THIS EXISTS
// ---------------
// Refactoring production code has a safety net: the tests. Refactoring TEST code has none. Delete an
// assertion, drop a test function while merging two files, lose a whole `_test.go` in a `git mv` gone
// wrong — the suite stays green, because the thing that would have noticed is the thing you removed.
// This repo has the receipts: layering_test.go:279-297 records that merging four regression files lost
// a whole row AND a clause while everything stayed green; promised_guard_test.go found 34 dead promises
// the day it landed; the e2e runner silently dropped TestAllPhases and eleven phase suites and got
// FASTER (docs/reviews/e2e-parallel-plan.md §4). None of those were caught by a test, because a test
// suite cannot notice its own shrinkage.
//
// This gate is the cross-commit identity receipt that was missing. It is the precondition for every
// other change to test code in docs/reviews/test-system-overhaul-plan.md: harness absorption, matrix
// de-duplication, the p11 merge, the fuzz targets — each of them is allowed to touch tests only
// because this file will name exactly what disappeared.
//
// THE SHAPE
// ---------
//   - Source is PARSED (go/parser, top-level *ast.FuncDecl with no receiver, name by cmd/go's isTest
//     rule) with the SAME extractor the naming freeze uses (testFuncDecls, shared with
//     test_naming_test.go), not `go test -list`: -list needs a build per tag set and would miss any
//     file whose tag is not passed; a source scan sees every `_test.go` regardless of tags, which is
//     the point. NOT a line-anchored regex: the first version was, and it inventoried seven phantom
//     functions that only existed inside raw-string synthetic samples of other gates' self-checks
//     (`func TestX(` at column 0 inside a backtick string is source TEXT but not a declaration).
//     Those seven lines were removed from the golden by hand on 2026-09-01 — internal review L1-F1.
//   - Keys are `path: Name`, site-scoped like the function-name ledger (M7): a name is not a reusable
//     pass, and a moved file changes its keys.
//   - The golden only grows. `-update-test-inventory` APPENDS new keys and REFUSES to remove any —
//     the same refuse-to-widen property structural_budget_test.go relies on, mirrored: here shrinking
//     is the event that must be visible in review. Deleting or renaming a test means hand-editing the
//     golden and saying why in the commit message. That friction is the whole value.
//   - The live-tree floor is legal under G2b: the success state of this gate still contains what it
//     counts (a repo with tests), so counting the live tree cannot fail on a correct tree.
//
// gate-control: TestTestInventoryExtractorRecognisesTheShapes

// testFunctionInventoryGolden is relative to this package directory.
const testFunctionInventoryGolden = "testdata/test_function_inventory.txt"

// testFunctionInventoryFloor is the live-tree non-vacuity floor. The golden held 2953 lines when the
// gate landed (2026-09-01: Test + Fuzz + Benchmark; the plan's "2880" counted `func Test` only, a
// different measure — internal review L5-F13); the floor sits below that so a legitimate hand-edited
// removal does not trip it, but far above anything a broken walk (wrong root, skipped tree) would
// produce.
const testFunctionInventoryFloor = 2800

var updateTestInventory = flag.Bool("update-test-inventory", false,
	"append newly discovered test functions to the inventory golden (never removes a line; refuses if any recorded function is missing)")

// isTestFuncName is cmd/go's rule (internal/load/test.go isTest): the prefix, then either nothing or
// a rune that is not lower-case. `Testify` is NOT a test to the toolchain and must not be pinned.
func isTestFuncName(name string) bool {
	for _, prefix := range []string{"Test", "Benchmark", "Fuzz"} {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if len(name) == len(prefix) {
			return true
		}
		r, _ := utf8.DecodeRuneInString(name[len(prefix):])
		return !unicode.IsLower(r)
	}
	return false
}

// testFuncDecls returns the names of every top-level Test/Fuzz/Benchmark function DECLARED in src —
// go/parser, receiver-less FuncDecl, isTestFuncName. A declaration-shaped line inside a string literal
// is not a declaration. Shared by this gate, its self-check and the naming freeze
// (TestNoNewProcessNamedTestFunctions) so the three cannot drift.
func testFuncDecls(filename string, src []byte) ([]string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), filename, src, 0)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || !isTestFuncName(fd.Name.Name) {
			continue
		}
		names = append(names, fd.Name.Name)
	}
	return names, nil
}

// extractTestFunctionKeys returns the `rel: Name` keys for every Test/Fuzz/Benchmark declaration in
// one source file. Shared by the gate and its self-check so the two cannot drift.
func extractTestFunctionKeys(rel string, src []byte) ([]string, error) {
	names, err := testFuncDecls(rel, src)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(names))
	for _, n := range names {
		keys = append(keys, rel+": "+n)
	}
	return keys, nil
}

// collectTestFunctionInventory walks the repo and returns every key plus the number of `_test.go`
// files it read (the walk-health signal the floor is derived from).
func collectTestFunctionInventory(root string) (map[string]bool, int, error) {
	keys := map[string]bool{}
	files := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
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
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, rlerr := filepath.Rel(root, p)
		if rlerr != nil {
			return rlerr
		}
		files++
		fileKeys, kerr := extractTestFunctionKeys(filepath.ToSlash(rel), src)
		if kerr != nil {
			return kerr
		}
		for _, k := range fileKeys {
			keys[k] = true
		}
		return nil
	})
	return keys, files, err
}

func readTestFunctionInventory(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read inventory golden %s: %v\n\nFirst run? Generate it with\n"+
			"  go test ./test/determinism/ -run TestTestFunctionInventoryOnlyGrows -update-test-inventory",
			path, err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[line] = true
	}
	return out
}

func writeTestFunctionInventory(t *testing.T, path string, keys map[string]bool) {
	t.Helper()
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var b strings.Builder
	b.WriteString("# test_function_inventory.txt — every Test/Fuzz/Benchmark function in the tree, `path: Name`.\n")
	b.WriteString("# Maintained by test/determinism/test_inventory_test.go. This file only GROWS:\n")
	b.WriteString("#   new function      -> go test ./test/determinism/ -run TestTestFunctionInventoryOnlyGrows -update-test-inventory\n")
	b.WriteString("#   deleted / renamed -> hand-remove the line and say why in the commit message. The updater refuses.\n")
	b.WriteString("# A line that names a function which no longer exists is the gate doing its job, not a stale entry.\n")
	for _, k := range sorted {
		b.WriteString(k)
		b.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create inventory golden dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write inventory golden: %v", err)
	}
}

// TestTestFunctionInventoryOnlyGrows is the gate.
func TestTestFunctionInventoryOnlyGrows(t *testing.T) {
	root := repoRoot(t)
	current, files, err := collectTestFunctionInventory(root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Live-tree floor (G2b-legal: the success state still contains tests).
	if len(current) < testFunctionInventoryFloor {
		t.Fatalf("only %d test functions found across %d _test.go files (floor %d) — the walk is not "+
			"seeing the tree (wrong root? SkipDir on the wrong name?)", len(current), files, testFunctionInventoryFloor)
	}

	goldenPath := filepath.Join(root, "test", "determinism", testFunctionInventoryGolden)
	if _, statErr := os.Stat(goldenPath); os.IsNotExist(statErr) && *updateTestInventory {
		writeTestFunctionInventory(t, goldenPath, current)
		t.Logf("wrote initial inventory: %d functions", len(current))
		return
	}
	golden := readTestFunctionInventory(t, goldenPath)

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
		// Refuse the update path outright. Shrinkage is a decision, and decisions are made by hand in
		// the golden with a reason in the commit message — never by re-running a tool.
		t.Errorf("%d test function(s) recorded in the inventory no longer exist:\n  %s\n\n"+
			"If they were deleted or renamed on purpose, remove those lines from\n  %s\n"+
			"by hand and explain why in the commit message. -update-test-inventory will not do it for you:\n"+
			"a suite cannot notice its own shrinkage, so shrinkage has to be visible in review.",
			len(missing), strings.Join(missing, "\n  "), testFunctionInventoryGolden)
		if *updateTestInventory {
			t.Fatalf("-update-test-inventory refused: %d recorded function(s) are missing (listed above)", len(missing))
		}
	}
	if len(extra) > 0 {
		if *updateTestInventory && len(missing) == 0 {
			for k := range current {
				golden[k] = true
			}
			writeTestFunctionInventory(t, goldenPath, golden)
			t.Logf("appended %d function(s) to the inventory", len(extra))
			return
		}
		t.Errorf("%d new test function(s) are not in the inventory:\n  %s\n\n"+
			"Record them (append-only):\n"+
			"  go test ./test/determinism/ -run TestTestFunctionInventoryOnlyGrows -update-test-inventory",
			len(extra), strings.Join(extra, "\n  "))
	}
}

// TestTestInventoryExtractorRecognisesTheShapes is the G2 self-check: a synthetic file with every
// declaration shape the extractor must and must not count. If the shared regex is ever narrowed, this
// reddens before the golden silently loses a class of functions.
func TestTestInventoryExtractorRecognisesTheShapes(t *testing.T) {
	// The sample is built by concatenation so that THIS file never carries `func Test…(` at column 0
	// inside a string: the extractor is a parser now, but the naming freeze's file-header check and a
	// reader's grep still see source text, and a self-check should not be the thing that looks like
	// what it forbids.
	decl := func(name string) string { return "func " + name + "(t *testing.T) {}\n" }
	src := []byte("package x\n\nimport \"testing\"\n\n" +
		decl("TestPlain") +
		decl("TestWith_Underscore9") +
		"func " + "FuzzSomething(f *testing.F) {}\n" +
		"func " + "BenchmarkSomething(b *testing.B) {}\n" +
		decl("Test") +
		decl("helperTest") +
		decl("testLower") +
		"func " + "(s *suite) TestMethod() {}\n" +
		// isTest rule: a lower-case rune after the prefix is not a test to the toolchain
		// (cmd/go/internal/load/test.go). The first version counted it, with a comment asserting the
		// opposite — internal review L1-F1.
		"func " + "Testify() {}\n" +
		// Declarations INSIDE STRING LITERALS are the phantom shape: a gate's synthetic sample. Seven
		// of these reached the golden through the regex version (raft_timing_guard 4,
		// leak_assert_shape 1, e2e/parallel/mixed_command_back 2).
		"const synth = `package synth\n" +
		"func " + "TestPhantomRaw(t *testing.T) {}\n" +
		"`\n" +
		"var s = \"\\nfunc " + "TestPhantomInterpreted(t *testing.T) {}\"\n" +
		"func " + "TestReal(t *testing.T) { _ = synth; _ = s }\n")
	got, err := extractTestFunctionKeys("pkg/x_test.go", src)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"pkg/x_test.go: BenchmarkSomething",
		"pkg/x_test.go: FuzzSomething",
		"pkg/x_test.go: Test",
		"pkg/x_test.go: TestPlain",
		"pkg/x_test.go: TestReal",
		"pkg/x_test.go: TestWith_Underscore9",
	}
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("extractor drifted:\n got  %v\n want %v", got, want)
	}
	// Negative shapes: a method, a lower-case helper, a non-prefixed helper, the isTest-excluded
	// name and both string-literal phantoms must NOT be counted — otherwise the inventory would pin
	// things `go test` never runs, a rename of a helper would masquerade as a lost test, and a gate's
	// synthetic sample could never be edited without a hand-edit of the golden.
	for _, k := range got {
		for _, bad := range []string{"helperTest", "testLower", "TestMethod", "Testify", "TestPhantomRaw", "TestPhantomInterpreted"} {
			if strings.HasSuffix(k, ": "+bad) {
				t.Fatalf("extractor counted %q, which go test would never run", bad)
			}
		}
	}
	// A file that does not parse is an error, not an empty file: silence here would let a broken
	// _test.go drop every one of its functions from the inventory without a word.
	if _, err := extractTestFunctionKeys("pkg/broken_test.go", []byte("package x\nfunc Test(")); err == nil {
		t.Fatal("a parse failure must surface, not read as zero functions")
	}
}
