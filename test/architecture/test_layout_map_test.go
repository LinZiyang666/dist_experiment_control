package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// test_layout_map_test.go — test/README.md is the registry of test/'s top-level directories, and the
// phase-named directories are a FROZEN, EXACT set.
//
// WHY THIS EXISTS
// ---------------
// Layer is a property of a test, not of its directory (docs/testing-standards.md §零), so the only
// way to know what a directory holds, which build tag gates it and which e2e matrix runs it is a map
// somebody keeps true. test/README.md was a 10-line stub that described one subdirectory for six
// weeks; this gate reconciles the map with the tree both ways and checks that every matrix name and
// build tag the map cites really exists.
//
// The phase-named directories (test/p1…, test/d3…) are CLAUDE.md §5's file-naming rule at directory
// level — but they are load-bearing for the e2e matrix (all_phases_test.go literals, shard.go, ledger
// path keys) and the review that landed this gate decided NOT to migrate them (plan §0 A2). So they
// are frozen as an exact set, TLS-pairing style: a directory disappearing is red, a new
// `<letter><digits>` directory is red. New directories are named by subject.
//
// gate-control: TestLayoutMapParserSeesTheShapes

var (
	layoutRowRe = regexp.MustCompile("^\\| `([A-Za-z0-9_]+)/` \\|")
	// Any backticked Test name a row cites must be a matrix func in all_phases_test.go — ANY, not
	// only names ending in Matrix/Phases: the first version matched the suffix, and a mutation that
	// appended a stray letter after "Matrix" sailed through unchecked.
	layoutMatrixRe    = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")
	layoutTagRe       = regexp.MustCompile("`([a-z0-9_]+_(?:integration|matrix))`")
	phaseNamedDirRe   = regexp.MustCompile(`^[a-z][0-9]+$`)
	allPhasesFuncRe   = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9]*)\(`)
	frozenPhaseDirSet = []string{
		"d3", "d4", "d5", "d6", "d7", "d8", "d9",
		"p1", "p10", "p13", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9",
	}
)

// layoutMapRows parses README's table: directory -> the row text. Pure; shared with the self-check.
func layoutMapRows(readme string) map[string]string {
	rows := map[string]string{}
	for _, line := range strings.Split(readme, "\n") {
		if m := layoutRowRe.FindStringSubmatch(line); m != nil {
			rows[m[1]] = line
		}
	}
	return rows
}

// testTopLevelDirs lists test/'s direct subdirectories that contain at least one .go or .sh file
// anywhere beneath them.
func testTopLevelDirs(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "test"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		has := false
		_ = filepath.WalkDir(filepath.Join(root, "test", e.Name()), func(p string, d os.DirEntry, err error) error {
			if err != nil || has {
				return nil
			}
			if !d.IsDir() && (strings.HasSuffix(p, ".go") || strings.HasSuffix(p, ".sh")) {
				has = true
			}
			return nil
		})
		if has {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func TestTestLayoutMapIsReconciledWithTheTree(t *testing.T) {
	root := repoRoot(t)
	readme, err := os.ReadFile(filepath.Join(root, "test", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	rows := layoutMapRows(string(readme))
	if len(rows) < 20 {
		t.Fatalf("parsed only %d directory rows from test/README.md — the table moved or the parser broke", len(rows))
	}
	dirs := testTopLevelDirs(t, root)

	// Both directions.
	var unregistered, phantom []string
	onDisk := map[string]bool{}
	for _, d := range dirs {
		onDisk[d] = true
		if _, ok := rows[d]; !ok {
			unregistered = append(unregistered, d)
		}
	}
	for d := range rows {
		if !onDisk[d] {
			phantom = append(phantom, d)
		}
	}
	sort.Strings(phantom)
	if len(unregistered) > 0 {
		t.Errorf("%d test/ director(y/ies) are not in test/README.md's table: %v", len(unregistered), unregistered)
	}
	if len(phantom) > 0 {
		t.Errorf("test/README.md lists %d director(y/ies) that do not exist: %v", len(phantom), phantom)
	}

	// Frozen phase-named set: exact.
	var present []string
	for _, d := range dirs {
		if phaseNamedDirRe.MatchString(d) {
			present = append(present, d)
		}
	}
	if strings.Join(present, ",") != strings.Join(frozenPhaseDirSet, ",") {
		t.Errorf("phase-named test directories drifted from the frozen set:\n  on disk: %v\n  frozen:  %v\n\n"+
			"New directories are named by subject (docs/testing-standards.md §0.2). Removing one means the "+
			"matrix, the ledgers and this set all change in the same commit.", present, frozenPhaseDirSet)
	}

	// Every matrix the map cites exists; every tag it cites is in the Makefile.
	allPhases, err := os.ReadFile(filepath.Join(root, "test", "e2e", "all_phases_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	matrices := map[string]bool{}
	for _, m := range allPhasesFuncRe.FindAllStringSubmatch(string(allPhases), -1) {
		matrices[m[1]] = true
	}
	tags := makefileTestTags(t, root)
	var badRefs []string
	for d, row := range rows {
		for _, m := range layoutMatrixRe.FindAllStringSubmatch(row, -1) {
			if !matrices[m[1]] {
				badRefs = append(badRefs, d+": matrix "+m[1]+" is not a func in all_phases_test.go")
			}
		}
		for _, m := range layoutTagRe.FindAllStringSubmatch(row, -1) {
			if !tags[m[1]] {
				badRefs = append(badRefs, d+": tag "+m[1]+" is not in the Makefile's ALL_TEST_TAGS")
			}
		}
	}
	sort.Strings(badRefs)
	if len(badRefs) > 0 {
		t.Errorf("%d stale reference(s) in test/README.md:\n  %s", len(badRefs), strings.Join(badRefs, "\n  "))
	}
	if len(matrices) < 10 {
		t.Fatalf("parsed only %d matrix functions from all_phases_test.go — non-vacuity floor", len(matrices))
	}
}

// TestLayoutMapParserSeesTheShapes is the G2 self-check for the row parser and the phase-name
// predicate (synthetic: the frozen set's success state is exactly the live set, so it is not counted).
func TestLayoutMapParserSeesTheShapes(t *testing.T) {
	table := "| 目录 | 层 |\n|---|---|\n| `chaos/` | L3 |\n| `e2e/` | L5 `e2e_matrix` `TestAllPhases` |\n| not a row |\n| `bad name/` | x |\n"
	rows := layoutMapRows(table)
	if len(rows) != 2 || rows["chaos"] == "" || rows["e2e"] == "" {
		t.Fatalf("layoutMapRows parsed %v", rows)
	}
	if m := layoutMatrixRe.FindStringSubmatch(rows["e2e"]); m == nil || m[1] != "TestAllPhases" {
		t.Fatalf("matrix ref not parsed from %q", rows["e2e"])
	}
	if m := layoutTagRe.FindStringSubmatch(rows["e2e"]); m == nil || m[1] != "e2e_matrix" {
		t.Fatalf("tag ref not parsed from %q", rows["e2e"])
	}
	for _, d := range []string{"p1", "p13", "d9", "z0"} {
		if !phaseNamedDirRe.MatchString(d) {
			t.Errorf("%q must read as phase-named", d)
		}
	}
	for _, d := range []string{"chaos", "cli_e2e", "d", "10", "p1x", "proxydial"} {
		if phaseNamedDirRe.MatchString(d) {
			t.Errorf("%q must NOT read as phase-named", d)
		}
	}
}
