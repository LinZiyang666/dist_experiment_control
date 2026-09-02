package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// gate_standards_test.go — every gate CLAUDE.md names carries a `// gate-control:` anchor pointing at
// its own positive/negative control test, and every draining ledger declares the unit its keys use.
//
// WHY THIS EXISTS
// ---------------
// docs/testing-standards.md G1 ("inject the defect, the gate must go red") is a one-time act that
// lives in commit messages and plan documents. Nothing in the tree says WHICH test is a gate's control
// — the synthetic positive/negative sample the gate's predicate is run against — so a gate whose
// predicate quietly stopped matching anything (the G2 shape) has no marked test to redden. The batch-B
// identity tests, the first reverse-ACL reconciler and two "mutation-verified" clauses in the
// remote-fs plan that turned out to be tautologies all had this in common: nobody could point at the
// control. This gate makes the pointer a required, checked anchor:
//
//	// gate-control: Test<Name>SeesTheShapes
//
// in the gate file, naming a Test in the SAME file (so promised_guard also checks it exists). Gates
// that predate the rule sit in a draining ledger. This is the THIN form the review settled on (plan
// §0 A11): an anchor plus existence, not an AST proof that the control feeds the same predicate —
// that would mean rewriting 26 gates around a shared predicate to satisfy a meta-gate.
//
// The second half (G3): every `legacy*` ledger in test/architecture and test/determinism must be
// registered with the unit of its keys — file, site (`path: func`), or promise — because a ledger
// keyed by bare file name silently covers every future site in that file (the `unresolved` incident
// in testing-standards G3).
//
// gate-control: TestGateStandardsParserSeesTheShapes

var (
	gateControlAnchorRe = regexp.MustCompile(`(?m)^// gate-control: (Test[A-Za-z0-9_]+)\s*$`)
	gateFuncDeclRe      = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
	// Two spellings of "a table that excuses sites from a gate": the `legacy*` draining ledgers, and
	// the `*Exceptions` / `*Exclusions` / `*Exemptions` tables. The first version registered only the
	// former, so four file- and package-keyed tables — the shape G3 warns about — sat outside the
	// registry (internal review L1-F8).
	legacyLedgerDeclRe = regexp.MustCompile(`(?m)^var ((?:legacy[A-Za-z0-9_]+)|(?:[A-Za-z0-9_]+(?:Exceptions|Exclusions|Exemptions))) = (?:func\(\) )?map\[string\]`)
)

// ledgerKeyUnits registers every draining ledger with the unit of its keys. A ledger declared in the
// tree but absent here is red; an entry here with no declaration is red. Units: file (a whole file —
// covers every future site in it), package (a whole package — wider still), site (`path: func`, or
// `path:Test`), promise (a promised Test name).
var ledgerKeyUnits = map[string]string{
	"legacyMissingGuards":         "promise", // promised_guard_test.go: the promised Test name
	"legacyProcessNamedTestFiles": "file",    // test_naming_test.go: a file name (its target is zero files)
	"legacyProcessNamedTestFuncs": "site",    // legacy_process_named_funcs.go: path: Func
	"legacyProductTimingSleeps":   "site",    // raft_timing_guard_test.go: path: func
	"legacySleepBarriers":         "site",    // legacy_sleep_barriers.go: path: func
	"legacyCtxBackgroundSites":    "site",    // legacy_ctx_background_sites.go: path: func
	"legacyLeaderPremiseSites":    "site",    // legacy_leader_premise_sites.go: path: func
	"legacyGatesWithoutControls":  "file",    // this file: a gate file (one anchor per file is the unit)
	"simGateSetExclusions":        "file",    // simcluster_gate_set_test.go: a tests/*.sh script
	"integrationTagExceptions":    "file",    // build_tags_test.go: a tag-gated _test.go outside its suite dir
	"leakCoverageExceptions":      "package", // leak_assert_shape_test.go: a risky package with no guard
	"singleExerciseExemptions":    "site",    // leak_assert_shape_test.go: path:Test
}

// gateAnchorStatus classifies one gate file's source. Pure; shared with the self-check.
func gateAnchorStatus(src string) (anchor string, resolved bool) {
	m := gateControlAnchorRe.FindStringSubmatch(src)
	if m == nil {
		return "", false
	}
	anchor = m[1]
	for _, fm := range gateFuncDeclRe.FindAllStringSubmatch(src, -1) {
		if fm[1] == anchor {
			return anchor, true
		}
	}
	return anchor, false
}

// claudeGateTestFiles lists the `_test.go` paths CLAUDE.md's gate table names in its location column.
func claudeGateTestFiles(t *testing.T, root string) []string {
	t.Helper()
	claude, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(claude)
	hdr := strings.Index(body, "| 闸门 | 位置 | 管什么 |")
	if hdr < 0 {
		t.Fatal("CLAUDE.md no longer has the gate table")
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(body[hdr:], "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			break
		}
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		if len(cells) < 2 {
			continue
		}
		for _, m := range claudeGatePathRe.FindAllStringSubmatch(cells[1], -1) {
			if strings.HasSuffix(m[1], "_test.go") {
				seen[m[1]] = true
			}
		}
	}
	var out []string
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func TestEveryGateNamesItsControl(t *testing.T) {
	root := repoRoot(t)
	files := claudeGateTestFiles(t, root)
	if len(files) < 15 {
		t.Fatalf("parsed only %d gate test files from CLAUDE.md's table — the table or the parser moved", len(files))
	}
	var missing, dangling, stale []string
	for _, f := range files {
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f)))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		anchor, ok := gateAnchorStatus(string(src))
		switch {
		case ok:
			if legacyGatesWithoutControls[f] {
				stale = append(stale, f)
			}
		case anchor != "":
			dangling = append(dangling, f+": gate-control names "+anchor+", which is not a func in that file")
		case !legacyGatesWithoutControls[f]:
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d gate file(s) named in CLAUDE.md carry no `// gate-control: TestXxx` anchor and are not in the ledger:\n  %s\n\n"+
			"Write the positive/negative control (synthetic samples through the gate's own predicate — "+
			"docs/testing-standards.md G1/G2), then anchor it.", len(missing), strings.Join(missing, "\n  "))
	}
	if len(dangling) > 0 {
		t.Errorf("%d dangling anchor(s):\n  %s", len(dangling), strings.Join(dangling, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("%d gate(s) now carry an anchor but are still in legacyGatesWithoutControls — delete the line(s):\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}
	for f := range legacyGatesWithoutControls {
		found := false
		for _, g := range files {
			if g == f {
				found = true
			}
		}
		if !found {
			t.Errorf("legacyGatesWithoutControls names %s, which CLAUDE.md's table no longer lists — delete the entry", f)
		}
	}
	if n := len(legacyGatesWithoutControls); n != legacyGatesWithoutControlsCap {
		t.Errorf("legacyGatesWithoutControls has %d entries, cap says %d — this ledger only drains; move the cap in the same commit", n, legacyGatesWithoutControlsCap)
	}
}

func TestEveryLedgerDeclaresItsKeyUnit(t *testing.T) {
	root := repoRoot(t)
	declared := map[string]string{}
	for _, dir := range []string{"test/architecture", "test/determinism"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			src, err := os.ReadFile(filepath.Join(root, dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range legacyLedgerDeclRe.FindAllStringSubmatch(string(src), -1) {
				declared[m[1]] = dir + "/" + e.Name()
			}
		}
	}
	if len(declared) < 4 {
		t.Fatalf("found only %d legacy* ledger declarations — the scan is not seeing the tree", len(declared))
	}
	for name, where := range declared {
		unit, ok := ledgerKeyUnits[name]
		if !ok {
			t.Errorf("ledger %s (%s) is not registered in ledgerKeyUnits — say whether its keys are file, site or promise "+
				"(docs/testing-standards.md G3: a file-keyed ledger silently covers every future site in that file)", name, where)
			continue
		}
		switch unit {
		case "file", "package", "site", "promise":
		default:
			t.Errorf("ledger %s registered with unknown key unit %q", name, unit)
		}
	}
	for name := range ledgerKeyUnits {
		if _, ok := declared[name]; !ok {
			t.Errorf("ledgerKeyUnits registers %s, which is not declared anywhere — delete the entry", name)
		}
	}
}

// TestEveryAnchoredGateIsNamedInCLAUDEMd is the reverse direction: a file under test/architecture or
// test/determinism that carries a `// gate-control:` anchor IS a gate, and CLAUDE.md's table must
// name it (by file, or by its directory) — otherwise it sits outside gate_standards' and
// gate_registry's reconciliation, and the next person who deletes its control test finds nothing
// red. On the day this landed, leader_premise_test.go had an anchor and no row (internal review
// L5-F2 / L6-F5).
func TestEveryAnchoredGateIsNamedInCLAUDEMd(t *testing.T) {
	root := repoRoot(t)
	named := map[string]bool{}
	for _, f := range claudeGateTestFiles(t, root) {
		named[f] = true
	}
	claude, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatal(err)
	}
	var orphans []string
	anchored := 0
	for _, dir := range []string{"test/architecture", "test/determinism"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		dirNamed := strings.Contains(string(claude), "`"+dir+"/`") // a row may name the directory
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			rel := dir + "/" + e.Name()
			src, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil {
				t.Fatal(err)
			}
			if !gateControlAnchorRe.Match(src) {
				continue
			}
			anchored++
			if named[rel] || dirNamed {
				continue
			}
			orphans = append(orphans, rel)
		}
	}
	if anchored < 10 {
		t.Fatalf("found only %d anchored gate files — the scan is not seeing the tree", anchored)
	}
	sort.Strings(orphans)
	if len(orphans) > 0 {
		t.Errorf("%d gate file(s) carry a gate-control anchor but CLAUDE.md's table does not name them (or their directory):\n  %s",
			len(orphans), strings.Join(orphans, "\n  "))
	}
}

// TestGateStandardsParserSeesTheShapes is the G2 self-check for gateAnchorStatus.
func TestGateStandardsParserSeesTheShapes(t *testing.T) {
	resolved := "package x\n\n// gate-control: TestFooSeesTheShapes\n\nfunc TestFoo(t *testing.T) {}\nfunc TestFooSeesTheShapes(t *testing.T) {}\n"
	if a, ok := gateAnchorStatus(resolved); !ok || a != "TestFooSeesTheShapes" {
		t.Fatalf("resolved anchor: got (%q, %v)", a, ok)
	}
	dangling := "package x\n\n// gate-control: TestNope\n\nfunc TestFoo(t *testing.T) {}\n"
	if a, ok := gateAnchorStatus(dangling); ok || a != "TestNope" {
		t.Fatalf("dangling anchor: got (%q, %v)", a, ok)
	}
	none := "package x\n\n// gate control: TestFoo (wrong spelling)\nfunc TestFoo(t *testing.T) {}\n"
	if a, ok := gateAnchorStatus(none); ok || a != "" {
		t.Fatalf("no anchor: got (%q, %v)", a, ok)
	}
	inProse := "package x\n\n// The gate-control: TestFoo idea is discussed here but not declared.\nfunc TestFoo(t *testing.T) {}\n"
	if a, _ := gateAnchorStatus(inProse); a != "" {
		t.Fatalf("an anchor must start the line; prose mention parsed as %q", a)
	}
}
