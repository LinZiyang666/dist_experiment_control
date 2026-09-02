// Package architecture holds the structural gates: invariants about the SHAPE of the tree
// (which package may import which, how big a package is allowed to get, which build tags exist)
// rather than about the behaviour of any one unit.
//
// Placement note (S3 §5.0 chose this directory; the line-2 plan §15 confirmed it): gates that assert
// invariants about the tree's SHAPE OR ITS OWN CONFIGURATION live here — layering, size budgets, build
// tags, the TLS pairing rule, nolint directives, the docs layout, and the reconciliation of the gate
// lists themselves. Gates that assert DETERMINISM or SSOT properties live in test/determinism.
//
// `make gates` enumerates five locations, not two: these two, plus cmd/tether (the wire error-code
// reconciliations), internal/auth (the ACL <-> subscription reconciliation) and test/concurrency (the
// NumGoroutine + fd leak baseline). The count is not a detail to keep in prose — it drifted once already,
// which is why TestGatesTargetCoversEveryGateCLAUDEMdNames now reconciles that list against CLAUDE.md's
// gate table rather than leaving either one to be believed (origin: line-2 review m24 and m26).
package architecture

import (
	"go/build/constraint"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// build_tags_test.go — the Makefile's ALL_TEST_TAGS must equal the set of custom build tags in the tree.
//
// WHY THIS GATE EXISTS AT ALL
// ---------------------------
// `make vet-tags` is the only thing that compiles the 24 test files hidden behind build tags (measured:
// `grep -rln '^//go:build' --include='*_test.go' .` — 8 in test/d9, 7 in test/d5, 3 in test/d8, 2 in
// test/d6, and one each in test/d7, test/e2e, test/concurrency and internal/broker); a bare
// `go test ./...` builds none of them. That makes the tag LIST load-bearing, and a hand-typed list of
// tags is uniquely prone to silent rot: Go does not error on an unknown build tag, it simply builds
// nothing for it. A typo does not fail — it disables.
//
// This is not a hypothetical failure mode. It had already happened, to the command this gate replaces:
// the main process self-checked for several review rounds with
//
//	go vet -tags phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix ./...
//
// where six of those eight names (`phasefluidity`, `c7`, `d5`, `d6`, `d7`, `d8`) match nothing in the
// tree — the real tags all carry an `_integration` suffix. Only `d9_integration` and `e2e_matrix` ever
// selected anything.
//
// Counting it exactly, because "six of eight" is a count of NAMES and the interesting number is a count
// of SUITES (origin: line-2 external review GI-12, which caught this sentence saying "eight hidden
// suites" while ALL_TEST_TAGS lists seven tags):
//
//	5  real suites left uncompiled by a command whose entire purpose was to compile them —
//	   d5_integration, d6_integration, d7_integration, d8_integration, phasefluidity_integration.
//	1  name, `c7`, for which no tag has ever existed in the tree under any spelling. A list naming a
//	   suite that does not exist is its own finding: nobody had read the list against the tree.
//	2  names that worked.
//
// Nothing said so, for rounds. A control that reports success without doing the work is worse than no
// control, because it also spends the attention that would have found the gap.
//
// WHY BOTH DIRECTIONS
// -------------------
// Missing-from-Makefile is the rot above: a new tag is added to the tree and its suite silently stops
// being built. Missing-from-tree is the mirror: a tag is removed or renamed and the Makefile keeps
// naming a tag that selects nothing, which re-creates the exact "list that looks complete but compiles
// less than it claims" state. Only asserting both keeps the list honest, so both are asserted.

var allTestTagsRe = regexp.MustCompile(`(?m)^ALL_TEST_TAGS\s*:?=\s*(.+)$`)

// predeclaredBuildTags are the constraint identifiers Go itself defines. They appear in //go:build
// lines but are NOT project tags, so they must never be expected in ALL_TEST_TAGS.
//
// GOOS/GOARCH are enumerated rather than probed because this gate must give the same verdict on every
// machine: deriving them from runtime.GOOS would make a `//go:build darwin` file a violation on Linux.
var predeclaredBuildTags = map[string]bool{
	"cgo": true, "race": true, "msan": true, "asan": true, "gc": true, "gccgo": true,
	"unix": true, "boringcrypto": true, "purego": true, "ignore": true,
	// GOOS
	"aix": true, "android": true, "darwin": true, "dragonfly": true, "freebsd": true,
	"hurd": true, "illumos": true, "ios": true, "js": true, "linux": true, "nacl": true,
	"netbsd": true, "openbsd": true, "plan9": true, "solaris": true, "wasip1": true,
	"windows": true, "zos": true,
	// GOARCH
	"386": true, "amd64": true, "amd64p32": true, "arm": true, "arm64": true, "arm64be": true,
	"armbe": true, "loong64": true, "mips": true, "mips64": true, "mips64le": true,
	"mips64p32": true, "mips64p32le": true, "mipsle": true, "ppc": true, "ppc64": true,
	"ppc64le": true, "riscv": true, "riscv64": true, "s390": true, "s390x": true,
	"sparc": true, "sparc64": true, "wasm": true,
}

// isPredeclaredBuildTag also folds in the `go1.N` release tags, which are open-ended and so cannot be
// enumerated.
func isPredeclaredBuildTag(tag string) bool {
	if predeclaredBuildTags[tag] {
		return true
	}
	return strings.HasPrefix(tag, "go1.")
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod found walking up from the test's working directory")
		}
		dir = parent
	}
}

// makefileTestTags parses ALL_TEST_TAGS out of the Makefile.
func makefileTestTags(t *testing.T, root string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	m := allTestTagsRe.FindSubmatch(b)
	if m == nil {
		t.Fatal("Makefile has no ALL_TEST_TAGS assignment — `make vet-tags` cannot be compiling anything")
	}
	out := map[string]bool{}
	for _, tag := range strings.Split(strings.TrimSpace(string(m[1])), ",") {
		if tag = strings.TrimSpace(tag); tag != "" {
			out[tag] = true
		}
	}
	return out
}

// treeBuildTags returns every custom build tag in the tree, mapped to the files that use it.
func treeBuildTags(t *testing.T, root string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		// Only the //go:build prologue matters, and it must precede the package clause.
		for _, line := range strings.Split(string(src), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "package ") {
				break
			}
			if !constraint.IsGoBuild(line) {
				continue
			}
			expr, perr := constraint.Parse(line)
			if perr != nil {
				t.Errorf("%s: unparseable build constraint %q: %v", rel, line, perr)
				continue
			}
			// WALK the expression rather than Eval-ing it with an all-false oracle.
			//
			// origin: line-2 closure verification §6 B6 — WHOSE STATED MECHANISM IS FALSE, and the
			// correction is worth more than the change. B6 said Go's `&&` short-circuits during Eval, so
			// the right-hand tag of `//go:build linux && d5_integration` would never be offered to the
			// oracle and would vanish from the tree set. Measured (see
			// TestConstraintTagWalkSeesBothSidesOfAnd's last block): constraint.AndExpr.Eval asks for BOTH
			// operands. The Eval-based version was not blind to anything.
			//
			// The walk is kept anyway, for a reason that does hold: it enumerates the expression
			// STRUCTURALLY, so it cannot depend on an evaluation strategy that is an implementation detail
			// of go/build/constraint. Recording tags as a side effect of asking a boolean question works
			// only as long as the evaluator keeps asking every question, which nothing in the API promises.
			//
			// The previous comment called Eval "the documented way to enumerate an expression's tags". That
			// was the actual defect here: it was not documented anywhere, and stating a method is sanctioned
			// is how the next person stops checking.
			for _, tag := range collectConstraintTags(expr) {
				if !isPredeclaredBuildTag(tag) {
					out[tag] = append(out[tag], rel)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Eval short-circuits on `||`, so a tag can be recorded twice across the two prologue forms.
	for tag, files := range out {
		sort.Strings(files)
		out[tag] = slicesCompact(files)
	}
	return out
}

func slicesCompact(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, s := range in[1:] {
		if s != out[len(out)-1] {
			out = append(out, s)
		}
	}
	return out
}

func TestBuildTagsAreReconciled(t *testing.T) {
	root := repoRoot(t)
	inMakefile := makefileTestTags(t, root)
	inTree := treeBuildTags(t, root)

	var missingFromMakefile []string
	for tag, files := range inTree {
		if !inMakefile[tag] {
			missingFromMakefile = append(missingFromMakefile,
				tag+" ("+strconv.Itoa(len(files))+" file(s), e.g. "+files[0]+")")
		}
	}
	sort.Strings(missingFromMakefile)
	if len(missingFromMakefile) > 0 {
		t.Errorf("%d build tag(s) exist in the tree but are NOT in the Makefile's ALL_TEST_TAGS:\n  %s\n\n"+
			"`make vet-tags` does not compile the files behind them, so those suites can rot without any\n"+
			"gate noticing. Add them to ALL_TEST_TAGS in the Makefile.",
			len(missingFromMakefile), strings.Join(missingFromMakefile, "\n  "))
	}

	var missingFromTree []string
	for tag := range inMakefile {
		if _, ok := inTree[tag]; !ok {
			missingFromTree = append(missingFromTree, tag)
		}
	}
	sort.Strings(missingFromTree)
	if len(missingFromTree) > 0 {
		t.Errorf("%d tag(s) in the Makefile's ALL_TEST_TAGS select NOTHING in the tree:\n  %s\n\n"+
			"Go does not error on an unknown build tag, it silently builds nothing for it — which is how\n"+
			"a tag list ends up compiling less than it claims (see this file's header for the six-name\n"+
			"instance this gate was written to stop). Remove them, or fix the spelling.",
			len(missingFromTree), strings.Join(missingFromTree, "\n  "))
	}
}

// TestBuildTagsReconcilerIsNonVacuous proves the two directions above can actually fail. Without it a
// green TestBuildTagsAreReconciled could mean either "the lists agree" or "the parser found nothing" —
// and the parser finding nothing is exactly the shape of the bug this file exists to prevent.
//
// This file was invisible to gate_standards_test.go until 2026-09-01: CLAUDE.md's row named
// `make vet-tags` and no `_test.go` path, so the meta-gate never asked it for an anchor (internal
// review, gates verifier). The row now names this file, and this is its control.
//
// gate-control: TestBuildTagsReconcilerIsNonVacuous
// integrationTagExceptions: files gated by an `_integration` tag that live OUTSIDE the tag's own
// test/<dir>. Site-keyed, with a reason; an entry naming a file that no longer exists reddens.
var integrationTagExceptions = map[string]string{
	"internal/broker/phasefluidity_lifecycle_test.go": "phasefluidity_integration: the failed-join lifecycle " +
		"drill needs package broker internals and has no test/<dir> of its own; it is run by " +
		"TestPhaseFluidityMatrix with its own -run filter.",
}

// integrationTagExceptionsCap pins the exception table both ways (internal review L1-F8): the exact
// file count below constrains it only indirectly, and a second exception could ride in under an
// unchanged count if a gated file moved at the same time.
const integrationTagExceptionsCap = 1

// integrationTaggedFileCount pins the number of `_integration`-gated files. Exact, TLS-pairing style:
// adding a gated file anywhere reddens this even when it is in the right directory, so the author
// reads this test and confirms the placement rule before bumping the number.
const integrationTaggedFileCount = 23

// TestIntegrationTagsAreLocalToTheirSuiteDir: every file behind an `<x>_integration` build tag lives in
// ONE test/<dir> per tag (or in the exception ledger). This is the precondition the matrix
// de-duplication relies on: `go list` proved the shared internal packages compile identically under
// every tag TODAY (docs/reviews/test-system-overhaul-plan.md §-1 F4), and this is what keeps that
// true — a `//go:build d5_integration` file dropped into internal/proc would make D5's broker binary
// differ from D4's without any other gate noticing.
// origin: docs/reviews/test-system-overhaul-plan.md B4 (architecture A2).
func TestIntegrationTagsAreLocalToTheirSuiteDir(t *testing.T) {
	root := repoRoot(t)
	if n := len(integrationTagExceptions); n != integrationTagExceptionsCap {
		t.Errorf("integrationTagExceptions has %d entries, cap says %d — move the cap in the same change, with the reason", n, integrationTagExceptionsCap)
	}
	inTree := treeBuildTags(t, root)
	seenException := map[string]bool{}
	total := 0
	var bad []string
	for tag, files := range inTree {
		if !strings.HasSuffix(tag, "_integration") {
			continue
		}
		homes := map[string]bool{}
		for _, f := range files {
			total++
			f = filepath.ToSlash(f)
			if _, ok := integrationTagExceptions[f]; ok {
				seenException[f] = true
				continue
			}
			parts := strings.Split(f, "/")
			if len(parts) < 3 || parts[0] != "test" {
				bad = append(bad, tag+": "+f+" (not under test/<dir>/)")
				continue
			}
			homes[parts[1]] = true
		}
		if len(homes) > 1 {
			var hs []string
			for h := range homes {
				hs = append(hs, h)
			}
			sort.Strings(hs)
			bad = append(bad, tag+": gated files span "+strings.Join(hs, ", ")+" — one tag, one suite directory")
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Errorf("%d build-tag locality violation(s):\n  %s", len(bad), strings.Join(bad, "\n  "))
	}
	for f := range integrationTagExceptions {
		if !seenException[f] {
			t.Errorf("integrationTagExceptions names %s, which is no longer an _integration-gated file — delete the entry", f)
		}
	}
	if total != integrationTaggedFileCount {
		t.Errorf("%d files are gated by an _integration tag; this test pins %d. If you added one, confirm it "+
			"lives in its tag's own test/<dir>/ and update the constant in the same commit.", total, integrationTaggedFileCount)
	}
}

func TestBuildTagsReconcilerIsNonVacuous(t *testing.T) {
	root := repoRoot(t)

	inTree := treeBuildTags(t, root)
	if len(inTree) == 0 {
		t.Fatal("the tree scanner found ZERO custom build tags — it is broken; the repo has 7 " +
			"(the number is ALL_TEST_TAGS' length, asserted for set equality by TestBuildTagsAreReconciled " +
			"above, so a wrong figure here is a figure the sibling test already contradicts)")
	}
	if files := inTree["d5_integration"]; len(files) == 0 {
		t.Errorf("scanner did not find d5_integration, which is on 7 files including test/d5/smoke_test.go")
	}
	// The predeclared filter must be doing something: `linux` appears on real files in this repo and
	// must never be reported as a project tag.
	if _, leaked := inTree["linux"]; leaked {
		t.Error("scanner reported the predeclared GOOS tag `linux` as a project tag")
	}

	inMakefile := makefileTestTags(t, root)
	if len(inMakefile) == 0 {
		t.Fatal("the Makefile parser found ZERO tags — it is broken")
	}
}

// execTagLiteralRe finds a tag name passed as an argument literal, i.e. `"-tags", "d5_integration"`.
var execTagLiteralRe = regexp.MustCompile(`"-tags",\s*"([a-z0-9_,]+)"`)

// TestExecCommandTagLiteralsAreReconciled is the THIRD build-tag SSOT, reconciled at last.
//
// origin: line-2 closure verification §6 B5, which the review's own collective-blind-spot section raised
// and which no review lane had opened. TestBuildTagsAreReconciled above compares the Makefile's
// ALL_TEST_TAGS against `//go:build` lines in the tree — two sources, both covered. There is a third: the
// e2e matrix SPAWNS `go test -tags <literal>` subprocesses, and those literals were compared against
// nothing.
//
// The measured consequence is the worst shape available. Change `d5_integration` to `d5_integratoin` in
// test/e2e/all_phases_test.go and:
//
//	go test -count=1 -tags d5_integratoin ./test/d5/   ->  ok 0.008s   (zero tests, exit 0)
//	go test ./test/architecture/                       ->  green
//	go test ./test/determinism/                        ->  green
//	make vet-tags                                      ->  rc=0        (it reads ALL_TEST_TAGS, not these)
//
// So the entire d5 clustered-JetStream suite silently stops compiling and running, and `make e2e-parallel`
// reports that shard as PASSED. Go does not error on an unknown build tag — it selects nothing — and a
// subprocess that ran zero tests exits 0. Every gate in the tree agreed the matrix was green.
//
// This is the same defect class as the six invented tag names in the original `vet-tags` command (see this
// file's header), one layer out: a hand-typed tag list that nothing compares to the tree.
func TestExecCommandTagLiteralsAreReconciled(t *testing.T) {
	root := repoRoot(t)
	real := treeBuildTags(t, root)

	// Scan the files that spawn tagged subprocesses. An explicit list, because a tree-wide scan would
	// also match this very test's regex literal and the two `-tags` flags the parallel runner passes
	// through as VARIABLES (shard.go, split.go, main.go:395 — those carry a variable, not a literal, so
	// there is nothing to reconcile; main.go:413's `e2e_matrix` literal IS checked).
	files := []string{
		"test/e2e/all_phases_test.go",
		"test/e2e/parallel/main.go",
	}

	found := map[string][]string{} // tag -> sites
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, m := range execTagLiteralRe.FindAllStringSubmatch(string(b), -1) {
			for _, tag := range strings.Split(m[1], ",") {
				if tag = strings.TrimSpace(tag); tag != "" {
					found[tag] = append(found[tag], rel)
				}
			}
		}
	}

	// AN EXACT REQUIRED SET, NOT A COUNT FLOOR. origin: line-2 INDEPENDENT EXTERNAL REVIEW M3.
	//
	// The floor was `len(found) < 6` against 7 tags actually spawned. Deleting the whole of TestD9Matrix
	// leaves 6 — still above the floor — so an entire release suite could be removed with this gate green,
	// and the e2e coverage self-check derives its subtest names from the same file, so it would not know
	// D9 had ever existed either. A quantity floor cannot say WHICH suite went missing, and that is the
	// only question worth asking here.
	//
	// This ledger is the answer to "which tagged suites must the matrix still spawn". Removing a suite is
	// then a deliberate two-line edit — delete the subtest AND delete it here — instead of a deletion that
	// nothing notices.
	requiredSpawnedTags := map[string]string{
		"d5_integration":            "clustered JetStream matrix",
		"d6_integration":            "membership / roster matrix",
		"d7_integration":            "force-single + recovery matrix",
		"d8_integration":            "alert / audit / transfer matrix",
		"d9_integration":            "cutover matrix",
		"phasefluidity_integration": "phase-fluidity suite",
		"e2e_matrix":                "the top-level matrix the parallel runner drives",
	}
	var missing []string
	for tag, what := range requiredSpawnedTags {
		if len(found[tag]) == 0 {
			missing = append(missing, tag+"  ("+what+")")
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d required suite(s) are no longer spawned by any `-tags` literal:\n  %s\n\n"+
			"Nothing else in the tree notices this: `go test -tags <absent>` selects zero tests and exits 0, "+
			"`make vet-tags` reads ALL_TEST_TAGS rather than these literals, and the e2e coverage self-check "+
			"derives its expectations from the same file that stopped spawning the suite. If the suite was "+
			"deliberately retired, delete its entry here in the same commit — that edit is the record.",
			len(missing), strings.Join(missing, "\n  "))
	}
	// And the reverse: a literal that is spawned but not required is either a new suite (register it) or a
	// typo that the tree-existence check below will also catch.
	for tag := range found {
		if _, ok := requiredSpawnedTags[tag]; !ok {
			t.Errorf("`-tags %s` is spawned but is not in requiredSpawnedTags. If it is a new suite, add it "+
				"with a one-line description so its future deletion is noticed; if it is a typo, the "+
				"tree-existence check below says so too.", tag)
		}
	}

	var unknown []string
	for tag, sites := range found {
		if _, ok := real[tag]; !ok {
			sort.Strings(sites)
			unknown = append(unknown, tag+"  (spawned from "+strings.Join(slicesCompact(sites), ", ")+")")
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		t.Errorf("%d `-tags` literal(s) name a build tag that appears in NO //go:build line in the tree:\n"+
			"  %s\n\n"+
			"Go does not error on an unknown build tag — it selects nothing, the subprocess runs zero tests, "+
			"and it exits 0. The suite behind that tag silently stops running while the matrix reports the "+
			"shard as passed. Fix the typo, or, if the suite really was removed, delete the subtest that "+
			"spawns it.", len(unknown), strings.Join(unknown, "\n  "))
	}
}

// collectConstraintTags returns every tag identifier in a parsed //go:build expression, including the ones
// an evaluation would short-circuit past. See the note at its call site (review §6 B6).
func collectConstraintTags(e constraint.Expr) []string {
	switch x := e.(type) {
	case *constraint.TagExpr:
		return []string{x.Tag}
	case *constraint.NotExpr:
		return collectConstraintTags(x.X)
	case *constraint.AndExpr:
		return append(collectConstraintTags(x.X), collectConstraintTags(x.Y)...)
	case *constraint.OrExpr:
		return append(collectConstraintTags(x.X), collectConstraintTags(x.Y)...)
	default:
		return nil
	}
}

// TestConstraintTagWalkSeesBothSidesOfAnd is the synthetic self-check for the walk above.
//
// The tree has no `&&` prologue today, so nothing real exercises the case this walk exists for — which is
// exactly why the Eval-based version's blindness went unnoticed. These are constructed constraints, and the
// first one is the shape that used to lose its right-hand tag.
func TestConstraintTagWalkSeesBothSidesOfAnd(t *testing.T) {
	tests := []struct {
		line string
		want []string
	}{
		{"//go:build linux && d5_integration", []string{"d5_integration", "linux"}},
		{"//go:build d5_integration && d6_integration && d7_integration",
			[]string{"d5_integration", "d6_integration", "d7_integration"}},
		{"//go:build !windows && e2e_matrix", []string{"e2e_matrix", "windows"}},
		{"//go:build d8_integration || d9_integration", []string{"d8_integration", "d9_integration"}},
		{"//go:build d5_integration", []string{"d5_integration"}},
	}
	for _, tc := range tests {
		expr, err := constraint.Parse(tc.line)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.line, err)
		}
		got := collectConstraintTags(expr)
		sort.Strings(got)
		got = slicesCompact(got)
		want := append([]string(nil), tc.want...)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s\n  got  %v\n  want %v\n\n"+
				"A tag missing from the right of `&&` is how the Eval-based version failed: the tag would "+
				"disappear from the tree set and this gate would then blame the Makefile for naming a tag "+
				"that selects nothing.", tc.line, got, want)
		}
	}

	// THE MEASUREMENT THAT REFUTED THE FINDING. Review §6 B6 claimed Eval short-circuits `&&` and therefore
	// never offers the right-hand tag to the oracle. It does offer it. This block pins that fact, because
	// the wrong version of it was about to be written into a comment as the justification for this walk —
	// and a gate justified by a false premise is the exact shape this whole increment is about.
	//
	// If a future Go release DOES start short-circuiting here, this assertion fails, and the failure is the
	// signal that the walk has become load-bearing for the reason B6 imagined rather than the reason it is
	// actually kept for (structural enumeration not depending on evaluation strategy).
	expr, err := constraint.Parse("//go:build linux && d5_integration")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var asked []string
	expr.Eval(func(tag string) bool {
		asked = append(asked, tag)
		return false
	})
	sort.Strings(asked)
	if strings.Join(asked, ",") != "d5_integration,linux" {
		t.Errorf("Eval with an all-false oracle asked for %v, expected both operands.\n\n"+
			"Measured behaviour has changed. Review §6 B6 predicted exactly this (short-circuit past the "+
			"right operand) and was wrong at the time; if it is right now, say so at the call site instead "+
			"of leaving a comment that argues from the other fact.", asked)
	}
}
