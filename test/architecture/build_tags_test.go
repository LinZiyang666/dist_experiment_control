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
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// build_tags_test.go — the Makefile's ALL_TEST_TAGS must equal the set of custom build tags in the tree.
//
// WHY THIS GATE EXISTS AT ALL
// ---------------------------
// `make vet-tags` compiles the test files hidden behind custom build tags. At gate inception:
// `grep -rln '^//go:build' --include='*_test.go' .` — 8 in test/d9, 7 in test/d5, 3 in test/d8, 2 in
// test/d6, and one each in test/d7, test/e2e, test/concurrency and internal/broker). S5 adds
// seven heavy hotupdate files under its suite-local tag; a bare `go test ./...` builds none of them. That makes the tag LIST load-bearing, and a hand-typed list of
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
// 23 → 24 (hot-update S4 close-out): test/d7/leader_premise_test.go, the D7 leadership-premise
// helpers (WithLeader-style segment retry) — d7_integration, inside test/d7/, placement confirmed.
// 24 → 31: seven hotupdate_integration files own the heavy S5 real-binary,
// carrier and watchdog tests, all local to test/hotupdate/. Reference probes stay untagged.
// origin: hot-update S5 plan §8 / §10.2.
// 31 → 32: the repeated three-topology broker cohort stays in its exact race
// matrix partitions without exhausting the default package's ten-minute budget.
// origin: hot-update S6 plan §9.3 / T48 / TESTFIX
// 32 → 33: broker Run/current-rollback consumers join test/hotupdate's local tag.
// 33 → 37: S7 P29 test/hotupdate/transfer_fixture_test.go (frozen T42 real-binary fixture) and
// S8 P25 test/hotupdate/transaction_{crash,faults,mutants}_test.go (supervised crash/fault/mutant
// receipts) — all hotupdate_integration, all inside test/hotupdate/, placement confirmed.
// origin: hot-update S7 plan §11 / T42 / P29; S8 plan §3.2 / §8.3 / P25
// origin: hot-update S8 plan §11.2 P23 (three files); S9 plan §12 P28 (two files).
// origin: hot-update S7 plan §11.4; ledger pass 4 verified 85 live files, including nine S9 paths.
// origin: hot-update S7 plan §11 / cutcap; transfer_transport_test.go stays in test/hotupdate.
// origin: S10 internal review ARCH-REDS-2 F1 / TEST-BUDGET
// 92 → 94: broker_lifecycle_seam_test.go and broker_rollback_test.go use
// hotupdate_integration inside test/hotupdate/, their tag's own suite directory.
// 94 → 95 including S9: broker_cohort_oracle_test.go uses p13_integration inside test/p13/.
// origin: hot-update S6 plan §6.3 / T48; S10 plan §11.1 / P29
// origin: hot-update S7 plan §11.4 / T52; ledger15 counts ten new tagged source files.
// 95 -> 105 total: two hotupdate helpers and eight S9 source memberships; locality stays independently checked.
// origin: hot-update S10 plan §12.2 / TIDY-1; S8 census and S9 NATS placement handoffs.
// 105 -> 109 total: rollback_transaction plus three NATS files, all in their suite homes.
// origin: hot-update S9 plan §12 / P35; runner source inputs and canonical receipt controls stay in their owning suites.
// 109 -> 111 total: infraprivilege/release_inputs_test.go and infrabusiness/producer_test.go.
const integrationTaggedFileCount = 102

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
	// origin: hot-update S9 plan §9.5 / T83 — keep the original 42-file
	// assertion and add nine exact S9 paths, independently reconciled below.
	if total != integrationTaggedFileCount+9 {
		t.Errorf("%d files are gated by an _integration tag; this test pins %d (including nine additional S9 paths). If you added one, confirm it "+
			"lives in its tag's own test/<dir>/ and update the constant in the same commit.", total, integrationTaggedFileCount+9)
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
		"d5_integration":        "clustered JetStream matrix",
		"d6_integration":        "membership / roster matrix",
		"d7_integration":        "force-single + recovery matrix",
		"d8_integration":        "alert / audit / transfer matrix",
		"d9_integration":        "cutover matrix",
		"hotupdate_integration": "S5 repeated real-binary flow/SS and watchdog suite",
		// origin: hot-update S9 plan §12 P28/P36 — adjacent-version NATS acceptance.
		"infranats_integration": "S9 supplied stock NATS cycles, quorum and process-lock suite",
		// origin: hot-update S9 plan §9.5 / T83 — all four independent heavy suites.
		// origin: S9 internal review rv-business F7
		"infracaddy_integration":     "S9 supplied stock Caddy runtime and ACME suite",
		"infrabusiness_integration":  "S9 original mixed-business continuity suite",
		"infraprivilege_integration": "S9 installer, authority and support runtime suite",
		"p13_integration":            "S6 repeated real-binary broker flow cohort",
		"phasefluidity_integration":  "phase-fluidity suite",
		"e2e_matrix":                 "the top-level matrix the parallel runner drives",
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

// Common image contracts must compile without platform selection. Syscalls
// live in explicit Linux or Linux/Darwin POSIX leaf pairs; the complementary
// other twin supplies an unsupported boundary rather than a missing symbol.
// origin: hot-update S6 plan §9.3 / T53
// gate-control: TestBrokerBuildTagControls
func checkBrokerPlatformSources(sources map[string]string) error {
	for _, path := range brokerSortedSourcePaths(sources) {
		source := sources[path]
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		pkg := filepath.ToSlash(filepath.Dir(path))
		ownedLeaf := pkg == "internal/brokerimage" || pkg == "internal/listenerimage" || pkg == "internal/controlbudget" || pkg == "internal/clusterio" || pkg == "internal/brokermaint"
		// The S7 common owners are portable; only syscall twins may be selected
		// by OS. Contract witnesses are portable declarations as well.
		// origin: hot-update S7 plan §11.1 / T51
		ownedLeaf = ownedLeaf || strings.HasPrefix(pkg, "internal/xfer")
		ownedLeaf = ownedLeaf || strings.HasPrefix(path, "internal/transferpath/transfer_atomic") || path == "internal/transferpath/transfer_contracts.go"
		imageSeam := ownedLeaf || strings.HasPrefix(path, "internal/tunnel/server_image_") || strings.HasPrefix(path, "internal/adminsock/image_") || strings.HasPrefix(path, "internal/httplisten/image_")
		if !imageSeam {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		linux := strings.HasSuffix(path, "_linux.go")
		posix := strings.HasSuffix(path, "_unix.go")
		other := strings.HasSuffix(path, "_other.go")
		constraints := []constraint.Expr{}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if !constraint.IsGoBuild(comment.Text) && !constraint.IsPlusBuild(comment.Text) {
					continue
				}
				expr, err := constraint.Parse(comment.Text)
				if err != nil {
					return err
				}
				constraints = append(constraints, expr)
			}
		}
		if !linux && !posix && !other && len(constraints) > 0 {
			return fmt.Errorf("%s: common image code has build tag", path)
		}
		if linux || posix || other {
			stem := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(path, "_linux.go"), "_unix.go"), "_other.go")
			twin := stem + "_other.go"
			unixPair := posix
			if other {
				twin = stem + "_linux.go"
				if _, ok := sources[stem+"_unix.go"]; ok {
					twin, unixPair = stem+"_unix.go", true
				}
			}
			if _, ok := sources[twin]; !ok {
				return fmt.Errorf("%s: missing platform twin %s", path, twin)
			}
			if (other || posix) && len(constraints) == 0 {
				return fmt.Errorf("%s: leaf twin needs explicit platform constraint", path)
			}
			for _, expr := range constraints {
				for _, tag := range collectConstraintTags(expr) {
					if tag != "linux" && (!unixPair || tag != "darwin") {
						return fmt.Errorf("%s: unexpected platform tag %s", path, tag)
					}
				}
				for _, platform := range []string{"linux", "darwin", "windows"} {
					selected := expr.Eval(func(tag string) bool { return tag == platform })
					want := linux && platform == "linux" || posix && platform != "windows" || other && platform != "linux" && (!unixPair || platform != "darwin")
					if selected != want {
						return fmt.Errorf("%s: platform constraint does not match responsibility", path)
					}
				}
			}
			// A portable POSIX implementation must refuse on the complementary
			// platforms. An empty twin would otherwise certify missing behavior.
			// origin: hot-update S7 plan §11.1 / T51 / S7-GATES lead decision 4
			if other && unixPair {
				refusal := false
				ast.Inspect(file, func(node ast.Node) bool {
					ret, ok := node.(*ast.ReturnStmt)
					if !ok {
						return true
					}
					for _, result := range ret.Results {
						if sel, ok := result.(*ast.SelectorExpr); ok && sel.Sel.Name == "ErrUnsupported" {
							if alias, ok := sel.X.(*ast.Ident); ok && brokerImports(file)[alias.Name] == modPrefix+"internal/handoff" {
								refusal = true
							}
						}
					}
					return true
				})
				if !refusal {
					return fmt.Errorf("%s: POSIX other twin lacks unsupported refusal", path)
				}
			}
		}

		imports := brokerImports(file)
		for _, imp := range imports {
			if imp == "golang.org/x/sys/unix" && !linux && !posix {
				return fmt.Errorf("%s: image unix syscall import outside platform leaf", path)
			}
		}
		var syscall bool
		ast.Inspect(file, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			name, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if imports[name.Name] == "syscall" {
				syscall = true
			}
			return true
		})
		if syscall && !linux && !posix {
			return fmt.Errorf("%s: image syscall outside platform leaf", path)
		}
	}
	return nil
}

// origin: hot-update S7 plan §11.1 / T51
// gate-control: TestTransferBuildTagControls
func TestTransferBuildTagControls(t *testing.T) {
	good := map[string]string{
		"internal/xferimage/codec.go":                    "package image;func Encode(){}",
		"internal/transferpath/transfer_atomic_linux.go": "//go:build linux\npackage path;import \"syscall\";var _=syscall.O_RDWR",
		"internal/transferpath/transfer_atomic_other.go": "//go:build !linux\npackage path",
	}
	root := transferFixtureTree(t, good)
	if err := checkBrokerPlatformSources(brokerGateSources(t, root)); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"//go:build linux\npackage image;func Encode(){}",
		"//go:build hotupdate_integration\npackage image;func Encode(){}",
		"package image;import alias \"syscall\";var _=alias.O_RDWR",
	} {
		transferWriteFixture(t, root, "internal/xferimage/codec.go", source)
		if checkBrokerPlatformSources(brokerGateSources(t, root)) == nil {
			t.Fatal("on-disk common transfer code tag/syscall mutation escaped")
		}
	}
	posix := map[string]string{
		"internal/transferpath/transfer_atomic_unix.go":  "//go:build linux || darwin\npackage path;import u \"golang.org/x/sys/unix\";var _=u.Renameat",
		"internal/transferpath/transfer_atomic_other.go": "//go:build !linux && !darwin\npackage path;import h \"github.com/LinZiyang666/tether/internal/handoff\";func New()error{return h.ErrUnsupported}",
	}
	posixRoot := transferFixtureTree(t, posix)
	if err := checkBrokerPlatformSources(brokerGateSources(t, posixRoot)); err != nil {
		t.Fatal(err)
	}
	// Linux's import validation and the portable POSIX receiver share one
	// complementary refusal leaf. A split must use that same responsibility
	// stem rather than buying a sixth file or hiding a missing twin.
	// origin: S7 internal review GATE-CONVERGE-layering F4
	transferWriteFixture(t, posixRoot, "internal/transferpath/transfer_atomic_linux.go", "//go:build linux\npackage path;import u \"golang.org/x/sys/unix\";var _=u.F_GETFL")
	if err := checkBrokerPlatformSources(brokerGateSources(t, posixRoot)); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ path, source string }{
		{"internal/transferpath/transfer_atomic.go", "package path;import u \"golang.org/x/sys/unix\";var _=u.Renameat"},
		{"internal/transferpath/transfer_atomic_unix.go", "//go:build linux || darwin || hidden\npackage path"},
		{"internal/transferpath/transfer_atomic_unix.go", "//go:build linux\npackage path"},
		{"internal/transferpath/transfer_atomic_other.go", "//go:build !linux\npackage path"},
		{"internal/transferpath/transfer_atomic_other.go", "//go:build !linux && !darwin\npackage path;func New()error{return nil}"},
	} {
		transferWriteFixture(t, posixRoot, mutation.path, mutation.source)
		if checkBrokerPlatformSources(brokerGateSources(t, posixRoot)) == nil {
			t.Fatalf("on-disk POSIX mutation escaped: %s", mutation.path)
		}
		if original, ok := posix[mutation.path]; ok {
			transferWriteFixture(t, posixRoot, mutation.path, original)
		} else if err := os.Remove(filepath.Join(posixRoot, mutation.path)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(posixRoot, "internal/transferpath/transfer_atomic_other.go")); err != nil {
		t.Fatal(err)
	}
	if checkBrokerPlatformSources(brokerGateSources(t, posixRoot)) == nil {
		t.Fatal("missing POSIX refusal twin escaped")
	}

}

// origin: hot-update S6 plan §9.3 / T53
func TestBrokerPlatformSyscallsStayInLeaves(t *testing.T) {
	if err := checkBrokerPlatformSources(brokerGateSources(t, repoRoot(t))); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §11 / T53
func TestBrokerBuildTagControls(t *testing.T) {
	good := map[string]string{
		"internal/listenerimage/listener_linux.go": "//go:build linux\npackage listenerimage;import u \"golang.org/x/sys/unix\";var _=u.FcntlInt",
		"internal/listenerimage/listener_other.go": "//go:build !linux\npackage listenerimage",
		"internal/listenerimage/contracts.go":      "package listenerimage",
	}
	if err := checkBrokerPlatformSources(good); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, source string }{
		{"internal/listenerimage/contracts.go", "//go:build linux\npackage listenerimage"},
		{"internal/listenerimage/contracts.go", "package listenerimage;import u \"golang.org/x/sys/unix\";var _=u.FcntlInt"},
		{"internal/listenerimage/contracts.go", "package listenerimage;import s \"syscall\";var _=s.Flock"},
		{"internal/listenerimage/listener_other.go", "//go:build linux\npackage listenerimage"},
		{"internal/listenerimage/listener_other.go", "package listenerimage"},
		{"internal/listenerimage/listener_linux.go", "//go:build linux && hidden_image\npackage listenerimage"},
	} {
		broken := map[string]string{}
		for path, source := range good {
			broken[path] = source
		}
		broken[tc.path] = tc.source
		if checkBrokerPlatformSources(broken) == nil {
			t.Fatalf("platform defect escaped %s: %s", tc.path, tc.source)
		}
	}
	for _, path := range []string{"internal/listenerimage/listener_linux.go", "internal/listenerimage/listener_other.go"} {
		broken := map[string]string{}
		for p, source := range good {
			if p != path {
				broken[p] = source
			}
		}
		if checkBrokerPlatformSources(broken) == nil {
			t.Fatalf("missing twin escaped %s", path)
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/listenerimage/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, dir := range []string{"internal/listenerimage", "cmd"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "internal/listenerimage/contracts.go"), append([]byte("//go:build linux\n\n"), raw...), 0600); err != nil {
		t.Fatal(err)
	}
	if checkBrokerPlatformSources(brokerGateSources(t, root)) == nil {
		t.Fatal("real-family common build tag escaped")
	}
}

var infraIntegrationHomes = map[string]string{
	"infranats_integration": "test/infranats", "infracaddy_integration": "test/infracaddy",
	"infrabusiness_integration": "test/infrabusiness", "infraprivilege_integration": "test/infraprivilege",
}

// A common file cannot hide behind an integration or OS tag. Kernel operations
// need a Linux responsibility and a complementary, explicitly refusing twin.
// origin: hot-update S9 plan §10.2 / T83
func checkInfraPlatformSources(sources map[string]string) error {
	for _, path := range brokerSortedSourcePaths(sources) {
		if !infraOwner(path) || strings.Contains(path, "/contractstub/") {
			continue
		}
		file, err := brokerParse(path, sources[path])
		if err != nil {
			return err
		}
		linux, other := strings.HasSuffix(path, "_linux.go"), strings.HasSuffix(path, "_other.go")
		var expr constraint.Expr
		for _, group := range file.Comments {
			for _, c := range group.List {
				if constraint.IsGoBuild(c.Text) {
					if expr != nil {
						return fmt.Errorf("%s: duplicate platform constraint", path)
					}
					expr, err = constraint.Parse(c.Text)
					if err != nil {
						return err
					}
					for _, tag := range collectConstraintTags(expr) {
						if tag != "linux" && tag != "amd64" {
							return fmt.Errorf("%s: S9 platform tag %s hides a production responsibility", path, tag)
						}
					}
				}
			}
		}
		if !linux && !other && expr != nil {
			return fmt.Errorf("%s: tagged common S9 implementation", path)
		}
		if linux || other {
			stem := strings.TrimSuffix(strings.TrimSuffix(path, "_linux.go"), "_other.go")
			twin := stem + "_other.go"
			if other {
				twin = stem + "_linux.go"
			}
			if sources[twin] == "" {
				return fmt.Errorf("%s: missing S9 platform twin %s", path, twin)
			}
			if other && expr == nil {
				return fmt.Errorf("%s: other twin needs complementary constraint", path)
			}
			if expr != nil {
				for _, osName := range []string{"linux", "darwin", "windows"} {
					for _, arch := range []string{"amd64", "arm64"} {
						active := expr.Eval(func(tag string) bool { return tag == osName || tag == arch })
						want := osName == "linux"
						if infraContains(collectConstraintTags(expr), "amd64") {
							want = want && arch == "amd64"
						}
						if other {
							want = !want
						}
						if active != want {
							return fmt.Errorf("%s: constraint selects wrong platform %s/%s", path, osName, arch)
						}
					}
				}
			}
			if other {
				refusal := false
				ast.Inspect(file, func(n ast.Node) bool {
					if ret, ok := n.(*ast.ReturnStmt); ok {
						for _, value := range ret.Results {
							if sel, ok := value.(*ast.SelectorExpr); ok {
								if id, ok := sel.X.(*ast.Ident); ok && brokerImports(file)[id.Name] == modPrefix+"internal/infracontract" && sel.Sel.Name == "ErrUnsupported" {
									refusal = true
								}
							}
						}
					}
					return true
				})
				if !refusal {
					return fmt.Errorf("%s: other platform has no explicit unsupported refusal", path)
				}
			}
		}
		for _, imp := range brokerImports(file) {
			if (imp == "syscall" || imp == "golang.org/x/sys/unix") && !linux {
				return fmt.Errorf("%s: syscall outside Linux leaf", path)
			}
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraBuildTagControls
func TestInfraPlatformResponsibilitiesAllOS(t *testing.T) {
	if err := checkInfraPlatformSources(brokerGateSources(t, repoRoot(t))); err != nil {
		t.Error(err)
	}
	tags := treeBuildTags(t, repoRoot(t))
	if err := checkInfraIntegrationFiles(tags); err != nil {
		t.Error(err)
	}
	mk := makefileTestTags(t, repoRoot(t))
	for tag, home := range infraIntegrationHomes {
		if !mk[tag] {
			t.Errorf("NOT_RUN pending P26: missing Makefile tag %s", tag)
		}
		if len(tags[tag]) == 0 {
			t.Errorf("NOT_RUN pending integration owner: empty S9 suite %s", tag)
		}
		for _, path := range tags[tag] {
			if filepath.ToSlash(filepath.Dir(path)) != home {
				t.Errorf("%s outside exact S9 suite %s", path, home)
			}
		}
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraBuildTagControls(t *testing.T) {
	tags := map[string][]string{}
	for tag, files := range infraIntegrationFiles {
		tags[tag] = append([]string{}, files...)
	}
	if err := checkInfraIntegrationFiles(tags); err != nil {
		t.Fatal(err)
	}
	for tag, files := range infraIntegrationFiles {
		tags[tag] = append([]string{}, files[1:]...)
		if checkInfraIntegrationFiles(tags) == nil {
			t.Fatal("missing S9 tagged test escaped")
		}
		tags[tag] = append([]string{}, files...)
		tags[tag][0] = "test/other/replacement_test.go"
		if checkInfraIntegrationFiles(tags) == nil {
			t.Fatal("equal-size S9 suite replacement escaped")
		}
		tags[tag] = append([]string{}, files...)
	}
	linux := "internal/infrahost/effect_linux.go"
	other := "internal/infrahost/effect_other.go"
	good := map[string]string{
		linux: "//go:build linux\npackage host;import \"syscall\";var _=syscall.O_RDWR",
		other: "//go:build !linux\npackage host;import c \"github.com/LinZiyang666/tether/internal/infracontract\";func Effect()error{return c.ErrUnsupported}",
	}
	root := transferFixtureTree(t, good)
	read := func() map[string]string { return brokerGateSources(t, root) }
	if err := checkInfraPlatformSources(read()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, source string }{
		{linux, "//go:build linux && hidden\npackage host"},
		{other, "//go:build linux\npackage host"},
		{other, "//go:build !linux\npackage host;func Effect()error{return nil}"},
		{"internal/infrahost/effect.go", "package host;import \"syscall\";var _=syscall.O_RDWR"},
		{"internal/infratransport/dial.go", "//go:build infranats_integration\npackage transport"},
	} {
		transferWriteFixture(t, root, tc.path, tc.source)
		if checkInfraPlatformSources(read()) == nil {
			t.Fatalf("on-disk platform mutation escaped %s", tc.path)
		}
		if source, ok := good[tc.path]; ok {
			transferWriteFixture(t, root, tc.path, source)
		} else {
			if err := os.Remove(filepath.Join(root, tc.path)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.Remove(filepath.Join(root, other)); err != nil {
		t.Fatal(err)
	}
	if checkInfraPlatformSources(read()) == nil {
		t.Fatal("on-disk missing twin escaped")
	}
}

// Every exact-current suite member has its tag discovered from disk. Removing
// an oracle's tag cannot be hidden by adding another file with the same tag.
// origin: S9 internal review GATE-CONVERGE-sliminfra F3
func TestInfraIntegrationSourcesKeepExactTags(t *testing.T) {
	files := map[string]string{}
	for tag, paths := range infraIntegrationFiles {
		for _, path := range paths {
			files[path] = "//go:build " + tag + "\n\npackage integration\n"
		}
	}
	root := transferFixtureTree(t, files)
	check := func() error { return checkInfraIntegrationFiles(treeBuildTags(t, root)) }
	if err := check(); err != nil {
		t.Fatal(err)
	}
	for path, original := range files {
		transferWriteFixture(t, root, path, "package integration\n")
		if check() == nil {
			t.Fatalf("untagged suite member escaped: %s", path)
		}
		replacement := filepath.Join(filepath.Dir(path), "replacement_test.go")
		transferWriteFixture(t, root, replacement, original)
		if check() == nil {
			t.Fatalf("equal-size suite substitution escaped: %s", path)
		}
		if err := os.Remove(filepath.Join(root, replacement)); err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, path, original)
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
}

// Pin the new members themselves. A nine-file count cannot detect an equal-size
// replacement that silently hides the stock/privilege acceptance tests.
// origin: hot-update S9 plan §9.5 / §10.2 / T83
// origin: S9 internal review GATE-CONVERGE-sliminfra F3 — keep every accepted oracle source in the exact suite.
var infraIntegrationFiles = map[string][]string{
	"infranats_integration":      {"test/infranats/current_store_test.go", "test/infranats/external_test.go", "test/infranats/lock_test.go", "test/infranats/placement_quorum_test.go", "test/infranats/placement_readiness_test.go", "test/infranats/stock_supply_test.go"},
	"infracaddy_integration":     {"test/infracaddy/acme_test.go", "test/infracaddy/binary_test.go", "test/infracaddy/retirement_test.go", "test/infracaddy/tcp_window_test.go", "test/infracaddy/websocket_stream_test.go"},
	"infrabusiness_integration":  {"test/infrabusiness/auth_test.go", "test/infrabusiness/business_oracle_test.go", "test/infrabusiness/business_supply_test.go", "test/infrabusiness/continuity_test.go", "test/infrabusiness/cut_evidence_test.go", "test/infrabusiness/migration_test.go", "test/infrabusiness/oracle_fixture_test.go", "test/infrabusiness/producer_test.go", "test/infrabusiness/receipt_schema_test.go", "test/infrabusiness/receipt_scope_test.go", "test/infrabusiness/transfer_evidence_test.go", "test/infrabusiness/transfer_oracle_test.go", "test/infrabusiness/transfer_test.go"},
	"infraprivilege_integration": {"test/infraprivilege/image_supply_test.go", "test/infraprivilege/installer_supply_test.go", "test/infraprivilege/installer_test.go", "test/infraprivilege/offline_admission_test.go", "test/infraprivilege/release_inputs_test.go", "test/infraprivilege/release_supply_test.go", "test/infraprivilege/support_test.go"},
}

func checkInfraIntegrationFiles(tags map[string][]string) error {
	for tag, want := range infraIntegrationFiles {
		got := append([]string{}, tags[tag]...)
		sort.Strings(got)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			return fmt.Errorf("%s: exact S9 tagged files drift: got %v want %v", tag, got, want)
		}
	}
	return nil
}

// S10 tag locality and platform twins. hotupdate_integration is the only suite
// tag S10 may use, and only on the heavy real-binary manifest owners P21–P26 in
// test/hotupdate, exactly as linux && hotupdate_integration; supply and store
// tests (P08/P12) stay in the untagged partitions and portable helpers carry no
// suite tag. The tagged set is the actual union of manifest and disk, not a
// frozen count. A Linux responsibility in an S10 package keeps a non-Linux twin
// for every exported or portable-referenced symbol, so Darwin compiles an
// explicit refusal instead of discovering a missing symbol at release time.
// origin: hot-update S10 plan §11.1 / §11.2 / §12.2 tags / P28
// gate-control: TestFullstackBuildTagControls
var fullstackTwinPackages = []string{"internal/drillobserve", "internal/drillobserve/receipt", "internal/drillobserve/infrastructure"}

func checkFullstackBuildTags(tree fullstackTree) (int, error) {
	manifest, err := parseFullstackManifest(tree.docs[fullstackPlanPath])
	if err != nil {
		return 0, err
	}
	owned, err := fullstackOwnedRows(tree.docs[fullstackOwnershipPath])
	if err != nil {
		return 0, err
	}
	heavy := map[string]bool{}
	for path, owner := range manifest.owner {
		if n, _ := strconv.Atoi(owner[1:]); fullstackDir(path) == "test/hotupdate" && strings.HasSuffix(path, "_test.go") && n >= 21 && n <= 26 {
			heavy[path] = true
		}
	}
	scanned := fullstackNewCode(tree, manifest, owned)
	packages := map[string]bool{}
	for _, pkg := range fullstackTwinPackages {
		packages[pkg] = true
	}
	tagged := 0
	for _, path := range brokerSortedSourcePaths(scanned) {
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		// The //go:build line precedes the package clause; nothing else is needed.
		file, err := parser.ParseFile(token.NewFileSet(), path, tree.files[path], parser.PackageClauseOnly|parser.ParseComments)
		if err != nil {
			return 0, err
		}
		if isProdGoFile(path) {
			packages[fullstackDir(path)] = true
		}
		var expr constraint.Expr
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if constraint.IsGoBuild(comment.Text) {
					if expr, err = constraint.Parse(comment.Text); err != nil {
						return 0, err
					}
				}
			}
		}
		suite := false
		if expr != nil {
			for _, tag := range collectConstraintTags(expr) {
				if tag == "hotupdate_integration" {
					suite = true
				} else if !isPredeclaredBuildTag(tag) {
					return 0, fmt.Errorf("%s: S10 file uses build tag %s", path, tag)
				}
			}
		}
		switch {
		case heavy[path]:
			selects := func(goos string, tagged bool) bool {
				return expr != nil && expr.Eval(func(tag string) bool { return tag == goos || tag == "hotupdate_integration" && tagged })
			}
			if !suite || !selects("linux", true) || selects("darwin", true) || selects("linux", false) {
				return 0, fmt.Errorf("%s: heavy real-binary owner must be exactly linux && hotupdate_integration", path)
			}
			tagged++
		case suite:
			return 0, fmt.Errorf("%s: hotupdate_integration outside the heavy test/hotupdate owners", path)
		}
	}
	if tagged == 0 {
		return 0, fmt.Errorf("empty S10 heavy integration root")
	}
	members := map[string][]string{}
	for _, path := range brokerSortedSourcePaths(tree.files) {
		if isProdGoFile(path) && packages[fullstackDir(path)] {
			members[fullstackDir(path)] = append(members[fullstackDir(path)], path)
		}
	}
	for pkg := range packages {
		if err := checkFullstackTwins(tree, pkg, members[pkg]); err != nil {
			return 0, err
		}
	}
	return tagged, nil
}

// fullstackDeclNames lists a file's top-level names; methods are Type.Method.
func fullstackDeclNames(file *ast.File) []string {
	var names []string
	for _, decl := range file.Decls {
		switch value := decl.(type) {
		case *ast.FuncDecl:
			if value.Recv != nil && len(value.Recv.List) > 0 {
				names = append(names, receiverTypeName(value.Recv.List[0].Type)+"."+value.Name.Name)
			} else if value.Name.Name != "init" {
				names = append(names, value.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range value.Specs {
				switch item := spec.(type) {
				case *ast.TypeSpec:
					names = append(names, item.Name.Name)
				case *ast.ValueSpec:
					for _, name := range item.Names {
						if name.Name != "_" {
							names = append(names, name.Name)
						}
					}
				}
			}
		}
	}
	return names
}

// A Linux-only declaration needs a non-Linux twin when it is exported, is main,
// or is referenced by a portable file of the same package. A method needs one
// only while its owner type does: a Linux-only private type that no portable
// file names takes its methods with it. References are read by name, a
// conservative over-approximation that can only demand a twin.
func checkFullstackTwins(tree fullstackTree, pkg string, paths []string) error {
	linux, other, refs := map[string]string{}, map[string]bool{}, map[string]bool{}
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, tree.files[path], parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		onLinux, onDarwin := fullstackSelects(path, file, "linux", "amd64"), fullstackSelects(path, file, "darwin", "arm64")
		for _, name := range fullstackDeclNames(file) {
			if onLinux && !onDarwin {
				linux[name] = path
			} else if onDarwin && !onLinux {
				other[name] = true
			}
		}
		if onLinux && onDarwin {
			ast.Inspect(file, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok {
					refs[ident.Name] = true
				}
				return true
			})
		}
	}
	needed := func(name string) bool { return ast.IsExported(name) || name == "main" || refs[name] }
	for _, name := range brokerSortedSourcePaths(linux) {
		owner, member, method := strings.Cut(name, ".")
		if !method {
			member = owner
		} else if _, linuxOwner := linux[owner]; linuxOwner && !needed(owner) {
			continue
		}
		if needed(member) && !other[name] {
			return fmt.Errorf("%s: Linux-only %s has no non-Linux twin in %s", linux[name], name, pkg)
		}
	}
	return nil
}

// origin: hot-update S10 plan §11.1 / §12.2 tags / P28
func TestFullstackPlatformAndTagsAllOS(t *testing.T) {
	tagged, err := checkFullstackBuildTags(fullstackRepoTree(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("S10 heavy hotupdate_integration owners: %d actual files", tagged)
}

// Every mutation edits a copied real S10 file on disk.
// origin: hot-update S10 plan §10.2 P28 / §12.1 wrong-tag / missing-OS-twin
func TestFullstackBuildTagControls(t *testing.T) {
	real := fullstackRepoTree(t)
	root := fullstackFixture(t, real, append(slices.Clone(fullstackRoots), "test/hotupdate/fullstack_*", "internal/drillobserve")...)
	check := func() error {
		_, err := checkFullstackBuildTags(readFullstackTree(t, root))
		return err
	}
	const harness = "test/fullstackharness/"
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: harness + "schedule.go", old: "package fullstackharness", new: "//go:build hotupdate_integration\n\npackage fullstackharness", want: "outside the heavy test/hotupdate owners"},
		fullstackMutation{path: "test/hotupdate/fullstack_store_witness_test.go", old: "//go:build linux\n", new: "//go:build linux && hotupdate_integration\n", want: "outside the heavy test/hotupdate owners"},
		fullstackMutation{path: "test/hotupdate/fullstack_soak_test.go", old: "//go:build linux && hotupdate_integration\n", new: "//go:build linux\n", want: "fullstack_soak_test.go: heavy real-binary owner"},
		fullstackMutation{path: "test/hotupdate/fullstack_rollout_test.go", old: "//go:build linux && hotupdate_integration\n", new: "//go:build hotupdate_integration\n", want: "fullstack_rollout_test.go: heavy real-binary owner"},
		fullstackMutation{path: "test/hotupdate/fullstack_helpers_test.go", old: "//go:build linux && hotupdate_integration\n", new: "//go:build linux && fullstack_integration\n", want: "uses build tag fullstack_integration"},
		fullstackMutation{path: "test/hotupdate/acceptance/plan.go", old: "package acceptance", new: "//go:build e2e_matrix\n\npackage acceptance", want: "uses build tag e2e_matrix"},
		fullstackMutation{path: harness + "identity_other.go", remove: true, want: "has no non-Linux twin"},
		fullstackMutation{path: harness + "identity_other.go", old: "func NewOFDWitness(", new: "func newOFDWitness(", want: "Linux-only NewOFDWitness has no non-Linux twin"},
		fullstackMutation{path: harness + "process.go", old: "package fullstackharness", new: "//go:build linux\n\npackage fullstackharness", want: "process.go: Linux-only CombinedOutput has no non-Linux twin"},
		fullstackMutation{path: harness + "cmd/supervise/inherited_other.go", old: "func watchInheritedChildren()", new: "func watchInheritedChildrenElsewhere()", want: "Linux-only watchInheritedChildren has no non-Linux twin"},
		fullstackMutation{path: "internal/drillobserve/custody_other.go", old: "func Flush()", new: "func flushed()", want: "Linux-only Flush has no non-Linux twin"},
		fullstackMutation{path: "internal/storewitness/witness_linux.go", new: "package storewitness\n\nfunc LinuxOnlyWitness() {}\n", want: "Linux-only LinuxOnlyWitness has no non-Linux twin"},
	)
	// A twin that exists only by an unexported, unreferenced helper is fine.
	transferWriteFixture(t, root, "internal/storewitness/probe_linux.go", "package storewitness\n\nfunc linuxProbe() {}\n")
	if err := check(); err != nil {
		t.Fatalf("private Linux helper without a portable reference refused: %v", err)
	}
}
