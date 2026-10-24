package architecture

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// structural_budget_test.go — the structural debt ratchet (S3 G3.2, reshaped by the line-2 plan §4).
//
// WHAT IT MEASURES
// ----------------
// FOUR dimensions, all of them COMMENT-IMMUNE:
//
//	type-methods            how many methods hang off one type
//	pkg-files               how many production files are in one package
//	pkg-code-lines          how much CODE (comment-stripped, quantised to 2000) is in one package
//	main-noncli-code-lines  how much non-CLI logic is trapped in package main (comment-stripped, q. 100)
//
// This header said "Three dimensions, and here is why S3's fourth is deliberately absent" for the whole of
// stage C, 290 lines above the fourth dimension it now implements. origin: line-2 closure verification —
// the same "a declaration nothing checks" defect this file is a gate against, in the gate's own doc.
//
// WHAT S3'S REJECTED FOURTH ACTUALLY WAS, AND WHAT REPLACED IT
// -----------------------------------------------------------
// S3 proposed `pkg-lines`: PHYSICAL .go lines per package. That one is still rejected, and the reason is
// measured rather than aesthetic: across batches A/B/C internal/broker grew 3249 lines, of which 2026 --
// 62.4% -- were COMMENT lines. A physical-line ratchet on this repo is, empirically, mostly a comment
// ratchet, and its failure text would have called comment growth "structural debt" and comment deletion
// "good". This repo's comments carry incident history and invariant arguments; a gate that taxes them is
// worse than no gate.
//
// `pkg-code-lines` is NOT that. It strips comments via go/scanner and quantises to 2000, so it measures the
// thing pkg-lines was reaching for without the tax. It exists because plan §11 X2's rejection of pkg-lines
// carried a reopen condition -- "find a comment-immune package-size measure AND show pkg-files missing real
// growth" -- and both halves came true inside the same change that wrote the rejection: countCodeLines is
// that measure, and a review lane appended 1082 lines of real code to internal/broker/loopset.go with every
// gate green, because type-methods and pkg-files are both blind to growth INSIDE existing files.
//
// The same measurement is why main-noncli is counted in NON-COMMENT CODE LINES via go/scanner rather
// than physical lines: the dimension is worth keeping (it fell 3673 -> 1722 when batches A/B moved
// orchestration out of package main, and that win should be locked in), but not at the price of taxing
// the comments in those files.
//
// THE RATCHET IS BIDIRECTIONAL, WHICH IS NOT WHAT S3 SPECIFIED
// -----------------------------------------------------------
// S3 said "current <= golden passes". That form rots: after a refactor drops Broker from 279 methods to
// 200, the golden still says 279 and 79 methods of slack accumulate silently, so the gate stops
// measuring anything long before anyone notices. This file uses the numeric form of B6's DRAINING
// LEDGER instead -- BOTH directions fail:
//
//	current > golden   debt grew. Hand-edit the golden and say why in the commit message.
//	current < golden   debt shrank. Run -update-structural-budget to lock the win in.
//	entity fell below the threshold entirely   delete its line; the ledger shrinks.
//	entity over threshold with no line         a new offender appeared.
//
// -update-structural-budget REFUSES TO WIDEN. That single property is what keeps the routine direction
// safe: because the flag can only tighten, running it can never smuggle unrelated growth past review,
// so "shrank -> red -> run update" being a common path costs nothing. Copying
// cmd/tether/command_tree_inventory_test.go's unconditional rewrite instead would have made the flag a
// one-keystroke way to grant yourself more budget, i.e. a gate that anybody over budget can dissolve.

var updateStructuralBudget = flag.Bool("update-structural-budget", false,
	"tighten the structural budget golden to today's values (never widens; over-budget entities are refused)")

const structuralBudgetGolden = "testdata/structural_budget_golden.txt"

// Thresholds. An entity below its threshold is not registered at all, so the ledger only ever lists
// things that are ALREADY oversized -- it is a list of debts, not an inventory.
const (
	typeMethodThreshold = 40
	pkgFileThreshold    = 20
	// Quantised deliberately coarse. A per-line ratchet on a package would redden on every ordinary
	// commit (see PI-9 for what that felt like on main-noncli), so the ledger records code lines
	// ROUNDED DOWN to this multiple: day-to-day work is invisible, and crossing a 2000-line boundary is
	// the deliberate event worth a conversation.
	pkgCodeLineQuantum   = 2000
	pkgCodeLineThreshold = 2000

	// main-noncli gets the same treatment at a finer grain. origin: line-2 review PI-9. This dimension
	// shipped as an EXACT line count, and the cost showed up immediately: two golden raises inside the
	// increment that introduced it (1102 -> 1106 -> 1116), for +4 and +10 lines of ordinary work. A
	// ratchet that reddens on a four-line edit teaches people to run `-update` reflexively, which is
	// exactly how a ratchet stops being read.
	//
	// 100 rather than 2000 because the quantity is a tenth the size: at 2000 this entity would round to
	// zero and vanish from the ledger entirely. A new non-CLI helper worth ~100 lines in package main is
	// the smallest thing this dimension should still notice, and it does.
	mainNonCLIQuantum = 100
)

type budgetEntry struct {
	kind   string // "type-methods" | "pkg-files" | "pkg-code-lines" | "main-noncli-code-lines"
	entity string
	value  int
}

// S7 caps are below the debt-registration thresholds. Keep them independent of
// the S6 table so its closed namespace and controls do not become allowances for
// transfer growth. AllOS measurement includes platform twins.
// origin: hot-update S7 plan §11.1 / T51
// gate-control: TestTransferOwnershipBudgetControls
var transferPackageCaps = map[string]struct{ files, code int }{
	"xfercontract": {3, 1800}, "xfercontract/contractstub": {1, 700},
	"xferworker": {3, 1800}, "xfersend": {3, 1800}, "xferreceive": {3, 1800},
	"xfercourier": {3, 1800}, "xferfinal": {3, 1800}, "xferhistory": {3, 1800},
	"xferbucket": {3, 1800}, "xfergc": {3, 1800}, "xferauth": {3, 1800},
	"xfertrack": {4, 2400}, "xferledger": {4, 2400}, "xfercontrol": {5, 2800},
	"xferclient": {7, 3600}, "xferimage": {5, 2800}, "xferbridge": {2, 1200},
	// The contract's NATS-free value closures (P00fix xferport, s7-p9i xfervalue
	// split) are first registrations at their measured AllOS size (+1 code line):
	// splitting a declaration out of xfercontract must not become new headroom.
	// origin: hot-update S7 plan §8.1 / §11.1 / P00 (s7-p9i R5)
	"xfercontract/xferport": {1, 95}, "xfercontract/xfervalue": {2, 435},
}

func checkTransferStructure(result brokerStructure, exact bool) error {
	if err := checkBrokerStructure(result); err != nil {
		return err
	}
	if result.files["internal/tunnel"] > 22 || result.mainCode >= 900 {
		return fmt.Errorf("S7 tunnel files/main non-CLI code ceiling exceeded")
	}
	if exact && (result.methods["internal/agent.Agent"] != 125 || result.methods["internal/execsession.Runtime"] != 39) {
		return fmt.Errorf("Agent must have exactly 125 methods; Runtime exactly 39")
	}
	for pkg, cap := range transferPackageCaps {
		if result.files["internal/"+pkg] > cap.files || result.code["internal/"+pkg] >= cap.code {
			return fmt.Errorf("%s: S7 production files/code exceed %d/<%d", pkg, cap.files, cap.code)
		}
	}
	for key, n := range result.fields {
		if strings.HasPrefix(key, "internal/xfer") && n >= 100 {
			return fmt.Errorf("%s: S7 receiver fields must remain below 100", key)
		}
	}
	return nil
}

// Descriptor ownership is keyed by resource ID/name, not kind alone: worker
// and tracker each own a sealed state resource in the same codec namespace.
func checkTransferResourceOwnership(refs, owners map[string]string) error {
	if len(refs) == 0 || len(owners) == 0 {
		return fmt.Errorf("empty transfer resource union")
	}
	if len(refs) != len(owners) {
		return fmt.Errorf("transfer descriptor/resource union differs")
	}
	for key, kind := range refs {
		if kind != "xfer-state-v1" && kind != "xfer-source-v1" && kind != "xfer-tmp-v1" && kind != "xfer-parent-v1" {
			return fmt.Errorf("unknown transfer resource kind %s", kind)
		}
		if key == "" || owners[key] == "" {
			return fmt.Errorf("unowned transfer resource %s", key)
		}
	}
	return nil
}

func checkTransferResourceRows(all map[string]transferDeclaration, rows [][]string) error {
	roles := map[string]bool{}
	refs, owners := map[string]string{}, map[string]string{}
	for _, row := range rows {
		if len(row) != 5 || roles[row[0]] || owners[row[1]] != "" {
			return fmt.Errorf("duplicate resource role/owner %v", row)
		}
		roles[row[0]] = true
		refs[row[1]], owners[row[1]] = row[2], row[3]
		nodes, err := reachableTransferDeclarations(all, row[4])
		if err != nil {
			return err
		}
		if !slices.Contains(transferRootTokens(nodes), "literal="+strconv.Quote(row[2])) {
			return fmt.Errorf("%s: live resource builder does not reference its kind", row[0])
		}
	}
	for _, role := range []string{"worker-state", "tracker-state", "source", "tmp", "parent"} {
		if !roles[role] {
			return fmt.Errorf("missing transfer resource role %s", role)
		}
	}
	return checkTransferResourceOwnership(refs, owners)
}

// origin: hot-update S7 plan §8.3 / §11.1 / T51
func TestTransferOwnershipAndBudgetsAreClosed(t *testing.T) {
	root := repoRoot(t)
	plan, err := os.ReadFile(filepath.Join(root, "docs/reviews/hot-update-transfer-continuation-plan.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTransferFileOwnership(string(plan)); err != nil {
		t.Error(err)
	}
	baselineRows, err := readTransferTSV(root, "test/architecture/testdata/transfer_source_moves.tsv#baseline-files", 1)
	if err != nil {
		t.Error(err)
	} else {
		baseline := map[string]bool{}
		for _, row := range baselineRows {
			baseline[row[0]] = true
		}
		if err := checkTransferNewFileLines(root, baseline); err != nil {
			t.Error(err)
		}
	}
	sources := brokerGateSources(t, root)
	result := measureBrokerStructure(t, sources)
	if err := checkTransferStructure(result, true); err != nil {
		t.Error(err)
	}
	initialCaps, err := readTransferTSV(root, "test/architecture/testdata/transfer_source_moves.tsv#existing-package-caps", 3)
	if err != nil {
		t.Error(err)
	} else if err := checkTransferInitialCaps(result, initialCaps); err != nil {
		t.Error(err)
	}
	legacy, err := readLegacyFileCodeCeilings(filepath.Join(root, "test/architecture", legacyFileCodeLedger))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerFileCode(result.fileCode, legacy); err != nil {
		t.Error(err)
	}
	for _, path := range brokerSortedSourcePaths(sources) {
		baseline := legacy[path]
		t.Logf("raw-file-code\t%s\tbaseline=%d\tafter=%d\tdelta=%+d", path, baseline, result.fileCode[path], result.fileCode[path]-baseline)
	}
	// origin: S10 internal review ARCH-REDS R5
	if err := checkBrokerCodeDecrease(brokerEntryCodeLines, result.code["internal/broker"]); err != nil {
		t.Error(err)
	}
	if agentimage.MaxBundleBytes != 16<<20 || brokerimage.MaxBundleBytes != 16<<20 || sealedstate.MaxStateBytes != 32<<20 || upgradebudget.NodeReplayBytes != 256<<20 {
		t.Error("S7 cannot create a separate private-state/replay capacity pool")
	}
	golden, err := readGolden(t, filepath.Join(root, "test/architecture", structuralBudgetGolden))
	if err != nil {
		t.Fatal(err)
	}
	for key, cap := range map[string]int{"main-noncli-code-lines cmd/tether": 800, "pkg-code-lines internal/brokerimage": 2000, "pkg-code-lines internal/brokercontrol": 4000} {
		if golden[key] > cap {
			t.Errorf("%s golden exceeds frozen S7 ceiling %d", key, cap)
		}
	}
	rows, err := readTransferTSV(root, "test/architecture/testdata/transfer_source_moves.tsv#resource-owners", 5)
	if err != nil {
		t.Fatal(err)
	}
	all, err := transferDeclarations(sources)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTransferResourceRows(all, rows); err != nil {
		t.Fatal(err)
	}
}

func checkTransferInitialCaps(result brokerStructure, rows [][]string) error {
	want := map[string]bool{"internal/jsstream": true, "internal/transferpath": true, "internal/drillobserve": true}
	for _, row := range rows {
		if len(row) != 3 || !want[row[0]] {
			return fmt.Errorf("duplicate/unknown S7 first-registration package %v", row)
		}
		files, e1 := strconv.Atoi(row[1])
		code, e2 := strconv.Atoi(row[2])
		if e1 != nil || e2 != nil || files <= 0 || code <= 0 || result.files[row[0]] == 0 || result.files[row[0]] > files || result.code[row[0]] >= code {
			return fmt.Errorf("%s: AllOS first-registration file/code cap exceeded or empty", row[0])
		}
		delete(want, row[0])
	}
	if len(want) != 0 {
		return fmt.Errorf("incomplete S7 first-registration package caps")
	}
	return nil
}

// The baseline is the frozen pre-S7 path set, not a list of today's offenders.
// Production Go already has the stripped per-file ceiling. New tests/scripts
// use total lines, so comments and semicolon packing cannot buy extra headroom.
func checkTransferNewFileLines(root string, baseline map[string]bool) error {
	if len(baseline) == 0 {
		return fmt.Errorf("empty S7 file baseline")
	}
	for _, top := range []string{"internal", "cmd", "test"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if baseline[rel] || !strings.HasSuffix(path, "_test.go") && !strings.HasSuffix(path, ".sh") && !strings.HasSuffix(path, ".py") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			lines := strings.Count(strings.TrimSuffix(string(raw), "\n"), "\n") + 1
			if lines > 700 {
				return fmt.Errorf("%s: new test/script has %d total lines, maximum 700", rel, lines)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTransferFileOwnership(plan string) error {
	owners := map[string]string{}
	packages := map[string]bool{}
	for _, line := range strings.Split(plan, "\n") {
		if !regexp.MustCompile(`^\| P[0-9]{2} [^|]+\| (HARD|NORMAL) \|`).MatchString(line) {
			continue
		}
		cells := strings.Split(line, "|")
		owner := strings.Fields(cells[1])[0]
		if packages[owner] {
			return fmt.Errorf("duplicate file ownership package %s", owner)
		}
		packages[owner] = true
		paths := regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(cells[5], -1)
		if len(paths) == 0 {
			return fmt.Errorf("empty file ownership package %s", owner)
		}
		for _, match := range paths {
			path := match[1]
			if owners[path] != "" {
				return fmt.Errorf("%s: duplicate file owner %s/%s", path, owners[path], owner)
			}
			owners[path] = owner
		}
	}
	if len(packages) == 0 {
		return fmt.Errorf("empty S7 file ownership manifest")
	}
	return nil
}

// origin: hot-update S7 plan §11.1 / T51
func TestTransferOwnershipBudgetControls(t *testing.T) {
	initial := brokerStructure{files: map[string]int{}, code: map[string]int{}}
	var capRows [][]string
	for _, pkg := range []string{"internal/jsstream", "internal/transferpath", "internal/drillobserve"} {
		initial.files[pkg] = 1
		initial.code[pkg] = 1
		capRows = append(capRows, []string{pkg, "1", "2"})
	}
	if err := checkTransferInitialCaps(initial, capRows); err != nil {
		t.Fatal(err)
	}
	initial.code["internal/drillobserve"] = 2
	if checkTransferInitialCaps(initial, capRows) == nil || checkTransferInitialCaps(initial, capRows[:2]) == nil {
		t.Fatal("first-registration code cap/missing package escaped")
	}
	lineRoot := transferFixtureTree(t, map[string]string{"test/old_test.go": strings.Repeat("// preserved old test\n", 800), "test/new_test.go": strings.Repeat("// new test\n", 700)})
	if err := checkTransferNewFileLines(lineRoot, map[string]bool{"test/old_test.go": true}); err != nil {
		t.Fatal(err)
	}
	transferWriteFixture(t, lineRoot, "test/new_test.go", strings.Repeat("// new test\n", 701))
	if checkTransferNewFileLines(lineRoot, map[string]bool{"test/old_test.go": true}) == nil || checkTransferNewFileLines(lineRoot, nil) == nil {
		t.Fatal("new test line 701/empty baseline escaped")
	}
	manifest := "| P00 Contracts | HARD | 1 | — | `internal/xfercontract/contracts.go` |\n| P34 Gates | HARD | 1 | P00 | `test/architecture/transfer_contract_test.go` |\n"
	if err := checkTransferFileOwnership(manifest); err != nil {
		t.Fatal(err)
	}
	if checkTransferFileOwnership("") == nil || checkTransferFileOwnership(manifest+"| P35 Ledgers | HARD | 1 | P00 | `internal/xfercontract/contracts.go` |\n") == nil {
		t.Fatal("empty/duplicate file ownership manifest escaped")
	}
	frozen := map[string]struct{ files, code int }{
		"xfercontract": {3, 1800}, "xfercontract/contractstub": {1, 700}, "xferworker": {3, 1800},
		"xfersend": {3, 1800}, "xferreceive": {3, 1800}, "xfercourier": {3, 1800}, "xferfinal": {3, 1800},
		"xferhistory": {3, 1800}, "xferbucket": {3, 1800}, "xfergc": {3, 1800}, "xferauth": {3, 1800},
		"xfertrack": {4, 2400}, "xferledger": {4, 2400}, "xfercontrol": {5, 2800}, "xferclient": {7, 3600},
		"xferimage": {5, 2800}, "xferbridge": {2, 1200},
		// Measured first registrations of the contract's value closures (s7-p9i R5);
		// they add constraints and widen no plan cap.
		"xfercontract/xferport": {1, 95}, "xfercontract/xfervalue": {2, 435},
	}
	if len(frozen) != len(transferPackageCaps) {
		t.Fatal("S7 leaf cap registry drifted")
	}
	for pkg, cap := range frozen {
		if transferPackageCaps[pkg] != cap {
			t.Fatalf("S7 frozen package cap changed %s", pkg)
		}
	}
	root := transferFixtureTree(t, map[string]string{
		"internal/agent/agent.go":         "package agent;type Agent struct{}",
		"internal/execsession/runtime.go": "package execsession;type Runtime struct{}",
	})
	writeMethods := func(path, typ string, count int) {
		source := "package owner;type " + typ + " struct{}\n"
		for i := 0; i < count; i++ {
			source += fmt.Sprintf("func (*%s) Method%d(){}\n", typ, i)
		}
		transferWriteFixture(t, root, path, source)
	}
	writeMethods("internal/agent/agent.go", "Agent", 125)
	writeMethods("internal/execsession/runtime.go", "Runtime", 39)
	measure := func() brokerStructure { return measureBrokerStructure(t, brokerGateSources(t, root)) }
	if err := checkTransferStructure(measure(), true); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{124, 126} {
		writeMethods("internal/agent/agent.go", "Agent", count)
		if checkTransferStructure(measure(), true) == nil {
			t.Fatalf("on-disk Agent%d escaped", count)
		}
	}
	writeMethods("internal/agent/agent.go", "Agent", 125)
	for pkg, cap := range transferPackageCaps {
		result := measure()
		result.files["internal/"+pkg] = cap.files + 1
		if checkTransferStructure(result, true) == nil {
			t.Fatalf("S7 file cap escaped %s", pkg)
		}
		result = measure()
		result.code["internal/"+pkg] = cap.code
		if checkTransferStructure(result, true) == nil {
			t.Fatalf("S7 code cap escaped %s", pkg)
		}
	}
	transferWriteFixture(t, root, "internal/xferimage/state_other.go", "//go:build !linux\npackage state\nvar (\n"+strings.Repeat("_ = 1\n", 698)+")\n")
	if checkBrokerFileCode(measure().fileCode, nil) == nil {
		t.Fatal("on-disk inactive OS file at code line 701 escaped")
	}
	if checkBrokerFileCode(map[string]int{"internal/old/file.go": 141}, map[string]int{"internal/old/file.go": 140}) == nil {
		t.Fatal("legacy ceiling+1 escaped")
	}
	for i := 0; i < 13; i++ {
		transferWriteFixture(t, root, fmt.Sprintf("internal/brokerimage/owner%d_other.go", i), "package owner")
	}
	if checkTransferStructure(measure(), true) == nil {
		t.Fatal("on-disk brokerimage thirteenth file escaped")
	}
	refs := map[string]string{"worker/state": "xfer-state-v1", "source/source": "xfer-source-v1"}
	owners := map[string]string{"worker/state": "worker", "source/source": "sender"}
	if err := checkTransferResourceOwnership(refs, owners); err != nil {
		t.Fatal(err)
	}
	delete(owners, "source/source")
	if checkTransferResourceOwnership(refs, owners) == nil || checkTransferResourceOwnership(nil, nil) == nil {
		t.Fatal("missing resource/empty ownership union escaped")
	}
	all, err := transferDeclarations(map[string]string{"internal/xferimage/resources.go": "package image;func Export(){resource(\"xfer-state-v1\",\"xfer-source-v1\",\"xfer-tmp-v1\",\"xfer-parent-v1\")}"})
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]string
	for _, role := range []string{"worker-state", "tracker-state", "source", "tmp", "parent"} {
		kind := "xfer-" + role + "-v1"
		if strings.Contains(role, "state") {
			kind = "xfer-state-v1"
		}
		rows = append(rows, []string{role, role + "/state", kind, "owner", "internal/xferimage/resources.go:Export"})
	}
	if err := checkTransferResourceRows(all, rows); err != nil {
		t.Fatal(err)
	}
	if checkTransferResourceRows(all, append(rows, rows[0])) == nil || checkTransferResourceRows(all, rows[:4]) == nil {
		t.Fatal("duplicate owner/missing resource role escaped")
	}
}

func (e budgetEntry) key() string { return e.kind + " " + e.entity }

// ---------- measurement ----------

// isProdGoFile reports whether a path is a non-test .go file.
func isProdGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// measureTypeMethods counts methods per receiver type across internal/ and cmd/.
func measureTypeMethods(t *testing.T, root string) []budgetEntry {
	t.Helper()
	counts := map[string]int{}
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			for _, de := range names {
				if de.IsDir() || !isProdGoFile(de.Name()) {
					continue
				}
				fset := token.NewFileSet()
				f, perr := parser.ParseFile(fset, filepath.Join(dir, de.Name()), nil, 0)
				if perr != nil {
					t.Fatalf("parse %s/%s: %v", rel, de.Name(), perr)
				}
				for _, d := range f.Decls {
					fn, ok := d.(*ast.FuncDecl)
					if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
						continue
					}
					if name := receiverTypeName(fn.Recv.List[0].Type); name != "" {
						counts[rel+"."+name]++
					}
				}
			}
		})
	}
	var out []budgetEntry
	for entity, n := range counts {
		if n > typeMethodThreshold {
			out = append(out, budgetEntry{kind: "type-methods", entity: entity, value: n})
		}
	}
	return out
}

func receiverTypeName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	case *ast.Ident:
		return v.Name
	case *ast.IndexExpr: // generic receiver Foo[T]
		return receiverTypeName(v.X)
	case *ast.IndexListExpr:
		return receiverTypeName(v.X)
	}
	return ""
}

// measurePkgFiles counts production files per package directory.
func measurePkgFiles(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			n := 0
			for _, de := range names {
				if !de.IsDir() && isProdGoFile(de.Name()) {
					n++
				}
			}
			if n > pkgFileThreshold {
				out = append(out, budgetEntry{kind: "pkg-files", entity: rel, value: n})
			}
		})
	}
	return out
}

// measureMainNonCLICodeLines counts NON-COMMENT, non-blank code lines in the cmd/tether files that do
// not IMPORT cobra -- i.e. the orchestration/protocol logic that is sitting in package main instead of
// in a package that can be imported and tested directly (L03-F6).
//
// go/scanner with mode 0 does not emit COMMENT tokens, so counting the distinct lines that carry at
// least one emitted token yields exactly "lines with code on them" -- comments and blank lines are
// excluded structurally rather than by regex.
//
// CLASSIFY BY IMPORT, NOT BY SUBSTRING. origin: line-2 review PI-2. The first version asked
// `strings.Contains(src, "cobra")`, so a file was exempted from the count by MENTIONING cobra anywhere
// -- including in a comment. That is not hypothetical: cmd/tether/poll.go discusses cobra in prose,
// imports nothing of the sort, and was therefore invisible to a dimension whose entire job is to find
// non-CLI logic hiding in package main. Worse, the exemption ran the wrong way for a ratchet: writing
// the word "cobra" in a comment was a one-line way to remove a file from the budget forever.
//
// Parsing imports answers the question the dimension is actually asking ("is this file wiring up the
// CLI?") and cannot be spelled into existence by a comment.
func measureMainNonCLICodeLines(t *testing.T, root string) []budgetEntry {
	t.Helper()
	dir := filepath.Join(root, "cmd", "tether")
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir cmd/tether: %v", err)
	}
	total := 0
	for _, de := range names {
		if de.IsDir() || !isProdGoFile(de.Name()) {
			continue
		}
		path := filepath.Join(dir, de.Name())
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		if importsCobra(t, path, src) {
			continue
		}
		total += countCodeLines(t, path, src)
	}
	q := total - total%mainNonCLIQuantum
	return []budgetEntry{{kind: "main-noncli-code-lines", entity: "cmd/tether", value: q}}
}

// importsCobra reports whether a file has cobra in its import list.
func importsCobra(t *testing.T, path string, src []byte) bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports of %s: %v", path, err)
	}
	for _, spec := range f.Imports {
		p, uerr := strconv.Unquote(spec.Path.Value)
		if uerr != nil {
			continue
		}
		if p == "github.com/spf13/cobra" {
			return true
		}
	}
	return false
}

func countCodeLines(t *testing.T, path string, src []byte) int {
	t.Helper()
	fset := token.NewFileSet()
	file := fset.AddFile(path, fset.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, func(pos token.Position, msg string) {
		t.Fatalf("scan %s:%d: %s", path, pos.Line, msg)
	}, 0) // mode 0: comments are NOT returned
	lines := map[int]bool{}
	for {
		pos, tok, _ := s.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON {
			// Auto-inserted semicolons carry the position of the newline, which would count a line
			// that has no code of its own. Explicit ones sit on a line that has other tokens anyway.
			continue
		}
		lines[file.Position(pos).Line] = true
	}
	return len(lines)
}

// walkPkgDirs invokes fn for every directory under base that holds at least one .go file. rel is
// reported relative to root, so entity names in the ledger are repo-relative import-ish paths
// (`internal/broker`) and do not embed the checkout directory's name.
func walkPkgDirs(t *testing.T, root, base string, fn func(dir, rel string)) {
	t.Helper()
	err := filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return nil
		}
		switch info.Name() {
		case "testdata", ".git", "vendor":
			return filepath.SkipDir
		}
		names, rerr := os.ReadDir(p)
		if rerr != nil {
			return rerr
		}
		has := false
		for _, de := range names {
			if !de.IsDir() && isProdGoFile(de.Name()) {
				has = true
				break
			}
		}
		if !has {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		fn(p, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", base, err)
	}
}

// measurePkgCodeLines counts NON-COMMENT code lines per package, comment-stripped by the same
// go/scanner pass main-noncli uses.
//
// origin: line-2 external review PI-8, and the auditor's re-adjudication of plan §11 X2. X2 rejected
// S3's pkg-lines dimension FOREVER, with the reopen condition "find a comment-immune package-size
// measure AND show pkg-files misses real growth". Both halves were satisfied inside the same change
// that wrote the rejection: countCodeLines (below, comment-immune) is that measure, and a review lane
// appended 1082 lines of REAL CODE to internal/broker/loopset.go (219 -> 1301) with every gate green,
// because type-methods and pkg-files are both blind to growth inside existing files.
//
// That blindness was the worst possible shape: the other two dimensions were already ratcheted, so the
// only unmeasured direction was "pile more into a file that already exists" -- and CLAUDE.md's own
// "prefer merging into an existing file" rule points straight at it. A ratchet whose one gap is also
// the path of least resistance is a ratchet that shapes behaviour toward god files.
//
// Physical lines stay rejected (X2's real argument): 62.4% of internal/broker's growth was comments.
// This measures code.
func measurePkgCodeLines(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			total := 0
			for _, de := range names {
				if de.IsDir() || !isProdGoFile(de.Name()) {
					continue
				}
				path := filepath.Join(dir, de.Name())
				src, rerr := os.ReadFile(path)
				if rerr != nil {
					t.Fatalf("read %s: %v", path, rerr)
				}
				total += countCodeLines(t, path, src)
			}
			if total > pkgCodeLineThreshold {
				quantised := total / pkgCodeLineQuantum * pkgCodeLineQuantum
				out = append(out, budgetEntry{kind: "pkg-code-lines", entity: rel, value: quantised})
			}
		})
	}
	return out
}

func measureAll(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	out = append(out, measureTypeMethods(t, root)...)
	out = append(out, measurePkgFiles(t, root)...)
	out = append(out, measurePkgCodeLines(t, root)...)
	out = append(out, measureMainNonCLICodeLines(t, root)...)
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// ---------- golden ----------

func readGolden(t *testing.T, path string) (map[string]int, error) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 {
			t.Fatalf("%s:%d: want `<kind> <entity> <count>`, got %q", path, i+1, line)
		}
		n, cerr := strconv.Atoi(f[2])
		if cerr != nil {
			t.Fatalf("%s:%d: unparseable count %q", path, i+1, f[2])
		}
		out[f[0]+" "+f[1]] = n
	}
	return out, nil
}

// goldenSentinel is the LAST line of the generated header. Everything after it is hand-written.
//
// The boundary is an exact full-line sentinel rather than a prefix match on some header sentence,
// because the prefix version had a compounding bug: the marker matched only the FIRST line of a
// two-line paragraph, so the second line was classified as hand-written and preserved -- while the
// template also re-emitted it. Every `-update` invocation therefore added one more copy of
// "# one already here, which is what makes running it a safe routine action." The flag CLAUDE.md calls
// "a safe routine action" grew the file it maintained, without bound, one line per run.
const goldenSentinel = "# ==== everything below this line is hand-written and survives regeneration ===="

// preservedComments recovers the hand-written comment blocks from an existing golden, keyed by the entry
// they sit above so that re-sorting entries does not detach a justification from what it justifies.
//
// origin: line-2 external review D3/GI-5/PF-5/IDG-4 — four lanes, same defect. -update-structural-budget
// rewrote the whole file from a fixed template, which silently deleted the 13 hand-written lines
// justifying the two budget raises. That flag is the action the RED path tells you to run, and CLAUDE.md
// §5 calls it "a safe routine action" — so the justification a reviewer is supposed to find had a
// half-life of one routine command. Worse, the same CLAUDE.md paragraph six lines up says "comments are
// an asset; any refactor must carry them across wholesale", and this was the gate's own flag violating it.
//
// The first fix preserved the comments but dumped them all in one blob at the top, which put the
// pkg-code-lines rationale directly above the main-noncli entry -- text that now described the wrong
// number. Attaching each block to its key is what makes the preservation actually readable.
func preservedComments(t *testing.T, path string) (orphans []string, byKey map[string][]string) {
	t.Helper()
	byKey = map[string][]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, byKey // first generation: nothing to preserve
		}
		t.Fatalf("read %s before preserving ledger justifications: %v\n\n"+
			"Only a genuinely absent file is first generation. Treating permission, I/O, or wrong-shape "+
			"errors as an empty ledger would let the update path overwrite a file whose hand-written "+
			"justifications it never read.", path, err)
	}

	// REFUSE rather than wipe when the boundary is missing. origin: line-2 closure verification M3.
	//
	// Everything below depends on finding goldenSentinel. Without it `past` never becomes true, nothing is
	// collected, and renderGolden then writes a file consisting of the template plus the data lines — i.e.
	// EVERY hand-written justification deleted, at rc=0, by the command the RED path tells people to run.
	// That is the worst available failure shape: silent, total, and triggered by following instructions.
	//
	// A file that exists but has no sentinel is either hand-mangled or written by an older version of this
	// code. Both need a human, and neither should be resolved by throwing the comments away.
	if len(strings.TrimSpace(string(b))) > 0 && !strings.Contains(string(b), goldenSentinel) {
		t.Fatalf("%s exists but contains no boundary line %q.\n\n"+
			"Refusing to regenerate: everything below that line is hand-written justification, and without "+
			"the boundary this function cannot tell it from the generated template — it would preserve "+
			"nothing and the write would delete all of it, silently, at rc=0.\n\n"+
			"Restore the line (it goes immediately after the NOTE paragraph, before the first comment block) "+
			"or, if the file really should be regenerated from scratch, delete it and re-run.",
			path, goldenSentinel)
	}

	past := false
	var pending []string
	for _, line := range strings.Split(string(b), "\n") {
		if line == goldenSentinel {
			past = true
			continue
		}
		if !past {
			continue // generated header; the template re-emits it verbatim
		}
		if strings.HasPrefix(line, "#") {
			pending = append(pending, line)
			continue
		}
		if k := entryKeyOfGoldenLine(line); k != "" {
			if len(pending) > 0 {
				byKey[k] = append(byKey[k], pending...)
				pending = nil
			}
			continue
		}
	}
	// A trailing block belongs to no entry -- most often because the entity it described dropped below
	// threshold and left the ledger. Keep it rather than delete it; losing the reasoning is the failure
	// this function exists to prevent, and an orphaned paragraph is a much cheaper problem than a
	// silently-deleted one.
	return pending, byKey
}

// entryKeyOfGoldenLine returns "<kind> <entity>" for a data line, or "" for anything else.
func entryKeyOfGoldenLine(line string) string {
	f := strings.Fields(line)
	if len(f) != 3 {
		return ""
	}
	if _, err := strconv.Atoi(f[2]); err != nil {
		return ""
	}
	return f[0] + " " + f[1]
}

func renderGolden(entries []budgetEntry, orphans []string, byKey map[string][]string) string {
	var b strings.Builder
	b.WriteString("# structural_budget_golden.txt — the debt ledger for test/architecture/structural_budget_test.go.\n")
	b.WriteString("#\n")
	b.WriteString("# Only entities ALREADY over threshold are listed, so this file is a list of debts, not an\n")
	b.WriteString("# inventory. Both directions of drift fail: growth demands a hand edit plus a justification in\n")
	b.WriteString("# the commit message, shrinkage demands `go test ./test/architecture/ -update-structural-budget`\n")
	b.WriteString("# so the improvement is locked in and cannot silently become slack.\n")
	b.WriteString("#\n")
	b.WriteString(fmt.Sprintf("# thresholds: type-methods > %d methods, pkg-files > %d files, pkg-code-lines > %d\n",
		typeMethodThreshold, pkgFileThreshold, pkgCodeLineThreshold))
	b.WriteString(fmt.Sprintf("# code lines. The two code-line dimensions are quantised DOWN (pkg-code-lines to %d,\n",
		pkgCodeLineQuantum))
	b.WriteString(fmt.Sprintf("# main-noncli-code-lines to %d) so ordinary commits are invisible and only crossing a boundary\n",
		mainNonCLIQuantum))
	b.WriteString("# is an event. main-noncli-code-lines has no lower threshold: it is always tracked.\n")
	b.WriteString("#\n")
	b.WriteString("# NOTE: -update-structural-budget only ever TIGHTENS. It refuses to write a value wider than the\n")
	b.WriteString("# one already here, and it refuses to admit a NEW over-budget entity at all, which is what makes\n")
	b.WriteString("# running it a safe routine action.\n")
	b.WriteString(goldenSentinel + "\n")
	for _, line := range orphans {
		b.WriteString(line + "\n")
	}
	for _, e := range entries {
		for _, line := range byKey[e.key()] {
			b.WriteString(line + "\n")
		}
		b.WriteString(fmt.Sprintf("%s %s %d\n", e.kind, e.entity, e.value))
	}
	return b.String()
}

func TestStructuralBudget(t *testing.T) {
	root := repoRoot(t)
	current := measureAll(t, root)
	goldenPath := filepath.Join(root, "test", "architecture", structuralBudgetGolden)

	if *updateStructuralBudget {
		updateGoldenTightenOnly(t, goldenPath, current)
		return
	}

	golden, err := readGolden(t, goldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v\nGenerate it with:\n"+
			"  go test ./test/architecture/ -run TestStructuralBudget -update-structural-budget", goldenPath, err)
	}

	seen := map[string]bool{}
	for _, e := range current {
		seen[e.key()] = true
		want, ok := golden[e.key()]
		if !ok {
			t.Errorf("NEW over-budget entity: %s = %d (threshold exceeded, not in the ledger).\n"+
				"  If this growth is intended, add the line and say WHY in the commit message.",
				e.key(), e.value)
			continue
		}
		switch {
		case e.value > want:
			t.Errorf("BUDGET EXCEEDED: %s = %d, ledger says %d (+%d).\n"+
				"  Raising a budget is an edit to an invariant: change the line BY HAND and justify it in the\n"+
				"  commit message. -update-structural-budget will refuse to widen it for you.",
				e.key(), e.value, want, e.value-want)
		case e.value < want:
			t.Errorf("DEBT SHRANK and the ledger is stale: %s = %d, ledger still says %d.\n"+
				"  Lock the improvement in:  go test ./test/architecture/ -run TestStructuralBudget -update-structural-budget\n"+
				"  (leaving the slack in place is how a ratchet quietly stops measuring anything.)",
				e.key(), e.value, want)
		}
	}
	var stale []string
	for key := range golden {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d ledger entr(ies) name something that is no longer over budget:\n  %s\n\n"+
			"Delete those lines — the ledger must drain. An allow-list that is only ever appended to stops\n"+
			"being a measurement and becomes a permanent exemption.",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// updateGoldenTightenOnly writes the golden, but ONLY where the new value is tighter. An entity that is
// over its recorded budget is refused, loudly: the whole point of the ledger is that widening costs a
// deliberate hand edit, and a flag that widens on request is a gate anybody over budget can dissolve.
func updateGoldenTightenOnly(t *testing.T, goldenPath string, current []budgetEntry) {
	t.Helper()
	prev, err := readGolden(t, goldenPath)
	firstGeneration := false
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("read golden for update: %v", err)
		}
		prev = map[string]int{}
		firstGeneration = true
	}
	// BOOTSTRAP IS A REAL MODE, NOT A COMMENT. origin: line-2 INDEPENDENT EXTERNAL REVIEW M6.
	//
	// The missing-golden failure message told the reader to run this flag to generate one. It could not
	// work: with no file, `prev` is empty, every measured entity hits the `!known` branch added for GI-4,
	// and the command refuses all twelve. So the documented recovery path was impossible — a instruction
	// that had never been executed by anyone, including the person who wrote it.
	//
	// The two halves are not in conflict once they are separated: `!known` exists to stop a NEW over-budget
	// entity being admitted to an EXISTING ledger without review. When there is no ledger at all, there is
	// no review to smuggle anything past — the whole file is the thing being reviewed. So bootstrap writes,
	// and says loudly that what it produced is a proposal rather than an accepted baseline.
	if firstGeneration {
		t.Logf("BOOTSTRAP: %s did not exist, so this run is generating it from scratch.\n"+
			"  The result is a PROPOSAL, not an accepted baseline: read every line before committing it, and "+
			"write the justification for each entry by hand. The refusal that normally guards new entities "+
			"is deliberately not applied here — there is no prior ledger for a new entity to be smuggled "+
			"past.", structuralBudgetGolden)
	}

	var refused []string
	out := make([]budgetEntry, 0, len(current))
	for _, e := range current {
		was, known := prev[e.key()]
		switch {
		case known && e.value > was:
			refused = append(refused, fmt.Sprintf("%s: ledger %d, measured %d (+%d)", e.key(), was, e.value, e.value-was))
			out = append(out, budgetEntry{kind: e.kind, entity: e.entity, value: was}) // keep the tighter value
		case !known && firstGeneration:
			// Bootstrap: there is no ledger to smuggle anything past. See the note above.
			out = append(out, e)
		case !known:
			// origin: line-2 external review GI-4. This branch used to fall through and WRITE the new
			// entity, rc=0, silently -- so a brand-new over-budget type or package was admitted to the
			// ledger by the routine tightening command, with no edit to review and no justification. The
			// file header claimed the flag "can never smuggle unrelated growth past review"; a new
			// entity is exactly unrelated growth, and it was the one shape the refusal missed.
			refused = append(refused, fmt.Sprintf("%s: NEW over-budget entity at %d (not in the ledger)",
				e.key(), e.value))
		default:
			out = append(out, e)
		}
	}
	if len(refused) > 0 {
		sort.Strings(refused)
		t.Fatalf("-update-structural-budget REFUSES %d change(s) that are not tightenings:\n  %s\n\n"+
			"This flag only ever tightens. To raise a budget or admit a new over-budget entity, edit %s by\n"+
			"hand and justify it in the commit message — that friction IS the gate.",
			len(refused), strings.Join(refused, "\n  "), structuralBudgetGolden)
	}

	if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
		t.Fatalf("mkdir testdata: %v", err)
	}
	orphans, byKey := preservedComments(t, goldenPath)
	if err := os.WriteFile(goldenPath, []byte(renderGolden(out, orphans, byKey)), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
	t.Logf("tightened %s to %d entries", structuralBudgetGolden, len(out))
}

// TestStructuralBudgetMeasurementIsNonVacuous proves the three measurements actually measure. A green
// TestStructuralBudget with a broken scanner and an empty ledger is indistinguishable from a green one
// with a working scanner, and the empty-ledger state is reachable by accident.
func TestStructuralBudgetMeasurementIsNonVacuous(t *testing.T) {
	root := repoRoot(t)

	tm := measureTypeMethods(t, root)
	if len(tm) == 0 {
		t.Error("type-methods found nothing over 40 — internal/broker.Broker alone has well over 200")
	}
	var foundBroker bool
	for _, e := range tm {
		if e.entity == "internal/broker.Broker" {
			foundBroker = true
			if e.value < 100 {
				t.Errorf("internal/broker.Broker measured %d methods, which is implausibly low", e.value)
			}
		}
	}
	if !foundBroker {
		t.Error("type-methods did not find internal/broker.Broker at all")
	}

	if pf := measurePkgFiles(t, root); len(pf) == 0 {
		t.Error("pkg-files found no package over 20 production files — internal/broker has ~70")
	}

	mn := measureMainNonCLICodeLines(t, root)
	if len(mn) != 1 || mn[0].value == 0 {
		t.Errorf("main-noncli-code-lines produced %v, want exactly one non-zero entry", mn)
	}
	// The comment-immunity property is the reason this dimension is counted in code lines at all, so
	// assert it directly rather than trusting the scanner mode: a file that is ALL comments must
	// contribute zero.
	if got := countCodeLines(t, "probe.go", []byte("package p\n\n// just\n// comments\n")); got != 1 {
		t.Errorf("countCodeLines counted %d lines for a file whose only code is `package p`, want 1 "+
			"(comments must not count)", got)
	}
}

// TestUpdateFlagPreservesLedgerJustifications is the regression guard review M3 asked for and that the
// closure verification found missing.
//
// WHY IT MATTERS THAT THIS EXISTS. `-update-structural-budget` REWRITES the golden. The golden's value is
// not the twelve numbers — those are re-derivable in a second — it is the forty-odd hand-written lines
// explaining why each budget was raised, which is the only record of what a reviewer accepted and on what
// grounds. M3's finding was that the flag used to delete them silently; the fix (goldenSentinel +
// preservedComments + per-key re-attachment) was written, and then nothing exercised it. The whole writer
// path — updateGoldenTightenOnly, preservedComments, entryKeyOfGoldenLine, renderGolden — was reachable
// only by a human typing the flag, so its failure mode was rc=0 and a quietly emptier file.
//
// WHAT IS AND IS NOT COVERED. This drives the data functions against a temp file: preservation,
// attachment, idempotency, orphan rescue, and the missing-sentinel refusal. It does NOT drive
// updateGoldenTightenOnly's refusal branch, because that branch reports via t.Fatalf and a test cannot
// assert a Fatalf without dying with it. That branch is covered by mutation instead (plan §14 judgement 10
// runs the flag on an over-budget tree and checks it refuses), and saying so here is better than implying
// this test covers more than it does.
func TestUpdateFlagPreservesLedgerJustifications(t *testing.T) {
	entries := []budgetEntry{
		{kind: "pkg-files", entity: "internal/broker", value: 70},
		{kind: "type-methods", entity: "internal/broker.Broker", value: 279},
	}

	const orphanBlock = "# ORPHAN: a block attached to nothing, e.g. an entity that left the ledger."
	const brokerWhy = "# +8 the loopset split; every line buys an operator a stated cause."
	const typeWhy = "# Broker.Broker is the god type four audit lanes agreed not to split."

	build := func(sentinel string) string {
		var b strings.Builder
		b.WriteString("# header line the template owns\n")
		b.WriteString(sentinel + "\n")
		b.WriteString(orphanBlock + "\n")
		b.WriteString(brokerWhy + "\n")
		b.WriteString("pkg-files internal/broker 70\n")
		b.WriteString(typeWhy + "\n")
		b.WriteString("type-methods internal/broker.Broker 279\n")
		return b.String()
	}

	t.Run("preserves each block and keeps it attached to its own entry", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "golden.txt")
		if err := os.WriteFile(path, []byte(build(goldenSentinel)), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		orphans, byKey := preservedComments(t, path)
		out := renderGolden(entries, orphans, byKey)

		for _, want := range []string{orphanBlock, brokerWhy, typeWhy} {
			if !strings.Contains(out, want) {
				t.Errorf("regeneration DROPPED a hand-written line:\n  %s\n\n"+
					"That is M3 exactly: the flag CLAUDE.md calls a safe routine action deleting the only "+
					"record of why a budget was raised.", want)
			}
		}
		// Attachment, not merely survival: the justification must still sit immediately above the entry it
		// justifies, or a reader finds the loopset argument above the type-methods number.
		brokerAt := strings.Index(out, brokerWhy)
		entryAt := strings.Index(out, "pkg-files internal/broker 70")
		typeAt := strings.Index(out, typeWhy)
		typeEntryAt := strings.Index(out, "type-methods internal/broker.Broker 279")
		if brokerAt >= entryAt || entryAt >= typeAt || typeAt >= typeEntryAt {
			t.Errorf("blocks were preserved but DETACHED from their entries.\n%s\n\n"+
				"A justification above the wrong number is worse than a deleted one: it is a false "+
				"explanation a reader has no reason to doubt.", out)
		}
	})

	t.Run("is idempotent — a second regeneration changes nothing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "golden.txt")
		if err := os.WriteFile(path, []byte(build(goldenSentinel)), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		o1, k1 := preservedComments(t, path)
		first := renderGolden(entries, o1, k1)
		if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
			t.Fatalf("write first: %v", err)
		}
		o2, k2 := preservedComments(t, path)
		second := renderGolden(entries, o2, k2)
		if first != second {
			t.Errorf("regeneration is NOT idempotent — running the flag twice changes the file.\n\n"+
				"The first version of this mechanism grew the file by one duplicated header line per "+
				"invocation, without bound, because the boundary was a prefix match on a two-line "+
				"paragraph. An unbounded-growth bug in the routine maintenance command is invisible until "+
				"somebody reads the diff.\nfirst:\n%s\nsecond:\n%s", first, second)
		}
	})

	// origin: line-2 INDEPENDENT EXTERNAL REVIEW M6, second half. This subtest was named "refuses to
	// regenerate a golden whose sentinel is gone" and never called preservedComments. It asserted the
	// fixture lacked a sentinel and that a render with no preserved blocks carries no comments — both true
	// of any input, neither touching the guard. Deleting the real Fatalf left it green, and its own t.Log
	// handed the proof obligation to a manual mutation nobody was scheduled to run.
	//
	// The guard reports via t.Fatalf, which a test cannot assert without dying with it — so run it in a
	// SUBPROCESS. `go test -run <this> -update-structural-budget` against a mangled copy must exit non-zero
	// AND leave the file untouched, which is the property that matters: a refusal that still writes is not
	// a refusal.
	t.Run("refuses to regenerate a golden whose sentinel is gone", func(t *testing.T) {
		if os.Getenv("TETHER_BUDGET_SENTINEL_CHILD") == "1" {
			// Child: drive the real writer path at the path the parent seeded.
			path := os.Getenv("TETHER_BUDGET_SENTINEL_PATH")
			orphans, byKey := preservedComments(t, path) // t.Fatalf's here when the sentinel is missing
			_ = renderGolden(entries, orphans, byKey)
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "golden.txt")
		mangled := build("# ==== somebody edited the boundary line ====")
		if err := os.WriteFile(path, []byte(mangled), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read seed: %v", err)
		}

		cmd := exec.Command(os.Args[0],
			"-test.run", "^TestUpdateFlagPreservesLedgerJustifications$/^refuses_to_regenerate_a_golden_whose_sentinel_is_gone$",
			"-test.v")
		cmd.Env = append(os.Environ(),
			"TETHER_BUDGET_SENTINEL_CHILD=1",
			"TETHER_BUDGET_SENTINEL_PATH="+path)
		out, runErr := cmd.CombinedOutput()

		if runErr == nil {
			t.Errorf("the child process exited 0 on a golden with no sentinel — the guard did not fire.\n\n"+
				"That is the state external review M6 found: this subtest was passing without ever calling "+
				"preservedComments, so deleting the Fatalf cost nothing.\nchild output:\n%s", out)
		}
		if !strings.Contains(string(out), "contains no boundary line") {
			t.Errorf("the child failed, but not with the sentinel refusal — so this test is proving something "+
				"other than what it claims.\nchild output:\n%s", out)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read after: %v", err)
		}
		if string(after) != string(before) {
			t.Errorf("the guard refused BUT the file was modified. A refusal that still writes is not a "+
				"refusal — the hand-written justifications are exactly what must survive.\nbefore:\n%s\nafter:\n%s",
				before, after)
		}
	})
}

// Direct S6 caps apply below the debt threshold as well. AllOS counts both
// platform twins; a Linux-only measurement would silently buy Darwin slack.
// Keep these separate from the quantized, tighten-only debt ledger.
// origin: hot-update S6 plan §9.3 / §13 / T53
// gate-control: TestBrokerStructuralBudgetControls
var brokerPackageCaps = map[string]struct{ files, code int }{
	"internal/broker": {70, 18000}, "internal/cluster": {32, 4000},
	"cmd/tether": {57, 14000}, "internal/agent": {19, 8000},
	"internal/tunnel": {0, 6000}, "internal/brokerimage": {12, 4000},
	"internal/brokerimage/contractstub": {1, 700}, "internal/listenerimage": {6, 1200},
	"internal/brokermaint": {4, 900}, "internal/controlbudget": {4, 500},
	"internal/clusterio": {4, 900},
	// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM; first caps for the moved responsibilities.
	"internal/cluster/state": {15, 1800}, "internal/cluster/storelock": {1, 300},
	"internal/cluster/logbridge": {1, 200}, "internal/cluster/routecert": {1, 100},
	// First registration of the observation leaf: 2 files / 148 AllOS code
	// lines. Moving the source out of custody grants no unmeasured headroom.
	// origin: S10 internal review MAKETEST-GATES F8 / S7 plan §11.1
	"internal/drillobserve/infrastructure": {2, 149},
	// origin: hot-update S6 plan §9.3; first cap for the extracted business control owner.
	// Seventeen existing responsibilities and the moved drain join stay below 5000 code lines.
	"internal/brokercontrol": {17, 5000},
	// origin: hot-update S9 plan §10.1 / S9-SLIM — effective unit policy is a separate leaf.
	"internal/infrahost": {14, 4000}, "internal/infraunit": {3, 500},
	// origin: S10 internal review ARCH-REDS R12; extracted clock/cancellation adapter.
	"internal/infrahost/budgetclock": {1, 44},
}

type brokerStructure struct {
	files    map[string]int
	code     map[string]int
	methods  map[string]int
	fields   map[string]int
	fileCode map[string]int
	mainCode int
}

func measureBrokerStructure(t *testing.T, sources map[string]string) brokerStructure {
	t.Helper()
	result := brokerStructure{map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}, 0}
	for _, path := range brokerSortedSourcePaths(sources) {
		if !isProdGoFile(path) {
			continue
		}
		source := sources[path]
		file, err := brokerParse(path, source)
		if err != nil {
			t.Fatal(err)
		}
		pkg := filepath.ToSlash(filepath.Dir(path))
		result.files[pkg]++
		lines := countCodeLines(t, path, []byte(source))
		result.code[pkg] += lines
		result.fileCode[path] = lines
		if pkg == "cmd/tether" && !importsCobra(t, path, []byte(source)) {
			result.mainCode += lines
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil && len(fn.Recv.List) > 0 {
				result.methods[pkg+"."+receiverTypeName(fn.Recv.List[0].Type)]++
			}
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.TYPE {
				continue
			}
			for _, spec := range group.Specs {
				typ := spec.(*ast.TypeSpec)
				structure, ok := typ.Type.(*ast.StructType)
				if !ok {
					continue
				}
				fields := 0
				for _, field := range structure.Fields.List {
					n := len(field.Names)
					if n == 0 {
						n = 1
					}
					fields += n
				}
				// Platform variants describe the same type. The largest shape is the cap;
				// methods, in contrast, remain the historical AllOS declaration union.
				key := pkg + "." + typ.Name.Name
				if fields > result.fields[key] {
					result.fields[key] = fields
				}
			}
		}
	}
	return result
}

func checkBrokerStructure(result brokerStructure) error {
	for pkg, cap := range brokerPackageCaps {
		if cap.files > 0 && result.files[pkg] > cap.files {
			return fmt.Errorf("%s: production files %d exceed %d", pkg, result.files[pkg], cap.files)
		}
		if result.code[pkg] >= cap.code {
			return fmt.Errorf("%s: code %d must be below %d", pkg, result.code[pkg], cap.code)
		}
	}
	for key, cap := range map[string]int{"internal/broker.Broker": 285, "internal/agent.Agent": 125, "internal/execsession.Runtime": 39} {
		if result.methods[key] > cap {
			return fmt.Errorf("%s: methods %d exceed %d", key, result.methods[key], cap)
		}
	}
	if result.mainCode >= 1300 {
		return fmt.Errorf("cmd/tether: non-CLI code %d must be below 1300", result.mainCode)
	}
	for key, n := range result.methods {
		// Existing debt has its exact ratchet above. New types get no blanket
		// Broker/Agent exception just because they share a package directory.
		if key == "internal/broker.Broker" || key == "internal/agent.Agent" {
			continue
		}
		if n >= 100 {
			return fmt.Errorf("%s: receiver methods %d must be below 100", key, n)
		}
		for _, pkg := range []string{"internal/brokerimage", "internal/listenerimage", "internal/brokermaint", "internal/controlbudget", "internal/clusterio"} {
			if strings.HasPrefix(key, pkg+".") && n >= 40 {
				return fmt.Errorf("%s: image leaf methods %d must be below 40", key, n)
			}
		}
	}
	for key, n := range result.fields {
		for _, pkg := range []string{"internal/brokerimage", "internal/listenerimage", "internal/brokermaint", "internal/controlbudget", "internal/clusterio"} {
			if strings.HasPrefix(key, pkg+".") && n >= 100 {
				return fmt.Errorf("%s: image fields %d must be below 100", key, n)
			}
		}
	}
	return nil
}

const legacyFileCodeLedger = "testdata/legacy_file_code_ceilings.tsv"

// The lead's per-file rule applies 700 to files created after cab90d1.
// Existing paths have their own decreasing ceiling, including small files;
// treating every old file as a blanket exemption would allow unbounded growth.
// Entries were selected with git cat-file -e cab90d1:<path>, then measured
// with countCodeLines. Removing a file requires removing its dead ledger row.
// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E lead decision
func readLegacyFileCodeCeilings(path string) (map[string]int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	ceilings := map[string]int{}
	for line, row := range strings.Split(string(raw), "\n") {
		if row == "" || strings.HasPrefix(row, "#") {
			continue
		}
		fields := strings.Split(row, "\t")
		if len(fields) != 2 || !isProdGoFile(fields[0]) || filepath.ToSlash(filepath.Clean(fields[0])) != fields[0] ||
			(!strings.HasPrefix(fields[0], "internal/") && !strings.HasPrefix(fields[0], "cmd/")) {
			return nil, fmt.Errorf("%s:%d: invalid legacy file row", path, line+1)
		}
		n, err := strconv.Atoi(fields[1])
		if err != nil || n < 1 {
			return nil, fmt.Errorf("%s:%d: invalid code ceiling", path, line+1)
		}
		if _, duplicate := ceilings[fields[0]]; duplicate {
			return nil, fmt.Errorf("%s:%d: duplicate legacy path %s", path, line+1, fields[0])
		}
		ceilings[fields[0]] = n
	}
	if len(ceilings) == 0 {
		return nil, fmt.Errorf("%s: empty legacy file ledger", path)
	}
	return ceilings, nil
}

// Use the same predicate for the repository and synthetic/on-disk controls.
// A stale entry is red even if every live file is within its ceiling.
// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E lead decision
func checkBrokerFileCode(actual, legacy map[string]int) error {
	var failures []string
	for path, n := range actual {
		ceiling, old := legacy[path]
		if !old {
			ceiling = 700
		}
		if n > ceiling {
			failures = append(failures, fmt.Sprintf("%s: file code %d exceeds %d (legacy=%t)", path, n, ceiling, old))
		}
	}
	for path := range legacy {
		if _, live := actual[path]; !live {
			failures = append(failures, path+": stale legacy file ceiling")
		}
	}
	sort.Strings(failures)
	if len(failures) != 0 {
		return fmt.Errorf("%s", strings.Join(failures, "\n"))
	}
	return nil
}

// Hard caps cannot be widened by editing the debt golden. Quantized package
// values retain S5's ceilings even when an updater is used during integration.
func checkBrokerGoldenCaps(golden map[string]int) error {
	for key, cap := range map[string]int{
		"pkg-files internal/broker": 70, "pkg-files internal/cluster": 32, "pkg-files cmd/tether": 57,
		"type-methods internal/broker.Broker": 285, "type-methods internal/agent.Agent": 125,
		"pkg-code-lines internal/broker": 16000, "pkg-code-lines internal/agent": 6000,
		"pkg-code-lines internal/cluster": 2000, "pkg-code-lines internal/tunnel": 4000,
		"pkg-code-lines cmd/tether": 12000, "main-noncli-code-lines cmd/tether": 1000,
	} {
		if n, ok := golden[key]; ok && n > cap {
			return fmt.Errorf("%s: golden %d exceeds frozen ceiling %d", key, n, cap)
		}
	}
	return nil
}

// origin: hot-update S6 plan §9.3 / §13 / T53
func TestBrokerStructuralCapsAllOS(t *testing.T) {
	root := repoRoot(t)
	result := measureBrokerStructure(t, brokerGateSources(t, root))
	for _, pkg := range []string{"internal/broker", "internal/cluster", "cmd/tether"} {
		if result.files[pkg] != brokerPackageCaps[pkg].files {
			t.Errorf("%s: frozen production count got %d want %d", pkg, result.files[pkg], brokerPackageCaps[pkg].files)
		}
	}
	if result.methods["internal/agent.Agent"] != 125 || result.methods["internal/execsession.Runtime"] != 39 {
		t.Error("Agent/Runtime method inventory drifted")
	}
	if err := checkBrokerStructure(result); err != nil {
		t.Error(err)
	}
	legacy, err := readLegacyFileCodeCeilings(filepath.Join(root, "test/architecture", legacyFileCodeLedger))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerFileCode(result.fileCode, legacy); err != nil {
		t.Error(err)
	}
	golden, err := readGolden(t, filepath.Join(root, "test/architecture", structuralBudgetGolden))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerGoldenCaps(golden); err != nil {
		t.Error(err)
	}
	// Print every breached package cap even though the predicate returns one
	// error. Map iteration must not hide a second owner's integration debt.
	// origin: hot-update S6 plan §9.3 / T53
	var breached []string
	for pkg, cap := range brokerPackageCaps {
		if cap.files > 0 && result.files[pkg] > cap.files || result.code[pkg] >= cap.code {
			breached = append(breached, fmt.Sprintf("%s: files=%d/%d code=%d/<%d", pkg, result.files[pkg], cap.files, result.code[pkg], cap.code))
		}
	}
	sort.Strings(breached)
	for _, row := range breached {
		t.Log(row)
	}
	for _, pkg := range []string{"internal/brokercontrol", "internal/brokerimage", "internal/clusterio"} {
		t.Logf("%s: files=%d/%d code=%d/<%d", pkg, result.files[pkg], brokerPackageCaps[pkg].files, result.code[pkg], brokerPackageCaps[pkg].code)
	}
	t.Logf("AllOS broker=%d files/%d methods/%d code; agent=%d files/%d methods; main=%d non-CLI code", result.files["internal/broker"], result.methods["internal/broker.Broker"], result.code["internal/broker"], result.files["internal/agent"], result.methods["internal/agent.Agent"], result.mainCode)
}

// origin: hot-update S6 plan §11 / T53
func TestBrokerStructuralBudgetControls(t *testing.T) {

	frozen := map[string]struct{ files, code int }{
		"internal/broker": {70, 18000}, "internal/cluster": {32, 4000}, "cmd/tether": {57, 14000}, "internal/agent": {19, 8000}, "internal/tunnel": {0, 6000},
		"internal/brokerimage": {12, 4000}, "internal/brokerimage/contractstub": {1, 700}, "internal/listenerimage": {6, 1200}, "internal/brokermaint": {4, 900}, "internal/controlbudget": {4, 500}, "internal/clusterio": {4, 900},
		"internal/brokercontrol": {17, 5000},
		"internal/infrahost":     {14, 4000}, "internal/infraunit": {3, 500},
		"internal/infrahost/budgetclock": {1, 44},
		"internal/cluster/state":         {15, 1800}, "internal/cluster/storelock": {1, 300},
		"internal/cluster/logbridge": {1, 200}, "internal/cluster/routecert": {1, 100},
		"internal/drillobserve/infrastructure": {2, 149},
	}
	if len(frozen) != len(brokerPackageCaps) {
		t.Fatal("S6 package cap inventory drifted")
	}
	for pkg, cap := range frozen {
		if brokerPackageCaps[pkg] != cap {
			t.Fatalf("%s frozen cap widened or changed", pkg)
		}
	}
	sources := map[string]string{"internal/brokerimage/x_linux.go": "//go:build linux\npackage brokerimage; type owner struct{};func(o *owner) Freeze(){}", "internal/brokerimage/x_other.go": "//go:build !linux\npackage brokerimage;func(o *owner) Thaw(){}"}
	measured := measureBrokerStructure(t, sources)
	if measured.files["internal/brokerimage"] != 2 || measured.methods["internal/brokerimage.owner"] != 2 {
		t.Fatal("AllOS union lost excluded files/methods")
	}
	if err := checkBrokerStructure(measured); err != nil {
		t.Fatal(err)
	}
	for pkg, cap := range brokerPackageCaps {
		if cap.files > 0 {
			old := measured.files[pkg]
			measured.files[pkg] = cap.files + 1
			if checkBrokerStructure(measured) == nil {
				t.Fatalf("file cap escaped %s", pkg)
			}
			measured.files[pkg] = old
		}
		old := measured.code[pkg]
		measured.code[pkg] = cap.code
		if checkBrokerStructure(measured) == nil {
			t.Fatalf("code cap escaped %s", pkg)
		}
		measured.code[pkg] = old
	}
	for key, cap := range map[string]int{"internal/agent.Agent": 125, "internal/broker.Broker": 285, "internal/execsession.Runtime": 39, "internal/brokerimage.owner": 39, "internal/x.owner": 99} {
		old := measured.methods[key]
		measured.methods[key] = cap + 1
		if checkBrokerStructure(measured) == nil {
			t.Fatalf("method cap escaped %s", key)
		}
		measured.methods[key] = old
	}
	measured.fields["internal/brokerimage.Owner"] = 100
	if checkBrokerStructure(measured) == nil {
		t.Fatal("image field cap escaped")
	}
	delete(measured.fields, "internal/brokerimage.Owner")
	measured.mainCode = 1300
	if checkBrokerStructure(measured) == nil {
		t.Fatal("main cap escaped")
	}
	measured.mainCode = 0
	measured.fileCode["internal/brokerimage/x_linux.go"] = 701
	if checkBrokerFileCode(measured.fileCode, nil) == nil {
		t.Fatal("file code cap escaped")
	}
	// Old files may shrink and new files may reach exactly 700. A legacy
	// path is not permission for a different file to borrow its ceiling.
	// origin: hot-update S6 plan §9.3 / T53 / GATES-S6-E lead decision
	legacy := map[string]int{"internal/legacy/owner.go": 750}
	for _, n := range []int{749, 750} {
		if err := checkBrokerFileCode(map[string]int{"internal/legacy/owner.go": n, "internal/new/owner.go": 700}, legacy); err != nil {
			t.Fatal(err)
		}
	}
	for _, actual := range []map[string]int{
		{"internal/legacy/owner.go": 751},
		{"internal/legacy/owner.go": 750, "internal/new/owner.go": 701},
		{"internal/new/owner.go": 700},
		{"internal/legacy/owner.go": 750, "internal/new/alias.go": 750},
	} {
		if checkBrokerFileCode(actual, legacy) == nil {
			t.Fatalf("legacy/new/stale file code control escaped: %v", actual)
		}
	}
	t.Run("file_code_on_disk", func(t *testing.T) {
		root := t.TempDir()
		for _, dir := range []string{"cmd", "internal/legacy", "internal/new"} {
			if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
				t.Fatal(err)
			}
		}
		write := func(path string, lines int) {
			t.Helper()
			source := "package owner\nvar (\n" + strings.Repeat("_ = 1\n", lines-3) + ")\n"
			if err := os.WriteFile(filepath.Join(root, path), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
		}
		check := func() error {
			return checkBrokerFileCode(measureBrokerStructure(t, brokerGateSources(t, root)).fileCode, legacy)
		}
		write("internal/legacy/owner.go", 750)
		write("internal/new/owner.go", 700)
		if err := check(); err != nil {
			t.Fatal(err)
		}
		write("internal/legacy/owner.go", 751)
		if check() == nil {
			t.Fatal("on-disk legacy growth escaped")
		}
		write("internal/legacy/owner.go", 749)
		write("internal/new/owner.go", 701)
		if check() == nil {
			t.Fatal("on-disk new file growth escaped")
		}
		write("internal/new/owner.go", 700)
		if err := os.Remove(filepath.Join(root, "internal/legacy/owner.go")); err != nil {
			t.Fatal(err)
		}
		if check() == nil {
			t.Fatal("on-disk stale legacy entry escaped")
		}
		ledger := filepath.Join(root, "ceilings.tsv")
		for _, raw := range []string{"", "internal/legacy/owner.go\t0\n", "internal/legacy/owner.go\t750\ninternal/legacy/owner.go\t750\n", "../owner.go\t750\n", "test/owner.go\t750\n", "internal-other/owner.go\t750\n", "cmd-other/owner.go\t750\n"} {
			if err := os.WriteFile(ledger, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readLegacyFileCodeCeilings(ledger); err == nil {
				t.Fatalf("malformed ledger accepted: %q", raw)
			}
		}
		if err := os.WriteFile(ledger, []byte("# frozen legacy path\ninternal/legacy/owner.go\t750\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if parsed, err := readLegacyFileCodeCeilings(ledger); err != nil || parsed["internal/legacy/owner.go"] != 750 {
			t.Fatalf("valid ledger: %v %v", parsed, err)
		}
	})
	for key, ceiling := range map[string]int{"pkg-files internal/broker": 70, "pkg-code-lines internal/broker": 16000, "type-methods internal/agent.Agent": 125, "main-noncli-code-lines cmd/tether": 1000} {
		if checkBrokerGoldenCaps(map[string]int{key: ceiling}) != nil || checkBrokerGoldenCaps(map[string]int{key: ceiling + 1}) == nil {
			t.Fatalf("golden ceiling control escaped %s", key)
		}
	}
	// Use a real family's copied source plus a disallowed AllOS file on disk.
	root := t.TempDir()
	for _, dir := range []string{"internal/brokerimage", "cmd"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/brokerimage/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 13; i++ {
		if err := os.WriteFile(filepath.Join(root, "internal/brokerimage", fmt.Sprintf("owner%d_other.go", i)), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if checkBrokerStructure(measureBrokerStructure(t, brokerGateSources(t, root))) == nil {
		t.Fatal("on-disk production file growth escaped")
	}
}

// Retain the S6 predicate and every legacy ceiling. The S9 leaf ceiling is
// additional: smaller existing package caps still win, never net package slack.
// origin: hot-update S9 plan §10.1 / T83
func checkInfraStructure(result brokerStructure) error {
	if err := checkBrokerStructure(result); err != nil {
		return err
	}
	for _, leaf := range infraLeafRoots {
		pkg := "internal/" + leaf
		ceiling := 2000
		if leaf == "infrahost" || leaf == "infraupgrade" {
			ceiling = 4000
		}
		if result.files[pkg] > 20 || result.code[pkg] >= ceiling {
			return fmt.Errorf("%s: S9 leaf files=%d/20 code=%d/<%d", pkg, result.files[pkg], result.code[pkg], ceiling)
		}
		for key, n := range result.methods {
			if strings.HasPrefix(key, pkg+".") && n > 40 {
				return fmt.Errorf("%s: S9 owner methods=%d/40", key, n)
			}
		}
	}
	if result.mainCode >= 900 {
		return fmt.Errorf("cmd/tether: non-CLI code %d crosses frozen 900 boundary", result.mainCode)
	}
	if n := result.methods["internal/brokercontrol.ClusterAdmin"]; n > 86 {
		return fmt.Errorf("ClusterAdmin methods=%d/86", n)
	}
	return nil
}

// origin: hot-update S9 plan §10.1 / T83
// gate-control: TestInfraStructuralBudgetControls
func TestInfraStructuralBudgetsAllOS(t *testing.T) {
	root := repoRoot(t)
	result := measureBrokerStructure(t, brokerGateSources(t, root))
	for _, pkg := range []string{"internal/broker", "internal/cluster", "cmd/tether"} {
		if result.files[pkg] != brokerPackageCaps[pkg].files {
			t.Errorf("%s exact file inventory drift: %d", pkg, result.files[pkg])
		}
	}
	for key, n := range map[string]int{"internal/agent.Agent": 125, "internal/execsession.Runtime": 39} {
		if result.methods[key] != n {
			t.Errorf("%s exact method inventory drift: %d", key, result.methods[key])
		}
	}
	if err := checkInfraStructure(result); err != nil {
		t.Error(err)
	}
	legacy, err := readLegacyFileCodeCeilings(filepath.Join(root, "test/architecture", legacyFileCodeLedger))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerFileCode(result.fileCode, legacy); err != nil {
		t.Error(err)
	}
	for _, pkg := range []string{"internal/broker", "internal/agent", "cmd/tether", "internal/cluster", "internal/brokercontrol", "internal/execsession", "internal/tunnel", "internal/cli"} {
		t.Logf("entry/after: %s files=%d code=%d (P34 changes no production source; delta=0)", pkg, result.files[pkg], result.code[pkg])
	}
	golden, err := readGolden(t, filepath.Join(root, "test/architecture", structuralBudgetGolden))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerGoldenCaps(golden); err != nil {
		t.Error(err)
	}
	if n := golden["main-noncli-code-lines cmd/tether"]; n > 800 {
		t.Errorf("non-CLI golden widened to %d", n)
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraStructuralBudgetControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{"internal/infraattach/owner_other.go": "package owner\n"})
	read := func() brokerStructure { return measureBrokerStructure(t, brokerGateSources(t, root)) }
	good := read()
	if err := checkInfraStructure(good); err != nil {
		t.Fatal(err)
	}
	path := "internal/infraattach/owner_other.go"
	transferWriteFixture(t, root, path, "package owner\n"+strings.Repeat("// harmless comment\n", 800))
	if read().fileCode[path] != 1 {
		t.Fatal("comments counted as code")
	}
	transferWriteFixture(t, root, path, "package owner\n"+strings.Repeat("var _ = 1\n", 700))
	measured := read()
	if measured.fileCode[path] != 701 || checkBrokerFileCode(measured.fileCode, map[string]int{}) == nil {
		t.Fatal("on-disk 701 code lines escaped")
	}
	legacy := map[string]int{path: 700}
	if checkBrokerFileCode(measured.fileCode, legacy) == nil {
		t.Fatal("legacy +1 escaped")
	}
	for _, leaf := range infraLeafRoots {
		pkg := "internal/" + leaf
		before := good.code[pkg]
		ceiling := 2000
		if leaf == "infrahost" || leaf == "infraupgrade" {
			ceiling = 4000
		}
		good.code[pkg] = ceiling
		if checkInfraStructure(good) == nil {
			t.Fatalf("code ceiling escaped %s", pkg)
		}
		good.code[pkg] = before
		good.files[pkg] = 21
		if checkInfraStructure(good) == nil {
			t.Fatalf("file ceiling escaped %s", pkg)
		}
		good.files[pkg] = 0
		good.methods[pkg+".Owner"] = 41
		if checkInfraStructure(good) == nil {
			t.Fatalf("owner method ceiling escaped %s", pkg)
		}
		delete(good.methods, pkg+".Owner")
	}
	good.mainCode = 900
	if checkInfraStructure(good) == nil {
		t.Fatal("non-CLI quantum boundary escaped")
	}
	// Shared controls retain exact existing counts and every small-file ceiling.
	t.Run("existing", TestBrokerStructuralBudgetControls)
}

// S10 first registrations (plan §12.1): AllOS production totals of the new
// leaves, independent of the S6/S7/S9 tables so none of them becomes headroom
// for fullstack growth. Most S10 production code lives under test/, which the
// S6 reader never scans; this gate measures it with the same counter. Every
// package directory under an S10 root must be registered, or a responsibility
// moved into a new subdirectory would escape every cap. The existing direct caps
// (Broker, cluster, cmd/tether, Agent exactly 125, Runtime, legacy per-file
// ceilings and the tighten-only golden) are re-enabled over the same union.
// origin: hot-update S10 plan §12.1 / P28
// gate-control: TestFullstackStructuralBudgetControls
var fullstackPackageCaps = map[string]struct{ files, code int }{
	"internal/storewitness":              {2, 1000},
	"test/hotupdate/acceptance":          {10, 6000},
	"test/hotupdate/acceptance/contract": {3, 1800},
	"test/fullstackharness":              {14, 8000},
}

// Registered S10 packages without a plan-sized total: each file still obeys 700.
var fullstackUncappedPackages = []string{"test/hotupdate/acceptance/contractstub", "test/hotupdate/evidencecmd", "test/fullstackharness/cmd/supervise"}

// fullstackStructureSources is the S6 production view plus the S10 roots under
// test/. Other test helpers keep their own history and are not re-sized here.
func fullstackStructureSources(tree fullstackTree) map[string]string {
	sources := map[string]string{}
	for path, source := range fullstackProductionGo(tree) {
		if strings.HasPrefix(path, "internal/") || strings.HasPrefix(path, "cmd/") || fullstackUnder(path, fullstackRoots...) {
			sources[path] = source
		}
	}
	return sources
}

// The method counter keys a method by its receiver's spelled type name. A
// local alias (type agentView = Agent) is a legal receiver for Agent's own
// method set, so every method declared through it would vanish from the exact
// 125 while the real set grows. Capped owners therefore admit no such alias.
// origin: hot-update S10 plan §12.1 Agent 124/126 / P28
var fullstackCappedReceivers = map[string]string{"internal/agent": "Agent", "internal/execsession": "Runtime", "internal/broker": "Broker"}

func checkFullstackReceiverAliases(sources map[string]string) error {
	for _, path := range brokerSortedSourcePaths(sources) {
		owner, capped := fullstackCappedReceivers[fullstackDir(path)]
		if !capped || !strings.Contains(sources[path], "= "+owner) && !strings.Contains(sources[path], "= *"+owner) {
			continue
		}
		file, err := brokerParse(path, sources[path])
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.TYPE {
				continue
			}
			for _, spec := range group.Specs {
				alias := spec.(*ast.TypeSpec)
				if alias.Assign.IsValid() && receiverTypeName(alias.Type) == owner {
					return fmt.Errorf("%s: alias %s hides %s's method count", path, alias.Name.Name, owner)
				}
			}
		}
	}
	return nil
}

func checkFullstackStructure(result brokerStructure) error {
	if err := checkBrokerStructure(result); err != nil {
		return err
	}
	if n := result.methods["internal/agent.Agent"]; n != 125 {
		return fmt.Errorf("internal/agent.Agent must keep exactly 125 methods, has %d", n)
	}
	for _, pkg := range brokerSortedSourcePaths(fullstackPackageCapsAsPaths()) {
		cap := fullstackPackageCaps[pkg]
		if result.files[pkg] == 0 {
			return fmt.Errorf("%s: empty S10 package root", pkg)
		}
		if result.files[pkg] > cap.files || result.code[pkg] >= cap.code {
			return fmt.Errorf("%s: S10 production files %d/%d, code %d/<%d", pkg, result.files[pkg], cap.files, result.code[pkg], cap.code)
		}
	}
	for _, pkg := range fullstackUncappedPackages {
		if result.files[pkg] == 0 {
			return fmt.Errorf("%s: empty S10 package root", pkg)
		}
	}
	for _, pkg := range brokerSortedSourcePaths(fileCountsAsPaths(result.files)) {
		_, capped := fullstackPackageCaps[pkg]
		if fullstackUnder(pkg+"/", fullstackRoots...) && !capped && !slices.Contains(fullstackUncappedPackages, pkg) {
			return fmt.Errorf("%s: unregistered S10 package directory", pkg)
		}
	}
	return nil
}

func fullstackPackageCapsAsPaths() map[string]string {
	paths := map[string]string{}
	for pkg := range fullstackPackageCaps {
		paths[pkg] = ""
	}
	return paths
}

func fileCountsAsPaths(files map[string]int) map[string]string {
	paths := map[string]string{}
	for pkg, n := range files {
		if n > 0 {
			paths[pkg] = ""
		}
	}
	return paths
}

// origin: hot-update S10 plan §12.1 / P28
func TestFullstackStructuralCapsAllOS(t *testing.T) {
	tree := fullstackRepoTree(t)
	sources := fullstackStructureSources(tree)
	if err := checkFullstackReceiverAliases(sources); err != nil {
		t.Error(err)
	}
	result := measureBrokerStructure(t, sources)
	if err := checkFullstackStructure(result); err != nil {
		t.Error(err)
	}
	legacy, err := readLegacyFileCodeCeilings(filepath.Join(tree.root, "test/architecture", legacyFileCodeLedger))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerFileCode(result.fileCode, legacy); err != nil {
		t.Error(err)
	}
	golden, err := readGolden(t, filepath.Join(tree.root, "test/architecture", structuralBudgetGolden))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerGoldenCaps(golden); err != nil {
		t.Error(err)
	}
	for _, pkg := range brokerSortedSourcePaths(fullstackPackageCapsAsPaths()) {
		cap := fullstackPackageCaps[pkg]
		t.Logf("S10 %s: files=%d/%d code=%d/<%d", pkg, result.files[pkg], cap.files, result.code[pkg], cap.code)
	}
	for _, pkg := range fullstackUncappedPackages {
		t.Logf("S10 %s: files=%d code=%d (registered; 700 per file)", pkg, result.files[pkg], result.code[pkg])
	}
	t.Logf("AllOS Agent=%d methods/%d files; Runtime=%d; Broker=%d files/%d methods/%d code; main non-CLI=%d",
		result.methods["internal/agent.Agent"], result.files["internal/agent"], result.methods["internal/execsession.Runtime"],
		result.files["internal/broker"], result.methods["internal/broker.Broker"], result.code["internal/broker"], result.mainCode)
}

// Every mutation edits a copied real S10 root (or the synthetic Agent/Runtime
// owners) on disk and re-measures it with the structural gate's own counter.
// origin: hot-update S10 plan §10.2 P28 / §12.1
func TestFullstackStructuralBudgetControls(t *testing.T) {
	frozen := map[string]struct{ files, code int }{
		"internal/storewitness": {2, 1000}, "test/hotupdate/acceptance": {10, 6000},
		"test/hotupdate/acceptance/contract": {3, 1800}, "test/fullstackharness": {14, 8000},
	}
	if len(frozen) != len(fullstackPackageCaps) {
		t.Fatal("S10 package cap registry drifted")
	}
	for pkg, cap := range frozen {
		if fullstackPackageCaps[pkg] != cap {
			t.Fatalf("%s: frozen S10 cap changed", pkg)
		}
	}
	real := fullstackRepoTree(t)
	root := fullstackFixture(t, real, fullstackRoots...)
	legacy, err := readLegacyFileCodeCeilings(filepath.Join(real.root, "test/architecture", legacyFileCodeLedger))
	if err != nil {
		t.Fatal(err)
	}
	// agent.go carries the exact 125-method inventory and is padded to its real
	// per-file legacy ceiling, so one more code line is the "legacy +1" defect.
	agent := "package agent\n\ntype Agent struct{}\n"
	for i := 0; i < 125; i++ {
		agent += fmt.Sprintf("func (*Agent) Method%d() {}\n", i)
	}
	agent += strings.Repeat("var _ = 1\n", legacy["internal/agent/agent.go"]-127)
	transferWriteFixture(t, root, "internal/agent/agent.go", agent)
	transferWriteFixture(t, root, "internal/execsession/runtime.go", "package execsession\n\ntype Runtime struct{}\n")
	ceilings := map[string]int{"internal/agent/agent.go": legacy["internal/agent/agent.go"]}
	measure := func() brokerStructure {
		return measureBrokerStructure(t, fullstackStructureSources(readFullstackTree(t, root)))
	}
	check := func() error {
		if err := checkFullstackReceiverAliases(fullstackStructureSources(readFullstackTree(t, root))); err != nil {
			return err
		}
		result := measure()
		if err := checkFullstackStructure(result); err != nil {
			return err
		}
		return checkBrokerFileCode(result.fileCode, ceilings)
	}
	if got := measure().fileCode["internal/agent/agent.go"]; got != ceilings["internal/agent/agent.go"] {
		t.Fatalf("agent fixture measures %d code lines, want its ceiling %d", got, ceilings["internal/agent/agent.go"])
	}
	// Fill the file caps to their limit: the limit itself is green, one more is red.
	good := measure()
	for pkg, cap := range fullstackPackageCaps {
		for i := good.files[pkg]; i < cap.files; i++ {
			transferWriteFixture(t, root, fmt.Sprintf("%s/filler%d_other.go", pkg, i), "package "+fullstackBase(pkg)+"\n")
		}
	}
	if err := check(); err != nil {
		t.Fatalf("S10 caps at their limits refused: %v", err)
	}
	full := measure()
	witnessPad := fullstackPackageCaps["internal/storewitness"].code - full.code["internal/storewitness"]
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: "internal/agent/agent.go", old: "func (*Agent) Method124() {}\n", new: "", want: "exactly 125 methods, has 124"},
		fullstackMutation{path: "internal/agent/agent.go", old: "type Agent struct{}\n", new: "type Agent struct{}\nfunc (*Agent) Method125() {}\n", want: "internal/agent.Agent: methods 126"},
		// A 126th method declared through an alias receiver must not read as 125.
		fullstackMutation{path: "internal/agent/view_other.go", new: "package agent\n\ntype agentView = Agent\n\nfunc (*agentView) Hidden() {}\n", want: "alias agentView hides Agent's method count"},
		fullstackMutation{path: "internal/execsession/runtime.go", new: "\ntype runtimeView = *Runtime\n", want: "alias runtimeView hides Runtime's method count"},
		fullstackMutation{path: "internal/execsession/runtime.go", new: strings.Repeat("func (*Runtime) M() {}\n", 40), want: "internal/execsession.Runtime: methods 40"},
		fullstackMutation{path: "internal/storewitness/filler_extra_other.go", new: "package storewitness\n", want: "internal/storewitness: S10 production files 3/2"},
		fullstackMutation{path: "test/fullstackharness/filler_extra_other.go", new: "package fullstackharness\n", want: "test/fullstackharness: S10 production files 15/14"},
		fullstackMutation{path: "test/hotupdate/acceptance/filler_extra_other.go", new: "package acceptance\n", want: "test/hotupdate/acceptance: S10 production files 11/10"},
		fullstackMutation{path: "test/hotupdate/acceptance/contract/filler_extra_other.go", new: "package contract\n", want: "test/hotupdate/acceptance/contract: S10 production files 4/3"},
		fullstackMutation{path: "internal/storewitness/witness.go", new: strings.Repeat("\nvar _ = 1", witnessPad), want: "internal/storewitness: S10 production files 2/2, code 1000/<1000"},
		fullstackMutation{path: "test/fullstackharness/relocated/owner.go", new: "package relocated\n", want: "unregistered S10 package directory"},
		fullstackMutation{path: "test/hotupdate/acceptance/contractstub/stub.go", remove: true, want: "contractstub: empty S10 package root"},
		fullstackMutation{path: "test/hotupdate/evidencecmd/oversized.go", new: "package main\n" + strings.Repeat("var _ = 1\n", 700), want: "oversized.go: file code 701 exceeds 700"},
		fullstackMutation{path: "internal/agent/agent.go", new: "var _ = 1\n", want: "agent.go: file code " + strconv.Itoa(ceilings["internal/agent/agent.go"]+1)},
	)
}
