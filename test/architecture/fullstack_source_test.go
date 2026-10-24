package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// fullstack_source_test.go — S10's new-tool manifest gate: every new Go,
// Python, shell and tool file is found, sized, named and anchored, and every S10
// gate keeps a live same-file control on the unfiltered central entry point.
//
// Plan §12.1: the S6 per-file rule reads only internal/ and cmd/ production Go,
// so the fullstack harness, the pure judges, the Python fixtures and the drill
// wrappers — most of S10 — sat outside every size gate. This gate scans the
// union of four real roots rather than a hand list: the frozen §10 manifest,
// P29's actual-ownership ledger, the directories S10 created, and S10's
// responsibility-named families. A file that predates S10 keeps its own history;
// it is exempt only through the frozen baseline below, and a baseline entry the
// manifest no longer names is stale rather than a quiet allowance.
//
// origin: hot-update S10 plan §10 / §12.1 / §12.2 naming / P28
// gate-control: TestFullstackGateControls

// Manifest code destinations that existed at the plan's static snapshot faeab41
// (checked with git cat-file -e when this gate landed). They keep their own
// legacy gates; every entry must remain a manifest path.
var fullstackPreexistingCode = []string{
	"cmd/tether/error_code_coverage_test.go", "internal/agent/agent.go",
	"internal/drillobserve/custody.go", "internal/drillobserve/custody_linux.go",
	"internal/drillobserve/custody_linux_test.go", "internal/drillobserve/custody_other.go",
	"internal/proto/wire_inventory_test.go", "internal/storage/current.go",
	"test/architecture/build_tags_test.go", "test/architecture/dataplane_lifetime_test.go",
	"test/architecture/image_field_inventory_test.go", "test/architecture/layering_test.go",
	"test/architecture/legacy_ctx_background_sites.go", "test/architecture/simcluster_gate_set_test.go",
	"test/architecture/structural_budget_test.go", "test/determinism/enum_switch_default_test.go",
	"test/determinism/leak_assert_shape_test.go", "test/determinism/test_inventory_test.go",
	"test/e2e/all_phases_test.go", "test/e2e/parallel/inventory_test.go",
	"test/e2e/parallel/partition_coverage_test.go", "test/e2e/parallel/shard.go",
	"test/simcluster/drills/lib/caddy.sh", "test/simcluster/drills/lib/flow_fixture.py",
	"test/simcluster/drills/lib/flow_fixture_peer.py", "test/simcluster/drills/lib/flow_fixture_transport.sh",
	"test/simcluster/drills/lib/flow_observation.py", "test/simcluster/drills/lib/flow_observer_service.py",
	"test/simcluster/drills/lib/logs.sh", "test/simcluster/lib/manifest.sh", "test/simcluster/lib/stage.sh",
	"test/simcluster/run-drills.sh", "test/simcluster/tests/run-all.sh",
}

// The S10 gate files and their same-file controls. The registry proves each one
// is documented, anchored and live; it never substitutes for those controls.
var fullstackGateControls = map[string]string{
	"fullstack_contract_test.go":  "TestFullstackContractGateControls",
	"fullstack_docs_test.go":      "TestFullstackDocumentationReceiptControls",
	"fullstack_source_test.go":    "TestFullstackGateControls",
	"layering_test.go":            "TestFullstackLayeringControls",
	"structural_budget_test.go":   "TestFullstackStructuralBudgetControls",
	"build_tags_test.go":          "TestFullstackBuildTagControls",
	"dataplane_lifetime_test.go":  "TestFullstackDataplaneLifetimeControls",
	"simcluster_gate_set_test.go": "TestFullstackHermeticGateRegistrationControls",
}

var (
	fullstackOriginRe  = regexp.MustCompile(`hot-update S10 plan (§[0-9]+(?:\.[0-9]+)*)([^;\n]*)`)
	fullstackHeadingRe = regexp.MustCompile(`^(?:## §([0-9]+)|### ([0-9]+\.[0-9]+)) `)
	fullstackSectionRe = regexp.MustCompile(`§[0-9]+(?:\.[0-9]+)*`)
	fullstackPackageRe = regexp.MustCompile(`\bP[0-9]{2}\b`)
	fullstackForeignRe = regexp.MustCompile(`\bS[0-9]+\b|\barchitecture\b|\brequirements\b|\btesting-standards\b|\bREADME\b|docs/`)
	fullstackHeaderRe  = regexp.MustCompile(`^(?://|#) ([A-Za-z0-9_.-]+\.(?:go|py|sh))\b`)
	fullstackEventRe   = regexp.MustCompile(`^(?:round[0-9]*|review|rereview|codex|allgreen|megaaudit|[a-z][0-9]+)$`)
	fullstackTokenRe   = regexp.MustCompile(`[_.-]`)
)

// fullstackNewCode is the scan set: everything below an S10 root or in an S10
// family, plus every manifest or ledger destination outside them, minus the
// frozen pre-S10 baseline. The reason is kept for the gate's log.
func fullstackNewCode(tree fullstackTree, manifest fullstackManifest, owned map[string]string) map[string]string {
	preexisting := map[string]bool{}
	for _, path := range fullstackPreexistingCode {
		preexisting[path] = true
	}
	scanned := map[string]string{}
	for path := range tree.files {
		switch {
		case preexisting[path]:
		case fullstackUnder(path, fullstackRoots...):
			scanned[path] = "root"
		case fullstackFamily(path):
			scanned[path] = "family"
		case manifest.owner[path] != "":
			scanned[path] = manifest.owner[path]
		case owned[path] != "":
			scanned[path] = owned[path]
		}
	}
	return scanned
}

func checkFullstackSources(t *testing.T, tree fullstackTree) (int, error) {
	t.Helper()
	manifest, err := parseFullstackManifest(tree.docs[fullstackPlanPath])
	if err != nil {
		return 0, err
	}
	owned, err := fullstackOwnedRows(tree.docs[fullstackOwnershipPath])
	if err != nil {
		return 0, err
	}
	for _, path := range fullstackPreexistingCode {
		if manifest.owner[path] == "" {
			return 0, fmt.Errorf("stale pre-S10 baseline entry %s is no longer a manifest path", path)
		}
	}
	for pkg, paths := range manifest.packages {
		if !slices.ContainsFunc(paths, tree.exists) {
			return 0, fmt.Errorf("%s: no manifest destination exists (empty S10 package root)", pkg)
		}
	}
	populated := map[string]bool{}
	for path := range tree.files {
		for _, dir := range fullstackRoots {
			populated[dir] = populated[dir] || fullstackUnder(path, dir)
		}
	}
	for _, dir := range fullstackRoots {
		if !populated[dir] {
			return 0, fmt.Errorf("empty S10 source root %s", dir)
		}
	}
	scanned := fullstackNewCode(tree, manifest, owned)
	if len(scanned) == 0 {
		return 0, fmt.Errorf("empty S10 scan set")
	}
	for _, path := range brokerSortedSourcePaths(scanned) {
		source := tree.files[path]
		size := measureFullstackFile(t, path, source)
		if size.code > 700 || !strings.HasSuffix(path, ".go") && size.physical > 700 {
			return 0, fmt.Errorf("%s (%s): new S10 file has %d code / %d physical lines, maximum 700", path, scanned[path], size.code, size.physical)
		}
		stem := strings.TrimSuffix(fullstackBase(path), filepath.Ext(path))
		for _, token := range fullstackTokenRe.Split(stem, -1) {
			if fullstackEventRe.MatchString(token) {
				return 0, fmt.Errorf("%s: process-named S10 file (token %q); name it by responsibility", path, token)
			}
		}
		lines := strings.SplitN(source, "\n", headerScanLimit+1)
		for _, line := range lines[:min(len(lines), headerScanLimit)] {
			if m := fullstackHeaderRe.FindStringSubmatch(line); m != nil {
				if m[1] != fullstackBase(path) {
					return 0, fmt.Errorf("%s: header names %s, not this file", path, m[1])
				}
				break
			}
		}
	}
	return len(scanned), checkFullstackOrigins(tree, manifest)
}

const headerScanLimit = 40

// Every `hot-update S10 plan §x` anchor anywhere in the tree names a real plan
// heading, and its S10 package tokens name manifest packages. Text after another
// stage or document (S8 P24, architecture §22.8.6) cites that source instead.
func checkFullstackOrigins(tree fullstackTree, manifest fullstackManifest) error {
	sections := map[string]bool{}
	for _, line := range strings.Split(tree.docs[fullstackPlanPath], "\n") {
		if m := fullstackHeadingRe.FindStringSubmatch(line); m != nil {
			sections["§"+m[1]+m[2]] = true
		}
	}
	anchors := 0
	for _, path := range brokerSortedSourcePaths(tree.files) {
		for _, m := range fullstackOriginRe.FindAllStringSubmatch(tree.files[path], -1) {
			anchors++
			tail := m[2]
			if cut := fullstackForeignRe.FindStringIndex(tail); cut != nil {
				tail = tail[:cut[0]]
			}
			for _, section := range append([]string{m[1]}, fullstackSectionRe.FindAllString(tail, -1)...) {
				if !sections[section] {
					return fmt.Errorf("%s: origin cites unknown S10 plan section %s", path, section)
				}
			}
			for _, pkg := range fullstackPackageRe.FindAllString(tail, -1) {
				if manifest.packages[pkg] == nil {
					return fmt.Errorf("%s: origin cites unknown S10 package %s", path, pkg)
				}
			}
		}
	}
	if anchors == 0 || len(sections) == 0 {
		return fmt.Errorf("S10 origin scan is blind: %d anchors, %d plan sections", anchors, len(sections))
	}
	return nil
}

func checkFullstackGateRegistry(tree fullstackTree) error {
	if err := brokerGateEntryProblem(tree.docs["Makefile"]); err != nil {
		return err
	}
	claude := tree.docs["CLAUDE.md"]
	header := strings.Index(claude, "| 闸门 | 位置 | 管什么 |")
	if header < 0 {
		return fmt.Errorf("missing CLAUDE.md gate table")
	}
	locations := map[string]bool{}
	for _, line := range strings.Split(claude[header:], "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			break
		}
		if cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|"); len(cells) > 1 {
			for _, m := range claudeGatePathRe.FindAllStringSubmatch(cells[1], -1) {
				locations[m[1]] = true
			}
		}
	}
	for _, file := range brokerSortedSourcePaths(fullstackGateControls) {
		path, control := "test/architecture/"+file, fullstackGateControls[file]
		if !locations[path] {
			return fmt.Errorf("undocumented S10 gate %s", path)
		}
		anchored := false
		for _, m := range gateControlAnchorRe.FindAllStringSubmatch(tree.files[path], -1) {
			anchored = anchored || m[1] == control
		}
		if !anchored {
			return fmt.Errorf("%s: no gate-control anchor for %s", path, control)
		}
		live, err := fullstackLiveControls(path, tree.files[path], control)
		if err != nil {
			return err
		}
		if live != 1 {
			return fmt.Errorf("%s: control %s is not one live Test function", path, control)
		}
	}
	return nil
}

// Controls re-check the registry after every mutation; the large gate files
// are parsed once per distinct content.
var fullstackControlMemo = struct {
	sync.Mutex
	live map[string]int
}{live: map[string]int{}}

func fullstackLiveControls(path, source, control string) (int, error) {
	key := path + "\x00" + control + "\x00" + source
	fullstackControlMemo.Lock()
	defer fullstackControlMemo.Unlock()
	if live, known := fullstackControlMemo.live[key]; known {
		return live, nil
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, source, parser.SkipObjectResolution)
	if err != nil {
		return 0, err
	}
	live := 0
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv == nil && fn.Name.Name == control && fn.Body != nil && len(fn.Body.List) > 0 &&
			len(fn.Type.Params.List) == 1 && transferExpr(fn.Type.Params.List[0].Type, brokerImports(parsed)) == "*testing.T" {
			live++
		}
	}
	fullstackControlMemo.live[key] = live
	return live, nil
}

// origin: hot-update S10 plan §10 / §12.1 / P28
func TestFullstackSourceContracts(t *testing.T) {
	tree := fullstackRepoTree(t)
	scanned, err := checkFullstackSources(t, tree)
	if err != nil {
		t.Error(err)
	}
	if err := checkFullstackGateRegistry(tree); err != nil {
		t.Error(err)
	}
	t.Logf("scanned %d new S10 code files; %d frozen pre-S10 destinations; %d gate/control pairs", scanned, len(fullstackPreexistingCode), len(fullstackGateControls))
}

// The mutations land in a copied real tree on disk. The rest of the plan §10.2
// P28 row (Agent 124/126, wrong layer/ctx/tag/stub) is killed by each sibling
// file's own control, which runs as a top-level Test under the unfiltered
// central entry; the registry mutations here prove those controls stay
// anchored, documented and live instead of re-running them (plan §12.3 keeps
// the new Go gates' marginal cost under 10s).
// origin: hot-update S10 plan §10.2 P28 / §12.1 / §12.3
func TestFullstackGateControls(t *testing.T) {
	real := fullstackRepoTree(t)
	prefixes := append(slices.Clone(fullstackRoots), "internal/drillobserve", "test/architecture/fullstack_*", "test/hotupdate/fullstack_*",
		"test/simcluster/drills/lib/fullstack_*", "test/simcluster/drills/99-*", "test/simcluster/drills/100-*",
		"test/simcluster/tests/hot-update-fullstack*", "test/simcluster/tests/hot_update_fullstack*", "test/simcluster/lib/fullstack-*")
	for file := range fullstackGateControls {
		prefixes = append(prefixes, "test/architecture/"+file)
	}
	root := fullstackFixture(t, real, prefixes...)
	manifest, err := parseFullstackManifest(real.docs[fullstackPlanPath])
	if err != nil {
		t.Fatal(err)
	}
	// Every real manifest destination, so non-code packages (P33 documents,
	// P34 tasklist) keep their roots in the copy as they do in the repository.
	for path := range manifest.owner {
		if raw, err := os.ReadFile(filepath.Join(real.root, path)); err == nil {
			transferWriteFixture(t, root, path, string(raw))
		}
	}
	check := func() error {
		tree := readFullstackTree(t, root)
		if _, err := checkFullstackSources(t, tree); err != nil {
			return err
		}
		return checkFullstackGateRegistry(tree)
	}
	// 700 code lines in a new file is the limit itself, not a violation.
	transferWriteFixture(t, root, "test/fullstackharness/bounded.go", "package fullstackharness\n"+strings.Repeat("var _ = 1\n", 699))
	if err := check(); err != nil {
		t.Fatalf("700-line new file refused: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "test/fullstackharness/bounded.go")); err != nil {
		t.Fatal(err)
	}
	anchor := "# origin: hot-update S10 " + "plan "
	lib := "test/simcluster/drills/lib/"
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: "test/fullstackharness/oversized.go", new: "package fullstackharness\n" + strings.Repeat("var _ = 1\n", 700), want: "maximum 700"},
		fullstackMutation{path: lib + "fullstack_padded.py", new: strings.Repeat("# padding\n", 701), want: "maximum 700"},
		fullstackMutation{path: lib + "fullstack_dense.py", new: strings.Repeat("x = 1\n", 701), want: "maximum 700"},
		fullstackMutation{path: "test/simcluster/tests/hot-update-fullstack-oversized.sh", new: "#!/bin/sh\n" + strings.Repeat(":\n", 700), want: "maximum 700"},
		fullstackMutation{path: "internal/drillobserve/runtime_sample.go", new: strings.Repeat("var _ = 1\n", 700), want: "(P06): new S10 file"},
		fullstackMutation{path: fullstackPlanPath, old: "| P17 RequestScheduler |", new: "| Q17 RequestScheduler |", want: "lists 35 packages"},
		fullstackMutation{path: fullstackPlanPath, old: "| P28 SourceGates | HARD | 8 |", new: "| P28 SourceGates | HARD | 9 |", want: "declares 9 files"},
		fullstackMutation{path: fullstackPlanPath, old: "`test/hotupdate/acceptance/resources_test.go`", new: "`test/hotupdate/acceptance/budget_test.go`", want: "two S10 manifest owners"},
		fullstackMutation{path: fullstackPlanPath, old: "`test/simcluster/drills/lib/logs.sh`", new: "`test/simcluster/drills/lib/strict_logs.sh`", want: "stale pre-S10 baseline"},
		fullstackMutation{path: fullstackOwnershipPath, old: "test/fullstackharness/schedule.go\tP17", new: "test/fullstackharness/schedule.go P17", want: "malformed ownership row"},
		fullstackMutation{path: lib + "fullstack_anchor.py", new: anchor + "§99 / P28\n", want: "unknown S10 plan section §99"},
		fullstackMutation{path: lib + "fullstack_anchor.py", new: anchor + "§12.1 / §12.9\n", want: "unknown S10 plan section §12.9"},
		fullstackMutation{path: lib + "fullstack_anchor.py", new: anchor + "§12.1 / P41\n", want: "unknown S10 package P41"},
		fullstackMutation{path: lib + "fullstack_round2_test.py", new: "print('named after an event')\n", want: "process-named"},
		fullstackMutation{path: lib + "fullstack_header.py", new: "# fullstack_other.py — a stale header\n", want: "header names fullstack_other.py"},
		fullstackMutation{path: "test/architecture/layering_test.go", old: "// gate-control: TestFullstackLayeringControls", new: "// gate control: TestFullstackLayeringControls", want: "no gate-control anchor"},
		fullstackMutation{path: "test/architecture/build_tags_test.go", old: "func TestFullstackBuildTagControls(", new: "func TestFullstackBuildTagControlsDormant(", want: "not one live Test"},
		fullstackMutation{path: "CLAUDE.md", old: "`test/architecture/fullstack_contract_test.go`", new: "`test/architecture/fullstack_contract.go`", want: "undocumented S10 gate"},
		fullstackMutation{path: "Makefile", old: "\tgo test ./test/architecture/...", new: "\tgo test -run TestOne ./test/architecture/...", want: "unfiltered"},
	)
	// Another stage's package after its stage name belongs to that stage.
	transferWriteFixture(t, root, lib+"fullstack_anchor.py", anchor+"§12.1 / S9 P41\n")
	if err := check(); err != nil {
		t.Fatalf("cross-stage origin refused: %v", err)
	}
	// An emptied S10 root is a blind scanner, not a clean tree.
	moved := filepath.Join(t.TempDir(), "storewitness")
	if err := os.Rename(filepath.Join(root, "internal/storewitness"), moved); err != nil {
		t.Fatal(err)
	}
	if err := check(); err == nil || !strings.Contains(err.Error(), "empty S10 source root internal/storewitness") {
		t.Errorf("on-disk empty S10 root escaped: %v", err)
	}
	if err := os.Rename(moved, filepath.Join(root, "internal/storewitness")); err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
}
