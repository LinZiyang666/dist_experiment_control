package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// gate_registry_test.go — the hand-copied lists that describe the gates must match the gates.
//
// origin: line-2 external review m3 and m24. Everything this increment added carries a reverse
// assertion except the lists that describe the machinery itself:
//
//	m3   .golangci.yml holds a register of function NAMES (10 maintidx sites, 22 unparam sites). Rename
//	     or delete any of them and the exemption stays, excusing nothing, while still reading to the next
//	     person as "this was looked at and accepted". Every other ledger in the repo fails on a stale
//	     entry; these were the exception.
//	m24  the `gates:` target's package list is a hand-typed constant with no reconciliation. Three gates
//	     were outside it, one of which CLAUDE.md's own gate table lists AS a gate — so `make gates`, whose
//	     entire promise is "re-runs every mechanical guard in one shot", did not run it.
//
// Both are the same shape as the defect this whole increment is about: a declaration that nothing checks.

var (
	// Any parenthesised alternation on a `text:` line. Deliberately loose: the config spells these
	// several ways (with and without a receiver-prefix group, with and without a `Function name:`
	// preamble), and a regex tuned to today's exact spelling would go quietly blind the next time one of
	// them is edited — which is the whole failure mode this file exists to prevent. Non-identifier
	// tokens are filtered afterwards instead.
	golangciAltGroupRe = regexp.MustCompile(`\(([A-Za-z_][A-Za-z0-9_|$\\d]*)\)`)
	golangciTextLineRe = regexp.MustCompile(`(?m)^\s*text: ".*"$`)
	// A rule's own path scope. origin: external review M4 — the register lookup must honour it.
	golangciPathLineRe = regexp.MustCompile(`^- path: (.+)$`)
	makeGoTestPkgsRe   = regexp.MustCompile(`(?m)^\tgo test ((?:\./[^\s]+\s*)+)$`)
	// A shell gate set run as its own recipe line (`\tsh test/simcluster/tests/run-all.sh`). Covered by
	// EXACT path, never by directory: the table names the script, and a directory prefix would let any
	// other script under tests/ claim to be run. origin: docs/reviews/test-system-overhaul-plan.md B1.
	makeShLineRe     = regexp.MustCompile(`(?m)^\tsh (test/\S+)$`)
	claudeGatePathRe = regexp.MustCompile("`((?:test|cmd|internal)/[A-Za-z0-9_/.-]+)`")
	funcDeclNameRe   = regexp.MustCompile(`(?m)^func (?:\([^)]*\) )?([A-Za-z_][A-Za-z0-9_]*)`)
	identRe          = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// TestGolangciNameRegistersNameLiveFunctions is m3's reverse assertion: every function named in an
// exclusion rule's alternation must still exist somewhere in the tree.
//
// It deliberately checks EXISTENCE, not that the linter still reports the site. Proving the latter means
// running golangci-lint with the exclusion removed, once per site — and golangci-lint takes a global lock,
// so that is not something a test in this package may do (see the note on `make gates` in CLAUDE.md). What
// it does catch is the rot that actually happens: a rename or a deletion leaving an exemption behind.
func TestGolangciNameRegistersNameLiveFunctions(t *testing.T) {
	root := repoRoot(t)

	cfg, err := os.ReadFile(filepath.Join(root, ".golangci.yml"))
	if err != nil {
		t.Fatalf("read .golangci.yml: %v", err)
	}

	// PARSED AS {path, names} PAIRS. origin: line-2 INDEPENDENT EXTERNAL REVIEW M4.
	//
	// The first version flattened every register into one bag of names and asked only "does a function with
	// this name exist ANYWHERE in the tree". `Run` exists in five packages. So renaming the exemption's
	// actual target — `(*Broker).Run` in internal/broker — left the exemption green forever, because some
	// other package still had a `Run`. The exclusion is scoped by `path:` in the config and the check threw
	// that scope away, which made the check weakest exactly where the config was most careful.
	//
	// Rules are read as blocks: a `- path:` line opens one and its `text:` lines carry the names.
	type register struct {
		path  *regexp.Regexp
		names []string
		line  int
	}
	var registers []register
	var cur *register
	for i, line := range strings.Split(string(cfg), "\n") {
		trimmed := strings.TrimSpace(line)
		if m := golangciPathLineRe.FindStringSubmatch(trimmed); m != nil {
			re, cerr := regexp.Compile(m[1])
			if cerr != nil {
				t.Errorf(".golangci.yml:%d: unparseable path regex %q: %v", i+1, m[1], cerr)
				cur = nil
				continue
			}
			registers = append(registers, register{path: re, line: i + 1})
			cur = &registers[len(registers)-1]
			continue
		}
		if strings.HasPrefix(trimmed, "- ") && !strings.HasPrefix(trimmed, "- path:") {
			cur = nil // a rule with no path: (global text rule) — names in it are not path-scoped
		}
		if !golangciTextLineRe.MatchString(line) {
			continue
		}
		for _, g := range golangciAltGroupRe.FindAllStringSubmatch(line, -1) {
			parts := strings.Split(g[1], "|")
			if len(parts) < 2 {
				continue // not an alternation of names; e.g. a lone capture group
			}
			for _, name := range parts {
				name = strings.TrimSpace(name)
				if !identRe.MatchString(name) {
					continue
				}
				if cur == nil {
					// Unscoped rule: keep the old whole-tree semantics rather than inventing a scope.
					registers = append(registers, register{path: nil, names: []string{name}, line: i + 1})
					continue
				}
				cur.names = append(cur.names, name)
			}
		}
	}
	total := 0
	for _, r := range registers {
		total += len(r.names)
	}
	if total < 20 {
		t.Fatalf("only %d names parsed out of .golangci.yml's exclusion registers — the parser is broken, "+
			"so a clean result below would mean nothing. Expected the maintidx register (10) plus the two "+
			"unparam lists (13 + 9).", total)
	}
	scoped := 0
	for _, r := range registers {
		if r.path != nil {
			scoped += len(r.names)
		}
	}
	if scoped == 0 {
		t.Fatal("no register was parsed WITH a path scope — the block parser is broken and this check has " +
			"silently degraded back to the whole-tree name lookup external review M4 rejected")
	}

	// declaredIn[name] is the set of repo-relative FILES declaring a function of that name, so a register's
	// path scope can be applied to the lookup.
	declaredIn := map[string][]string{}
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
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
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for _, m := range funcDeclNameRe.FindAllStringSubmatch(string(src), -1) {
			declaredIn[m[1]] = append(declaredIn[m[1]], rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	seen := map[string]bool{}
	var dead []string
	for _, r := range registers {
		for _, name := range r.names {
			key := name
			if r.path != nil {
				key = r.path.String() + "::" + name
			}
			if seen[key] {
				continue
			}
			seen[key] = true

			files := declaredIn[name]
			if r.path == nil {
				if len(files) == 0 {
					dead = append(dead, name+"  (unscoped rule at .golangci.yml:"+strconv.Itoa(r.line)+")")
				}
				continue
			}
			// The declaration must be inside the rule's own path scope. A same-named function elsewhere
			// does not keep the exemption alive — that was M4 exactly.
			inScope := false
			for _, f := range files {
				if r.path.MatchString(f) {
					inScope = true
					break
				}
			}
			if !inScope {
				detail := "no function of that name anywhere"
				if len(files) > 0 {
					sort.Strings(files)
					detail = "declared only OUTSIDE the scope, in " + strings.Join(slicesCompact(files), ", ")
				}
				dead = append(dead, name+"  (rule path `"+r.path.String()+"` at .golangci.yml:"+
					strconv.Itoa(r.line)+" — "+detail+")")
			}
		}
	}
	sort.Strings(dead)
	if len(dead) > 0 {
		t.Errorf("%d name(s) in .golangci.yml's exclusion registers no longer name any function in the "+
			"tree:\n  %s\n\n"+
			"A dead exemption excuses nothing while still reading as a considered decision. Delete the "+
			"name in the same commit as the rename — that is the rule every other ledger in this repo "+
			"already enforces on itself.", len(dead), strings.Join(dead, "\n  "))
	}
}

// TestGatesTargetCoversEveryGateCLAUDEMdNames is m24's reverse assertion.
//
// CLAUDE.md's gate table is the user-facing answer to "what are this repo's gates". `make gates` is the
// command that claims to run all of them. Nothing connected the two, and they had diverged.
func TestGatesTargetCoversEveryGateCLAUDEMdNames(t *testing.T) {
	root := repoRoot(t)

	mk, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	// The gates recipe's `go test` line, isolated from the rest of the Makefile.
	gatesIdx := strings.Index(string(mk), "\ngates:")
	if gatesIdx < 0 {
		t.Fatal("the Makefile has no `gates:` target — CLAUDE.md tells people to run it")
	}
	recipe := string(mk)[gatesIdx:]
	if end := strings.Index(recipe[1:], "\n\n"); end > 0 {
		recipe = recipe[:end+1]
	}
	pm := makeGoTestPkgsRe.FindStringSubmatch(recipe)
	if pm == nil {
		t.Fatal("could not parse the `go test` package list out of the gates recipe")
	}
	var covered []string
	for _, p := range strings.Fields(pm[1]) {
		p = strings.TrimPrefix(p, "./")
		p = strings.TrimSuffix(p, "/...")
		covered = append(covered, strings.TrimSuffix(p, "/"))
	}
	if len(covered) < 4 {
		t.Fatalf("parsed only %d packages from the gates recipe — parser broken", len(covered))
	}
	for _, m := range makeShLineRe.FindAllStringSubmatch(recipe, -1) {
		covered = append(covered, strings.TrimSuffix(m[1], "/"))
	}

	claude, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatalf("read CLAUDE.md: %v", err)
	}
	// The gate table, bounded by its header row and the first line after it that is not a table row.
	body := string(claude)
	hdr := strings.Index(body, "| 闸门 | 位置 | 管什么 |")
	if hdr < 0 {
		t.Fatal("CLAUDE.md no longer has the gate table this test reconciles against")
	}
	table := body[hdr:]
	var rows []string
	for _, line := range strings.Split(table, "\n") {
		// The table is indented inside a bullet, so trim before testing for a row.
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			break
		}
		rows = append(rows, line)
	}
	if len(rows) < 6 {
		t.Fatalf("the gate table parsed to %d rows — too few to be the real table", len(rows))
	}

	var uncovered []string
	for _, row := range rows {
		// The LOCATION column only. The table is `| 闸门 | 位置 | 管什么 |`, and the third column routinely
		// names files a gate READS (a scanned document, a golden, a config) rather than a place a gate LIVES.
		//
		// origin: line-2 closure verification. Scanning the whole row made this gate demand that
		// `make gates` run `test/simcluster` because the 确定性 lint row mentions
		// test/simcluster/README.md as part of the docs wire-version SCAN SURFACE. That is a false
		// positive of this gate's own making, and it fired on a documentation edit — which is exactly the
		// friction that teaches people to delete the documentation instead.
		cells := strings.Split(strings.Trim(strings.TrimSpace(row), "|"), "|")
		if len(cells) < 2 {
			continue
		}
		location := cells[1]
		for _, m := range claudeGatePathRe.FindAllStringSubmatch(location, -1) {
			path := m[1]
			// Reduce a file path to its package directory.
			dir := path
			if strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".md") {
				dir = filepath.Dir(path)
			}
			dir = strings.TrimSuffix(dir, "/")
			ok := false
			for _, c := range covered {
				if dir == c || strings.HasPrefix(dir+"/", c+"/") {
					ok = true
					break
				}
			}
			if !ok {
				uncovered = append(uncovered, dir+"  (from "+path+")")
			}
		}
	}
	sort.Strings(uncovered)
	uncovered = slicesCompact(uncovered)
	if len(uncovered) > 0 {
		t.Errorf("CLAUDE.md's gate table names %d location(s) that `make gates` does not run:\n  %s\n\n"+
			"gates says it \"re-runs every mechanical guard in one shot\". Either add the package to the "+
			"recipe, or take the row out of the table — a gate listed but not run is worse than one that "+
			"is neither, because the table is what people check instead of looking.",
			len(uncovered), strings.Join(uncovered, "\n  "))
	}
}

var flowSSGateControls = map[string]string{
	"flow_frame_inventory_test.go":  "TestFlowFrameInventoryControls",
	"ss_state_inventory_test.go":    "TestSSStateInventoryControls",
	"image_field_inventory_test.go": "TestFlowSSImageInventoryControls",
	"dataplane_lifetime_test.go":    "TestFlowSSDataplaneLifetimeControls",
	"layering_test.go":              "TestFlowSSLayeringControls",
	"flow_ss_contract_test.go":      "TestFlowSSContractControls",
	"upgrade_budget_pin_test.go":    "TestFlowSSBudgetControls",
	"gate_registry_test.go":         "TestFlowSSGateRegistryControls",
}

// A documented gate must actually run unfiltered, and its named control must
// remain in the same file. A declaration in a review plan is not an entry point.
// origin: hot-update S5 plan §10.1 / T49.
// gate-control: TestFlowSSGateRegistryControls
func TestFlowSSGateRegistryCoversLiveControls(t *testing.T) {
	root := repoRoot(t)
	mk, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	docs, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{}
	for file := range flowSSGateControls {
		raw, err := os.ReadFile(filepath.Join(root, "test/architecture", file))
		if err != nil {
			t.Fatal(err)
		}
		sources[file] = string(raw)
	}
	if err := checkFlowSSGateRegistry(string(mk), string(docs), sources); err != nil {
		t.Fatalf("gate registration pending P28 when documentation is missing: %v", err)
	}
}
func checkFlowSSGateRegistry(makefile, docs string, sources map[string]string) error {
	idx := strings.Index(makefile, "\ngates:")
	if idx < 0 {
		return fmt.Errorf("missing gates target")
	}
	recipe := makefile[idx:]
	if end := strings.Index(recipe[1:], "\n\n"); end > 0 {
		recipe = recipe[:end+1]
	}
	match := makeGoTestPkgsRe.FindStringSubmatch(recipe)
	if match == nil {
		return fmt.Errorf("missing unfiltered go test gates entry")
	}
	covered := false
	for _, pkg := range strings.Fields(match[1]) {
		covered = covered || pkg == "./test/architecture" || pkg == "./test/architecture/" || pkg == "./test/architecture/..." || pkg == "./test/..."
	}
	if !covered {
		return fmt.Errorf("gates omits architecture package")
	}
	header := strings.Index(docs, "| 闸门 | 位置 | 管什么 |")
	if header < 0 {
		return fmt.Errorf("missing documented gate table")
	}
	locations := map[string]bool{}
	for _, line := range strings.Split(docs[header:], "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			break
		}
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		if len(cells) < 2 {
			continue
		}
		for _, match := range claudeGatePathRe.FindAllStringSubmatch(cells[1], -1) {
			locations[match[1]] = true
		}
	}
	for file, control := range flowSSGateControls {
		if !locations["test/architecture/"+file] {
			return fmt.Errorf("undocumented S5 gate test/architecture/%s", file)
		}
		source, ok := sources[file]
		if !ok {
			return fmt.Errorf("missing S5 gate source %s", file)
		}
		anchored := false
		for _, match := range gateControlAnchorRe.FindAllStringSubmatch(source, -1) {
			anchored = anchored || match[1] == control
		}
		if !anchored {
			return fmt.Errorf("%s has no anchor for %s", file, control)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, source, 0)
		if err != nil {
			return err
		}
		live := false
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != control || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
				continue
			}
			ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			typ, ok := ptr.X.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			pkg, ok := typ.X.(*ast.Ident)
			live = ok && pkg.Name == "testing" && typ.Sel.Name == "T" && fn.Body != nil
		}
		if !live {
			return fmt.Errorf("%s control %s is not a live Test function", file, control)
		}
	}
	return nil
}

// Each missing ingredient is injected separately through the gate's actual
// parser: package recipe, table location, anchor, source and Test declaration.
// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSGateRegistryControls(t *testing.T) {
	if len(flowSSGateControls) != 8 {
		t.Fatal("S5 gate/control registry must contain all eight owned files")
	}
	for _, row := range []string{
		"flow_frame_inventory_test.go:TestFlowFrameInventoryControls",
		"ss_state_inventory_test.go:TestSSStateInventoryControls",
		"image_field_inventory_test.go:TestFlowSSImageInventoryControls",
		"dataplane_lifetime_test.go:TestFlowSSDataplaneLifetimeControls",
		"layering_test.go:TestFlowSSLayeringControls",
		"flow_ss_contract_test.go:TestFlowSSContractControls",
		"upgrade_budget_pin_test.go:TestFlowSSBudgetControls",
		"gate_registry_test.go:TestFlowSSGateRegistryControls",
	} {
		file, control, _ := strings.Cut(row, ":")
		if flowSSGateControls[file] != control {
			t.Fatalf("missing exact S5 gate/control %s", row)
		}
	}

	makefile := "\ngates:\n\tgo test ./test/architecture ./test/determinism\n\n"
	docs := "| 闸门 | 位置 | 管什么 |\n"
	sources := map[string]string{}
	for file, control := range flowSSGateControls {
		docs += "| binary | `test/architecture/" + file + "` | invariant |\n"
		sources[file] = "package architecture\n// gate-control: " + control + "\nfunc " + control + "(t *testing.T) {}\n"
	}
	if err := checkFlowSSGateRegistry(makefile, docs, sources); err != nil {
		t.Fatal(err)
	}
	for file, control := range flowSSGateControls {
		t.Run(file, func(t *testing.T) {
			if checkFlowSSGateRegistry(makefile, strings.Replace(docs, "test/architecture/"+file, "test/missing/"+file, 1), sources) == nil {
				t.Fatal("missing documented gate accepted")
			}
			for _, part := range []string{"// gate-control: " + control, "func " + control} {
				broken := map[string]string{}
				for path, source := range sources {
					broken[path] = source
				}
				broken[file] = strings.Replace(broken[file], part, strings.Replace(part, control, "MissingControl", 1), 1)
				if checkFlowSSGateRegistry(makefile, docs, broken) == nil {
					t.Fatal("missing live control/anchor accepted")
				}
			}
			broken := map[string]string{}
			for path, source := range sources {
				if path != file {
					broken[path] = source
				}
			}
			if checkFlowSSGateRegistry(makefile, docs, broken) == nil {
				t.Fatal("missing source accepted")
			}
		})
	}
	for _, recipe := range []string{strings.Replace(makefile, "./test/architecture", "./test/missing", 1), strings.Replace(makefile, "go test ", "go test -run '^TestOne$' ", 1), strings.Replace(makefile, "gates:", "other:", 1)} {
		if checkFlowSSGateRegistry(recipe, docs, sources) == nil {
			t.Fatal("missing unfiltered gates entry accepted")
		}
	}
}

// S6's gate entry points remain unfiltered. A same-directory script is not an
// entry point for the ledger, and a filtered Go invocation cannot certify all
// controls even when one of them passes.
// origin: hot-update S6 plan §13 / T54 / T58
func brokerGateEntryProblem(makefile string) error {
	idx := strings.Index(makefile, "\ngates:")
	if idx < 0 {
		return fmt.Errorf("missing gates target")
	}
	recipe := makefile[idx:]
	if end := strings.Index(recipe[1:], "\n\n"); end > 0 {
		recipe = recipe[:end+1]
	}
	match := makeGoTestPkgsRe.FindStringSubmatch(recipe)
	if match == nil {
		return fmt.Errorf("missing unfiltered go gates entry")
	}
	packages := map[string]bool{}
	for _, pkg := range strings.Fields(match[1]) {
		packages[strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(pkg, "./"), "/..."), "/")] = true
	}
	for _, pkg := range []string{"test/architecture", "test/determinism", "test/e2e/parallel"} {
		if !packages[pkg] {
			return fmt.Errorf("missing S6 gate package %s", pkg)
		}
	}
	for _, line := range makeShLineRe.FindAllStringSubmatch(recipe, -1) {
		if line[1] == "test/simcluster/tests/run-all.sh" {
			return nil
		}
	}
	return fmt.Errorf("missing hermetic run-all entry")
}

// origin: hot-update S6 plan §13 / T54 / T58
// gate-control: TestBrokerGateRegistryControls
func TestBrokerGatesHaveUnfilteredEntryPoints(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join(repoRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if err := brokerGateEntryProblem(string(makefile)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §11 / T54 / T58
func TestBrokerGateRegistryControls(t *testing.T) {
	const good = "\ngates:\n\tgo test ./test/architecture/... ./test/determinism/... ./test/e2e/parallel/\n\tsh test/simcluster/tests/run-all.sh\n\n"
	if err := brokerGateEntryProblem(good); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"./test/architecture/...", "./test/determinism/...", "./test/e2e/parallel/", "sh test/simcluster/tests/run-all.sh"} {
		if brokerGateEntryProblem(strings.Replace(good, fragment, "", 1)) == nil {
			t.Fatalf("missing gate %s accepted", fragment)
		}
	}
	if brokerGateEntryProblem(strings.Replace(good, "go test ", "go test -run '^TestOne$' ", 1)) == nil {
		t.Fatal("filtered gates accepted")
	}
}

// A shared name is insufficient if both consumers silently adopt a widened
// constant. The frozen acceptance values must remain independently pinned.
// origin: hot-update S6 plan §13 / T58
func brokerAcceptanceBudgetProblem(values map[string]int64) bool {
	frozen := map[string]int64{"request_ns": int64(5 * time.Second), "maintenance_ns": int64(8 * time.Second), "probe_rate": 10, "flow_stall_ns": int64(5 * time.Second), "freeze_ns": int64(500 * time.Millisecond)}
	if len(values) != len(frozen) {
		return true
	}
	for name, want := range frozen {
		if got, present := values[name]; !present || got != want {
			return true
		}
	}
	return false
}

// origin: hot-update S6 plan §13 / T58
// gate-control: TestBrokerAcceptanceBudgetControls
func TestBrokerAcceptanceBudgetsRemainFrozen(t *testing.T) {
	values := map[string]int64{
		"request_ns":     int64(upgradebudget.RequestLatency),
		"maintenance_ns": int64(upgradebudget.BrokerMaintenanceTTL),
		"probe_rate":     upgradebudget.ProbeRatePerSecond,
		"flow_stall_ns":  int64(upgradebudget.LiveFlowStall),
		"freeze_ns":      int64(upgradebudget.LocalFreezeWindow),
	}
	if brokerAcceptanceBudgetProblem(values) {
		t.Fatal("S6 acceptance budgets changed")
	}
	if upgradebudget.BrokerMaintenanceTTL != upgradebudget.RequestLatency+3*time.Second {
		t.Fatal("maintenance lost the fixed admission/proof margin")
	}
}

// origin: hot-update S6 plan §11 / T58
func TestBrokerAcceptanceBudgetControls(t *testing.T) {
	good := map[string]int64{"request_ns": int64(5 * time.Second), "maintenance_ns": int64(8 * time.Second), "probe_rate": 10, "flow_stall_ns": int64(5 * time.Second), "freeze_ns": int64(500 * time.Millisecond)}
	if brokerAcceptanceBudgetProblem(good) {
		t.Fatal("frozen budgets rejected")
	}
	for name := range good {
		broken := map[string]int64{}
		for key, value := range good {
			if key != name {
				broken[key] = value
			}
		}
		if !brokerAcceptanceBudgetProblem(broken) {
			t.Fatalf("missing %s accepted", name)
		}
		broken[name] = good[name] + 1
		if !brokerAcceptanceBudgetProblem(broken) {
			t.Fatalf("widened %s accepted", name)
		}
	}
}

var infraGateControls = map[string]string{
	"infra_contract_test.go":        "TestInfraContractGateControls",
	"infra_runtime_test.go":         "TestInfraRuntimeGateControls",
	"infra_supply_test.go":          "TestInfraSupplyControls",
	"structural_budget_test.go":     "TestInfraStructuralBudgetControls",
	"layering_test.go":              "TestInfraLayeringControls",
	"ctx_background_ledger_test.go": "TestInfraContextRootControls",
	"dataplane_lifetime_test.go":    "TestInfraDataplaneLifetimeControls",
	"build_tags_test.go":            "TestInfraBuildTagControls",
	"upgrade_budget_pin_test.go":    "TestInfraBudgetPinControls",
	"nats_server_pin_test.go":       "TestInfraNATSCatalogPinControls",
	"nats_ping_defaults_test.go":    "TestInfraPingProfileControls",
	"gate_registry_test.go":         "TestInfraGateRegistryControls",
	"ci_workflow_test.go":           "TestInfraReleaseArtifactControls",
	"broker_image_contract_test.go": "TestInfraImageStateControls",
}

// A registry row needs its own declaration, same-file control, documentation
// entry and unfiltered central recipe. Planned paths are not installed gates.
// origin: hot-update S9 plan §10.2 / T83
func checkInfraGateRegistry(makefile, docs string, sources map[string]string) error {
	if err := brokerGateEntryProblem(makefile); err != nil {
		return err
	}
	for path, name := range infraGateControls {
		source := sources[path]
		if source == "" {
			return fmt.Errorf("missing S9 gate source %s", path)
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		count := 0
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name && fn.Body != nil && len(fn.Body.List) > 0 {
				count++
			}
		}
		if count != 1 || !strings.Contains(source, "// gate-control: "+name) {
			return fmt.Errorf("%s: missing/empty same-file S9 control %s", path, name)
		}
		if !strings.Contains(docs, "test/architecture/"+path) {
			return fmt.Errorf("NOT_RUN pending P38: undocumented S9 gate %s", path)
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraGateRegistryControls
func TestInfraGateRegistryHasLiveUnfilteredEntries(t *testing.T) {
	root := repoRoot(t)
	sources := map[string]string{}
	for path := range infraGateControls {
		raw, err := os.ReadFile(filepath.Join(root, "test/architecture", path))
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = string(raw)
	}
	mk, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	docs, err := os.ReadFile(filepath.Join(root, "CLAUDE.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkInfraGateRegistry(string(mk), string(docs), sources); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraGateRegistryControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	docs := ""
	sources := map[string]string{}
	for path, name := range infraGateControls {
		docs += "test/architecture/" + path + "\n"
		source := "package architecture\n// gate-control: " + name + "\nfunc " + name + "(){check()}\n"
		transferWriteFixture(t, root, "test/architecture/"+path, source)
		sources[path] = source
	}
	mk := "\ngates:\n\tgo test ./test/architecture/... ./test/determinism/... ./test/e2e/parallel/\n\tsh test/simcluster/tests/run-all.sh\n\n"
	if err := checkInfraGateRegistry(mk, docs, sources); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(mk, "go test ", "go test -run TestTiny ", 1), strings.Replace(mk, "./test/architecture/...", "./test/architecture/empty", 1), strings.Replace(mk, "sh test/simcluster/tests/run-all.sh", "# removed hermetic entry", 1)} {
		if checkInfraGateRegistry(bad, docs, sources) == nil {
			t.Fatal("filtered/missing S9 gate recipe escaped")
		}
	}
	for path, name := range infraGateControls {
		before := sources[path]
		transferWriteFixture(t, root, "test/architecture/"+path, strings.Replace(before, "func "+name+"()", "func DormantControl()", 1))
		raw, err := os.ReadFile(filepath.Join(root, "test/architecture", path))
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = string(raw)
		if checkInfraGateRegistry(mk, docs, sources) == nil {
			t.Fatalf("on-disk dead S9 control escaped %s", path)
		}
		sources[path] = before
		if checkInfraGateRegistry(mk, strings.Replace(docs, "test/architecture/"+path, "", 1), sources) == nil {
			t.Fatalf("missing S9 registry path escaped %s", path)
		}
	}
}
