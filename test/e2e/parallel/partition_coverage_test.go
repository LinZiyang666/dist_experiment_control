package main

import (
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/LinZiyang666/tether/test/transferharness"
)

// Whole-package runs exceeded their budgets from cumulative fixture work. Exact
// partitions preserve the declared suite only if new tests cannot silently fall
// outside the filters. Discover source independently of the hand-written matrix
// and require one owner for each test, with no dangling or duplicate selectors.
// origin: hot-update S6 FIX-MATRIX / lead decision / §9.3 / T54.
// gate-control: TestPartitionCoverageControls
func TestPartitionedPackagesCoverEveryTestExactlyOnce(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	// Each spec inspects the same immutable tree. Cache only within this Test:
	// another invocation or on-disk control must always discover fresh source.
	// origin: hot-update S10 plan §10.2 P30 / §11.1
	sourceCache := map[string][]string{}
	discover := func(pkg, tags string) ([]string, error) {
		key := pkg + "|" + tags
		if names, ok := sourceCache[key]; ok {
			return names, nil
		}
		names, err := partitionPackageTests(root, pkg, tags)
		if err == nil {
			sourceCache[key] = names
		}
		return names, err
	}
	// Direct matrix runs also need discovery: disabling the parallel splitter
	// must not restore the measured 813s/689s cumulative owner commands.
	// origin: hot-update S7 plan lead L17 / T52; S6 scanner seam review.
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "test/e2e/all_phases_test.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]map[string]bool{}
	heavy := map[string]bool{}
	seedDiscovery := false
	var separatelyOwnedCLI []string
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		calls[fn.Name.Name] = map[string]bool{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok {
					calls[fn.Name.Name][name.Name] = true
					// Direct entry points omit these exact CLI tests from dynamic
					// discovery. Their separate matrix declarations must match this
					// source list, or one runner can drop or duplicate a test while
					// the parallel runner still reports a complete union.
					// origin: S10 internal review ARCH-REDS-3 F3; hot-update S10 plan §11.1
					if fn.Name.Name == "runOwnerMatrixPartitions" && name.Name == "runInfraMatrixPartitions" {
						for _, arg := range call.Args[3:] {
							literal, ok := arg.(*ast.BasicLit)
							if !ok || literal.Kind != token.STRING {
								t.Fatal("direct CLI partition exclusions must name exact tests")
							}
							test, err := strconv.Unquote(literal.Value)
							if err != nil {
								t.Fatal(err)
							}
							separatelyOwnedCLI = append(separatelyOwnedCLI, test)
						}
					}
					if fn.Name.Name == "runInfraMatrixPartitions" && name.Name == "run" && len(call.Args) == 2 {
						list, listOK := call.Args[0].(*ast.BasicLit)
						filter, filterOK := call.Args[1].(*ast.BasicLit)
						if listOK && filterOK && list.Value == `"-list"` && filter.Value == `"^Fuzz"` {
							seedDiscovery = true
						}
					}
				}
			}
			if fn.Name.Name == "runOwnerMatrixPartitions" {
				if comparison, ok := node.(*ast.BinaryExpr); ok && comparison.Op == token.EQL {
					if literal, ok := comparison.Y.(*ast.BasicLit); ok {
						heavy[literal.Value] = true
					}
				}
			}
			return true
		})
	}
	for _, matrix := range []string{"TestHotUpdateFlowMatrix", "TestHotUpdateBrokerMatrix", "TestTransferContinuationMatrix"} {
		if !calls[matrix]["runOwnerMatrixPartitions"] {
			t.Fatalf("%s: direct heavy owners must reach per-test discovery", matrix)
		}
	}
	// origin: hot-update S7 plan lead L17 / T52 — direct seed replay must remain reachable.
	if !seedDiscovery {
		t.Fatal("direct heavy discovery must retain fuzz seed replay")
	}
	// origin: hot-update S8 plan lead L21 / S8 LEGACY-ROUTE-TESTS; direct runs must split CLI too.
	if !calls["runOwnerMatrixPartitions"]["runInfraMatrixPartitions"] || !heavy[`"./internal/broker/..."`] || !heavy[`"./internal/agent/..."`] || !heavy[`"./cmd/tether/..."`] || !heavy[`"./test/cli_e2e/..."`] || !heavy[`"./test/clusterharness/..."`] {
		t.Fatal("direct owner helper must discover every measured heavy package")
	}
	var explicitCLI []unit
	for _, u := range units {
		if u.matrix == "TestHotUpdateBrokerMatrix" && u.pkg == "./cmd/tether/..." && (u.hasRun || u.runFilter != "") {
			explicitCLI = append(explicitCLI, u)
		}
	}
	if problems := partitionCoverageProblems(explicitCLI, "TestHotUpdateBrokerMatrix", "./cmd/tether/...", "", "600s", separatelyOwnedCLI); len(problems) != 0 {
		t.Fatalf("direct CLI exclusions differ from explicit matrix ownership:\n%s", strings.Join(problems, "\n"))
	}
	for _, spec := range []struct{ matrix, pkg, tags, budget string }{
		{"TestHotUpdateFlowMatrix", "./test/hotupdate/...", "", "600s"},
		{"TestHotUpdateBrokerMatrix", "./test/hotupdate/...", "", "600s"},
		{"TestHotUpdateBrokerMatrix", "./test/p13/...", "p13_integration", "600s"},
		{"TestHotUpdateIntegrationMatrix", "./test/hotupdate/...", "hotupdate_integration", "600s"},
		// origin: hot-update S9 plan §12 P28/P36/P40; S10 plan §10.2 P27/P30.
		// origin: hot-update S9 plan §12 / P22/P24/P36; new Tests must enter exact discovery.
		{"TestHotUpdateInfraMatrix", "./internal/infraattach/...", "", "600s"},
		// origin: hot-update S9 plan §12 / P36; discover the complete registered composition leaf.
		{"TestHotUpdateInfraMatrix", "./internal/infracompose/...", "", "600s"},
		// origin: hot-update S9 plan §12 / P36; readback and supplied build/stock controls retain exact discovery.
		{"TestHotUpdateInfraMatrix", "./internal/caddyconfig/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./test/infraharness/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infraauth/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infracli/...", "", "600s"},
		// origin: hot-update S9 plan §12 / P36; INSTRUMENTS-3 observer tests require exact discovery.
		{"TestHotUpdateInfraMatrix", "./test/simcluster/drills/lib/infraobserve/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infrahost/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infraupgrade/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infraroot/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./cmd/tether-infra-helper/...", "", "600s"},
		{"TestHotUpdateInfraNATSMatrix", "./test/infranats/...", "infranats_integration", "600s"},
		// origin: S9 internal review rv-business F7 — build-tag registration alone cannot run a suite.
		{"TestHotUpdateInfraCaddyMatrix", "./test/infracaddy/...", "infracaddy_integration", "600s"},
		{"TestHotUpdateInfraBusinessMatrix", "./test/infrabusiness/...", "infrabusiness_integration", "600s"},
		{"TestHotUpdateInfraPrivilegeMatrix", "./test/infraprivilege/...", "infraprivilege_integration", "600s"},
		// origin: S10 internal review GATE-FINAL F3 / S8 FREEZE-WINDOW.
		// Every reader/publication witness must have its own exact race tuple.
		{"TestUpgradeTransactionMatrix", "./internal/agentupgradeport/...", "", "600s"},
		{"TestFullstackAcceptanceMatrix", "./test/hotupdate/acceptance/...", "", "600s"},
		{"TestFullstackAcceptanceMatrix", "./test/hotupdate/evidencecmd/...", "", "600s"},
		{"TestFullstackHarnessMatrix", "./test/fullstackharness/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/infrarepair/...", "", "600s"},
		{"TestHotUpdateInfraMatrix", "./internal/ingressswitch/...", "", "600s"},
		// origin: hot-update S7 plan lead L17 / T52 — measured heavy owners.
		{"TestHotUpdateBrokerMatrix", "./internal/broker/...", "", "600s"},
		// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F4; plan L17 / §11.1
		{"TestHotUpdateBrokerMatrix", "./internal/agent/...", "", "600s"},
		{"TestHotUpdateBrokerMatrix", "./cmd/tether/...", "", "600s"},
		{"TestTransferContinuationMatrix", "./internal/broker/...", "", "600s"},
		// origin: hot-update S8 plan lead L21 / S8 LEGACY-ROUTE-TESTS; discover the complete CLI roster.
		{"TestHotUpdateBrokerMatrix", "./test/cli_e2e/...", "", "600s"},
		// origin: hot-update S8 plan lead L21 / S8 UPGRADE-MAKETEST; independent complete cluster roster.
		{"TestHotUpdateBrokerMatrix", "./test/clusterharness/...", "", "600s"},
		{"TestTransferContinuationMatrix", "./test/cli_e2e/...", "", "600s"},
	} {
		t.Run(spec.matrix+"/"+spec.pkg, func(t *testing.T) {
			names, err := discover(spec.pkg, spec.tags)
			if err != nil {
				t.Fatal(err)
			}
			planned := units
			if spec.tags != "" || spec.matrix == "TestHotUpdateInfraMatrix" || spec.matrix == "TestFullstackAcceptanceMatrix" || spec.matrix == "TestFullstackHarnessMatrix" || spec.pkg == "./internal/broker/..." || spec.pkg == "./internal/agent/..." || spec.pkg == "./cmd/tether/..." || spec.pkg == "./test/cli_e2e/..." || spec.pkg == "./test/clusterharness/..." {
				// Keep the existing tagged dynamic partitioning. Its independent
				// source list still witnesses exactly one planned unit per test.
				planned, err = shardPackagesWithList(units, 0, func(u unit) ([]string, error) {
					// The real provider must preserve seed functions too. Source
					// discovery is the independent denominator; reusing it on
					// both sides would hide a broken -list/filter implementation.
					// origin: hot-update S7 plan lead L17 / T52 / seed replay.
					if u.matrix == spec.matrix && u.pkg == spec.pkg && (u.pkg == "./internal/broker/..." || u.pkg == "./internal/agent/..." || u.pkg == "./cmd/tether/..." || u.pkg == "./test/cli_e2e/..." || u.pkg == "./test/clusterharness/..." || u.pkg == "./internal/infrahost/...") {
						return listPackageTests(u)
					}
					return discover(u.pkg, u.tags)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if problems := partitionCoverageProblems(planned, spec.matrix, spec.pkg, spec.tags, spec.budget, names); len(problems) != 0 {
				t.Fatal(strings.Join(problems, "\n"))
			}
		})
	}
}

var exactTestSelector = regexp.MustCompile(`^\^(Test|Fuzz|Benchmark)[\pL\pN_]+\$$`)

func partitionTupleProblems(u unit, tags, budget string) []string {
	extra := ""
	if strings.HasPrefix(u.runFilter, "^Benchmark") {
		extra = "-bench=" + u.runFilter + " -benchtime=1x"
	}
	if u.whole || !u.race || u.tags != tags || u.extra != extra || u.timeo != budget ||
		(!exactTestSelector.MatchString(u.runFilter) && u.runFilter != "^$" && !transferBoundarySelector(u)) {
		return []string{u.pkg + ": requires an exact per-test race partition with tags " + fmt.Sprintf("%q", tags) + " and timeout " + budget}
	}
	return nil
}

// Only declared boundary selectors are valid nested filters. Broad subtest
// regexps must not turn a missing cell or round into apparently complete coverage.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F5 / hot-update S7 plan lead L17
func transferBoundarySelector(u unit) bool {
	head, _, _ := strings.Cut(u.runFilter, "/")
	name := strings.TrimSuffix(strings.TrimPrefix(head, "^"), "$")
	for _, selector := range transferharness.BoundarySelectors(u.pkg, name, u.tags) {
		if selector == u.runFilter {
			return true
		}
	}
	return false
}

func transferCoverageSelectors(pkg, name, tags string) []string {
	if name == "" {
		return []string{""}
	}
	if selectors := transferharness.BoundarySelectors(pkg, name, tags); len(selectors) != 0 {
		return selectors
	}
	return []string{"^" + name + "$"}
}

func partitionCoverageProblems(units []unit, matrix, pkg, tags, budget string, names []string) []string {
	var problems []string
	seen := map[string]int{}
	for _, name := range names {
		path, test := partitionIdentity(pkg, name)
		for _, selector := range transferCoverageSelectors(path, test, tags) {
			key := strings.TrimSuffix(path, "/...") + ":" + selector
			if _, duplicate := seen[key]; duplicate {
				problems = append(problems, "ambiguous test name in package closure: "+name)
			}
			seen[key] = 0
		}
	}
	if len(names) == 0 {
		problems = append(problems, pkg+": partition source contains no tests")
	}
	labels := map[string]bool{}
	for _, u := range units {
		if u.matrix != matrix || !packagePatternContains(pkg, u.pkg) {
			continue
		}
		problems = append(problems, partitionTupleProblems(u, tags, budget)...)
		name := u.runFilter
		matches := 0
		for key := range seen {
			path, test, _ := strings.Cut(key, ":")
			if !packagePatternContains(u.pkg, path) {
				continue
			}
			switch test {
			case "":
				// Every selector compiles its recursive no-test leaves. Repeated
				// compilation is not repeated Test execution.
				seen[key] = 1
			case name:
				seen[key]++
			}
			if test == name || test == "" && name == "^$" {
				matches++
			}
		}
		if matches == 0 {
			problems = append(problems, pkg+": partition selects an undeclared test: "+u.runFilter)
		}
		if labels[u.name] {
			problems = append(problems, "duplicate partition receipt label: "+u.name)
		}
		labels[u.name] = true
	}
	for name, count := range seen {
		if count != 1 {
			problems = append(problems, fmt.Sprintf("%s: %s covered by %d partitions, want exactly 1", pkg, name, count))
		}
	}
	sort.Strings(problems)
	return problems
}

// These are Linux process/image suites. Match Linux source and the actual unit's
// race/integration tags even when the fast plan guard runs on a different host.
// Walk the /... closure too: a new child package must not silently disappear.
func partitionPackageTests(root, pkg, tags string) ([]string, error) {
	ctx := build.Default
	ctx.GOOS = "linux"
	ctx.BuildTags = append([]string{"race"}, strings.Split(tags, ",")...)
	var names []string
	packages, tested := map[string]bool{}, map[string]bool{}
	dir := filepath.Join(root, strings.TrimSuffix(strings.TrimPrefix(pkg, "./"), "/..."))
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != dir && !strings.HasSuffix(pkg, "/...") {
				return filepath.SkipDir
			}
			if entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".") || strings.HasPrefix(entry.Name(), "_") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		match, err := ctx.MatchFile(filepath.Dir(path), filepath.Base(path))
		if err != nil || !match {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		key := "./" + filepath.ToSlash(rel)
		packages[key] = true
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if ok && fd.Recv == nil && fd.Body != nil && partitionDeclarationName(fd.Name.Name) {
				tested[key] = true
				names = append(names, key+":"+fd.Name.Name)
			}
		}
		return nil
	})
	// Recursive command leaves with no Test still belong to the race compile
	// closure. An empty selector compiles that real package without pretending
	// it exercised a product test or a pending integration dependency.
	for pkg := range packages {
		if !tested[pkg] {
			names = append(names, pkg+":")
		}
	}
	sort.Strings(names)
	return names, err
}

func partitionDeclarationName(name string) bool {
	if name == "TestMain" {
		return false
	}
	for _, prefix := range []string{"Test", "Fuzz", "Benchmark"} {
		if suffix, ok := strings.CutPrefix(name, prefix); ok {
			for _, r := range suffix {
				return !unicode.IsLower(r)
			}
			return true
		}
	}
	return false
}

// origin: hot-update S6 FIX-MATRIX / lead decision / §9.3 / T54.
func TestPartitionCoverageControls(t *testing.T) {
	const matrix, pkg = "TestExampleMatrix", "./test/example/..."
	names := []string{"TestAlpha", "TestBeta"}
	good := []unit{
		{matrix: matrix, pkg: pkg, race: true, timeo: "600s", hasRun: true, runFilter: "^TestAlpha$", name: "alpha"},
		{matrix: matrix, pkg: pkg, race: true, timeo: "600s", hasRun: true, runFilter: "^TestBeta$", name: "beta"},
	}
	if problems := partitionCoverageProblems(good, matrix, pkg, "", "600s", names); len(problems) != 0 {
		t.Fatal(problems)
	}
	for _, defect := range []string{"delete", "new test", "duplicate", "stale test", "broad filter", "empty filter", "race", "tags", "timeout", "extra", "whole", "receipt collision", "empty source", "empty suite", "ambiguous source"} {
		t.Run(defect, func(t *testing.T) {
			bad := append([]unit(nil), good...)
			tests := append([]string(nil), names...)
			switch defect {
			case "delete":
				bad = bad[1:]
			case "new test":
				tests = append(tests, "TestNew")
			case "duplicate":
				bad = append(bad, bad[0])
			case "stale test":
				bad[0].runFilter = "^TestRemoved$"
			case "broad filter":
				bad[0].runFilter = "^Test.*$"
			case "empty filter":
				bad[0].runFilter = ""
			case "race":
				bad[0].race = false
			case "tags":
				bad[0].tags = "hidden"
			case "timeout":
				bad[0].timeo = "601s"
			case "extra":
				bad[0].extra = "-short"
			case "whole":
				bad[0].whole = true
			case "receipt collision":
				bad[0].name = bad[1].name
			case "empty source":
				tests = nil
			case "empty suite":
				tests, bad = nil, nil
			case "ambiguous source":
				tests = append(tests, tests[0])
			}
			// Assert the tuple guard directly as well as the complete-set oracle;
			// otherwise dangling-name rejection can mask a disabled shape guard.
			switch defect {
			case "broad filter", "empty filter", "race", "tags", "timeout", "extra", "whole":
				if len(partitionTupleProblems(bad[0], "", "600s")) == 0 {
					t.Fatal("defective partition tuple accepted")
				}
			}
			if len(partitionCoverageProblems(bad, matrix, pkg, "", "600s", tests)) == 0 {
				t.Fatal("defective partition accepted")
			}
		})
	}
}

// origin: hot-update S6 FIX-MATRIX / lead decision / §9.3 / T54.
func TestPartitionSourceDiscoveryUsesBuildConstraints(t *testing.T) {
	root := t.TempDir()
	for file, source := range map[string]string{
		"ordinary_test.go":         "package example\nfunc TestOrdinary(t *testing.T) {}\nfunc TestMain(m *testing.M) {}",
		"linux_test.go":            "//go:build linux\n\npackage example\nfunc TestLinux(t *testing.T) {}",
		"darwin_test.go":           "//go:build darwin\n\npackage example\nfunc TestDarwin(t *testing.T) {}",
		"race_test.go":             "//go:build race\n\npackage example\nfunc TestRace(t *testing.T) {}",
		"tagged_test.go":           "//go:build hotupdate_integration\n\npackage example\nfunc TestTagged(t *testing.T) {}",
		"child/child_test.go":      "package child\nfunc TestChild(t *testing.T) {}",
		"testdata/ignored_test.go": "package example\nfunc TestIgnored(t *testing.T) {}",
	} {
		path := filepath.Join(root, "test/example", file)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tags := range []string{"", "hotupdate_integration"} {
		names, err := partitionPackageTests(root, "./test/example/...", tags)
		want := "./test/example/child:TestChild,./test/example:TestLinux,./test/example:TestOrdinary,./test/example:TestRace"
		if tags != "" {
			want += ",./test/example:TestTagged"
		}
		if err != nil || strings.Join(names, ",") != want {
			t.Fatalf("tags %q: names=%v err=%v, want %s", tags, names, err, want)
		}
	}
}

// origin: hot-update S6 FIX-MATRIX / lead decision / §9.3 / T54.
func TestPartitionReceiptsKeepDistinctSelectors(t *testing.T) {
	var units []unit
	for _, name := range []string{"TestAlpha", "TestBeta"} {
		units = append(units, parseGoTestArgs("TestExampleMatrix", []string{"go", "test", "-race", "-timeout", "600s", "-run", "^" + name + "$", "./test/example/..."})...)
	}
	if len(units) != 2 || units[0].name == units[1].name {
		t.Fatalf("partition receipts collide: %v", units)
	}
	planned, err := shardPackagesWithList(units, 8, func(unit) ([]string, error) {
		t.Fatal("an explicit partition must not be resharded")
		return nil, nil
	})
	if err != nil || len(planned) != 2 {
		t.Fatalf("explicit partition drift: %v, %v", planned, err)
	}
	for i, u := range planned {
		if strings.Join(u.goTestArgs(), " ") != strings.Join(units[i].goTestArgs(), " ") {
			t.Fatal("sharding changed an exact partition selector")
		}
	}
}

// Every S10 source declaration is the denominator, including recursive leaves
// and the integration-only delta. Matrix declarations cannot define their own
// expected count: replacing a Test while keeping that count must still fail.
// origin: hot-update S10 plan §10.2 P30 / §11.1
// gate-control: TestFullstackPartitionControls
func TestFullstackPartitionsCoverEveryDiscoveredTest(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if problems := fullstackPartitionProblems(root); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

func fullstackPartitionProblems(root string) []string {
	units, _, err := splitMatrices(root)
	if err != nil {
		return []string{err.Error()}
	}
	// Source discovery is independent of the production go-test listing. This
	// gate never forks the matrix or signs a live integration/drill receipt.
	var subjects []unit
	for _, u := range units {
		if u.matrix == "TestFullstackAcceptanceMatrix" || u.matrix == "TestFullstackHarnessMatrix" || (u.matrix == "TestHotUpdateFlowMatrix" || u.matrix == "TestHotUpdateBrokerMatrix" || u.matrix == "TestHotUpdateIntegrationMatrix") && u.pkg == "./test/hotupdate/..." {
			subjects = append(subjects, u)
		}
	}
	planned, err := shardPackagesWithList(subjects, 0, func(u unit) ([]string, error) {
		return partitionPackageTests(root, u.pkg, u.tags)
	})
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "test/e2e/all_phases_test.go"), nil, 0)
	if err != nil {
		return []string{err.Error()}
	}
	serial, serialErr := partitionListingLoop(root, "test/e2e/all_phases_test.go", "matrixListedTests")
	parallel, parallelErr := partitionListingLoop(root, "test/e2e/parallel/shard.go", "packageListedTests")
	if serialErr != nil || parallelErr != nil || serial == "" || serial != parallel {
		problems = append(problems, fmt.Sprintf("direct/parallel package listing must agree: %v / %v", serialErr, parallelErr))
	}
	// Direct entry points must reach the same per-test planner. A complete
	// parallel plan cannot excuse restoring a cumulative direct command.
	// origin: hot-update S10 plan §10.2 P30 / §11.1
	direct := map[string]bool{"TestFullstackAcceptanceMatrix": false, "TestFullstackHarnessMatrix": false}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if _, wanted := direct[fn.Name.Name]; !wanted {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "runInfraMatrixPartitions" {
					direct[fn.Name.Name] = true
				}
			}
			return true
		})
	}
	for name, reached := range direct {
		if !reached {
			problems = append(problems, name+": direct entry must reach per-test discovery")
		}
	}
	for _, spec := range []struct{ matrix, pkg, tags string }{
		{"TestFullstackAcceptanceMatrix", "./test/hotupdate/acceptance/...", ""},
		{"TestFullstackAcceptanceMatrix", "./test/hotupdate/evidencecmd/...", ""},
		{"TestFullstackHarnessMatrix", "./test/fullstackharness/...", ""},
		{"TestHotUpdateFlowMatrix", "./test/hotupdate/...", ""},
		{"TestHotUpdateBrokerMatrix", "./test/hotupdate/...", ""},
		{"TestHotUpdateIntegrationMatrix", "./test/hotupdate/...", "hotupdate_integration"},
	} {
		names, err := partitionPackageTests(root, spec.pkg, spec.tags)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		problems = append(problems, partitionCoverageProblems(planned, spec.matrix, spec.pkg, spec.tags, "600s", names)...)
	}
	// These explicit resource owners have no import-based guarantee. Require
	// their whole race declarations even if another matrix covers a subset.
	for _, pkg := range []string{"internal/drillobserve", "internal/storewitness"} {
		count := 0
		for _, u := range units {
			if u.matrix == "TestLeakRiskMatrix" && u.pkg == "./"+pkg+"/..." && u.race && !u.whole && !u.hasRun && u.runFilter == "" && u.tags == "" && u.extra == "" && u.timeo == "600s" {
				count++
			}
		}
		if count != 1 {
			problems = append(problems, pkg+": requires one explicit whole race tuple in TestLeakRiskMatrix")
		}
	}
	sort.Strings(problems)
	return problems
}

// The controls edit real source on disk, preserving its size/count where that
// is the defect. The same oracle reads the repository and each mutated copy.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func TestFullstackPartitionControls(t *testing.T) {
	checkPartitionGenericBuckets(t)
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if problems := fullstackPartitionProblems(root); len(problems) != 0 {
		t.Fatal(problems)
	}
	// Listing two identically named tests must produce two exact package
	// commands. A no-test command leaf is compiled; a benchmark really gets
	// -bench rather than an inert -run that would execute nothing.
	listed := packageListedTests("TestShared\nFuzzShared\nBenchmarkShared\nok github.com/LinZiyang666/tether/test/fullstackharness 0.1s\nTestShared\nok github.com/LinZiyang666/tether/test/fullstackharness/child 0.1s\n? github.com/LinZiyang666/tether/test/fullstackharness/cmd/tool [no test files]\n")
	declaration := unit{matrix: "TestFullstackHarnessMatrix", pkg: "./test/fullstackharness/...", race: true, timeo: "600s", name: "harness"}
	planned, err := shardPackagesWithList([]unit{declaration}, 0, func(unit) ([]string, error) { return listed, nil })
	if err != nil || len(planned) != 5 {
		t.Fatalf("package listing lost declarations: %v %v", planned, err)
	}
	seen := map[string]bool{}
	for _, u := range planned {
		key := u.pkg + ":" + u.runFilter
		if seen[key] || strings.HasSuffix(u.pkg, "/...") || !u.race || u.timeo != "600s" {
			t.Fatalf("package identity/budget lost: %+v", u)
		}
		seen[key] = true
		args := strings.Join(u.goTestArgs(), " ")
		if strings.HasPrefix(u.runFilter, "^Benchmark") && !strings.Contains(args, "-bench ^BenchmarkShared$ -benchtime 1x") {
			t.Fatalf("benchmark was only listed: %s", args)
		}
	}
	for _, defect := range []string{"same count", "child package", "child closure", "tagged test", "duplicate selector", "timeout", "risk tuple", "equal child names", "benchmark", "dynamic benchmark", "direct cumulative", "direct package identity"} {
		t.Run(defect, func(t *testing.T) {
			copyRoot := t.TempDir()
			for _, dir := range []string{"test/hotupdate", "test/fullstackharness", "test/e2e"} {
				if err := copyPartitionSources(root, copyRoot, dir); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(copyRoot, "test/e2e/all_phases_test.go")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(raw)
			switch defect {
			case "same count":
				text = strings.ReplaceAll(text, "^TestFullstackFlowSupplyOutlivesSchedule$", "^TestUnregisteredReplacement$")
			case "child closure":
				text = strings.Replace(text, `"./test/hotupdate/acceptance/...", "./test/hotupdate/evidencecmd/..."`, `"./test/hotupdate/acceptance", "./test/hotupdate/evidencecmd/..."`, 1)
			case "child package", "tagged test", "equal child names", "benchmark", "dynamic benchmark":
				file, source := "test/hotupdate/child/child_test.go", "package child\nfunc TestUnregisteredChild(t *testing.T) {}\n"
				if defect == "tagged test" {
					file, source = "test/hotupdate/hidden_test.go", "//go:build hotupdate_integration\n\npackage hotupdate_test\nfunc TestTaggedAddition(t *testing.T) {}\n"
					text = strings.ReplaceAll(text, `"hotupdate_integration"`, `"missing_integration"`)
				}
				if defect == "equal child names" {
					file, source = "test/hotupdate/acceptance/child/child_test.go", "package child\nfunc TestFullstackContractsFailClosed(t *testing.T) {}\n"
				}
				if defect == "benchmark" {
					source = "package child\nfunc BenchmarkUnregistered(b *testing.B) {}\n"
				}
				if defect == "dynamic benchmark" {
					file, source = "test/fullstackharness/child/bench_test.go", "package child\nfunc BenchmarkDynamic(b *testing.B) {}\n"
				}
				if err := os.MkdirAll(filepath.Dir(filepath.Join(copyRoot, file)), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(copyRoot, file), []byte(source), 0o600); err != nil {
					t.Fatal(err)
				}
			case "duplicate selector":
				line := `exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackFlowSupplyOutlivesSchedule$", "./test/hotupdate/..."),`
				text = strings.Replace(text, line, line+"\n"+line, 1)
			case "timeout":
				text = strings.ReplaceAll(text, `"600s"`, `"601s"`)
			case "risk tuple":
				text = strings.ReplaceAll(text, `"./internal/storewitness/..."`, `"./internal/drillobserve/..."`)
			case "direct cumulative":
				text = strings.Replace(text, "runInfraMatrixPartitions(t, repoRoot, cmd)\n}\n\n// Whole declarations", "_ = cmd\n}\n\n// Whole declarations", 1)
			case "direct package identity":
				text = strings.Replace(text, `names = append(names, pkg+":"+name)`, `names = append(names, name)`, 1)
			}
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
			problems := fullstackPartitionProblems(copyRoot)
			// New children in the dynamic acceptance owner remain covered,
			// even when another package declares the identical test name.
			wantAccepted := defect == "equal child names" || defect == "dynamic benchmark"
			if (len(problems) == 0) != wantAccepted {
				t.Fatalf("%s: problems=%v", defect, problems)
			}
		})
	}
}

func copyPartitionSources(root, dest, dir string) error {
	return filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		return os.WriteFile(target, raw, 0o600)
	})
}
