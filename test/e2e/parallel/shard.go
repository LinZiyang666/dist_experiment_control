package main

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/test/transferharness"
)

// shardPackages splits the slowest units by TEST NAME.
//
// This is the split that actually moves the ceiling. The earlier package-level
// split did not, and the measurement says exactly why:
//
//	D4 = 4m45s total, of which internal/broker alone is 4m37s (97%)
//
// `go test ./a/... ./b/...` already runs its packages concurrently, so pulling
// them apart just re-implements what the toolchain was doing — while the one
// package that dominates stays exactly as slow. The bottleneck is INSIDE the
// package: Go runs the tests within a package serially unless they call
// t.Parallel(), and internal/broker has 496 of them taking 270s at 0.7 cores of
// actual CPU. Most of that is waiting, not computing.
//
// Sharding by name gives each shard its own process, so shards genuinely run at
// the same time. It is safe in a way that adding t.Parallel() would not be:
// nothing shares memory, and any per-test global state stays per-process.
//
// Ordering matters. Tests are assigned round-robin rather than in contiguous
// blocks so that a cluster of slow neighbours (they tend to be adjacent — same
// file, same feature) does not all land in one shard.
func shardPackages(units []unit, shards int) ([]unit, error) {
	return shardPackagesWithList(units, shards, listPackageTests)
}

func shardPackagesWithList(units []unit, shards int, list func(unit) ([]string, error)) ([]unit, error) {
	var out []unit
	for _, u := range units {
		// Each S5 integration test owns repeated real processes. In particular the
		// watchdog itself costs five real 120s rounds, so even an eight-way bucket
		// must not combine it with another upgrade test. Listing failure is fatal:
		// falling back to the cumulative suite would recreate the timeout.
		// origin: hot-update S5 plan §8 / §10.2 / T45 / T50.
		// The new infrastructure and acceptance suites also own repeated process
		// fixtures. A whole declaration is partitioned by each discovered Test,
		// retaining its tag and 600s budget; no filtered unit is repartitioned.
		// The infraunit policy leaf has no own tests; infrahost exercises it, while
		// its declaration still compiles the entire leaf under race.
		// origin: hot-update S9 plan §12 P28/P36/P40; S10 plan §10.2 P27/P30.
		// origin: S9 internal review rv-business F7 — all four supplied suites must partition.
		partitioned := u.pkg == "./test/hotupdate/..." && u.tags == "hotupdate_integration" ||
			u.matrix == "TestHotUpdateInfraMatrix" && u.pkg != "./internal/infraunit/..." || u.matrix == "TestHotUpdateInfraNATSMatrix" ||
			u.matrix == "TestHotUpdateInfraCaddyMatrix" || u.matrix == "TestHotUpdateInfraBusinessMatrix" ||
			u.matrix == "TestHotUpdateInfraPrivilegeMatrix" || u.matrix == "TestFullstackAcceptanceMatrix" ||
			u.matrix == "TestFullstackHarnessMatrix" ||
			// Cumulative races measured 813s in broker and 689s in cmd/tether.
			// Mandatory per-test splitting applies even when -shards=0.
			// origin: hot-update S7 plan lead L17 / T52; ledger pass 4 receipts.
			u.pkg == "./internal/broker/..." || u.pkg == "./cmd/tether/..." ||
			// The complete agent race reached 600s with tests still waiting to
			// start. Preserve its recursive discovery, including fuzz seeds.
			// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F4; plan L17 / §11.1
			u.pkg == "./internal/agent/..." ||
			// origin: hot-update S8 plan lead L21 / S8 LEGACY-ROUTE-TESTS; five-round CLI cuts require separate units.
			u.pkg == "./test/cli_e2e/..." ||
			// origin: hot-update S8 plan lead L21 / S8 UPGRADE-MAKETEST; retain every real cluster fixture.
			u.pkg == "./test/clusterharness/..."
		if partitioned && !u.isWhole() && !u.hasRun && u.runFilter == "" {
			names, err := list(u)
			if err != nil {
				return nil, err
			}
			if len(names) == 0 {
				return nil, fmt.Errorf("%s: %s contains no tests", u.matrix, u.pkg)
			}
			for _, name := range names {
				su := namedPartition(u, name)
				// An exact, separately declared partition already owns this Test.
				// Retain the complete discovered remainder without executing those
				// heavy tests twice or changing any of their flags or budgets.
				// origin: S8 internal review SIGNED-ROLL-FIXTURES F1–F5; S8 plan L21.
				owned := false
				for _, explicit := range units {
					if explicit.matrix == u.matrix && packagePatternContains(explicit.pkg, su.pkg) && explicit.tags == u.tags && explicit.race == u.race && explicit.timeo == u.timeo && explicit.extra == su.extra && explicit.hasRun && explicit.runFilter == su.runFilter {
						owned = true
						break
					}
				}
				if owned {
					continue
				}
				// A top-level S7 matrix itself exceeded 600s. Split it into
				// its declared cells after discovery, preserving five rounds.
				// origin: hot-update S7 plan lead L17 / TRANSFER-OVERLAY-REPAIR F5
				if selectors := transferharness.BoundarySelectors(su.pkg, strings.TrimSuffix(strings.TrimPrefix(su.runFilter, "^"), "$"), su.tags); len(selectors) != 0 {
					for _, selector := range selectors {
						cell := su
						cell.runFilter = selector
						cell.name = su.name + "[" + selector + "]"
						out = append(out, cell)
					}
				} else {
					out = append(out, su)
				}
			}
			continue
		}
		if shards < 2 {
			out = append(out, u)
			continue
		}
		if u.isWhole() || !u.shardable() {
			out = append(out, u)
			continue
		}
		names, err := list(u)
		if err != nil || len(names) < shards*2 {
			// Too few tests to be worth splitting, or the list failed: run whole.
			out = append(out, u)
			continue
		}
		// A recursive list can contain the same name in several packages.
		// Never join qualified identities into a regexp or launch it on /...:
		// both would lose which package owns the selector.
		// origin: hot-update S10 plan §10.2 P30 / §11.1
		qualified := false
		for _, name := range names {
			qualified = qualified || strings.Contains(name, ":")
		}
		if qualified {
			grouped, err := shardQualifiedPackages(u, names, shards)
			if err != nil {
				return nil, err
			}
			out = append(out, grouped...)
			continue
		}
		buckets := make([][]string, shards)
		for i, n := range names {
			buckets[i%shards] = append(buckets[i%shards], n)
		}
		for i, b := range buckets {
			if len(b) == 0 {
				continue
			}
			su := u
			su.runFilter = "^(" + strings.Join(b, "|") + ")$"
			su.name = fmt.Sprintf("%s[%d/%d]", u.name, i+1, shards)
			out = append(out, su)
		}
	}
	return out, nil
}

// Optional generic sharding keeps its original bucket budget. Package custody
// adds one grouping dimension; it must not turn every cluster test into a new
// process, while benchmarks still need their explicit execution flag.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func shardQualifiedPackages(u unit, names []string, shards int) ([]unit, error) {
	groups := map[string][]string{}
	var out []unit
	for _, entry := range names {
		pkg, name := partitionIdentity(u.pkg, entry)
		if name == "" || strings.HasPrefix(name, "Benchmark") {
			out = append(out, namedPartition(u, entry))
			continue
		}
		groups[pkg] = append(groups[pkg], name)
	}
	var packages []string
	for pkg := range groups {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	for _, pkg := range packages {
		pu := u
		pu.pkg, pu.name = pkg, u.name+":"+pkg
		grouped, err := shardPackagesWithList([]unit{pu}, shards, func(unit) ([]string, error) { return groups[pkg], nil })
		if err != nil {
			return nil, err
		}
		out = append(out, grouped...)
	}
	return out, nil
}

// shardable reports whether this unit is a candidate for name-level sharding.
// Only the packages known to dominate a matrix are split: sharding a 10s package
// adds process overhead for nothing.
func (u unit) shardable() bool {
	// External review B3. A unit that already carries the matrix's own -run
	// cannot be name-sharded: go test takes ONE -run, so a shard filter would
	// REPLACE the matrix's selection instead of intersecting with it, quietly
	// running a different (usually larger) set. Go's -run has no AND form, so
	// the safe representation is to run such a unit whole.
	if u.runFilter != "" {
		return false
	}
	for _, p := range []string{"internal/broker", "internal/cluster", "test/d4", "test/d5"} {
		if strings.Contains(u.pkg, p) {
			return true
		}
	}
	return false
}

// listPackageTests asks the package which top-level tests it has, under the
// same tags the unit will be built with — a different tag set is a different
// test list, and using the wrong one would silently skip tests.
func listPackageTests(u unit) ([]string, error) {
	// origin: hot-update S7 plan lead L17 / T52; preserve default fuzz seed replay.
	args := []string{"test", "-list", "^(Test|Fuzz|Benchmark)"}
	if u.race {
		// -race also selects the `race` build tag. Listing without it can miss
		// race-only tests and then shard a smaller set than the command declares.
		args = append(args, "-race")
	}
	if u.tags != "" {
		args = append(args, "-tags", u.tags)
	}
	args = append(args, u.pkg)
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	// Gate tests start in test/e2e/parallel; the real provider must list the
	// same repository packages the runner lists from its root directory.
	// origin: hot-update S7 plan lead L17 / T52; independent seed census.
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	return packageListedTests(string(out)), nil
}

// Go prints a package summary after its list. Bind each preceding declaration
// to that summary before combining recursive output; bare names are retained
// only for injected list providers used by the runner's unit controls.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func packageListedTests(out string) []string {
	var names, pending []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if (strings.HasPrefix(line, "Test") || strings.HasPrefix(line, "Fuzz") || strings.HasPrefix(line, "Benchmark")) && !strings.ContainsAny(line, " \t") {
			pending = append(pending, line)
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && (fields[0] == "ok" || fields[0] == "?") && strings.HasPrefix(fields[1], "github.com/LinZiyang666/tether/") {
			pkg := "./" + strings.TrimPrefix(fields[1], "github.com/LinZiyang666/tether/")
			if len(pending) == 0 {
				names = append(names, pkg+":")
			}
			for _, name := range pending {
				names = append(names, pkg+":"+name)
			}
			pending = nil
		}
	}
	names = append(names, pending...)
	sort.Strings(names)
	return names
}

func packagePatternContains(pattern, pkg string) bool {
	base := strings.TrimSuffix(pattern, "/...")
	pkg = strings.TrimSuffix(pkg, "/...")
	return base == pkg || strings.HasSuffix(pattern, "/...") && strings.HasPrefix(pkg, base+"/")
}

func partitionIdentity(pattern, entry string) (string, string) {
	if pkg, name, ok := strings.Cut(entry, ":"); ok {
		return pkg, name
	}
	return pattern, entry
}

func namedPartition(u unit, entry string) unit {
	su := u
	su.pkg, entry = partitionIdentity(u.pkg, entry)
	su.runFilter = "^" + entry + "$"
	su.name = u.name + "[" + su.pkg + ":" + entry + "]"
	if su.pkg == u.pkg {
		su.name = u.name + "[" + entry + "]"
	}
	if strings.HasPrefix(entry, "Benchmark") {
		// Benchmark names are part of the frozen denominator. One iteration
		// exercises them under race without an unbounded calibration loop.
		su.baseArgs = u.goTestArgs()
		su.baseArgs = append(su.baseArgs[:len(su.baseArgs)-1:len(su.baseArgs)-1], "-bench", su.runFilter, "-benchtime", "1x")
		su.extra = "-bench=" + su.runFilter + " -benchtime=1x"
	}
	return su
}
