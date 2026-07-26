package main

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
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
	if shards < 2 {
		return units, nil
	}
	var out []unit
	for _, u := range units {
		if u.isWhole() || !u.shardable() {
			out = append(out, u)
			continue
		}
		names, err := listPackageTests(u)
		if err != nil || len(names) < shards*2 {
			// Too few tests to be worth splitting, or the list failed: run whole.
			out = append(out, u)
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
	args := []string{"test", "-list", "^Test"}
	if u.race {
		// -race also selects the `race` build tag. Listing without it can miss
		// race-only tests and then shard a smaller set than the command declares.
		args = append(args, "-race")
	}
	if u.tags != "" {
		args = append(args, "-tags", u.tags)
	}
	args = append(args, u.pkg)
	out, err := exec.Command("go", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%v: %s", err, out)
	}
	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Test") && !strings.Contains(line, " ") {
			names = append(names, line)
		}
	}
	sort.Strings(names)
	return names, nil
}
