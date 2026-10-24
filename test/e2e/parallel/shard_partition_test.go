package main

import "testing"

// origin: S8 internal review SIGNED-ROLL-FIXTURES F1–F5; S8 plan L21
func TestExplicitPartitionRetainsEveryOtherTestAndSeed(t *testing.T) {
	whole := unit{matrix: "TestHotUpdateBrokerMatrix", pkg: "./cmd/tether/...", race: true, timeo: "600s"}
	exact := whole
	exact.hasRun = true
	exact.runFilter = "^TestHeavy$"
	for _, shards := range []int{0, 1, 8} {
		calls := 0
		out, err := shardPackagesWithList([]unit{whole, exact}, shards, func(u unit) ([]string, error) { calls++; return []string{"TestHeavy", "TestOther", "FuzzSeeds"}, nil })
		if err != nil || calls != 1 || len(out) != 3 {
			t.Fatalf("partition closure: calls=%d units=%+v error=%v", calls, out, err)
		}
		counts := map[string]int{}
		for _, u := range out {
			counts[u.runFilter]++
			if !u.race || u.timeo != "600s" {
				t.Fatal("partition changed race ownership or budget")
			}
		}
		for _, name := range []string{"TestHeavy", "TestOther", "FuzzSeeds"} {
			if counts["^"+name+"$"] != 1 {
				t.Fatalf("%s has %d partitions", name, counts["^"+name+"$"])
			}
		}
	}
	for _, mutate := range []func(*unit){func(u *unit) { u.tags = "other" }, func(u *unit) { u.race = false }, func(u *unit) { u.timeo = "30s" }, func(u *unit) { u.extra = "-short" }} {
		different := exact
		mutate(&different)
		out, err := shardPackagesWithList([]unit{whole, different}, 0, func(unit) ([]string, error) { return []string{"TestHeavy", "TestOther", "FuzzSeeds"}, nil })
		if err != nil || len(out) != 4 {
			t.Fatalf("different configuration suppressed full ownership: %+v %v", out, err)
		}
	}
}
