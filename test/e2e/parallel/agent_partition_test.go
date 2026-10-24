package main

import "testing"

// The cumulative agent race exhausted 600s while independent tests were still
// waiting to start. Every discovered test and fuzz seed needs a separate unit,
// including the recursive ssproxy leaf; explicit transactions remain exact once.
// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F4; plan L17 / §11.1
func TestAgentOwnerPartitionsEveryDiscoveredTest(t *testing.T) {
	whole := unit{matrix: "TestHotUpdateBrokerMatrix", pkg: "./internal/agent/...", race: true, timeo: "600s"}
	explicit := whole
	explicit.hasRun, explicit.runFilter = true, "^TestTransaction$"
	names := []string{"TestAgent", "TestTransaction", "TestProxy", "FuzzAgent"}
	for _, shards := range []int{0, 1, 8} {
		calls := 0
		out, err := shardPackagesWithList([]unit{whole, explicit}, shards, func(got unit) ([]string, error) {
			calls++
			if got.pkg != whole.pkg || got.race != whole.race || got.tags != whole.tags {
				t.Fatalf("discovery changed the original build: %+v", got)
			}
			return names, nil
		})
		if err != nil || calls != 1 || len(out) != len(names) {
			t.Fatalf("agent discovery was not partitioned: calls=%d units=%+v error=%v", calls, out, err)
		}
		counts := map[string]int{}
		for _, got := range out {
			if got.pkg != whole.pkg || !got.race || got.timeo != "600s" || got.tags != "" || got.extra != "" {
				t.Fatalf("agent partition changed its build or budget: %+v", got)
			}
			counts[got.runFilter]++
		}
		for _, name := range names {
			if counts["^"+name+"$"] != 1 {
				t.Fatalf("%s has %d exact units", name, counts["^"+name+"$"])
			}
		}
	}
}
