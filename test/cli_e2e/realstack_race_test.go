//go:build linux && race

// realstack_race_test.go — flips raceEnabled when this test binary itself was
// built with -race, so buildTether instruments the product binary too (S1
// review E4). Nothing else; the flag's effect is pinned by
// TestRealStackBuildPassesRaceThrough.
package cli_e2e_test

func init() { raceEnabled = true }
