package broker

import (
	"fmt"
	"testing"

	"github.com/LinZiyang666/tether/internal/port"
)

// proxy_rebalance_test.go (C-rebalance) — adversarial table tests for the PURE greedy planner. The
// DB/raft execution path is exercised by the gated cluster integration drills; here we pin the math:
// convergence to max−min ≤ 1, idempotence, determinism, and the boundary/defensive cases.

func mkProxyAllocs(homeCounts map[string]int) []*port.Allocation {
	var out []*port.Allocation
	i := 0
	// Deterministic construction order (sorted homes) so a test's input isn't itself nondeterministic.
	for _, home := range sortedKeys(homeCounts) {
		for c := 0; c < homeCounts[home]; c++ {
			out = append(out, &port.Allocation{
				Port: 20000 + i, SID: "s", NID: fmt.Sprintf("n%d", i), Name: "__proxy__", HomeBroker: home,
			})
			i++
		}
	}
	return out
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// tiny insertion sort (avoid importing sort in the test for a 1–4 element slice)
	for a := 1; a < len(out); a++ {
		for b := a; b > 0 && out[b-1] > out[b]; b-- {
			out[b-1], out[b] = out[b], out[b-1]
		}
	}
	return out
}

// applyPlan returns the per-home load after applying every move (the post-rebalance distribution).
func applyPlan(allocs []*port.Allocation, plan []plannedMove) map[string]int {
	home := make(map[string]string, len(allocs)) // port → current home
	load := make(map[string]int)
	for _, a := range allocs {
		home[fmt.Sprint(a.Port)] = a.HomeBroker
		load[a.HomeBroker]++
	}
	for _, m := range plan {
		k := fmt.Sprint(m.a.Port)
		load[home[k]]--
		home[k] = m.to
		load[m.to]++
	}
	return load
}

func maxMinGap(voters []string, load map[string]int) int {
	hi, lo := -1, int(^uint(0)>>1)
	for _, v := range voters {
		n := load[v]
		if n > hi {
			hi = n
		}
		if n < lo {
			lo = n
		}
	}
	return hi - lo
}

func TestPlanProxyRebalanceConverges(t *testing.T) {
	cases := []struct {
		name      string
		voters    []string
		homes     map[string]int // initial per-home proxy count
		wantMoves int            // exact expected move count
	}{
		{"already balanced 2x2", []string{"a", "b"}, map[string]int{"a": 2, "b": 2}, 0},
		{"off-by-one is balanced", []string{"a", "b"}, map[string]int{"a": 3, "b": 2}, 0},
		{"all on one of two", []string{"a", "b"}, map[string]int{"a": 4}, 2},
		{"all on one of three", []string{"a", "b", "c"}, map[string]int{"a": 6}, 4},
		{"fill a fresh empty voter", []string{"a", "b", "c"}, map[string]int{"a": 2, "b": 2}, 1},
		{"lopsided three-way", []string{"a", "b", "c"}, map[string]int{"a": 5, "b": 1}, 3}, // 5/1/0 → 2/2/2: a sheds 3
		{"single voter never moves", []string{"a"}, map[string]int{"a": 9}, 0},
		{"no proxies", []string{"a", "b"}, map[string]int{}, 0},
		// R2 boundary rows:
		{"one proxy three voters", []string{"a", "b", "c"}, map[string]int{"a": 1}, 0},                       // 1/0/0, gap 1
		{"two proxies three voters", []string{"a", "b", "c"}, map[string]int{"a": 2}, 1},                     // → 1/1/0
		{"gap-one with empty voter is balanced", []string{"a", "b", "c"}, map[string]int{"a": 1, "b": 1}, 0}, // 1/1/0 left alone — don't chase perfect evenness
		{"two overloaded one empty", []string{"a", "b", "c"}, map[string]int{"a": 4, "c": 4}, 2},             // 4/0/4 → {3,2,3}
		{"seven across three", []string{"a", "b", "c"}, map[string]int{"a": 7}, 4},                           // → 3/2/2
		{"hundred on one of two", []string{"a", "b"}, map[string]int{"a": 100}, 50},
		{"hundred on one of three", []string{"a", "b", "c"}, map[string]int{"a": 100}, 66},          // → 34/33/33
		{"skip dead-home but still move", []string{"a", "b"}, map[string]int{"dead": 3, "a": 4}, 2}, // dead skipped; a4/b0 → 2/2
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			allocs := mkProxyAllocs(tc.homes)
			plan := planProxyRebalance(tc.voters, allocs)
			if len(plan) != tc.wantMoves {
				t.Fatalf("move count = %d, want %d (plan=%+v)", len(plan), tc.wantMoves, plan)
			}
			// Post-plan distribution must be balanced (max−min ≤ 1) when ≥2 voters.
			if len(tc.voters) >= 2 && len(allocs) > 0 {
				if gap := maxMinGap(tc.voters, applyPlan(allocs, plan)); gap > 1 {
					t.Fatalf("post-rebalance gap = %d, want ≤ 1", gap)
				}
			}
			// Every move is between two DISTINCT eligible voters (never a self-move, never FROM/TO a
			// non-eligible/dead home — those belong to the reaper).
			for _, m := range plan {
				if m.from == m.to {
					t.Fatalf("self-move %s->%s", m.from, m.to)
				}
				if !contains(tc.voters, m.to) {
					t.Fatalf("move target %q is not an eligible voter", m.to)
				}
				if !contains(tc.voters, m.from) {
					t.Fatalf("move source %q is not an eligible voter (dead-home proxies belong to the reaper)", m.from)
				}
			}
		})
	}
}

// TestPlanProxyRebalanceIdempotent: running the planner on the post-rebalance distribution yields NO
// further moves (a converge script can re-run safely; re-run is a no-op once balanced).
func TestPlanProxyRebalanceIdempotent(t *testing.T) {
	voters := []string{"a", "b", "c"}
	allocs := mkProxyAllocs(map[string]int{"a": 7})
	plan := planProxyRebalance(voters, allocs)
	if len(plan) == 0 {
		t.Fatal("expected moves on a 7-on-one-of-three start")
	}
	// Re-materialize the allocations at their post-rebalance homes, then plan again → must be empty.
	post := applyPlan(allocs, plan)
	allocs2 := mkProxyAllocs(post)
	if plan2 := planProxyRebalance(voters, allocs2); len(plan2) != 0 {
		t.Fatalf("re-plan on a balanced distribution moved %d (want 0): %+v", len(plan2), plan2)
	}
}

// TestPlanProxyRebalanceDeterministic: same input → byte-identical plan (replica/replay safety). Tie
// homes are broken by node_id, so the chosen targets are stable.
func TestPlanProxyRebalanceDeterministic(t *testing.T) {
	voters := []string{"c", "a", "b"} // intentionally unsorted input
	allocs := mkProxyAllocs(map[string]int{"a": 4})
	p1 := planProxyRebalance(voters, allocs)
	p2 := planProxyRebalance(voters, mkProxyAllocs(map[string]int{"a": 4}))
	if len(p1) != len(p2) {
		t.Fatalf("nondeterministic move count: %d vs %d", len(p1), len(p2))
	}
	for i := range p1 {
		if p1[i].to != p2[i].to || p1[i].from != p2[i].from {
			t.Fatalf("move %d differs: %s->%s vs %s->%s", i, p1[i].from, p1[i].to, p2[i].from, p2[i].to)
		}
		// R2 #7: also pin WHICH proxy (byHome pop order) is chosen — full plan reproducibility.
		if p1[i].a.Port != p2[i].a.Port {
			t.Fatalf("move %d picked a different proxy: port %d vs %d", i, p1[i].a.Port, p2[i].a.Port)
		}
	}
}

// TestPlanProxyRebalanceSkipsNonEligibleHome: a proxy whose home is NOT an eligible voter (dead/draining)
// is left to the reaper — the planner must not count it toward a voter's load nor move it.
func TestPlanProxyRebalanceSkipsNonEligibleHome(t *testing.T) {
	voters := []string{"a", "b"}
	// "dead" is not in voters: 3 on "dead", 1 on "a", 0 on "b". The dead-homed proxies are skipped, so
	// the eligible load is a=1,b=0 → already balanced (gap 1) → zero moves.
	allocs := mkProxyAllocs(map[string]int{"dead": 3, "a": 1})
	plan := planProxyRebalance(voters, allocs)
	if len(plan) != 0 {
		t.Fatalf("expected 0 moves (dead-homed proxies belong to the reaper), got %d: %+v", len(plan), plan)
	}
	for _, m := range plan {
		if m.from == "dead" {
			t.Fatal("planner moved a dead-homed proxy (must be left to the reaper)")
		}
	}
}

// (contains lives in proxy_cluster_test.go — reused here.)
