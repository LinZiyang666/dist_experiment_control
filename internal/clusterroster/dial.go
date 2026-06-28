package clusterroster

import "strings"

// dial.go — the floor-last dial-string builder shared by the agent (effectiveDialURLs) and the ctl/cli
// (ResolveDial). Lifted verbatim from the agent so BOTH consumers honor the SAME invariant with no drift:
// a comma-joined nats.Connect server list in tiers — learned roster URLs (VOTER-first) → verified seed
// endpoints → the configured floor (split on ",", LAST, never removed) — de-duplicated in first-seen
// order. With empty learned+seeds and floorCSV==base it returns base unchanged (the non-cluster
// byte-equivalence invariant both consumers rely on).
func BuildDialString(learned, seeds []string, floorCSV string) string {
	seen := make(map[string]struct{}, len(learned)+len(seeds)+2)
	out := make([]string, 0, len(learned)+len(seeds)+2)
	add := func(u string) {
		u = strings.TrimSpace(u)
		if u == "" {
			return
		}
		if _, ok := seen[u]; ok {
			return
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	for _, u := range learned { // roster (steady-state, NATS-refreshed) first
		add(u)
	}
	for _, u := range seeds { // verified seed endpoints (cold-start / rotation) next
		add(u)
	}
	for _, s := range strings.Split(floorCSV, ",") { // configured floor, last, never removed
		add(s)
	}
	return strings.Join(out, ",")
}
