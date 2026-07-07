package cluster

import (
	"testing"
	"time"
)

func TestPlanClusterSeedsPublishValidation(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	for _, eps := range [][]string{nil, {"http://x"}, {"nats://"}, {""}, {"ftp://b:21"}} {
		if _, err := PlanClusterSeedsPublish(eps, "https://c/c.json", now); err == nil {
			t.Fatalf("endpoints %v must be rejected", eps)
		}
	}
	many := make([]string, MaxSeedEndpoints+1)
	for i := range many {
		many[i] = "nats://b:4222"
	}
	if _, err := PlanClusterSeedsPublish(many, "https://c", now); err == nil {
		t.Fatal("too many endpoints must reject")
	}
	if _, err := PlanClusterSeedsPublish([]string{"https://c"}, "ftp://x", now); err == nil {
		t.Fatal("a non-http bootstrap must reject")
	}
	if _, err := PlanClusterSeedsPublish([]string{"wss://b1:443"}, "https://c/c.json", now); err != nil {
		t.Fatalf("a valid publish must plan: %v", err)
	}
}

// TestSeedGenMonotoneAndNoRosterBump: a seeds publish advances seed_generation (strictly, monotone)
// and leaves roster_generation UNCHANGED (D-8 — seeds and membership are independent axes).
func TestSeedGenMonotoneAndNoRosterBump(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Unix(1700000000, 0).UTC()
	rgBefore := rgen(t, f) // roster_generation (helper in roster_gen_test.go, same package)

	cmd, _ := PlanClusterSeedsPublish([]string{"wss://b1:443"}, "https://c/c.json", now)
	d7Apply(t, f, 1, cmd)
	_, _, sg1, err := Seeds(f.db)
	if err != nil {
		t.Fatal(err)
	}
	if sg1 == 0 {
		t.Fatal("seed_generation must advance on publish")
	}
	if rgen(t, f) != rgBefore {
		t.Fatal("seeds publish must NOT bump roster_generation (D-8)")
	}

	cmd2, _ := PlanClusterSeedsPublish([]string{"wss://b1:443", "wss://b2:443"}, "https://c/c.json", now)
	d7Apply(t, f, 2, cmd2)
	eps, bootstrap, sg2, _ := Seeds(f.db)
	if sg2 <= sg1 {
		t.Fatalf("seed_generation must strictly increase: %d -> %d", sg1, sg2)
	}
	if len(eps) != 2 || bootstrap != "https://c/c.json" {
		t.Fatalf("seeds read back wrong: %v %q", eps, bootstrap)
	}
}
