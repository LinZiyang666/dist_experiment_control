package cluster

import (
	"testing"
	"time"
)

// roster_gen_test.go (C1 §8 / §D-2) — the monotone roster_generation counter: it advances on a REAL
// membership/phase change, is CHANGE-GATED (a no-op transition does not inflate it), and NEVER
// regresses across a full add→retire cycle (the exact scenario the derived-max generation got wrong).

func rgen(t *testing.T, f *fsm) uint64 {
	t.Helper()
	g, err := RosterGeneration(f.db)
	if err != nil {
		t.Fatalf("read roster_generation: %v", err)
	}
	return g
}

func TestRosterGenMonotoneAndChangeGated(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Date(2026, 6, 23, 3, 0, 0, 0, time.UTC)

	if g := rgen(t, f); g != 0 {
		t.Fatalf("absent counter must read 0, got %d", g)
	}

	// Upsert (join) does NOT bump (custom applier, len(Body)==1 — by design; the immediate
	// PENDING→CATCHING_UP phase op below bumps before the node is a dialable VOTER).
	seed, pub := d7GenKey(t)
	nonce := "n-gen"
	sig := d7Sign(t, seed, JoinSignBytes("g1", pub, nonce))
	up, _ := PlanClusterNodeUpsert(d7UpsertInput("g1", pub, nonce, sig))
	d7Apply(t, f, 1, up)
	if g := rgen(t, f); g != 0 {
		t.Fatalf("upsert must not bump the counter (covered by the following phase op), got %d", g)
	}

	// Real phase change → bump.
	d7Apply(t, f, 2, mustPhase(t, "g1", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, now))
	g1 := rgen(t, f)
	if g1 == 0 {
		t.Fatal("a real phase transition must advance the counter")
	}

	// No-op phase change (disallowed predecessor → RowsAffected 0) must NOT advance it (change-gated).
	noop, _ := PlanClusterNodePhase("g1", "VOTER", []string{"JOIN_VERIFIED_PENDING_VOTER"}, "", now)
	d7Apply(t, f, 3, noop)
	if g := rgen(t, f); g != g1 {
		t.Fatalf("a no-op transition must NOT inflate the counter: was %d now %d", g1, g)
	}

	// Real transition to VOTER → advance.
	d7Apply(t, f, 4, mustPhase(t, "g1", "VOTER", []string{"CATCHING_UP"}, now))
	g2 := rgen(t, f)
	if g2 <= g1 {
		t.Fatalf("counter must advance on a real transition: %d -> %d", g1, g2)
	}
}

// TestRosterGenNeverRegressesOnRetireOfNewest walks a node through a full add→retire cycle and
// asserts the counter is STRICTLY non-decreasing at every step — including the final remove, the
// exact point where the old derived-max generation regressed (and would have made a strict-monotone
// agent reject the corrected roster).
func TestRosterGenNeverRegressesOnRetireOfNewest(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Date(2026, 6, 23, 3, 0, 0, 0, time.UTC)
	seed, pub := d7GenKey(t)
	nonce := "n-ret"
	sig := d7Sign(t, seed, JoinSignBytes("r1", pub, nonce))
	up, _ := PlanClusterNodeUpsert(d7UpsertInput("r1", pub, nonce, sig))
	d7Apply(t, f, 1, up)

	prev := rgen(t, f)
	steps := []*Command{
		mustPhase(t, "r1", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, now),
		mustPhase(t, "r1", "VOTER", []string{"CATCHING_UP"}, now),
		mustPhase(t, "r1", "DRAINING", []string{"VOTER"}, now),
		mustPhase(t, "r1", "RETIRING", []string{"DRAINING"}, now),
	}
	idx := uint64(2)
	for _, c := range steps {
		d7Apply(t, f, idx, c)
		idx++
		g := rgen(t, f)
		if g < prev {
			t.Fatalf("counter regressed: %d -> %d", prev, g)
		}
		prev = g
	}
	// Final remove of the (newest) node — the derived-max regression point. The counter must advance.
	rm, _ := PlanClusterNodeRemove("r1", now)
	d7Apply(t, f, idx, rm)
	if g := rgen(t, f); g <= prev {
		t.Fatalf("remove must still advance the counter (no regression): %d -> %d", prev, g)
	}
}
