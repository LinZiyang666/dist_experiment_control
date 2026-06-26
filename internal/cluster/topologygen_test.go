package cluster

import (
	"testing"
	"time"

	"github.com/nats-io/nkeys"
)

// topologygen_test.go (C3 §D-E/§D-G) — the dedicated topology_generation counter: it advances ONLY on
// changes that alter the rendered cluster nats.conf (mesh ENTER / LEAVE / bus-nkey fill), NOT on
// within-mesh phase transitions (which would flap the cluster to DEGRADED), is change-gated (a no-op
// does not inflate it), and is DISTINCT from roster_generation.

func tgen(t *testing.T, f *fsm) uint64 {
	t.Helper()
	g, err := TopologyGeneration(f.db)
	if err != nil {
		t.Fatalf("read topology_generation: %v", err)
	}
	return g
}

func TestTopologyGenMeshEnterOnly(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Date(2026, 6, 24, 3, 0, 0, 0, time.UTC)

	if g := tgen(t, f); g != 0 {
		t.Fatalf("absent topology counter must read 0, got %d", g)
	}

	// Upsert (join) does NOT bump the topology counter (custom applier, len(Body)==1).
	seed, pub := d7GenKey(t)
	nonce := "tn-1"
	sig := d7Sign(t, seed, JoinSignBytes("t1", pub, nonce))
	up, _ := PlanClusterNodeUpsert(d7UpsertInput("t1", pub, nonce, sig))
	d7Apply(t, f, 1, up)
	if g := tgen(t, f); g != 0 {
		t.Fatalf("upsert must not bump topology_generation, got %d", g)
	}

	// Mesh ENTER (→CATCHING_UP) → topology bump.
	d7Apply(t, f, 2, mustPhase(t, "t1", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, now))
	enter := tgen(t, f)
	if enter == 0 {
		t.Fatal("mesh ENTER (→CATCHING_UP) must advance topology_generation")
	}

	// WITHIN-mesh transition (CATCHING_UP→VOTER) must NOT bump topology (the rendered conf is identical) —
	// but it DOES bump roster_generation, proving the two counters are independent (the whole point of a
	// dedicated topology counter: a routine within-mesh / cert-rotate change must not flap DEGRADED).
	rosterBefore := rgen(t, f)
	d7Apply(t, f, 3, mustPhase(t, "t1", "VOTER", []string{"CATCHING_UP"}, now))
	if g := tgen(t, f); g != enter {
		t.Fatalf("a within-mesh transition (→VOTER) must NOT bump topology_generation: was %d now %d", enter, g)
	}
	if rg := rgen(t, f); rg <= rosterBefore {
		t.Fatalf("→VOTER must still bump roster_generation (counters are independent): %d -> %d", rosterBefore, rg)
	}

	// Drain → retire are WITHIN-mesh (rows stay so routes don't vanish mid-drain) → no topology bump.
	d7Apply(t, f, 4, mustPhase(t, "t1", "DRAINING", []string{"VOTER"}, now))
	d7Apply(t, f, 5, mustPhase(t, "t1", "RETIRING", []string{"DRAINING"}, now))
	if g := tgen(t, f); g != enter {
		t.Fatalf("DRAINING/RETIRING are within-mesh and must NOT bump topology_generation: was %d now %d", enter, g)
	}

	// Mesh LEAVE (remove the RETIRING row) → topology bump.
	rm, _ := PlanClusterNodeRemove("t1", now)
	d7Apply(t, f, 6, rm)
	if g := tgen(t, f); g <= enter {
		t.Fatalf("mesh LEAVE (remove) must advance topology_generation: %d -> %d", enter, g)
	}
}

func TestTopologyGenChangeGatedNoOp(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Date(2026, 6, 24, 4, 0, 0, 0, time.UTC)
	seed, pub := d7GenKey(t)
	nonce := "tn-2"
	sig := d7Sign(t, seed, JoinSignBytes("t2", pub, nonce))
	up, _ := PlanClusterNodeUpsert(d7UpsertInput("t2", pub, nonce, sig))
	d7Apply(t, f, 1, up)
	d7Apply(t, f, 2, mustPhase(t, "t2", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, now))
	g := tgen(t, f)

	// A no-op ENTER (disallowed predecessor → RowsAffected 0) must NOT inflate the counter (idle-zero-writes).
	noop, _ := PlanClusterNodePhase("t2", "CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}, "", now)
	d7Apply(t, f, 3, noop)
	if g2 := tgen(t, f); g2 != g {
		t.Fatalf("a no-op mesh-enter must NOT inflate topology_generation: was %d now %d", g, g2)
	}
}

func TestPlanClusterBusNkeySetRejectsBadNkey(t *testing.T) {
	now := time.Date(2026, 6, 24, 5, 0, 0, 0, time.UTC)
	if _, err := PlanClusterBusNkeySet("t3", "not-a-valid-nkey", now); err == nil {
		t.Fatal("PlanClusterBusNkeySet must reject a non-user-key (a garbage nkey breaks every replica's nats-server -t)")
	}
	// A valid NATS user public key (prefix U) is accepted.
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	if _, err := PlanClusterBusNkeySet("t3", pub, now); err != nil {
		t.Fatalf("PlanClusterBusNkeySet must accept a valid user public key: %v", err)
	}
}
