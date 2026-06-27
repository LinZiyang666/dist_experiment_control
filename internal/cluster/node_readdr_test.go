package cluster

import (
	"testing"
	"time"
)

// node_readdr_test.go (v0.4.2 phase-fluidity) — the online raft/NATS advertise-address rebind ops
// that replace force-single for the routine localhost->public case. The load-bearing distinction:
// raft_addr is NOT in the agent-facing roster SELECT nor the rendered nats.conf, so
// OpClusterNodeReaddr must NOT bump roster_generation (else every rebind spuriously re-pushes the
// roster to agents); the NATS route IS rendered into nats.conf, so OpClusterNodeRoute MUST bump
// topology_generation. Both are change-gated (an unchanged re-propose is an idempotent no-op).

func nodeStrCol(t *testing.T, f *fsm, nodeID, col string) string {
	t.Helper()
	var v string
	if err := f.db.QueryRow(`SELECT `+col+` FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&v); err != nil {
		t.Fatalf("read %s for %s: %v", col, nodeID, err)
	}
	return v
}

// seedReaddrNode upserts a roster row (raft_addr 10.0.0.9:7400, nats_route nats://10.0.0.9:6222)
// at raft index 1 so the readdr/route UPDATEs have a row to target.
func seedReaddrNode(t *testing.T, f *fsm, nodeID string) {
	t.Helper()
	seed, pub := d7GenKey(t)
	nonce := "rn-" + nodeID
	sig := d7Sign(t, seed, JoinSignBytes(nodeID, pub, nonce))
	up, _ := PlanClusterNodeUpsert(d7UpsertInput(nodeID, pub, nonce, sig))
	d7Apply(t, f, 1, up)
}

func TestPlanClusterNodeReaddrUpdatesAddrNoRosterBump(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seedReaddrNode(t, f, "n1")
	if got := nodeStrCol(t, f, "n1", "raft_addr"); got != "10.0.0.9:7400" {
		t.Fatalf("seeded raft_addr = %q, want 10.0.0.9:7400", got)
	}
	rosterBefore := rgen(t, f)
	topoBefore := tgen(t, f)

	cmd, err := PlanClusterNodeReaddr("n1", "203.0.113.5:7400")
	if err != nil {
		t.Fatalf("PlanClusterNodeReaddr: %v", err)
	}
	d7Apply(t, f, 2, cmd)
	if got := nodeStrCol(t, f, "n1", "raft_addr"); got != "203.0.113.5:7400" {
		t.Fatalf("after readdr raft_addr = %q, want 203.0.113.5:7400", got)
	}
	// THE load-bearing claim: raft_addr is neither roster- nor conf-facing, so a rebind must bump
	// NEITHER generation (no spurious roster recompute / agent push, no DEGRADED flap). A regression
	// appending either gen stmt to readdr would be caught here (review m5).
	if rg := rgen(t, f); rg != rosterBefore {
		t.Fatalf("OpClusterNodeReaddr must NOT bump roster_generation (raft_addr is not roster-facing): %d -> %d", rosterBefore, rg)
	}
	if tg := tgen(t, f); tg != topoBefore {
		t.Fatalf("OpClusterNodeReaddr must NOT bump topology_generation (raft_addr is not in the rendered conf): %d -> %d", topoBefore, tg)
	}

	// Idempotent re-apply with the SAME addr: value unchanged (change-gated no-op, never an error).
	d7Apply(t, f, 3, cmd)
	if got := nodeStrCol(t, f, "n1", "raft_addr"); got != "203.0.113.5:7400" {
		t.Fatalf("idempotent re-apply changed raft_addr to %q", got)
	}
}

func TestPlanClusterNodeRouteUpdatesRouteTopologyBump(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seedReaddrNode(t, f, "n2")
	now := time.Date(2026, 6, 27, 1, 0, 0, 0, time.UTC)
	topoBefore := tgen(t, f)
	rosterBefore := rgen(t, f)

	cmd, err := PlanClusterNodeRoute("n2", "nats://203.0.113.5:6222", now)
	if err != nil {
		t.Fatalf("PlanClusterNodeRoute: %v", err)
	}
	d7Apply(t, f, 2, cmd)
	if got := nodeStrCol(t, f, "n2", "nats_route"); got != "nats://203.0.113.5:6222" {
		t.Fatalf("after route nats_route = %q, want nats://203.0.113.5:6222", got)
	}
	// nats_route is BOTH rendered into nats.conf (topology) AND in the signed roster body (review m4),
	// so the change MUST bump BOTH generations.
	topoAfter := tgen(t, f)
	if topoAfter <= topoBefore {
		t.Fatalf("OpClusterNodeRoute must bump topology_generation (nats_route is conf-facing): %d -> %d", topoBefore, topoAfter)
	}
	rosterAfter := rgen(t, f)
	if rosterAfter <= rosterBefore {
		t.Fatalf("OpClusterNodeRoute must bump roster_generation (nats_route is in the signed roster body): %d -> %d", rosterBefore, rosterAfter)
	}
	// Idempotent re-apply: no further bump of EITHER generation (change-gated).
	d7Apply(t, f, 3, cmd)
	if g := tgen(t, f); g != topoAfter {
		t.Fatalf("idempotent re-apply must NOT bump topology_generation again: %d -> %d", topoAfter, g)
	}
	if g := rgen(t, f); g != rosterAfter {
		t.Fatalf("idempotent re-apply must NOT bump roster_generation again: %d -> %d", rosterAfter, g)
	}
}

func TestPlanClusterNodeReaddrRejectsBadAddr(t *testing.T) {
	if _, err := PlanClusterNodeReaddr("n1", "not-a-host-port"); err == nil {
		t.Fatal("PlanClusterNodeReaddr must reject a non host:port advertise address")
	}
	if _, err := PlanClusterNodeReaddr("n1", "203.0.113.5:7400"); err != nil {
		t.Fatalf("PlanClusterNodeReaddr must accept a valid host:port: %v", err)
	}
	// LitText safety (review m5): a NUL / invalid-UTF-8 in either arg must be rejected before baking.
	if _, err := PlanClusterNodeReaddr("bad\x00id", "203.0.113.5:7400"); err == nil {
		t.Fatal("PlanClusterNodeReaddr must reject a NUL in node_id (LitText)")
	}
	if _, err := PlanClusterNodeReaddr("n1", "203.0.113.5:7400\x00"); err == nil {
		t.Fatal("PlanClusterNodeReaddr must reject a NUL in the addr")
	}
}

func TestPlanClusterNodeRouteRejectsBadRoute(t *testing.T) {
	now := time.Date(2026, 6, 27, 2, 0, 0, 0, time.UTC)
	for _, bad := range []string{"http://h:6222", "nats://", "nats://h", "garbage", "nats://h:6222\x00"} {
		if _, err := PlanClusterNodeRoute("n1", bad, now); err == nil {
			t.Errorf("PlanClusterNodeRoute must reject %q (review m9: structural + LitText validation)", bad)
		}
	}
	if _, err := PlanClusterNodeRoute("n1", "nats://203.0.113.5:6222", now); err != nil {
		t.Fatalf("PlanClusterNodeRoute must accept a valid nats:// route: %v", err)
	}
}

// TestKnownOpsAppliersSymmetry (review m6) locks the never-wedge invariant: every knownOps entry
// MUST have a defaultAppliers entry — fsm.go turns a knownOp-without-applier into a PLAIN un-wrapped
// error (NOT errAppliedRejected) → raft fail-stop retry → cluster-wide panic on log replay; and the
// reverse (an applier for an op decodeCommand poisons) is dead. Also pins the two v0.4.2 ops present.
func TestKnownOpsAppliersSymmetry(t *testing.T) {
	appliers := defaultAppliers()
	for op := range knownOps {
		if appliers[op] == nil {
			t.Errorf("knownOps[%s] has NO applier — a committed entry would fail-stop the FSM cluster-wide", op)
		}
	}
	for op := range appliers {
		if !knownOps[op] {
			t.Errorf("defaultAppliers()[%s] is not in knownOps — decodeCommand poisons it before the applier runs", op)
		}
	}
	for _, op := range []OpType{OpClusterNodeReaddr, OpClusterNodeRoute} {
		if !knownOps[op] || appliers[op] == nil {
			t.Errorf("v0.4.2 op %s must be in BOTH knownOps and defaultAppliers", op)
		}
	}
}
