package cluster

import (
	"testing"
	"time"
)

// g2_prune_test.go (G2 #12) — PlanClusterNodePrune is the force-single / ghost-removal delete: it DELETEs
// roster rows by node_id UNCONDITIONALLY (any phase), bumping BOTH generations (a prune is a roster change
// AND a mesh leave), change-gated so a re-prune of absent rows is an idempotent no-op. The caller proves
// the node is out of the committed raft config, so the delete cannot fork quorum.

func nodeCount(t *testing.T, f *fsm, nodeID string) int {
	t.Helper()
	var n int
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", nodeID, err)
	}
	return n
}

// seedPruneNode upserts a VOTER roster row at a caller-chosen raft index (so several can be seeded).
func seedPruneNode(t *testing.T, f *fsm, nodeID string, idx uint64) {
	t.Helper()
	seed, pub := d7GenKey(t)
	nonce := "pn-" + nodeID
	sig := d7Sign(t, seed, JoinSignBytes(nodeID, pub, nonce))
	up, err := PlanClusterNodeUpsert(d7UpsertInput(nodeID, pub, nonce, sig))
	if err != nil {
		t.Fatalf("seed upsert %s: %v", nodeID, err)
	}
	d7Apply(t, f, idx, up)
}

func TestPlanClusterNodePruneDeletesByIDBumpsBothGens(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	seedPruneNode(t, f, "n1", 1)
	seedPruneNode(t, f, "n2", 2)
	seedPruneNode(t, f, "n3", 3)
	for _, id := range []string{"n1", "n2", "n3"} {
		if nodeCount(t, f, id) != 1 {
			t.Fatalf("seed %s missing", id)
		}
	}
	rosterBefore := rgen(t, f)
	topoBefore := tgen(t, f)
	now := time.Date(2026, 7, 6, 1, 0, 0, 0, time.UTC)

	cmd, err := PlanClusterNodePrune([]string{"n2", "n3"}, now)
	if err != nil {
		t.Fatalf("PlanClusterNodePrune: %v", err)
	}
	d7Apply(t, f, 4, cmd)
	// n2 + n3 gone; n1 survives (prune is by explicit id, NOT by phase — the whole point vs
	// PlanClusterNodeRemove, which only matches RETIRING/VOTER_ADD_FAILED/CATCHING_UP).
	if nodeCount(t, f, "n2") != 0 || nodeCount(t, f, "n3") != 0 {
		t.Fatalf("prune did not delete n2/n3: counts n2=%d n3=%d", nodeCount(t, f, "n2"), nodeCount(t, f, "n3"))
	}
	if nodeCount(t, f, "n1") != 1 {
		t.Fatal("prune wrongly deleted the un-listed n1")
	}
	// both generations bump (roster change + mesh leave → reconciler wakes, agents get the corrected roster).
	if rg := rgen(t, f); rg <= rosterBefore {
		t.Fatalf("prune must bump roster_generation: %d -> %d", rosterBefore, rg)
	}
	if tg := tgen(t, f); tg <= topoBefore {
		t.Fatalf("prune must bump topology_generation (mesh leave): %d -> %d", topoBefore, tg)
	}

	// Idempotent re-prune of now-absent rows: RowsAffected==0 → NEITHER generation advances again.
	rosterAfter := rgen(t, f)
	topoAfter := tgen(t, f)
	d7Apply(t, f, 5, cmd)
	if rg := rgen(t, f); rg != rosterAfter {
		t.Fatalf("re-prune of absent rows must NOT bump roster_generation: %d -> %d", rosterAfter, rg)
	}
	if tg := tgen(t, f); tg != topoAfter {
		t.Fatalf("re-prune of absent rows must NOT bump topology_generation: %d -> %d", topoAfter, tg)
	}
}

func TestPlanClusterNodePruneRejectsEmptyAndBadID(t *testing.T) {
	if _, err := PlanClusterNodePrune(nil, time.Now()); err == nil {
		t.Fatal("PlanClusterNodePrune must reject an empty id set")
	}
	if _, err := PlanClusterNodePrune([]string{"ok", "bad\x00id"}, time.Now()); err == nil {
		t.Fatal("PlanClusterNodePrune must reject a NUL in a node_id (LitText)")
	}
	if _, err := PlanClusterNodePrune([]string{"n1"}, time.Now()); err != nil {
		t.Fatalf("PlanClusterNodePrune must accept a valid single id: %v", err)
	}
}
