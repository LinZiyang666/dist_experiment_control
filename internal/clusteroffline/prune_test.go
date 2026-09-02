package clusteroffline

import (
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// prune_test.go (formerly g2_prune_test.go; G2 #12) — pruneRosterPeers is the OFFLINE force-single roster prune: a direct-SQL
// DELETE of the abandoned peers + a monotone bump of both generation counters, the daemon-down equivalent
// of cluster.PlanClusterNodePrune, so the offline path leaves the SAME {self}-only cluster_nodes state as
// the online path (parity) and the survivor's next signed roster advances (agents converge to N=1).

func insertNode(t *testing.T, dbPath, nodeID string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) `+
			`VALUES(?,?,'Upub','tether-x','127.0.0.1:1','nats://x','x:7000','h','sha256:ab','VOTER','2026-06-23 00:00:00 +0000 UTC')`,
		nodeID, nodeID); err != nil {
		t.Fatalf("insert %s: %v", nodeID, err)
	}
}

func metaUint(t *testing.T, dbPath, key string) uint64 {
	t.Helper()
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer func() { _ = db.Close() }()
	var s string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&s); err != nil {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func rowCount(t *testing.T, dbPath, nodeID string) int {
	t.Helper()
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", nodeID, err)
	}
	return n
}

func TestPruneRosterPeersDeletesAbandonedBumpsGens(t *testing.T) {
	_, dbPath := mustDataDir(t)
	insertNode(t, dbPath, "self")
	insertNode(t, dbPath, "dead-1")
	insertNode(t, dbPath, "dead-2")
	now := time.Date(2026, 7, 6, 2, 0, 0, 0, time.UTC)

	peers := []Peer{{NodeID: "dead-1"}, {NodeID: "dead-2"}}
	if err := pruneRosterPeers(dbPath, peers, now, nil); err != nil {
		t.Fatalf("pruneRosterPeers: %v", err)
	}
	if rowCount(t, dbPath, "dead-1") != 0 || rowCount(t, dbPath, "dead-2") != 0 {
		t.Fatal("prune did not delete the abandoned peers")
	}
	if rowCount(t, dbPath, "self") != 1 {
		t.Fatal("prune wrongly deleted self")
	}
	if metaUint(t, dbPath, cluster.MetaKeyRosterGeneration) == 0 {
		t.Error("prune must bump roster_generation so agents accept the corrected roster")
	}
	if metaUint(t, dbPath, cluster.MetaKeyTopologyGeneration) == 0 {
		t.Error("prune must bump topology_generation so the reconciler re-renders")
	}

	// Change-gated idempotency (internal review): re-prune of now-absent rows must NOT bump EITHER
	// generation (parity with the online rosterGenBumpStmt's `WHERE changes()>0`), not merely stay monotone.
	rgBefore := metaUint(t, dbPath, cluster.MetaKeyRosterGeneration)
	tgBefore := metaUint(t, dbPath, cluster.MetaKeyTopologyGeneration)
	if err := pruneRosterPeers(dbPath, peers, now.Add(time.Second), nil); err != nil {
		t.Fatalf("re-prune: %v", err)
	}
	if rgAfter := metaUint(t, dbPath, cluster.MetaKeyRosterGeneration); rgAfter != rgBefore {
		t.Errorf("re-prune of absent rows must NOT bump roster_generation (change-gated): %d -> %d", rgBefore, rgAfter)
	}
	if tgAfter := metaUint(t, dbPath, cluster.MetaKeyTopologyGeneration); tgAfter != tgBefore {
		t.Errorf("re-prune of absent rows must NOT bump topology_generation (change-gated): %d -> %d", tgBefore, tgAfter)
	}
}
