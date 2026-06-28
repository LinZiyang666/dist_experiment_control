//go:build d9_integration

// resnapshot_test.go — STEP-1 grow-onto-migrated-broker remediation for a broker init'd BEFORE the
// fix (e.g. the live pc732): config@1 with NO raft snapshot. `cluster recovery resnapshot` must write
// a snapshot + compact the log so a future joiner installs it, with an audit-window guard that refuses
// to truncate unpublished audit unless --accept-audit-loss.
package d9_test

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

func TestD9ResnapshotRemediatesUnSnapshottedNode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	// OpenWAL runs ApplyMigrations (incl the cluster tables readRoster/cluster_meta need), then we
	// bootstrap config@1 with NO snapshot — exactly the pre-fix `cluster init --from-existing` state.
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open+migrate DB: %v", err)
	}
	// Seed the self VOTER row the way `cluster init --from-existing` does (readRoster refuses an
	// unknown self), so this models a real migrated single-voter broker.
	if _, err := db.Exec(`INSERT INTO cluster_nodes
		(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
		VALUES('pc732','pc732','pub-pc732','pc732','10.0.0.1:7400','nats://10.0.0.1:6222','h:7000','h','fp','VOTER','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed self row: %v", err)
	}
	_ = db.Close()
	if err := cluster.BootstrapSingleNode(dir, "pc732", "10.0.0.1:7400", nil); err != nil {
		t.Fatalf("BootstrapSingleNode: %v", err)
	}
	if ex, _, _, _ := cluster.RaftSnapshotMeta(dir); ex {
		t.Fatal("precondition: a freshly bootstrapped (pre-fix) node must have NO snapshot")
	}

	// AUDIT-WINDOW guard: raft last_index (config@1) > audit_published_index (0) → refuse without the flag.
	if err := clusteroffline.Resnapshot(clusteroffline.ResnapshotOptions{
		DataDir: dir, DBPath: dbPath, SelfID: "pc732", SelfRaftAddr: "10.0.0.1:7400",
	}); err == nil {
		t.Fatal("resnapshot must REFUSE while raft last_index > audit_published_index without --accept-audit-loss (would truncate unpublished audit)")
	}

	// With --accept-audit-loss → remediates: a snapshot now exists, so a joiner installs it instead
	// of replaying the log onto an un-seeded DB and FK-fail-stopping.
	if err := clusteroffline.Resnapshot(clusteroffline.ResnapshotOptions{
		DataDir: dir, DBPath: dbPath, SelfID: "pc732", SelfRaftAddr: "10.0.0.1:7400", AcceptAuditLoss: true,
	}); err != nil {
		t.Fatalf("resnapshot --accept-audit-loss: %v", err)
	}
	ex, idx, _, err := cluster.RaftSnapshotMeta(dir)
	if err != nil {
		t.Fatalf("RaftSnapshotMeta: %v", err)
	}
	if !ex {
		t.Fatal("resnapshot left NO snapshot — the migrated single-voter broker is still un-growable")
	}
	if idx < 1 {
		t.Fatalf("resnapshot snapshot index = %d, want >= 1", idx)
	}
}
