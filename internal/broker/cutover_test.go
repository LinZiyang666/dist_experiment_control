package broker

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// cutover_test.go — D9 step-1/step-2 unit gates: the cluster-mode detection truth
// table + the DB-side migration-marker cross-check + the single-mode proxy-gen floor.
// The N>=3 "cluster mode wires everything" proof + the byte-equivalence golden live in
// the gated test/d9 harness (step 4 / step 7).

func cutoverTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedClusterMarker writes the `cluster init` marker (applied_index key + one
// cluster_nodes VOTER row) so clusterDBSeeded reports true.
func seedClusterMarker(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO cluster_meta(key, value) VALUES('applied_index', '0')`); err != nil {
		t.Fatalf("seed applied_index: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_nodes
		(node_id, name, node_ident_pub, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, phase, added_at)
		VALUES('n1','n1','pub','127.0.0.1:7400','127.0.0.1:6222','127.0.0.1:7000','localhost','sha256:deadbeef','VOTER','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed cluster_nodes self row: %v", err)
	}
}

func TestD9ClusterModeEnabledDetection(t *testing.T) {
	// data_dir unset ⇒ single mode (every pre-D9 deployment), no error.
	if mode, err := clusterModeEnabled(Config{ClusterDataDir: ""}); err != nil || mode {
		t.Fatalf("unset data_dir: want (false,nil), got (%v,%v)", mode, err)
	}

	// data_dir set but no raft/ on disk ⇒ FATAL (never auto-bootstrap onto a live DB).
	tmp := t.TempDir()
	mode, err := clusterModeEnabled(Config{ClusterDataDir: tmp})
	if err == nil {
		t.Fatalf("data_dir set + no raft/: want FATAL, got (mode=%v, nil)", mode)
	}
	if mode {
		t.Fatalf("data_dir set + no raft/: mode must be false on error, got true")
	}
	if !strings.Contains(err.Error(), "cluster init") {
		t.Fatalf("data_dir set + no raft/: error should name the fix `cluster init`, got %q", err)
	}

	// raftStateOnDisk on a dir with no raft.db is a clean false (no side-effect file
	// creation — it must NOT create raft/raft.db just by probing).
	ok, err := raftStateOnDisk(tmp)
	if err != nil || ok {
		t.Fatalf("raftStateOnDisk(empty): want (false,nil), got (%v,%v)", ok, err)
	}
	if _, statErr := os.Stat(filepath.Join(tmp, "raft", "raft.db")); !os.IsNotExist(statErr) {
		t.Fatalf("raftStateOnDisk probe must NOT create raft/raft.db as a side effect (stat=%v)", statErr)
	}
}

func TestD9ClusterDBMarkerCrossCheck(t *testing.T) {
	// Fresh v2 DB: storage.Open ran migrations 0008-0013, so cluster_meta +
	// cluster_nodes EXIST but are EMPTY ⇒ NOT seeded ⇒ single mode OK, cluster FATAL.
	db := cutoverTestDB(t)
	if seeded, err := clusterDBSeeded(db); err != nil || seeded {
		t.Fatalf("fresh DB: want not-seeded, got (%v,%v)", seeded, err)
	}
	if err := assertClusterDBConsistent(db, false); err != nil {
		t.Fatalf("fresh DB + single mode: want nil, got %v", err)
	}
	if err := assertClusterDBConsistent(db, true); err == nil {
		t.Fatal("fresh DB + cluster mode: want FATAL (half-migrated raft-without-seed), got nil")
	} else if !strings.Contains(err.Error(), "half-migrated") {
		t.Fatalf("fresh DB + cluster mode: error should say half-migrated, got %q", err)
	}

	// Seeded DB: cluster mode OK, single mode FATAL (refuse silent downgrade).
	seedClusterMarker(t, db)
	if seeded, err := clusterDBSeeded(db); err != nil || !seeded {
		t.Fatalf("seeded DB: want seeded, got (%v,%v)", seeded, err)
	}
	if err := assertClusterDBConsistent(db, true); err != nil {
		t.Fatalf("seeded DB + cluster mode: want nil, got %v", err)
	}
	if err := assertClusterDBConsistent(db, false); err == nil {
		t.Fatal("seeded DB + single mode: want FATAL (refuse silent cluster→single downgrade), got nil")
	} else if !strings.Contains(err.Error(), "downgrade") {
		t.Fatalf("seeded DB + single mode: error should say downgrade, got %q", err)
	}
}

// TestD9ClusterDBSeededPartialMarkers guards the AND in clusterDBSeeded: applied_index
// WITHOUT a cluster_nodes row (and vice versa) is NOT a complete marker — a half-write
// must not read as seeded (it would mask a half-migration).
func TestD9ClusterDBSeededPartialMarkers(t *testing.T) {
	db := cutoverTestDB(t)
	if _, err := db.Exec(`INSERT INTO cluster_meta(key, value) VALUES('applied_index', '0')`); err != nil {
		t.Fatalf("insert applied_index: %v", err)
	}
	if seeded, err := clusterDBSeeded(db); err != nil || seeded {
		t.Fatalf("applied_index without self row: want not-seeded, got (%v,%v)", seeded, err)
	}
}

// TestD9ClusterModeOffByteEquivalence is the regression FLOOR that replaces the deleted
// six TestDxProductionWiresNoCluster guards (step 7): a broker with NO cluster config
// (every pre-D9 deployment) constructs NO cluster.Node and leaves EVERY D2-D8 seam at
// its zero value, so the D2 N=1 functional-equivalence promise is preserved through the
// live wiring. It is also the planted-regression self-check: an un-gated AttachClusterSeam
// / wireCluster* in production would make selfID!="" or cl!=nil here and FAIL this test
// (the discriminating power the old GuardSelfCheck had).
func TestD9ClusterModeOffByteEquivalence(t *testing.T) {
	db := cutoverTestDB(t)
	b, err := New(Config{NATSURL: "nats://127.0.0.1:4222", DB: db})
	if err != nil {
		t.Fatalf("New (single mode): %v", err)
	}
	if b.clusterMode {
		t.Fatal("New with no cluster config must be single mode")
	}
	if b.cl != nil {
		t.Fatal("single mode must construct NO cluster runtime (cl != nil)")
	}
	// Every D2-D8 seam stays the zero value (byte-identical responses).
	if b.selfID != "" {
		t.Errorf("D6 selfID seam must be empty in single mode, got %q", b.selfID)
	}
	if b.tunnelCert != nil {
		t.Error("D6 tunnelCert seam must be nil in single mode")
	}
	if b.transferAuditSink != nil {
		t.Error("D8a transferAuditSink seam must be nil in single mode")
	}
	if b.xferReplicasFn != nil {
		t.Error("D8a xferReplicasFn seam must be nil in single mode")
	}
	if b.alertSink != nil {
		t.Error("D8b alertSink seam must be nil in single mode")
	}
	// And the one single-mode startup write (the P13 fence) DID run (byte-identical).
	if b.proxyGen == 0 {
		t.Fatal("single-mode New must advance proxyGen (durable P13 fence), got 0")
	}
	// livenessDB in single mode is the same handle (no cluster write pool).
	if b.livenessDB() != db {
		t.Error("single-mode livenessDB must be the configured DB handle")
	}
}
