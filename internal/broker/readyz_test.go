package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
)

// b7_readyz_test.go — Audit TEST-MAJOR-1: the /readyz cluster-mode predicate (the B5 no-silent-fork
// LB guard) was untested. Bring up a real single-voter Node + a seeded phase DB and exercise the
// bands: leader+VOTER+in-config → ready; CATCHING_UP → 503.
func TestMetricsReadyClusterBands(t *testing.T) {
	dataDir := t.TempDir()
	_, trans := raft.NewInmemTransport("node-A")
	n, err := cluster.New(cluster.Config{
		LocalID: "node-A", DataDir: dataDir, DBPath: filepath.Join(dataDir, "state.db"),
		Transport: trans, ApplyTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	defer func() { _ = n.Shutdown() }()
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}

	// A SEPARATE phase DB (metricsReady reads phase from b.cfg.DB).
	phaseDB, err := storage.OpenWAL("file:" + filepath.Join(t.TempDir(), "phase.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = phaseDB.Close() })
	setPhase := func(p string) {
		if _, err := phaseDB.Exec(`DELETE FROM cluster_nodes`); err != nil {
			t.Fatal(err)
		}
		if _, err := phaseDB.Exec(
			`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('node-A','a','p','node-A','node-A','n','t','h','c',?,'2026-06-24 00:00:00 +0000 UTC')`, p); err != nil {
			t.Fatal(err)
		}
	}

	b := &Broker{clusterMode: true, cl: &clusterRuntime{node: n}}
	b.cfg.DB = phaseDB

	setPhase("VOTER")
	if ready, why := b.metricsReady(); !ready {
		t.Fatalf("a leader + serving VOTER + in-config must be READY, got 503: %s", why)
	}
	setPhase("CATCHING_UP")
	if ready, _ := b.metricsReady(); ready {
		t.Fatal("a CATCHING_UP self must be 503 (LB must drain it)")
	}
	setPhase("RETIRING")
	if ready, _ := b.metricsReady(); ready {
		t.Fatal("a RETIRING self must be 503")
	}
}
