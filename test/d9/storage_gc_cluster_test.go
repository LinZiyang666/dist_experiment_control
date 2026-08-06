//go:build d9_integration

// storage_gc_cluster_test.go — h1 B1 end-to-end: the cluster-mode retention GC
// really drains a terminal-row backlog THROUGH RAFT on a live single-voter
// broker (the production racknerd shape). This is the path the incident proved
// missing: before h1, cluster mode skipped proc GC entirely and port
// GC did not exist in any mode, so 24k FREED + 8.5k EXITED rows accumulated
// with nothing able to delete them.
// origin: docs/reviews/h1-plan.md workstream B.
package d9_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	natstest "github.com/nats-io/nats-server/v2/test"
)

func TestClusterStorageGCDrainsBacklogThroughRaft(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "tether.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Seed the incident shape BEFORE InitFromExisting (the migrated-broker
	// path): a session, a node, a live allocation + a RUNNING proc (must
	// survive), and a backlog of old terminal rows (must drain).
	seedT := time.Now().UTC().Add(-48 * time.Hour)
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) VALUES('lab','lab','o','p','ACTIVE',?)`, seedT); err != nil {
		t.Fatal(err)
	}
	// The node must be freshly-heartbeating, NOT stamped with the ancient
	// seedT: a node the liveness reconciler judges OFFLINE gets its live
	// allocations REVOKED, and that fresh REVOKED row (revoked_at = now) sits
	// inside the 24h retention forever — the drain would then never reach
	// zero and the "live allocation survives" assertion would be a race, not
	// a property. Only the HISTORY rows below carry ancient timestamps.
	if _, err := db.Exec(`INSERT INTO nodes(nid,sid,status,registered_at,last_heartbeat_at) VALUES('n1','lab','ONLINE',?,?)`,
		time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at) VALUES (14000,'lab','n1','live',0,'live-h','ALLOCATED','SHA256:t',?)`, seedT); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp) VALUES ('running','lab','n1','["x"]',?, 'RUNNING','SHA256:u')`, seedT); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// 1,200 FREED rows = 3 chunks of 500 through gcProposeChunks in one tick;
	// 700 EXITED rows = 2 chunks. Both far past any retention.
	pstmt, err := tx.Prepare(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,created_at,revoked_at) VALUES (?,?,?,?,0,?,'FREED','SHA256:t',?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1200; i++ {
		if _, err := pstmt.Exec(14001+i%2, "lab", "n1", "__proxy__", fmt.Sprintf("f%d", i), seedT, seedT); err != nil {
			t.Fatal(err)
		}
	}
	qstmt, err := tx.Prepare(`INSERT INTO processes(pid,sid,nid,argv,started_at,ended_at,status,exit_code,started_by_fp) VALUES (?,?,?,?,?,?,'EXITED',0,'SHA256:u')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 700; i++ {
		if _, err := qstmt.Exec(fmt.Sprintf("e%04d", i), "lab", "n1", `["x"]`, seedT, seedT); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	ca := newD9CA(t)
	secrets := writeD9Secrets(t, ca)
	raftAddr := freeTCPAddr(t)
	if err := clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{
		DataDir: dataDir, DBPath: dbPath, SecretsDir: secrets,
		SelfID: "gc-A", Name: "gc-A", NodeIdentPub: "pub-gc-A",
		RaftAddr: raftAddr, NatsRoute: "127.0.0.1:6222",
		TunnelAddr: "127.0.0.1:7000", PublicHost: "localhost",
		Now: time.Now,
	}); err != nil {
		t.Fatal(err)
	}

	natsOpts := natstest.DefaultTestOptions
	natsOpts.Port = -1
	natsOpts.JetStream = true
	natsOpts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&natsOpts)
	t.Cleanup(ns.Shutdown)

	cfg := broker.Config{
		NATSURL:           ns.ClientURL(),
		ClusterDataDir:    dataDir,
		ClusterRaftAddr:   raftAddr,
		ClusterSecretsDir: secrets,
		DBPath:            dbPath,
		StoreDir:          t.TempDir(),
		Now:               time.Now,
		ReadyCh:           make(chan struct{}),
		// Fast cadence so the drain happens inside the test budget; retention
		// keeps its production semantics (rows above are 48h old).
		ProcGCInterval: 300 * time.Millisecond,
		ProcRetention:  time.Hour,
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("broker.Run did not exit within 10s of cancel")
		}
	})
	select {
	case <-cfg.ReadyCh:
	case err := <-done:
		t.Fatalf("broker exited before ready: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("broker not ready within 30s")
	}

	ro, err := storage.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ro.Close() })

	counts := func() (terminalPorts, exitedProcs int) {
		t.Helper()
		if err := ro.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE state IN ('FREED','REVOKED')`).Scan(&terminalPorts); err != nil {
			t.Fatal(err)
		}
		if err := ro.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='EXITED'`).Scan(&exitedProcs); err != nil {
			t.Fatal(err)
		}
		return terminalPorts, exitedProcs
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		tp, ep := counts()
		if tp == 0 && ep == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backlog not drained through raft: %d terminal ports, %d exited procs remain", tp, ep)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Survivors: the live allocation and the RUNNING row are untouchable.
	var liveState, runStatus string
	if err := ro.QueryRow(`SELECT state FROM port_allocations WHERE name='live'`).Scan(&liveState); err != nil {
		t.Fatalf("live allocation was GC'd: %v", err)
	}
	if err := ro.QueryRow(`SELECT status FROM processes WHERE pid='running'`).Scan(&runStatus); err != nil {
		t.Fatalf("RUNNING row was GC'd: %v", err)
	}
	if liveState != "ALLOCATED" || runStatus != "RUNNING" {
		t.Fatalf("survivors mutated: live=%s running=%s", liveState, runStatus)
	}
}
