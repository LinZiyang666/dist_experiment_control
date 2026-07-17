package clusteroffline

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// TestS6S8ExternalReviewResnapshotRefusesPeerInRaftTail exposes a destructive
// preflight/commit mismatch. Resnapshot checks the materialized SQLite roster,
// but RecoverCluster subsequently replays the local Raft tail before taking its
// single-node snapshot. A peer admission in that tail therefore has to make the
// SINGLE-VOTER preflight refuse; accepting it and only then rewriting Raft to
// {self} silently revives a peer in the FSM while excluding it from Raft.
func TestS6S8ExternalReviewResnapshotRefusesPeerInRaftTail(t *testing.T) {
	dataDir, dbPath := mustDataDir(t)
	seedDB(t, dbPath) // peer-2 is the sole materialized node and is used as self.

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := cluster.BootstrapSingleNode(dataDir, "peer-2", "127.0.0.1:7400", logger); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(dataDir, "raft", "raft.db")})
	if err != nil {
		t.Fatalf("open raft store: %v", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		_ = store.Close()
		t.Fatalf("last index: %v", err)
	}
	cmd := cluster.NewCommand(cluster.OpClusterNodePhase, cluster.Stmt(
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('peer-tail','peer-tail','Upub-tail','tether-tail','127.0.0.1:2','nats://tail','tail:7000','tail','sha256:cd','VOTER','2026-07-16 00:00:00 +0000 UTC')`,
	))
	data, err := json.Marshal(cmd)
	if err != nil {
		_ = store.Close()
		t.Fatalf("encode peer-tail command: %v", err)
	}
	if err := store.StoreLog(&raft.Log{Index: last + 1, Term: 1, Type: raft.LogCommand, Data: data}); err != nil {
		_ = store.Close()
		t.Fatalf("store peer-tail command: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close raft store: %v", err)
	}

	err = Resnapshot(ResnapshotOptions{
		DataDir: dataDir, DBPath: dbPath, SelfID: "peer-2", SelfRaftAddr: "127.0.0.1:7400",
		Logger: logger,
	})
	if err == nil {
		t.Fatal("resnapshot accepted a non-self peer in the recoverable Raft tail; RecoverCluster can revive it while rewriting Raft to {self}")
	}
	if !strings.Contains(err.Error(), "SINGLE-VOTER") {
		t.Fatalf("resnapshot must fail at the SINGLE-VOTER safety gate, got: %v", err)
	}
}

// TestS6S8ExternalReviewForceSingleSnapshotsThePrunedRoster reproduces the
// simcluster-42 sequence: an older Raft snapshot contains brk2, the live DB was
// pruned to self, and force-single must not leave a snapshot that a subsequent
// resnapshot can use to revive brk2.
func TestS6S8ExternalReviewForceSingleSnapshotsThePrunedRoster(t *testing.T) {
	dataDir, dbPath := mustDataDir(t)
	seedDB(t, dbPath) // peer-2 is self.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('peer-stale','peer-stale','Upub-stale','tether-stale','127.0.0.1:1','nats://127.0.0.1:1','127.0.0.1:1','stale','sha256:ef','VOTER','2026-07-16 00:00:00 +0000 UTC')`)
	if err == nil {
		_, err = db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index','500') ON CONFLICT(key) DO UPDATE SET value='500'`)
	}
	_ = db.Close()
	if err != nil {
		t.Fatalf("seed stale peer/high applied index: %v", err)
	}
	if err := cluster.BootstrapSingleNode(dataDir, "peer-2", "127.0.0.1:7400", logger); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := cluster.GrowReadySnapshot(dataDir, dbPath, "peer-2", "127.0.0.1:7400", logger); err != nil {
		t.Fatalf("snapshot stale roster: %v", err)
	}

	// Make the materialized projection self-only while retaining the stale peer in
	// the persisted Raft snapshot, exactly the state the old force-single left.
	db, err = storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DELETE FROM cluster_nodes WHERE node_id='peer-stale'`)
	_ = db.Close()
	if err != nil {
		t.Fatalf("prune materialized stale peer: %v", err)
	}

	abandoned, err := ForceSingle(ForceSingleOptions{
		DataDir: dataDir, DBPath: dbPath, SelfID: "peer-2", SelfRaftAddr: "127.0.0.1:7400",
		ConfirmedDead: []string{"peer-stale"}, Logger: logger,
	})
	if err != nil {
		t.Fatalf("force-single: %v", err)
	}
	if len(abandoned) != 1 || abandoned[0].NodeID != "peer-stale" {
		t.Fatalf("force-single did not account for recovered stale peer: %+v", abandoned)
	}
	if peers, err := readRoster(dbPath, "peer-2"); err != nil || len(peers) != 0 {
		t.Fatalf("post-force-single roster not self-only: peers=%+v err=%v", peers, err)
	}
	applied, err := readAppliedIndexPath(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	exists, snapshotIndex, _, err := cluster.RaftSnapshotMeta(dataDir)
	if err != nil || !exists || snapshotIndex <= applied {
		t.Fatalf("rebuilt Raft snapshot must start above old applied_index: exists=%v snapshot=%d applied=%d err=%v", exists, snapshotIndex, applied, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "raft", "raft.db")); err != nil {
		t.Fatalf("post-force-single replacement must include raft.db for the production fail-closed startup probe: %v", err)
	}
	if exists, err := cluster.RaftStateExists(dataDir); err != nil || !exists {
		t.Fatalf("post-force-single store must be visible to the production startup probe before any resnapshot: exists=%v err=%v", exists, err)
	}

	// Do not let a subsequent resnapshot create raft.db and mask a snapshot-only
	// rebuild. Boot the rebuilt store immediately, exactly as production does, and
	// commit a real post-recovery command; a bad index floor can otherwise make
	// the FSM silently discard successful-looking writes until the new Raft log
	// catches up with the old applied_index.
	_, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:7400"))
	node, err := cluster.New(cluster.Config{
		LocalID: "peer-2", DataDir: dataDir, DBPath: dbPath, Transport: trans,
		ApplyTimeout: 10 * time.Second, Logger: logger,
	})
	if err != nil {
		t.Fatalf("boot rebuilt Raft store: %v", err)
	}
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		_ = node.Shutdown()
		t.Fatalf("rebuilt Raft store did not elect self: %v", err)
	}
	if err := node.ApplyMetaSet("t:external-review-post-force-single", "committed"); err != nil {
		_ = node.Shutdown()
		t.Fatalf("post-force-single authoritative write failed: %v", err)
	}
	var got string
	if err := node.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:external-review-post-force-single'`).Scan(&got); err != nil || got != "committed" {
		_ = node.Shutdown()
		t.Fatalf("post-force-single write was swallowed: value=%q err=%v", got, err)
	}
	if err := node.Shutdown(); err != nil {
		t.Fatalf("shutdown rebuilt Raft store: %v", err)
	}
	if err := Resnapshot(ResnapshotOptions{
		DataDir: dataDir, DBPath: dbPath, SelfID: "peer-2", SelfRaftAddr: "127.0.0.1:7400", Logger: logger,
	}); err != nil {
		t.Fatalf("post-force-single resnapshot revived stale peer or failed: %v", err)
	}
}
