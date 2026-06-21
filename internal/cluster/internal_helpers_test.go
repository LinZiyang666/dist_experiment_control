package cluster

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// mustNode builds a leader-ready single-node cluster.Node under dataDir. The
// inmem transport uses a STABLE address (== the node id) so a restart on the same
// dataDir recovers against the persisted raft configuration (which records that
// address). InmemTransport addresses are independent labels (no global registry),
// so reusing id across sequential tests is safe.
func mustNode(t *testing.T, dataDir string, id raft.ServerID) *Node {
	t.Helper()
	n, err := openNodeRaw(dataDir, id)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	return n
}

// openNodeRaw constructs a Node without a *testing.T (usable from the kill-9 child
// subprocess, which has no test context). Stable inmem addr == id.
func openNodeRaw(dataDir string, id raft.ServerID) (*Node, error) {
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	return New(Config{
		LocalID:      id,
		DataDir:      dataDir,
		DBPath:       filepath.Join(dataDir, "state.db"),
		Transport:    trans,
		Logger:       quietLogger(),
		ApplyTimeout: 30 * time.Second,
	})
}

// clusterMetaHash returns a stable hash of cluster_meta's (key,value) rows
// ordered by key — the §3.6/§13.2 logical-content comparison (NOT a byte compare).
func clusterMetaHash(t *testing.T, db *sql.DB) string {
	t.Helper()
	h, err := hashClusterMeta(db)
	if err != nil {
		t.Fatalf("clusterMetaHash: %v", err)
	}
	return h
}

// hashClusterMeta is the *testing.T-free core (usable from the kill-9 child).
func hashClusterMeta(db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT key, value FROM cluster_meta ORDER BY key`)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	h := sha256.New()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return "", err
		}
		_, _ = io.WriteString(h, k+"\x00"+v+"\x00")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// freshFSM builds an fsm on a brand-new empty WAL DB under dir (for restore
// targets). Returns the fsm and its write pool.
func freshFSM(t *testing.T, dir string) (*fsm, *sql.DB) {
	t.Helper()
	dsn := "file:" + filepath.Join(dir, "state.db")
	db, err := storage.OpenWAL(dsn)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	ro, err := storage.OpenReadOnly(dsn)
	if err != nil {
		_ = db.Close()
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = ro.Close(); _ = db.Close() })
	return &fsm{
		db:       db,
		ro:       ro,
		tmpDir:   dir,
		dbPath:   filepath.Join(dir, "state.db"),
		appliers: defaultAppliers(),
		logger:   quietLogger(),
	}, db
}

// openSnapshot reopens the FileSnapshotStore under dataDir and returns the latest
// snapshot's meta + reader.
func openSnapshot(t *testing.T, dataDir string) (*raft.SnapshotMeta, io.ReadCloser) {
	t.Helper()
	store, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), 2, io.Discard)
	if err != nil {
		t.Fatalf("reopen snapshot store: %v", err)
	}
	list, err := store.List()
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("no snapshots found")
	}
	meta, rc, err := store.Open(list[0].ID)
	if err != nil {
		t.Fatalf("open snapshot %s: %v", list[0].ID, err)
	}
	return meta, rc
}
