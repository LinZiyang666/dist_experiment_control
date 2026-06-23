package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

// offline.go — D7 §8.4 force-single disk surgery primitives. These run with the
// daemon STOPPED, directly on the on-disk raft/ + tether.db. The RecoverCluster +
// FSM wiring needs the unexported fsm, so it lives HERE (in internal/cluster) — the
// orchestration (flock, the (b)(c)(d) preconditions, peer TCP-liveness probe,
// dump-divergent) lives in internal/clusteroffline so raft stays confined to this
// package (L-2). The boltLockProbeTimeout governs the (b) "is a live daemon holding
// raft.db?" probe.
const boltLockProbeTimeout = 400 * time.Millisecond

// ErrNoExistingState is the §8.4(b) precondition failure: force-single on a node
// with no raft state would BUILD AN EMPTY CLUSTER and silently lose all data, so it
// is refused.
var ErrNoExistingState = errors.New("cluster: no existing raft state on disk (raft/ or tether.db empty)")

func raftPaths(dataDir string) (raftDir, boltPath string) {
	raftDir = filepath.Join(dataDir, "raft")
	return raftDir, filepath.Join(raftDir, "raft.db")
}

// RaftStoreLockedByDaemon reports whether a live process holds the exclusive lock on
// raft.db (a running daemon opened the BoltStore). It is the D7 offline-vs-live-daemon
// interlock (§8.4(a)/B-7): the production daemon does NOT take ${DataDir}/tether.lock
// until D9, but it ALWAYS holds the raft.db bolt lock while running, so a short-timeout
// open that times out means "daemon still up — `systemctl mask` + stop it first".
func RaftStoreLockedByDaemon(dataDir string) (bool, error) {
	_, boltPath := raftPaths(dataDir)
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return true, nil // a live daemon holds the lock
		}
		return false, fmt.Errorf("cluster: probe raft.db lock: %w", err)
	}
	_ = store.Close()
	return false, nil
}

// RaftStateExists reports whether the on-disk stores hold a recoverable raft state
// (§8.4(b)). Caller must already hold the flock and have confirmed no live daemon.
func RaftStateExists(dataDir string) (bool, error) {
	raftDir, boltPath := raftPaths(dataDir)
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		return false, fmt.Errorf("cluster: open boltstore: %w", err)
	}
	defer func() { _ = store.Close() }()
	snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		return false, fmt.Errorf("cluster: open snapshot store: %w", err)
	}
	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		return false, fmt.Errorf("cluster: probe existing state: %w", err)
	}
	return existing, nil
}

// RecoverSingleNode rewrites the on-disk raft configuration to a single voter
// {selfID @ selfRaftAddr} via raft.RecoverCluster (§8.4(c), the BLOCKER fix). It
// does NOT hand-roll a two-store reconcile: RecoverCluster ITSELF replays the local
// BoltDB log [snapshotIndex+1 .. LastIndex()] through the idempotent
// applied_index-advancing fsm.Apply (so already-applied entries are no-ops and the
// committed-but-unapplied tail lands in SQLite), then writes a single-server
// snapshot + config. Recovery point = local LastIndex() (commitIndex is never
// persisted offline; the operator has declared the peers dead, so this node's whole
// log is the authoritative timeline and its uncommitted tail is committed-by-fiat).
//
// Caller MUST already hold the flock, have confirmed no live daemon (the bolt lock),
// have confirmed RaftStateExists, and have HARD-REFUSED on any reachable peer.
func RecoverSingleNode(dataDir, dbPath, selfID, selfRaftAddr string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if selfID == "" || selfRaftAddr == "" {
		return errors.New("cluster: RecoverSingleNode requires selfID and selfRaftAddr")
	}
	raftDir, boltPath := raftPaths(dataDir)

	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		return fmt.Errorf("cluster: open recovery DB: %w", err)
	}
	defer func() { _ = db.Close() }()
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return fmt.Errorf("cluster: open recovery RO DB: %w", err)
	}
	defer func() { _ = ro.Close() }()

	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		return fmt.Errorf("cluster: open boltstore for recovery: %w", err)
	}
	defer func() { _ = store.Close() }()
	snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		return fmt.Errorf("cluster: open snapshot store for recovery: %w", err)
	}

	f := &fsm{
		db:       db,
		ro:       ro,
		tmpDir:   filepath.Dir(dbPath),
		dbPath:   dbPath,
		appliers: defaultAppliers(),
		logger:   logger,
	}
	rc := raftConfig(Config{LocalID: raft.ServerID(selfID)})

	// An in-memory transport: RecoverCluster takes a Transport but performs no
	// network IO (it only needs LocalAddr to match the single-server config). Inmem
	// binds no socket, correct for an offline tool. Close it after.
	_, trans := raft.NewInmemTransport(raft.ServerAddress(selfRaftAddr))
	defer func() { _ = trans.Close() }()

	cfg := raft.Configuration{Servers: []raft.Server{{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(selfID),
		Address:  raft.ServerAddress(selfRaftAddr),
	}}}
	if err := raft.RecoverCluster(rc, f, store, store, snaps, trans, cfg); err != nil {
		return fmt.Errorf("cluster: RecoverCluster({%s}): %w", selfID, err)
	}
	return nil
}
