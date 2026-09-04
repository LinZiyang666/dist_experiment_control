package cluster

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
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

// RaftSnapshotMeta reports the newest persisted raft snapshot (index/term) for an on-disk store,
// or exists=false if there is none. Offline, read-only. The grow-onto-migrated-broker fix asserts a
// snapshot EXISTS after `cluster init --from-existing` / `recovery resnapshot` — without it a fresh
// joiner replays the leader's log onto an un-seeded DB and FK-fail-stops. Used by tests + doctor.
func RaftSnapshotMeta(dataDir string) (exists bool, index, term uint64, err error) {
	raftDir, _ := raftPaths(dataDir)
	snaps, e := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if e != nil {
		return false, 0, 0, fmt.Errorf("cluster: open snapshot store: %w", e)
	}
	list, e := snaps.List()
	if e != nil {
		return false, 0, 0, fmt.Errorf("cluster: list snapshots: %w", e)
	}
	if len(list) == 0 {
		return false, 0, 0, nil
	}
	return true, list[0].Index, list[0].Term, nil // List() is newest-first
}

// RaftLastIndex returns the on-disk raft log's last index (offline, read-only). Used by the
// resnapshot audit-window guard: if audit_published_index < LastIndex, compacting the log would
// truncate unpublished audit. Returns 0 on an empty log (e.g. just after a compaction).
func RaftLastIndex(dataDir string) (uint64, error) {
	_, boltPath := raftPaths(dataDir)
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		return 0, fmt.Errorf("cluster: open boltstore for last-index: %w", err)
	}
	defer func() { _ = store.Close() }()
	last, err := store.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("cluster: read last index: %w", err)
	}
	return last, nil
}

// UnpublishedAuditOpsInLog scans the on-disk raft log range (afterIndex, LastIndex] (offline, read-only)
// and counts the entries that are RE-DERIVABLE AUDIT-SOURCE ops — OpReconcileBatch / OpTransferAudit, the
// ONLY ops the D5 publisher turns into replicated audit that a log truncation could actually lose.
//
// It deliberately IGNORES everything else: the trailing OpAuditCheckpointSet (self-begetting — it commits
// at LastIndex and keeps audit_published_index == LastIndex-1 FOREVER, so a raw LastIndex>published delta
// is structurally always >=1 and would force --accept-audit-loss on every broker), raft config/noop
// entries, and register/heartbeat/port/session ops (whose state is fully preserved in the snapshotted
// SQLite DB). An undecodable command in the window is counted (fail-closed). Returns (count, firstIndex).
// Lives here so raft + decodeCommand stay in internal/cluster (L-2); the resnapshot audit-window guard
// (internal/clusteroffline) calls it so the no-loss remediation path is actually reachable (v0.4.4 review).
func UnpublishedAuditOpsInLog(dataDir string, afterIndex uint64) (count int, firstIndex uint64, err error) {
	_, boltPath := raftPaths(dataDir)
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		return 0, 0, fmt.Errorf("cluster: open boltstore for audit scan: %w", err)
	}
	defer func() { _ = store.Close() }()
	first, err := store.FirstIndex()
	if err != nil {
		return 0, 0, fmt.Errorf("cluster: audit scan first index: %w", err)
	}
	last, err := store.LastIndex()
	if err != nil {
		return 0, 0, fmt.Errorf("cluster: audit scan last index: %w", err)
	}
	if first == 0 || last == 0 {
		return 0, 0, nil // empty log (e.g. already compacted)
	}
	lo := afterIndex + 1
	if lo < first {
		lo = first // the floor below FirstIndex was snapshotted; only scan what is still on disk
	}
	for idx := lo; idx <= last; idx++ {
		var lg raft.Log
		if e := store.GetLog(idx, &lg); e != nil {
			if errors.Is(e, raft.ErrLogNotFound) {
				continue
			}
			return 0, 0, fmt.Errorf("cluster: audit scan get log %d: %w", idx, e)
		}
		if lg.Type != raft.LogCommand {
			continue // noop / config carry no audit
		}
		cmd, derr := decodeCommand(lg.Data)
		isAudit := derr != nil || cmd.Op == OpReconcileBatch || cmd.Op == OpTransferAudit
		if isAudit {
			count++
			if firstIndex == 0 {
				firstIndex = idx
			}
		}
	}
	return count, firstIndex, nil
}

// ErrAlreadyBootstrapped is BootstrapSingleNode's idempotent signal: raft/ already holds
// state (a prior `cluster init` created it), so there is nothing to bootstrap. The caller
// treats it as success (the init is idempotent).
var ErrAlreadyBootstrapped = errors.New("cluster: raft state already exists (already bootstrapped)")

// BootstrapSingleNode creates a fresh single-voter raft/ for a never-clustered DB — the
// D9 `cluster init [--from-existing]` FINAL step (after the DB is migrated + seeded).
// Unlike RecoverSingleNode it REQUIRES empty stores and replays NO log (a never-clustered
// DB has none): it writes only the {self} configuration as the initial raft state, so the
// already-migrated DB stands as the index-0 snapshot. It opens NO SQLite handle — the DB
// seeding (self_node_id / applied_index / cluster_nodes self row) is a separate seed step,
// committed BEFORE this call so the detection probe (raft/ present) flips true only once the
// DB is fully prepared. Returns ErrAlreadyBootstrapped (idempotent) if raft/ already exists.
func BootstrapSingleNode(dataDir, selfID, selfRaftAddr string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if selfID == "" || selfRaftAddr == "" {
		return errors.New("cluster: BootstrapSingleNode requires selfID and selfRaftAddr")
	}
	logger.Info("cluster: bootstrapping single-voter raft", "self", selfID, "data_dir", dataDir)
	raftDir, boltPath := raftPaths(dataDir)
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		return fmt.Errorf("cluster: mkdir raft dir: %w", err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		return fmt.Errorf("cluster: open boltstore for bootstrap: %w", err)
	}
	defer func() { _ = store.Close() }()
	snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		return fmt.Errorf("cluster: open snapshot store for bootstrap: %w", err)
	}
	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		return fmt.Errorf("cluster: probe existing state: %w", err)
	}
	if existing {
		return ErrAlreadyBootstrapped
	}
	// An in-memory transport: BootstrapCluster needs LocalAddr to match the single-server
	// config but performs no network IO. Closed after.
	_, trans := raft.NewInmemTransport(raft.ServerAddress(selfRaftAddr))
	defer func() { _ = trans.Close() }()
	cfg := raft.Configuration{Servers: []raft.Server{{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(selfID),
		Address:  raft.ServerAddress(selfRaftAddr),
	}}}
	if err := raft.BootstrapCluster(raftConfig(Config{LocalID: raft.ServerID(selfID)}),
		store, store, snaps, trans, cfg); err != nil {
		return fmt.Errorf("cluster: BootstrapCluster({%s}): %w", selfID, err)
	}
	return nil
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
	return recoverClusterToSelf(dataDir, dbPath, selfID, selfRaftAddr, logger)
}

// GrowReadySnapshot makes a freshly-bootstrapped (or already-init'd) single-voter store
// GROW-READY by writing a full FSM snapshot (the online SQLite backup carrying every
// direct-seeded row) and DeleteRange-truncating the log so FirstIndex advances PAST the
// bootstrap/seed entries. This is the keystone of the grow-onto-migrated-broker fix: a
// broker migrated by `cluster init --from-existing` direct-seeds rows into SQLite that no
// log entry created, so without a snapshot a fresh joiner replays the log from index 1 onto
// an un-seeded DB and FK-fail-stops (or — silently worse — replays an FK-safe log onto empty
// tables and is promoted as a hollow voter). After this call FirstIndex>1, so a joiner's
// nextIndex decays below FirstIndex, raft ships InstallSnapshot (not the log), and fsm.Restore
// loads the full DB — no replay, no FK crash, no hollow fork.
//
// It is mechanically identical to RecoverSingleNode (both raft.RecoverCluster to {self}), but
// semantically distinct: the caller does NOT raise the force_single marker and the config is
// already {self} (a no-op rewrite at N=1). Offline only — daemon STOPPED, flock held. The
// caller is responsible for the D5 audit-window guard (drain audit_published_index up to the
// snapshot index BEFORE compacting, else accept a bounded LOUD loss — RecoverCluster's
// DeleteRange is unconditional).
func GrowReadySnapshot(dataDir, dbPath, selfID, selfRaftAddr string, logger *slog.Logger) error {
	return recoverClusterToSelf(dataDir, dbPath, selfID, selfRaftAddr, logger)
}

// RebuildSingleNodeFromDB atomically replaces an offline node's Raft store with a
// fresh {self} store whose first configuration/snapshot index is above baseIndex.
// The SQLite DB is the authoritative, already-recovered FSM image. Keeping the
// new Raft indices above its durable applied_index is load-bearing: bootstrapping
// again at index 1 would make the FSM skip all new commands until Raft caught up
// with the old timeline.
//
// The replacement store is fully built in a sibling directory, then exchanged
// with raft/ using renameat2(RENAME_EXCHANGE). There is no crash window with a
// missing or half-built live raft directory; a failure before the exchange leaves
// the original untouched, and the old store is removed only after the exchange.
func RebuildSingleNodeFromDB(dataDir, dbPath, selfID, selfRaftAddr string, baseIndex uint64, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if baseIndex == ^uint64(0) {
		return errors.New("cluster: cannot rebuild raft above maximum applied_index")
	}
	stageRoot, err := os.MkdirTemp(dataDir, ".raft-rebuild-")
	if err != nil {
		return fmt.Errorf("cluster: create staged raft rebuild: %w", err)
	}
	defer func() { _ = os.RemoveAll(stageRoot) }()

	// Build the replacement from a snapshot directly. Moving only a bootstrap
	// configuration log from index 1 to baseIndex+1 creates a log hole: with no
	// preceding snapshot, RecoverCluster still replays from index 1 and fails.
	// A snapshot at baseIndex+1 is the correct representation of the authoritative
	// DB: the one-index gap is a non-command boundary, and the embedded {self}
	// configuration lets the next real command start strictly above applied_index.
	stagedRaft := filepath.Join(stageRoot, "raft")
	if err := os.MkdirAll(stagedRaft, 0o700); err != nil {
		return fmt.Errorf("cluster: mkdir staged raft rebuild: %w", err)
	}
	// Production's fail-closed cluster-mode probe intentionally requires the
	// raft.db path before it opens any Raft store (opening one has the side effect
	// of creating it). A snapshot-only replacement is valid to Hashicorp Raft but
	// is therefore invisible to the broker and crash-loops as "no raft state".
	// Create the empty stable/log store alongside the snapshot; HasExistingState
	// then proves state from the snapshot without weakening that startup guard.
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(stagedRaft, "raft.db")})
	if err != nil {
		return fmt.Errorf("cluster: create staged raft store: %w", err)
	}
	if err := store.Close(); err != nil {
		return fmt.Errorf("cluster: close staged raft store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(stagedRaft, 2, io.Discard)
	if err != nil {
		return fmt.Errorf("cluster: open staged snapshot store: %w", err)
	}
	config := raft.Configuration{Servers: []raft.Server{{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(selfID),
		Address:  raft.ServerAddress(selfRaftAddr),
	}}}
	snapshotIndex := baseIndex + 1
	_, trans := raft.NewInmemTransport(raft.ServerAddress(selfRaftAddr))
	defer func() { _ = trans.Close() }()
	sink, err := snaps.Create(raft.SnapshotVersionMax, snapshotIndex, 1, config, snapshotIndex, trans)
	if err != nil {
		return fmt.Errorf("cluster: create staged raft snapshot: %w", err)
	}
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("cluster: open DB for staged raft snapshot: %w", err)
	}
	persistErr := (&fsmSnapshot{ro: ro, tmpDir: filepath.Dir(dbPath)}).Persist(sink)
	closeErr := ro.Close()
	if persistErr != nil {
		return fmt.Errorf("cluster: persist staged raft snapshot: %w", persistErr)
	}
	if closeErr != nil {
		return fmt.Errorf("cluster: close DB after staged raft snapshot: %w", closeErr)
	}

	liveRaft := filepath.Join(dataDir, "raft")
	// Round-5 S5-02: the staged tree is owned by whoever RAN the tool. The runbook says to run force-single
	// with sudo, so a root-run rebuild would transplant root:root onto raft/ and the `User=tether` daemon
	// would then EACCES-crash-loop FOREVER on the last surviving broker — with every documented recovery
	// command also failing EACCES. Mirror the LIVE store's ownership+mode onto the staged tree BEFORE the
	// swap so the daemon that has to open it afterwards still can.
	if err := mirrorTreeOwnership(liveRaft, stagedRaft); err != nil {
		return fmt.Errorf("cluster: mirror raft store ownership onto the rebuilt tree: %w", err)
	}
	// Round-5 B2: ONE atomic exchange, no fallback — a three-step rename would pass through a window with no
	// raft/ at all. AtomicExchangeCapable proved the capability before any mutation began.
	//
	// Round-6 HONESTY: that precheck is NOT a total guarantee, and this comment used to claim it was. The
	// probe exchanges two fresh EMPTY siblings; the real target is the long-lived dataDir/raft, so a separate
	// mount there still yields EBUSY, and ENOSPC is not probed at all (the rebuild writes ~2x tether.db
	// between the prune and this point). Both surface HERE — after the prune. The rename is crash-consistent;
	// the surrounding transaction is not yet. The recovery journal is what makes those cases forward-
	// completable rather than terminal.
	if err := swapDirs(stagedRaft, liveRaft); err != nil {
		return fmt.Errorf("cluster: swap the rebuilt raft store into place: %w", err)
	}
	if dir, err := os.Open(dataDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	logger.Warn("cluster: rebuilt single-node raft store from recovered FSM",
		"self", selfID, "base_index", baseIndex, "snapshot_floor", snapshotIndex)
	return nil
}

// mirrorTreeOwnership copies ref's uid/gid + directory mode onto every entry under dst. Round-5 S5-02: the
// offline tools are run by an operator (the runbook says `sudo`), but the store is re-opened by the
// `User=tether` daemon — a rebuilt tree carrying the operator's ownership permanently EACCES-crash-loops the
// survivor. Best-effort per entry only when we CAN read ref's owner (a non-unix FileInfo is not fatal: the
// caller is then on a platform that does not run the daemon).
func mirrorTreeOwnership(ref, dst string) error {
	fi, err := os.Stat(ref)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no live store to mirror (fresh node) — leave the staged ownership as-is
		}
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	uid, gid := int(st.Uid), int(st.Gid)
	mode := fi.Mode().Perm()
	return filepath.Walk(dst, func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := os.Lchown(p, uid, gid); err != nil {
			return fmt.Errorf("chown %s: %w", p, err)
		}
		if info.IsDir() {
			if err := os.Chmod(p, mode); err != nil {
				return fmt.Errorf("chmod %s: %w", p, err)
			}
		}
		return nil
	})
}

// ErrAtomicExchangeUnsupported is the fail-CLOSED precondition for any offline store rebuild: this
// filesystem/platform cannot swap the store atomically, so the rebuild would have to pass through a state
// where raft/ does not exist. Round-5 B2: that is NOT crash-consistent, so the operation is REFUSED UP
// FRONT — before a single byte of SQLite/Raft is touched — instead of being discovered after the roster
// prune has already made the node un-recoverable.
var ErrAtomicExchangeUnsupported = errors.New("cluster: this filesystem cannot atomically exchange directories (RENAME_EXCHANGE), so an offline raft-store rebuild cannot be made crash-consistent here — run the recovery on a broker host whose data dir is on a filesystem that supports it (ext4/xfs/btrfs on Linux ≥3.15)")

// AtomicExchangeCapable probes — WITHOUT touching anything real — whether dataDir's filesystem can perform
// the atomic directory exchange the rebuild depends on. It exchanges two throwaway probe directories and
// removes them. Round-5 B2: callers MUST run this before any destructive mutation.
func AtomicExchangeCapable(dataDir string) error {
	a, err := os.MkdirTemp(dataDir, ".xchg-probe-a-")
	if err != nil {
		return fmt.Errorf("cluster: create exchange probe: %w", err)
	}
	defer func() { _ = os.RemoveAll(a) }()
	b, err := os.MkdirTemp(dataDir, ".xchg-probe-b-")
	if err != nil {
		return fmt.Errorf("cluster: create exchange probe: %w", err)
	}
	defer func() { _ = os.RemoveAll(b) }()
	switch err := exchangeDirs(a, b); {
	case err == nil:
		return nil
	case errors.Is(err, errExchangeUnsupported):
		return ErrAtomicExchangeUnsupported
	default:
		return fmt.Errorf("cluster: probe atomic directory exchange: %w", err)
	}
}

// swapDirs puts staged where live is (and live where staged is, so the caller's cleanup removes the old
// store) in ONE atomic step. Round-5 B2: there is deliberately NO non-atomic fallback — a three-step rename
// passes through a window in which raft/ is missing (the production startup probe then reports "no raft
// state") and, worse, it would run AFTER the roster prune already mutated the node. Capability is a
// PRECONDITION (AtomicExchangeCapable), checked before any mutation; reaching an unsupported filesystem
// here means the caller skipped that check.
func swapDirs(staged, live string) error {
	switch err := exchangeDirs(staged, live); {
	case err == nil:
		return nil
	case errors.Is(err, errExchangeUnsupported):
		return fmt.Errorf("%w (this should have been refused by the AtomicExchangeCapable precondition)", ErrAtomicExchangeUnsupported)
	default:
		return err
	}
}

func recoverClusterToSelf(dataDir, dbPath, selfID, selfRaftAddr string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if selfID == "" || selfRaftAddr == "" {
		return errors.New("cluster: recoverClusterToSelf requires selfID and selfRaftAddr")
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
		localID:  selfID,
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
