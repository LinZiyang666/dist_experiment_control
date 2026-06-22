package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// defaultApplyTimeout bounds a single raft.Apply round-trip.
const defaultApplyTimeout = 10 * time.Second

// Config constructs a cluster Node.
type Config struct {
	// LocalID is this node's raft ServerID (== cluster_nodes.node_id later).
	LocalID raft.ServerID
	// DataDir holds the raft sub-tree (raft/raft.db + raft/snapshots).
	DataDir string
	// DBPath is the cluster FSM SQLite file (opened WAL). Snapshot/restore temp
	// files are created in its directory (same filesystem).
	DBPath string
	// Transport is injected. D1 tests pass raft.NewInmemTransport; the real mTLS
	// NetworkTransport is D3. Required.
	Transport raft.Transport
	// Logger backs the FSM's own logging (poison entries etc.). Defaults to
	// slog.Default(). raft's internal chatter is discarded in D1.
	Logger *slog.Logger
	// ApplyTimeout overrides defaultApplyTimeout when non-zero.
	ApplyTimeout time.Duration
}

// Node is a single-node raft state layer.
type Node struct {
	raft         *raft.Raft
	fsm          *fsm
	store        *raftboltdb.BoltStore
	db           *sql.DB // write pool (owned)
	ro           *sql.DB // read-only backup handle (owned)
	applyTimeout time.Duration
	logger       *slog.Logger

	// applyMu serializes the leader-side {Plan reads leader DB} + {raft.Apply} +
	// {await} window so two concurrent compound mutators (e.g. PlanAllocate's
	// findFreePort scan) cannot bake conflicting keys that both commit
	// (d2-plan §3 PA-5/PA-8). Held only by Propose; liveness/GC writes do NOT take
	// it (they touch disjoint tables/columns — the table-ownership disjointness
	// argument in d2-plan §3).
	applyMu sync.Mutex
}

// New opens the cluster DB (WAL + a dedicated read-only handle), the raft
// log/stable/snapshot stores, bootstraps a single-node cluster on first boot, and
// starts raft. On any failure the partially-opened resources are closed.
func New(cfg Config) (*Node, error) {
	if cfg.Transport == nil {
		return nil, errors.New("cluster: New requires a Transport")
	}
	if cfg.DBPath == "" || cfg.DataDir == "" {
		return nil, errors.New("cluster: New requires DBPath and DataDir")
	}
	if cfg.LocalID == "" {
		return nil, errors.New("cluster: New requires a LocalID")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	applyTimeout := cfg.ApplyTimeout
	if applyTimeout == 0 {
		applyTimeout = defaultApplyTimeout
	}

	dsn := "file:" + cfg.DBPath
	db, err := storage.OpenWAL(dsn)
	if err != nil {
		return nil, err
	}
	ro, err := storage.OpenReadOnly(dsn)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o700); err != nil {
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: mkdir raft dir: %w", err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(raftDir, "raft.db")})
	if err != nil {
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: open boltstore: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		_ = store.Close()
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: open snapshot store: %w", err)
	}

	f := &fsm{
		db:       db,
		ro:       ro,
		tmpDir:   filepath.Dir(cfg.DBPath),
		dbPath:   cfg.DBPath,
		appliers: defaultAppliers(),
		logger:   logger,
	}
	rc := raftConfig(cfg.LocalID)

	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		_ = store.Close()
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: probe existing raft state: %w", err)
	}
	if !existing {
		cfgr := raft.Configuration{Servers: []raft.Server{{
			Suffrage: raft.Voter, ID: cfg.LocalID, Address: cfg.Transport.LocalAddr(),
		}}}
		if err := raft.BootstrapCluster(rc, store, store, snaps, cfg.Transport, cfgr); err != nil {
			_ = store.Close()
			_ = ro.Close()
			_ = db.Close()
			return nil, fmt.Errorf("cluster: bootstrap: %w", err)
		}
	}

	r, err := raft.NewRaft(rc, f, store, store, snaps, cfg.Transport)
	if err != nil {
		_ = store.Close()
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: NewRaft: %w", err)
	}

	return &Node{
		raft:         r,
		fsm:          f,
		store:        store,
		db:           db,
		ro:           ro,
		applyTimeout: applyTimeout,
		logger:       logger,
	}, nil
}

// raftConfig builds the raft.Config. PreVoteDisabled stays false (pre-vote
// enabled — a forward invariant for D3 multinode; vacuous at N=1). Sub-second
// timeouts give fast N=1 leadership with no contention; D3 tunes them for
// multinode.
func raftConfig(id raft.ServerID) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = id
	c.HeartbeatTimeout = 200 * time.Millisecond
	c.ElectionTimeout = 200 * time.Millisecond
	c.LeaderLeaseTimeout = 200 * time.Millisecond
	c.CommitTimeout = 20 * time.Millisecond
	c.LogOutput = io.Discard // D1: discard raft's internal chatter; D3 wires logging
	// SQLite is the durable apply authority (§3.7): on restart it already holds
	// the committed state, and raft re-applies the log idempotently (the FSM
	// self-skips index <= applied_index). So do NOT let raft roll SQLite back to a
	// snapshot on every startup — that would contradict "applied_index authority =
	// SQLite" and add a restore-interruption window. fsm.Restore is therefore
	// reached only via InstallSnapshot (follower catch-up, D3) and the §13.3
	// direct restore test; the N=1 crash-recovery path is pure idempotent replay.
	c.NoSnapshotRestoreOnStart = true
	return c
}

// Apply proposes a command through raft. On a deposed leader the returned error
// is raft.ErrLeadershipLost / raft.ErrNotLeader, UNWRAPPED so D4 can type-match.
// A typed FSM error (e.g. an op business error) surfaces via the future Response.
func (n *Node) Apply(cmd *Command) error {
	data, err := cmd.encode()
	if err != nil {
		return err
	}
	fut := n.raft.Apply(data, n.applyTimeout)
	if err := fut.Error(); err != nil {
		return err
	}
	if resp := fut.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// ApplyMetaSet is a thin D1 convenience that exercises the full Plan/Apply seam:
// the leader-only Planner renders the op (reading the leader DB + baking the
// literal), then it is proposed through raft and applied on every replica.
func (n *Node) ApplyMetaSet(key, value string) error {
	cmd, err := clusterMetaPlanner{}.Plan(context.Background(), n.db, metaSetReq{Key: key, Value: value})
	if err != nil {
		return err
	}
	return n.Apply(cmd)
}

// Propose renders an op on the leader DB and applies it through raft, holding the
// leader-side applyMu across the whole {Plan read -> raft.Apply -> await} window.
// This is the D2 ops-only seam (the per-op typed Plan* funcs live in the mutator
// packages and return a *Command). Any secret the plan mints (e.g. a raw port
// token) is captured by the closure and returned to the caller OUT OF BAND — it
// is NEVER part of the replicated Command (only its hash is). A plan returning a
// nil command + nil error is a no-op (nothing proposed).
//
// CONTRACT (load-bearing — fsm.db and n.db are the SAME single-writer pool,
// SetMaxOpenConns(1)): the plan closure MUST fully materialize and close every
// *sql.Rows and hold NO open *sql.Tx before returning the Command. Otherwise the
// FSM's Begin() inside raft.Apply blocks forever on the one pooled connection.
func (n *Node) Propose(plan func(db *sql.DB) (*Command, error)) error {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	cmd, err := plan(n.db)
	if err != nil {
		return err
	}
	if cmd == nil {
		return nil
	}
	return n.Apply(cmd)
}

// Snapshot forces raft to take a snapshot now (tests; raft's automatic cadence is
// time/threshold based).
func (n *Node) Snapshot() error { return n.raft.Snapshot().Error() }

// Barrier blocks until every log entry preceding the call has been applied to the
// FSM (architecture §3.2 read-after-write). On a freshly recovered node it forces
// the startup log replay to drain before reads. Leader-only.
func (n *Node) Barrier(timeout time.Duration) error { return n.raft.Barrier(timeout).Error() }

// WaitForLeader blocks until this node is the raft leader or timeout elapses.
func (n *Node) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.raft.State() == raft.Leader {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("cluster: no leadership within %s", timeout)
}

// IsLeader reports whether this node currently believes it is leader (bounded
// stale; for correctness-sensitive checks use VerifyLeaderRead).
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Shutdown stops raft and closes the stores + DB handles in deterministic order.
func (n *Node) Shutdown() error {
	var errs []error
	if n.raft != nil {
		errs = append(errs, n.raft.Shutdown().Error())
	}
	if n.store != nil {
		errs = append(errs, n.store.Close())
	}
	if n.ro != nil {
		errs = append(errs, n.ro.Close())
	}
	if n.db != nil {
		errs = append(errs, n.db.Close())
	}
	return errors.Join(errs...)
}
