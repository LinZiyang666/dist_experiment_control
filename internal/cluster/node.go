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

	// HeartbeatTimeout / ElectionTimeout / LeaderLeaseTimeout / CommitTimeout
	// override raft's per-Node timeouts. Zero => the D1/D2 fast N=1 defaults
	// (200ms / 20ms) so existing single-node tests are byte-unchanged. D3
	// multinode harnesses (and the D9 production cutover) pass the safe
	// Multinode* values below (1000/1000/500ms). raft validates
	// LeaderLeaseTimeout <= HeartbeatTimeout <= ElectionTimeout (config.go:369/372).
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration

	// BootstrapPeers, when non-empty, is the initial raft Configuration used on
	// FIRST boot (no existing state) — the D3 static multi-node bootstrap. Empty
	// => bootstrap a {self}-only single-node cluster (production / D1 / D2).
	// Dynamic membership (raft.AddVoter + join-PoP) is D7, NOT this.
	BootstrapPeers []raft.Server
}

// Multinode raft timeouts (§8.4 / d3-plan R6): the safe values D3 harnesses and
// the D9 production cutover pass into Config. LeaderLeaseTimeout=500ms (the raft
// default), deliberately NOT 1000ms — a longer lease widens the §3.2 stale-leader
// fail-open window. raft requires Lease <= Heartbeat <= Election.
const (
	MultinodeHeartbeatTimeout   = 1000 * time.Millisecond
	MultinodeElectionTimeout    = 1000 * time.Millisecond
	MultinodeLeaderLeaseTimeout = 500 * time.Millisecond
)

// Node is a single-node raft state layer.
type Node struct {
	raft         *raft.Raft
	fsm          *fsm
	store        *raftboltdb.BoltStore
	db           *sql.DB        // write pool (owned)
	ro           *sql.DB        // read-only backup handle (owned)
	transport    raft.Transport // injected; closed by Shutdown (raft does not reap it)
	applyTimeout time.Duration
	localID      raft.ServerID // this node's raft ServerID (== cluster_nodes.node_id); see SelfID
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
	// On ANY error exit, close the injected transport ourselves: New returns
	// (nil, err) so the caller has no *Node to Shutdown, and the transport's
	// listener fd + accept goroutine would otherwise leak (d3-review B3). On
	// success the Node owns it and Shutdown closes it.
	success := false
	defer func() {
		if !success {
			if c, ok := cfg.Transport.(io.Closer); ok {
				_ = c.Close()
			}
		}
	}()
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
	rc := raftConfig(cfg)

	existing, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		_ = store.Close()
		_ = ro.Close()
		_ = db.Close()
		return nil, fmt.Errorf("cluster: probe existing raft state: %w", err)
	}
	if !existing {
		// D3 static multi-node bootstrap: if BootstrapPeers is set, bring up the
		// whole initial voter set at once; else a {self}-only single node. Dynamic
		// AddVoter/join-PoP is D7 (not here).
		servers := cfg.BootstrapPeers
		if len(servers) == 0 {
			servers = []raft.Server{{
				Suffrage: raft.Voter, ID: cfg.LocalID, Address: cfg.Transport.LocalAddr(),
			}}
		}
		cfgr := raft.Configuration{Servers: servers}
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

	success = true // Node now owns the transport; Shutdown will close it.
	return &Node{
		raft:         r,
		fsm:          f,
		store:        store,
		db:           db,
		ro:           ro,
		transport:    cfg.Transport,
		applyTimeout: applyTimeout,
		localID:      cfg.LocalID,
		logger:       logger,
	}, nil
}

// SelfID returns this node's raft ServerID as a string (== cluster_nodes.node_id).
// The broker uses it as the "self" home identity in tunnelTokenLookup's
// home_broker==self filter (D6 §7.2/R-10). It is stable for the node's lifetime.
func (n *Node) SelfID() string { return string(n.localID) }

// RODB returns the node's READ-ONLY SQLite handle (the dedicated read pool, separate
// from the FSM write pool). At the D9 cutover the broker's reads route through this so
// the FSM stays the single writer on the merged WAL DB (§3.8/§590): broker code does
// `b.cfg.DB = node.RODB()` and routes every authoritative write through Propose. WAL
// permits concurrent readers + one writer, so reads here never block the FSM.
func (n *Node) RODB() *sql.DB { return n.ro }

// DB returns the node's WAL WRITE pool. It is the FSM's single-writer handle; callers
// MUST NOT issue authoritative writes through it directly (that bypasses raft and
// breaks replication) — it is exposed only for the offline `cluster init --from-existing`
// seed path, which writes BEFORE raft is serving. Live writes go through Propose.
func (n *Node) DB() *sql.DB { return n.db }

// BackupDBTo writes a consistent online-backup of the committed FSM DB into dstPath, off the
// read-only handle (the same paged modernc backup the snapshot path uses). It takes NO raft
// action — it does NOT truncate the log the way Snapshot()/raft.Snapshot() does — so it is a
// safe operator backup any node (leader or follower) can serve while the FSM keeps writing
// through the WAL. dstPath must not already exist (modernc NewBackup creates it). This is the
// B6 `cluster backup` seam: the bundle's state.db is exactly this file; the raft log is
// node-local and is re-bootstrapped fresh on restore, so it is intentionally not carried.
func (n *Node) BackupDBTo(ctx context.Context, dstPath string) error {
	if n.ro == nil {
		return fmt.Errorf("cluster: BackupDBTo: node has no read handle")
	}
	return backupTo(ctx, n.ro, dstPath)
}

// raftConfig builds the raft.Config. Timeouts come from cfg when set, else the
// D1/D2 fast N=1 defaults (200ms / 20ms) so existing single-node tests are
// byte-unchanged; D3 multinode harnesses pass cluster.Multinode* values via
// Config. PreVoteDisabled stays false (pre-vote enabled — re-proven over the real
// mTLS transport in D3; vacuous at N=1).
func raftConfig(cfg Config) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = cfg.LocalID
	c.HeartbeatTimeout = orDur(cfg.HeartbeatTimeout, 200*time.Millisecond)
	c.ElectionTimeout = orDur(cfg.ElectionTimeout, 200*time.Millisecond)
	c.LeaderLeaseTimeout = orDur(cfg.LeaderLeaseTimeout, 200*time.Millisecond)
	c.CommitTimeout = orDur(cfg.CommitTimeout, 20*time.Millisecond)
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

// orDur returns v when positive, else def.
func orDur(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

// IsNotLeader reports whether err is the raft "not the leader" family
// (raft.ErrNotLeader / raft.ErrLeadershipLost) that Apply/Propose surface
// unwrapped on a follower or a just-deposed leader. It is the seam-building
// primitive the broker/D9 uses to translate a Propose failure into the
// raft-free authcallout.ErrNotLeader sentinel (architecture L-2 keeps raft out of
// authcallout): `if cluster.IsNotLeader(err) { return authcallout.ErrNotLeader }`.
func IsNotLeader(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost)
}

// ErrForwardNotLeader is the raft-free sentinel a broker-side forwarder (§4.1 D4,
// internal/broker/cluster_forward.go) returns when a forwarded write could not be
// committed by a leader — the answering broker replied not_leader, or the NATS
// request timed out (NOT proof of non-commit; the retry's stable ReqID dedups). It
// is RETRIABLE (re-request the broadcast bus, never the deposed broker). The broker
// maps it to authcallout.ErrNotLeader for the PIN path; cluster stays nats-free so
// the sentinel (not the wire) is what the broker adapter translates.
var ErrForwardNotLeader = errors.New("cluster: forwarded write not committed by a leader (retriable)")

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
		// A committed-but-op-skipped outcome (poison / dedup / deterministic reject) is a
		// SUCCESSFUL commit, NOT an Apply failure — the entry is durable and applied_index
		// advanced. Never surface it as an error (the forged-sig poison-skip path relies on
		// Apply returning nil). Only a genuine error Response is returned. (Belt-and-suspenders
		// with the "applied* must not implement error" invariant in fsm.go.)
		switch resp.(type) {
		case appliedOK, appliedNoOp, appliedPoison, appliedDedup, appliedRejected:
			return nil
		}
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
//
// LEADER GATE (external-review F1): Plan is LEADER-ONLY (§3.3) — it reads n.db as
// the authoritative leader DB and bakes keys/fences. On a FOLLOWER the local
// replica may be bounded-stale, so running Plan there can leak a spurious PERMANENT
// business error (e.g. agentprov.ErrSessionMissing when the session row has not
// replicated yet) instead of the transient not-leader signal the caller needs
// (D3-R3). So fail fast with raft.ErrNotLeader BEFORE planning when not leader; the
// seam maps it to authcallout.ErrNotLeader via cluster.IsNotLeader. A leadership
// loss racing between this check and Apply surfaces as raft.ErrLeadershipLost from
// Apply — also transient (cluster.IsNotLeader). Held under applyMu so the check is
// part of the same serialized {gate -> Plan -> Apply} window.
func (n *Node) Propose(plan func(db *sql.DB) (*Command, error)) error {
	return n.ProposeWithReqID("", plan)
}

// ProposeWithReqID is Propose with a cross-retry-stable idempotency key (§4.1 D4).
// It stamps reqID into the planned Command before Apply so the FSM dedups a
// committed-but-ack-lost forwarder retry (same reqID at a new raft index). The
// leader gate, applyMu window, and nil-command no-op are identical to Propose
// (which delegates here with an empty reqID; D1/D2 callers are byte-unchanged). The
// reqID is the broker-side forwarding key (the originating broker mints it; NEVER
// the leader — a per-leader re-mint cannot dedup an already-committed entry).
func (n *Node) ProposeWithReqID(reqID string, plan func(db *sql.DB) (*Command, error)) error {
	// Validate the key BEFORE proposing (external-review F2): an invalid reqID would
	// otherwise be stamped into a Command that decodeCommand POISONS on apply — the
	// FSM skips the op SQL but still advances applied_index, and Node.Apply maps the
	// non-error appliedPoison to nil, so the caller would see a false "ok" for a write
	// that never executed. A malformed key cannot become valid on retry, so this is a
	// permanent error (the responder maps it to a non-ok typed reply, never retriable).
	if reqID != "" && !validReqID(reqID) {
		return fmt.Errorf("cluster: invalid req_id %q (must be <=64 lowercase hex)", reqID)
	}
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	if n.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	cmd, err := plan(n.db)
	if err != nil {
		return err
	}
	if cmd == nil {
		return nil
	}
	if reqID != "" {
		cmd.ReqID = reqID
	}
	return n.Apply(cmd)
}

// DedupCount returns how many committed entries the FSM has short-circuited via the
// §4.1 D4 cross-retry ReqID ledger. Tests read it to assert the dedup branch fired
// (the in-scope writes are SQL-idempotent, so a row-count assertion is vacuous).
func (n *Node) DedupCount() uint64 { return n.fsm.dedupCount.Load() }

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

// TransferLeadership asks raft to hand leadership to a caught-up follower and blocks
// until the transfer completes or fails. Leader-only (returns raft.ErrNotLeader
// otherwise). Used by the D4 forwarding tests to deterministically land a retry under
// a NEW leader (decoupling the commit fact from the leadership change, §0bis-C) and
// by D7 drain. Not part of the §5 op set (it is a raft config/leadership action).
func (n *Node) TransferLeadership() error { return n.raft.LeadershipTransfer().Error() }

// Shutdown stops raft and closes the stores + DB handles in deterministic order.
func (n *Node) Shutdown() error {
	var errs []error
	if n.raft != nil {
		errs = append(errs, n.raft.Shutdown().Error())
	}
	// Close the injected transport ourselves — raft.Shutdown() does not reap an
	// externally-supplied transport (d3-plan R5). NetworkTransport holds a listener
	// + goroutines; InmemTransport.Close is a harmless DisconnectAll.
	if c, ok := n.transport.(io.Closer); ok {
		errs = append(errs, c.Close())
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
