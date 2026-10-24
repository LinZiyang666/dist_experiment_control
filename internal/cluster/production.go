package cluster

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
)

// production.go — the D9 cutover constructor. The broker (and every other caller
// outside this package) is forbidden from importing hashicorp/raft (the L-2 layering
// invariant: raft is confined to internal/cluster). But the D9 daemon must build a
// real mTLS raft Node from string config + on-disk secrets. NewProduction is that
// seam: it takes a pure-string ProductionConfig, reads + parses the cluster-CA and
// this node's route leaf, builds the mTLS NetworkTransport, and constructs the Node
// with the safe multinode raft timeouts — all raft/transport types stay inside this
// package, so the broker holds only a *cluster.Node.

// ProductionConfig is the string-only config the D9 daemon passes to NewProduction.
// All identity (LocalID == cluster_nodes.node_id == raft ServerID) and paths come from
// the seeded DB + the §15 secrets dir; nothing here is a raft type, so the broker can
// construct it without importing raft.
type ProductionConfig struct {
	// LocalID is this node's raft ServerID (== cluster_nodes.node_id). Read from the
	// DB marker (cluster_meta self_node_id) by the daemon before this call.
	LocalID string
	// DataDir / DBPath: the raft sub-tree parent and the merged WAL SQLite file.
	DataDir string
	DBPath  string
	// RaftBind is the mTLS raft transport listen address (private net, e.g.
	// "0.0.0.0:7400").
	RaftBind string
	// CACertPath is the cluster CA PEM (trust anchor for peer leaves).
	CACertPath string
	// LeafCertPath / LeafKeyPath: this node's cluster-CA-signed route leaf + key.
	LeafCertPath string
	LeafKeyPath  string
	// Logger backs both the FSM's own logging and the raft library bridge
	// (raftlog.go). Batch-A review M1: this field did not exist, so every
	// production Node was built with a nil Logger and raftLogBridge.emit
	// returned on its first line — the raft logging A15 claimed to have "finally
	// wired" was, on the only path that actually runs in production, a black
	// hole. nil is still tolerated (tests construct Config directly), but the
	// daemon must pass one.
	Logger *slog.Logger

	// Storage callbacks keep hot construction independent of cold migrations.
	ImageOpenCurrent storage.OpenCurrentFunc
	ImageReadOnly    func(string) (*sql.DB, error)
	ImageRegistry    storage.DriverRegistry
	ImageStamp       func(*sql.DB) (storage.StoreStamp, error)
}

// NewProduction builds the mTLS transport and the cluster Node for the D9 daemon. The
// returned Node owns the transport (Node.Shutdown closes it). It uses the multinode
// raft timeouts (§8.4 / d3-plan R6) — the production cutover never uses the D1/D2 fast
// N=1 values. On any error nothing is leaked (NewMTLSTransport's listener is closed by
// New's failure path once injected; a pre-injection error closes nothing raft-side).
func NewProduction(cfg ProductionConfig) (*Node, error) { return newProduction(cfg, nil) }

// NewProductionForImage uses production election timers with the current-store
// opener. It must not bootstrap or migrate after an image cut.
// origin: hot-update S6 plan §3.3 / T10
func NewProductionForImage(cfg ProductionConfig, stamp storage.StoreStamp) (*Node, error) {
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	return newProduction(cfg, &stamp)
}

func newProduction(cfg ProductionConfig, stamp *storage.StoreStamp) (*Node, error) {
	if cfg.LocalID == "" {
		return nil, fmt.Errorf("cluster: NewProduction requires LocalID (the seeded self node_id)")
	}
	caPEM, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("cluster: read cluster CA %q: %w", cfg.CACertPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("cluster: cluster CA %q contains no PEM certificate", cfg.CACertPath)
	}
	leaf, err := tls.LoadX509KeyPair(cfg.LeafCertPath, cfg.LeafKeyPath)
	if err != nil {
		return nil, fmt.Errorf("cluster: load route leaf: %w", err)
	}

	mtlsCfg := MTLSTransportConfig{
		BindAddr: cfg.RaftBind,
		CACert:   pool,
		Leaf:     leaf,
		Timeout:  10 * time.Second,
	}
	trans, err := NewMTLSTransport(mtlsCfg)
	if err != nil {
		return nil, err
	}

	nodeConfig := Config{
		ImageOpenCurrent: cfg.ImageOpenCurrent, ImageReadOnly: cfg.ImageReadOnly, ImageRegistry: cfg.ImageRegistry, ImageStamp: cfg.ImageStamp,
		LocalID:            raft.ServerID(cfg.LocalID),
		Logger:             cfg.Logger,
		DataDir:            cfg.DataDir,
		DBPath:             cfg.DBPath,
		Transport:          trans,
		HeartbeatTimeout:   MultinodeHeartbeatTimeout,
		ElectionTimeout:    MultinodeElectionTimeout,
		LeaderLeaseTimeout: MultinodeLeaderLeaseTimeout,
		// BootstrapPeers empty: a D9 daemon ALWAYS finds an existing raft/ (created by
		// `cluster init [--from-existing]`), so cluster.New takes the HasExistingState
		// path and never auto-bootstraps. The detection gate (broker.clusterModeEnabled)
		// has already refused startup if raft/ is absent.
		//
		// TransportFactory lets the online force-single (RecoverToSelfOnline) rebuild the mTLS
		// transport after re-recovering raft IN-PROCESS — it re-binds the same RaftBind (Go sets
		// SO_REUSEADDR; harmless at N=1 with confirmed-dead peers). Without it the online path
		// refuses and the offline floor is used. Captures mtlsCfg (the pool + leaf are immutable).
		TransportFactory: func() (raft.Transport, error) { return NewMTLSTransport(mtlsCfg) },
	}
	if stamp != nil {
		return NewForImage(nodeConfig, *stamp)
	}
	return New(nodeConfig)
}

// nodeImageLifecycle records actual close outcomes. A failed close is sticky:
// retrying Shutdown cannot promote an unknown object to closed by assumption.
// origin: hot-update S6 plan §3.2 / T10
type nodeImageLifecycle struct {
	generationMu  sync.RWMutex
	shutdownMu    sync.Mutex
	mu            sync.Mutex
	seal          storage.StoreSeal
	prepared      bool
	shutdownDone  bool
	shutdownErr   error
	deadline      controlbudget.Deadline
	notify        chan bool
	roleStop      chan struct{}
	roleDone      chan struct{}
	roleBarrier   chan chan struct{}
	roleStarted   bool
	role          storage.RoleReceipt
	fenced        bool
	work          uint32
	backups       uint32
	changed       chan struct{}
	backupContext context.Context
	cancelBackups context.CancelFunc
	closeHook     func(string) error // populated only by in-package tests
}

func newNodeImageLifecycle() *nodeImageLifecycle {
	// ctx-root: cancellable backup lifecycle owned by this Node generation.
	ctx, cancel := context.WithCancel(context.Background())
	return &nodeImageLifecycle{
		seal:   storage.StoreSeal{Writer: storage.CloseClosed, ReadOnly: storage.CloseClosed, Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed},
		notify: make(chan bool, 1), roleStop: make(chan struct{}), roleDone: make(chan struct{}), roleBarrier: make(chan chan struct{}), changed: make(chan struct{}), backupContext: ctx, cancelBackups: cancel,
	}
}

func startImageRoleObserver(n *Node) {
	life := n.image
	life.roleStarted = true
	go func() {
		defer close(life.roleDone)
		for {
			select {
			case leader := <-life.notify:
				recordImageRoleEdge(life, leader)
			case ack := <-life.roleBarrier:
				// Raft's terminal notification is best-effort after shutdownCh
				// closes. One buffered slot retains it, and a barrier drains that
				// slot before acknowledging the observer's final join receipt.
				for draining := true; draining; {
					select {
					case leader := <-life.notify:
						recordImageRoleEdge(life, leader)
					default:
						draining = false
					}
				}
				close(ack)
			case <-life.roleStop:
				return
			}
		}
	}()
}

func recordImageRoleEdge(life *nodeImageLifecycle, leader bool) {
	life.mu.Lock()
	defer life.mu.Unlock()
	life.role.EdgeSeq++ // even false -> true -> false must be visible at cut
	life.role.LastRunningIsLeader = leader
}

func syncImageRole(n *Node) {
	life := n.image
	if !life.roleStarted {
		return
	}
	ack := make(chan struct{})
	select {
	case life.roleBarrier <- ack:
		<-ack
	case <-life.roleDone:
	}
}

// ReadImageRole binds every role edge to fresh term and committed configuration.
// Two follower samples do not rule out a leader interval between them.
// origin: hot-update S6 plan §5 / T38
func ReadImageRole(n *Node) (storage.RoleReceipt, error) {
	if n == nil || n.image == nil {
		return storage.RoleReceipt{}, errors.New("cluster: missing image role owner")
	}
	syncImageRole(n)
	life := n.image
	life.mu.Lock()
	receipt := life.role
	if receipt.ObserverClosed {
		receipt = life.seal.Role
		life.mu.Unlock()
		return receipt, nil
	}
	life.mu.Unlock()
	r := n.raft.Load()
	if r == nil {
		return receipt, errors.New("cluster: raft role unavailable")
	}
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return receipt, err
	}
	servers := append([]raft.Server(nil), future.Configuration().Servers...)
	sort.Slice(servers, func(i, j int) bool { return servers[i].ID < servers[j].ID })
	bytes, err := json.Marshal(servers)
	if err != nil {
		return receipt, err
	}
	digest := sha256.Sum256(bytes)
	receipt.ConfigIndex, err = readImageConfigIndex(n, servers, r.CommitIndex())
	if err != nil {
		return receipt, err
	}
	receipt.VoterDigest = hex.EncodeToString(digest[:])
	receipt.Term, err = strconv.ParseUint(r.Stats()["term"], 10, 64)
	if err != nil {
		return receipt, err
	}
	receipt.LastRunningIsLeader = r.State() == raft.Leader
	syncImageRole(n)
	life.mu.Lock()
	changed := receipt.EdgeSeq != life.role.EdgeSeq
	life.mu.Unlock()
	if changed {
		return receipt, errors.New("cluster: role changed during image observation")
	}
	return receipt, nil
}

// ValidateImageRole compares an earlier observation to a fresh one, including
// the monotone notification sequence. It is the final ABA guard for the driver.
// origin: hot-update S6 plan §5 / T38
func ValidateImageRole(n *Node, expected storage.RoleReceipt) error {
	current, err := ReadImageRole(n)
	if err != nil {
		return err
	}
	if current.EdgeSeq != expected.EdgeSeq || current.Term != expected.Term || current.ConfigIndex != expected.ConfigIndex || current.VoterDigest != expected.VoterDigest || current.LastRunningIsLeader != expected.LastRunningIsLeader || current.ObserverClosed != expected.ObserverClosed {
		return errors.New("cluster: image role or configuration changed before shutdown")
	}
	return nil
}

// Shutdown can overlap one final term/configuration change even after its
// pre-shutdown sample. The stopped Raft is immutable, so this second sample
// closes that window. A leader may emit its one terminal false notification;
// any other edge, including follower -> leader -> follower, invalidates the cut.
// origin: hot-update S6 plan §5 / T38
func validateStoppedImageRole(expected, current storage.RoleReceipt) error {
	maximumEdge := expected.EdgeSeq
	if expected.LastRunningIsLeader {
		maximumEdge++
	}
	if current.EdgeSeq < expected.EdgeSeq || current.EdgeSeq > maximumEdge || current.Term != expected.Term || current.ConfigIndex != expected.ConfigIndex || current.VoterDigest != expected.VoterDigest {
		return errors.New("cluster: image role changed during raft shutdown")
	}
	return nil
}

// Raft v1.7.3 GetConfiguration().Index() is zero: its public implementation
// copies latest but omits latestIndex. Recover the actual committed configuration
// index from durable log/snapshot metadata and bind it to the sampled voter set.
// origin: hot-update S6 plan §5 / T38
func readImageConfigIndex(n *Node, servers []raft.Server, committed uint64) (uint64, error) {
	desired, err := json.Marshal(servers)
	if err != nil {
		return 0, err
	}
	first, err := n.store.FirstIndex()
	if err != nil {
		return 0, err
	}
	last, err := n.store.LastIndex()
	if err != nil {
		return 0, err
	}
	var index uint64
	var configuration raft.Configuration
	for i := last; i >= first && i > 0; i-- {
		var entry raft.Log
		if err := n.store.GetLog(i, &entry); err != nil {
			return 0, err
		}
		if entry.Type == raft.LogConfiguration {
			index = entry.Index
			configuration = raft.DecodeConfiguration(entry.Data)
			break
		}
	}
	snapshots, err := n.snaps.List()
	if err != nil {
		return 0, err
	}
	for _, snapshot := range snapshots {
		if snapshot.ConfigurationIndex > index {
			index = snapshot.ConfigurationIndex
			configuration = snapshot.Configuration
		}
	}
	if index == 0 || index > committed {
		return 0, errors.New("cluster: image configuration is unknown or uncommitted")
	}
	sort.Slice(configuration.Servers, func(i, j int) bool { return configuration.Servers[i].ID < configuration.Servers[j].ID })
	actual, err := json.Marshal(configuration.Servers)
	if err != nil {
		return 0, err
	}
	if string(actual) != string(desired) {
		return 0, errors.New("cluster: image configuration changed during durable observation")
	}
	return index, nil
}

func beginNodeImageWork(n *Node, backup bool) (func(), error) {
	life := n.image
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.fenced {
		return nil, errors.New("cluster: image store admission fenced")
	}
	life.work++
	if backup {
		life.backups++
	}
	return func() {
		life.mu.Lock()
		defer life.mu.Unlock()
		life.work--
		if backup {
			life.backups--
		}
		close(life.changed)
		life.changed = make(chan struct{})
	}, nil
}

func fenceNodeImageWork(n *Node, deadline controlbudget.Deadline) error {
	life := n.image
	life.mu.Lock()
	life.fenced = true
	life.cancelBackups()
	for life.work != 0 {
		changed := life.changed
		life.mu.Unlock()
		if deadline == 0 {
			<-changed
		} else {
			remaining, err := clusterio.Remaining(deadline)
			if err != nil {
				return err
			}
			timer := time.NewTimer(remaining)
			select {
			case <-changed:
				timer.Stop()
			case <-timer.C:
				return clusterio.ErrDeadline
			}
		}
		life.mu.Lock()
	}
	life.mu.Unlock()
	return nil
}

// ImageStoreStamp reads through the storage-owner seam before final closing.
// The cold embedding path remains supported but is explicitly image-ineligible
// without a schema/identity reader and an actual-driver registry.
func ImageStoreStamp(n *Node) (storage.StoreStamp, error) {
	if n == nil || n.image == nil {
		return storage.StoreStamp{}, errors.New("cluster: missing store owner")
	}
	if n.cfg.ImageStamp == nil {
		return storage.StoreStamp{}, errors.New("cluster: image-ineligible storage stamp reader missing")
	}
	return n.cfg.ImageStamp(n.db)
}

func PrepareImageShutdown(n *Node, deadline controlbudget.Deadline) error {
	if n == nil || n.image == nil {
		return errors.New("cluster: missing image shutdown owner")
	}
	if _, err := clusterio.Remaining(deadline); err != nil {
		return err
	}
	life := n.image
	life.shutdownMu.Lock()
	defer life.shutdownMu.Unlock()
	if life.shutdownDone {
		return errors.New("cluster: image store already stopped")
	}
	if life.prepared {
		return errors.New("cluster: image shutdown already prepared")
	}
	if n.cfg.ImageRegistry == nil {
		return errors.New("cluster: image-ineligible actual driver registry missing")
	}
	if _, network := n.transport.(*imageNetworkTransport); network && (imageSnapshotInitialize == nil || imageSnapshotStage == nil || imageSnapshotFence == nil || imageSnapshotResume == nil || imageSnapshotBackup == nil) {
		return errors.New("cluster: image-ineligible snapshot hooks unavailable (S6 P05)")
	}
	if err := requireImageDataDirLock(n.cfg.DataDir); err != nil {
		return err
	}
	stamp, err := ImageStoreStamp(n)
	if err != nil {
		return err
	}
	if stamp.Identity.Path != n.cfg.DBPath || stamp.Identity.DataDir != n.cfg.DataDir || stamp.Journal != "wal" || stamp.Synchronous != 2 {
		return errors.New("cluster: image store binding or durability mismatch")
	}
	if err := n.cfg.ImageRegistry.AssertExclusive(stamp.Identity); err != nil {
		return err
	}
	if err := fenceNodeImageWork(n, deadline); err != nil {
		return err
	}
	// Transport IO retirement is irreversible. The real Shutdown entry below
	// owns it, so role/operation refusals here can resume the original writer.
	// origin: S6 internal review rv-final F1
	if _, ok := n.transport.(*imageNetworkTransport); !ok {
		if _, ok := n.transport.(*raft.InmemTransport); !ok {
			return errors.New("cluster: image-ineligible untracked raft transport")
		}
	}
	receipt, err := ReadImageRole(n)
	if err != nil {
		return err
	}
	// Resample at the actual shutdown boundary: admission can precede an
	// election or a sole voter's AddVoter by several seconds.
	// origin: hot-update S6 internal review rv-cluster-7
	if receipt.LastRunningIsLeader {
		voters, err := n.NumVoters()
		if err != nil {
			return err
		}
		if voters != 1 {
			return errors.New("cluster: multi-voter leader cannot prepare image shutdown")
		}
	}
	ops, err := NonTerminalOperations(n.RODB())
	if err != nil {
		return err
	}
	if len(ops) != 0 {
		return errors.New("cluster: membership operation blocks image shutdown")
	}
	life.seal.Stamp = stamp
	life.seal.Role = receipt
	life.deadline = deadline
	life.prepared = true
	return nil
}

// QuiesceImageStore fences direct proposals and cancels cancellable backups.
// Committed Apply already admitted to this Node finishes before the cut proceeds.
func QuiesceImageStore(n *Node, deadline controlbudget.Deadline) error {
	if n == nil || n.image == nil {
		return errors.New("cluster: missing store owner")
	}
	if _, err := clusterio.Remaining(deadline); err != nil {
		return err
	}
	return fenceNodeImageWork(n, deadline)
}

func ResumeImageStore(n *Node) error {
	if n == nil || n.image == nil {
		return errors.New("cluster: missing store owner")
	}
	life := n.image
	life.shutdownMu.Lock()
	defer life.shutdownMu.Unlock()
	if life.shutdownDone {
		return errors.New("cluster: store shutdown cannot resume old admission")
	}
	if transport, ok := n.transport.(*imageNetworkTransport); ok && transport.fenced.Load() {
		return errors.New("cluster: transport cut cannot resume old admission")
	}
	life.mu.Lock()
	if life.work != 0 {
		life.mu.Unlock()
		return errors.New("cluster: store users have not joined")
	}
	if !life.fenced {
		life.mu.Unlock()
		return nil
	}
	life.mu.Unlock()
	if imageSnapshotResume != nil {
		if err := imageSnapshotResume(n); err != nil {
			return err
		}
	}
	life.mu.Lock()
	defer life.mu.Unlock()
	// ctx-root: resumed backup lifecycle owned by the same live Node generation.
	life.backupContext, life.cancelBackups = context.WithCancel(context.Background())
	life.fenced = false
	// No close has entered: discard only preparation metadata, retaining the
	// same Node, pools and role observer. A stopped/unknown transport stays above.
	// origin: S6 internal review rv-final F1
	life.prepared, life.deadline = false, 0
	return nil
}

func closeNodeImageObject(life *nodeImageLifecycle, name string, phase *storage.ClosePhase, closeObject func() error) error {
	*phase = storage.CloseUnknown
	err := closeObject()
	if life.closeHook != nil {
		err = errors.Join(err, life.closeHook(name))
	}
	if err == nil {
		*phase = storage.CloseClosed
	}
	return err
}

func shutdownNode(n *Node) error {
	life := n.image
	life.shutdownMu.Lock()
	defer life.shutdownMu.Unlock()
	if life.shutdownDone {
		return life.shutdownErr
	}
	// Never abandon a started close behind a deadline goroutine. Cut deadlines
	// are checked before and after; once shutdown starts this owner stays liable.
	_ = fenceNodeImageWork(n, 0)
	var errs []error
	if transport, ok := n.transport.(*imageNetworkTransport); ok {
		errs = append(errs, transport.cancelIO())
	}
	errs = append(errs, joinImageSnapshotWork(n, 0))
	if life.prepared {
		errs = append(errs, ValidateImageRole(n, life.seal.Role))
	} else if n.raft.Load() != nil {
		// Normal embedding shutdown has no image stamp/registry, but retains
		// its last running role for callers inspecting the joined observer.
		if role, err := ReadImageRole(n); err == nil {
			life.seal.Role = role
		}
	}
	if r := n.raft.Load(); r != nil {
		errs = append(errs, closeNodeImageObject(life, "raft", &life.seal.Raft, func() error { return r.Shutdown().Error() }))
		stopped, err := ReadImageRole(n)
		if life.prepared {
			errs = append(errs, err)
			if err == nil {
				errs = append(errs, validateStoppedImageRole(life.seal.Role, stopped))
			}
		}
		if err == nil {
			life.seal.Role.EdgeSeq = stopped.EdgeSeq
		}
	}
	if life.roleStarted {
		close(life.roleStop)
		<-life.roleDone
	}
	life.mu.Lock()
	life.role.ObserverClosed = true
	life.seal.Role.ObserverClosed = true
	life.mu.Unlock()
	// Closing transport AFTER raft joins keeps already committed FSM work alive.
	// Accepted handlers and their snapshot readers are reaped by the wrapper.
	if c, ok := n.transport.(io.Closer); ok {
		errs = append(errs, closeNodeImageObject(life, "transport", &life.seal.Transport, c.Close))
	} else if n.transport != nil {
		life.seal.Transport = storage.CloseUnknown
		errs = append(errs, errors.New("cluster: transport has no close receipt"))
	}
	if n.store != nil {
		errs = append(errs, closeNodeImageObject(life, "bolt", &life.seal.Bolt, n.store.Close))
	}
	if life.prepared {
		// Raft/transport/Bolt no longer create SQL work. Cursor is final before the
		// WAL checkpoint and pool close, not a prediction made before Shutdown.
		index, err := readAppliedIndexDB(n.db)
		errs = append(errs, err)
		life.seal.Stamp.AppliedIndex = index
		var term string
		err = n.db.QueryRow("SELECT value FROM cluster_meta WHERE key = ?", metaKeyAppliedTerm).Scan(&term)
		if errors.Is(err, sql.ErrNoRows) {
			term = "0"
			err = nil
		}
		if err == nil {
			life.seal.Stamp.AppliedTerm, err = strconv.ParseUint(term, 10, 64)
		}
		errs = append(errs, err)
		life.seal.Stamp.RaftLastIndex = n.raft.Load().LastIndex()
		life.seal.Stamp.RaftConfigIndex = life.seal.Role.ConfigIndex
		life.seal.Stamp.RaftConfigDigest = life.seal.Role.VoterDigest
		err = n.db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&life.seal.CheckpointBusy, &life.seal.CheckpointLog, &life.seal.CheckpointDone)
		if err == nil && (life.seal.CheckpointBusy != 0 || life.seal.CheckpointLog != life.seal.CheckpointDone) {
			err = errors.New("cluster: WAL checkpoint incomplete")
		}
		errs = append(errs, err)
	}
	if n.ro != nil {
		errs = append(errs, closeNodeImageObject(life, "read_only", &life.seal.ReadOnly, n.ro.Close))
	}
	if n.db != nil {
		errs = append(errs, closeNodeImageObject(life, "writer", &life.seal.Writer, n.db.Close))
	}
	if life.prepared {
		life.seal.DriverWriters, life.seal.DriverReaders = n.cfg.ImageRegistry.Connections(life.seal.Stamp.Identity)
		if life.seal.DriverWriters != 0 || life.seal.DriverReaders != 0 {
			errs = append(errs, errors.New("cluster: actual driver handles still open after shutdown"))
		}
		if t, ok := n.transport.(*imageNetworkTransport); ok {
			life.seal.ActiveRPCs, life.seal.ActiveBackups = t.tracker.Counts()
		}
		if _, err := clusterio.Remaining(life.deadline); err != nil {
			errs = append(errs, err)
		}
	}
	life.shutdownErr = errors.Join(errs...)
	life.shutdownDone = true
	return life.shutdownErr
}

// ClosedImageStore is a closed receipt, never a provisional intent. Any unknown
// object, failed checkpoint, reader or driver handle forbids reopening.
// origin: hot-update S6 plan §3.2 / T10
func ClosedImageStore(n *Node) (storage.StoreSeal, error) {
	if n == nil || n.image == nil {
		return storage.StoreSeal{}, errors.New("cluster: missing image store owner")
	}
	life := n.image
	life.shutdownMu.Lock()
	defer life.shutdownMu.Unlock()
	if !life.prepared || !life.shutdownDone || life.shutdownErr != nil {
		return life.seal, errors.Join(errors.New("cluster: store_close_unknown"), life.shutdownErr)
	}
	for _, phase := range []storage.ClosePhase{life.seal.Raft, life.seal.Transport, life.seal.Bolt, life.seal.ReadOnly, life.seal.Writer} {
		if phase != storage.CloseClosed {
			return life.seal, errors.New("cluster: store_close_unknown")
		}
	}
	if !life.seal.Role.ObserverClosed || life.seal.DriverWriters != 0 || life.seal.DriverReaders != 0 || life.seal.ActiveRPCs != 0 || life.seal.ActiveBackups != 0 || life.seal.CheckpointBusy != 0 {
		return life.seal, errors.New("cluster: store_close_unknown")
	}
	if n.cfg.ImageRegistry == nil {
		return life.seal, errors.New("cluster: image-ineligible actual driver registry missing")
	}
	writers, readers := n.cfg.ImageRegistry.Connections(life.seal.Stamp.Identity)
	if writers != 0 || readers != 0 {
		return life.seal, errors.New("cluster: store_close_unknown")
	}
	return life.seal, nil
}
