package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"time"

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
}

// NewProduction builds the mTLS transport and the cluster Node for the D9 daemon. The
// returned Node owns the transport (Node.Shutdown closes it). It uses the multinode
// raft timeouts (§8.4 / d3-plan R6) — the production cutover never uses the D1/D2 fast
// N=1 values. On any error nothing is leaked (NewMTLSTransport's listener is closed by
// New's failure path once injected; a pre-injection error closes nothing raft-side).
func NewProduction(cfg ProductionConfig) (*Node, error) {
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

	n, err := New(Config{
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
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}
