package broker

// cutover.go — the D9 production cutover surface (build-and-prove ENDS here). It is
// the single place that decides single-mode vs cluster-mode and, in cluster mode,
// constructs cluster.Node and attaches every D2-D8 seam. A broker with NO cluster
// config (broker.cluster.data_dir unset) takes the single-mode path and stays
// byte-equivalent to a pre-D9 broker (the D2 N=1 functional-equivalence floor); the
// guard test TestD9ClusterModeOffByteEquivalence is the regression floor and the six
// per-phase TestDxProductionWiresNoCluster guards are deleted in favour of it (the
// L-2 import guards stay verbatim). See docs/reviews/d9-plan.md §2 steps 1-7.

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

// §15 secrets-dir filenames the cluster-mode daemon loads (the provisioning runbook +
// `cluster init` write them; broker-ops.md §2.3 documents them). The route leaf is this
// node's cluster-CA-signed raft/route mTLS cert; the CA is the shared trust anchor.
const (
	secretClusterCA = "cluster-ca.pem"
	secretRouteCert = "route-cert.pem"
	secretRouteKey  = "route-key.pem"
	// secretTunnelCert/Key are this node's STABLE tunnel-server cert (§15 RF3): bound
	// to node identity, generated at provision time, unchanged across restarts so the
	// advertised cert_fp stays stable and agents' cert pins keep matching.
	secretTunnelCert = "tunnel-cert.pem"
	secretTunnelKey  = "tunnel-key.pem"
)

// clusterModeEnabled decides single vs cluster mode from the operator's INTENT
// (cfg.ClusterDataDir set) cross-checked against the on-disk raft/ state. It does NOT
// touch the DB — the DB-side migration marker is cross-checked post-open by
// assertClusterDBConsistent once DB ownership is decided. Every inconsistent
// combination is a FATAL error so a misconfiguration can never (a) silently bootstrap
// an empty cluster onto a live DB (the cluster.Node BootstrapCluster-on-
// !HasExistingState hazard at node.go) nor (b) silently downgrade a migrated DB to
// single mode. Truth table (d9-plan §5 OQ-1 / §1 C-* / §2 step 1):
//
//	data_dir unset                      ⇒ single mode (every pre-D9 deployment)
//	data_dir set + raft/ present        ⇒ cluster mode (marker cross-checked post-open)
//	data_dir set + raft/ absent         ⇒ FATAL ("run cluster init first"; no auto-bootstrap)
func clusterModeEnabled(cfg Config) (bool, error) {
	if cfg.ClusterDataDir == "" {
		return false, nil
	}
	raftExists, err := raftStateOnDisk(cfg.ClusterDataDir)
	if err != nil {
		return false, fmt.Errorf("broker: probe raft state under %q: %w", cfg.ClusterDataDir, err)
	}
	if !raftExists {
		return false, fmt.Errorf("broker: broker.cluster.data_dir=%q is set but no raft state exists — "+
			"run `tether cluster init [--from-existing]` first (the daemon never auto-bootstraps a cluster "+
			"onto a live DB)", cfg.ClusterDataDir)
	}
	return true, nil
}

// DetectClusterMode reports whether a broker with this cluster data dir will run in
// cluster mode — exported so cmd/tether/serve.go can decide BEFORE it opens the DB whether
// to call storage.Open itself (single mode) or leave the WAL DB entirely to the FSM
// (cluster mode: d9-plan C-1, the cluster.Node is the SOLE writable pool — serve.go must
// NOT open a second one). Same probe as the internal clusterModeEnabled.
func DetectClusterMode(clusterDataDir string) (bool, error) {
	return clusterModeEnabled(Config{ClusterDataDir: clusterDataDir})
}

// raftStateOnDisk reports whether DataDir/raft/raft.db exists AND holds recoverable
// raft state, WITHOUT the side effect of creating raft.db (raftboltdb.New, which
// cluster.RaftStateExists uses, would create the file): it stats the bolt file first
// and only probes HasExistingState when the file is present.
func raftStateOnDisk(dataDir string) (bool, error) {
	boltPath := filepath.Join(dataDir, "raft", "raft.db")
	if _, err := os.Stat(boltPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return cluster.RaftStateExists(dataDir)
}

// assertClusterDBConsistent cross-checks the DB-side migration marker against the
// decided mode, AFTER DB ownership is established. The marker = cluster_meta has the
// seeded 'applied_index' key AND cluster_nodes has >=1 (self) row — both written only
// by `cluster init [--from-existing]`, never by a plain v2-binary single-mode start
// (storage.Open's ApplyMigrations creates the tables EMPTY). Cluster mode REQUIRES the
// marker (else raft/ exists but the DB was never seeded ⇒ half-migrated); single mode
// FORBIDS it (else a migrated DB was started with broker.cluster.data_dir unset ⇒ a
// silent downgrade). This is the cross-check arm of d9-plan §2 step 1.
func assertClusterDBConsistent(db *sql.DB, clusterMode bool) error {
	// Audit DI-MAJOR-2: a restore that was interrupted (SIGKILL between the DB install and the raft
	// bootstrap) leaves restore_in_progress set in the installed DB. Refuse to start (fail-closed) —
	// the restored DB + the OLD raft log would otherwise resurrect pruned peers. Re-run `cluster
	// restore` (idempotent) to complete it.
	var rip string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key='restore_in_progress'`).Scan(&rip); err == nil && rip != "" {
		return errors.New("broker: a `cluster recovery restore` was interrupted (restore_in_progress is set) — " +
			"re-run `tether cluster recovery restore <bundle> --confirm-node-id <id>` to complete it before starting the daemon")
	}
	seeded, err := clusterDBSeeded(db)
	if err != nil {
		return err
	}
	switch {
	case clusterMode && !seeded:
		return errors.New("broker: raft state present but the DB is not seeded " +
			"(no cluster_meta.applied_index / cluster_nodes self row) — half-migrated; " +
			"re-run `tether cluster init --from-existing`")
	case !clusterMode && seeded:
		return errors.New("broker: this DB was migrated to cluster mode " +
			"(cluster_meta.applied_index + cluster_nodes seeded) but broker.cluster.data_dir is unset — " +
			"set it (refusing to silently downgrade a cluster DB to single mode)")
	}
	return nil
}

// readSelfNodeID reads the seeded self node_id (== raft ServerID == cluster_nodes.node_id)
// from cluster_meta. `cluster init [--from-existing]` writes it; a cluster-mode daemon
// needs it to set the raft LocalID BEFORE cluster.Node opens the DB, so this does a
// brief read-only open. A missing key means the DB was not initialized for cluster mode.
func readSelfNodeID(dbPath string) (string, error) {
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return "", fmt.Errorf("broker: open DB to read self node_id: %w", err)
	}
	defer func() { _ = ro.Close() }()
	var id string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key = 'self_node_id'`).Scan(&id); err != nil {
		return "", fmt.Errorf("broker: read cluster_meta self_node_id (run `tether cluster init`): %w", err)
	}
	if id == "" {
		return "", errors.New("broker: cluster_meta self_node_id is empty")
	}
	return id, nil
}

// assertNoInterruptedRestore is the External-review F1 fail-closed PREFLIGHT: it checks the
// restore_in_progress marker on a READ-ONLY handle, BEFORE any raft store / FSM / transport is
// constructed. An interrupted `cluster restore` (SIGKILL between DB install and raft bootstrap)
// leaves the restored DB next to the OLD raft log — and `cluster.New` (NoSnapshotRestoreOnStart)
// would replay that log onto the restored DB, resurrecting pruned peers, BEFORE the post-construct
// assertClusterDBConsistent could refuse. So this must run first, touching no raft state.
func assertNoInterruptedRestore(dbPath string) error {
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return fmt.Errorf("broker: open DB for restore preflight: %w", err)
	}
	defer func() { _ = ro.Close() }()
	var rip string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='restore_in_progress'`).Scan(&rip); err == nil && rip != "" {
		return errors.New("broker: a `cluster recovery restore` was interrupted (restore_in_progress is set) — " +
			"re-run `tether cluster recovery restore <bundle> --confirm-node-id <id>` to complete it before starting the daemon")
	}
	return nil
}

// buildClusterRuntime constructs the cluster.Node (mTLS transport + raft + the merged
// WAL DB) from the §15 secrets dir. The forwarder, seam attaches, and leader-gated
// loops are wired later in Run (after the NATS connect + JS probe), so this returns a
// runtime carrying only the node.
func (b *Broker) buildClusterRuntime() (*clusterRuntime, error) {
	// External-review F1: FAIL CLOSED on an interrupted restore BEFORE constructing raft/FSM —
	// cluster.New would otherwise replay the old raft log onto the restored DB first.
	if err := assertNoInterruptedRestore(b.cfg.DBPath); err != nil {
		return nil, err
	}
	selfID, err := readSelfNodeID(b.cfg.DBPath)
	if err != nil {
		return nil, err
	}
	sec := b.cfg.ClusterSecretsDir
	if sec == "" {
		return nil, errors.New("broker: cluster mode requires broker.cluster.secrets_dir")
	}
	// D9 round-1 MAJOR: run the §15 secrets preflight at STARTUP (not only the operator's
	// `cluster doctor`) — a missing/unreadable/world-readable key is FATAL before the node
	// loads them; FDE-absent is an advisory (logged, non-fatal).
	adv, fatal := clusteroffline.SecretsPreflightWithOptions(sec, clusteroffline.SecretsPreflightOptions{
		RequireAuthSeeds: b.cfg.AuthCallout == nil,
	})
	for _, a := range adv {
		b.cfg.Logger.Warn("broker: secrets preflight advisory", "detail", a)
	}
	if fatal != nil {
		return nil, fmt.Errorf("broker: secrets preflight: %w", fatal)
	}
	node, err := cluster.NewProduction(cluster.ProductionConfig{
		Logger:       b.cfg.Logger, // batch-A review M1: without this the raft log bridge is a no-op
		LocalID:      selfID,
		DataDir:      b.cfg.ClusterDataDir,
		DBPath:       b.cfg.DBPath,
		RaftBind:     b.cfg.ClusterRaftAddr,
		CACertPath:   filepath.Join(sec, secretClusterCA),
		LeafCertPath: filepath.Join(sec, secretRouteCert),
		LeafKeyPath:  filepath.Join(sec, secretRouteKey),
	})
	if err != nil {
		return nil, err
	}
	return &clusterRuntime{node: node, fsArm: newForceSingleArm()}, nil
}

// ClusterStateForTest reports whether the broker constructed its cluster runtime and
// whether it is currently the raft leader. TEST-ONLY (like TunnelTokenLookupForTest) —
// the d9_integration harness asserts the cutover actually stood up cluster mode + the
// single voter took leadership, without a NATS round-trip.
func (b *Broker) ClusterStateForTest() (clustered, leader bool) {
	if !b.clusterMode || b.cl == nil {
		return false, false
	}
	return true, b.cl.node.IsLeader()
}

// AppliedIndexForTest exposes the node's command-domain AppliedIndex (the d9_integration
// 2-broker drill uses it as the join catch-up probe). TEST-ONLY. 0 if not clustered.
func (b *Broker) AppliedIndexForTest() uint64 {
	if b.cl == nil {
		return 0
	}
	ai, _ := b.cl.node.AppliedIndex()
	return ai
}

// RODBForTest exposes the cluster node's read-only DB handle so the d9_integration grow drill can assert
// a joiner gained the leader's snapshot-only rows (row parity). TEST-ONLY. nil if not clustered.
func (b *Broker) RODBForTest() *sql.DB {
	if b.cl == nil || b.cl.node == nil {
		return nil
	}
	return b.cl.node.RODB()
}

// clusterDBSeeded reports whether the DB carries the `cluster init` marker (the seeded
// applied_index key AND >=1 cluster_nodes row). Both cluster_meta and cluster_nodes
// always exist on a v2 binary (storage.Open runs migrations 0008-0013), so emptiness —
// not table-absence — is what distinguishes "never cluster-init'd" from "migrated".
func clusterDBSeeded(db *sql.DB) (bool, error) {
	var hasIdx int
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM cluster_meta WHERE key = 'applied_index')`).Scan(&hasIdx); err != nil {
		return false, fmt.Errorf("broker: probe cluster_meta marker: %w", err)
	}
	var nodeCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes`).Scan(&nodeCount); err != nil {
		return false, fmt.Errorf("broker: probe cluster_nodes marker: %w", err)
	}
	return hasIdx == 1 && nodeCount >= 1, nil
}
