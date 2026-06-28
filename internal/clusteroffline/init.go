package clusteroffline

import (
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// rejectBadText fail-closes on a text field that would be written to the DB / rendered into operator
// command lines: a NUL byte or invalid UTF-8 has no legitimate use and could truncate/forge output
// (External-review F13). It is the offline mirror of the online path's LitText discipline.
func rejectBadText(name, val string) error {
	if strings.ContainsRune(val, 0) {
		return fmt.Errorf("clusteroffline: %s contains a NUL byte (refused)", name)
	}
	if !utf8.ValidString(val) {
		return fmt.Errorf("clusteroffline: %s is not valid UTF-8 (refused)", name)
	}
	return nil
}

// init.go — the D9 `cluster init --from-existing` one-time migration (§11). OFFLINE
// surgery with the daemon STOPPED, directly on the on-disk tether.db + raft/. It mirrors
// the ForceSingle discipline (flock + live-daemon interlock + idempotent steps) and runs
// the steps in an ORDER where a kill-9 at any point re-runs cleanly: migrations are
// filename-idempotent, the seeds are ON CONFLICT DO NOTHING / empty-only, and the raft/
// bootstrap is the FINAL step (so the daemon's detection probe — raft/ present — flips
// true only once the DB is fully seeded). The DB is never rewritten except by forward
// migrations; the pristine pre-migration copy is preserved in tether.db.bak (skip-if-exists).

// InitFromExistingOptions carries the node identity + paths. Identity (SelfID == the raft
// ServerID == cluster_nodes.node_id == the deterministic nats server_name, §6.5 SSOT) and
// the route/tunnel addresses are operator-provided (the provisioning runbook prints them);
// cert_fp is DERIVED from the tunnel cert the daemon will actually present, so seed-time and
// runtime fingerprints are provably the same file (§15 RF3, d9-plan §6 cert_fp loop).
type InitFromExistingOptions struct {
	DataDir      string
	DBPath       string
	SecretsDir   string
	SelfID       string
	Name         string
	NodeIdentPub string
	RaftAddr     string
	NatsRoute    string
	TunnelAddr   string
	PublicHost   string
	Now          func() time.Time
	Logger       *slog.Logger
}

// ErrInitDaemonRunning is returned when a live (pre-cutover or cluster) daemon still holds
// the DB/raft store. The runbook's `systemctl stop tether-broker` must precede init.
var ErrInitDaemonRunning = errors.New("clusteroffline: a tether daemon is still running on this DataDir; `systemctl stop tether-broker` first")

// InitFromManifest is the B6 OPS#11 consume side: it reads the node IDENTITY from a manifest
// (emitted by `recover --emit-manifest` or carried in a backup bundle) instead of nine flags,
// then delegates to InitFromExisting. The DataDir / DBPath / SecretsDir are LOCAL (never trusted
// from a file). cert_fp is re-derived LIVE by InitFromExisting from the local secrets dir — the
// manifest's self_cert_fp is only CROSS-CHECKED (a rotated cert is a non-fatal WARN, since the
// recovered node may legitimately present a new cert; the live fp is authoritative so agents
// still pin correctly). Every identity-completeness guard / interlock in InitFromExisting applies
// unchanged.
func InitFromManifest(manifestPath, dataDir, dbPath, secretsDir string, now func() time.Time, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	m, err := ReadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.Mode != ManifestModeCluster {
		return fmt.Errorf("clusteroffline: manifest mode %q cannot seed a cluster identity", m.Mode)
	}
	// Audit MAJOR: `init --from-manifest` is IDENTITY-ONLY (it seeds the self row, NOT business
	// state). A BACKUP manifest's data lives in the bundle's state.db — pointing init at it would
	// silently discard every session/port/alert. Refuse loudly + point at `cluster recovery restore`.
	if m.Kind != ManifestKindRecover {
		return fmt.Errorf("clusteroffline: manifest kind %q is not a recover manifest; for a backup bundle use `cluster recovery restore <bundle>` (init --from-manifest only re-seeds identity)", m.Kind)
	}
	// Cross-check the manifest's self_cert_fp against the LIVE secrets dir (advisory; the live fp
	// is what InitFromExisting actually seeds).
	if m.SelfCertFP != "" {
		if liveFP, ferr := TunnelCertFingerprint(secretsDir); ferr == nil && liveFP != m.SelfCertFP {
			logger.Warn("clusteroffline: manifest self_cert_fp differs from the live tunnel cert (rotated?); seeding the LIVE fp",
				"manifest", m.SelfCertFP, "live", liveFP)
		}
	}
	return InitFromExisting(InitFromExistingOptions{
		DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		SelfID: m.SelfID, Name: m.Name, NodeIdentPub: m.NodeIdentPub,
		RaftAddr: m.RaftAddr, NatsRoute: m.NatsRoute, TunnelAddr: m.TunnelAddr, PublicHost: m.PublicHost,
		Now: now, Logger: logger,
	})
}

// InitFromExisting migrates a live single-broker DB into a single-voter cluster.
func InitFromExisting(opts InitFromExistingOptions) error {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.SelfID == "" || opts.RaftAddr == "" || opts.DBPath == "" || opts.DataDir == "" {
		return errors.New("clusteroffline: InitFromExisting requires SelfID, RaftAddr, DBPath, DataDir")
	}
	// External-review F13: the offline path must apply the SAME node_id + text validation the online
	// admission path does (proto.ValidateNID + NUL/non-UTF-8 rejection) — these fields are written to
	// the DB and rendered into operator command lines / nats.conf. Fail-closed here, pre-mutation.
	if err := proto.ValidateClusterNodeID(opts.SelfID); err != nil {
		return fmt.Errorf("clusteroffline: invalid SelfID: %w", err)
	}
	// External-review F4: the self VOTER row must carry a COMPLETE identity, else the
	// migrated cluster has a self row that cannot serve as an expose home, cannot be rendered
	// into a correct NATS topology, and only fails later at startup / data-plane time. Reject
	// an empty (or structurally invalid) identity field BEFORE the lock/backup/migration.
	for _, f := range []struct{ name, val string }{
		{"Name", opts.Name}, {"NodeIdentPub", opts.NodeIdentPub}, {"NatsRoute", opts.NatsRoute},
		{"TunnelAddr", opts.TunnelAddr}, {"PublicHost", opts.PublicHost},
	} {
		if strings.TrimSpace(f.val) == "" {
			return fmt.Errorf("clusteroffline: InitFromExisting requires a non-empty %s "+
				"(the self VOTER row must be a usable expose home + renderable NATS peer)", f.name)
		}
		if err := rejectBadText(f.name, f.val); err != nil {
			return err
		}
	}
	// Structural checks on the fields that are unambiguously host:port (raft + tunnel addrs).
	// NatsRoute is left to non-empty only — it is carried as host:port in cluster_nodes and
	// rendered into the nats:// route URL by the takeover step, so both forms reach it.
	if _, _, err := net.SplitHostPort(opts.RaftAddr); err != nil {
		return fmt.Errorf("clusteroffline: RaftAddr %q must be host:port: %w", opts.RaftAddr, err)
	}
	if _, _, err := net.SplitHostPort(opts.TunnelAddr); err != nil {
		return fmt.Errorf("clusteroffline: TunnelAddr %q must be host:port: %w", opts.TunnelAddr, err)
	}

	// (1) flock — bar two concurrent inits.
	release, err := acquireFlock(filepath.Join(opts.DataDir, lockFileName))
	if err != nil {
		return err
	}
	defer release()

	// (2) live-daemon interlock — TWO probes (d9-plan OQ-2 "BOTH"):
	//  (2a) bolt probe: catches a CLUSTER daemon (it holds raft.db). Only meaningful once
	//       raft/raft.db exists (a prior init / a running cluster daemon); on a never-
	//       clustered box raft.db is absent so no daemon can hold it — skip.
	if _, statErr := os.Stat(filepath.Join(opts.DataDir, "raft", "raft.db")); statErr == nil {
		if locked, err := cluster.RaftStoreLockedByDaemon(opts.DataDir); err != nil {
			return err
		} else if locked {
			return ErrInitDaemonRunning
		}
	}
	//  (2b) SQLite write-lock probe (round-1 BLOCKER: the plan-mandated defense the impl had
	//       dropped to runbook-only): a pre-cutover (v1) daemon NEVER opens raft.db, so the
	//       bolt probe cannot see it, yet it IS writing tether.db. A no-migration BEGIN
	//       IMMEDIATE with busy_timeout=0 detects that writer (reliable for a DELETE-mode v1
	//       DB) and refuses BEFORE OpenWAL runs the irreversible migration against a file a
	//       live daemon has open. Runs only if the DB exists (a fresh box has nothing to
	//       lock). The runbook's `systemctl stop` stays the primary guard (a WAL idle daemon
	//       can slip past), this is defense-in-depth.
	if _, statErr := os.Stat(opts.DBPath); statErr == nil {
		if busy, err := storage.ProbeWriterLock(opts.DBPath); err != nil {
			return fmt.Errorf("clusteroffline: write-lock probe: %w", err)
		} else if busy {
			return ErrInitDaemonRunning
		}
	}

	// (3) secrets precondition (FATAL): the stable tunnel cert MUST exist before we seed
	// cert_fp from it, else the daemon would present a cert whose fp mismatches the seeded
	// row and EVERY agent dial is rejected (ErrHomePinsRequired). Also require the cluster
	// CA + this node's route leaf (cluster.NewProduction loads them at daemon start).
	certFP, err := TunnelCertFingerprint(opts.SecretsDir)
	if err != nil {
		return fmt.Errorf("clusteroffline: secrets precondition: %w", err)
	}
	// External-review Q1: run the SAME SecretsPreflight the broker runs at startup, BEFORE the
	// .bak/migration — so a secrets dir that would make cluster startup FATAL (missing
	// node-ident.nk, a world-readable private key, …) is rejected up front instead of after
	// the irreversible migration. FDE-absent stays a non-fatal advisory.
	if adv, fatal := SecretsPreflight(opts.SecretsDir); fatal != nil {
		return fmt.Errorf("clusteroffline: secrets preflight: %w", fatal)
	} else {
		for _, a := range adv {
			opts.Logger.Warn("clusteroffline: secrets advisory", "detail", a)
		}
	}

	// (4) .bak the pristine pre-migration DB (skip-if-exists: a prior half-run's .bak is the
	// real pristine copy — never overwrite it).
	if err := backupOnce(opts.DBPath); err != nil {
		return err
	}

	// (5) forward migrations 0008-0013 + (6) seed, in the opened WAL DB.
	db, err := storage.OpenWAL("file:" + opts.DBPath) // OpenWAL runs ApplyMigrations
	if err != nil {
		return fmt.Errorf("clusteroffline: open+migrate DB: %w", err)
	}
	if err := seedClusterState(db, opts, certFP); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("clusteroffline: close after seed: %w", err)
	}

	// (7) bootstrap raft/ — config@1 (idempotent: ErrAlreadyBootstrapped on re-run).
	if err := cluster.BootstrapSingleNode(opts.DataDir, opts.SelfID, opts.RaftAddr, opts.Logger); err != nil &&
		!errors.Is(err, cluster.ErrAlreadyBootstrapped) {
		return err
	}
	// (8) GROW-READY snapshot — the KEYSTONE of the grow-onto-migrated-broker fix. seedClusterState
	// direct-seeds migrated rows into SQLite that NO log entry created, so the FSM is not
	// reconstructable from (snapshot + log-from-1). Without a snapshot, a future joiner replays this
	// node's log from index 1 onto its own un-seeded DB and FK-fail-stops (or, silently worse, replays
	// an FK-safe log onto empty tables and is promoted as a hollow voter). GrowReadySnapshot writes a
	// full FSM snapshot (the online SQLite backup carrying every seeded row) and compacts the log past
	// config@1 so FirstIndex>1 — then a joiner's nextIndex decays below FirstIndex, raft ships
	// InstallSnapshot (not the log), and fsm.Restore loads the full DB. No replay, no FK crash, no fork.
	// The freshly-init'd log is just config@1 (zero unpublished cluster audit), so no D5 audit-window
	// guard is needed here (the publisher floors at LogFirstIndex). Idempotent on re-run.
	if err := cluster.GrowReadySnapshot(opts.DataDir, opts.DBPath, opts.SelfID, opts.RaftAddr, opts.Logger); err != nil {
		return fmt.Errorf("clusteroffline: grow-ready snapshot: %w", err)
	}
	opts.Logger.Warn("clusteroffline: cluster init --from-existing complete; this node is now a single-voter cluster (grow-ready snapshot taken)",
		"self", opts.SelfID, "data_dir", opts.DataDir)
	return nil
}

// seedClusterState writes the migration marker + the self VOTER row + the home backfill in
// ONE txn (idempotent). applied_index=0 is the index-0-snapshot marker (the daemon-start
// half-state cross-check); self_node_id lets the daemon set its raft LocalID before opening
// the DB. A re-run with a CONFLICTING self row (same node_id, different name/addr) is a loud
// refusal, not a silent INSERT OR IGNORE that masks an operator typo. nats_server_id is the
// SelfID itself (the §6.5 SSOT: server_name == node_id == nats_server_id).
func seedClusterState(db *sql.DB, opts InitFromExistingOptions, certFP string) error {
	now := opts.Now().UTC().Format(time.RFC3339Nano)
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("clusteroffline: begin seed: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, kv := range []struct{ k, v string }{{"applied_index", "0"}, {"applied_term", "0"}} {
		if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING`, kv.k, kv.v); err != nil {
			return fmt.Errorf("clusteroffline: seed %s: %w", kv.k, err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, opts.SelfID); err != nil {
		return fmt.Errorf("clusteroffline: seed self_node_id: %w", err)
	}

	// Re-run guard (round-1 MAJOR: widen beyond name+raft_addr): a re-run with a
	// CONFLICTING self row on ANY identity column is a loud refusal, not a silent overwrite
	// that would mask an operator typo (e.g. a changed cert_fp would silently un-pin agents).
	var exName, exRaft, exIdent, exServer, exTunnel, exHost, exFP string
	switch err := tx.QueryRow(`SELECT name, raft_addr, node_ident_pub, nats_server_id, tunnel_addr, public_host, cert_fp
		FROM cluster_nodes WHERE node_id=?`, opts.SelfID).Scan(
		&exName, &exRaft, &exIdent, &exServer, &exTunnel, &exHost, &exFP); err {
	case nil:
		mismatch := exName != opts.Name || exRaft != opts.RaftAddr || exIdent != opts.NodeIdentPub ||
			exServer != opts.SelfID || exTunnel != opts.TunnelAddr || exHost != opts.PublicHost || exFP != certFP
		if mismatch {
			return fmt.Errorf("clusteroffline: self row already seeded with a DIFFERENT identity "+
				"(name/raft/ident/server/tunnel/host/cert_fp = %q/%q/%q/%q/%q/%q/%q vs %q/%q/%q/%q/%q/%q/%q) "+
				"— refusing to overwrite (re-run with the SAME identity or wipe + re-init)",
				exName, exRaft, exIdent, exServer, exTunnel, exHost, exFP,
				opts.Name, opts.RaftAddr, opts.NodeIdentPub, opts.SelfID, opts.TunnelAddr, opts.PublicHost, certFP)
		}
	case sql.ErrNoRows:
		if _, err := tx.Exec(`INSERT INTO cluster_nodes
			(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
			VALUES(?,?,?,?,?,?,?,?,?,'VOTER',?)`,
			opts.SelfID, opts.Name, opts.NodeIdentPub, opts.SelfID, opts.RaftAddr,
			opts.NatsRoute, opts.TunnelAddr, opts.PublicHost, certFP, now); err != nil {
			return fmt.Errorf("clusteroffline: seed self cluster_nodes row: %w", err)
		}
	default:
		return fmt.Errorf("clusteroffline: probe self row: %w", err)
	}

	// home_broker backfill: stamp self on every ALLOCATED, un-homed port (empty-only, idempotent).
	if _, err := tx.Exec(`UPDATE port_allocations SET home_broker=? WHERE state='ALLOCATED' AND home_broker=''`, opts.SelfID); err != nil {
		return fmt.Errorf("clusteroffline: backfill home_broker: %w", err)
	}
	return tx.Commit()
}

// TunnelCertFingerprint loads the stable tunnel cert from the secrets dir and returns its
// canonical fingerprint (== cluster_nodes.cert_fp, §15 RF3). Exported for B3 sign-join, which
// prints the real --cert-fp into the pasteable re-run line.
func TunnelCertFingerprint(secretsDir string) (string, error) {
	certPEM, err := os.ReadFile(filepath.Join(secretsDir, "tunnel-cert.pem"))
	if err != nil {
		return "", fmt.Errorf("read tunnel cert: %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(secretsDir, "tunnel-key.pem"))
	if err != nil {
		return "", fmt.Errorf("read tunnel key: %w", err)
	}
	cert, err := tunnel.LoadServerCert(string(certPEM), string(keyPEM))
	if err != nil {
		return "", fmt.Errorf("load stable tunnel cert: %w", err)
	}
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return "", errors.New("tunnel cert has no certificate")
		}
		leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return "", fmt.Errorf("parse tunnel leaf: %w", err)
		}
	}
	return tunnel.CertFingerprint(leaf), nil
}

// backupToUnique copies the live DB to a NON-colliding `<db>.pre-restore[-N].bak` (External-review
// F4). Unlike backupOnce (which skips when `.bak` exists), this ALWAYS preserves the current state
// under a fresh name and returns the actual path — so `cluster restore` can report it truthfully and
// a second restore never silently discards the prior live DB.
func backupToUnique(dbPath string) (string, error) {
	if err := checkpointWALForBackup(dbPath); err != nil {
		return "", err
	}
	src, err := os.Open(dbPath)
	if err != nil {
		return "", fmt.Errorf("clusteroffline: open DB for backup: %w", err)
	}
	defer func() { _ = src.Close() }()
	for i := 0; ; i++ {
		bak := dbPath + ".pre-restore.bak"
		if i > 0 {
			bak = fmt.Sprintf("%s.pre-restore.%d.bak", dbPath, i)
		}
		dst, err := os.OpenFile(bak, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue // pick the next free name (never clobber a prior preservation)
			}
			return "", fmt.Errorf("clusteroffline: create pre-restore backup: %w", err)
		}
		if _, err := io.Copy(dst, src); err != nil {
			_ = dst.Close()
			return "", fmt.Errorf("clusteroffline: copy pre-restore backup: %w", err)
		}
		if err := dst.Sync(); err != nil {
			_ = dst.Close()
			return "", fmt.Errorf("clusteroffline: fsync pre-restore backup: %w", err)
		}
		if err := dst.Close(); err != nil {
			return "", err
		}
		return bak, nil
	}
}

// backupOnce copies dbPath to dbPath+".bak" iff the .bak does not already exist (a prior
// half-run's .bak is the pristine pre-migration copy). fsync'd.
func backupOnce(dbPath string) error {
	bak := dbPath + ".bak"
	if _, err := os.Stat(bak); err == nil {
		return nil // pristine pre-migration copy already preserved
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clusteroffline: stat .bak: %w", err)
	}
	if err := checkpointWALForBackup(dbPath); err != nil {
		return err
	}
	src, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("clusteroffline: open DB for backup: %w", err)
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(bak, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("clusteroffline: create .bak: %w", err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return fmt.Errorf("clusteroffline: copy .bak: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return fmt.Errorf("clusteroffline: fsync .bak: %w", err)
	}
	return dst.Close()
}

func checkpointWALForBackup(dbPath string) error {
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("clusteroffline: open DB for checkpoint: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("clusteroffline: checkpoint WAL before backup: %w", err)
	}
	return nil
}
