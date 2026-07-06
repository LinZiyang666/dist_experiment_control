package clusteroffline

// restore.go — the B6 `cluster restore <bundle>` path (OPS#3), OFFLINE-only (daemon STOPPED).
// It installs a backup bundle's state.db over the on-disk DB and re-bootstraps a fresh
// single-voter raft, so the restored node is a clean N=1 origin the operator re-grows with
// `cluster add`. It is the most corruption-prone B6 surgery, so the discipline is strict:
//
//   - 4-layer provenance gate (all FATAL, BEFORE any disk mutation): the LIVE tunnel-cert fp must
//     equal both the manifest's self_cert_fp AND the bundle state.db's self-row cert_fp;
//     --confirm-node-id must equal manifest.self_id; the bundle's self row identity must agree with
//     the manifest (a torn/edited manifest is refused). The tunnel-cert anchor proves the restorer
//     OWNS this node's tunnel private key (security-pragmatic v1) — it is not a cryptographic proof
//     of bundle provenance, but it does stop a restore onto a DIFFERENT node's secrets dir.
//   - verify-on-STAGING-before-swap: the live DB is touched only at the .bak (read) and the
//     final install (write), and ONLY after every staging verify has passed.
//   - applied_index reset to 0 — THE load-bearing correctness fix. BootstrapSingleNode lays a
//     fresh raft log at index 1; the FSM Apply skips `l.Index <= applied`. A real-cluster
//     bundle carries applied_index = N ≫ 0, so without the reset the FSM would silently
//     swallow EVERY post-restore write until the new log climbed past N. Reset to 0 makes the
//     restored DB the index-0 snapshot (the BootstrapSingleNode contract).
//   - prune non-self roster rows: a restored single-voter cluster whose roster still lists the
//     old peers as VOTER renders permanently INCONSISTENT/DEGRADED (clusterstatus §8.1), so
//     restore leaves a clean {self}-only roster. The original membership is preserved in the
//     manifest's roster[] for forensics.

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// RestoreOptions configures an offline restore.
type RestoreOptions struct {
	BundleDir        string // the { state.db, manifest.json } bundle directory
	DataDir          string // target raft data dir (raft/ is wiped + re-bootstrapped)
	DBPath           string // target tether.db (the install destination)
	SecretsDir       string // the LIVE secrets dir — the un-forgeable provenance anchor
	ConfirmNodeID    string // operator-named target node id; must equal manifest.self_id
	RaftAddrOverride string // External-review Q1: override the bundle's raft_addr for a fresh-host restore ("" = same host)
	Now              func() time.Time
	Logger           *slog.Logger
}

// RestoreResult summarizes a completed restore.
type RestoreResult struct {
	SelfID           string
	BundleAppliedIdx uint64 // the bundle's committed cursor (reset to 0 in the installed DB)
	PrunedPeers      int64  // non-self roster rows removed for a clean single-voter origin
	PreRestoreBackup string // External-review F4: the unique path the prior live DB was preserved at ("" if none)
}

// RestoreFromBackup installs a cluster backup bundle as a fresh single-voter cluster.
func RestoreFromBackup(opts RestoreOptions) (*RestoreResult, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.BundleDir == "" || opts.DataDir == "" || opts.DBPath == "" || opts.SecretsDir == "" || opts.ConfirmNodeID == "" {
		return nil, fmt.Errorf("clusteroffline: RestoreFromBackup requires BundleDir, DataDir, DBPath, SecretsDir, ConfirmNodeID")
	}

	// (1) flock + (2) live-daemon interlock.
	warnRootDataDirOwner(opts.DataDir, opts.Logger) // #6: nudge if run as root against a tether-owned data dir
	release, err := acquireFlock(filepath.Join(opts.DataDir, lockFileName))
	if err != nil {
		return nil, err
	}
	defer release()
	if err := checkNoLiveDaemon(opts.DataDir, opts.DBPath); err != nil {
		return nil, err
	}

	// (3) read + gate the manifest. Single-broker bundles are NOT restorable as a cluster.
	manifestPath := filepath.Join(opts.BundleDir, "manifest.json")
	statePath := filepath.Join(opts.BundleDir, "state.db")
	m, err := ReadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if m.Mode != ManifestModeCluster {
		return nil, fmt.Errorf("clusteroffline: bundle mode %q is not a cluster bundle; `cluster recovery restore` restores cluster bundles only", m.Mode)
	}
	// Stage-C M-d: a recover-divergent manifest is identity-only (no state.db) and is consumed by
	// `init --from-manifest`, not restore — refuse it loudly here rather than relying on the
	// state.db-missing check below.
	if m.Kind != ManifestKindBackup {
		return nil, fmt.Errorf("clusteroffline: manifest kind %q is not a backup bundle; use `cluster init --from-manifest` for a recover manifest", m.Kind)
	}
	if _, statErr := os.Stat(statePath); statErr != nil {
		return nil, fmt.Errorf("clusteroffline: bundle state.db missing: %w", statErr)
	}

	// (4) provenance gate — ALL FATAL, before any disk mutation.
	if err := restoreProvenanceGate(opts, m, statePath); err != nil {
		return nil, err
	}

	// (5) stage: byte-copy the bundle state.db onto the target filesystem (same dir as DBPath
	// so the final install is a rename). A leftover staging file from a prior half-run is
	// removed and re-copied (kill-9 idempotent).
	stage := filepath.Join(filepath.Dir(opts.DBPath), "restore-staging.db")
	// Remove a prior half-run's staging file AND its WAL sidecars before copying: a stale
	// restore-staging.db-wal from a kill-9'd normalize would otherwise be applied over the FRESH
	// copy on the next OpenWAL, producing a corrupt-but-integrity_check-passing DB that the
	// re-run would install over the live DB (Stage-C BLOCKER). Clean .db last (after sidecars).
	for _, suffix := range []string{"-wal", "-shm", ""} {
		_ = os.Remove(stage + suffix)
	}
	if err := copyFileSync(statePath, stage); err != nil {
		return nil, fmt.Errorf("clusteroffline: stage bundle: %w", err)
	}
	defer func() {
		for _, suffix := range []string{"-wal", "-shm", ""} {
			_ = os.Remove(stage + suffix)
		}
	}()

	// (6) verify pre-migration → forward-migrate → verify post-migration.
	if err := cluster.VerifyIntegrity(stage); err != nil {
		return nil, fmt.Errorf("clusteroffline: staging verify (pre-migration): %w", err)
	}
	if mdb, oerr := storage.Open("file:" + stage); oerr != nil {
		return nil, fmt.Errorf("clusteroffline: forward-migrate staging: %w", oerr)
	} else {
		_ = mdb.Close()
	}
	// Test-only seam (Stage-C BLOCKER-2): lets a test corrupt the migrated staging so the
	// post-migration verify is actually exercised — proving the live DB is NEVER installed when a
	// migration-introduced corruption is present (the verify-before-swap ordering).
	if restoreAfterMigrateHook != nil {
		restoreAfterMigrateHook(stage)
	}
	if err := cluster.VerifyIntegrity(stage); err != nil {
		return nil, fmt.Errorf("clusteroffline: staging verify (post-migration): %w", err)
	}

	// External-review Q1: restore re-bootstraps raft at the bundle's RaftAddr, which is right for a
	// same-host restore. For a FRESH-HOST restore (the box's IP changed), --raft-addr overrides it;
	// the override is also re-stamped into the self roster row so the address is consistent everywhere.
	effectiveRaftAddr := m.RaftAddr
	if opts.RaftAddrOverride != "" {
		if _, _, err := net.SplitHostPort(opts.RaftAddrOverride); err != nil {
			return nil, fmt.Errorf("clusteroffline: --raft-addr %q must be host:port: %w", opts.RaftAddrOverride, err)
		}
		effectiveRaftAddr = opts.RaftAddrOverride
	}

	// (7) normalize staging: reset applied_index/term to 0 + prune non-self roster (+ re-stamp the
	// self raft_addr when overridden).
	pruned, err := normalizeRestoreStaging(stage, opts.ConfirmNodeID, effectiveRaftAddr)
	if err != nil {
		return nil, err
	}
	if err := cluster.VerifyIntegrity(stage); err != nil {
		return nil, fmt.Errorf("clusteroffline: staging verify (post-normalize): %w", err)
	}

	// (8) .bak the current live DB (preserve the pre-restore copy; skip if absent — a fresh DR box
	// has no DB yet). External-review F4: use a UNIQUE name (not the shared `.bak` that backupOnce
	// skips when present) so a second restore — or a box that already ran init — truly preserves
	// the current state and the CLI's "preserved at" message is honest.
	var preBak string
	if _, statErr := os.Stat(opts.DBPath); statErr == nil {
		preBak, err = backupToUnique(opts.DBPath)
		if err != nil {
			return nil, err
		}
	}

	// (9) install staging over the live DB — the LAST disk mutation before raft. Remove the
	// old DB + WAL sidecars, then rename staging into place, then fsync the dir.
	for _, suffix := range []string{"-wal", "-shm", ""} {
		if rerr := os.Remove(opts.DBPath + suffix); rerr != nil && !os.IsNotExist(rerr) {
			return nil, fmt.Errorf("clusteroffline: remove old DB%s: %w", suffix, rerr)
		}
	}
	if err := os.Rename(stage, opts.DBPath); err != nil {
		return nil, fmt.Errorf("clusteroffline: install restored DB: %w", err)
	}
	if err := fsyncDir(filepath.Dir(opts.DBPath)); err != nil {
		return nil, fmt.Errorf("clusteroffline: fsync DB dir after install: %w", err)
	}

	// (10) wipe raft/ then bootstrap a fresh single voter (the FINAL step).
	if err := os.RemoveAll(filepath.Join(opts.DataDir, "raft")); err != nil {
		return nil, fmt.Errorf("clusteroffline: wipe raft dir: %w", err)
	}
	if err := cluster.BootstrapSingleNode(opts.DataDir, m.SelfID, effectiveRaftAddr, opts.Logger); err != nil &&
		!isAlreadyBootstrapped(err) {
		return nil, fmt.Errorf("clusteroffline: bootstrap restored single voter: %w", err)
	}

	// Audit DI-MAJOR-2: clear restore_in_progress ONLY now (raft bootstrapped) — the DB is now a
	// complete, startable single-voter cluster. A crash before this point leaves the marker set →
	// the daemon refuses to start until restore is re-run.
	if err := clearRestoreInProgress(opts.DBPath); err != nil {
		return nil, fmt.Errorf("clusteroffline: clear restore_in_progress: %w", err)
	}

	opts.Logger.Warn("clusteroffline: restore complete; node is a single-voter cluster — re-grow with `cluster join prepare`/`cluster join approve`",
		"self", m.SelfID, "bundle_applied_index", m.AppliedIndex, "pruned_peers", pruned, "pre_restore_backup", preBak)
	return &RestoreResult{SelfID: m.SelfID, BundleAppliedIdx: m.AppliedIndex, PrunedPeers: pruned, PreRestoreBackup: preBak}, nil
}

// restoreProvenanceGate enforces the 4-layer identity gate (all FATAL, pre-mutation).
func restoreProvenanceGate(opts RestoreOptions, m *Manifest, statePath string) error {
	// External-review F13: the manifest's self identity is re-bootstrapped into a fresh raft config
	// and re-stamped into the DB — validate it with the SAME discipline as the online path before
	// trusting it (a malformed node_id would be rendered into operator command lines / nats.conf).
	if err := proto.ValidateClusterNodeID(m.SelfID); err != nil {
		return fmt.Errorf("clusteroffline: bundle manifest self_id invalid: %w", err)
	}
	for _, f := range []struct{ name, val string }{
		{"name", m.Name}, {"node_ident_pub", m.NodeIdentPub}, {"raft_addr", m.RaftAddr},
		{"nats_route", m.NatsRoute}, {"tunnel_addr", m.TunnelAddr}, {"public_host", m.PublicHost},
		{"nats_server_id", m.NatsServerID},
	} {
		if err := rejectBadText(f.name, f.val); err != nil {
			return fmt.Errorf("clusteroffline: bundle manifest %w", err)
		}
	}
	// (b) the operator must name the exact node this bundle is for.
	if opts.ConfirmNodeID != m.SelfID {
		return fmt.Errorf("clusteroffline: --confirm-node-id %q does not match the bundle's self_id %q", opts.ConfirmNodeID, m.SelfID)
	}
	// (a-primary) the un-forgeable anchor: the LIVE secrets dir's tunnel-cert fp.
	liveFP, err := TunnelCertFingerprint(opts.SecretsDir)
	if err != nil {
		return fmt.Errorf("clusteroffline: restore secrets precondition: %w", err)
	}
	if liveFP != m.SelfCertFP {
		return fmt.Errorf("clusteroffline: tunnel-cert fingerprint mismatch — the live secrets dir is not this bundle's node "+
			"(live %q vs manifest %q); refusing to adopt a foreign cluster", liveFP, m.SelfCertFP)
	}
	// (c) read the bundle's own self row and (e) cross-check it agrees with the manifest.
	bundleRO, err := storage.OpenReadOnly("file:" + statePath)
	if err != nil {
		return fmt.Errorf("clusteroffline: open bundle for provenance: %w", err)
	}
	defer func() { _ = bundleRO.Close() }()
	bundleSelf, err := ReadSelfIdentity(bundleRO, m.SelfID)
	if err != nil {
		return fmt.Errorf("clusteroffline: bundle self row: %w", err)
	}
	if liveFP != bundleSelf.SelfCertFP {
		return fmt.Errorf("clusteroffline: tunnel-cert fingerprint mismatch — live secrets vs bundle state.db self row "+
			"(live %q vs bundle %q); refusing", liveFP, bundleSelf.SelfCertFP)
	}
	// (e) manifest vs bundle-DB identity consistency — a torn/edited manifest is refused.
	for _, c := range []struct{ field, manifestVal, bundleVal string }{
		{"name", m.Name, bundleSelf.Name},
		{"node_ident_pub", m.NodeIdentPub, bundleSelf.NodeIdentPub},
		{"raft_addr", m.RaftAddr, bundleSelf.RaftAddr},
		{"nats_route", m.NatsRoute, bundleSelf.NatsRoute},
		{"tunnel_addr", m.TunnelAddr, bundleSelf.TunnelAddr},
		{"public_host", m.PublicHost, bundleSelf.PublicHost},
		{"nats_server_id", m.NatsServerID, bundleSelf.NatsServerID},
	} {
		if c.manifestVal != c.bundleVal {
			return fmt.Errorf("clusteroffline: manifest/state.db disagree on %s (%q vs %q); refusing a torn/edited bundle",
				c.field, c.manifestVal, c.bundleVal)
		}
	}
	// External-review F5: cross-check the cursor too — a newer manifest spliced onto an older
	// state.db (or vice-versa) would otherwise restore while the logs report the wrong cursor.
	if m.AppliedIndex != bundleSelf.AppliedIndex {
		return fmt.Errorf("clusteroffline: manifest/state.db disagree on applied_index (%d vs %d); refusing a torn/spliced bundle",
			m.AppliedIndex, bundleSelf.AppliedIndex)
	}
	// (d) account_fp advisory (defense-in-depth, never fatal).
	if m.AccountFP != "" {
		if liveAcct, aerr := AccountFingerprint(opts.SecretsDir); aerr == nil && liveAcct != m.AccountFP {
			opts.Logger.Warn("clusteroffline: account_fp advisory mismatch (cluster-ca.pem differs from the bundle's); proceeding (un-forgeable tunnel-cert anchor matched)",
				"live", liveAcct, "manifest", m.AccountFP)
		}
	}
	return nil
}

// normalizeRestoreStaging resets the applied cursor to 0 and prunes every non-self roster row,
// in ONE txn. Returns the number of pruned peers. raftAddr (when non-empty AND different from the
// stored value) re-stamps the self row's raft_addr — External-review Q1 fresh-host restore.
func normalizeRestoreStaging(stagePath, selfID, raftAddr string) (int64, error) {
	db, err := storage.OpenWAL("file:" + stagePath)
	if err != nil {
		return 0, fmt.Errorf("clusteroffline: open staging for normalize: %w", err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("clusteroffline: begin normalize: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// External-review Q1: re-stamp the self row's raft_addr for a fresh-host restore.
	if raftAddr != "" {
		if _, err := tx.Exec(`UPDATE cluster_nodes SET raft_addr=? WHERE node_id=?`, raftAddr, selfID); err != nil {
			return 0, fmt.Errorf("clusteroffline: re-stamp self raft_addr: %w", err)
		}
	}

	// Audit DI-MAJOR-1: also reset audit_published_index — a real bundle carries the old high commit
	// N; after the fresh BootstrapSingleNode (CommitIndex≈1) the publisher's `hi <= cur` guard would
	// otherwise wedge audit re-derivation forever (the monotone UPSERT guard governs only the live
	// OpAuditCheckpointSet path; a direct reset here is correct).
	for _, kv := range []struct{ k, v string }{{"applied_index", "0"}, {"applied_term", "0"}, {"audit_published_index", "0"}} {
		if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, kv.k, kv.v); err != nil {
			return 0, fmt.Errorf("clusteroffline: reset %s: %w", kv.k, err)
		}
	}
	// Audit MAJOR: a backup taken from a force-single'd survivor carries force_single_active; clear it
	// so the restored node does not falsely report exit-3 / the severe banner.
	if _, err := tx.Exec(`DELETE FROM cluster_meta WHERE key='force_single_active'`); err != nil {
		return 0, fmt.Errorf("clusteroffline: clear force_single_active: %w", err)
	}
	// Audit DI-MAJOR-1 (cont.): the reqid ledger rows are keyed by the OLD high raft_index and would
	// never GC against the fresh log — drop them (the dedup window is meaningless post-rebootstrap).
	if _, err := tx.Exec(`DELETE FROM cluster_reqid_ledger`); err != nil {
		return 0, fmt.Errorf("clusteroffline: clear reqid ledger: %w", err)
	}
	// Audit DI-MAJOR-2: stamp a restore_in_progress marker. It rides the install into the live DB; the
	// daemon's assertClusterDBConsistent FATALs while it is set, so a SIGKILL between install and the
	// final raft-bootstrap leaves a NON-startable state (re-run restore to complete). RestoreFromBackup
	// clears it ONLY after BootstrapSingleNode succeeds.
	if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('restore_in_progress','1') ON CONFLICT(key) DO UPDATE SET value='1'`); err != nil {
		return 0, fmt.Errorf("clusteroffline: set restore_in_progress: %w", err)
	}
	// Stage-C MAJOR: force cluster_meta.self_node_id == the restore target so the daemon's raft
	// LocalID matches the {selfID} config BootstrapSingleNode lays down (a bundle whose self_node_id
	// somehow drifted would otherwise never elect).
	if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, selfID); err != nil {
		return 0, fmt.Errorf("clusteroffline: re-seed self_node_id: %w", err)
	}
	res, err := tx.Exec(`DELETE FROM cluster_nodes WHERE node_id != ?`, selfID)
	if err != nil {
		return 0, fmt.Errorf("clusteroffline: prune non-self roster: %w", err)
	}
	pruned, _ := res.RowsAffected()
	// Stage-C MAJOR-4: re-home every ALLOCATED port to self (+epoch bump). After pruning the peers,
	// a port still homed to a now-absent peer can NEVER be served — its home broker is gone from the
	// roster, so clusternodes.LookupByNodeID fails and the home ladder leaves the expose un-homed,
	// and NOTHING proposes a D6 reassign (the old home no longer exists to be detected down). The
	// restored node is the sole owner = the home for everything; the epoch bump makes agents re-pin.
	if _, err := tx.Exec(`UPDATE port_allocations SET home_broker=?, epoch=epoch+1 WHERE state='ALLOCATED' AND home_broker!=?`, selfID, selfID); err != nil {
		return 0, fmt.Errorf("clusteroffline: re-home ports to self: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("clusteroffline: commit normalize: %w", err)
	}
	// Checkpoint the WAL into the .db so the staged file is SELF-CONTAINED before the rename — no
	// -wal sidecar to leave orphaned (which a re-run could misapply, the BLOCKER above).
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return 0, fmt.Errorf("clusteroffline: checkpoint staging: %w", err)
	}
	return pruned, nil
}

// copyFileSync copies src to dst (O_EXCL was already cleared by the caller) and fsyncs dst.
func copyFileSync(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func isAlreadyBootstrapped(err error) bool {
	return errors.Is(err, cluster.ErrAlreadyBootstrapped)
}

// clearRestoreInProgress removes the DI-MAJOR-2 marker from the installed DB after the raft bootstrap
// completes (a kill-9 before this leaves the marker set → the daemon refuses to start).
func clearRestoreInProgress(dbPath string) error {
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key='restore_in_progress'`); err != nil {
		return err
	}
	_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return nil
}

// restoreAfterMigrateHook is a test-only injection point (nil in production); see its use site.
var restoreAfterMigrateHook func(stagePath string)
