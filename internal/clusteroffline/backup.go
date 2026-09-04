package clusteroffline

// backup.go — the B6 `cluster backup --offline` path (OPS#3). It produces a self-describing
// bundle directory { state.db, manifest.json } off a daemon-STOPPED on-disk DB. The ONLINE
// path (a live cluster node) goes through adminsock → Node.BackupDBTo instead; this offline
// path is for a stopped daemon (and is the non-cluster byte-equivalence path: a single broker
// backs up as "the SQLite file + a single-broker manifest", no raft involved).
//
// A backup is read-only and can never corrupt durable state, but it still refuses to run while
// a daemon is live (the live DB may be mid-transaction; the supported way to back up a RUNNING
// cluster is the online path). The manifest is built from the COPY (the bundle's own state.db),
// so manifest.applied_index is exactly the bundle's committed cursor.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// BackupOptions configures an offline backup.
type BackupOptions struct {
	DataDir    string // for the raft.db live-daemon probe (may have no raft/ for a single broker)
	DBPath     string // the on-disk tether.db to back up
	SecretsDir string // optional; enables the advisory account_fp (sha256 cluster-ca.pem)
	OutDir     string // the bundle directory to CREATE (must not already exist)
	Now        func() time.Time
	Logger     *slog.Logger
}

// BackupResult summarizes a completed backup.
type BackupResult struct {
	BundleDir    string
	StateDBBytes int64
	Mode         ManifestMode
	SelfID       string
	AppliedIndex uint64
}

// OfflineBackup writes a { state.db, manifest.json } bundle. Steps: refuse if a daemon is live
// → create the bundle dir (O_EXCL) → paged read-only backup into state.db → verify it →
// build the manifest from the copy → write manifest. Any failure removes the half-written
// bundle dir so a retry starts clean.
func OfflineBackup(opts BackupOptions) (result *BackupResult, err error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.DBPath == "" || opts.OutDir == "" {
		return nil, fmt.Errorf("clusteroffline: OfflineBackup requires DBPath and OutDir")
	}
	if _, statErr := os.Stat(opts.DBPath); statErr != nil {
		return nil, fmt.Errorf("clusteroffline: source DB %q not found: %w", opts.DBPath, statErr)
	}

	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = filepath.Dir(opts.DBPath)
	}

	// (1) live-daemon interlock FIRST — it is the diagnosis an operator can act on.
	//
	// A running broker holds the data-dir lock for its whole lifetime, so if the flock
	// came first it would answer first, and it would answer with
	// "resource temporarily unavailable" — which names neither the broker nor the fix.
	// The interlock names both. origin: prerelease audit round 2, CC-6; the first
	// version of this hunk had the two in the other order and a comment asserting this
	// one.
	if err := checkNoLiveDaemon(dataDir, opts.DBPath); err != nil {
		return nil, err
	}

	// (2) flock — bar a CONCURRENT OFFLINE TOOL.
	//
	// origin: prerelease audit, §3 MINOR sweep. Every other offline operation in this
	// package takes this lock; the backup was the one that did not. The live-daemon
	// interlock above is a different question and does not answer this one: with the
	// daemon stopped, a `recovery restore` and a `backup` both pass their daemon probes
	// and then run against the same file. The restore swaps the DB out from under the
	// backup's copy, and the resulting bundle is a mix of two databases — with a
	// manifest built from the copy, so it describes itself as consistent.
	//
	// A READ-ONLY SOURCE IS ALLOWED THROUGH, deliberately. Taking the lock CREATES a
	// file, so a backup of a read-only mount or of a filesystem snapshot — a normal DR
	// practice, and one that used to work — would otherwise start failing for a reason
	// that has nothing to do with the backup. Letting it through is not fail-open here:
	// what the lock guards against is another offline tool MUTATING the source, and
	// every one of those (restore, init, force-single) writes. A source we cannot write
	// is a source none of them can be running against either.
	release, err := cluster.AcquireDataDirLock(dataDir)
	switch {
	case err == nil:
		defer release()
	case errors.Is(err, syscall.EROFS):
		opts.Logger.Warn("clusteroffline: backup source is on a read-only filesystem; proceeding "+
			"without the data-dir lock (no offline tool can be mutating a source it cannot write)",
			"data_dir", dataDir)
	default:
		return nil, err
	}

	// (2) create the bundle dir with O_EXCL semantics (os.Mkdir fails if it exists), so a
	// backup never clobbers an existing bundle. Clean it up on any later failure.
	if err := os.MkdirAll(filepath.Dir(opts.OutDir), 0o700); err != nil {
		return nil, fmt.Errorf("clusteroffline: prepare bundle parent: %w", err)
	}
	if err := os.Mkdir(opts.OutDir, 0o700); err != nil {
		return nil, fmt.Errorf("clusteroffline: create bundle dir %q (must not exist): %w", opts.OutDir, err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(opts.OutDir)
		}
	}()

	statePath := filepath.Join(opts.OutDir, "state.db")
	manifestPath := filepath.Join(opts.OutDir, "manifest.json")

	// (3) paged read-only backup into state.db (never mutates the source).
	if err := cluster.BackupDBFile(context.Background(), opts.DBPath, statePath); err != nil {
		return nil, fmt.Errorf("clusteroffline: backup state.db: %w", err)
	}
	// (4) verify the produced copy (a torn backup is refused here, not at restore time).
	if err := cluster.VerifyIntegrity(statePath); err != nil {
		return nil, fmt.Errorf("clusteroffline: backup verify: %w", err)
	}

	// (5) build the manifest FROM THE COPY (so applied_index == the bundle's cursor).
	m, mode, selfID, err := buildBackupManifest(statePath, opts)
	if err != nil {
		return nil, err
	}
	if err := WriteManifest(manifestPath, m); err != nil {
		return nil, err
	}

	st, _ := os.Stat(statePath)
	opts.Logger.Info("clusteroffline: backup complete", "bundle", opts.OutDir, "mode", mode, "self", selfID, "applied_index", m.AppliedIndex)
	// R10 #53: the bundle is state.db-only. Say so from the library too, so a non-CLI caller (or a
	// journald-only record of a cron backup) still carries the scope statement — see bundle_scope.go.
	opts.Logger.Warn("clusteroffline: bundle contains the FSM state DB ONLY — JetStream (history/audit/events/in-flight transfers) is NOT included and a restore does NOT bring it back; back JetStream up separately with `nats stream backup`",
		"bundle", opts.OutDir)
	return &BackupResult{BundleDir: opts.OutDir, StateDBBytes: sizeOf(st), Mode: mode, SelfID: selfID, AppliedIndex: m.AppliedIndex}, nil
}

// buildBackupManifest opens the bundle's state.db read-only and assembles the manifest. A DB
// with a seeded self_node_id + a matching cluster_nodes row is ManifestModeCluster (full
// identity + roster); anything else is ManifestModeSingleBroker (minimal — no identity).
func buildBackupManifest(statePath string, opts BackupOptions) (*Manifest, ManifestMode, string, error) {
	db, err := storage.OpenReadOnly("file:" + statePath)
	if err != nil {
		return nil, "", "", fmt.Errorf("clusteroffline: open bundle for manifest: %w", err)
	}
	defer func() { _ = db.Close() }()

	now := opts.Now().UTC().Format(time.RFC3339Nano)
	selfID, err := ReadSelfNodeID(db)
	if err != nil {
		return nil, "", "", err
	}

	if selfID != "" {
		m, err := ReadSelfIdentity(db, selfID)
		if err == nil {
			m.Kind = ManifestKindBackup
			m.Mode = ManifestModeCluster
			m.CreatedAt = now
			m.ToolVersion = proto.ReleaseVersion
			roster, rerr := ProjectRoster(db)
			if rerr != nil {
				return nil, "", "", rerr
			}
			m.Roster = roster
			if opts.SecretsDir != "" {
				if fp, ferr := AccountFingerprint(opts.SecretsDir); ferr == nil {
					m.AccountFP = fp
				} else {
					opts.Logger.Warn("clusteroffline: account_fp unavailable (advisory only)", "err", ferr)
				}
			}
			return m, ManifestModeCluster, selfID, nil
		}
		// self_node_id present but no matching row: treat as a malformed/partial cluster DB —
		// fall through to single-broker so the backup still succeeds (the bundle is then only
		// restorable as a single broker, which the restore mode-gate enforces).
		opts.Logger.Warn("clusteroffline: self_node_id present but self row missing; recording single-broker bundle", "self", selfID)
	}

	return &Manifest{
		SchemaVersion: ManifestSchemaVersion,
		Kind:          ManifestKindBackup,
		Mode:          ManifestModeSingleBroker,
		CreatedAt:     now,
		ToolVersion:   proto.ReleaseVersion,
	}, ManifestModeSingleBroker, "", nil
}

// checkNoLiveDaemon runs the two live-daemon interlock probes shared by every offline B6
// surgery (backup + restore): the bolt raft.db lock (catches a cluster daemon) and the SQLite
// writer lock (catches a pre-cutover single-broker daemon). Either positive → ErrDaemonRunning.
// Each probe is skipped when its target file is absent (a fresh / non-cluster box).
func checkNoLiveDaemon(dataDir, dbPath string) error {
	if _, statErr := os.Stat(filepath.Join(dataDir, "raft", "raft.db")); statErr == nil {
		if locked, err := cluster.RaftStoreLockedByDaemon(dataDir); err != nil {
			return err
		} else if locked {
			return ErrDaemonRunning
		}
	}
	if _, statErr := os.Stat(dbPath); statErr == nil {
		if busy, err := storage.ProbeWriterLock(dbPath); err != nil {
			return fmt.Errorf("clusteroffline: write-lock probe: %w", err)
		} else if busy {
			return ErrDaemonRunning
		}
	}
	return nil
}
