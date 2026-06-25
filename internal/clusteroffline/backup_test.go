package clusteroffline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// backup_test.go — B6 step3 units (make test): offline backup bundle shape + manifest
// correctness + the manifest-level allowlist scrub + single-broker mode + O_EXCL bundle dir +
// live-daemon refuse. The full backup→restore round-trip + the index-durability proof live in
// restore_test.go (they need the restore half).

// seedClusterMetaSelf records self_node_id so the backup records a cluster-mode bundle.
func seedClusterMetaSelf(t *testing.T, dbPath, selfID string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?)`, selfID); err != nil {
		t.Fatalf("seed self_node_id: %v", err)
	}
}

func TestOfflineBackupClusterMode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfRow(t, dbPath, "node-A", "5000")
	seedClusterMetaSelf(t, dbPath, "node-A")

	out := filepath.Join(dir, "bundle")
	res, err := OfflineBackup(BackupOptions{DataDir: dir, DBPath: dbPath, OutDir: out})
	if err != nil {
		t.Fatalf("OfflineBackup: %v", err)
	}
	if res.Mode != ManifestModeCluster || res.SelfID != "node-A" || res.AppliedIndex != 5000 {
		t.Fatalf("result wrong: %+v", res)
	}
	// bundle layout.
	if _, err := os.Stat(filepath.Join(out, "state.db")); err != nil {
		t.Fatalf("state.db missing: %v", err)
	}
	m, err := ReadManifest(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.Kind != ManifestKindBackup || m.Mode != ManifestModeCluster || m.SelfID != "node-A" ||
		m.Name != "node-A-name" || m.RaftAddr != "10.0.0.1:7400" || m.AppliedIndex != 5000 ||
		m.SelfCertFP != "sha256:deadbeef" || m.ToolVersion == "" {
		t.Fatalf("manifest wrong: %+v", m)
	}
	if len(m.Roster) != 1 || m.Roster[0].NodeID != "node-A" || m.Roster[0].Phase != "VOTER" {
		t.Fatalf("roster wrong: %+v", m.Roster)
	}
}

func TestOfflineBackupManifestNoSecretLeak(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfRow(t, dbPath, "node-A", "1") // seeds join_nonce=SECRET_NONCE_xyz / join_sig=SECRET_SIG_abc
	seedClusterMetaSelf(t, dbPath, "node-A")

	out := filepath.Join(dir, "bundle")
	if _, err := OfflineBackup(BackupOptions{DataDir: dir, DBPath: dbPath, OutDir: out}); err != nil {
		t.Fatalf("OfflineBackup: %v", err)
	}
	// The manifest is the operator-shareable provenance sidecar — it must carry NO PoP material
	// and NO PEM/seed bytes (those are allowlist-projected out; the un-forgeable anchor is a
	// fingerprint, never a key).
	blob, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"SECRET_NONCE_xyz", "SECRET_SIG_abc", "join_nonce", "join_sig", "-----BEGIN", "SEED"} {
		if strings.Contains(string(blob), secret) {
			t.Fatalf("manifest leaked %q:\n%s", secret, blob)
		}
	}
}

func TestOfflineBackupSingleBrokerMode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	// A migrated-but-never-clustered DB: open it so migrations run, but seed NO self_node_id.
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_ = db.Close()

	out := filepath.Join(dir, "bundle")
	res, err := OfflineBackup(BackupOptions{DataDir: dir, DBPath: dbPath, OutDir: out})
	if err != nil {
		t.Fatalf("OfflineBackup: %v", err)
	}
	if res.Mode != ManifestModeSingleBroker || res.SelfID != "" {
		t.Fatalf("want single-broker/empty self, got %+v", res)
	}
	m, err := ReadManifest(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.Mode != ManifestModeSingleBroker || m.SelfID != "" || m.Name != "" || len(m.Roster) != 0 {
		t.Fatalf("single-broker manifest should be minimal: %+v", m)
	}
}

func TestOfflineBackupRefusesExistingBundleDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfRow(t, dbPath, "node-A", "1")
	seedClusterMetaSelf(t, dbPath, "node-A")

	out := filepath.Join(dir, "bundle")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := OfflineBackup(BackupOptions{DataDir: dir, DBPath: dbPath, OutDir: out}); err == nil {
		t.Fatal("OfflineBackup must refuse a pre-existing bundle dir (O_EXCL)")
	}
}

func TestOfflineBackupRefusesLiveDaemon(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfRow(t, dbPath, "node-A", "1")
	seedClusterMetaSelf(t, dbPath, "node-A")

	// Simulate a live writer holding the DB's write lock (a running daemon).
	hold, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open hold: %v", err)
	}
	defer func() { _ = hold.Close() }()
	tx, err := hold.Begin()
	if err != nil {
		t.Fatalf("begin hold: %v", err)
	}
	if _, err := tx.Exec(`UPDATE cluster_meta SET value=value WHERE key='self_node_id'`); err != nil {
		t.Fatalf("grab write lock: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	out := filepath.Join(dir, "bundle")
	if _, err := OfflineBackup(BackupOptions{DataDir: dir, DBPath: dbPath, OutDir: out}); err == nil {
		t.Fatal("OfflineBackup must refuse while a daemon holds the write lock")
	}
	// the half-bundle must be cleaned up.
	if _, statErr := os.Stat(out); statErr == nil {
		t.Fatal("refused backup must not leave a bundle dir behind")
	}
}
