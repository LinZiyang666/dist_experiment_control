package clusteroffline_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

// ops11_test.go — B6 OPS#11 recover→manifest emit + init→consume round-trip (make test).

// seedRecoverableNode builds a DataDir (with raft/) + a DB carrying a self VOTER row whose
// cert_fp == the secrets dir's live tunnel fp, plus an extra peer. Returns the paths + fp.
func seedRecoverableNode(t *testing.T, selfID string) (dataDir, dbPath, secretsDir, fp string) {
	t.Helper()
	root := t.TempDir()
	dataDir = filepath.Join(root, "data")
	if err := os.MkdirAll(filepath.Join(dataDir, "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath = filepath.Join(dataDir, "tether.db")
	secretsDir = filepath.Join(root, "secrets")
	fp = writeSecrets(t, secretsDir)

	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
		 VALUES(?,?,?,?,?,?,?,?,?,'VOTER','2026-06-24 00:00:00 +0000 UTC')`,
		selfID, "broker-A", "Ident-A", selfID, "10.0.0.1:7400", "nats://10.0.0.1:6222",
		"10.0.0.1:7443", "a.example", fp,
	); err != nil {
		t.Fatalf("seed self: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
		 VALUES('node-B','broker-B','Ident-B','node-B','10.0.0.2:7400','nats://10.0.0.2:6222','10.0.0.2:7443','b.example','sha256:bb','VOTER','2026-06-24 00:00:00 +0000 UTC')`,
	); err != nil {
		t.Fatalf("seed peer: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?)`, selfID); err != nil {
		t.Fatalf("seed self_node_id: %v", err)
	}
	_ = db.Close()
	return dataDir, dbPath, secretsDir, fp
}

func TestRecoverEmitManifestThenInitFromManifest(t *testing.T) {
	selfID := "node-A"
	dataDir, dbPath, secretsDir, fp := seedRecoverableNode(t, selfID)
	manifestPath := filepath.Join(t.TempDir(), "ident.json")
	dumpPath := filepath.Join(t.TempDir(), "dump.json")

	// recover --emit-manifest: capture identity, then wipe.
	if _, err := clusteroffline.Recover(clusteroffline.RecoverOptions{
		DataDir: dataDir, DBPath: dbPath, DumpPath: dumpPath,
		SelfID: selfID, ManifestPath: manifestPath, SecretsDir: secretsDir,
		Now: func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// the DB is wiped.
	if _, err := os.Stat(dbPath); err == nil {
		t.Fatal("recover must wipe the DB")
	}
	// the manifest carries the identity.
	m, err := clusteroffline.ReadManifest(manifestPath)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.Kind != clusteroffline.ManifestKindRecover || m.SelfID != selfID || m.Name != "broker-A" ||
		m.RaftAddr != "10.0.0.1:7400" || m.SelfCertFP != fp {
		t.Fatalf("emitted manifest wrong: %+v", m)
	}
	if len(m.Roster) != 2 {
		t.Fatalf("manifest roster should hold both nodes, got %d", len(m.Roster))
	}

	// init --from-manifest: rebuild the self VOTER row from the manifest (cert_fp re-derived live).
	if err := clusteroffline.InitFromManifest(manifestPath, dataDir, dbPath, secretsDir,
		func() time.Time { return time.Unix(1700000001, 0).UTC() }, nil); err != nil {
		t.Fatalf("InitFromManifest: %v", err)
	}
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open reinited: %v", err)
	}
	defer func() { _ = ro.Close() }()
	var name, raftAddr, certFP, phase string
	if err := ro.QueryRow(`SELECT name, raft_addr, cert_fp, phase FROM cluster_nodes WHERE node_id=?`, selfID).Scan(&name, &raftAddr, &certFP, &phase); err != nil {
		t.Fatalf("reinited self row: %v", err)
	}
	if name != "broker-A" || raftAddr != "10.0.0.1:7400" || phase != "VOTER" {
		t.Fatalf("reinited identity wrong: name=%q raft=%q phase=%q", name, raftAddr, phase)
	}
	// cert_fp is the LIVE fp (re-derived from the secrets dir), not blindly replayed.
	if certFP != fp {
		t.Fatalf("cert_fp must be the live fp %q, got %q", fp, certFP)
	}
	// only the self row was seeded (the manifest is identity-only — no business-row replay).
	var n int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM cluster_nodes`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("init seeds ONLY the self row (identity-only), got %d", n)
	}
}

func TestInitFromManifestRefusesEmptyIdentity(t *testing.T) {
	// A manifest missing a required identity field must be refused by InitFromExisting's F4 guard.
	root := t.TempDir()
	secretsDir := filepath.Join(root, "secrets")
	writeSecrets(t, secretsDir)
	manifestPath := filepath.Join(root, "bad.json")
	// hand-write a manifest with an empty Name.
	if err := os.WriteFile(manifestPath, []byte(`{
		"schema_version":1,"kind":"recover-divergent","mode":"cluster","self_id":"node-A",
		"self_cert_fp":"x","name":"","node_ident_pub":"p","raft_addr":"10.0.0.1:7400",
		"nats_route":"nats://10.0.0.1:6222","tunnel_addr":"10.0.0.1:7443","public_host":"a.example","nats_server_id":"node-A"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	err := clusteroffline.InitFromManifest(manifestPath, dataDir, filepath.Join(dataDir, "tether.db"), secretsDir, nil, nil)
	if err == nil {
		t.Fatal("InitFromManifest must refuse a manifest with an empty identity field")
	}
}

// TestInitFromManifestRefusesBackupKind — Audit MAJOR: init --from-manifest must refuse a BACKUP
// manifest (its data lives in state.db; init would silently discard it). Use `cluster restore`.
func TestInitFromManifestRefusesBackupKind(t *testing.T) {
	root := t.TempDir()
	secretsDir := filepath.Join(root, "secrets")
	writeSecrets(t, secretsDir)
	manifestPath := filepath.Join(root, "backup.json")
	if err := os.WriteFile(manifestPath, []byte(`{
		"schema_version":1,"kind":"backup","mode":"cluster","self_id":"node-A","self_cert_fp":"x",
		"name":"n","node_ident_pub":"p","raft_addr":"10.0.0.1:7400","nats_route":"nats://10.0.0.1:6222",
		"tunnel_addr":"10.0.0.1:7443","public_host":"a.ex","nats_server_id":"node-A"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	_ = os.MkdirAll(dataDir, 0o700)
	err := clusteroffline.InitFromManifest(manifestPath, dataDir, filepath.Join(dataDir, "tether.db"), secretsDir, nil, nil)
	if err == nil {
		t.Fatal("init --from-manifest must refuse a backup-kind manifest")
	}
	if !strings.Contains(err.Error(), "cluster restore") {
		t.Fatalf("error should point at `cluster restore`: %v", err)
	}
}
