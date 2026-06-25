package clusteroffline_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// writeSelfSigned writes a self-signed ECDSA cert + key PEM to certPath/keyPath and
// returns the leaf fingerprint (tunnel.CertFingerprint) so the test can assert the
// seeded cluster_nodes.cert_fp matches the exact cert the daemon will present.
func writeSelfSigned(t *testing.T, certPath, keyPath string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "tether-test"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tunnel.CertFingerprint(leaf)
}

func writeSecrets(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	fp := writeSelfSigned(t, filepath.Join(dir, "tunnel-cert.pem"), filepath.Join(dir, "tunnel-key.pem"))
	// The CA/route certs are parsed by the daemon at start (not here) but InitFromExisting now
	// runs the full SecretsPreflight (external-review Q1), so write every required §15 file
	// (private keys 0600, certs 0644) — dummy content satisfies presence + perm checks.
	for _, f := range []string{"cluster-ca.pem", "route-cert.pem"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"route-key.pem", "node-ident.nk", "broker.nk", "account.nk"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return fp
}

func initOpts(dataDir, dbPath, secretsDir string) clusteroffline.InitFromExistingOptions {
	return clusteroffline.InitFromExistingOptions{
		DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		SelfID: "node-A", Name: "broker-A", NodeIdentPub: "pub-A",
		RaftAddr: "127.0.0.1:7400", NatsRoute: "127.0.0.1:6222",
		TunnelAddr: "127.0.0.1:7000", PublicHost: "a.example.com",
		Now: func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}
}

func TestD9InitFromExistingSeedsAndBootstraps(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	dbPath := filepath.Join(dataDir, "tether.db")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// A pre-migration single-broker DB: storage.Open runs migrations 0001-0013 (so the
	// cluster tables EXIST but are empty — the "never cluster-init'd" state).
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}
	_ = db.Close()

	secretsDir := filepath.Join(tmp, "secrets")
	wantFP := writeSecrets(t, secretsDir)

	if err := clusteroffline.InitFromExisting(initOpts(dataDir, dbPath, secretsDir)); err != nil {
		t.Fatalf("InitFromExisting: %v", err)
	}

	// raft/ now exists (the detection trigger) and holds state.
	if ok, err := cluster.RaftStateExists(dataDir); err != nil || !ok {
		t.Fatalf("raft state after init: ok=%v err=%v (must exist)", ok, err)
	}
	// .bak preserved.
	if _, err := os.Stat(dbPath + ".bak"); err != nil {
		t.Fatalf("pristine .bak must exist: %v", err)
	}

	// Re-open read-only and assert the seeds.
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()

	var idx, selfID, certFP, phase string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&idx); err != nil || idx != "0" {
		t.Fatalf("applied_index seed: %q err=%v (want 0)", idx, err)
	}
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='self_node_id'`).Scan(&selfID); err != nil || selfID != "node-A" {
		t.Fatalf("self_node_id seed: %q err=%v (want node-A)", selfID, err)
	}
	if err := ro.QueryRow(`SELECT cert_fp, phase FROM cluster_nodes WHERE node_id='node-A'`).Scan(&certFP, &phase); err != nil {
		t.Fatalf("self cluster_nodes row: %v", err)
	}
	if certFP != wantFP {
		t.Fatalf("cert_fp mismatch: seeded %q want %q (the daemon's presented cert fp)", certFP, wantFP)
	}
	if phase != "VOTER" {
		t.Fatalf("self row phase = %q, want VOTER", phase)
	}
}

func TestD9InitFromExistingIdempotent(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	dbPath := filepath.Join(dataDir, "tether.db")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	secretsDir := filepath.Join(tmp, "secrets")
	writeSecrets(t, secretsDir)
	opts := initOpts(dataDir, dbPath, secretsDir)

	if err := clusteroffline.InitFromExisting(opts); err != nil {
		t.Fatalf("first init: %v", err)
	}
	// Re-run must be a clean no-op (idempotent): migrations skip, seeds ON CONFLICT,
	// bootstrap returns ErrAlreadyBootstrapped (swallowed).
	if err := clusteroffline.InitFromExisting(opts); err != nil {
		t.Fatalf("re-run init must be idempotent, got: %v", err)
	}
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	var idx string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&idx); err != nil || idx != "0" {
		t.Fatalf("applied_index after re-run: %q err=%v (want still 0)", idx, err)
	}
	var n int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id='node-A'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("self row count after re-run: %d err=%v (want exactly 1)", n, err)
	}
}

// TestD9InitFromExistingRefusesNameChange guards the loud-refusal on a re-run with the
// same node_id but a different identity (an operator typo must not silently overwrite).
func TestD9InitFromExistingRefusesNameChange(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	dbPath := filepath.Join(dataDir, "tether.db")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	secretsDir := filepath.Join(tmp, "secrets")
	writeSecrets(t, secretsDir)
	opts := initOpts(dataDir, dbPath, secretsDir)
	if err := clusteroffline.InitFromExisting(opts); err != nil {
		t.Fatalf("first init: %v", err)
	}
	// Same node_id, different name → loud refusal (not a silent overwrite).
	opts.Name = "broker-A-renamed"
	if err := clusteroffline.InitFromExisting(opts); err == nil {
		t.Fatal("re-run with a different identity must be refused, got nil")
	}
}

// TestD9InitFromExistingRefusesLiveDaemon — Audit TEST-MAJOR-2: the irreversible live-DB migration
// must refuse-before-mutate when a daemon holds the DB write lock (restore has this; init lacked it).
func TestD9InitFromExistingRefusesLiveDaemon(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "tether.db")
	secrets := filepath.Join(dataDir, "secrets")
	writeSecrets(t, secrets)
	// A pre-migration single-broker DB.
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Hold a write lock (a live daemon).
	hold, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hold.Close() }()
	tx, err := hold.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:held','1') ON CONFLICT(key) DO UPDATE SET value='1'`); err != nil {
		// cluster_meta exists post-migration; if not, grab the lock another way.
		_, _ = tx.Exec(`CREATE TABLE IF NOT EXISTS t_lock(x)`)
		_, _ = tx.Exec(`INSERT INTO t_lock(x) VALUES(1)`)
	}
	defer func() { _ = tx.Rollback() }()
	_ = db.Close()

	if err := clusteroffline.InitFromExisting(initOpts(dataDir, dbPath, secrets)); err == nil {
		t.Fatal("InitFromExisting must refuse while a daemon holds the DB write lock")
	}
	// No .bak created (the irreversible migration never started).
	if _, statErr := os.Stat(dbPath + ".bak"); statErr == nil {
		t.Fatal("a refused init must NOT create a .bak (it never mutated)")
	}
}

// TestF13InitRejectsBadIdentity — External-review F13: the offline init must apply the online path's
// node_id + text validation (proto.ValidateNID + NUL/non-UTF-8 rejection), fail-closed pre-mutation.
func TestF13InitRejectsBadIdentity(t *testing.T) {
	mk := func() (string, string, string) {
		dir := t.TempDir()
		sec := filepath.Join(dir, "secrets")
		writeSecrets(t, sec)
		return dir, filepath.Join(dir, "tether.db"), sec
	}
	t.Run("bad node_id charset", func(t *testing.T) {
		dir, db, sec := mk()
		o := initOpts(dir, db, sec)
		o.SelfID = "brk a; rm -rf /"
		if err := clusteroffline.InitFromExisting(o); err == nil {
			t.Fatal("must reject a node_id with shell metachars/spaces")
		}
		if _, statErr := os.Stat(db + ".bak"); statErr == nil {
			t.Fatal("a rejected init must not have mutated (no .bak)")
		}
	})
	t.Run("NUL in text field", func(t *testing.T) {
		dir, db, sec := mk()
		o := initOpts(dir, db, sec)
		o.Name = "broker\x00evil"
		if err := clusteroffline.InitFromExisting(o); err == nil {
			t.Fatal("must reject a NUL byte in a text field")
		}
	})
}
