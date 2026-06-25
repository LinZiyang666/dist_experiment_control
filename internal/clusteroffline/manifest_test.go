package clusteroffline

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// manifest_test.go — B6 step1 units (make test): the shared allowlist projection (the
// security invariant that PoP material can never leak), self-identity capture, applied_index
// read, account_fp, and manifest round-trip + O_EXCL + forward-incompat refusal.

// seedSelfRow inserts a COMPLETE cluster_nodes self VOTER row including the secret-bearing
// join_nonce/join_sig columns, so the allowlist test is not vacuous (it must prove those are
// NOT projected). applied_index is seeded in cluster_meta.
func seedSelfRow(t *testing.T, dbPath, selfID string, appliedIndex string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig)
		 VALUES(?,?,?,?,?,?,?,?,?,'VOTER','2026-06-24 00:00:00 +0000 UTC',?,?)`,
		selfID, "node-A-name", "Identpub", selfID, "10.0.0.1:7400", "nats://10.0.0.1:6222",
		"10.0.0.1:7443", "pub.example", "sha256:deadbeef", "SECRET_NONCE_xyz", "SECRET_SIG_abc",
	); err != nil {
		t.Fatalf("seed self row: %v", err)
	}
	if appliedIndex != "" {
		if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index',?)`, appliedIndex); err != nil {
			t.Fatalf("seed applied_index: %v", err)
		}
	}
}

func openRO(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	t.Cleanup(func() { _ = ro.Close() })
	return ro
}

func TestProjectRosterAllowlistNoSecretLeak(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	seedSelfRow(t, dbPath, "node-A", "42")
	ro := openRO(t, dbPath)

	roster, err := ProjectRoster(ro)
	if err != nil {
		t.Fatalf("ProjectRoster: %v", err)
	}
	if len(roster) != 1 {
		t.Fatalf("want 1 roster entry, got %d", len(roster))
	}
	e := roster[0]
	if e.NodeID != "node-A" || e.Name != "node-A-name" || e.Phase != "VOTER" || e.RaftAddr != "10.0.0.1:7400" {
		t.Fatalf("roster projection wrong: %+v", e)
	}
	// The load-bearing assertion: the serialized roster must contain NEITHER the PoP material
	// NOR the cert fingerprint — only the 4 allowlisted columns can ever appear.
	blob, _ := json.Marshal(roster)
	for _, secret := range []string{"SECRET_NONCE_xyz", "SECRET_SIG_abc", "join_nonce", "join_sig", "sha256:deadbeef", "cert_fp", "Identpub"} {
		if strings.Contains(string(blob), secret) {
			t.Fatalf("roster JSON leaked %q: %s", secret, blob)
		}
	}
}

func TestReadSelfIdentity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	seedSelfRow(t, dbPath, "node-A", "5000")
	ro := openRO(t, dbPath)

	m, err := ReadSelfIdentity(ro, "node-A")
	if err != nil {
		t.Fatalf("ReadSelfIdentity: %v", err)
	}
	if m.SelfID != "node-A" || m.Name != "node-A-name" || m.NodeIdentPub != "Identpub" ||
		m.NatsServerID != "node-A" || m.RaftAddr != "10.0.0.1:7400" || m.NatsRoute != "nats://10.0.0.1:6222" ||
		m.TunnelAddr != "10.0.0.1:7443" || m.PublicHost != "pub.example" || m.SelfCertFP != "sha256:deadbeef" {
		t.Fatalf("identity capture wrong: %+v", m)
	}
	if m.AppliedIndex != 5000 {
		t.Fatalf("applied_index want 5000, got %d", m.AppliedIndex)
	}
	if m.SchemaVersion != ManifestSchemaVersion || m.Mode != ManifestModeCluster {
		t.Fatalf("defaults wrong: schema=%d mode=%q", m.SchemaVersion, m.Mode)
	}
}

func TestReadSelfIdentityMissingRow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	seedSelfRow(t, dbPath, "node-A", "1")
	ro := openRO(t, dbPath)
	if _, err := ReadSelfIdentity(ro, "node-ZZ"); err == nil {
		t.Fatal("expected ErrSelfRowMissing for absent self id")
	} else if !strings.Contains(err.Error(), "not present") {
		t.Fatalf("want ErrSelfRowMissing, got %v", err)
	}
}

func TestReadAppliedIndexMissingKeyIsZero(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	seedSelfRow(t, dbPath, "node-A", "") // no applied_index key
	ro := openRO(t, dbPath)
	m, err := ReadSelfIdentity(ro, "node-A")
	if err != nil {
		t.Fatalf("ReadSelfIdentity: %v", err)
	}
	if m.AppliedIndex != 0 {
		t.Fatalf("missing applied_index must read 0, got %d", m.AppliedIndex)
	}
}

func TestAccountFingerprint(t *testing.T) {
	dir := t.TempDir()
	body := []byte("-----BEGIN CERTIFICATE-----\nfake-ca-bytes\n-----END CERTIFICATE-----\n")
	if err := os.WriteFile(filepath.Join(dir, "cluster-ca.pem"), body, 0o644); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	got, err := AccountFingerprint(dir)
	if err != nil {
		t.Fatalf("AccountFingerprint: %v", err)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("account_fp = %q, want %q", got, want)
	}
	if _, err := AccountFingerprint(filepath.Join(dir, "nope")); err == nil {
		t.Fatal("expected error for missing secrets dir")
	}
}

func TestManifestRoundTripAndGuards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	m := &Manifest{
		SchemaVersion: ManifestSchemaVersion,
		Kind:          ManifestKindBackup,
		CreatedAt:     "2026-06-24T00:00:00Z",
		ToolVersion:   "v0.0.0-dev",
		Mode:          ManifestModeCluster,
		SelfID:        "node-A",
		SelfCertFP:    "sha256:deadbeef",
		AccountFP:     "abc123",
		AppliedIndex:  5000,
		Name:          "node-A-name",
		NodeIdentPub:  "Identpub",
		RaftAddr:      "10.0.0.1:7400",
		NatsRoute:     "nats://10.0.0.1:6222",
		TunnelAddr:    "10.0.0.1:7443",
		PublicHost:    "pub.example",
		NatsServerID:  "node-A",
		Roster:        []RosterEntry{{NodeID: "node-A", Name: "node-A-name", Phase: "VOTER", RaftAddr: "10.0.0.1:7400"}},
	}
	if err := WriteManifest(path, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	// O_EXCL: a second write to the same path must refuse (never clobber).
	if err := WriteManifest(path, m); err == nil {
		t.Fatal("WriteManifest must refuse to overwrite an existing manifest")
	}
	got, err := ReadManifest(path)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.SelfID != m.SelfID || got.AppliedIndex != m.AppliedIndex || got.SelfCertFP != m.SelfCertFP ||
		len(got.Roster) != 1 || got.Roster[0].NodeID != "node-A" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	// mode 0600
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("manifest perm = %o, want 600", st.Mode().Perm())
	}
}

func TestReadManifestForwardIncompatRefused(t *testing.T) {
	dir := t.TempDir()
	// schema_version newer than this binary → refuse.
	newer := filepath.Join(dir, "newer.json")
	if err := os.WriteFile(newer, []byte(`{"schema_version":99,"kind":"backup"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(newer); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("want unsupported-schema refusal, got %v", err)
	}
	// schema_version 0 (absent) → refuse.
	zero := filepath.Join(dir, "zero.json")
	if err := os.WriteFile(zero, []byte(`{"kind":"backup"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(zero); err == nil {
		t.Fatal("want refusal for schema_version 0")
	}
	// unknown kind → refuse.
	badKind := filepath.Join(dir, "badkind.json")
	if err := os.WriteFile(badKind, []byte(`{"schema_version":1,"kind":"evil"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(badKind); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("want unknown-kind refusal, got %v", err)
	}
}

// TestManifestSourceRoleSerializes — External-review re-review: a follower backup's "follower" role
// MUST serialize (the prior `bool false,omitempty` silently vanished); offline ("") still omits.
func TestManifestSourceRoleSerializes(t *testing.T) {
	follower := Manifest{SchemaVersion: 1, Kind: ManifestKindBackup, Mode: ManifestModeCluster, SourceNodeID: "n1", SourceRole: "follower"}
	blob, err := json.Marshal(follower)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), `"source_role":"follower"`) {
		t.Fatalf("a follower backup must serialize source_role=follower (the stale warning): %s", blob)
	}
	// offline (no role) omits the key.
	offline := Manifest{SchemaVersion: 1, Kind: ManifestKindBackup, Mode: ManifestModeCluster}
	blob2, _ := json.Marshal(offline)
	if strings.Contains(string(blob2), "source_role") {
		t.Fatalf("an offline backup must OMIT source_role: %s", blob2)
	}
}
