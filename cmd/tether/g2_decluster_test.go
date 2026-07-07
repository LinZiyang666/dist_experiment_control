package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// g2_decluster_test.go (G2 #20 BLOCKER regression) — the offline de-cluster render MUST harvest THIS
// survivor's broker nkey from its cluster_nodes row, NOT via own.AuthIdentity() (which returns "" for a
// multi-user clustered conf — the only shape that ever needs de-clustering). A regression to AuthIdentity
// refuses every real N>=2 de-cluster, leaving the operator one restart from the exit-70 crash-loop #20
// exists to prevent. buildStandaloneConf is the pure (no nats-server binary) render core, so this runs
// hermetically under `make test`.

const g2ClusteredMultiUserConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
cluster {
  name: "tether"
  listen: "0.0.0.0:6222"
  routes: [ "nats://10.0.0.2:6222" ]
  tls {
    ca_file: "/ca.pem"
    cert_file: "/cert.pem"
    key_file: "/key.pem"
    verify: true
  }
}
authorization {
  auth_callout {
    issuer: "ABROKERACCOUNTPUB"
    account: "$G"
    auth_users: [ "USELFBROKERNKEY", "UPEERBROKERNKEY" ]
  }
  users: [
    { nkey: "USELFBROKERNKEY" }
    { nkey: "UPEERBROKERNKEY" }
  ]
}
`

func seedSelfIdentity(t *testing.T, dbPath, selfID, serverName, busNkey string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,bus_nkey_pub,added_at) `+
			`VALUES(?,?,'Uident',?,'127.0.0.1:7400','nats://x','x:7000','h','sha256:ab','VOTER',?,'2026-07-06 00:00:00 +0000 UTC')`,
		selfID, selfID, serverName, busNkey); err != nil {
		t.Fatalf("seed self: %v", err)
	}
}

func TestBuildStandaloneConfIdentityFromClusterNodes(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(g2ClusteredMultiUserConf), 0o600); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfIdentity(t, dbPath, "brk-a", "brk-a", "USELFBROKERNKEY")

	merged, storeDir, already, err := buildStandaloneConf(confPath, "brk-a", dbPath)
	if err != nil {
		t.Fatalf("buildStandaloneConf on a 2-auth_user clustered conf must succeed (BLOCKER regression): %v", err)
	}
	if already {
		t.Fatal("a clustered conf must NOT report already-standalone")
	}
	if storeDir != "/var/lib/tether/js" {
		t.Errorf("store_dir = %q, want /var/lib/tether/js (preserved)", storeDir)
	}
	if strings.Contains(merged, "cluster {") || strings.Contains(merged, "routes:") {
		t.Errorf("standalone render must DROP the cluster{} block:\n%s", merged)
	}
	if !strings.Contains(merged, "USELFBROKERNKEY") {
		t.Errorf("standalone render must carry THIS survivor's bus nkey (from cluster_nodes, NOT AuthIdentity):\n%s", merged)
	}
	if strings.Contains(merged, "UPEERBROKERNKEY") {
		t.Errorf("standalone render must NOT carry the peer's nkey (Peers=[self]):\n%s", merged)
	}
}

func TestBuildStandaloneConfAlreadyStandalone(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	standalone := "server_name: \"brk-a\"\nhost: \"0.0.0.0\"\nport: 4222\njetstream {\n  store_dir: \"/var/lib/tether/js\"\n}\n"
	if err := os.WriteFile(confPath, []byte(standalone), 0o600); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfIdentity(t, dbPath, "brk-a", "brk-a", "USELFBROKERNKEY")
	merged, storeDir, already, err := buildStandaloneConf(confPath, "brk-a", dbPath)
	if err != nil {
		t.Fatalf("buildStandaloneConf on a standalone conf: %v", err)
	}
	if !already {
		t.Fatal("a standalone conf must report already-standalone (no-op — no re-clobber of a hand-tuned survivor conf)")
	}
	if merged != "" {
		t.Error("already-standalone must return empty merged (nothing to swap)")
	}
	if storeDir != "/var/lib/tether/js" {
		t.Errorf("store_dir = %q, want preserved", storeDir)
	}
}

func TestBuildStandaloneConfRefusesMissingStoreDir(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	// clustered conf whose jetstream{} has NO store_dir → a standalone render would silently disable JS.
	noStore := strings.Replace(g2ClusteredMultiUserConf, "  store_dir: \"/var/lib/tether/js\"\n", "", 1)
	if err := os.WriteFile(confPath, []byte(noStore), 0o600); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	dbPath := filepath.Join(dir, "tether.db")
	seedSelfIdentity(t, dbPath, "brk-a", "brk-a", "USELFBROKERNKEY")
	if _, _, _, err := buildStandaloneConf(confPath, "brk-a", dbPath); err == nil {
		t.Fatal("buildStandaloneConf must REFUSE a clustered conf with no explicit store_dir (would silently disable JS)")
	}
}
