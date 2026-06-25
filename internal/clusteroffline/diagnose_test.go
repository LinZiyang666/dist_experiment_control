package clusteroffline

import (
	"net"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// diagnose_test.go — B7 DOC#7: DiagnosePeers probes the roster (read-only). A peer at 127.0.0.1:1
// is connection-refused → DEAD; the derived list enumerates every peer.
func TestDiagnosePeersDeadPeer(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// self + a peer whose ports are all connection-refused (127.0.0.1:1).
	for _, stmt := range []string{
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','127.0.0.1:1','127.0.0.1:1','127.0.0.1:1','h','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`,
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('peer-x','peer-x','p','peer-x','127.0.0.1:1','127.0.0.1:1','127.0.0.1:1','h','sha256:x','VOTER','2026-06-24 00:00:00 +0000 UTC')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	_ = db.Close()

	diags, err := DiagnosePeers(dbPath, "self")
	if err != nil {
		t.Fatalf("DiagnosePeers: %v", err)
	}
	if len(diags) != 1 || diags[0].NodeID != "peer-x" {
		t.Fatalf("want 1 peer (peer-x), got %+v", diags)
	}
	if diags[0].Alive {
		t.Fatalf("a 127.0.0.1:1 peer must be DEAD, got alive=%v on %q", diags[0].Alive, diags[0].AliveOn)
	}
	if AnyAlive(diags) {
		t.Fatal("AnyAlive must be false")
	}
	if ids := DeadPeerIDs(diags); len(ids) != 1 || ids[0] != "peer-x" {
		t.Fatalf("DeadPeerIDs wrong: %v", ids)
	}
}

// TestDiagnosePeersAlivePeer — Stage-C: a peer with a LISTENING port is ALIVE → force-single must
// be blocked.
func TestDiagnosePeersAlivePeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('self','self','p','self','127.0.0.1:1','127.0.0.1:1','127.0.0.1:1','h','sha256:s','VOTER','2026-06-24 00:00:00 +0000 UTC')`,
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES('peer-alive','peer-alive','p','peer-alive','` + addr + `','127.0.0.1:1','127.0.0.1:1','h','sha256:x','VOTER','2026-06-24 00:00:00 +0000 UTC')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	_ = db.Close()

	diags, err := DiagnosePeers(dbPath, "self")
	if err != nil {
		t.Fatalf("DiagnosePeers: %v", err)
	}
	if len(diags) != 1 || !diags[0].Alive {
		t.Fatalf("a listening peer must be ALIVE: %+v", diags)
	}
	if !AnyAlive(diags) {
		t.Fatal("AnyAlive must be true — force-single must be blocked")
	}
}
