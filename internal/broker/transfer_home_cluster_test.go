package broker

import (
	"database/sql"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// seedHomedNode inserts a session + a node bound (via nats_server) to a VOTER cluster_nodes
// row, so resolveHomeForAgent(sid,nid) resolves to nodeID (the D6 server-id bridge).
func seedHomedNode(t *testing.T, db *sql.DB, sid, nid, natsServer, nodeID string) {
	t.Helper()
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES(?,?,?,?)`,
		sid, sid, "fp", "ph"); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,nats_server) VALUES(?,?,?)`, sid, nid, natsServer); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO cluster_nodes(
		node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		nodeID, nodeID, "pub-"+nodeID, natsServer, "127.0.0.1:7400", "127.0.0.1:6222",
		"127.0.0.1:7000", "host-"+nodeID, "sha256:deadbeef", "VOTER", "2026-06-23T00:00:00Z"); err != nil {
		t.Fatalf("seed cluster_node: %v", err)
	}
}

// TestXferBucketOrphanedEverywhere (external review N-6) pins the noise-guard predicate behind the
// unreapable-bucket gauge: only a bucket NO broker can ever reap (split-home OR zero-node session)
// is "orphaned everywhere". A single-home session — to self OR to a peer — is reapable by exactly one
// broker and must NOT count (else the gauge is ~always nonzero on a healthy N-broker cluster).
func TestXferBucketOrphanedEverywhere(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-A"}

	seedHomedNode(t, db, "self", "s1", "srv-A", "node-A")
	if b.xferBucketOrphanedEverywhere("self") {
		t.Fatal("single-home-to-self is reapable by self — not orphaned everywhere")
	}
	seedHomedNode(t, db, "else", "e1", "srv-B", "node-B")
	if b.xferBucketOrphanedEverywhere("else") {
		t.Fatal("single-home-elsewhere is reapable by that broker — not orphaned everywhere (the noise guard)")
	}
	seedHomedNode(t, db, "split", "sp1", "srv-A", "node-A")
	seedHomedNode(t, db, "split", "sp2", "srv-B", "node-B")
	if !b.xferBucketOrphanedEverywhere("split") {
		t.Fatal("a split-home session is unreapable by every broker — orphaned everywhere")
	}
	if !b.xferBucketOrphanedEverywhere("ghost-no-nodes") {
		t.Fatal("a zero-node session bucket is unreapable by every broker — orphaned everywhere")
	}
}

// TestD8TransferHomeGateClustered (review m3): the clustered home gate — the load-bearing
// anti-fan-out mechanism — proceeds ONLY on the agent's home broker; a non-home or an
// unresolved (no binding) target is silently refused.
func TestD8TransferHomeGateClustered(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	seedHomedNode(t, db, "lab", "lab-1", "srv-A", "node-A")

	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-A"}
	if !b.transferHomeGate("lab", "lab-1") {
		t.Fatal("home==self must proceed")
	}
	b.selfID = "node-B"
	if b.transferHomeGate("lab", "lab-1") {
		t.Fatal("home!=self must NOT proceed (anti-fan-out)")
	}
	if b.transferHomeGate("lab", "no-such-nid") {
		t.Fatal("unresolved binding must NOT proceed (silent retry posture)")
	}
}

// TestD8HomeOwnsXferBucketClustered (review m3): the boot orphan reaper reaps a bucket ONLY
// when EVERY bound node homes to self — a mixed-home session is left to a peer (never deletes
// another broker's live object from the shared replicated bucket).
func TestD8HomeOwnsXferBucketClustered(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	seedHomedNode(t, db, "lab", "lab-1", "srv-A", "node-A")

	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "node-A"}
	if !b.homeOwnsXferBucket("lab") {
		t.Fatal("a session whose only node homes to self must be reapable")
	}
	// A second node homed ELSEWHERE makes it a mixed-home session → not ours to reap.
	seedHomedNode(t, db, "lab", "lab-2", "srv-B", "node-B")
	if b.homeOwnsXferBucket("lab") {
		t.Fatal("a mixed-home session must NOT be reaped by self (would delete a peer's live object)")
	}
}
