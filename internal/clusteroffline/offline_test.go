package clusteroffline

import (
	"database/sql"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
)

// offline_test.go — D7 §8.4 cheap offline-tool units (make test): empty-state
// refuse, flock exclusivity (two fds in-process — NOT vacuous: flock is per open
// file description, so two os.OpenFile's conflict), dump durability + O_EXCL +
// wipe-refused-on-dump-fail. The full 3-node force-single->recover drill is gated
// d7_integration (test/d7).

func mustDataDir(t *testing.T) (dataDir, dbPath string) {
	t.Helper()
	dataDir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "raft"), 0o700); err != nil {
		t.Fatalf("mkdir raft: %v", err)
	}
	dbPath = filepath.Join(dataDir, "tether.db")
	return dataDir, dbPath
}

// seedDB builds a real tether.db (migrations applied) with one cluster_nodes peer +
// one cluster_meta row, so the dump has something to capture.
func seedDB(t *testing.T, dbPath string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) ` +
			`VALUES('peer-2','peer-2','Upub','tether-2','127.0.0.1:1','nats://x','x:7000','h','sha256:ab','VOTER','2026-06-23 00:00:00 +0000 UTC')`,
	); err != nil {
		t.Fatalf("seed cluster_nodes: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:demo','1')`); err != nil {
		t.Fatalf("seed cluster_meta: %v", err)
	}
}

func TestD7ForceSingleEmptyStateRefuse(t *testing.T) {
	dataDir, dbPath := mustDataDir(t)
	_, err := ForceSingle(ForceSingleOptions{
		DataDir: dataDir, DBPath: dbPath, SelfID: "self-1", SelfRaftAddr: "127.0.0.1:7400",
		Now: func() time.Time { return time.Unix(0, 0) },
	})
	if !errors.Is(err, cluster.ErrNoExistingState) {
		t.Fatalf("force-single on empty disk: want ErrNoExistingState, got %v", err)
	}
}

func TestD7OfflineFlockExclusive(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, lockFileName)
	release, err := acquireFlock(lock)
	if err != nil {
		t.Fatalf("first flock: %v", err)
	}
	// A SECOND open file description on the same path must be denied (flock is
	// per-open-file-description, so two os.OpenFile's conflict even in-process).
	if _, err := acquireFlock(lock); err == nil {
		t.Fatal("second flock acquired while first held — exclusivity broken")
	}
	release()
	// After release, it can be re-acquired.
	r2, err := acquireFlock(lock)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	r2()
}

func TestD7RecoverDumpsThenWipes(t *testing.T) {
	dataDir, dbPath := mustDataDir(t)
	seedDB(t, dbPath)
	dump := filepath.Join(t.TempDir(), "divergent.json")

	n, err := Recover(RecoverOptions{DataDir: dataDir, DBPath: dbPath, DumpPath: dump})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n == 0 {
		t.Fatal("recover dumped 0 rows; expected the seeded rows")
	}
	st, err := os.Stat(dump)
	if err != nil || st.Size() == 0 {
		t.Fatalf("dump not durable: size=%d err=%v", sizeOf(st), err)
	}
	if fi, _ := os.Stat(dump); fi != nil && fi.Mode().Perm() != 0o600 {
		t.Fatalf("dump perm = %o, want 0600", fi.Mode().Perm())
	}
	if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tether.db not wiped after recover: %v", err)
	}
}

func TestD7PeerReachableHardRefuse(t *testing.T) {
	// A peer whose raft_addr COMPLETES a TCP connection is ALIVE -> HARD-REFUSE, even
	// though the operator listed it as dead (§8.4(d)/B-8: TCP-completes is the gate).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	live := ln.Addr().String()
	if err := checkPeersDead([]Peer{{NodeID: "peer-live", RaftAddr: live}}, []string{"peer-live"}); err == nil || !strings.Contains(err.Error(), "HARD-REFUSE") {
		t.Fatalf("a TCP-reachable peer must HARD-REFUSE, got %v", err)
	}

	// A roster peer NOT in --confirm-peers-dead -> refuse (an unlisted, maybe-partitioned peer splits-brain).
	if err := checkPeersDead([]Peer{{NodeID: "peer-x"}}, nil); err == nil || !strings.Contains(err.Error(), "must list EVERY peer") {
		t.Fatalf("an unlisted peer must refuse, got %v", err)
	}

	// A dead (unreachable) + listed peer passes. 127.0.0.1:1 has no listener (a
	// non-root process can't bind port 1), so the dial is refused immediately —
	// deterministic, unlike a just-closed ephemeral port the OS may reuse.
	if err := checkPeersDead([]Peer{{NodeID: "peer-dead", RaftAddr: "127.0.0.1:1"}}, []string{"peer-dead"}); err != nil {
		t.Fatalf("a dead+listed peer should pass, got %v", err)
	}
}

func TestD7ReadRosterRejectsUnknownSelfID(t *testing.T) {
	_, dbPath := mustDataDir(t)
	seedDB(t, dbPath)
	if _, err := readRoster(dbPath, "typo-self"); err == nil || !strings.Contains(err.Error(), "self-id") {
		t.Fatalf("unknown self-id must be rejected before raft rewrite, got %v", err)
	}
	peers, err := readRoster(dbPath, "peer-2")
	if err != nil {
		t.Fatalf("known self-id should read roster: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("self-only fixture should have no peers, got %+v", peers)
	}
}

func TestD9BackupOnceCheckpointsWAL(t *testing.T) {
	_, dbPath := mustDataDir(t)
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open raw wal db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatalf("disable autocheckpoint: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE demo(id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO demo(value) VALUES('from-wal')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := os.Stat(dbPath + "-wal"); err != nil {
		t.Fatalf("expected WAL sidecar before backup: %v", err)
	}
	if err := backupOnce(dbPath); err != nil {
		t.Fatalf("backupOnce: %v", err)
	}
	bak, err := sql.Open("sqlite", "file:"+dbPath+".bak?mode=ro")
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer func() { _ = bak.Close() }()
	var value string
	if err := bak.QueryRow(`SELECT value FROM demo WHERE id=1`).Scan(&value); err != nil {
		t.Fatalf("backup is missing WAL-only row: %v", err)
	}
	if value != "from-wal" {
		t.Fatalf("backup value = %q, want from-wal", value)
	}
}

func TestD7RecoverDumpExistsRefusesWipe(t *testing.T) {
	dataDir, dbPath := mustDataDir(t)
	seedDB(t, dbPath)
	dump := filepath.Join(t.TempDir(), "divergent.json")
	// Pre-create the dump file so O_EXCL fails — the dump must refuse, and the wipe
	// must NOT happen (the DB stays intact).
	if err := os.WriteFile(dump, []byte("prior"), 0o600); err != nil {
		t.Fatalf("pre-create dump: %v", err)
	}
	if _, err := Recover(RecoverOptions{DataDir: dataDir, DBPath: dbPath, DumpPath: dump}); err == nil {
		t.Fatal("recover overwrote a pre-existing dump (O_EXCL bypassed)")
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("tether.db was wiped despite the dump failing: %v", err)
	}
}
