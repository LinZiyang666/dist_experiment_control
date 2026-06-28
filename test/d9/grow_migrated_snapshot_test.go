//go:build d9_integration

// grow_migrated_snapshot_test.go — pins the grow-onto-migrated-broker KEYSTONE (the bug the live
// pc732→racknerd grow hit + the 20-expert audit confirmed). `cluster init --from-existing` direct-
// seeds migrated rows into SQLite that no raft log entry created; without a snapshot a fresh joiner
// replays the leader's log from index 1 onto its own un-seeded DB and FK-fail-stops in a crash loop
// (or, FK-safe variant, becomes a hollow voter). The v0.4.3 SnapshotForJoin was a confirmed no-op
// (raft.Snapshot won't compact a log shorter than TrailingLogs=10240). The STEP-0 fix takes a full
// FSM snapshot + compacts the log at init so FirstIndex>1 → the joiner installs the snapshot.
//
// The pre-existing d9 grow tests use FRESH EMPTY DBs seeded only through the raft log, so the leader
// NEVER carries direct-seeded rows and this class is structurally unreachable there — exactly the
// test-coverage hole the audit flagged. This test closes it.
package d9_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

func TestD9GrowReadySnapshotAtInit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}
	_ = db.Close()

	secrets := writeD9Secrets(t, newD9CA(t))
	raftAddr := freeTCPAddr(t)
	if err := clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{
		DataDir: dir, DBPath: dbPath, SecretsDir: secrets,
		SelfID: "mig-a", Name: "mig-a", NodeIdentPub: "pub-mig-a",
		RaftAddr: raftAddr, NatsRoute: "127.0.0.1:6222",
		TunnelAddr: "127.0.0.1:7000", PublicHost: "localhost",
		Now: time.Now,
	}); err != nil {
		t.Fatalf("InitFromExisting: %v", err)
	}

	// KEYSTONE: init must leave a raft snapshot carrying the seeded DB, so a fresh joiner installs
	// it (InstallSnapshot → fsm.Restore) instead of replaying the log from index 1 and FK-crashing.
	exists, idx, _, err := cluster.RaftSnapshotMeta(dir)
	if err != nil {
		t.Fatalf("RaftSnapshotMeta: %v", err)
	}
	if !exists {
		t.Fatal("grow-ready KEYSTONE regressed: `cluster init --from-existing` left NO raft snapshot — " +
			"a fresh joiner would replay the log onto an un-seeded DB and FK-fail-stop (the v0.4.3 no-op). " +
			"InitFromExisting must call cluster.GrowReadySnapshot after BootstrapSingleNode.")
	}
	if idx < 1 {
		t.Fatalf("grow-ready snapshot index = %d, want >= 1 (must sit at/after the bootstrap config@1 so a "+
			"joiner whose nextIndex decays to 1 receives InstallSnapshot, not the truncated log)", idx)
	}
	// COMPACTION is the load-bearing half (v0.4.4 review): existence alone does NOT distinguish the real
	// fix from a v0.4.3-class no-op (raft.Snapshot writes a snapshot file but won't compact a log shorter
	// than TrailingLogs=10240 → FirstIndex stays 1 → the leader ships the LOG, joiner replays → FK crash).
	// RecoverCluster's UNCONDITIONAL DeleteRange empties the offline raft log, so a joiner whose nextIndex
	// decays past it hits ErrLogNotFound → InstallSnapshot. Assert the log is truly compacted (==0 offline).
	if last, lerr := cluster.RaftLastIndex(dir); lerr != nil {
		t.Fatalf("RaftLastIndex: %v", lerr)
	} else if last != 0 {
		t.Fatalf("grow-ready log NOT compacted: RaftLastIndex=%d, want 0 — init wrote a snapshot but left the "+
			"log un-truncated (the v0.4.3 no-op class); a joiner would replay the log, not InstallSnapshot. "+
			"GrowReadySnapshot must DeleteRange the log via raft.RecoverCluster.", last)
	}
}
