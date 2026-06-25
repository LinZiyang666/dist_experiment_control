package clusteroffline_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// restore_test.go — B6 step4 units (make test): the applied_index reset + roster prune (the
// load-bearing correctness fix, asserted on the INSTALLED DB), the 4-layer provenance refusals,
// torn-bundle + live-daemon + .bak guards, the LIVE "write not swallowed" round-trip (a real raft
// node on a restored DB), and the backup-under-concurrent-write consistency proof (-race). The
// live round-trip exercises the real raft bring-up on a restored data dir; the full multi-broker
// DR drill (backup → destroy → restore → `cluster add` re-grow over NATS) is the operator drill in
// docs/cluster-runbook.md §5.2 (the raft mechanics it depends on are proven here).

// buildClusterBundle creates a source cluster DB (self row cert_fp == the secrets dir's live
// tunnel fp, applied_index=5000, two extra peer rows) and backs it up into a bundle dir.
// Returns the bundle dir, the secrets dir, and the self id.
func buildClusterBundle(t *testing.T) (bundleDir, secretsDir, selfID string) {
	t.Helper()
	root := t.TempDir()
	secretsDir = filepath.Join(root, "secrets")
	fp := writeSecrets(t, secretsDir)
	selfID = "node-A"

	srcDir := filepath.Join(root, "src")
	if err := os.MkdirAll(srcDir, 0o700); err != nil {
		t.Fatal(err)
	}
	srcDB := filepath.Join(srcDir, "tether.db")
	db, err := storage.OpenWAL("file:" + srcDB)
	if err != nil {
		t.Fatalf("open src db: %v", err)
	}
	insertNode := func(id, name, addr string) {
		if _, err := db.Exec(
			`INSERT INTO cluster_nodes
			 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig)
			 VALUES(?,?,?,?,?,?,?,?,?,'VOTER','2026-06-24 00:00:00 +0000 UTC',?,?)`,
			id, name, "Ident-"+id, id, addr, "nats://"+addr, addr, "h."+id, "sha256:cert-"+id, "NONCE-"+id, "SIG-"+id,
		); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	// self row: cert_fp MUST equal the live tunnel fp (provenance anchor).
	if _, err := db.Exec(
		`INSERT INTO cluster_nodes
		 (node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,join_nonce,join_sig)
		 VALUES(?,?,?,?,?,?,?,?,?,'VOTER','2026-06-24 00:00:00 +0000 UTC',?,?)`,
		selfID, "broker-A", "Ident-A", selfID, "10.0.0.1:7400", "nats://10.0.0.1:6222",
		"10.0.0.1:7443", "a.example", fp, "NONCE-A", "SIG-A",
	); err != nil {
		t.Fatalf("insert self: %v", err)
	}
	insertNode("node-B", "broker-B", "10.0.0.2:7400")
	insertNode("node-C", "broker-C", "10.0.0.3:7400")
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?)`, selfID); err != nil {
		t.Fatalf("seed self_node_id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index','5000')`); err != nil {
		t.Fatalf("seed applied_index: %v", err)
	}
	_ = db.Close()

	bundleDir = filepath.Join(root, "bundle")
	if _, err := clusteroffline.OfflineBackup(clusteroffline.BackupOptions{
		DataDir: srcDir, DBPath: srcDB, SecretsDir: secretsDir, OutDir: bundleDir,
		Now: func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}); err != nil {
		t.Fatalf("OfflineBackup: %v", err)
	}
	return bundleDir, secretsDir, selfID
}

// freshTarget returns an empty DataDir + DBPath for a restore destination (DR: nothing there).
func freshTarget(t *testing.T) (dataDir, dbPath string) {
	t.Helper()
	dataDir = t.TempDir()
	return dataDir, filepath.Join(dataDir, "tether.db")
}

func TestRestoreResetsAppliedIndexAndPrunesRoster(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)

	res, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		ConfirmNodeID: selfID, Now: func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("RestoreFromBackup: %v", err)
	}
	if res.BundleAppliedIdx != 5000 || res.PrunedPeers != 2 {
		t.Fatalf("result wrong: %+v", res)
	}

	// The installed DB: applied_index reset to 0 (the load-bearing fix) + only the self row.
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open installed: %v", err)
	}
	defer func() { _ = ro.Close() }()
	var idx string
	if err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&idx); err != nil {
		t.Fatalf("read applied_index: %v", err)
	}
	if idx != "0" {
		t.Fatalf("applied_index must be reset to 0 (else the FSM swallows post-restore writes), got %q", idx)
	}
	var n int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM cluster_nodes`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("roster must be pruned to {self}, got %d rows", n)
	}
	var keptID string
	if err := ro.QueryRow(`SELECT node_id FROM cluster_nodes`).Scan(&keptID); err != nil {
		t.Fatal(err)
	}
	if keptID != selfID {
		t.Fatalf("the kept roster row must be self, got %q", keptID)
	}

	// raft/ was wiped + freshly bootstrapped — HasExistingState true.
	assertRaftBootstrapped(t, dataDir)
}

// assertRaftBootstrapped opens the raft stores and asserts a config exists.
func assertRaftBootstrapped(t *testing.T, dataDir string) {
	t.Helper()
	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(dataDir, "raft", "raft.db")})
	if err != nil {
		t.Fatalf("open raft store: %v", err)
	}
	defer func() { _ = store.Close() }()
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), 2, nil)
	if err != nil {
		t.Fatalf("open snaps: %v", err)
	}
	has, err := raft.HasExistingState(store, store, snaps)
	if err != nil {
		t.Fatalf("HasExistingState: %v", err)
	}
	if !has {
		t.Fatal("restore must leave a bootstrapped raft state")
	}
}

func TestRestoreProvenanceRefusals(t *testing.T) {
	t.Run("wrong confirm-node-id", func(t *testing.T) {
		bundleDir, secretsDir, _ := buildClusterBundle(t)
		dataDir, dbPath := freshTarget(t)
		_, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
			BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
			ConfirmNodeID: "node-WRONG",
		})
		if err == nil {
			t.Fatal("must refuse a mismatched --confirm-node-id")
		}
		assertNoDBInstalled(t, dbPath)
	})

	t.Run("foreign secrets dir (cert-fp mismatch)", func(t *testing.T) {
		bundleDir, _, selfID := buildClusterBundle(t)
		// A DIFFERENT secrets dir → different tunnel fp → provenance refuse.
		foreign := filepath.Join(t.TempDir(), "foreign-secrets")
		writeSecrets(t, foreign)
		dataDir, dbPath := freshTarget(t)
		_, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
			BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: foreign,
			ConfirmNodeID: selfID,
		})
		if err == nil {
			t.Fatal("must refuse a foreign secrets dir (tunnel-cert fp mismatch)")
		}
		assertNoDBInstalled(t, dbPath)
	})

	t.Run("single-broker bundle refused", func(t *testing.T) {
		// A single-broker bundle (no self_node_id) must not restore as a cluster.
		root := t.TempDir()
		srcDB := filepath.Join(root, "tether.db")
		db, err := storage.OpenWAL("file:" + srcDB)
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		bundle := filepath.Join(root, "bundle")
		if _, err := clusteroffline.OfflineBackup(clusteroffline.BackupOptions{
			DataDir: root, DBPath: srcDB, OutDir: bundle,
		}); err != nil {
			t.Fatalf("backup: %v", err)
		}
		secretsDir := filepath.Join(root, "secrets")
		writeSecrets(t, secretsDir)
		dataDir, dbPath := freshTarget(t)
		_, err = clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
			BundleDir: bundle, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
			ConfirmNodeID: "node-A",
		})
		if err == nil {
			t.Fatal("must refuse to restore a single-broker bundle as a cluster")
		}
	})
}

func TestRestoreTornBundleRefused(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	// Corrupt the bundle's state.db so VerifyIntegrity rejects it (overwrite the header).
	statePath := filepath.Join(bundleDir, "state.db")
	if err := os.WriteFile(statePath, []byte("not a sqlite file at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir, dbPath := freshTarget(t)
	_, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		ConfirmNodeID: selfID,
	})
	if err == nil {
		t.Fatal("must refuse a torn bundle")
	}
	assertNoDBInstalled(t, dbPath)
}

func TestRestoreBakPreservesPriorDB(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)
	// A pre-existing live DB at the target → restore must .bak it before installing.
	pre, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pre.Exec(`INSERT INTO cluster_meta(key,value) VALUES('marker','PRIOR')`); err != nil {
		t.Fatal(err)
	}
	_ = pre.Close()

	res, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		ConfirmNodeID: selfID,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// External-review F4: the prior DB is preserved at a UNIQUE path (reported in the result), not
	// the shared `.bak` that a prior init/restore would have made backupOnce skip.
	if res.PreRestoreBackup == "" {
		t.Fatal("restore must report the pre-restore backup path for an existing prior DB")
	}
	bak, err := storage.OpenReadOnly("file:" + res.PreRestoreBackup)
	if err != nil {
		t.Fatalf("open pre-restore backup: %v", err)
	}
	defer func() { _ = bak.Close() }()
	var marker string
	if err := bak.QueryRow(`SELECT value FROM cluster_meta WHERE key='marker'`).Scan(&marker); err != nil {
		t.Fatalf("read pre-restore backup marker: %v", err)
	}
	if marker != "PRIOR" {
		t.Fatalf("pre-restore backup must preserve the prior DB, got marker %q", marker)
	}

	// External-review F4: a SECOND restore must still preserve (a unique name), not silently discard.
	res2, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir,
		ConfirmNodeID: selfID,
	})
	if err != nil {
		t.Fatalf("second restore: %v", err)
	}
	if res2.PreRestoreBackup == "" || res2.PreRestoreBackup == res.PreRestoreBackup {
		t.Fatalf("a second restore must preserve under a DISTINCT path, got %q (first %q)", res2.PreRestoreBackup, res.PreRestoreBackup)
	}
}

// TestRestoreIndexDurabilityLiveNode is the MANDATORY load-bearing proof: a bundle carrying
// applied_index=5000 is restored, a REAL single-voter raft node is brought up on the restored
// data dir, and an authoritative write is proposed. The write MUST land in the DB — it would be
// SILENTLY SWALLOWED (FSM Apply skips l.Index <= applied) if restore had not reset applied_index
// to 0. The node uses an inmem transport at the bundle's raft_addr (the address the restore's
// BootstrapSingleNode recorded), no NATS — so it runs in make test.
func TestRestoreIndexDurabilityLiveNode(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t) // self raft_addr == 10.0.0.1:7400, applied_index 5000
	dataDir, dbPath := freshTarget(t)
	const raftAddr = "10.0.0.1:7400"

	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err != nil {
		t.Fatalf("RestoreFromBackup: %v", err)
	}

	// Bring up a real raft node on the restored data dir (inmem transport at the recorded addr).
	_, trans := raft.NewInmemTransport(raft.ServerAddress(raftAddr))
	n, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID(selfID), DataDir: dataDir, DBPath: dbPath,
		Transport: trans, ApplyTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("cluster.New on restored data dir: %v", err)
	}
	defer func() { _ = n.Shutdown() }()
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader after restore: %v", err)
	}

	// An authoritative write AFTER restore. If applied_index were still 5000, the FSM would skip
	// this (l.Index <= applied) and the row would never appear.
	if err := n.ApplyMetaSet("t:restore-probe", "applied-after-restore"); err != nil {
		t.Fatalf("ApplyMetaSet after restore: %v", err)
	}
	var got string
	if err := n.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key='t:restore-probe'`).Scan(&got); err != nil {
		t.Fatalf("post-restore write was SWALLOWED (applied_index not reset?): %v", err)
	}
	if got != "applied-after-restore" {
		t.Fatalf("post-restore write wrong value: %q", got)
	}
}

// TestBackupUnderConcurrentWrite proves the ONLINE paged backup (Node.BackupDBTo, off the RO
// handle) produces a CONSISTENT point-in-time copy while the FSM is actively writing — no torn
// state. Bring up a node, hammer it with authoritative writes in a goroutine, take a backup
// mid-flight, then verify the backup's integrity + FK consistency. Run with -race.
func TestBackupUnderConcurrentWrite(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "state.db")
	const raftAddr = "10.9.9.9:7400"
	const selfID = "node-W"

	// Seed + bootstrap a single voter directly (no bundle needed for this test).
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err := cluster.BootstrapSingleNode(dataDir, selfID, raftAddr, nil); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	_, trans := raft.NewInmemTransport(raft.ServerAddress(raftAddr))
	n, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID(selfID), DataDir: dataDir, DBPath: dbPath, Transport: trans, ApplyTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("cluster.New: %v", err)
	}
	defer func() { _ = n.Shutdown() }()
	if err := n.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}

	// Concurrent writer: keep advancing the FSM while the backup runs.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = n.ApplyMetaSet("t:w", time.Duration(i).String())
			i++
		}
	}()

	// Let some writes land, then back up mid-flight.
	time.Sleep(50 * time.Millisecond)
	statePath := filepath.Join(t.TempDir(), "snap.db")
	if err := n.BackupDBTo(context.Background(), statePath); err != nil {
		close(stop)
		<-done
		t.Fatalf("BackupDBTo under load: %v", err)
	}
	close(stop)
	<-done

	// The mid-flight backup must be a consistent DB (integrity_check + foreign_key_check pass).
	if err := cluster.VerifyIntegrity(statePath); err != nil {
		t.Fatalf("backup taken under concurrent writes is torn: %v", err)
	}
	// And it must be openable + readable (a real DB, not a half page).
	ro, err := storage.OpenReadOnly("file:" + statePath)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer func() { _ = ro.Close() }()
	var nMeta int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM cluster_meta`).Scan(&nMeta); err != nil {
		t.Fatalf("read backup cluster_meta: %v", err)
	}
}

func assertNoDBInstalled(t *testing.T, dbPath string) {
	t.Helper()
	if _, err := os.Stat(dbPath); err == nil {
		t.Fatal("a refused restore must NOT install a DB over the target")
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected stat err: %v", err)
	}
}

// metaVal reads a cluster_meta value from a DB path (RO).
func metaVal(t *testing.T, dbPath, key string) string {
	t.Helper()
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer func() { _ = ro.Close() }()
	var v string
	_ = ro.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&v)
	return v
}

// TestRestoreOrphanStagingWalNotApplied — Stage-C BLOCKER-1: a stale restore-staging.db-wal from a
// kill-9'd prior normalize must NOT be layered over the fresh bundle copy on re-run.
func TestRestoreOrphanStagingWalNotApplied(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)
	// Plant a poisoned orphan staging .db + -wal carrying a STALE marker (simulating a kill-9'd run).
	stage := filepath.Join(dataDir, "restore-staging.db")
	poison, err := storage.OpenWAL("file:" + stage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := poison.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:stale','POISON')`); err != nil {
		t.Fatal(err)
	}
	_ = poison.Close() // leaves restore-staging.db + (likely) -wal/-shm sidecars on disk

	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err != nil {
		t.Fatalf("RestoreFromBackup: %v", err)
	}
	// The installed DB must be the BUNDLE content — the poison marker must NOT be present.
	if v := metaVal(t, dbPath, "t:stale"); v != "" {
		t.Fatalf("stale orphan-wal content leaked into the restored DB: %q", v)
	}
	// No staging sidecars left behind.
	for _, sfx := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(stage + sfx); err == nil {
			t.Fatalf("staging file %q must be cleaned up", stage+sfx)
		}
	}
}

// TestRestorePostMigrationCorruptionNotInstalled — Stage-C BLOCKER-2: if the staging DB is
// corrupted AFTER the forward-migration, the post-migration verify must refuse + leave the live DB
// untouched (the verify-before-swap ordering).
func TestRestorePostMigrationCorruptionNotInstalled(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)
	// A pre-existing live DB at the target so we can prove it is NOT overwritten.
	pre, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pre.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:marker','LIVE')`); err != nil {
		t.Fatal(err)
	}
	_ = pre.Close()

	// Corrupt the migrated staging DB so the post-migration verify fails.
	clusteroffline.SetRestoreAfterMigrateHook(func(stagePath string) {
		if werr := os.WriteFile(stagePath, []byte("CORRUPT-NOT-SQLITE"), 0o600); werr != nil {
			t.Fatalf("corrupt staging: %v", werr)
		}
	})
	defer clusteroffline.SetRestoreAfterMigrateHook(nil)

	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err == nil {
		t.Fatal("restore must refuse when the post-migration verify fails")
	}
	// The live DB is untouched (still holds the LIVE marker) — never overwritten by the corrupt staging.
	if v := metaVal(t, dbPath, "t:marker"); v != "LIVE" {
		t.Fatalf("live DB was overwritten despite a failed post-migration verify (marker=%q)", v)
	}
}

// TestRestoreReStampsSelfNodeID — Stage-C MAJOR-3: a bundle whose cluster_meta.self_node_id drifted
// must be re-stamped to m.SelfID so the daemon's raft LocalID matches the bootstrapped config.
func TestRestoreReStampsSelfNodeID(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	// Edit the bundle's state.db self_node_id to a FOREIGN value (the gate doesn't read it).
	statePath := filepath.Join(bundleDir, "state.db")
	sdb, err := storage.OpenWAL("file:" + statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Exec(`UPDATE cluster_meta SET value='foreign-node' WHERE key='self_node_id'`); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close()

	dataDir, dbPath := freshTarget(t)
	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if v := metaVal(t, dbPath, "self_node_id"); v != selfID {
		t.Fatalf("self_node_id must be re-stamped to %q, got %q", selfID, v)
	}
}

// TestRestoreReHomesAllocatedPorts — Stage-C MAJOR-4: ALLOCATED ports homed on (now-pruned) peers
// must be re-homed to self so they remain servable on the restored sole owner.
func TestRestoreReHomesAllocatedPorts(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	// Seed the FK chain (session → node → ALLOCATED port homed on a peer restore will prune).
	statePath := filepath.Join(bundleDir, "state.db")
	sdb, err := storage.OpenWAL("file:" + statePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('s1','sess','fp','h')`,
		`INSERT INTO nodes(sid,nid) VALUES('s1','n1')`,
		`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,home_broker,epoch)
		 VALUES(14022,'s1','n1','svc',8080,'th','ALLOCATED','cfp','node-B',7)`,
	} {
		if _, err := sdb.Exec(stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	_ = sdb.Close()

	dataDir, dbPath := freshTarget(t)
	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	ro, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	var home string
	var epoch int
	if err := ro.QueryRow(`SELECT home_broker, epoch FROM port_allocations WHERE port=14022`).Scan(&home, &epoch); err != nil {
		t.Fatalf("read port: %v", err)
	}
	if home != selfID {
		t.Fatalf("ALLOCATED port must be re-homed to self %q, got %q", selfID, home)
	}
	if epoch != 8 {
		t.Fatalf("epoch must be bumped (7->8) so agents re-pin, got %d", epoch)
	}
}

// TestRestoreRefusesLiveDaemon — Stage-C MAJOR-7: restore must refuse while a daemon holds the DB
// write lock (the single-WAL-owner interlock).
func TestRestoreRefusesLiveDaemon(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)
	// Create + hold a write lock on the target DB (a live daemon).
	hold, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hold.Close() }()
	tx, err := hold.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('t:held','1')`); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err == nil {
		t.Fatal("restore must refuse while a daemon holds the DB write lock")
	}
}

// TestRestoreReRunIsIdempotent — Stage-C MAJOR-7: restoring twice leaves a clean single-voter DB
// with no stale staging files.
func TestRestoreReRunIsIdempotent(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	dataDir, dbPath := freshTarget(t)
	opts := clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}
	if _, err := clusteroffline.RestoreFromBackup(opts); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	if _, err := clusteroffline.RestoreFromBackup(opts); err != nil {
		t.Fatalf("second restore (idempotent): %v", err)
	}
	if v := metaVal(t, dbPath, "applied_index"); v != "0" {
		t.Fatalf("applied_index must stay 0 after re-run, got %q", v)
	}
	for _, sfx := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(filepath.Join(dataDir, "restore-staging.db"+sfx)); err == nil {
			t.Fatalf("no stale staging file may remain: restore-staging.db%s", sfx)
		}
	}
}

// TestRestoreClearsAuditAndStateMarkers — Audit DI-MAJOR-1/2: restore resets audit_published_index,
// clears force_single_active, and clears restore_in_progress after success.
func TestRestoreClearsAuditAndStateMarkers(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	// Seed the bundle's state.db with a stale audit cursor + a force_single marker.
	statePath := filepath.Join(bundleDir, "state.db")
	sdb, err := storage.OpenWAL("file:" + statePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`INSERT INTO cluster_meta(key,value) VALUES('audit_published_index','5000') ON CONFLICT(key) DO UPDATE SET value='5000'`,
		`INSERT INTO cluster_meta(key,value) VALUES('force_single_active','2026-06-24T00:00:00Z') ON CONFLICT(key) DO UPDATE SET value='x'`,
	} {
		if _, err := sdb.Exec(stmt); err != nil {
			t.Fatalf("seed marker: %v", err)
		}
	}
	_ = sdb.Close()

	dataDir, dbPath := freshTarget(t)
	if _, err := clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if v := metaVal(t, dbPath, "audit_published_index"); v != "0" {
		t.Fatalf("audit_published_index must be reset to 0, got %q", v)
	}
	if v := metaVal(t, dbPath, "force_single_active"); v != "" {
		t.Fatalf("force_single_active must be cleared, got %q", v)
	}
	if v := metaVal(t, dbPath, "restore_in_progress"); v != "" {
		t.Fatalf("restore_in_progress must be cleared after a successful restore, got %q", v)
	}
}

// TestRestoreRefusesAppliedIndexMismatch — External-review F5: a manifest whose applied_index does
// not match the bundle state.db's cursor (a newer manifest spliced onto an older state.db) is refused.
func TestRestoreRefusesAppliedIndexMismatch(t *testing.T) {
	bundleDir, secretsDir, selfID := buildClusterBundle(t)
	// Tamper ONLY the manifest's applied_index (state.db still says 5000).
	mPath := filepath.Join(bundleDir, "manifest.json")
	m, err := clusteroffline.ReadManifest(mPath)
	if err != nil {
		t.Fatal(err)
	}
	m.AppliedIndex = 99999 // splice a newer cursor onto the older state.db
	_ = os.Remove(mPath)   // WriteManifest is O_EXCL; replace the original
	if err := clusteroffline.WriteManifest(mPath, m); err != nil {
		t.Fatal(err)
	}
	dataDir, dbPath := freshTarget(t)
	_, err = clusteroffline.RestoreFromBackup(clusteroffline.RestoreOptions{
		BundleDir: bundleDir, DataDir: dataDir, DBPath: dbPath, SecretsDir: secretsDir, ConfirmNodeID: selfID,
	})
	if err == nil {
		t.Fatal("restore must refuse a manifest/state.db applied_index mismatch")
	}
	if !strings.Contains(err.Error(), "applied_index") {
		t.Fatalf("error should name the applied_index mismatch, got %v", err)
	}
	assertNoDBInstalled(t, dbPath)
}
