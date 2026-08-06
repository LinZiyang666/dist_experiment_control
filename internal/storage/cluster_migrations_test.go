package storage

import (
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// D0 — cluster migrations 0008 / 0009 / 0010.
//
// White-box (package storage): these tests call the unexported migration engine
// (ApplyMigrations / applyOne / migrationsFS / migrationsTable) and the DSN
// helper (withForeignKeysPragma), following the internal/storage/*_test.go
// precedent (p13_generation_migration_test.go). They assert the §4.2/§18.3
// schema contract, NOT any Apply/runtime behavior (which lands in D1+).
// ---------------------------------------------------------------------------

// rawMemDB opens an in-memory SQLite WITHOUT running migrations, so a test can
// drive the migration engine to an arbitrary point (subset application). FK
// enforcement + single-conn serialization match storage.Open.
func rawMemDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", withForeignKeysPragma(":memory:"))
	if err != nil {
		t.Fatalf("open raw mem db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// migrationNames returns the sorted embedded migration filenames.
func migrationNames(t *testing.T) []string {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// applyThrough applies (and records) every migration up to and including
// lastInclusive, leaving later migrations un-applied. Used to reconstruct a
// pre-0008 production DB.
func applyThrough(t *testing.T, db *sql.DB, lastInclusive string) {
	t.Helper()
	if _, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS ` + migrationsTable +
			` (version TEXT PRIMARY KEY, applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);`,
	); err != nil {
		t.Fatalf("create %s: %v", migrationsTable, err)
	}
	for _, n := range migrationNames(t) {
		body, err := migrationsFS.ReadFile("migrations/" + n)
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		if err := applyOne(db, n, string(body)); err != nil {
			t.Fatalf("apply %s: %v", n, err)
		}
		if n == lastInclusive {
			return
		}
	}
	t.Fatalf("migration %q not found", lastInclusive)
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&n); err != nil {
		t.Fatalf("table existence %s: %v", name, err)
	}
	return n == 1
}

func tableDDL(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var ddl string
	if err := db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&ddl); err != nil {
		t.Fatalf("read DDL for %s: %v", name, err)
	}
	return ddl
}

// TestClusterMigrations_TablesCreated — the 4 new cluster tables exist after a
// full Open, and the pre-existing tables are untouched.
func TestClusterMigrations_TablesCreated(t *testing.T) {
	db := openTest(t)
	for _, tbl := range []string{"cluster_nodes", "cluster_meta", "alerts", "alert_acks"} {
		if !tableExists(t, db, tbl) {
			t.Errorf("expected table %q after migrations", tbl)
		}
	}
	// New indexes present.
	for _, idx := range []string{"idx_cluster_nodes_phase", "idx_alerts_dedup_active", "idx_port_alloc_home_active"} {
		var n int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, idx,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("expected index %q", idx)
		}
	}
}

// TestClusterMigrations_NoRevokedIdentitiesTable — §18.3 deleted
// cluster_revoked_identities (no writer/reader). It must NOT exist.
func TestClusterMigrations_NoRevokedIdentitiesTable(t *testing.T) {
	db := openTest(t)
	if tableExists(t, db, "cluster_revoked_identities") {
		t.Error("cluster_revoked_identities must NOT exist (§18.3: no writer/reader)")
	}
}

// TestClusterMigrations_NoCurrentTimestampDefaults — §4.2: cluster/alert tables
// carry NO CURRENT_TIMESTAMP default (leader bakes time literals for
// determinism). port_allocations.created_at (0001/0003) DOES, proving we don't
// accidentally over-normalize.
func TestClusterMigrations_NoCurrentTimestampDefaults(t *testing.T) {
	db := openTest(t)
	for _, tbl := range []string{"cluster_nodes", "cluster_meta", "alerts", "alert_acks"} {
		if ddl := tableDDL(t, db, tbl); strings.Contains(ddl, "CURRENT_TIMESTAMP") {
			t.Errorf("%s DDL must not contain CURRENT_TIMESTAMP (§4.2 determinism):\n%s", tbl, ddl)
		}
	}
	// Contrast: the pre-existing table legitimately keeps its default.
	if ddl := tableDDL(t, db, "port_allocations"); !strings.Contains(ddl, "CURRENT_TIMESTAMP") {
		t.Errorf("port_allocations.created_at should still default CURRENT_TIMESTAMP; DDL:\n%s", ddl)
	}
}

// insertClusterNode inserts a fully-specified cluster_nodes row (all NOT NULL
// columns present) with the given phase; returns the INSERT error.
func insertClusterNode(db *sql.DB, nodeID, name, phase string) error {
	_, err := db.Exec(
		`INSERT INTO cluster_nodes
		   (node_id, name, node_ident_pub, raft_addr, nats_route, tunnel_addr,
		    public_host, cert_fp, phase, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?, '2026-06-21T00:00:00Z')`,
		nodeID, name, "Nident", "127.0.0.1:7400", "127.0.0.1:6222",
		"host:7000", "host", "SHA256:fp", phase,
	)
	return err
}

// TestClusterNodes_PhaseCheck — the canonical ALL-UPPERCASE 6-value set is
// accepted; lowercase (the pre-normalization spelling) and bogus values are
// rejected.
func TestClusterNodes_PhaseCheck(t *testing.T) {
	db := openTest(t)
	accept := []string{
		"JOIN_VERIFIED_PENDING_VOTER", "CATCHING_UP", "VOTER",
		"VOTER_ADD_FAILED", "DRAINING", "RETIRING",
	}
	for i, p := range accept {
		if err := insertClusterNode(db, "ok-"+p, "name-ok-"+p, p); err != nil {
			t.Errorf("phase %q (#%d) should be accepted: %v", p, i, err)
		}
	}
	// Includes whitespace-padded near-misses — SQLite IN(...) is exact-match, so
	// ' VOTER'/'VOTER '/'VOTER\t' must be rejected; a future TRIM()-wrapped or
	// typo'd CHECK that let a padded value through would fail here.
	reject := []string{"draining", "retiring", "voter", "BOGUS", "", "Voter", " VOTER", "VOTER ", "VOTER\t"}
	for _, p := range reject {
		if err := insertClusterNode(db, "no-"+p, "name-no-"+p, p); err == nil {
			t.Errorf("phase %q should be rejected by CHECK", p)
		}
	}
}

// TestClusterNodes_NameUnique — UNIQUE(name) is enforced.
func TestClusterNodes_NameUnique(t *testing.T) {
	db := openTest(t)
	if err := insertClusterNode(db, "n1", "dup", "VOTER"); err != nil {
		t.Fatal(err)
	}
	if err := insertClusterNode(db, "n2", "dup", "VOTER"); err == nil {
		t.Error("duplicate name must violate UNIQUE(name)")
	}
}

// TestClusterNodes_AddedAtNoDefault — added_at is NOT NULL with NO default, so
// an INSERT omitting it must fail (values are leader-baked, §4.2).
func TestClusterNodes_AddedAtNoDefault(t *testing.T) {
	db := openTest(t)
	_, err := db.Exec(
		`INSERT INTO cluster_nodes
		   (node_id, name, node_ident_pub, raft_addr, nats_route, tunnel_addr,
		    public_host, cert_fp, phase)
		 VALUES ('x','x','i','a','r','t','h','f','VOTER')`,
	)
	if err == nil {
		t.Error("omitting added_at must fail (NOT NULL, no default — leader-baked)")
	}
}

func insertAlert(db *sql.DB, id, kind, severity, dedupKey, state string) error {
	_, err := db.Exec(
		`INSERT INTO alerts (id, kind, severity, dedup_key, state, message, raised_at)
		 VALUES (?,?,?,?,?,?, '2026-06-21T00:00:00Z')`,
		id, kind, severity, dedupKey, state, "msg",
	)
	return err
}

// TestAlerts_KindCheck — the 7 replication-store-backed kinds are accepted; the
// 2 client-synthesized severe gating kinds (quorum_lost/force_single_active) and
// any bogus kind are REJECTED (§10.4: no writer in the replicated store).
func TestAlerts_KindCheck(t *testing.T) {
	db := openTest(t)
	accept := []string{
		"manual", "below_quorum", "broker_draining", "broker_down",
		"replication_degraded", "disk_pressure", "raft_lag",
		"proxy_bind_stalled", // h1 E2 (0018 rebuild widened the CHECK)
	}
	for i, k := range accept {
		if err := insertAlert(db, "ok"+k, k, "info", "dk-ok-"+k, "ACTIVE"); err != nil {
			t.Errorf("kind %q (#%d) should be accepted: %v", k, i, err)
		}
	}
	reject := []string{"quorum_lost", "force_single_active", "not_a_kind", "", "Manual", " manual", "manual "}
	for _, k := range reject {
		if err := insertAlert(db, "no"+k, k, "info", "dk-no-"+k, "ACTIVE"); err == nil {
			t.Errorf("kind %q should be rejected (client-synthesized, unknown, or whitespace-padded)", k)
		}
	}
}

// TestAlerts_SeverityCheck — only {severe, info}.
func TestAlerts_SeverityCheck(t *testing.T) {
	db := openTest(t)
	if err := insertAlert(db, "s1", "manual", "severe", "dk1", "ACTIVE"); err != nil {
		t.Errorf("severity severe should be accepted: %v", err)
	}
	if err := insertAlert(db, "s2", "manual", "info", "dk2", "ACTIVE"); err != nil {
		t.Errorf("severity info should be accepted: %v", err)
	}
	if err := insertAlert(db, "s3", "manual", "critical", "dk3", "ACTIVE"); err == nil {
		t.Error("severity 'critical' must be rejected")
	}
}

// TestAlerts_DedupPartialUnique — at most one ACTIVE per dedup_key; an
// ACTIVE+CLEARED pair on the same key is allowed (history rows).
func TestAlerts_DedupPartialUnique(t *testing.T) {
	db := openTest(t)
	if err := insertAlert(db, "a1", "manual", "info", "same", "ACTIVE"); err != nil {
		t.Fatal(err)
	}
	if err := insertAlert(db, "a2", "manual", "info", "same", "ACTIVE"); err == nil {
		t.Error("a second ACTIVE alert with the same dedup_key must be rejected")
	}
	// A CLEARED row on the same key is fine (partial index only covers ACTIVE).
	if err := insertAlert(db, "a3", "manual", "info", "same", "CLEARED"); err != nil {
		t.Errorf("CLEARED row with same dedup_key should be allowed: %v", err)
	}
	// MULTIPLE CLEARED rows for the same dedup_key must ALL be allowed — the
	// partial WHERE state='ACTIVE' exists precisely to keep unbounded history
	// (mirrors 0003 port history). A regression dropping the partial WHERE
	// (making it unconditionally unique on dedup_key) would fail here only.
	if err := insertAlert(db, "a4", "manual", "info", "same", "CLEARED"); err != nil {
		t.Errorf("a second CLEARED row with same dedup_key must be allowed (partial index): %v", err)
	}
}

// TestAlertAcks_SingleClusterLevelAck — PK(dedup_key) alone: a second ack of the
// same dedup_key conflicts EVEN with a different acked_by (proves the ack is
// cluster-level, NOT per-identity — §18.3).
func TestAlertAcks_SingleClusterLevelAck(t *testing.T) {
	db := openTest(t)
	mustExec(t, db,
		`INSERT INTO alert_acks (dedup_key, acked_by, acked_at) VALUES (?,?, '2026-06-21T00:00:00Z')`,
		"dk", "fpA")
	_, err := db.Exec(
		`INSERT INTO alert_acks (dedup_key, acked_by, acked_at) VALUES (?,?, '2026-06-21T00:00:00Z')`,
		"dk", "fpB")
	if err == nil {
		t.Error("a second ack of the same dedup_key (even different acked_by) must conflict (cluster-level ack, §18.3)")
	}
}

// TestAlerts_RaisedAtNoDefault / TestAlertAcks_AckedAtNoDefault — leader-baked,
// no default backstop.
func TestAlerts_RaisedAtNoDefault(t *testing.T) {
	db := openTest(t)
	_, err := db.Exec(
		`INSERT INTO alerts (id, kind, severity, dedup_key, message) VALUES ('x','manual','info','dk','m')`)
	if err == nil {
		t.Error("omitting alerts.raised_at must fail (NOT NULL, no default)")
	}
}

func TestAlertAcks_AckedAtNoDefault(t *testing.T) {
	db := openTest(t)
	_, err := db.Exec(`INSERT INTO alert_acks (dedup_key, acked_by) VALUES ('dk','fp')`)
	if err == nil {
		t.Error("omitting alert_acks.acked_at must fail (NOT NULL, no default)")
	}
}

// seedPortRow inserts the FK parents (session + node) and one ALLOCATED
// port_allocations row using the PRE-0010 column set (no cluster columns).
func seedPortRow(t *testing.T, db *sql.DB) {
	t.Helper()
	mustExec(t, db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:o','ph')`)
	mustExec(t, db, `INSERT INTO nodes(sid, nid, status) VALUES ('lab','lab-1','ONLINE')`)
	mustExec(t, db,
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp)
		 VALUES (14022,'lab','lab-1','jupyter',8888,'deadbeef','ALLOCATED','SHA256:o')`)
}

// TestPortColumns_DefaultsOnFreshInsert — inserting a port row omitting the new
// columns yields the deterministic constant defaults (§7.3/§4.2).
func TestPortColumns_DefaultsOnFreshInsert(t *testing.T) {
	db := openTest(t)
	seedPortRow(t, db)
	var rof, epoch int
	var home string
	if err := db.QueryRow(
		`SELECT rebuild_on_failure, home_broker, epoch FROM port_allocations WHERE port=14022`,
	).Scan(&rof, &home, &epoch); err != nil {
		t.Fatal(err)
	}
	if rof != 1 || home != "" || epoch != 0 {
		t.Errorf("new-column defaults wrong: rebuild_on_failure=%d home_broker=%q epoch=%d (want 1, '', 0)", rof, home, epoch)
	}
}

// TestPortColumns_RebuildCheck — rebuild_on_failure ∈ {0,1}.
func TestPortColumns_RebuildCheck(t *testing.T) {
	db := openTest(t)
	seedPortRow(t, db)
	if _, err := db.Exec(`UPDATE port_allocations SET rebuild_on_failure=2 WHERE port=14022`); err == nil {
		t.Error("rebuild_on_failure=2 must violate CHECK(rebuild_on_failure IN (0,1))")
	}
}

// TestPortColumns_BackfillExistingRows is the TRUE production-upgrade path: a row
// that already existed at migration 0007 must be backfilled to (1,”,0) when
// 0008–0010 run. (TestPortColumns_DefaultsOnFreshInsert only exercises the
// insert-omitting-column path; this exercises ALTER ADD COLUMN ... DEFAULT
// rewriting pre-existing rows.)
func TestPortColumns_BackfillExistingRows(t *testing.T) {
	db := rawMemDB(t)
	applyThrough(t, db, "0007_proxy_generation.sql")

	// At 0007 the cluster columns do not exist yet — seed an old-shape row.
	seedPortRow(t, db)

	// Sanity: the new columns are absent at 0007.
	if _, err := db.Exec(`SELECT home_broker FROM port_allocations LIMIT 1`); err == nil {
		t.Fatal("home_broker must not exist before 0010")
	}

	// Apply the remaining migrations (0008–0010) via the real engine.
	if err := ApplyMigrations(db); err != nil {
		t.Fatalf("ApplyMigrations (0008-0010): %v", err)
	}

	var rof, epoch int
	var home string
	if err := db.QueryRow(
		`SELECT rebuild_on_failure, home_broker, epoch FROM port_allocations WHERE port=14022`,
	).Scan(&rof, &home, &epoch); err != nil {
		t.Fatal(err)
	}
	if rof != 1 || home != "" || epoch != 0 {
		t.Errorf("0010 ALTER must backfill the pre-existing row to (1,'',0); got (%d,%q,%d)", rof, home, epoch)
	}
}

// TestClusterMigrations_PreserveExistingSchema — applying 0008–0010 on a DB that
// already had port_allocations rows leaves the 0003 unique index + FK intact and
// the DB consistent (no silent corruption from ALTER).
func TestClusterMigrations_PreserveExistingSchema(t *testing.T) {
	db := openTest(t)
	seedPortRow(t, db)

	// 0003 partial-unique index still rejects a second ALLOCATED row for the same port.
	_, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp)
		 VALUES (14022,'lab','lab-1','dup',9999,'beef','ALLOCATED','SHA256:o')`)
	if err == nil {
		t.Error("idx_port_alloc_unique_active must still reject a 2nd ALLOCATED row for port 14022")
	}

	// foreign_key_check yields one row per violation; zero rows = clean.
	var fkProblems int
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		fkProblems++
	}
	_ = rows.Close()
	if fkProblems != 0 {
		t.Errorf("foreign_key_check reported %d problems after 0008-0010", fkProblems)
	}

	var ic string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&ic); err != nil {
		t.Fatal(err)
	}
	if ic != "ok" {
		t.Errorf("integrity_check = %q, want ok", ic)
	}
}

// TestClusterMigrations_EngineIdempotent — re-running the engine on a fully
// migrated DB is a no-op (cluster tables persist, recorded count unchanged). The
// new migrations are NOT IF-NOT-EXISTS, so this proves the engine (not the
// migration bodies) provides idempotency by recorded filename.
func TestClusterMigrations_EngineIdempotent(t *testing.T) {
	db := openTest(t)
	var before int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + migrationsTable).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(db); err != nil {
		t.Fatalf("second ApplyMigrations: %v", err)
	}
	var after int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + migrationsTable).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("re-apply changed migration count: before=%d after=%d", before, after)
	}
	for _, tbl := range []string{"cluster_nodes", "cluster_meta", "alerts", "alert_acks"} {
		if !tableExists(t, db, tbl) {
			t.Errorf("table %q vanished after re-apply", tbl)
		}
	}
}

// TestClusterMigrations_FkCascadeSurvivesAlter (m1) proves 0010's ALTER ADD
// COLUMN preserved the port_allocations → nodes FK ON DELETE CASCADE: deleting
// the parent node must cascade-delete the child port row. foreign_key_check
// (TestClusterMigrations_PreserveExistingSchema) only proves no dangling refs;
// it would stay clean even if the cascade ACTION had been lost in a rebuild.
func TestClusterMigrations_FkCascadeSurvivesAlter(t *testing.T) {
	db := openTest(t)
	seedPortRow(t, db)
	mustExec(t, db, `DELETE FROM nodes WHERE sid='lab' AND nid='lab-1'`)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE port=14022`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("FK ON DELETE CASCADE must remove the port row after the 0010 ALTER; got %d rows", n)
	}
}

// TestClusterMeta_KVConstraints (m2) pins the cluster_meta KV contract that D1's
// Apply upsert (applied_index/applied_term/bootstrapped) is load-bearing on, and
// that D0 is the only place to establish.
func TestClusterMeta_KVConstraints(t *testing.T) {
	db := openTest(t)
	// value NOT NULL.
	if _, err := db.Exec(`INSERT INTO cluster_meta(key, value) VALUES ('applied_index', NULL)`); err == nil {
		t.Error("cluster_meta.value=NULL must be rejected (NOT NULL)")
	}
	// key PRIMARY KEY uniqueness.
	mustExec(t, db, `INSERT INTO cluster_meta(key, value) VALUES ('applied_index', '0')`)
	if _, err := db.Exec(`INSERT INTO cluster_meta(key, value) VALUES ('applied_index', '1')`); err == nil {
		t.Error("duplicate cluster_meta.key must conflict (PRIMARY KEY)")
	}
	// Upsert round-trip — the shape D1's idempotent Apply will use.
	mustExec(t, db, `INSERT INTO cluster_meta(key, value) VALUES ('applied_index','5')
	                 ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
	var v string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "5" {
		t.Errorf("upsert should set applied_index=5, got %q", v)
	}
}

// TestPortColumns_HomeBrokerNotNull (m4) proves the 0010 default is ” (empty
// string), NOT NULL — Scan into a Go string would coerce NULL to "" and mask the
// bug, so assert via IS NULL. NULL vs ” changes the D6 home==self filter +
// partial-index equality semantics.
func TestPortColumns_HomeBrokerNotNull(t *testing.T) {
	db := openTest(t)
	seedPortRow(t, db)
	var nNull int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE home_broker IS NULL`).Scan(&nNull); err != nil {
		t.Fatal(err)
	}
	if nNull != 0 {
		t.Errorf("home_broker default must be '' not NULL; found %d NULL rows", nNull)
	}
}

// TestPortColumns_HomeIndexNonUnique (m5) guards against a stray CREATE UNIQUE
// INDEX on idx_port_alloc_home_active: two ALLOCATED rows on the same
// home_broker (different ports) must both succeed (per-port uniqueness is
// already held by 0003's idx_port_alloc_unique_active).
func TestPortColumns_HomeIndexNonUnique(t *testing.T) {
	db := openTest(t)
	mustExec(t, db, `INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES ('lab','lab','SHA256:o','ph')`)
	mustExec(t, db, `INSERT INTO nodes(sid,nid,status) VALUES ('lab','lab-1','ONLINE')`)
	mustExec(t, db, `INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,home_broker)
	                 VALUES (14022,'lab','lab-1','a',8888,'h1','ALLOCATED','SHA256:o','brokerX')`)
	if _, err := db.Exec(`INSERT INTO port_allocations(port,sid,nid,name,local_port,token_hash,state,created_by_fp,home_broker)
	                      VALUES (14023,'lab','lab-1','b',8889,'h2','ALLOCATED','SHA256:o','brokerX')`); err != nil {
		t.Errorf("two ALLOCATED rows on the same home_broker (different ports) must both succeed: %v", err)
	}
	var ddl string
	if err := db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_port_alloc_home_active'`,
	).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(ddl), "UNIQUE") {
		t.Errorf("idx_port_alloc_home_active must NOT be UNIQUE; DDL: %s", ddl)
	}
}

// TestClusterMigrations_MigrationOrderContiguous (n6) kills the hardcoded
// "0007_proxy_generation.sql" boundary drift in applyThrough: if a migration is
// ever inserted out of the expected 0001..0010 sequence, applyThrough would
// silently test a different pre-cluster baseline. Pin the exact set + order.
func TestClusterMigrations_MigrationOrderContiguous(t *testing.T) {
	want := []string{
		"0001_init.sql", "0002_agent_provisioning.sql", "0003_port_alloc_history.sql",
		"0004_token_hash_index.sql", "0005_processes_gc_indexes.sql", "0006_proxy.sql",
		"0007_proxy_generation.sql", "0008_cluster_nodes.sql",
		"0009_cluster_meta_alerts.sql", "0010_port_cluster_columns.sql",
		"0011_cluster_reqid_ledger.sql", "0012_nodes_nats_server.sql",
		"0013_cluster_nodes_join_pop.sql", "0014_cluster_nodes_bus_nkey.sql",
		"0015_cluster_operations.sql", "0016_proxy_cluster.sql",
		"0017_port_alloc_last_rehome.sql",
		"0018_alerts_kind_proxy_bind_stalled.sql",
	}
	got := migrationNames(t)
	if len(got) != len(want) {
		t.Fatalf("migration set drift: got %d %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("migration[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestClusterMigrations_FileRestartIdempotent (T16) covers the close/reopen path
// the in-memory engine-idempotency test misses: a file-backed DB reopened after
// 0008-0010 applied must not re-run any migration (recorded by filename).
func TestClusterMigrations_FileRestartIdempotent(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "state.db")
	db1, err := Open(dsn)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if !tableExists(t, db1, "cluster_nodes") {
		t.Error("cluster_nodes missing after first Open")
	}
	var before int
	if err := db1.QueryRow(`SELECT COUNT(*) FROM ` + migrationsTable).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_ = db1.Close()

	db2, err := Open(dsn) // reopen the SAME file
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	var after int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM ` + migrationsTable).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("reopen re-applied migrations: before=%d after=%d", before, after)
	}
}
