package storage_test

import (
	"database/sql"
	"testing"
)

// mustExec runs INSERT/DELETE statements that are expected to succeed
// in the FK / cascade tests below. We accept *sql.DB directly (no
// interface gymnastics) — every caller in this file uses the concrete
// type. Errors fail-fast because they indicate the test setup is wrong,
// not the SUT.
func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// C11 — Cascade DELETE through every dependent table when a session is
// removed. Mirrors architecture H.3 (session rm 三阶段) but at the
// FK-engine level: even if the broker code forgot to run the explicit
// child DELETEs, the cascading FKs in the migration files must finish
// the job. Verifies the foreign_keys=ON pragma C11 fix is live.
func TestC11_SessionRmCascadesEverything(t *testing.T) {
	db := openTestDB(t)
	const sid, nid = "lab", "lab-1"

	mustExec(t, db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		sid, sid, "SHA256:owner", "$argon2id$pin")
	mustExec(t, db, `INSERT INTO members(sid, pubkey_fp, role, via) VALUES (?,?,?,?)`,
		sid, "SHA256:owner", "owner", "create")
	mustExec(t, db, `INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`, sid, nid, "ONLINE")
	mustExec(t, db, `INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		sid, nid, "SHA256:agent")
	mustExec(t, db, `INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		VALUES (?,?,?,?,CURRENT_TIMESTAMP,?,?)`,
		"01hzx", sid, nid, `["sleep","1"]`, "RUNNING", "SHA256:owner")
	mustExec(t, db, `INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp)
		VALUES (?,?,?,?,?,?,?,?)`,
		14022, sid, nid, "jupyter", 8888, "deadbeef", "ALLOCATED", "SHA256:owner")

	mustExec(t, db, `DELETE FROM sessions WHERE sid=?`, sid)

	for _, q := range []struct {
		label, query string
	}{
		{"members", `SELECT COUNT(*) FROM members WHERE sid=?`},
		{"nodes", `SELECT COUNT(*) FROM nodes WHERE sid=?`},
		{"processes", `SELECT COUNT(*) FROM processes WHERE sid=?`},
		{"port_allocations", `SELECT COUNT(*) FROM port_allocations WHERE sid=?`},
		{"agent_provisioning", `SELECT COUNT(*) FROM agent_provisioning WHERE sid=?`},
	} {
		var n int
		if err := db.QueryRow(q.query, sid).Scan(&n); err != nil {
			t.Fatalf("%s count: %v", q.label, err)
		}
		if n != 0 {
			t.Errorf("%s rows survived session DELETE: count=%d", q.label, n)
		}
	}
}

// C12 — process referencing a non-existent (sid, nid) is rejected.
// Pins the composite-FK on processes(sid, nid) → nodes(sid, nid).
// If foreign_keys is silently OFF (regression of C11), this test
// passes the INSERT instead of failing it — caught by the err==nil
// branch.
func TestC12_ProcessRequiresNodeRow(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:o", "phc")
	_, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES (?,?,?,?,CURRENT_TIMESTAMP,?,?)`,
		"01hzx", "lab", "lab-ghost", `["x"]`, "RUNNING", "SHA256:o",
	)
	if err == nil {
		t.Fatal("expected FK violation inserting process for missing node")
	}
}

// C13 — port_allocation referencing a non-existent (sid, nid) is
// rejected.
func TestC13_PortAllocRequiresNodeRow(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:o", "phc")
	_, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp)
		 VALUES (?,?,?,?,?,?,?,?)`,
		14001, "lab", "lab-ghost", "x", 1234, "hash", "ALLOCATED", "SHA256:o",
	)
	if err == nil {
		t.Fatal("expected FK violation inserting port_allocation for missing node")
	}
}

// C14 — agent_provisioning shares the cascade chain with nodes via
// sessions. Verify that a session DELETE removes BOTH agent_provisioning
// rows AND the nodes rows in one transaction.
func TestC14_AgentProvisioningCascadeIsConsistent(t *testing.T) {
	db := openTestDB(t)
	const sid = "evict"
	mustExec(t, db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		sid, sid, "SHA256:o", "phc")
	mustExec(t, db, `INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`, sid, "n1", "ONLINE")
	mustExec(t, db, `INSERT INTO nodes(sid, nid, status) VALUES (?,?,?)`, sid, "n2", "ONLINE")
	mustExec(t, db, `INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		sid, "n1", "SHA256:a1")
	mustExec(t, db, `INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		sid, "n2", "SHA256:a2")

	mustExec(t, db, `DELETE FROM sessions WHERE sid=?`, sid)

	for _, tbl := range []string{"nodes", "agent_provisioning"} {
		var n int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM "+tbl+" WHERE sid=?", sid,
		).Scan(&n); err != nil {
			t.Fatalf("%s count: %v", tbl, err)
		}
		if n != 0 {
			t.Errorf("%s: %d rows survived session DELETE", tbl, n)
		}
	}
}
