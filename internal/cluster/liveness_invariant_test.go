package cluster

import (
	"database/sql"
	"testing"
)

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestRestore_ResetsLivenessBaseline proves §3.5 NON-vacuously: a node that was
// ONLINE / proxy_ready=1 / heartbeat-set in the snapshot comes back from restore
// reset to the baseline (OFFLINE / 0 / NULL), while its IDENTITY row survives.
// (The online-backup page-copy carries the stale liveness values along, so the
// reset must be unconditional.)
func TestRestore_ResetsLivenessBaseline(t *testing.T) {
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "a")

	// Seed a session + an ONLINE node directly (test setup; not via the FSM —
	// real node mutators migrate in D2).
	mustExec(t, a.db, `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES('s1','sess','fp','hash')`)
	mustExec(t, a.db, `INSERT INTO nodes(sid, nid, status, proxy_ready, last_heartbeat_at, proto_version)
	                   VALUES('s1','n1','ONLINE',1,'2026-01-01 00:00:00',2)`)
	// raft needs at least one committed log entry to take a snapshot; the
	// online-backup captures the whole DB file (incl. the seeded rows) regardless.
	if err := a.ApplyMetaSet("t:trigger", "1"); err != nil {
		t.Fatalf("trigger op: %v", err)
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	_, rc := openSnapshot(t, srcDir)
	tf, tdb := freshFSM(t, t.TempDir())
	if err := tf.Restore(rc); err != nil {
		t.Fatalf("restore: %v", err)
	}

	var (
		status     string
		proxyReady int
		hb         sql.NullString
	)
	if err := tdb.QueryRow(
		`SELECT status, proxy_ready, last_heartbeat_at FROM nodes WHERE sid='s1' AND nid='n1'`,
	).Scan(&status, &proxyReady, &hb); err != nil {
		t.Fatalf("read restored node: %v", err)
	}
	if status != "OFFLINE" || proxyReady != 0 || hb.Valid {
		t.Fatalf("restore must reset liveness to baseline: got status=%q proxy_ready=%d hb_valid=%v, want OFFLINE/0/NULL",
			status, proxyReady, hb.Valid)
	}

	// Identity must survive — only liveness columns are reset.
	var n int
	if err := tdb.QueryRow(`SELECT count(*) FROM nodes WHERE sid='s1' AND nid='n1'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("node identity row must survive restore, got %d rows", n)
	}
}
