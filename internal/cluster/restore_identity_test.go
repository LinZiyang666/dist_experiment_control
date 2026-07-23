package cluster

import (
	"bytes"
	"io"
	"testing"
)

// TestRestorePreservesLocalSelfNodeID pins the v0.4.4 grow fix: a raft InstallSnapshot copies the LEADER's
// DB byte-for-byte, and cluster_meta.self_node_id in it is the LEADER's id. A joiner that catches up by
// installing the leader's snapshot MUST keep ITS OWN id — otherwise the next restart's readSelfNodeID
// returns the leader's id and the joiner comes up as the leader (two nodes claiming one raft ServerID =
// split brain). This path had ZERO coverage: no prior test exercised a real InstallSnapshot (the d9
// 2-broker join aligned via low-index log replay, never installing the leader's snapshot).
func TestRestorePreservesLocalSelfNodeID(t *testing.T) {
	// SOURCE = a "leader" whose snapshot carries self_node_id='pc732-leader' + a replicated data row.
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "pc732-leader")
	// self_node_id is not a "t:"-prefixed test key, so seed it directly into the FSM write pool (it lives
	// in cluster_meta, which the online-backup snapshot captures — exactly as `cluster init` writes it).
	if _, err := a.db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','pc732-leader') ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		t.Fatalf("seed leader self_node_id: %v", err)
	}
	if err := a.ApplyMetaSet("t:grow", "leader-data"); err != nil {
		t.Fatalf("seed leader data: %v", err)
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	_, rc := openSnapshot(t, srcDir)
	snapBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("read snapshot: %v (len=%d)", err, len(snapBytes))
	}

	// TARGET = a "joiner" with its OWN identity. It installs the leader's snapshot.
	tf, tdb := freshFSM(t, t.TempDir())
	if _, err := tdb.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','racknerd-joiner')`); err != nil {
		t.Fatalf("seed joiner self_node_id: %v", err)
	}
	if err := tf.Restore(io.NopCloser(bytes.NewReader(snapBytes))); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// (1) IDENTITY PRESERVED — the joiner keeps its own id, NOT the leader's (the fix; RED before it).
	var self string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='self_node_id'`).Scan(&self); err != nil {
		t.Fatalf("read self_node_id: %v", err)
	}
	if self != "racknerd-joiner" {
		t.Fatalf("self_node_id=%q after InstallSnapshot — the joiner ADOPTED THE LEADER's identity "+
			"(split brain on next restart); must preserve 'racknerd-joiner'", self)
	}

	// (2) DATA TRANSFERRED — the joiner did receive the leader's replicated state (snapshot install worked).
	var data string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:grow'`).Scan(&data); err != nil || data != "leader-data" {
		t.Fatalf("joiner missing the leader's snapshot data: got %q err=%v", data, err)
	}
}

// TestRestoreStripsRestoreInProgressMarker pins R16 B1: `restore_in_progress` is a SOURCE-NODE-LOCAL
// recovery flag. A grow-ready snapshot is taken (correctly) while the source still carries it (restore.go's
// fail-closed crash window), so the snapshot DB has restore_in_progress='1'. A fresh joiner that
// InstallSnapshots it MUST NOT inherit the marker — otherwise its next restart FATALs
// (assertNoInterruptedRestore) on a restore it never ran. fsm.restoreFrom strips it on every install, like
// self_node_id. RED before the fix (the marker rode the snapshot into the joiner).
func TestRestoreStripsRestoreInProgressMarker(t *testing.T) {
	srcDir := t.TempDir()
	a := mustNode(t, srcDir, "restored-survivor")
	// The survivor's live DB carries the marker at snapshot time (grow-ready snapshot precedes the clear).
	if _, err := a.db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('restore_in_progress','1') ON CONFLICT(key) DO UPDATE SET value='1'`); err != nil {
		t.Fatalf("seed restore_in_progress: %v", err)
	}
	if err := a.ApplyMetaSet("t:grow", "survivor-data"); err != nil {
		t.Fatalf("seed data: %v", err)
	}
	if err := a.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	_, rc := openSnapshot(t, srcDir)
	snapBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("read snapshot: %v (len=%d)", err, len(snapBytes))
	}

	tf, tdb := freshFSM(t, t.TempDir())
	if _, err := tdb.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id','fresh-joiner')`); err != nil {
		t.Fatalf("seed joiner id: %v", err)
	}
	if err := tf.Restore(io.NopCloser(bytes.NewReader(snapBytes))); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The marker MUST be stripped on install (else the joiner bricks on its next restart).
	var n int
	if err := tdb.QueryRow(`SELECT COUNT(*) FROM cluster_meta WHERE key='restore_in_progress'`).Scan(&n); err != nil {
		t.Fatalf("count restore_in_progress: %v", err)
	}
	if n != 0 {
		t.Fatal("restore_in_progress rode the grow-ready snapshot into the joiner — it FATALs on next restart " +
			"(assertNoInterruptedRestore) on a restore it never ran; fsm.restoreFrom must strip it")
	}
	// The real data still transferred.
	var data string
	if err := tdb.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:grow'`).Scan(&data); err != nil || data != "survivor-data" {
		t.Fatalf("joiner missing the survivor's snapshot data: got %q err=%v", data, err)
	}
}
