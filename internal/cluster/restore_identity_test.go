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
