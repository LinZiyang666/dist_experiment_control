package broker

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// origin: b7_ops_test.go (renamed in B6) — B7 DOC#2: the membership-op derivation maps each phase to a
// kind/state/resume. See docs/reviews/b7-plan.md and docs/reviews/b7-review.md.
func TestOpFromPhase(t *testing.T) {
	cases := []struct {
		phase, addErr       string
		wantKind, wantState string
		wantResume          bool
	}{
		{"VOTER", "", "add", "done", false},
		{"CATCHING_UP", "", "add", "in_progress", false},
		{"CATCHING_UP", "catch_up_stalled", "add", "stalled", true},
		{"VOTER_ADD_FAILED", "addvoter timeout", "add", "failed", true},
		{"DRAINING", "", "drain", "draining", true},
		{"RETIRING", "", "retire", "retiring", true},
		{"JOIN_VERIFIED_PENDING_VOTER", "", "add", "in_progress", true},
	}
	for _, c := range cases {
		e := opFromPhase("brk-b", c.phase, "2026-06-24", "2026-06-25", c.addErr)
		if e.Kind != c.wantKind || e.State != c.wantState {
			t.Errorf("phase %q → kind=%q state=%q, want %q/%q", c.phase, e.Kind, e.State, c.wantKind, c.wantState)
		}
		if (e.Resume != "") != c.wantResume {
			t.Errorf("phase %q resume presence = %v, want %v", c.phase, e.Resume != "", c.wantResume)
		}
		if c.addErr != "" && e.LastError != c.addErr {
			t.Errorf("phase %q last_error = %q, want %q", c.phase, e.LastError, c.addErr)
		}
	}
}

// TestDeriveClusterOpsRoundTrip — Stage-C: derive over seeded cluster_nodes; scope to one node;
// empty DB → nil; and the m3 boundary (a DELETEd retired node yields no entry).
func TestDeriveClusterOpsRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tether.db")
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ins := func(id, phase, addErr string) {
		if _, err := db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at,phase_changed_at,voter_add_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, id, "p", id, "10.0.0.1:7400", "n", "t", "h", "c", phase, "2026-06-24 00:00:00 +0000 UTC", "2026-06-25 00:00:00 +0000 UTC", addErr); err != nil {
			t.Fatalf("ins %s: %v", id, err)
		}
	}
	ins("a", "VOTER", "")
	ins("b", "CATCHING_UP", "catch_up_stalled")
	ins("c", "DRAINING", "")

	ops, err := deriveClusterOps(db, "")
	if err != nil || len(ops) != 3 {
		t.Fatalf("want 3 ops, got %d (%v)", len(ops), err)
	}
	if ops[1].NodeID != "b" || ops[1].State != "stalled" || ops[1].Resume == "" {
		t.Fatalf("stalled catch-up mapping wrong: %+v", ops[1])
	}
	// non-add op (DRAINING) StartedAt is the phase_changed_at, not added_at (m11).
	if ops[2].NodeID != "c" || ops[2].StartedAt != "2026-06-25 00:00:00 +0000 UTC" {
		t.Fatalf("drain StartedAt should be phase_changed_at: %+v", ops[2])
	}
	// scope to one node.
	one, _ := deriveClusterOps(db, "b")
	if len(one) != 1 || one[0].NodeID != "b" {
		t.Fatalf("scoped derive wrong: %+v", one)
	}
	// m3 boundary: a DELETEd (retired) node yields no entry.
	if _, err := db.Exec(`DELETE FROM cluster_nodes WHERE node_id='c'`); err != nil {
		t.Fatal(err)
	}
	after, _ := deriveClusterOps(db, "c")
	if len(after) != 0 {
		t.Fatalf("a retired (DELETEd) node must yield NO ops entry (the m3 boundary): %+v", after)
	}
}
