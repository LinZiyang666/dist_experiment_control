package adminsock

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
)

// evict_failclosed_test.go (batch B, B3 — plan §15.2 "adminsock.Backend.DB / authcallout.Handler.DB")
//
// handleEvict had the same defect as the authcallout PIN seams: `EvictWrite != nil` was doing
// double duty as BOTH the raft seam AND the cluster-mode flag, so a clustered broker that failed to
// wire the seam fell through to the single-mode direct tx against the read-only FSM handle.
//
// The un-loud half is the one these tests exist for. If this package is ever handed the FSM WRITE
// pool rather than the read-only one, that direct tx SUCCEEDS: agent_provisioning/nodes rows are
// deleted on ONE broker, outside raft, with no error on any surface. So the assertions below check
// the ROWS, not just the reply — a reply-only assertion cannot tell "refused" from "wrote, then
// reported an error".

// evictFixture builds a server whose DB really can be written, with the two rows an evict would
// delete already present. A DB that could not be written would make every assertion below pass for
// the wrong reason.
func evictFixture(t *testing.T, clusterMode bool) (*Server, *Backend, *bytes.Buffer) {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('lab','lab','SHA256:o','$argon2id$fake')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES ('lab','lab-1','SHA256:a')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid, nid) VALUES ('lab','lab-1')`); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	be := &Backend{
		DB:          db,
		ClusterMode: clusterMode,
		Logger:      slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	return &Server{backend: *be}, be, logs
}

func evictRowsPresent(t *testing.T, s *Server) (prov, node bool) {
	t.Helper()
	return s.rowExists(`SELECT 1 FROM agent_provisioning WHERE sid=? AND nid=?`, "lab", "lab-1"),
		s.rowExists(`SELECT 1 FROM nodes WHERE sid=? AND nid=?`, "lab", "lab-1")
}

// TestEvictFailsClosedWhenClusteredWithoutSeam is the load-bearing assertion.
func TestEvictFailsClosedWhenClusteredWithoutSeam(t *testing.T) {
	s, _, _ := evictFixture(t, true)

	resp := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"})
	if resp.OK {
		t.Fatal("a clustered adminsock with no EvictWrite seam reported OK — it took the " +
			"single-mode direct tx, which in production writes the read-only FSM handle and, with " +
			"a write handle, deletes rows outside raft")
	}
	if resp.Error == "" {
		t.Fatal("refusal carried no error text; the operator would see an empty failure")
	}
	prov, node := evictRowsPresent(t, s)
	if !prov || !node {
		t.Fatalf("rows were DELETED despite the refusal (agent_provisioning present=%v, nodes "+
			"present=%v). The direct tx ran: the error reply was not the reason the evict failed, "+
			"and in a real cluster those deletes are invisible to raft.", prov, node)
	}
}

// TestEvictDirectTxStillRunsInSingleMode is the fleet half. racknerd is single-broker: there
// ClusterMode is false, EvictWrite is nil, and the direct tx is the CORRECT path. It asserts the
// rows are actually gone, not merely that OK came back.
func TestEvictDirectTxStillRunsInSingleMode(t *testing.T) {
	s, _, _ := evictFixture(t, false)

	resp := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"})
	if !resp.OK {
		t.Fatalf("single-mode evict failed: %v. The direct tx must remain the path when "+
			"ClusterMode is false — every broker in the fleet is single-mode today.", resp.Error)
	}
	if resp.Evict == nil || !resp.Evict.AgentProvDeleted || !resp.Evict.NodeRowDeleted {
		t.Fatalf("single-mode evict must report both deletes, got %+v", resp.Evict)
	}
	if prov, node := evictRowsPresent(t, s); prov || node {
		t.Fatalf("single-mode evict reported success but rows remain (prov=%v node=%v)", prov, node)
	}
}

// TestEvictSeamTakesPrecedenceOverClusterMode proves the guard fires only when the seam is MISSING.
// A guard written as "ClusterMode ⇒ refuse" would break every clustered evict while the fail-closed
// test above stayed green.
func TestEvictSeamTakesPrecedenceOverClusterMode(t *testing.T) {
	s, _, _ := evictFixture(t, true)
	calls := 0
	s.backend.EvictWrite = func(sid, nid string) error { calls++; return nil }

	resp := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"})
	if !resp.OK {
		t.Fatalf("a wired EvictWrite seam must be used in cluster mode, got %v", resp.Error)
	}
	if calls != 1 {
		t.Fatalf("EvictWrite called %d times, want 1 — ClusterMode short-circuited a seam that WAS "+
			"wired, disabling clustered evict entirely", calls)
	}
	// The seam is a stub here, so the rows survive; what matters is that the DIRECT tx did not run.
	if prov, node := evictRowsPresent(t, s); !prov || !node {
		t.Fatal("the direct tx ran even though the seam was wired — the raft path and the local " +
			"path both executed, which double-applies the evict")
	}
}

// TestEvictRefusalIsDetailedOnTheWire is the deliberate ASYMMETRY with authcallout's twin, and it is
// stated as a test so a future "make the two consistent" cleanup has to argue with it.
//
// authcallout's ErrSeamNotWired text is opaque because it reaches a client that has not
// authenticated. This reply travels a root-owned unix socket to the operator who typed the command,
// so there is no one to disclose to, and an opaque message would make a broker wiring bug
// indistinguishable from "no such agent".
func TestEvictRefusalIsDetailedOnTheWire(t *testing.T) {
	s, _, logs := evictFixture(t, true)
	resp := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"})

	for _, want := range []string{"clustered", "wiring bug"} {
		if !strings.Contains(resp.Error, want) {
			t.Errorf("the operator-facing refusal must contain %q so a wiring bug is not mistaken "+
				"for a missing agent; got %q", want, resp.Error)
		}
	}
	if !strings.Contains(logs.String(), "EvictWrite") {
		t.Errorf("the refusal must also name the seam in the broker log, got: %s", logs.String())
	}
}

// TestEvictFailClosedIsNotVacuous pins that the fixture reaches the guard at all. Every assertion
// above would also hold if handleEvict rejected the request earlier (a missing sid, a schema
// mismatch), so this establishes the same arguments SUCCEED with one field flipped.
func TestEvictFailClosedIsNotVacuous(t *testing.T) {
	single, _, _ := evictFixture(t, false)
	if resp := single.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"}); !resp.OK {
		t.Fatalf("the fixture's request never reaches the seam decision (single mode failed with "+
			"%q), so the fail-closed assertions could be passing on an earlier rejection", resp.Error)
	}
	clustered, _, _ := evictFixture(t, true)
	if resp := clustered.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "lab-1"}); resp.OK {
		t.Fatal("flipping ClusterMode did not change the outcome")
	}
}
