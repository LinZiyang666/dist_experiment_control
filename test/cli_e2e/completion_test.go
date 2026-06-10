// Broker-backed end-to-end tests for `tether __complete` dynamic
// completion. Implementation review Medium #3 demanded these to
// validate ONLINE-only node candidates, owner-only session filtering,
// ALLOCATED-only expose names, and the High #1 stale-SID + Medium #2
// admin-socket-timeout fixes.
//
// Same in-process anonymous-NATS pattern as the rest of cli_e2e (no
// auth_callout — that path is pinned in test/p3). We stand up a
// broker + agent, then drive the cli completion helpers directly
// against the broker. Cobra `__complete` dispatch wiring is already
// covered by cmd/tether/completion_test.go; what's NEW here is
// verifying real broker-side responses produce the right candidate
// sets.

package cli_e2e_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// newCompletionTransportForTest builds a natsTransport against the given
// broker URL with a synthetic identity. The anonymous NATS test
// harness ignores the nkey-name connection options, so the test
// exercises the Transport's request/response logic without paying for
// callout setup. Exposed via cli.NewTestNATSTransport.
func newCompletionTransport(url, actorPub, sid string) cli.Transport {
	return cli.NewTestNATSTransport(url, actorPub, sid)
}

// TestCompletion_E2E_OnlineNodesFiltered verifies that
// CompleteOnlineNodes returns ONLY nodes whose status is ONLINE.
// The broker auto-promotes the live agent to ONLINE; we additionally
// seed a STALE row to confirm the filter drops it.
func TestCompletion_E2E_OnlineNodesFiltered(t *testing.T) {
	cli.ClearCompletionCacheForTest()
	t.Cleanup(cli.ClearCompletionCacheForTest)

	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "live-1")()
	testharness.WaitNodeOnline(t, db, "lab", "live-1", 3*time.Second)

	if _, err := db.Exec(
		`INSERT INTO nodes (sid, nid, status, last_heartbeat_at, boot_id, release_version, proto_version)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"lab", "ghost-stale", "STALE", time.Now().Add(-1*time.Hour).UTC(),
		"", "0.0.0", 1,
	); err != nil {
		t.Fatalf("seed stale node: %v", err)
	}

	tr := newCompletionTransport(url, pub, "lab")
	defer tr.Close()

	cctx := cli.CompletionContext{NATSURL: url, ActorPubKey: pub, SID: "lab"}
	cands, _ := cli.CompleteOnlineNodes(tr, cctx, "")
	if len(cands) != 1 || cands[0] != "live-1" {
		t.Fatalf("got %v, want [live-1] (STALE node should be filtered)", cands)
	}
}

// TestCompletion_E2E_OwnedSessionsOnly seeds two sessions — one owned
// by the test actor, one owned by a different actor — and verifies
// CompleteOwnedSessions returns only the owner-owned ACTIVE session,
// while CompleteVisibleSessions surfaces both.
func TestCompletion_E2E_OwnedSessionsOnly(t *testing.T) {
	cli.ClearCompletionCacheForTest()
	t.Cleanup(cli.ClearCompletionCacheForTest)

	url := startNATS(t)
	db := openDB(t)
	mePub, meFp := freshUserPub(t)
	_, otherFp := freshUserPub(t)

	seedSession(t, db, "mine", meFp)
	seedSession(t, db, "theirs", otherFp)
	defer startBroker(t, url, db)()

	// Add `me` as a member of `theirs` so it's "visible" but not "owned".
	if err := session.AddMember(db, "theirs", meFp, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatalf("AddMember(theirs): %v", err)
	}

	tr := newCompletionTransport(url, mePub, "")
	defer tr.Close()

	cctx := cli.CompletionContext{NATSURL: url, ActorPubKey: mePub}
	owned, _ := cli.CompleteOwnedSessions(tr, cctx, "")
	if len(owned) != 1 || owned[0] != "mine" {
		t.Errorf("owned: got %v, want [mine]", owned)
	}

	cli.ClearCompletionCacheForTest()
	visible, _ := cli.CompleteVisibleSessions(tr, cctx, "")
	if len(visible) != 2 {
		t.Errorf("visible: got %v, want 2 (mine + theirs)", visible)
	}
}

// TestCompletion_E2E_StaleSIDDoesNotBlockSessionList — the High #1
// fix. Even when ~/.tether/current_session points at a deleted /
// nonexistent sid, `login -s` and `session rm` completion must still
// surface visible / owned sessions because ListSessions uses
// CtlNameUnactivated, not CtlNameForSession(stale-sid).
func TestCompletion_E2E_StaleSIDDoesNotBlockSessionList(t *testing.T) {
	cli.ClearCompletionCacheForTest()
	t.Cleanup(cli.ClearCompletionCacheForTest)

	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "real", fp)
	defer startBroker(t, url, db)()

	// Stale current_session: "ghost-deleted" doesn't exist.
	tr := newCompletionTransport(url, pub, "ghost-deleted")
	defer tr.Close()

	cctx := cli.CompletionContext{NATSURL: url, ActorPubKey: pub, SID: "ghost-deleted"}
	visible, _ := cli.CompleteVisibleSessions(tr, cctx, "")
	if len(visible) != 1 || visible[0] != "real" {
		t.Fatalf("visible with stale SID: got %v, want [real] — High #1 regression?", visible)
	}

	cli.ClearCompletionCacheForTest()
	owned, _ := cli.CompleteOwnedSessions(tr, cctx, "")
	if len(owned) != 1 || owned[0] != "real" {
		t.Fatalf("owned with stale SID: got %v, want [real]", owned)
	}
}

// TestCompletion_E2E_AllocatedExposeNamesFiltered seeds one ALLOCATED
// + one FREED port_allocations row and verifies the FREED one is
// filtered out from CompleteAllocatedExposeNames.
func TestCompletion_E2E_AllocatedExposeNamesFiltered(t *testing.T) {
	cli.ClearCompletionCacheForTest()
	t.Cleanup(cli.ClearCompletionCacheForTest)

	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	now := time.Now().UTC()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed port_allocations: %v", err)
		}
	}
	// Seed the nodes row first; port_allocations has FK to nodes(sid,nid).
	mustExec(`INSERT INTO nodes
		(sid, nid, status, last_heartbeat_at, boot_id, release_version, proto_version)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"lab", "a100", "ONLINE", now, "", "0.1.3", 1)
	mustExec(`INSERT INTO port_allocations
		(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		14000, "lab", "a100", "live-web", 8080, "hash-a", "ALLOCATED", fp, now)
	mustExec(`INSERT INTO port_allocations
		(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		14001, "lab", "a100", "old-freed", 9090, "hash-b", "FREED", fp, now)

	tr := newCompletionTransport(url, pub, "lab")
	defer tr.Close()

	cctx := cli.CompletionContext{NATSURL: url, ActorPubKey: pub, SID: "lab"}

	all, _ := cli.CompleteAllocatedExposeNames(tr, cctx, "", "")
	if len(all) != 1 || all[0] != "live-web" {
		t.Errorf("no nid filter: got %v, want [live-web]", all)
	}

	cli.ClearCompletionCacheForTest()
	a100Only, _ := cli.CompleteAllocatedExposeNames(tr, cctx, "a100", "")
	if len(a100Only) != 1 || a100Only[0] != "live-web" {
		t.Errorf("nid=a100: got %v, want [live-web]", a100Only)
	}

	cli.ClearCompletionCacheForTest()
	otherNid, _ := cli.CompleteAllocatedExposeNames(tr, cctx, "different-node", "")
	if len(otherNid) != 0 {
		t.Errorf("nid filter excludes other nodes: got %v, want []", otherNid)
	}
}

// TestCompletion_E2E_AdminSocketTimeout — Medium #2 fix. A Unix
// socket that accepts connections but never replies must NOT block
// completion past CompletionBudget. Default adminsock client timeout
// is 5s; completion client should override to CompletionBudget (1s).
func TestCompletion_E2E_AdminSocketTimeout(t *testing.T) {
	cli.ClearCompletionCacheForTest()
	t.Cleanup(cli.ClearCompletionCacheForTest)

	// Short temp dir, not t.TempDir(): the test name would push the
	// socket path past macOS's 104-byte sun_path limit (bind EINVAL).
	sockDir, err := os.MkdirTemp("", "tsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sockPath := filepath.Join(sockDir, "hang.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	stopAccept := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				<-stopAccept
			}(conn)
		}
	}()
	t.Cleanup(func() {
		close(stopAccept)
		_ = ln.Close()
	})

	start := time.Now()
	cands, _ := cli.CompleteAdminSessions(sockPath, "")
	elapsed := time.Since(start)

	if len(cands) != 0 {
		t.Errorf("expected empty candidates on hang, got %v", cands)
	}
	if elapsed > cli.CompletionBudget+250*time.Millisecond {
		t.Errorf("admin completion blocked for %s, exceeds budget %s (Medium #2 regression?)",
			elapsed, cli.CompletionBudget)
	}
	if elapsed < 800*time.Millisecond {
		t.Logf("admin completion returned in %s (faster than expected — verify stub setup)", elapsed)
	}

	// Symmetric check for CompleteAdminNodes.
	start = time.Now()
	cands, _ = cli.CompleteAdminNodes(sockPath, "", "")
	elapsed = time.Since(start)
	if len(cands) != 0 {
		t.Errorf("admin nodes on hang: expected empty, got %v", cands)
	}
	if elapsed > cli.CompletionBudget+250*time.Millisecond {
		t.Errorf("admin nodes blocked for %s, exceeds budget %s",
			elapsed, cli.CompletionBudget)
	}
}
