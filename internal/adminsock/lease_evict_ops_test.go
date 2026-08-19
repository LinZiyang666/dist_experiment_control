package adminsock

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// `tether admin evict <sid> <lease-name>` is the one lever an operator reaches
// for when `node ls` grows a row they did not create. It answers OK, it deletes
// the row, it CASCADE-deletes that instance's entire port and process history —
// and it stops nothing, because every agent's agent_evicted matcher compares the
// broadcast nid against a.cfg.NID (the agent.yaml BASENAME), which a lease name
// never equals.
func TestEvictingALeaseNameReportsSuccessAndDestroysHistoryWithoutStoppingAnything(t *testing.T) {
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:o", "h"); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	// The basename holder, provisioned the ordinary way.
	if _, err := db.Exec(
		`INSERT INTO nodes(sid,nid,status,last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "jupyter", "ONLINE", time.Now().UTC()); err != nil {
		t.Fatalf("seed holder: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES (?,?,?,?)`,
		"lab", "jupyter", "SHA256:image-cred", time.Now().UTC()); err != nil {
		t.Fatalf("seed prov: %v", err)
	}
	// The leased clone: a nodes row created by the broker, NO provisioning row
	// (D3 — the credential is bound to the basename), plus the history an
	// operator would expect an evict to preserve.
	if _, err := db.Exec(
		`INSERT INTO nodes(sid,nid,status,last_heartbeat_at) VALUES (?,?,?,?)`,
		"lab", "jupyter-02", "ONLINE", time.Now().UTC()); err != nil {
		t.Fatalf("seed lease row: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp) VALUES (?,?,?,?,?,?,?)`,
		"p1", "lab", "jupyter-02", `["train.py"]`, time.Now().UTC(), "RUNNING", "SHA256:actor"); err != nil {
		t.Fatalf("seed proc: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(sid,nid,name,port,local_port,token_hash,state,created_at,created_by_fp) `+
			`VALUES (?,?,?,?,?,?,?,?,?)`,
		"lab", "jupyter-02", "web", 14001, 8888, "hash1", "ALLOCATED", time.Now().UTC(), "SHA256:actor"); err != nil {
		t.Fatalf("seed port: %v", err)
	}

	var broadcastNID string
	s := New("/unused", Backend{
		DB:              db,
		PubAgentEvicted: func(_, nid string) { broadcastNID = nid },
	})
	resp := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "jupyter-02"})

	// REWRITTEN by the main process from a characterization of the old behaviour
	// into a guard on the adjudicated one (external review F10 + F15).
	//
	// The old behaviour — reported ok, deleted the node/process/port rows, and
	// changed nothing about the running instance — was the finding, so a green
	// test asserting it would have taught the next reader that it was intended.
	//
	// ADJUDICATION: refuse. Evict REVOKES A CREDENTIAL, and a lease name has
	// none: it owns no provisioning row, and the agent_evicted broadcast is
	// matched against an agent's CONFIGURED name, so a running leased instance
	// never hears it. Doing the destructive half of an operation whose effective
	// half cannot apply is strictly worse than declining.
	if resp.OK {
		t.Fatalf("evicting a lease name must be REFUSED, not reported ok: %+v", resp.Evict)
	}
	if resp.Code != CodeBadRequest {
		t.Fatalf("refusal must be a bad_request the operator can act on; got code=%q", resp.Code)
	}
	if !strings.Contains(resp.Error, "jupyter") {
		t.Fatalf("the refusal must name the basename to use instead; got %q", resp.Error)
	}
	if broadcastNID != "" {
		t.Fatalf("a refused evict must broadcast nothing; broadcast nid=%q", broadcastNID)
	}
	// NOTHING may have been destroyed.
	var procs, ports, nodes int
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE sid='lab' AND nid='jupyter-02'`).Scan(&procs)
	_ = db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE sid='lab' AND nid='jupyter-02'`).Scan(&ports)
	_ = db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid='jupyter-02'`).Scan(&nodes)
	if procs != 1 || ports != 1 || nodes != 1 {
		t.Fatalf("a refused evict destroyed live bookkeeping anyway: procs=%d ports=%d nodes=%d "+
			"(want 1/1/1). An operator following the OFFLINE-cleanup advice in usage §5.18 would "+
			"silently lose a running instance's history.", procs, ports, nodes)
	}

	// The BASENAME remains evictable — that is the coherent operation, and the
	// refusal above points at it.
	if r := s.handleEvict(Request{Op: OpEvict, SID: "lab", NID: "jupyter"}); !r.OK {
		t.Fatalf("evicting the basename (the whole clone family's credential) must still work: %+v", r)
	}
}
