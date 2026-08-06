package broker

// proc_event_ack_test.go — the h1 C3 ACK contract of handleProcEvent: settled
// events ack OK (recorded / already_recorded / already_exited / unknown_pid),
// transients ack OK:false (node_missing / store_error), duplicates never
// double-publish audit, and a reply-less publish (a pre-h1 agent) stays the
// byte-identical silent path.
// origin: docs/reviews/h1-plan.md workstream C3 (2026-08-04 incident).

import (
	"database/sql"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type procAckHarness struct {
	b      *Broker
	db     *sql.DB
	sub    *nats.Subscription // captures the event msg (broker side)
	nc     *nats.Conn         // agent side
	audits *atomic.Int32
}

func newProcAckHarness(t *testing.T) *procAckHarness {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats not ready")
	}

	db := testharness.OpenDB(t)
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('lab','n1','ONLINE')`); err != nil {
		t.Fatal(err)
	}

	b := &Broker{}
	b.cfg.DB = db
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now

	audits := &atomic.Int32{}
	auditTapForTest = func(string, []byte) { audits.Add(1) }
	t.Cleanup(func() { auditTapForTest = nil })

	brokerNC, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(brokerNC.Close)
	sub, err := brokerNC.SubscribeSync(proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.*")
	if err != nil {
		t.Fatal(err)
	}
	_ = brokerNC.Flush()

	agentNC, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agentNC.Close)

	return &procAckHarness{b: b, db: db, sub: sub, nc: agentNC, audits: audits}
}

// roundTrip publishes one event (with or without a reply inbox), hands the
// captured msg to handleProcEvent, and returns the decoded ack (nil when
// withReply=false).
func (h *procAckHarness) roundTrip(t *testing.T, pid, kind string, body any, withReply bool) *proto.ProcEventAck {
	t.Helper()
	payload, _ := json.Marshal(body)
	subj := proto.SubjEvProc("lab", "n1", pid, kind)
	var inbox *nats.Subscription
	if withReply {
		var err error
		inbox, err = h.nc.SubscribeSync("ack.inbox." + pid + "." + kind)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.nc.PublishRequest(subj, "ack.inbox."+pid+"."+kind, payload); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := h.nc.Publish(subj, payload); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.nc.Flush()
	msg, err := h.sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.b.handleProcEvent(msg)
	if !withReply {
		return nil
	}
	ackMsg, err := inbox.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no ack arrived for %s/%s: %v", pid, kind, err)
	}
	var ack proto.ProcEventAck
	if err := json.Unmarshal(ackMsg.Data, &ack); err != nil {
		t.Fatal(err)
	}
	return &ack
}

func TestProcEventAckLifecycle(t *testing.T) {
	h := newProcAckHarness(t)
	started := proto.ProcStartedEvent{PID: "p1", Argv: []string{"x"}, StartedAt: time.Now().UTC(), StartedByFP: "SHA256:u"}
	exit := proto.ProcExitEvent{PID: "p1", ExitCode: 5, EndedAt: time.Now().UTC()}

	// started → recorded, one audit.
	if ack := h.roundTrip(t, "p1", "started", started, true); !ack.OK || ack.Code != "recorded" {
		t.Fatalf("started ack = %+v, want OK/recorded", ack)
	}
	if n := h.audits.Load(); n != 1 {
		t.Fatalf("audit count after started = %d, want 1", n)
	}
	// duplicate started (a courier retry whose ack was lost) → already_recorded,
	// NO second audit — the dedup pre-read is exactly what prevents a retry
	// from double-writing audit.proc{start}.
	if ack := h.roundTrip(t, "p1", "started", started, true); !ack.OK || ack.Code != "already_recorded" {
		t.Fatalf("dup started ack = %+v, want OK/already_recorded", ack)
	}
	if n := h.audits.Load(); n != 1 {
		t.Fatalf("audit count after dup started = %d, want still 1", n)
	}

	// exit → recorded with the real rc, one more audit.
	if ack := h.roundTrip(t, "p1", "exit", exit, true); !ack.OK || ack.Code != "recorded" {
		t.Fatalf("exit ack = %+v, want OK/recorded", ack)
	}
	var status string
	var rc int
	if err := h.db.QueryRow(`SELECT status, exit_code FROM processes WHERE pid='p1'`).Scan(&status, &rc); err != nil {
		t.Fatal(err)
	}
	if status != "EXITED" || rc != 5 {
		t.Fatalf("row after exit: %s/%d, want EXITED/5", status, rc)
	}
	if n := h.audits.Load(); n != 2 {
		t.Fatalf("audit count after exit = %d, want 2", n)
	}
	// duplicate exit → already_exited, no third audit.
	if ack := h.roundTrip(t, "p1", "exit", exit, true); !ack.OK || ack.Code != "already_exited" {
		t.Fatalf("dup exit ack = %+v, want OK/already_exited", ack)
	}
	if n := h.audits.Load(); n != 2 {
		t.Fatalf("audit count after dup exit = %d, want still 2", n)
	}
}

func TestProcEventAckTerminalAndTransientCodes(t *testing.T) {
	h := newProcAckHarness(t)

	// unknown pid on the COMMITTED (single-mode = leader) view → terminal
	// OK/unknown_pid: the courier must stop retrying a GC'd/never-known pid.
	exit := proto.ProcExitEvent{PID: "ghost", ExitCode: 0, EndedAt: time.Now().UTC()}
	if ack := h.roundTrip(t, "ghost", "exit", exit, true); !ack.OK || ack.Code != "unknown_pid" {
		t.Fatalf("ghost exit ack = %+v, want OK/unknown_pid", ack)
	}

	// started for a node with no row → TRANSIENT OK:false/node_missing (the
	// node row lands with the next register; dropping the entry here would
	// lose the start and orphan-kill the process at that register).
	started := proto.ProcStartedEvent{PID: "p2", Argv: []string{"x"}, StartedAt: time.Now().UTC()}
	subj := proto.SubjEvProc("lab", "ghost-node", "p2", "started")
	payload, _ := json.Marshal(started)
	inbox, err := h.nc.SubscribeSync("ack.inbox.node-missing")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.nc.PublishRequest(subj, "ack.inbox.node-missing", payload); err != nil {
		t.Fatal(err)
	}
	_ = h.nc.Flush()
	msg, err := h.sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h.b.handleProcEvent(msg)
	ackMsg, err := inbox.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ack proto.ProcEventAck
	_ = json.Unmarshal(ackMsg.Data, &ack)
	if ack.OK || ack.Code != "node_missing" {
		t.Fatalf("ghost-node started ack = %+v, want !OK/node_missing", ack)
	}
	if n := h.audits.Load(); n != 0 {
		t.Fatalf("failed writes must not audit: count=%d", n)
	}
}

// TestProcEventNoReplyIsOldWirePath: a pre-h1 agent PUBLISHES (no reply
// inbox). The write must land exactly as before and nothing may be sent back.
func TestProcEventNoReplyIsOldWirePath(t *testing.T) {
	h := newProcAckHarness(t)
	started := proto.ProcStartedEvent{PID: "p3", Argv: []string{"x"}, StartedAt: time.Now().UTC()}
	if ack := h.roundTrip(t, "p3", "started", started, false); ack != nil {
		t.Fatal("no-reply publish must not produce an ack")
	}
	var status string
	if err := h.db.QueryRow(`SELECT status FROM processes WHERE pid='p3'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("old-wire started row = %s, want RUNNING", status)
	}
}

// origin: h1 external review F1
// A register reply is also the proc courier's replay acknowledgement. If an
// exited snapshot row cannot be committed, the response must not make that
// PID indistinguishable from an already-settled exit: the agent clears every
// exit absent from AcceptedProcesses. ReconciledProcesses (or a failed whole
// register) therefore has to carry the write outcome.
func TestRegisterReconcileFailureCannotLookSettledToExitCourier(t *testing.T) {
	db := testharness.OpenDB(t)
	if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','SHA256:o','h')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO nodes(sid,nid,status) VALUES('lab','n1','ONLINE')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid,sid,nid,argv,started_at,status,started_by_fp)
		 VALUES('p1','lab','n1','["x"]',?,'RUNNING','SHA256:u')`, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	// Deterministic transient-write injection: reads still work, but the
	// register's RUNNING -> EXITED transition fails exactly where a Raft/DB
	// error would. reconcileOnRegister currently logs and continues.
	if _, err := db.Exec(`CREATE TRIGGER fail_proc_exit BEFORE UPDATE ON processes
		WHEN OLD.pid='p1' BEGIN SELECT RAISE(FAIL, 'injected exit write failure'); END`); err != nil {
		t.Fatal(err)
	}
	b := &Broker{}
	b.cfg.DB = db
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now
	rc := 23
	accepted, reconciled, _, _, _ := b.reconcileOnRegister("lab", "n1", proto.NodeRegisterReq{
		LocalProcesses: []proto.LocalProcess{{PID: "p1", State: "exited", RC: &rc}},
	})

	var status string
	if err := db.QueryRow(`SELECT status FROM processes WHERE pid='p1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("failure injection did not hold the row RUNNING: %s", status)
	}
	settled := false
	for _, r := range reconciled {
		if r.PID == "p1" {
			settled = true
		}
	}
	keptForRetry := false
	for _, pid := range accepted {
		if pid == "p1" {
			keptForRetry = true
		}
	}
	if !settled && !keptForRetry {
		t.Fatalf("failed exit is absent from both response sets: accepted=%v reconciled=%v; "+
			"the agent interprets absence from AcceptedProcesses as settled and deletes the only pending real-rc event", accepted, reconciled)
	}
}

// A failed process LIST is more dangerous than a failed exit UPDATE: if the
// empty result is treated as authoritative, exited snapshots disappear from
// the courier acknowledgement while live snapshots are ordered killed as
// "orphans". Fail closed until a later register can read committed state.
func TestRegisterReconcileReadFailureRetainsExitAndIssuesNoDirectives(t *testing.T) {
	db := testharness.OpenDB(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	b := &Broker{}
	b.cfg.DB = db
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now
	rc := 23
	accepted, reconciled, keepPorts, revokePorts, dropProcesses := b.reconcileOnRegister("lab", "n1", proto.NodeRegisterReq{
		LocalProcesses: []proto.LocalProcess{
			{PID: "finished", State: "exited", RC: &rc},
			{PID: "live", State: "running"},
		},
		LocalPorts: []proto.LocalPort{{Port: 30001, TokenHash: "unknown"}},
	})

	if len(accepted) != 1 || accepted[0] != "finished" {
		t.Fatalf("read failure accepted=%v, want only exited snapshot retained for courier retry", accepted)
	}
	if len(reconciled) != 0 || len(keepPorts) != 0 || len(revokePorts) != 0 || len(dropProcesses) != 0 {
		t.Fatalf("read failure issued unproved directives: reconciled=%v keep=%v revoke=%v drop=%v",
			reconciled, keepPorts, revokePorts, dropProcesses)
	}
}
