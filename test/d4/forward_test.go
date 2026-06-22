package d4_test

import (
	"database/sql"
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
)

func provisioned(t *testing.T, dbPath, sid, nid string) bool {
	t.Helper()
	db := openReadHandle(t, dbPath)
	_, err := agentprov.Lookup(db, sid, nid)
	return err == nil
}

// TestD4PINForwardedFollowerReachesLeader (§13.7 #1 happy path): a PIN provision whose
// auth_callout ran on a FOLLOWER is transparently forwarded to the leader over the
// real cluster.apply bus, committed through real mTLS raft, and the handler ALLOWS —
// the agent never sees not_leader. The row replicates to BOTH node DBs.
func TestD4PINForwardedFollowerReachesLeader(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li

	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	clientPub, _ := freshClient(t)

	h := &authcallout.Handler{DB: openReadHandle(t, c.dbPaths[fi]), AccountKp: c.acctKp, Now: time.Now}
	h.ProvisionAgentWrite = broker.NewProvisionSeam(c.nodes[fi], c.forwarder(fi))

	out, err := h.Handle(signedAgentReq(t, clientPub, "lab", "lab-1", "test-pin"))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if resp := mustDecodeResp(t, out); resp.Error != "" {
		t.Fatalf("a PIN provision forwarded follower->leader must ALLOW (agent never sees not_leader); got %q", resp.Error)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisioned(t, pth, "lab", "lab-1") }) {
			t.Fatalf("the forwarded provision must commit through raft and replicate to node %d", i)
		}
	}
}

// TestD4PINForwardNoLeaderTransientDeny (§13.7 #1 no-false-allow): with the leader
// killed (2-node => the survivor is a minority, no leader), a follower's forwarded PIN
// times out -> ErrForwardNotLeader -> authcallout.ErrNotLeader -> the handler DENIES
// (transient, classified not_leader). NO row is written on any replica (never a false
// allow under quorum loss).
func TestD4PINForwardNoLeaderTransientDeny(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li

	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	// Kill the leader node => the survivor cannot win an election (minority).
	_ = c.nodes[li].Shutdown()
	c.nodes[li] = nil
	if !waitForCond(3*time.Second, func() bool { return !c.nodes[fi].IsLeader() }) {
		t.Fatal("survivor must not be leader after quorum loss")
	}

	clientPub, _ := freshClient(t)
	// Use a SHORT-timeout forwarder so the no-leader case fails fast.
	fwd := broker.NewForwarder(c.conns[fi], 800*time.Millisecond)
	h := &authcallout.Handler{DB: openReadHandle(t, c.dbPaths[fi]), AccountKp: c.acctKp, Now: time.Now}
	h.ProvisionAgentWrite = broker.NewProvisionSeam(c.nodes[fi], fwd)

	out, err := h.Handle(signedAgentReq(t, clientPub, "lab", "lab-1", "test-pin"))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := mustDecodeResp(t, out)
	if resp.Error == "" {
		t.Fatal("a forwarded PIN with NO leader must be DENIED (no false allow under quorum loss)")
	}
	if !contains(resp.Error, "not_leader") {
		t.Errorf("the deny must be classified transient not_leader; got %q", resp.Error)
	}
	if provisioned(t, c.dbPaths[fi], "lab", "lab-1") {
		t.Fatal("a transient deny must NOT have written an agent_provisioning row")
	}
}

// TestD4DedupReplicatedLedgerAcrossLeadershipTransfer (§13.7 #2, §0bis-C) proves the
// cross-leader dedup property: a committed ReqID's ledger row REPLICATES, so the NEW
// leader dedups a retry of the same ReqID (no double-execute), which is the
// committed-but-ack-lost-then-retry-hits-new-leader scenario made deterministic via an
// explicit raft.LeadershipTransfer.
//
// Driven via direct node.ProposeWithReqID, NOT the forwarder: production provision/join
// carry NO ReqID and the cluster.apply boundary REJECTS a non-empty one (external RF1),
// so a reqID-bearing forward of those verbs cannot exist. This exercises the reqID
// dedup PRIMITIVE (as reconcile + future non-idempotent forwarded ops use it) at the
// Node level. The forwarder→ProposeWithReqID→FSM-dedup WIRE composition is covered by
// TestD4ForwardAckDropCommittedRetryDedups; the op-agnostic dedup branch by
// TestD4FSMReqIDDedupSemantics.
func TestD4DedupReplicatedLedgerAcrossLeadershipTransfer(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	_, fp := freshClient(t)
	plan := func(db *sql.DB) (*cluster.Command, error) {
		return agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", fp, "test-pin", auth.VerifyPIN, time.Now())
	}

	// Commit under the original leader with an explicit ReqID -> ledger replicates to F.
	if err := c.nodes[li].ProposeWithReqID(testReqID, plan); err != nil {
		t.Fatalf("initial commit on leader: %v", err)
	}
	// Hand leadership to F and wait for it to take over.
	if err := c.nodes[li].TransferLeadership(); err != nil {
		t.Fatalf("transfer leadership: %v", err)
	}
	if !waitForCond(5*time.Second, func() bool { return c.nodes[fi].IsLeader() }) {
		t.Fatal("leadership did not move to the former follower")
	}
	// Re-propose the SAME ReqID on the NEW leader: it dedups against the REPLICATED
	// ledger row (no double-execute). Assert the new leader's DedupCount==1 (deterministic
	// — it applies+dedups before ProposeWithReqID returns) + exactly one row on both
	// replicas; NOT the racy cross-node DedupCount sum (review M2).
	if err := c.nodes[fi].ProposeWithReqID(testReqID, plan); err != nil {
		t.Fatalf("retry on the new leader must succeed (dedup): %v", err)
	}
	if got := c.nodes[fi].DedupCount(); got != 1 {
		t.Fatalf("new-leader DedupCount=%d, want 1 (the retry must dedup against the REPLICATED ledger)", got)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 1 }) {
			t.Fatalf("node %d must hold EXACTLY ONE agent_provisioning row (no double-execute); got %d", i, provisionRowCount(t, pth, "lab", "lab-1"))
		}
	}
}

// TestD4ReconcileForwardReplicates (§13.7 #3, leader-authoritative reconcile): a
// follower forwards the RAW register request; the LEADER resolves it against its own
// replicated DB into ONE self-sufficient ReconcileBatch, commits it through raft, and
// the MarkExited transition replicates to the follower (proving the entry flows leader
// -> follower over real mTLS raft).
func TestD4ReconcileForwardReplicates(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li

	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
		seedNode(t, p, "lab", "lab-1")
		seedRunningProc(t, p, "lab", "lab-1", "01run")
	}
	// The agent's register lists NO processes -> "01run" is a missed-exit -> MarkExited.
	req := proto.NodeRegisterReq{BootID: "boot1"}
	reqID := broker.ReconcileReqID("lab", "lab-1", req)
	payload, _ := json.Marshal(broker.ReconcilePayload{SID: "lab", NID: "lab-1", Req: req})

	if err := c.forwarder(fi).Forward(broker.VerbReconcile, reqID, payload); err != nil {
		t.Fatalf("reconcile forward must commit: %v", err)
	}
	// The MarkExited replicated to the follower.
	if !waitForCond(3*time.Second, func() bool { return procStatus(t, c.dbPaths[fi], "01run") == "EXITED" }) {
		t.Fatalf("the leader-resolved ReconcileBatch MarkExited must replicate to the follower; got %q", procStatus(t, c.dbPaths[fi], "01run"))
	}
}

// TestD4ForwardChurnNoGoroutineLeak: the forwarder's reply-inbox subscriptions + the
// responder churn must not leak goroutines (CLAUDE.md §5 built-in NumGoroutine gate;
// goleak deliberately NOT used). The harness servers/nodes are up before AND after the
// baseline, so only the forward churn is measured.
func TestD4ForwardChurnNoGoroutineLeak(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	_, fp := freshClient(t)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})
	fwd := c.forwarder(fi)

	// Settle, then baseline (goroutines + fds — the forwarder opens a reply inbox per
	// Request; a leaked subscription shows as a leaked fd a pooled goroutine could mask).
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	before := runtime.NumGoroutine()
	fdBefore := fdCount()

	for i := 0; i < 40; i++ {
		_ = fwd.Forward(broker.VerbProvision, "", payload) // no reqID (provision idempotent); measures reply-inbox churn
	}
	assertNoGoroutineLeak(t, "forward-reply-inbox-churn", before)
	assertNoFDLeak(t, "forward-reply-inbox-churn", fdBefore)
}

// ---- small helpers local to forward_test ----

func seedRunningProc(t *testing.T, dbPath, sid, nid, pid string) {
	t.Helper()
	db := openReadHandle(t, dbPath)
	if err := proc.Insert(db, proc.Process{
		PID: pid, SID: sid, NID: nid, Argv: []string{"x"}, StartedAt: time.Now().UTC(), BootID: "seed", StartTimeTicks: 1,
	}); err != nil {
		t.Fatalf("seed proc: %v", err)
	}
}

func procStatus(t *testing.T, dbPath, pid string) string {
	t.Helper()
	db := openReadHandle(t, dbPath)
	var st string
	switch err := db.QueryRow(`SELECT status FROM processes WHERE pid=?`, pid).Scan(&st); err {
	case nil:
		return st
	case sql.ErrNoRows:
		return ""
	default:
		t.Fatalf("proc status: %v", err)
		return ""
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
