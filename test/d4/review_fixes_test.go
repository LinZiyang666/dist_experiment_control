package d4_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

// ---- review B1: forwarded bad PIN maps to the typed sentinel + emits pin_failed ----

// TestD4ForwardedBadPINMapsInvalidPINAndEmitsPinFailed (review B1) proves the
// forwarded write path is behaviorally identical to the local path on a bad PIN: the
// handler emits `pin_failed` exactly once and denies with the canonical "invalid PIN"
// message (NOT a degraded "provision: ..." default). RED on the pre-fix code (ErrKind
// via %T + no ForwardBusinessError.Is): zero pin_failed + a "provision:" deny.
func TestD4ForwardedBadPINMapsInvalidPINAndEmitsPinFailed(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "correct-pin")
	}
	clientPub, _ := freshClient(t)
	var events []string
	h := &authcallout.Handler{
		DB: openReadHandle(t, c.dbPaths[fi]), AccountKp: c.acctKp, Now: time.Now,
		EmitEvent: func(kind string, _ map[string]any) { events = append(events, kind) },
	}
	h.ProvisionAgentWrite = broker.NewProvisionSeam(c.nodes[fi], c.forwarder(fi))

	out, err := h.Handle(signedAgentReq(t, clientPub, "lab", "lab-1", "WRONG-pin"))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	resp := mustDecodeResp(t, out)
	if resp.Error == "" {
		t.Fatal("a wrong PIN must be denied")
	}
	if !contains(resp.Error, "invalid PIN") {
		t.Fatalf("forwarded bad-PIN deny must be the canonical 'invalid PIN' (B1 parity), got %q", resp.Error)
	}
	if n := count(events, "pin_failed"); n != 1 {
		t.Fatalf("forwarded bad PIN must emit pin_failed exactly once (B1 parity), got %d (%v)", n, events)
	}
	if provisioned(t, c.dbPaths[fi], "lab", "lab-1") {
		t.Fatal("a bad PIN must not provision")
	}
}

// TestD4ForwardBadPINIsTerminalNotRetriable (review M3 + B1 wire) proves the `error`
// reply path: a forwarded bad PIN returns a *ForwardBusinessError (NOT the retriable
// ErrForwardNotLeader) that errors.Is-matches agentprov.ErrInvalidPIN, and writes no row.
func TestD4ForwardBadPINIsTerminalNotRetriable(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "correct-pin")
	}
	_, fp := freshClient(t)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "WRONG"})
	err := c.forwarder(fi).Forward(broker.VerbProvision, "", payload) // provision carries no reqID (F1)
	if errors.Is(err, cluster.ErrForwardNotLeader) {
		t.Fatalf("a bad PIN is a PERMANENT business error, must NOT be retriable; got %v", err)
	}
	var be *broker.ForwardBusinessError
	if !errors.As(err, &be) {
		t.Fatalf("expected *ForwardBusinessError, got %T (%v)", err, err)
	}
	if !errors.Is(err, agentprov.ErrInvalidPIN) {
		t.Fatalf("forwarded invalid PIN must errors.Is-match agentprov.ErrInvalidPIN (B1); got %v", err)
	}
	for i, p := range c.dbPaths {
		if provisioned(t, p, "lab", "lab-1") {
			t.Fatalf("no row may be written for a bad PIN (node %d)", i)
		}
	}
}

// TestD4ForwardInvalidReqIDMustNotReturnOK_Review is reviewer-owned coverage for the
// forwarder/responder boundary: an invalid ReqID must not be allowed to enter raft as a
// poison Command and still surface as forward status=ok. Today dispatchForward stamps
// the unchecked ReqID, decodeCommand poisons it, FSM advances applied_index without
// running the op, Node.Apply returns nil, and the responder replies ok — a success with
// no durable effect. That is not a valid fail-closed direction for a write-forward seam.
func TestD4ForwardInvalidReqIDMustNotReturnOK_Review(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	_, fp := freshClient(t)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})

	err := c.forwarder(fi).Forward(broker.VerbProvision, "BAD-REQID", payload)
	if err == nil {
		t.Fatal("invalid ReqID must not return success; poison-skipping the op and replying ok is a false success")
	}
	for i, p := range c.dbPaths {
		if got := provisionRowCount(t, p, "lab", "lab-1"); got != 0 {
			t.Fatalf("invalid ReqID must not write an agent_provisioning row on node %d; got %d", i, got)
		}
	}
}

// TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review (external F1) is the reviewer
// lifecycle regression: after an operator evicts a node, the same agent identity may
// legitimately re-provision the same (sid,nid) — a NEW logical write, not an ack-lost
// retry. The FIX is that provision forwards carry NO ReqID (its INSERT OR IGNORE is
// idempotent + the binding is deletable, so a content key would falsely dedup-skip the
// re-provision), so the re-provision recreates the row. This test forwards with the
// empty ReqID the production seam uses and asserts the recreate.
func TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	_, fp := freshClient(t)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})

	if err := c.forwarder(fi).Forward(broker.VerbProvision, "", payload); err != nil {
		t.Fatalf("initial provision: %v", err)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 1 }) {
			t.Fatalf("initial provision must replicate to node %d", i)
		}
	}

	if err := c.nodes[li].Propose(func(*sql.DB) (*cluster.Command, error) {
		return nodepkg.PlanEvict("lab", "lab-1")
	}); err != nil {
		t.Fatalf("evict through raft: %v", err)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 0 }) {
			t.Fatalf("evict must remove provisioning row on node %d", i)
		}
	}

	if err := c.forwarder(fi).Forward(broker.VerbProvision, "", payload); err != nil {
		t.Fatalf("re-provision of the same agent identity should be a valid new write, got: %v", err)
	}
	for i, p := range c.dbPaths {
		pth := p
		// waitForCond: the leader commits synchronously but the follower replicates
		// async (the initial-provision check above uses the same wait).
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 1 }) {
			t.Fatalf("re-provision after evict must recreate the row on node %d; got %d", i, provisionRowCount(t, pth, "lab", "lab-1"))
		}
	}
}

// TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review is the
// boundary regression for the external F1 fix. Production provision/join seams now
// carry an empty ReqID, but the cluster.apply responder must also enforce or ignore
// that scope: a broker-bus caller that still sends a valid non-empty provision key
// must not be able to resurrect the old stale-ledger false success.
//
// Acceptable fixes:
//   - reject non-empty ReqID on provision/join with a permanent error; or
//   - ignore ReqID for these structurally-idempotent, deletable writes.
//
// What is not acceptable: first provision writes a ledger row, node-evict deletes the
// binding, retry with the same ReqID returns ok, and the binding stays absent.
func TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review(t *testing.T) {
	c := startCluster4(t, 2)
	li := c.leaderIdx(t)
	fi := 1 - li
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	_, fp := freshClient(t)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})
	reqID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	if err := c.forwarder(fi).Forward(broker.VerbProvision, reqID, payload); err != nil {
		if errors.Is(err, cluster.ErrForwardNotLeader) {
			t.Fatalf("healthy initial provision returned transient not_leader instead of committing or permanently rejecting the non-empty ReqID: %v", err)
		}
		for i, p := range c.dbPaths {
			if got := provisionRowCount(t, p, "lab", "lab-1"); got != 0 {
				t.Fatalf("permanently rejected non-empty provision ReqID must not write row on node %d; got %d", i, got)
			}
		}
		return
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 1 }) {
			t.Fatalf("initial provision must replicate to node %d", i)
		}
	}

	if err := c.nodes[li].Propose(func(*sql.DB) (*cluster.Command, error) {
		return nodepkg.PlanEvict("lab", "lab-1")
	}); err != nil {
		t.Fatalf("evict through raft: %v", err)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 0 }) {
			t.Fatalf("evict must remove provisioning row on node %d", i)
		}
	}

	err := c.forwarder(fi).Forward(broker.VerbProvision, reqID, payload)
	if errors.Is(err, cluster.ErrForwardNotLeader) {
		t.Fatalf("healthy re-provision returned transient not_leader instead of committing or permanently rejecting the non-empty ReqID: %v", err)
	}
	if err != nil {
		return // permanent rejection is an acceptable boundary fix.
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return provisionRowCount(t, pth, "lab", "lab-1") == 1 }) {
			t.Fatalf("non-empty provision ReqID returned success after evict but did not recreate the row on node %d; got %d", i, provisionRowCount(t, pth, "lab", "lab-1"))
		}
	}
}

// ---- review B2: the JOIN verb forwarded follower->leader (the unexercised shape) ----

// TestD4MemberJoinForwardedFollowerReachesLeader (review B2) exercises the join verb
// end-to-end: a CLI member-join whose auth_callout ran on a FOLLOWER forwards through
// NewJoinSeam → VerbJoin dispatch → session.PlanJoinWithPIN on the leader; ALLOW + the
// member row replicates; a re-join is observably idempotent (still exactly one member
// row — join carries NO ReqID per external F1; INSERT OR IGNORE makes it structurally
// idempotent).
func TestD4MemberJoinForwardedFollowerReachesLeader(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	clientPub, fp := freshClient(t)
	h := &authcallout.Handler{DB: openReadHandle(t, c.dbPaths[fi]), AccountKp: c.acctKp, Now: time.Now}
	h.JoinMemberWrite = broker.NewJoinSeam(c.nodes[fi], c.forwarder(fi))

	out, err := h.Handle(signedCliReq(t, clientPub, "lab", "test-pin"))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if resp := mustDecodeResp(t, out); resp.Error != "" {
		t.Fatalf("a forwarded member-join must ALLOW (agent never sees not_leader); got %q", resp.Error)
	}
	for i, p := range c.dbPaths {
		pth := p
		if !waitForCond(3*time.Second, func() bool { return isMember(t, pth, "lab", fp) }) {
			t.Fatalf("the forwarded member-join must replicate to node %d", i)
		}
	}
	// Re-join (no ReqID) is observably idempotent: still exactly one member row.
	payload, _ := json.Marshal(broker.JoinPayload{SID: "lab", FP: fp, PIN: "test-pin"})
	if err := c.forwarder(fi).Forward(broker.VerbJoin, "", payload); err != nil {
		t.Fatalf("re-join must succeed (idempotent): %v", err)
	}
	for i, p := range c.dbPaths {
		if got := memberRowCount(t, p, "lab", fp); got != 1 {
			t.Fatalf("re-join must leave EXACTLY ONE member row on node %d, got %d", i, got)
		}
	}
}

// ---- review M4: reconcile forwarding idempotency + ReqID derivation properties ----

// TestD4ReconcileForwardIdempotent (review M4, main-process adjudication) re-forwards
// the SAME reconcile and asserts OBSERVABLE no-double-execute. Note a reconcile is
// DOUBLY idempotent: once the first batch commits, the leader's RUNNING proc is now
// EXITED, so the re-resolve produces an EMPTY batch (nil command, never reaches Apply)
// — the FSM ReqID-dedup branch is short-circuited by this earlier natural no-op, so
// DedupCount stays 0 here (correct, not a miss). The op-agnostic ReqID dedup branch
// itself is proven by TestD4FSMReqIDDedupSemantics; this proves the reconcile FORWARD
// path commits once and a retry is a harmless no-op (no resurrection, no error).
func TestD4ReconcileForwardIdempotent(t *testing.T) {
	c := startCluster4(t, 2)
	fi := 1 - c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
		seedNode(t, p, "lab", "lab-1")
		seedRunningProc(t, p, "lab", "lab-1", "01run")
	}
	req := proto.NodeRegisterReq{BootID: "boot1"}
	reqID := broker.ReconcileReqID("lab", "lab-1", req)
	payload, _ := json.Marshal(broker.ReconcilePayload{SID: "lab", NID: "lab-1", Req: req})
	fwd := c.forwarder(fi)
	if err := fwd.Forward(broker.VerbReconcile, reqID, payload); err != nil {
		t.Fatalf("first reconcile forward: %v", err)
	}
	if !waitForCond(3*time.Second, func() bool { return procStatus(t, c.dbPaths[fi], "01run") == "EXITED" }) {
		t.Fatal("first reconcile must mark the proc EXITED on the follower")
	}
	// Retry the SAME reqID: must not error and must not resurrect / double-anything.
	if err := fwd.Forward(broker.VerbReconcile, reqID, payload); err != nil {
		t.Fatalf("reconcile retry (same ReqID) must be a harmless no-op: %v", err)
	}
	if got := procStatus(t, c.dbPaths[fi], "01run"); got != "EXITED" {
		t.Fatalf("proc must stay EXITED after the retry (no double-execute/resurrection), got %q", got)
	}
}

// TestReconcileReqIDDerivation (review M4 + M5) pins ReconcileReqID — the only D4 verb
// that carries a forwarding key (provision/join carry none, external F1): STABLE under
// shuffled/duplicate-key agent lists (the M5 fix), epoch on bootID, sid/nid sensitivity,
// and the decode-guard charset (32 lowercase hex).
func TestReconcileReqIDDerivation(t *testing.T) {
	rc := 5
	// Duplicate PID, different other fields, reversed input order (the M5 case).
	a := proto.NodeRegisterReq{BootID: "b", LocalProcesses: []proto.LocalProcess{
		{PID: "01a", State: "running", StartTimeTicks: 1},
		{PID: "01a", State: "exited", RC: &rc},
	}}
	b := proto.NodeRegisterReq{BootID: "b", LocalProcesses: []proto.LocalProcess{
		{PID: "01a", State: "exited", RC: &rc},
		{PID: "01a", State: "running", StartTimeTicks: 1},
	}}
	if broker.ReconcileReqID("lab", "n", a) != broker.ReconcileReqID("lab", "n", b) {
		t.Fatal("duplicate-key reconcile key must be input-order-stable (M5: SliceStable + full tiebreak)")
	}
	bootDiff := proto.NodeRegisterReq{BootID: "b2", LocalProcesses: a.LocalProcesses}
	if broker.ReconcileReqID("lab", "n", a) == broker.ReconcileReqID("lab", "n", bootDiff) {
		t.Fatal("a different bootID (the epoch) must yield a different reconcile key")
	}
	if broker.ReconcileReqID("lab", "n", a) == broker.ReconcileReqID("lab", "n2", a) {
		t.Fatal("reconcile key must be nid-sensitive")
	}
	if broker.ReconcileReqID("lab", "n", a) == broker.ReconcileReqID("lab2", "n", a) {
		t.Fatal("reconcile key must be sid-sensitive")
	}
	k := broker.ReconcileReqID("lab", "n", a)
	if len(k) != 32 {
		t.Fatalf("key %q len=%d, want 32 (128-bit hex prefix)", k, len(k))
	}
	for i := 0; i < len(k); i++ {
		ch := k[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			t.Fatalf("key %q has a non-lowercase-hex char (would poison decodeCommand)", k)
		}
	}
}

// ---- review M6: a leader that lost quorum fails closed (3-node, kill 2) ----

// TestD4ForwardLeaderLosesQuorumFailsClosed (review M6, achievable half) proves the
// load-bearing no-false-allow property: a node that lost quorum cannot commit a
// forwarded write — the forward returns a retriable deny within bounded time and NO
// row is written. (The full stale-leader-in-lease-vs-new-majority case needs a
// transport-partition seam over the NetworkTransport, recorded as deferred in
// d4-review.md M6 — the mechanism is panel-confirmed sound.)
func TestD4ForwardLeaderLosesQuorumFailsClosed(t *testing.T) {
	c := startCluster4(t, 3)
	li := c.leaderIdx(t)
	for _, p := range c.dbPaths {
		seedSession(t, p, "lab", "test-pin")
	}
	for i := range c.nodes {
		if i != li && c.nodes[i] != nil {
			_ = c.nodes[i].Shutdown()
			c.nodes[i] = nil
		}
	}
	// The former leader can no longer reach a quorum.
	_, fp := freshClient(t)
	fwd := broker.NewForwarder(c.conns[li], 1*time.Second)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})
	err := fwd.Forward(broker.VerbProvision, "", payload)
	if !errors.Is(err, cluster.ErrForwardNotLeader) {
		t.Fatalf("a leader that lost quorum must fail-closed RETRIABLE (no commit), got %v", err)
	}
	if provisioned(t, c.dbPaths[li], "lab", "lab-1") {
		t.Fatal("no write may commit without quorum (no false allow)")
	}
}

// ---- review M7: wire-level retriable classification + committed-but-ack-lost retry ----

// TestD4ForwardWireRetriableClassification (review M7) proves the two ambiguous wire
// outcomes — a malformed reply and a no-responder/timeout — are classified RETRIABLE
// (cluster.ErrForwardNotLeader), never a permanent deny.
func TestD4ForwardWireRetriableClassification(t *testing.T) {
	t.Run("malformed-reply", func(t *testing.T) {
		nc := soloBrokerConn(t)
		sub, err := nc.Subscribe(proto.SubjClusterApplyWildcard, func(m *nats.Msg) {
			if m.Reply != "" {
				_ = m.Respond([]byte("{not valid json"))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
		_ = nc.Flush()
		if err := broker.NewForwarder(nc, 800*time.Millisecond).Forward(broker.VerbProvision, "abcd", []byte("{}")); !errors.Is(err, cluster.ErrForwardNotLeader) {
			t.Fatalf("a malformed reply must be retriable, got %v", err)
		}
	})
	t.Run("no-responder-timeout", func(t *testing.T) {
		nc := soloBrokerConn(t) // nobody subscribed to cluster.apply.>
		if err := broker.NewForwarder(nc, 500*time.Millisecond).Forward(broker.VerbProvision, "abcd", []byte("{}")); !errors.Is(err, cluster.ErrForwardNotLeader) {
			t.Fatalf("a no-responder/timeout must be retriable, got %v", err)
		}
	})
}

// TestD4ForwardAckDropCommittedRetryDedups (review M7, §0bis-C gold case) proves the
// "committed but ack lost" invariant directly: the responder COMMITS the entry then
// DROPS the first reply; the forwarder times out (retriable); the retry with the SAME
// ReqID dedups (DedupCount==1) and writes exactly one row — a timeout is never a fresh
// write.
func TestD4ForwardAckDropCommittedRetryDedups(t *testing.T) {
	nd, dbPath := startSoloRaftLeader(t)
	seedSession(t, dbPath, "lab", "test-pin")
	nc := soloBrokerConn(t)
	_, fp := freshClient(t)
	reqID := testReqID // explicit key: the ack-drop dedup PRIMITIVE (production provision carries none, F1)

	var calls int32
	sub, err := nc.Subscribe(proto.SubjClusterApplyWildcard, func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		// The test owns reqID/payload, so the responder commits the KNOWN write
		// directly (no need to decode the unexported forward envelope).
		_ = nd.ProposeWithReqID(reqID, func(db *sql.DB) (*cluster.Command, error) {
			return agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", fp, "test-pin", auth.VerifyPIN, time.Now())
		})
		if atomic.AddInt32(&calls, 1) == 1 {
			return // DROP the first reply: committed under the hood, ack lost
		}
		_ = m.Respond([]byte(`{"status":"ok"}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	_ = nc.Flush()

	fwd := broker.NewForwarder(nc, 600*time.Millisecond)
	payload, _ := json.Marshal(broker.ProvisionPayload{SID: "lab", NID: "lab-1", FP: fp, PIN: "test-pin"})
	if err := fwd.Forward(broker.VerbProvision, reqID, payload); !errors.Is(err, cluster.ErrForwardNotLeader) {
		t.Fatalf("a dropped reply on a committed entry must be retriable, got %v", err)
	}
	if err := fwd.Forward(broker.VerbProvision, reqID, payload); err != nil {
		t.Fatalf("the retry (same ReqID) must succeed: %v", err)
	}
	if got := nd.DedupCount(); got != 1 {
		t.Fatalf("the committed-but-ack-lost retry must dedup; DedupCount=%d want 1", got)
	}
	if n := provisionRowCount(t, dbPath, "lab", "lab-1"); n != 1 {
		t.Fatalf("exactly one provisioning row (timeout is never a fresh write), got %d", n)
	}
}

// ---- helpers ----

func provisionRowCount(t *testing.T, dbPath, sid, nid string) int {
	t.Helper()
	db := openReadHandle(t, dbPath)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM agent_provisioning WHERE sid=? AND nid=?`, sid, nid).Scan(&n); err != nil {
		t.Fatalf("provision row count: %v", err)
	}
	return n
}

func isMember(t *testing.T, dbPath, sid, fp string) bool {
	t.Helper()
	db := openReadHandle(t, dbPath)
	ok, err := session.IsMember(db, sid, fp)
	if err != nil {
		t.Fatalf("is member: %v", err)
	}
	return ok
}

func memberRowCount(t *testing.T, dbPath, sid, fp string) int {
	t.Helper()
	db := openReadHandle(t, dbPath)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM members WHERE sid=? AND pubkey_fp=?`, sid, fp).Scan(&n); err != nil {
		t.Fatalf("member row count: %v", err)
	}
	return n
}

func count(haystack []string, needle string) int {
	n := 0
	for _, s := range haystack {
		if s == needle {
			n++
		}
	}
	return n
}

// soloBrokerConn brings up ONE routed-opts NATS server + a broker-nkey conn (RF1
// cluster.apply pub/sub), for wire-level forwarder tests that need no raft.
func soloBrokerConn(t *testing.T) *nats.Conn {
	t.Helper()
	b := newBrokerID(t)
	_, acctPub := newAccount(t)
	srv := startRouted(t, authClusterOpts([]brokerID{b}, acctPub, 1))[0]
	nc, err := nats.Connect(srv.ClientURL(), nats.Nkey(b.pub, sigFromSeed(b.seed)), nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("solo broker conn: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// startSoloRaftLeader brings up ONE mTLS raft node (single-node bootstrap => immediate
// leader) for the ack-drop test's custom responder to commit through.
func startSoloRaftLeader(t *testing.T) (*cluster.Node, string) {
	t.Helper()
	ca := newRouteCA(t)
	tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
		BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.clusterLeaf(t),
	})
	if err != nil {
		t.Fatalf("solo transport: %v", err)
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	nd, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID("solo"), DataDir: dir, DBPath: dbPath, Transport: tr,
		HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
		LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout, ApplyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("solo node: %v", err)
	}
	t.Cleanup(func() { _ = nd.Shutdown() })
	if err := nd.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("solo leader: %v", err)
	}
	return nd, dbPath
}
