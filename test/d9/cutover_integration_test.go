//go:build d9_integration

package d9_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestD9Matrix is the D9 cutover proof (gated d9_integration, runs under -race). It
// replaces the deleted build-and-prove guards' "production wires NO cluster" obligation
// with the POSITIVE proof that the real cutover path stands up cluster mode end to end
// AND that an authoritative control write routes through raft.
func TestD9Matrix(t *testing.T) {
	t.Run("ClusterModeConstructsAndLeads", testClusterModeConstructsAndLeads)
	t.Run("SessionCreateRoutesThroughRaft", testSessionCreateRoutesThroughRaft)
	t.Run("SessionRmCascadeRoutesThroughRaft", testSessionRmCascadeRoutesThroughRaft)
	t.Run("TwoBrokerJoinReplicates", testTwoBrokerJoinReplicates)
}

// testSessionRmCascadeRoutesThroughRaft is the round-1 BLOCKER regression: a `session rm`
// in cluster mode used to write the READ-ONLY FSM handle (Tombstone + dropSessionRows on
// b.cfg.DB==RODB) and HARD-FAIL with store_error. After the fix it routes the tombstone +
// the H.3 hard-delete cascade through raft. Proof: create → rm (must NOT be store_error) →
// re-create the SAME name SUCCEEDS (the row was truly dropped from the replicated FSM, not
// left tombstoned). Single-mode is byte-identical (the routers fall through to the direct
// mutators), so this also guards the cluster branch specifically.
func testSessionRmCascadeRoutesThroughRaft(t *testing.T) {
	h := startD9Broker(t, "d9-node-rm")
	waitLeader(t, h)
	nc, err := nats.Connect(h.url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()
	seed, _ := auth.GenerateUserSeed()
	actor, _ := auth.PublicKeyFromSeed(seed)

	create := func() proto.SessionCreateResp {
		body, _ := json.Marshal(proto.SessionCreateReq{Name: "rmtest", PIN: "123456"})
		msg, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 8*time.Second)
		if err != nil {
			t.Fatalf("session.create request: %v", err)
		}
		var r proto.SessionCreateResp
		_ = json.Unmarshal(msg.Data, &r)
		return r
	}
	if r := create(); r.Error != "" {
		t.Fatalf("first create errored: %s", r.Error)
	}
	// rm by the SAME actor (the owner) — must route the tombstone + cascade through raft.
	rmBody, _ := json.Marshal(proto.SessionRmReq{})
	msg, err := nc.Request(proto.SubjCtrlSessionRm(actor, "rmtest"), rmBody, 8*time.Second)
	if err != nil {
		t.Fatalf("session.rm request: %v", err)
	}
	var rm proto.SessionRmResp
	_ = json.Unmarshal(msg.Data, &rm)
	if rm.Code == "store_error" {
		t.Fatalf("session rm hit store_error in cluster mode (the RODB-write BLOCKER regressed): %s", rm.Error)
	}
	// The cascade dropped the row via raft ⇒ a re-create of the same name must SUCCEED.
	if r := create(); r.Error != "" {
		t.Fatalf("re-create after rm errored %q — the rm cascade did not drop the row through raft", r.Error)
	}
}

// testTwoBrokerJoinReplicates is the N>=2 cutover/grow proof: two full brokers in cluster
// mode (shared NATS + shared cluster CA) form a real raft cluster when the leader admits the
// second via the §8.1 two-phase add (challenge nonce → join-PoP signature → ClusterNodeUpsert
// → AddVoter → catch-up). After the join B is a VOTER FOLLOWER of A (it adopted A's config),
// proving the `cluster add` grow path works at the broker level (the N=1 cutover + this N=2
// grow are the automated halves of the §13.12 drill; the full kill-the-leader failover is the
// staged operator drill in docs/cluster-runbook.md).
func testTwoBrokerJoinReplicates(t *testing.T) {
	a, b, bID, bRaft := startD9Pair(t)
	waitLeader(t, a) // A leads its initial {A} cluster

	admin := a.b.ClusterAdminForTest()
	if admin == nil {
		t.Fatal("leader A has no cluster admin (cutover wiring regressed)")
	}
	nonce, err := admin.IssueJoinNonce()
	if err != nil {
		t.Fatalf("issue join nonce: %v", err)
	}
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(bID, pub, nonce))
	if err != nil {
		t.Fatal(err)
	}
	in := cluster.ClusterNodeUpsertInput{
		NodeID: bID, Name: bID, NodeIdentPub: pub, NatsServerID: "tether-" + bID,
		RaftAddr: bRaft, NatsRoute: "nats://x", TunnelAddr: "x:7000", PublicHost: "h",
		CertFP: "sha256:ab", JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig), Now: time.Now(),
	}
	caughtUp := func(barrier uint64) (bool, error) { return b.b.AppliedIndexForTest() >= barrier, nil }
	if err := admin.AddNode(in, bRaft, caughtUp, 25*time.Second); err != nil {
		t.Fatalf("cluster add B failed: %v", err)
	}

	// After the join B is a VOTER FOLLOWER (clustered, not leader) and A still leads.
	deadline := time.Now().Add(15 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		bClustered, bLeader := b.b.ClusterStateForTest()
		_, aLeader := a.b.ClusterStateForTest()
		if bClustered && !bLeader && aLeader {
			settled = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !settled {
		t.Fatal("after the join, B did not settle as a VOTER FOLLOWER of leader A within 15s")
	}

	// round-2 MAJOR (forward-path coverage): with a FOLLOWER present, drive an authoritative
	// write over the shared bus. Both brokers handle the broadcast; the follower's
	// proposeOrForward FORWARDS to the leader (the leader-Propose path AND the follower-
	// Forward path both run). The write must commit (no store_error) and a re-create of the
	// same name must be rejected — proving it committed to the replicated FSM via the cluster.
	nc, ncErr := nats.Connect(a.url)
	if ncErr != nil {
		t.Fatalf("nats connect: %v", ncErr)
	}
	defer nc.Close()
	wseed, _ := auth.GenerateUserSeed()
	actor, _ := auth.PublicKeyFromSeed(wseed)
	body, _ := json.Marshal(proto.SessionCreateReq{Name: "fwd", PIN: "123456"})
	msg, reqErr := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 8*time.Second)
	if reqErr != nil {
		t.Fatalf("session.create in 2-broker cluster: %v", reqErr)
	}
	var resp proto.SessionCreateResp
	_ = json.Unmarshal(msg.Data, &resp)
	if resp.Error != "" {
		t.Fatalf("session.create errored in 2-broker cluster: %s", resp.Error)
	}
	// Second create rejected ⇒ the first truly committed through raft (with a follower in the cluster).
	msg2, req2Err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 8*time.Second)
	if req2Err != nil {
		t.Fatalf("second session.create: %v", req2Err)
	}
	var resp2 proto.SessionCreateResp
	_ = json.Unmarshal(msg2.Data, &resp2)
	if resp2.Error == "" {
		t.Fatal("second create must be rejected (the first committed to the replicated FSM)")
	}
}

func waitLeader(t *testing.T, h *d9Broker) {
	t.Helper()
	if clustered, _ := h.b.ClusterStateForTest(); !clustered {
		t.Fatal("broker did not enter cluster mode (detection / construction regressed)")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, leader := h.b.ClusterStateForTest(); leader {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("single-voter node did not take leadership within 15s (mTLS transport / seeded raft config regressed)")
}

// testClusterModeConstructsAndLeads proves the cutover construction: InitFromExisting
// seeds + bootstraps a single-voter cluster DB, then broker.New + Run detect cluster
// mode, construct cluster.Node over the real mTLS raft transport (loading the §15
// secrets), attach the seams, and the single voter takes leadership.
func testClusterModeConstructsAndLeads(t *testing.T) {
	waitLeader(t, startD9Broker(t, "d9-node-A"))
}

// testSessionCreateRoutesThroughRaft proves the step-3 write cutover: a session.create
// over NATS is handled, routed through node.Propose (leader), committed to the FSM-owned
// WAL DB, read back, and replied — the path that in single mode is the direct mutator. A
// non-empty SID + no error in the reply means the Propose committed AND the read-back saw
// the committed row (the DB is now FSM-authoritative, not direct-written).
func testSessionCreateRoutesThroughRaft(t *testing.T) {
	h := startD9Broker(t, "d9-node-B")
	waitLeader(t, h)

	nc, err := nats.Connect(h.url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "123456"})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 8*time.Second)
	if err != nil {
		t.Fatalf("session.create request (cluster mode): %v", err)
	}
	var resp proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("session.create errored in cluster mode: %s", resp.Error)
	}
	if resp.SID != "lab" {
		t.Fatalf("SID = %q, want lab (the routed Propose + read-back path)", resp.SID)
	}

	// A second create of the SAME name must be rejected (ErrAlreadyExists), proving the
	// first one truly COMMITTED to the replicated FSM (not a transient local write).
	msg2, err := nc.Request(proto.SubjCtrlSessionCreate(actor), body, 8*time.Second)
	if err != nil {
		t.Fatalf("second session.create request: %v", err)
	}
	var resp2 proto.SessionCreateResp
	_ = json.Unmarshal(msg2.Data, &resp2)
	if resp2.Error == "" {
		t.Fatal("second create of the same name must be rejected (the first committed via raft)")
	}
}
