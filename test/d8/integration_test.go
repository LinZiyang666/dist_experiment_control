//go:build d8_integration

package d8_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestD8Matrix is the single -race entrypoint for the gated D8 suite (run with
// `go test ./test/d8 -tags d8_integration -race -run TestD8Matrix`).
func TestD8Matrix(t *testing.T) {
	t.Run("AlertReplicatedRaiseClear", testAlertReplicatedRaiseClear)
	t.Run("AlertClusterLevelAck", testAlertClusterLevelAck)
	t.Run("TransferAuditReDerivable", testTransferAuditReDerivable)
	t.Run("ClusterHealthGate", testClusterHealthGate)
	t.Run("TierBSurvivesHomeKill", testTierBSurvivesHomeKill)
}

// wireForwarders subscribes the §4.1 leader responder (dispatchForward) on every node and
// returns a Forwarder per conn. The leader answers VerbAlertSignal/VerbAlertAck/
// VerbTransferAudit; followers stay silent.
func wireForwarders(t *testing.T, c *d8cluster) []*broker.Forwarder {
	t.Helper()
	now := func() time.Time { return time.Now().UTC() }
	fwds := make([]*broker.Forwarder, c.n)
	for i := 0; i < c.n; i++ {
		sub, err := broker.SubscribeClusterApply(c.conns[i], c.nodes[i], now)
		if err != nil {
			t.Fatalf("subscribe apply %d: %v", i, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
		fwds[i] = broker.NewForwarder(c.conns[i], 3*time.Second)
	}
	return fwds
}

// testAlertReplicatedRaiseClear: the leader-gated reconcile loop raises replication_degraded
// when the (controlled) replica observation is Degraded and the row REPLICATES to every node;
// a positive AllAtTarget observation clears it on every node. A transient unobserved pass in
// between must NOT false-clear.
func testAlertReplicatedRaiseClear(t *testing.T) {
	c := startD8Cluster(t, 3)

	// Controlled replica observation: Degraded -> (unobserved) -> AllAtTarget.
	var phase atomic.Int32 // 0=degraded, 1=unobserved, 2=at-target
	observe := func(context.Context) (broker.ReplicaReport, error) {
		switch phase.Load() {
		case 2:
			return broker.ReplicaReport{Observed: true, Streams: []jsstream.StreamReplicaState{{Ready: true, Target: 3, Actual: 3}}}, nil
		case 1:
			return broker.ReplicaReport{Observed: false}, nil
		default:
			return broker.ReplicaReport{Observed: true, Streams: []jsstream.StreamReplicaState{{Ready: false, Target: 3, Actual: 1}}}, nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < c.n; i++ {
		// Each node runs its own reconciler; only the leader's Propose commits (the others
		// are no-ops via cluster.Node's leader gate). The committed alert replicates to all.
		rr := broker.NewAlertReconciler(broker.AlertReconcilerConfig{
			Node: c.nodes[i], DB: c.dbs[i], Observe: observe, Poll: 100 * time.Millisecond,
			Propose: c.nodes[i].Propose,
		})
		go rr.Run(ctx)
	}

	key := cluster.DedupKeyGlobal(cluster.AlertKindReplicationDegraded)
	if !waitFor(8*time.Second, func() bool {
		for i := 0; i < c.n; i++ {
			if !c.activeAlertOn(i, key) {
				return false
			}
		}
		return true
	}) {
		cancel()
		t.Fatal("replication_degraded did not raise + replicate to all nodes")
	}

	// Transient unobserved pass: must NOT clear.
	phase.Store(1)
	time.Sleep(400 * time.Millisecond)
	for i := 0; i < c.n; i++ {
		if !c.activeAlertOn(i, key) {
			cancel()
			t.Fatalf("replication_degraded FALSE-CLEARED on node %d during a transient unobserved pass", i)
		}
	}

	// Positive AllAtTarget: clear on all nodes.
	phase.Store(2)
	if !waitFor(8*time.Second, func() bool {
		for i := 0; i < c.n; i++ {
			if c.activeAlertOn(i, key) {
				return false
			}
		}
		return true
	}) {
		cancel()
		t.Fatal("replication_degraded did not clear after AllAtTarget")
	}
	cancel()
	// NOTE: the reconciler goroutine-cleanup leak gate is a FOCUSED unit test
	// (TestD8AlertReconcileRunCancels, broker pkg) with a fake node — measuring it against
	// the live 3-node cluster here would track cluster background work, not the loop.
}

// testAlertClusterLevelAck: raise an alert, then a member sends an ack over the
// member-reachable ctrl.by.<actor>.alert.ack.req RPC; one broker forwards it to the leader and
// the SINGLE cluster-level ack REPLICATES to every node.
func testAlertClusterLevelAck(t *testing.T) {
	c := startD8Cluster(t, 3)
	wireForwarders(t, c)
	for i := 0; i < c.n; i++ {
		sub, err := broker.SubscribeAlertAck(c.conns[i], broker.NewForwarder(c.conns[i], 3*time.Second))
		if err != nil {
			t.Fatalf("subscribe ack %d: %v", i, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	// Raise an alert to ack.
	key := cluster.DedupKeyGlobal(cluster.AlertKindBelowQuorum)
	now := time.Now().UTC()
	c.proposeOn(t, func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanAlertRaise("ack-test", cluster.AlertKindBelowQuorum, cluster.AlertSeverityInfo, key, "below quorum", now)
	})

	// A member acks via NATS (actor-scoped subject); the broker derives acked_by from it.
	const actor = "UMEMBERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	body := mustJSON(t, proto.AlertAckReq{DedupKey: key})
	reply, err := c.conns[0].Request(proto.SubjCtrlAlertAck(actor), body, 3*time.Second)
	if err != nil {
		t.Fatalf("ack request: %v", err)
	}
	if string(reply.Data) != "ok" {
		t.Fatalf("ack reply = %q, want ok", reply.Data)
	}
	// The ack replicates to every node (acked_by == the authenticated actor).
	if !waitFor(8*time.Second, func() bool {
		for i := 0; i < c.n; i++ {
			by := ackedBy(c, i, key)
			if by != actor {
				return false
			}
		}
		return true
	}) {
		t.Fatal("cluster-level ack did not replicate to all nodes with the authenticated actor")
	}
}

// testTransferAuditReDerivable: a forwarded VerbTransferAudit commits a re-derivable
// OpTransferAudit; the leader AuditPublisher replays it to history-<sid>. Then — to actually
// EXERCISE the cross-election/cross-retry collapse (M3: merely killing the leader + re-draining
// is vacuous, since the new leader inherits the REPLICATED publish cursor and skips the entry)
// — the SAME (tid,complete) record is forwarded a SECOND time after an election: a NEW raft
// index with the SAME content-derived reqID. Both anchors must collapse it: (1) the 0011
// cluster_reqid_ledger dedups the duplicate at the FSM layer (DedupCount grows), and (2) the
// q<reqID>:xfer:0 JS MsgId keeps the history stream at exactly ONE row.
func testTransferAuditReDerivable(t *testing.T) {
	c := startD8Cluster(t, 3)
	fwds := wireForwarders(t, c)
	const sid = "lab"

	if !waitFor(20*time.Second, func() bool {
		cx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		return jsstream.EnsureHistoryStream(cx, c.js[0], sid, c.n) == nil
	}) {
		t.Fatal("history stream never placed")
	}

	rec := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Session: sid, Node: "lab-1", TransferID: "tid-x", Tier: "b"}
	payload := mustJSON(t, rec)
	forwardToLeader(t, c, fwds, broker.VerbTransferAudit, payload)
	drainPublishers(t, c)

	stream := jsstream.HistoryStreamName(sid)
	if !waitFor(8*time.Second, func() bool { return streamMsgCount(t, c, stream) == 1 }) {
		t.Fatalf("transfer audit not published exactly once (have %d)", streamMsgCount(t, c, stream))
	}

	// Kill the leader, then GENUINELY re-derive: forward the SAME record again post-election.
	li := c.leaderIdx(t)
	_ = c.nodes[li].Shutdown()
	c.nodes[li] = nil
	c.leaderIdx(t) // new leader

	dedupBefore := totalDedup(c)
	forwardToLeader(t, c, fwds, broker.VerbTransferAudit, payload) // same reqID, new raft index
	drainPublishers(t, c)

	// (1) the 0011 ledger collapsed the duplicate at the FSM layer (every live node's FSM saw
	// the already-committed reqID and short-circuited it).
	if !waitFor(8*time.Second, func() bool { return totalDedup(c) > dedupBefore }) {
		t.Fatalf("cross-election re-forward was NOT deduped by the 0011 ledger (DedupCount unchanged at %d)", dedupBefore)
	}
	// (2) JS MsgId dedup kept the history stream at exactly one row.
	if got := streamMsgCount(t, c, stream); got != 1 {
		t.Fatalf("re-forwarded transfer audit produced %d history rows, want exactly 1 (JS dedup failed)", got)
	}
}

// testClusterHealthGate: the broadcast cluster-health responder reports a confirmed writable
// leader on a healthy cluster, so the client-synth gate does NOT fire (EXIT-B no-false-positive).
func testClusterHealthGate(t *testing.T) {
	c := startD8Cluster(t, 3)
	now := func() time.Time { return time.Now().UTC() }
	const actorNR = "UMEMBERNRAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	// No-responders fast-path (M1): BEFORE any broker subscribes, the probe must return
	// quickly (the server's 503 sentinel), not block the full window — and yield no gate.
	t0 := time.Now()
	if pre := probeHealth(t, c.conns[0], actorNR); len(pre) != 0 {
		t.Fatalf("no responders yet, got %d health replies", len(pre))
	}
	if elapsed := time.Since(t0); elapsed > 300*time.Millisecond {
		t.Fatalf("no-responders probe blocked %v — the 503 fast-path is not working", elapsed)
	}
	if proto.EvalDestructiveGate(nil).Blocked() {
		t.Fatal("zero replies must NOT gate (ctl jitter / non-cluster)")
	}

	for i := 0; i < c.n; i++ {
		sub, err := broker.SubscribeClusterHealth(c.conns[i], c.nodes[i], c.dbs[i], now)
		if err != nil {
			t.Fatalf("subscribe health %d: %v", i, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	const actor = "UMEMBERBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	// Retry the probe until every broker's wildcard subscription interest has propagated
	// across the routes (a fresh sub is not instantly visible on a remote server).
	var replies []proto.ClusterHealthResp
	if !waitFor(8*time.Second, func() bool {
		replies = probeHealth(t, c.conns[0], actor)
		return len(replies) >= c.n
	}) {
		t.Fatalf("expected >= %d health replies, got %d", c.n, len(replies))
	}
	confirmed := false
	for _, r := range replies {
		if r.WritableLeaderConfirmed {
			confirmed = true
		}
	}
	if !confirmed {
		t.Fatal("no broker confirmed a writable leader on a healthy cluster")
	}
	if g := proto.EvalDestructiveGate(replies); g.Blocked() {
		t.Fatalf("healthy cluster must NOT gate destructive ops, got %+v", g)
	}
}

// testTierBSurvivesHomeKill (EXIT-A): an OBJ_xfer bucket created at R=n survives killing the
// broker (NATS server) that created it — the completed object is still readable elsewhere.
func testTierBSurvivesHomeKill(t *testing.T) {
	c := startD8Cluster(t, 3)
	const sid = "lab"
	bucket := proto.XferBucketName(sid)

	cx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Create the object store at R=n on node 0 (the "home").
	var store jetstream.ObjectStore
	if !waitFor(20*time.Second, func() bool {
		os, err := c.js[0].CreateObjectStore(cx, jetstream.ObjectStoreConfig{
			Bucket: bucket, Replicas: c.n, Storage: jetstream.FileStorage, MaxBytes: 64 << 20,
		})
		if err != nil {
			os, err = c.js[0].ObjectStore(cx, bucket)
		}
		if err == nil {
			store = os
		}
		return err == nil
	}) {
		t.Fatal("xfer object store never placed at R=n")
	}
	// Wait for the object-store's backing stream to place at R=n BEFORE the put (a freshly
	// created clustered stream briefly answers "no responders" until its leader is elected).
	if !waitForStreamReplicas(t, c, "OBJ_"+bucket, c.n) {
		t.Fatal("OBJ_xfer stream never reached R=n")
	}
	if !waitFor(15*time.Second, func() bool {
		_, perr := store.PutBytes(cx, "tid-x", []byte("payload-survives"))
		return perr == nil
	}) {
		t.Fatal("put object never succeeded (stream not ready)")
	}

	// Kill the "home" broker (node 0's NATS server). The R=n object survives on the others;
	// the JS meta-group + OBJ stream re-elect among the 2 survivors (quorum 2 of 3).
	c.servers[0].Shutdown()
	c.servers[0].WaitForShutdown()

	// Read it back from another broker (node 1). Use a PER-ITERATION context so the read
	// window is the full retry budget (a single shared context would expire mid-re-election).
	if !waitFor(40*time.Second, func() bool {
		rx, rcancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer rcancel()
		os1, err := c.js[1].ObjectStore(rx, bucket)
		if err != nil {
			return false
		}
		data, err := os1.GetBytes(rx, "tid-x")
		return err == nil && string(data) == "payload-survives"
	}) {
		t.Fatal("completed tier-B object did NOT survive killing its home broker (EXIT-A)")
	}
}

// ---- drill helpers ----

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// totalDedup sums the FSM cross-retry dedup count across live nodes (the §4.1 D4 ledger
// short-circuit). A genuine re-forward of an already-committed reqID grows it on every replica.
func totalDedup(c *d8cluster) uint64 {
	var n uint64
	for _, nd := range c.nodes {
		if nd != nil {
			n += nd.DedupCount()
		}
	}
	return n
}

func ackedBy(c *d8cluster, i int, dedupKey string) string {
	var by string
	_ = c.dbs[i].QueryRow(`SELECT acked_by FROM alert_acks WHERE dedup_key=?`, dedupKey).Scan(&by)
	return by
}

// forwardToLeader publishes a §4.1 forward via each conn's Forwarder until one reaches the
// leader (the others time out / answer not_leader, which Forward maps to a retriable error).
func forwardToLeader(t *testing.T, c *d8cluster, fwds []*broker.Forwarder, verb string, payload []byte) {
	t.Helper()
	if !waitFor(8*time.Second, func() bool {
		for _, f := range fwds {
			if f.Forward(verb, "", payload) == nil {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("forward %s never reached a leader", verb)
	}
}

// drainPublishers constructs a leader AuditPublisher and drains it until the publish cursor
// CONVERGES (stable across rounds). It does not wait for adv==CommitIndex: each
// AdvanceAuditPublished commits a checkpoint op (bumping the commit index), and PublishOnce
// deliberately skips a trailing checkpoint without advancing past it — so the cursor settles
// one short of commit by design. Convergence (adv unchanged) means all re-derivable audit has
// been published.
func drainPublishers(t *testing.T, c *d8cluster) {
	t.Helper()
	li := c.leaderIdx(t)
	pub := broker.NewAuditPublisher(broker.AuditPublisherConfig{Node: c.nodes[li], JS: c.js[li]})
	var last uint64
	stable := 0
	for i := 0; i < 40 && stable < 3; i++ {
		adv, err := pub.PublishOnce(context.Background())
		if err != nil {
			time.Sleep(80 * time.Millisecond)
			continue
		}
		if adv == last {
			stable++
		} else {
			stable, last = 0, adv
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// streamMsgCount returns a stream's committed message count, read from the first live conn.
func streamMsgCount(t *testing.T, c *d8cluster, name string) uint64 {
	t.Helper()
	for i := 0; i < c.n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s, err := c.js[i].Stream(ctx, name)
		if err != nil {
			cancel()
			continue
		}
		info, err := s.Info(ctx)
		cancel()
		if err != nil {
			continue
		}
		return info.State.Msgs
	}
	return 0
}

// probeHealth broadcasts a cluster-health request and collects every reply within a window
// (mirrors the ctl-side probe; the cmd/tether package is not importable from a test).
func probeHealth(t *testing.T, nc *nats.Conn, actor string) []proto.ClusterHealthResp {
	t.Helper()
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	_ = nc.PublishRequest(proto.SubjCtrlClusterHealth(actor), inbox, nil)
	deadline := time.Now().Add(800 * time.Millisecond)
	var out []proto.ClusterHealthResp
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			break
		}
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			break // no-responders fast-path (M1): mirrors the production probeClusterHealth
		}
		var r proto.ClusterHealthResp
		if json.Unmarshal(msg.Data, &r) == nil {
			out = append(out, r)
		}
	}
	return out
}

func waitForStreamReplicas(t *testing.T, c *d8cluster, name string, want int) bool {
	t.Helper()
	return waitFor(15*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		s, err := c.js[0].Stream(ctx, name)
		if err != nil {
			return false
		}
		info, err := s.Info(ctx)
		if err != nil || info.Cluster == nil {
			return false
		}
		actual := 1
		for _, p := range info.Cluster.Replicas {
			if p != nil && p.Current && !p.Offline {
				actual++
			}
		}
		return actual >= want
	})
}
