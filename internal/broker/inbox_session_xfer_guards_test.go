package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestClusterProxyCreateAlsoWithholdsSecretsFromLegacyInbox is the alternate
// production branch of external-review B-1. The single-broker handler and its new
// test do not run in cluster mode: handleProxySub dispatches to
// handleProxySubCreateCluster instead, so the credential fence must exist there too.
func TestClusterProxyCreateAlsoWithholdsSecretsFromLegacyInbox(t *testing.T) {
	const sid, fp = "lab", "owner-fp"
	b, _ := newClusterProxyBroker(t, sid, fp)

	url := startNATS(t)
	responder, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(responder.Close)
	b.nc.Store(responder)
	t.Cleanup(func() { b.nc.Store(nil) })

	sub, err := responder.Subscribe("review.cluster.proxy.create", func(msg *nats.Msg) {
		b.handleProxySubCreateCluster(sid, fp, "owner-actor", "legacy", msg)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := responder.Flush(); err != nil {
		t.Fatal(err)
	}

	legacy, err := nats.Connect(url) // default reply prefix is the shared _INBOX root
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(legacy.Close)
	msg, err := legacy.Request("review.cluster.proxy.create", nil, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var got proto.ProxySubCreateResp
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SubURL != "" {
		t.Fatalf("cluster proxy create published a bearer URL into legacy _INBOX: %q; "+
			"the new fence covers only proxySubCreate, but cluster mode uses a different handler", got.SubURL)
	}
	if got.Code != proto.CodeLegacyInboxNoSecrets {
		t.Fatalf("cluster legacy reply code=%q, want %q", got.Code, proto.CodeLegacyInboxNoSecrets)
	}
}

// TestForwardedSessionCreateRechecksAdmissionOnTheLeader covers both an N-1
// broker and a follower whose policy view is stale. The origin broker's handler check
// is not authoritative: the old binary has no check at all, and a current follower can
// lag a revocation. The leader must therefore re-check before committing the write.
func TestForwardedSessionCreateRechecksAdmissionOnTheLeader(t *testing.T) {
	n, _ := d7SingleNode(t, "creator-forward-fence")
	now := time.Now().UTC()
	const fp = "SHA256:forwarded-attacker"

	// Establish the post-migration state, then revoke the identity. A stale current
	// follower or an N-1 broker can still forward the old payload after this point.
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSetCreator(fp, "operator", "", true, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSeedCreators(nil, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSetCreator(fp, "operator", "", false, now)
	}); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(SessionCreatePayload{Name: "rogue", FP: fp, PinHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	err = dispatchForward(n, func() time.Time { return now }, forwardDeps{}, forwardEnvelope{
		Verb: VerbSessionCreate, Payload: body,
	})
	if err == nil {
		if _, getErr := session.Get(n.RODB(), "rogue"); getErr == nil {
			t.Fatal("leader committed a session for a revoked/unadmitted fingerprint forwarded by a peer; " +
				"the admission check exists only on the origin handler, so an N-1 broker bypasses it")
		}
		t.Fatal("leader accepted an unadmitted forwarded session create")
	}
	if _, getErr := session.Get(n.RODB(), "rogue"); !errors.Is(getErr, session.ErrNotFound) {
		t.Fatalf("refused create left an unexpected row/read error: %v", getErr)
	}
}

// TestLeaderLocalSessionCreateRechecksAdmissionAtTheCommitBoundary is the other
// half of the authoritative admission fence. The forwarded verb handler can be
// correct while a request already executing on the leader still races a creator
// revocation between the handler's early check and the raft proposal. The closure
// passed to proposeOrForward is the last committed-DB boundary before PlanCreate,
// so it must repeat the check there as well.
func TestLeaderLocalSessionCreateRechecksAdmissionAtTheCommitBoundary(t *testing.T) {
	b := newLeaderBrokerForSessions(t)
	now := time.Now().UTC()
	const fp = "SHA256:leader-local-revoked"

	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSetCreator(fp, "operator", "", true, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSeedCreators(nil, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanSetCreator(fp, "operator", "", false, now)
	}); err != nil {
		t.Fatal(err)
	}

	_, err := b.createSession("leader-local-rogue", fp, "hash")
	if !errors.Is(err, session.ErrNotAllowedToCreate) {
		t.Fatalf("leader-local create after committed revocation returned %v, want %v; "+
			"the forwarded handler was fenced but the leader-local propose closure still calls PlanCreate directly",
			err, session.ErrNotAllowedToCreate)
	}
	if _, getErr := session.Get(b.cl.node.RODB(), "leader-local-rogue"); !errors.Is(getErr, session.ErrNotFound) {
		t.Fatalf("refused leader-local create left an unexpected row/read error: %v", getErr)
	}
}

// TestSingleBrokerTerminalReplayAfterDedupWindowDoesNotDuplicate proves that a
// content-derived Msg-Id is only a bounded-time guard. A real restart can last longer
// than the stream's duplicate window; recovery still owes exactly one terminal then.
func TestSingleBrokerTerminalReplayAfterDedupWindowDoesNotDuplicate(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	const duplicateWindow = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "history-lab-review", Subjects: []string{proto.SubjAuditTransfer("lab")},
		Storage: jetstream.MemoryStorage, Duplicates: duplicateWindow,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now,
		Session: "lab", Node: "n1", TransferID: "tid-window", Tier: "a",
	}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: t.TempDir(), Logger: silentLogger(), Now: time.Now}
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	b.pubAuditTransfer(rec)

	// Model a process that stayed down beyond the configured history-stream window.
	time.Sleep(3 * duplicateWindow)
	staged := xferInflightRecord{
		TransferID: rec.TransferID, Session: rec.Session, Node: rec.Node,
		Verb: rec.Verb, Tier: rec.Tier, StartedAt: now.Add(-time.Minute), Terminal: &rec,
	}
	if !b.replayStagedTerminal(ctx, staged) {
		t.Fatal("recovery did not dispose of the staged terminal")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("history contains %d terminals after replay beyond the dedup window, want 1; "+
			"equal Nats-Msg-Id values stop deduplicating once JetStream forgets them", info.State.Msgs)
	}
}

// TestSingleBrokerRecoveryDoesNotAppendAContradictoryTerminal pins the invariant
// stated by replayStagedTerminal itself: there may be exactly one terminal per
// transfer, regardless of whether that terminal says complete or failed. Looking
// up only the staged row's kind misses an already-committed opposite outcome, and
// the content-derived Msg-Ids intentionally differ, so JetStream cannot dedup it.
func TestSingleBrokerRecoveryDoesNotAppendAContradictoryTerminal(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "history-lab-contradict-review", Subjects: []string{proto.SubjAuditTransfer("lab")},
		Storage: jetstream.MemoryStorage, Duplicates: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	complete := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now,
		Session: "lab", Node: "n1", TransferID: "tid-contradict", Tier: "a",
	}
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: t.TempDir(), Logger: silentLogger(), Now: time.Now}
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	b.pubAuditTransfer(complete)

	failed := complete
	failed.Kind = "failed"
	failed.Error = "home_broker_restart"
	staged := xferInflightRecord{
		TransferID: failed.TransferID, Session: failed.Session, Node: failed.Node,
		Verb: failed.Verb, Tier: failed.Tier, StartedAt: now.Add(-time.Minute), Terminal: &failed,
	}
	if !b.replayStagedTerminal(ctx, staged) {
		t.Fatal("recovery did not dispose of the staged terminal")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("history contains %d terminal rows for one transfer, want 1; "+
			"an existing complete must suppress a staged failed (and vice versa), not merely an identical kind",
			info.State.Msgs)
	}
}
