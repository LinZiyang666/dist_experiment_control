package security_test

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// The authentication boundary consumes retained authority; this read-only
// fixture supplies that authority without claiming to exercise ledger IO.
// The original fixture boundary above now uses the production graph. That
// graph supplies both the retained grant and the capsule, keeping callout
// authentication and the session-removal barrier on the same ledger.
// Retirement is exercised through session rm rather than a synthetic census.
// origin: hot-update S7 plan §6.3–§6.4 / T31
// origin: S7 internal review MAKETEST-TRANSFER F8
func TestTransferContinuationSettlementNATSPermissions(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferSettlementNATSPermissions(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer settlement NATS", g)
	testharness.AssertNoFDLeak(t, "transfer settlement NATS", fd, 0)
}
func exerciseTransferSettlementNATSPermissions(t *testing.T) {
	t.Helper()
	url, brokerSeed, accountSeed := startJSAuthCalloutNATS(t)
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Wipe()
	pub, _ := receiver.PublicKey()
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", fp, stackharness.SessionPINHash(t), time.Now()); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	graphs := make(chan *broker.BrokerTransfers, 1)
	b, err := broker.New(broker.Config{NATSURL: url, DB: db, Logger: silentTransferLog(), ReadyCh: ready,
		TransferFactory: func(b *broker.Broker, js jetstream.JetStream, d controlbudget.Deadline) (*broker.BrokerTransfers, error) {
			graph, err := broker.ServeTransferFactory(b, js, d)
			if err == nil {
				graphs <- graph
			}
			return graph, err
		}, AuthCallout: &broker.AuthCalloutConfig{BrokerNkeySeed: brokerSeed, AccountSeed: accountSeed}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("broker did not stop")
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("broker startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker not ready")
	}
	// The callout's read-only authority fixture does not install a broker
	// settlement responsibility. Retain a real granted entry as well, so rm
	// cannot legitimately complete against an otherwise empty production graph.
	// origin: S7 internal review MAKETEST-TRANSFER F8; S7 plan §4 / §6.4 / T31
	capsule := retainSecurityTransferGrant(t, <-graphs, fp, pub)
	brokerKey, err := nkeys.FromSeed(brokerSeed)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerKey.Wipe()
	brokerPub, _ := brokerKey.PublicKey()
	authority, err := nats.Connect(url, nats.Nkey(brokerPub, brokerKey.Sign))
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	js, err := jetstream.New(authority)
	if err != nil {
		t.Fatal(err)
	}
	opCtx, opCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer opCancel()
	if err := jsstream.EnsureHistoryStream(opCtx, js, "lab", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	store, err := js.CreateObjectStore(opCtx, jetstream.ObjectStoreConfig{Bucket: "xfer-lab"})
	if err != nil {
		t.Fatal(err)
	}
	_ = store
	// Positive broker authority must reach the real UPDATE API, not only match
	// a wildcard string in the permission template.
	stream, err := js.Stream(opCtx, "OBJ_xfer-lab")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(opCtx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.Description = "settlement ACL positive control"
	if _, err := js.UpdateStream(opCtx, cfg); err != nil {
		t.Fatal("broker UPDATE denied:", err)
	}
	if err := session.Tombstone(db, "lab", time.Now()); err != nil {
		t.Fatal(err)
	}
	if ordinary, err := nats.Connect(url, nats.Nkey(pub, receiver.Sign), nats.Name("tether-cli:lab"), nats.NoReconnect()); err == nil {
		ordinary.Close()
		t.Fatal("ordinary CONNECT survived deletion")
	}
	// A new rm process must reach the retained tombstone and receive its
	// pending result, while its new connection still lacks business access.
	// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
	removalErrs := make(chan error, 8)
	connectRemoval := func(key nkeys.KeyPair) (*nats.Conn, error) {
		keyPub, _ := key.PublicKey()
		opts := []nats.Option{nats.Nkey(keyPub, key.Sign), nats.Name("session-removal:lab"), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			removalErrs <- err
		})}
		opts = append(opts, natsinbox.InboxConnectOptions(keyPub)...)
		return nats.Connect(url, opts...)
	}
	removal, err := connectRemoval(receiver)
	if err != nil {
		t.Fatal("removal owner CONNECT denied:", err)
	}
	defer removal.Close()
	response, err := removal.Request("tether.v2.ctrl.by."+pub+".session.lab.rm.req", []byte(`{}`), time.Second)
	if err != nil {
		t.Fatal("removal owner cannot retry through private reply:", err)
	}
	var pending proto.SessionRmResp
	if err := json.Unmarshal(response.Data, &pending); err != nil || pending.Code != xfercontract.CodeOwnerRecovering || pending.OK {
		t.Fatalf("missing pending removal response: %s (%v)", response.Data, err)
	}
	for _, subject := range []string{"tether.v2.ctrl.by." + pub + ".session.other.rm.req", "tether.v2.ctrl.by." + pub + ".s.lab.node.node.transfer.transfer.permit.req", "$O.xfer-lab.C.chunk"} {
		if err := removal.Publish(subject, nil); err != nil {
			t.Fatal(err)
		}
		if err := removal.Flush(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-removalErrs:
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatalf("wrong removal denial for %s: %v", subject, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("removal publish escaped ACL: %s", subject)
		}
	}
	token, err := json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	connect := func(key nkeys.KeyPair, errs chan error) (*nats.Conn, error) {
		keyPub, _ := key.PublicKey()
		opts := []nats.Option{nats.Nkey(keyPub, key.Sign), nats.Name("transfer-settlement:lab:node"), nats.Token(string(token)), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case errs <- err:
			default:
			}
		})}
		opts = append(opts, natsinbox.InboxConnectOptions(keyPub)...)
		return nats.Connect(url, opts...)
	}
	errs := make(chan error, 32)
	nc, err := connect(receiver, errs)
	if err != nil {
		t.Fatal("settlement CONNECT denied:", err)
	}
	defer nc.Close()
	prefix := "tether.v2.ctrl.by." + pub + ".s.lab.node.node.transfer.transfer."
	reply, err := authority.Subscribe(prefix+"status.req", func(m *nats.Msg) { _ = m.Respond([]byte("retained")) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reply.Unsubscribe() }()
	if err := authority.Flush(); err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(prefix+"status.req", nil, time.Second)
	if err != nil || string(msg.Data) != "retained" {
		t.Fatalf("settlement status/private reply denied: %v", err)
	}
	for _, subject := range []string{prefix + "permit.req", prefix + "reserve.req", "$JS.API.STREAM.UPDATE.OBJ_xfer-lab", "$JS.API.STREAM.MSG.GET.OBJ_xfer-lab", "$O.xfer-lab.C.chunk", "tether.v2.s.lab.audit.transfer", "tether.v2.ctrl.by." + pub + ".s.lab.node.node.transfer.other.terminal.req"} {
		if err := nc.Publish(subject, nil); err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errs:
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatalf("wrong denial for %s: %v", subject, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("settlement publish escaped ACL: %s", subject)
		}
	}
	thief, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer thief.Wipe()
	if unauthorized, err := connectRemoval(thief); err == nil {
		unauthorized.Close()
		t.Fatal("non-owner removal CONNECT accepted")
	}
	if stolen, err := connect(thief, errs); err == nil {
		stolen.Close()
		t.Fatal("stolen capsule accepted")
	}
	nc.Close()
	// Retire the actual entry through the explicit operator protocol, rather
	// than deleting a synthetic verifier row while the broker remains empty.
	// origin: S7 internal review MAKETEST-TRANSFER F8; S7 plan lead LQ1
	response, err = removal.Request("tether.v2.ctrl.by."+pub+".session.lab.rm.req", []byte(`{"abandon_unsettled_transfers":true}`), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var retiredResponse proto.SessionRmResp
	if err := json.Unmarshal(response.Data, &retiredResponse); err != nil || !retiredResponse.OK {
		t.Fatalf("explicit retirement did not settle the actual entry: %s (%v)", response.Data, err)
	}
	if retired, err := connect(receiver, errs); err == nil {
		retired.Close()
		t.Fatal("retired entry accepted")
	}
}

func retainSecurityTransferGrant(t *testing.T, graph *broker.BrokerTransfers, fp, receiver string) xfercontract.SettlementCapsule {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	issuer, ok := graph.Assembly.Config.Permits.(interface {
		IssueFor(xfercontract.Identity, xfercontract.Intent, controlbudget.Deadline) (xfercontract.CreationPermit, error)
	})
	if !ok {
		t.Fatal("production permit authority is unavailable")
	}
	intent := xfercontract.Intent{Verb: "push", Tier: "a", Path: "/pending-grant", Size: 1, SHA256: strings.Repeat("a", 64)}
	permit, err := issuer.IssueFor(xfercontract.Identity{SID: "lab", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "transfer"}, intent, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Tracker.Admit(permit, intent, d); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Tracker.SourceSeal(permit.Identity, xfercontract.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, SourceFingerprint: "pinned-source"}, d); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.Tracker.Reserve(permit.Identity, xfercontract.ReserveRequest{CallID: "reserve-pending", Origin: permit.Origin, ReceiverInstance: "agent"}, d); err != nil {
		t.Fatal(err)
	}
	capsule, err := graph.Link.ReceiverCapsule(permit.Identity, permit.Origin, receiver, d)
	if err != nil {
		t.Fatal(err)
	}
	return capsule
}
