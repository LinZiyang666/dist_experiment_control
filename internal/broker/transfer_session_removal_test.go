package broker

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type removalBarrierFunc func(string, bool, controlbudget.Deadline) error

func (f removalBarrierFunc) Settle(s string, a bool, d controlbudget.Deadline) error {
	return f(s, a, d)
}

// This tests the real rm handler, SQL tombstone and JS deletion boundary.
// P09/P11/P12/P14/P22/P23 must additionally run T31 with their real durable
// receiver/courier/origin graph; a controlled barrier is not that receipt.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestTransferSessionRemovalAuditLifetime(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferSessionRemoval(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer session removal", g)
	testharness.AssertNoFDLeak(t, "transfer session removal", fd, 0)
}
func exerciseTransferSessionRemoval(t *testing.T) {
	t.Helper()
	js, nc := newXferTestJS(t)
	db := openDB(t)
	actor := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", fp, brokerAuthorityPIN(1), time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, js, "lab", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-lab"}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"kind":"start","transfer_id":"pending-transfer"}`)
	if _, err := js.Publish(ctx, proto.SubjAuditTransfer("lab"), body); err != nil {
		t.Fatal(err)
	}
	var settled, abandonSeen atomic.Bool
	barrier := removalBarrierFunc(func(sid string, abandon bool, d controlbudget.Deadline) error {
		if sid != "lab" {
			return errors.New("wrong session")
		}
		if _, err := controlbudget.Remaining(d); err != nil {
			return err
		}
		if abandon {
			abandonSeen.Store(true)
		}
		if !settled.Load() {
			return &xferauth.PendingError{Transfers: []xferauth.PendingTransfer{{TransferID: "pending-transfer", Reason: "receiver_commit_unsettled"}}}
		}
		return nil
	})
	b, err := New(Config{NATSURL: nc.ConnectedUrl(), DB: db, Logger: silentLogger(), TransferSettlement: barrier})
	if err != nil {
		t.Fatal(err)
	}
	b.nc.Store(nc)
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	sub, err := nc.Subscribe(proto.SubjCtrlSessionRm(actor, "lab"), b.handleSessionRm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	request := func(payload string) proto.SessionRmResp {
		t.Helper()
		msg, err := nc.Request(proto.SubjCtrlSessionRm(actor, "lab"), []byte(payload), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var r proto.SessionRmResp
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	assertPreserved := func() {
		t.Helper()
		var state string
		if err := db.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state); err != nil || state != "DELETING" {
			t.Fatalf("pending session lost tombstone: %q %v", state, err)
		}
		stream, err := js.Stream(ctx, "history-lab")
		if err != nil {
			t.Fatal("pending history deleted:", err)
		}
		raw, err := stream.GetMsg(ctx, 1)
		if err != nil || string(raw.Data) != string(body) {
			t.Fatalf("original history replaced: %v", err)
		}
		if _, err := js.Stream(ctx, "OBJ_xfer-lab"); err != nil {
			t.Fatal("pending OBJ deleted:", err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		r := request(`{}`)
		if r.OK || r.Code != xfercontract.CodeOwnerRecovering || !strings.Contains(r.Error, "pending-transfer") || !strings.Contains(r.Error, "receiver_commit_unsettled") {
			t.Fatalf("pending removal reported success or lost IDs: %+v", r)
		}
		assertPreserved()
	}
	// Reconstruct the broker against the same durable SQL/JS, then run the cold
	// finalizer and the orphan arms in the same pass.
	cold, err := New(Config{NATSURL: nc.ConnectedUrl(), DB: db, Logger: silentLogger(), TransferSettlement: barrier})
	if err != nil {
		t.Fatal(err)
	}
	cold.nc.Store(nc)
	setBrokerJS(cold, js)
	t.Cleanup(func() { setBrokerJS(cold, nil) })
	if err := cold.reconcileHistoryStreamsOnBoot(ctx); err != nil {
		t.Fatal(err)
	}
	assertPreserved()
	r := request(`{"abandon_unsettled_transfers":true}`)
	if r.OK || !abandonSeen.Load() {
		t.Fatal("flag was ignored or unsettled abandonment permitted deletion")
	}
	assertPreserved()
	settled.Store(true)
	if err := cold.reconcileHistoryStreamsOnBoot(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"history-lab", "OBJ_xfer-lab"} {
		if _, err := js.Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("settled stream retained: %s %v", name, err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='lab'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("settled SQL retained: %d %v", count, err)
	}
}

// A continuation-capable broker that cannot reach its barrier must keep the
// tombstone and every piece of evidence. "Capable" is decided by the broker's
// own transfer wiring, not by whether a barrier happens to be present: the
// broker without any continuation wiring is the legacy shape and is pinned by
// TestSessionRemovalWithoutContinuationRunsLegacyCascade instead.
// origin: hot-update S7 plan §6.4 / T31; S7 lead integration (capable shapes made explicit)
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestTransferSessionRemovalFailsClosedWithoutCensus(t *testing.T) {
	cases := []struct {
		name    string
		capable func(*Broker)
	}{
		{"factory_configured_graph_not_installed", func(b *Broker) {
			b.cfg.TransferFactory = func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error) {
				return nil, errors.New("finalize must not assemble a graph")
			}
		}},
		{"graph_installed_without_settlement_port", func(b *Broker) {
			b.transferRuntime.Store(&BrokerTransfers{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openDB(t)
			if _, err := session.Create(db, "lab", "lab", "owner", brokerAuthorityPIN(1), time.Now()); err != nil {
				t.Fatal(err)
			}
			b, err := New(Config{NATSURL: "nats://127.0.0.1:1", DB: db, Logger: silentLogger()})
			if err != nil {
				t.Fatal(err)
			}
			tc.capable(b)
			if err := b.finalizeSessionRm(t.Context(), "lab"); !errors.Is(err, session.ErrDeleting) {
				t.Fatalf("ACTIVE session reached deletion: %v", err)
			}
			if err := session.Tombstone(db, "lab", time.Now()); err != nil {
				t.Fatal(err)
			}
			err = b.finalizeSessionRm(t.Context(), "lab")
			var pending *xferauth.PendingError
			if !errors.As(err, &pending) || pending.ExitCode() != 75 || !strings.Contains(err.Error(), "origin_census_unavailable") {
				t.Fatalf("missing census allowed deletion: %v", err)
			}
			var state string
			if err := db.QueryRow(`SELECT state FROM sessions WHERE sid='lab'`).Scan(&state); err != nil || state != "DELETING" {
				t.Fatalf("refused removal lost its tombstone: %q %v", state, err)
			}
		})
	}
}

// A broker with no continuation capability never issued a continuation Grant,
// so `session rm` must run the pre-S7 cascade exactly as before: one request,
// OK, history stream + OBJ bucket + SQL rows gone, and the boot DELETING
// resumer finishing an interrupted removal. The first P19 cut failed closed on
// the nil barrier and turned every such rm into a permanent rc75.
// origin: hot-update S7 plan §6.4 / lead LQ1; S7 lead integration — make test on the merged tree
// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2 (valid creation material)
func TestSessionRemovalWithoutContinuationRunsLegacyCascade(t *testing.T) {
	js, nc := newXferTestJS(t)
	db := openDB(t)
	actor := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, sid := range []string{"lab", "resume"} {
		if _, err := session.Create(db, sid, sid, fp, brokerAuthorityPIN(1), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := jsstream.EnsureHistoryStream(ctx, js, sid, jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-" + sid}); err != nil {
			t.Fatal(err)
		}
	}
	b, err := New(Config{NATSURL: nc.ConnectedUrl(), DB: db, Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	b.nc.Store(nc)
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	sub, err := nc.Subscribe(proto.SubjCtrlSessionRm(actor, "lab"), b.handleSessionRm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	assertGone := func(sid string) {
		t.Helper()
		for _, name := range []string{"history-" + sid, "OBJ_xfer-" + sid} {
			if _, err := js.Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("legacy removal retained %s: %v", name, err)
			}
		}
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid=?`, sid).Scan(&count); err != nil || count != 0 {
			t.Fatalf("legacy removal retained SQL for %s: %d %v", sid, count, err)
		}
	}
	msg, err := nc.Request(proto.SubjCtrlSessionRm(actor, "lab"), []byte(`{}`), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var r proto.SessionRmResp
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		t.Fatal(err)
	}
	if !r.OK || r.Code != "" {
		t.Fatalf("legacy session rm did not succeed on the first request: %+v", r)
	}
	assertGone("lab")
	// A crash between tombstone and cascade leaves DELETING; the cold resumer
	// must finish it without any barrier, as it did before S7.
	if err := session.Tombstone(db, "resume", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := b.reconcileHistoryStreamsOnBoot(ctx); err != nil {
		t.Fatal(err)
	}
	assertGone("resume")
}

func newXferTestJS(t *testing.T) (jetstream.JetStream, *nats.Conn) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js, nc
}
