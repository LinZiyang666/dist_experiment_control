package brokercontrol

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/nats-io/nats.go/jetstream"
)

func recoveryBudget(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func recoveryHash(value interface{}) c.Digest {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return c.Digest(sha256.Sum256(data))
}

// This fixture owns the only account in its embedded server. Cross-account LIST
// and physical identity parsing are exercised through P11 in infrarepair tests.
func recoveryAssets(ctx context.Context, js jetstream.JetStream) (c.AssetSeal, error) {
	type asset struct {
		Name      string
		CreatedNS int64
		Config    c.Digest
	}
	var assets []asset
	list := js.ListStreams(ctx)
	for info := range list.Info() {
		assets = append(assets, asset{info.Config.Name, info.Created.UnixNano(), recoveryHash(info.Config)})
		stream, err := js.Stream(ctx, info.Config.Name)
		if err != nil {
			return c.AssetSeal{}, err
		}
		consumers := stream.ListConsumers(ctx)
		for consumer := range consumers.Info() {
			assets = append(assets, asset{info.Config.Name + "/" + consumer.Name, consumer.Created.UnixNano(), recoveryHash(consumer.Config)})
		}
		if err := consumers.Err(); err != nil {
			return c.AssetSeal{}, err
		}
	}
	if err := list.Err(); err != nil {
		return c.AssetSeal{}, err
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].Name < assets[j].Name })
	return c.AssetSeal{Accounts: recoveryHash([]string{"$G"}), Assets: recoveryHash(assets), Complete: true}, nil
}

// origin: hot-update S9 plan §3.2 / §4.3 / T40 / T41 / M20
func TestBrokerRecoveryFencesStreamAndAuditRoots(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerRecoveryRoots(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker recovery roots", goroutines)
	testharness.AssertNoFDLeak(t, "broker recovery roots", fds, 0)
}

func exerciseBrokerRecoveryRoots(t *testing.T) {
	raw, history := auditHistoryFixture(t)
	plan := c.Plan{Tx: c.ID{1}, Domain: c.ID{2}, Component: c.NATS}
	// Use the existing deterministic cursor owner so a blocked PublishOnce cannot
	// silently advance local state. All history/config effects are real JetStream.
	node := &fakeReader{leader: true, commit: 2, first: 1, cmds: map[uint64]*cluster.Command{1: nonReconcile(), 2: nonReconcile()}}
	original := infrarepair.Responsibilities{}
	gate, err := infrarepair.New(infrarepair.Config{Domain: plan.Domain, Required: infrarepair.LegacyRoots(), Accounts: map[string]jetstream.JetStream{"$G": raw},
		Assets: func(d c.Deadline, _ c.Stop) (c.AssetSeal, error) {
			remaining, err := controlbudget.Remaining(d)
			if err != nil {
				return c.AssetSeal{}, err
			}
			ctx, cancel := context.WithTimeout(t.Context(), remaining)
			defer cancel()
			return recoveryAssets(ctx, raw)
		},
		Exclusive: func(d c.Deadline, _ c.Stop) error { _, err := controlbudget.Remaining(d); return err },
		Transfers: func(c.Deadline) (c.TransferSeal, error) {
			return c.TransferSeal{Version: 1, SourceSHA256: recoveryHash("fixture"), SchemaSHA256: recoveryHash("schema"), InvocationSet: recoveryHash("invocations"), ReceiptSet: recoveryHash("receipts"), ConsumerSet: recoveryHash("consumers"), Complete: true}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range infrarepair.LegacyRoots() {
		producer := infrarepair.Producer{Name: root, Capture: func(c.Deadline) (infrarepair.Responsibilities, error) { return infrarepair.Responsibilities{}, nil }, Check: func(infrarepair.Responsibilities, c.Deadline, c.Stop) error { return nil }}
		if root == infrarepair.AuditPublish {
			producer.Capture = func(c.Deadline) (infrarepair.Responsibilities, error) { return original, nil }
			producer.Acknowledged = func(ack infrarepair.ACK) error { original.ACKs = append(original.ACKs, ack); return nil }
		}
		if root == infrarepair.AuditReplay {
			producer.Capture = func(c.Deadline) (infrarepair.Responsibilities, error) {
				return infrarepair.Responsibilities{Local: []infrarepair.LocalReceipt{{Identity: recoveryHash("publisher cursor"), PublisherCursor: node.published}}}, nil
			}
			producer.Check = func(r infrarepair.Responsibilities, _ c.Deadline, _ c.Stop) error {
				if len(r.Local) != 1 || r.Local[0].PublisherCursor != node.published {
					return infrarepair.ErrReceipt
				}
				return nil
			}
		}
		if err = gate.Register(producer); err != nil {
			t.Fatal(err)
		}
	}
	bound, err := infrarepair.BindLegacy(raw, gate, "$G")
	if err != nil {
		t.Fatal(err)
	}
	p := NewAuditPublisher(AuditPublisherConfig{JS: bound, Node: node, ListSIDs: func(context.Context) ([]string, error) { return []string{auditHistoryRecord("start").Session}, nil }})
	sid := auditHistoryRecord("start").Session
	if err = p.Publish(t.Context(), proto.SubjAuditProc(sid), []byte(`{"original":"audit"}`), "original-audit-id"); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"start", "failed"} {
		cmd, err := xferaudit.PlanTransferAudit(auditHistoryRecord(kind))
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := ConfirmTransferAuditHistory(p, t.Context(), cmd)
		if err != nil {
			t.Fatal(err)
		}
		msg, err := history.GetMsg(t.Context(), receipt.Sequence)
		if err != nil {
			t.Fatal(err)
		}
		original.ACKs = append(original.ACKs, infrarepair.ACK{Account: "$G", Stream: receipt.Stream, Subject: msg.Subject, Sequence: receipt.Sequence, Message: infrarepair.MessageDigest(msg.Header, msg.Data)})
	}
	if len(original.ACKs) != 3 {
		t.Fatalf("missing actual ACK capture: %d", len(original.ACKs))
	}
	if _, err = raw.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName(sid), Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	cp, err := gate.Seal(plan, recoveryBudget(t))
	if err != nil {
		t.Fatal(err)
	}
	before, err := history.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := xferaudit.PlanTransferAudit(auditHistoryRecord("start"))
	if err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		call func() error
	}{
		{"ensure-events", func() error { return jsstream.EnsureEventsStream(t.Context(), bound, 1) }},
		{"ensure-history", func() error { return jsstream.EnsureHistoryStream(t.Context(), bound, "new-history", 1) }},
		{"raise-existing", func() error { return jsstream.EnsureHistoryStream(t.Context(), bound, sid, 3) }},
		{"delete-history", func() error { return jsstream.DeleteHistoryStream(t.Context(), bound, sid) }},
		{"delete-object", func() error { return jsstream.DeleteXferBucket(t.Context(), bound, sid) }},
		{"placement-probe", func() error { return jsstream.ProbeMetaCanPlace(t.Context(), bound, 1) }},
		{"audit-publish", func() error { return p.Publish(t.Context(), proto.SubjAuditProc(sid), []byte("repair"), "repair") }},
		{"audit-bounded-loss", func() error {
			return p.PublishAudit(t.Context(), proto.SubjAuditProc(sid), []byte("repair"), "repair", 3, sid)
		}},
		{"replay-cursor", func() error { _, err := p.PublishOnce(t.Context()); return err }},
		{"reconcile-audit", func() error { return p.PublishReconcile(t.Context(), 3, cmd) }},
		{"transfer-audit", func() error { return p.PublishTransferAudit(t.Context(), 3, cmd) }},
		{"canonical-history", func() error { _, err := ConfirmTransferAuditHistory(p, t.Context(), cmd); return err }},
		{"reconcile-all", func() error { _, err := p.ReconcileOnce(t.Context()); return err }},
		{"reconcile-session", func() error { _, err := p.ReconcileSessions(t.Context(), []string{sid}, 3); return err }},
	}
	for _, op := range operations {
		if err = op.call(); !errors.Is(err, infrarepair.ErrFenced) {
			t.Fatalf("producer %s bypassed recovery: %v", op.name, err)
		}
	}
	if node.published != 0 || node.advanceCalls != 0 {
		t.Fatal("replay advanced original cursor while fenced")
	}
	after, err := history.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if before.State.Msgs != after.State.Msgs || before.State.LastSeq != after.State.LastSeq {
		t.Fatal("fenced producer repaired history")
	}
	if err = gate.VerifyBeforeReplay(cp, recoveryBudget(t), nil); err != nil {
		t.Fatal(err)
	}
	if err = gate.Release(cp, recoveryBudget(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.PublishOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if node.published != 2 {
		t.Fatal("verified generation did not resume original cursor")
	}
	if err = jsstream.EnsureEventsStream(t.Context(), bound, 1); err != nil {
		t.Fatal(err)
	}
	if err = jsstream.ProbeMetaCanPlace(t.Context(), bound, 1); err != nil {
		t.Fatal(err)
	}
	if err = jsstream.DeleteXferBucket(t.Context(), bound, sid); err != nil {
		t.Fatal(err)
	}
}
