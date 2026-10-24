package brokercontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §5.2 / T23 / T24 / T25
func TestTransferAuditHistorySeparatesStartAndTerminalSlots(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferAuditHistorySlots(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer audit history slots", beforeG)
	testharness.AssertNoFDLeak(t, "transfer audit history slots", beforeFD, 0)
}

func exerciseTransferAuditHistorySlots(t *testing.T) {
	js, stream := auditHistoryFixture(t)
	start := auditHistoryRecord("start")
	terminal := auditHistoryRecord("failed")
	cmds := map[uint64]*cluster.Command{}
	for i, rec := range []schema.AuditTransfer{start, terminal} {
		cmd, err := xferaudit.PlanTransferAudit(rec)
		if err != nil {
			t.Fatal(err)
		}
		cmds[uint64(i+1)] = cmd
	}
	var original [2]uint64
	// Rebuild the publisher with an empty checkpoint: both slots must read back
	// their original rows, regardless of the discarded publisher's local state.
	for replay := 0; replay < 2; replay++ {
		reader := &fakeReader{leader: true, commit: 2, first: 1, cmds: cmds}
		p := NewAuditPublisher(AuditPublisherConfig{Node: reader, JS: js})
		if _, err := p.PublishOnce(t.Context()); err != nil {
			t.Fatal(err)
		}
		for i := range original {
			receipt, err := ConfirmTransferAuditHistory(p, t.Context(), cmds[uint64(i+1)])
			if err != nil {
				t.Fatal(err)
			}
			if replay == 0 {
				original[i] = receipt.Sequence
			}
			if receipt.Sequence != original[i] {
				t.Fatal("replay replaced the original physical START/TERMINAL row")
			}
		}
		opposite, err := xferaudit.PlanTransferAudit(auditHistoryRecord("complete"))
		if err != nil {
			t.Fatal(err)
		}
		if receipt, err := ConfirmTransferAuditHistory(p, t.Context(), opposite); !errors.Is(err, xferhistory.ErrConflict) || receipt.Sequence != 0 {
			t.Fatalf("opposite outcome acknowledged after failed: %+v %v", receipt, err)
		}
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != 2 || original[0] == original[1] {
		t.Fatalf("START/TERMINAL physical slots collapsed or duplicated: %+v %v", info, err)
	}
	for i, rec := range []schema.AuditTransfer{start, terminal} {
		canonical, err := schema.CanonicalTransferAudit(rec)
		if err != nil {
			t.Fatal(err)
		}
		row, err := stream.GetMsg(t.Context(), original[i])
		if err != nil || row.Subject != proto.SubjAuditTransfer(rec.Session) || !bytes.Equal(row.Data, canonical.Bytes) {
			t.Fatalf("replay changed canonical history bytes for %s: %v", rec.Kind, err)
		}
		var physical schema.AuditTransfer
		if err := json.Unmarshal(row.Data, &physical); err != nil || physical.Kind != rec.Kind || physical.TerminalID != rec.TerminalID {
			t.Fatalf("physical history changed the original outcome/terminal slot: %+v %v", physical, err)
		}
	}
}

// origin: hot-update S7 plan §5.2 / T24
func TestTransferAuditCanceledAttemptRetainsHistoryResponsibility(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferAuditCanceledAttempt(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer canceled audit attempt", beforeG)
	testharness.AssertNoFDLeak(t, "transfer canceled audit attempt", beforeFD, 0)
}

func exerciseTransferAuditCanceledAttempt(t *testing.T) {
	js, stream := auditHistoryFixture(t)
	cmd, err := xferaudit.PlanTransferAudit(auditHistoryRecord("failed"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lost := &cancelAuditHistoryJS{JetStream: js, cancel: cancel}
	p := NewAuditPublisher(AuditPublisherConfig{JS: lost})
	if receipt, err := ConfirmTransferAuditHistory(p, ctx, cmd); !errors.Is(err, context.Canceled) || receipt.Sequence != 0 {
		t.Fatalf("attempt canceled after real JS acceptance returned acknowledgement: %+v %v", receipt, err)
	}
	if lost.accepted != 1 {
		t.Fatal("fixture did not cancel after a real JS publish ACK")
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("canceled attempt lost the already accepted physical row: %+v %v", info, err)
	}
	p = NewAuditPublisher(AuditPublisherConfig{JS: js})
	receipt, err := ConfirmTransferAuditHistory(p, t.Context(), cmd)
	if err != nil || receipt.Sequence != info.State.LastSeq {
		t.Fatalf("retained retry failed to read the original exact receipt: %+v %v", receipt, err)
	}
}

type cancelAuditHistoryJS struct {
	jetstream.JetStream
	cancel   context.CancelFunc
	accepted int
}

func (j *cancelAuditHistoryJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	if err == nil {
		j.accepted++
		j.cancel()
	}
	return ack, err
}

func auditHistoryFixture(t *testing.T) (jetstream.JetStream, jetstream.Stream) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(t.Context(), js, "audit-slots", 1); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), jsstream.HistoryStreamName("audit-slots"))
	if err != nil {
		t.Fatal(err)
	}
	return js, stream
}

func auditHistoryRecord(kind string) schema.AuditTransfer {
	rec := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Tier: "b",
		Ts: time.Date(2026, 10, 12, 1, 2, 3, 0, time.UTC), Session: "audit-slots", Node: "node", ActorFp: "actor",
		TransferID: "task", AgentInstance: "instance", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "object"}
	if kind != "start" {
		rec.TerminalID = schema.TransferTerminalID(rec.Session, rec.TransferID, rec.EntryNonce)
	}
	return rec
}
