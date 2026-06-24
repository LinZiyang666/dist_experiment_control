package broker

import (
	"context"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/nats-io/nats.go/jetstream"
)

// captureJS is a minimal jetstream.JetStream whose ONLY live method is Publish; it records
// each publish subject and returns a fake ack. The publisher's MsgId travels via the
// OnPublish tap (we do not reach into the functional opts). Every other interface method is
// nil-embedded and must never be called by these tests.
type captureJS struct {
	jetstream.JetStream
	mu   sync.Mutex
	subs []string
}

func (c *captureJS) Publish(_ context.Context, subject string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.mu.Lock()
	c.subs = append(c.subs, subject)
	c.mu.Unlock()
	return &jetstream.PubAck{}, nil
}

// TestD8PublisherReplaysTransferAudit: an OpTransferAudit entry at idx 1 is REPLAYED and
// published to history-<sid> with the q<reqID>:xfer:0 dedup id, and the cursor advances.
// Paired with TestD8PublisherVacuityControl below, this proves the OpTransferAudit case in
// PublishOnce is load-bearing: a non-replayable op at the same index is SILENTLY advanced
// past with zero publish, which is exactly how a transfer entry would behave if the case
// were removed — so removing it would fail this test.
func TestD8PublisherReplaysTransferAudit(t *testing.T) {
	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Session: "lab", Node: "lab-1", TransferID: "tid-1", Tier: "b",
	}
	cmd, err := xferaudit.PlanTransferAudit(rec)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	f := &fakeReader{leader: true, commit: 1, first: 1, published: 0,
		cmds: map[uint64]*cluster.Command{1: cmd}}
	js := &captureJS{}
	var gotIDs []string
	p := NewAuditPublisher(AuditPublisherConfig{Node: f, JS: js,
		OnPublish: func(_, msgID string) { gotIDs = append(gotIDs, msgID) }})
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv != 1 || f.published != 1 {
		t.Fatalf("cursor not advanced: adv=%d published=%d", adv, f.published)
	}
	if len(js.subs) != 1 || js.subs[0] != proto.SubjAuditTransfer("lab") {
		t.Fatalf("publish subjects = %v, want one %s", js.subs, proto.SubjAuditTransfer("lab"))
	}
	wantID := "q" + cmd.ReqID + ":xfer:0"
	if len(gotIDs) != 1 || gotIDs[0] != wantID {
		t.Fatalf("dedup id = %v, want %q", gotIDs, wantID)
	}
}

// TestD8PublisherVacuityControl: a non-replayable op (OpClusterMetaSet) at idx 1 hits the
// default path — advance the cursor, publish NOTHING. This is the "case absent" control for
// the positive test above (it would behave identically if the OpTransferAudit case were
// deleted), making that test non-vacuous.
func TestD8PublisherVacuityControl(t *testing.T) {
	f := &fakeReader{leader: true, commit: 1, first: 1, published: 0,
		cmds: map[uint64]*cluster.Command{1: cluster.NewCommand(cluster.OpClusterMetaSet)}}
	js := &captureJS{}
	p := NewAuditPublisher(AuditPublisherConfig{Node: f, JS: js})
	adv, err := p.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if adv != 1 || f.published != 1 {
		t.Fatalf("cursor must still advance past a non-replayable op: adv=%d", adv)
	}
	if len(js.subs) != 0 {
		t.Fatalf("non-replayable op must NOT publish, got %v", js.subs)
	}
}
