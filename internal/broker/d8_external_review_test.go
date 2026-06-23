package broker

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func seedD8TransferMemberNode(t *testing.T, b *Broker, actor, sid, nid string) {
	t.Helper()
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if _, err := session.Create(b.cfg.DB, sid, sid, fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := b.cfg.DB.Exec(`INSERT INTO nodes(sid,nid,status) VALUES(?,?, 'ONLINE')`, sid, nid); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

func expectNoReply(t *testing.T, nc *nats.Conn, subject string, body []byte) {
	t.Helper()
	if msg, err := nc.Request(subject, body, 150*time.Millisecond); err == nil {
		t.Fatalf("expected no reply on %s, got %q", subject, string(msg.Data))
	} else if !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("expected timeout/no reply on %s, got %v", subject, err)
	}
}

// TestD8ReviewPushCommitTrackerMissIsSilent pins the D8 §9 continuation routing contract:
// non-origin brokers that do not hold the transfer tracker entry must stay silent. In a routed
// NATS cluster every broker receives push-commit; if a non-home broker replies
// transfer_unknown, ctl can consume that error before the real home broker's OK.
func TestD8ReviewPushCommitTrackerMissIsSilent(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	actor := freshUserActor(t)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}, transfers: newTransferTracker(), selfID: "node-B"}
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")

	subj := proto.SubjCmdBy("lab", actor, "lab-1", "push-commit")
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { b.handlePushCommitReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.TransferCommitReq{TransferID: "tid-missing", Bucket: "xfer-lab", ObjectKey: "tid-missing"})
	expectNoReply(t, nc, subj, body)
}

// TestD8ReviewFinalizeTrackerMissIsSilent is the same tracker-presence rule for
// ctrl.by.*.transfer.*.finalize.req. The sender ignores finalize best-effort errors in some
// paths, but a spurious transfer_unknown reply still violates §9 and can surface as a false
// pull/finalize refusal in paths that check sendFinalize's error.
func TestD8ReviewFinalizeTrackerMissIsSilent(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	actor := freshUserActor(t)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}, transfers: newTransferTracker(), selfID: "node-B"}
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")

	subj := proto.SubjCtrlTransferFinalize(actor, "lab", "tid-missing")
	sub, err := nc.Subscribe(subj, b.handleFinalizeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.TransferFinalize{Kind: "complete", TransferID: "tid-missing", Tier: "b"})
	expectNoReply(t, nc, subj, body)
}
