package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// noStreamJS embeds jetstream.JetStream (nil) so it satisfies the large interface; only
// Publish is overridden to return ErrNoStreamResponse — what JetStream returns when no
// stream captures the subject (a deleted history-<sid>, OR a not-yet-ensured one). Any
// OTHER method call would nil-panic; the test only exercises Publish.
type noStreamJS struct{ jetstream.JetStream }

func (noStreamJS) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, jetstream.ErrNoStreamResponse
}

// transientJS returns a NON-no-stream error (a JS flap / timeout) that must be retried.
type transientJS struct{ jetstream.JetStream }

func (transientJS) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, errors.New("nats: jetstream temporarily unavailable")
}

// TestPublishAuditDeletedStreamIsBoundedLoss is the audit-M5 / transfer-F2 regression: a no-stream
// publish error is a BOUNDED LOSS (advance the cursor) ONLY when the session is gone (rm'd) — its
// history stream was permanently deleted. For a still-ACTIVE session a no-stream error is TRANSIENT
// (the stream is not-yet-ensured) and MUST stay R-22 (retry), or the queue-not-drop contract breaks.
func TestPublishAuditDeletedStreamIsBoundedLoss(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	gone := func(string) (bool, error) { return false, nil }  // session removed
	active := func(string) (bool, error) { return true, nil } // session still ACTIVE
	subj, sid := "tether.v2.s.lab.audit.transfer", "lab"

	// (a) no-stream + session GONE → bounded loss (nil), counter increments → cursor advances.
	pGone := &AuditPublisher{cfg: AuditPublisherConfig{JS: noStreamJS{}, Logger: quiet, SessionExists: gone}}
	if err := pGone.publishAudit(context.Background(), subj, []byte(`{}`), "qkey:xfer:0", 7, sid); err != nil {
		t.Fatalf("no-stream for a removed session must be a bounded loss (nil), got %v", err)
	}
	if got := pGone.DeletedStreamLossCount(); got != 1 {
		t.Fatalf("DeletedStreamLossCount = %d, want 1", got)
	}

	// (b) no-stream + session ACTIVE → TRANSIENT: propagate so the loop retries (R-22), no loss.
	pActive := &AuditPublisher{cfg: AuditPublisherConfig{JS: noStreamJS{}, Logger: quiet, SessionExists: active}}
	if err := pActive.publishAudit(context.Background(), subj, []byte(`{}`), "qkey:xfer:0", 7, sid); err == nil {
		t.Fatal("no-stream for an ACTIVE session must propagate (R-22 retry, stream not-yet-ensured), got nil")
	}
	if got := pActive.DeletedStreamLossCount(); got != 0 {
		t.Fatalf("an active-session no-stream must NOT count as a loss, got %d", got)
	}

	// (c) no SessionExists wired (tests / not wired) → conservative: never a loss → R-22 retry.
	pNil := &AuditPublisher{cfg: AuditPublisherConfig{JS: noStreamJS{}, Logger: quiet}}
	if err := pNil.publishAudit(context.Background(), subj, []byte(`{}`), "id", 7, sid); err == nil {
		t.Fatal("with no SessionExists checker, no-stream must propagate (preserve pre-M5 R-22), got nil")
	}

	// (d) a transient (non-no-stream) error MUST propagate so the loop retries — NOT a loss.
	pTrans := &AuditPublisher{cfg: AuditPublisherConfig{JS: transientJS{}, Logger: quiet, SessionExists: gone}}
	if err := pTrans.publishAudit(context.Background(), subj, []byte(`{}`), "id", 7, sid); err == nil {
		t.Fatal("transient publish error must propagate (retry), got nil")
	}
	if got := pTrans.DeletedStreamLossCount(); got != 0 {
		t.Fatalf("transient error must NOT count as a deleted-stream loss, got %d", got)
	}
}
