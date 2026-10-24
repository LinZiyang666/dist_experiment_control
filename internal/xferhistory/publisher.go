package xferhistory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
	"unicode"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These classifications retain publication responsibility at the caller. In
// particular, Unknown never means that it is safe to retire a staged record.
var (
	ErrConflict       = errors.New(xfercontract.CodeTerminalConflict)
	ErrUnknown        = errors.New(xfercontract.CodeResultUnknown)
	ErrStorageHorizon = errors.New(xfercontract.CodeStorageHorizon)
)

type publisher struct{ cfg Config }

var _ Factory = New

// New borrows the current JetStream binding. It creates no connection, worker,
// consumer or cached receipt: every confirmation must consult durable history.
// origin: hot-update S7 plan §5.2 / T25
func New(cfg Config) (Publisher, error) {
	if cfg.JS == nil || !validToken(cfg.SID) {
		return nil, fmt.Errorf("%w: history requires JetStream and a session token", ErrUnknown)
	}
	return &publisher{cfg: cfg}, nil
}

func (p *publisher) Confirm(record xfercontract.CanonicalRecord, deadline controlbudget.Deadline) (xfercontract.HistoryReceipt, error) {
	// Own the request bytes throughout readback. The owner must retain the same
	// canonical decision across calls; this copy prevents SDK header/body reuse.
	record.Bytes = bytes.Clone(record.Bytes)
	decoded, continuation, err := decodeRecord(record.Bytes)
	if err != nil || !continuation || record.Identity.SID != p.cfg.SID ||
		record.Identity != decoded.Identity || record.Kind != decoded.Kind ||
		record.Digest != decoded.Digest || record.ReqID != decoded.ReqID {
		return xfercontract.HistoryReceipt{}, fmt.Errorf("%w: invalid canonical transfer record", ErrConflict)
	}
	ctx, cancel, err := operationContext(deadline)
	if err != nil {
		return xfercontract.HistoryReceipt{}, err
	}
	defer cancel()
	key := xfercontract.Key{SID: record.Identity.SID, TransferID: record.Identity.TransferID}
	for {
		set, stream, err := p.scan(ctx, key)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		receipt, found, err := exactReceipt(set, record)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		if found {
			p.observe(record, set.ScannedThrough, receipt)
			return receipt, nil
		}
		// origin: hot-update S7 plan §5.2 / T25
		// H belongs to this attempt, including a packet still in flight after
		// timeout. Two attempts that both observed absence must compete on H;
		// MsgID alone cannot protect them beyond the server's dedupe window.
		ack, err := p.cfg.JS.PublishMsg(ctx, &nats.Msg{
			Subject: proto.SubjAuditTransfer(key.SID), Data: record.Bytes,
		}, jetstream.WithMsgID(record.ReqID), jetstream.WithExpectStream(stream),
			jetstream.WithExpectLastSequencePerSubject(set.ScannedThrough))
		if err != nil {
			var api *jetstream.APIError
			if errors.As(err, &api) && api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				// Another transfer shares this subject. Re-scan under the original
				// deadline, never switch to stream CAS (call audits must not starve us).
				continue
			}
			return xfercontract.HistoryReceipt{}, fmt.Errorf("%w: publish transfer history: %w", ErrUnknown, err)
		}
		if ack == nil || ack.Stream != stream || ack.Sequence == 0 {
			return xfercontract.HistoryReceipt{}, fmt.Errorf("%w: invalid history publish receipt", ErrUnknown)
		}
		// Even a successful/deduplicated PubAck is not exact history proof.
		// Re-read the whole matching set: an old MsgID may name different bytes,
		// and no first-terminal shortcut may hide a second physical terminal.
		confirmed, _, err := p.scan(ctx, key)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		receipt, found, err = exactReceipt(confirmed, record)
		if err != nil {
			return xfercontract.HistoryReceipt{}, err
		}
		if !found || receipt.Stream != ack.Stream || receipt.Sequence != ack.Sequence {
			return xfercontract.HistoryReceipt{}, fmt.Errorf("%w: publish receipt lacks exact history readback", ErrUnknown)
		}
		p.observe(record, set.ScannedThrough, receipt)
		return receipt, nil
	}
}

func (p *publisher) Lookup(key xfercontract.Key, deadline controlbudget.Deadline) (xfercontract.HistorySet, error) {
	if key.SID != p.cfg.SID || !validToken(key.TransferID) {
		return xfercontract.HistorySet{}, fmt.Errorf("%w: history lookup scope mismatch", ErrConflict)
	}
	ctx, cancel, err := operationContext(deadline)
	if err != nil {
		return xfercontract.HistorySet{}, err
	}
	defer cancel()
	set, _, err := p.scan(ctx, key)
	return set, err
}

// exactReceipt runs only after a complete scan. Counts are checked for BOTH
// slots, even when the caller only asks to acknowledge START.
// origin: hot-update S7 plan §5.2 / T25
func exactReceipt(set xfercontract.HistorySet, record xfercontract.CanonicalRecord) (xfercontract.HistoryReceipt, bool, error) {
	if !set.Complete {
		return xfercontract.HistoryReceipt{}, false, fmt.Errorf("%w: incomplete history scan", ErrUnknown)
	}
	if len(set.Start) > 1 || len(set.Terminal) > 1 {
		return xfercontract.HistoryReceipt{}, false, fmt.Errorf("%w: multiple physical history records in one slot", ErrConflict)
	}
	var match *xfercontract.CanonicalRecord
	for _, rows := range [][]xfercontract.CanonicalRecord{set.Start, set.Terminal} {
		for i := range rows {
			row := &rows[i]
			if row.Identity != record.Identity {
				return xfercontract.HistoryReceipt{}, false, fmt.Errorf("%w: history identity differs", ErrConflict)
			}
			if (row.Kind == "start") == (record.Kind == "start") {
				match = row
			}
		}
	}
	if match == nil {
		return xfercontract.HistoryReceipt{}, false, nil
	}
	if match.Kind != record.Kind || match.ReqID != record.ReqID ||
		match.Digest != record.Digest || !bytes.Equal(match.Bytes, record.Bytes) {
		return xfercontract.HistoryReceipt{}, false, fmt.Errorf("%w: canonical history differs", ErrConflict)
	}
	for _, receipt := range set.Receipts {
		if receipt.RecordDigest == record.Digest && receipt.Stream != "" && receipt.Sequence != 0 {
			return receipt, true, nil
		}
	}
	return xfercontract.HistoryReceipt{}, false, fmt.Errorf("%w: missing physical history receipt", ErrUnknown)
}

func operationContext(deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: history deadline: %w", ErrUnknown, err)
	}
	// One entire Confirm/Lookup pass, including all CAS retries, shares the
	// settlement bound. No network request can renew the caller's budget.
	remaining = min(remaining, 5*time.Second)
	// ctx-none: the frozen history port supplies an absolute deadline, not a context.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	return ctx, cancel, nil
}

func validToken(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r == '.' || r == '*' || r == '>' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (p *publisher) observe(record xfercontract.CanonicalRecord, expected uint64, receipt xfercontract.HistoryReceipt) {
	if p.cfg.Observe != nil {
		// The binding owner supplies common process/cut/event metadata. This
		// leaf supplies the independently readable physical history identity.
		p.cfg.Observe(xfercontract.Observation{
			Kind: xfercontract.ObserveHistory, TransferID: record.Identity.TransferID,
			EntryNonce: record.Identity.EntryNonce, ObjectVersion: record.Identity.ObjectVersion,
			ExpectedSubjectSeq: expected, ActualACKSeq: receipt.Sequence, SHA256: record.Digest,
			StreamIdentity: receipt.Stream,
			HistorySeq:     receipt.Sequence, CanonicalDigest: receipt.RecordDigest,
		})
	}
}
