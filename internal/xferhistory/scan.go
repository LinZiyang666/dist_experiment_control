package xferhistory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// scan is a raw next-by-subject traversal, not a consumer and not a walk over
// the entire history stream. Call/proc/port audit volume must not amplify a
// recovery pass, and a timeout can never stand in for authoritative absence.
// origin: hot-update S7 plan §5.2 / T25
func (p *publisher) scan(ctx context.Context, key xfercontract.Key) (xfercontract.HistorySet, string, error) {
	var set xfercontract.HistorySet
	subject := proto.SubjAuditTransfer(key.SID)
	name, err := p.cfg.JS.StreamNameBySubject(ctx, subject)
	if err != nil {
		return set, "", fmt.Errorf("%w: locate transfer history: %w", ErrUnknown, err)
	}
	stream, err := p.cfg.JS.Stream(ctx, name)
	if err != nil {
		return set, name, fmt.Errorf("%w: open transfer history: %w", ErrUnknown, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return set, name, fmt.Errorf("%w: inspect history retention: %w", ErrUnknown, err)
	}
	// origin: hot-update S7 plan §5.2 / T25
	// CAS needs a subject whose evidence cannot age out or roll back to zero.
	// MaxBytes with DiscardNew is safe: fullness fails the write, retaining the
	// outbox. We never rewrite an operator's configuration to obtain admission.
	cfg := info.Config
	if cfg.MaxAge != 0 || cfg.Discard != jetstream.DiscardNew ||
		cfg.Retention != jetstream.LimitsPolicy || cfg.MaxMsgs > 0 || cfg.MaxMsgsPerSubject > 0 ||
		cfg.AllowRollup || cfg.AllowMsgTTL {
		return set, name, fmt.Errorf("%w: history requires MaxAge=0, DiscardNew, LimitsPolicy, unlimited MaxMsgs/MaxMsgsPerSubject and no rollup/message TTL", ErrStorageHorizon)
	}
	last, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		set.Complete = true
		return set, name, nil
	}
	if err != nil {
		return set, name, fmt.Errorf("%w: read transfer subject boundary: %w", ErrUnknown, err)
	}
	if last.Subject != subject || last.Sequence == 0 {
		return set, name, fmt.Errorf("%w: invalid transfer subject boundary", ErrUnknown)
	}
	set.ScannedThrough = last.Sequence
	for seq := uint64(1); ; {
		msg, err := stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if err != nil {
			// H was observed, so not-found before reaching H is missing evidence,
			// not EOF. This also catches a purge racing the bounded scan.
			return set, name, fmt.Errorf("%w: incomplete transfer history: %w", ErrUnknown, err)
		}
		if msg.Subject != subject || msg.Sequence < seq || msg.Sequence > set.ScannedThrough {
			return set, name, fmt.Errorf("%w: transfer history changed during scan", ErrUnknown)
		}
		record, _, err := decodeRecord(msg.Data)
		if err != nil {
			// An unreadable row cannot be proved unrelated. Retain responsibility
			// rather than skipping malformed bytes and publishing a duplicate.
			return set, name, fmt.Errorf("%w: unreadable transfer history at sequence %d", ErrUnknown, msg.Sequence)
		}
		if record.Identity.SID != key.SID {
			return set, name, fmt.Errorf("%w: history subject and record scope differ", ErrConflict)
		}
		if record.Identity.TransferID == key.TransferID {
			if record.Kind == "start" {
				set.Start = append(set.Start, record)
			} else {
				set.Terminal = append(set.Terminal, record)
			}
			set.Receipts = append(set.Receipts, xfercontract.HistoryReceipt{
				Stream: name, Sequence: msg.Sequence, RecordDigest: record.Digest,
			})
		}
		if msg.Sequence == set.ScannedThrough {
			set.Complete = true
			return set, name, nil
		}
		seq = msg.Sequence + 1
	}
}

// decodeRecord preserves the original bytes, including additive audit fields.
// Re-marshalling today's schema here would silently discard a future field and
// turn different decisions into the same receipt. Legacy rows remain visible
// in Lookup, but Confirm requires a continuation identity and cannot certify a
// legacy best-effort event as a v1 acknowledgement. A cap0 row whose broker
// retained an admission identity is still marked best_effort in its own bytes:
// its exact receipt proves that row is in history, never v1 receiver authority.
// origin: hot-update S7 plan §4 / §5.2 / T25
func decodeRecord(body []byte) (xfercontract.CanonicalRecord, bool, error) {
	var audit struct {
		schema.AuditTransfer
		Continuation *xfercontract.Identity `json:"continuation"`
	}
	if err := json.Unmarshal(body, &audit); err != nil {
		return xfercontract.CanonicalRecord{}, false, err
	}
	if audit.V != schema.AuditSchemaVersion || audit.Ts.IsZero() ||
		!validToken(audit.Session) || !validToken(audit.Node) || !validToken(audit.TransferID) ||
		(audit.Kind != "start" && audit.Kind != "complete" && audit.Kind != "failed") {
		return xfercontract.CanonicalRecord{}, false, ErrConflict
	}
	identity := xfercontract.Identity{
		SID: audit.Session, NID: audit.Node, ActorFP: audit.ActorFp, TransferID: audit.TransferID,
	}
	// TransferContinuation is true when ANY flat continuation field is present,
	// so a row carrying only part of them enters this branch and is rejected by
	// the canonical validation below instead of being classified as legacy.
	// origin: hot-update S7 plan §5.2 / T13 / INTEG-E canonical storage regression
	continuation := schema.TransferContinuation(audit.AuditTransfer)
	if continuation {
		// Use the same flat identity/terminal validation as the Raft producer.
		// A private nested envelope would reject valid START/TERMINAL records
		// before publication and turn replay into a permanently pending outbox.
		// The complete and failed decisions compete for the SAME terminal slot.
		// A self-consistent digest with a different terminal ID is still invalid.
		// origin: hot-update S7 plan §5.2 / T23 / T24 / T25
		canonical, err := schema.CanonicalTransferAudit(audit.AuditTransfer)
		if err != nil {
			return xfercontract.CanonicalRecord{}, false, ErrConflict
		}
		identity = canonical.Identity
		// A best-effort cap0 row was validated to carry NO agent/intent identity;
		// it must still decode, or one cap0 transfer would make every exact scan
		// of its session unreadable for v1 publication.
		// origin: hot-update S7 plan §4 / §5.2 / §9 (S7 integration oplegacy F2)
		if !validToken(identity.ActorFP) || !validToken(identity.EntryNonce) ||
			!audit.BestEffort && (!validToken(identity.AgentInstance) || !validToken(identity.IntentDigest)) ||
			(identity.ObjectVersion != "" && !validToken(identity.ObjectVersion)) {
			return xfercontract.CanonicalRecord{}, false, ErrConflict
		}
	}
	// Flat fields are authoritative. An old private envelope may not smuggle
	// a second identity into otherwise valid exact history bytes, nor turn a
	// nested-only v1 record into a legacy event.
	// origin: hot-update S7 plan §5.2 / T25 / INTEG-R storage regression
	if audit.Continuation != nil && (!continuation || *audit.Continuation != identity) {
		return xfercontract.CanonicalRecord{}, false, ErrConflict
	}

	return xfercontract.CanonicalRecord{
		Identity: identity, Kind: audit.Kind, Bytes: body,
		Digest: digest(body), ReqID: digest(append([]byte("xferaudit-record:"), body...)),
	}, continuation, nil
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
