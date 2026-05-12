package broker

import (
	"context"
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/nats-io/nats.go"
)

// handleFinalizeReq is invoked on
// `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req`.
//
// Pull receiver-side: ctl has finished local writing (tier A inline
// or tier B Get+rename). It tells the broker so audit complete|failed
// can be written and the bucket cleaned up. Two layers of defence:
//
//  1. NATS-layer: the JWT pub allow is scoped to (sid, actor) via
//     `ctrl.by.<actor>.s.<sid>.transfer.*.finalize.req`. A different
//     sid never reaches here. A different actor publishing for sid
//     reaches here only via subject forgery — which is impossible
//     because the broker parses the actor segment, not the body.
//  2. Application-layer (this function): the in-memory entry for
//     transfer_id is looked up; the publishing actor MUST match the
//     creator. Mismatch → not_owner_or_creator (Round-3 #1).
//
// Idempotent on duplicate finalize (Round-3 case #18): the second
// caller sees finalized=true and gets OK back without re-writing
// audit / re-deleting bucket.
//
// file-transfer-plan §Wire / §Audit / §Risk Round-4 #1.
func (b *Broker) handleFinalizeReq(msg *nats.Msg) {
	actor, sid, transferID, ok := proto.ParseTransferFinalize(msg.Subject)
	if !ok {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "subject_malformed"})
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "actor_invalid", Error: err.Error()})
		return
	}
	if code := b.transferGateNoNode(sid, fp); code != "" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: code})
		return
	}

	var fin proto.TransferFinalize
	if err := json.Unmarshal(msg.Data, &fin); err != nil {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	if fin.TransferID != "" && fin.TransferID != transferID {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "request_invalid", Error: "transfer_id mismatch subject vs body"})
		return
	}
	if fin.Kind != "complete" && fin.Kind != "failed" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "request_invalid", Error: "kind must be complete|failed"})
		return
	}

	// Application-layer ownership + idempotency.
	entry, claimed := b.transfers.markFinalized(transferID)
	if entry == nil {
		// Either watchdog already reaped, or an unknown transfer_id.
		// Either way: idempotent reply, no audit double-write.
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "transfer_unknown"})
		return
	}
	if entry.actor != actor {
		// Don't unclaim — if the actor is wrong this transfer should
		// stay in flight for the real owner / watchdog.
		entry.finalized = false
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "not_owner_or_creator"})
		return
	}
	if entry.verb != "pull" {
		entry.finalized = false
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "verb_mismatch", Error: "finalize.req is pull-only"})
		return
	}
	if !claimed {
		// Already finalized — duplicate finalize from the same actor
		// is OK; reply success no-op.
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: true})
		return
	}

	// Resolve tier. ctl-supplied body wins over the optimistic "b" we
	// stamped at pull.req time (tier-A pulls have an empty bucket).
	tier := entry.tier
	if fin.Tier == "a" || fin.Tier == "b" {
		tier = fin.Tier
	}

	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: fin.Kind, Verb: "pull",
		Ts: b.cfg.Now(), Session: entry.sid, Node: entry.nid,
		ActorNkey: entry.actor, ActorFp: entry.actorFP,
		TransferID: entry.transferID, Path: entry.path,
		Tier: tier, Bucket: entry.bucket,
		Bytes: fin.Bytes, DurationMs: fin.DurationMs,
	}
	if fin.Kind == "failed" {
		rec.Code = fin.Code
		rec.Error = fin.Error
	}
	b.pubAuditTransfer(rec)
	_ = b.deleteXferBucket(context.Background(), entry.bucket)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)

	b.replyFinalize(msg, proto.TransferFinalizeResp{OK: true})
	b.cfg.Logger.Info("broker: pull finalize handled",
		"transfer_id", transferID, "kind", fin.Kind, "tier", tier)
}

// transferGateNoNode is the gate variant for finalize/caps subjects
// that don't carry a nid in their subject. Drops the node-online
// check; otherwise identical to transferGate.
func (b *Broker) transferGateNoNode(sid, fp string) string {
	if code := b.transferGate(sid, fp, "_unused_"); code != "" {
		// transferGate's node check uses LookupStatus which fails for
		// "_unused_"; ignore that one specific code and accept the
		// rest.
		if code == "node_not_found" || code == "node_offline" {
			return ""
		}
		return code
	}
	return ""
}

func (b *Broker) replyFinalize(msg *nats.Msg, resp proto.TransferFinalizeResp) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(resp)
	_ = msg.Respond(payload)
}
