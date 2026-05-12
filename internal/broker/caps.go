package broker

import (
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// handleCapsReq replies to `ctrl.by.<actor>.s.<sid>.caps.req` with
// broker capabilities the CLI needs before chooseTier:
//
//   - JetStreamReady: whether tier-B (ObjectStore) is usable.
//   - MaxPayload: the server-advertised max NATS payload (bytes).
//     CLI uses this to cap tier-A inline_data to a value the actual
//     server will accept (operator may have left max_payload at the
//     1 MiB default even though the design ceiling is 8 MiB).
//   - BrokerRelease / BrokerProto: lets ctl print a useful version-
//     skew message instead of hanging on a wrong-proto agent.
//
// Membership gate mirrors handlePsReq — sid is parsed from the subject
// and validated against the session row + the actor's fp.
//
// file-transfer-plan §Wire protocol "caps".
func (b *Broker) handleCapsReq(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.CapsResp{Code: "subject_malformed"})
		return
	}
	parts := splitDot(leaf)
	// leaf = "s.<sid>.caps.req"
	if len(parts) != 4 || parts[0] != "s" || parts[2] != "caps" || parts[3] != "req" {
		b.replyJSON(msg, proto.CapsResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !active {
		b.replyJSON(msg, proto.CapsResp{Code: "session_not_found_or_deleting"})
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !member {
		b.replyJSON(msg, proto.CapsResp{Code: "not_a_member"})
		return
	}

	resp := proto.CapsResp{
		OK:             true,
		JetStreamReady: b.js != nil,
		BrokerRelease:  proto.ReleaseVersion,
		BrokerProto:    proto.ProtoVersion,
	}
	if b.nc != nil {
		resp.MaxPayload = b.nc.MaxPayload()
	}
	b.replyJSON(msg, resp)
}
