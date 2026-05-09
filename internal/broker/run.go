package broker

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// handleRunReq is the broker side of the P5 PTY run flow. Mirrors
// handleExecReq's pre-flight (parse / actor fp / IsActive / IsMember /
// node ONLINE), then re-marshals proto.RunReq with broker-stamped
// ActorFP and forwards to the agent. Lifecycle chunks (ready / started
// / exit / failed) flow back on msg.Reply directly from the agent —
// broker is not in that data path. PTY byte streams flow on a separate
// pty.<pid>.* subject family that broker also doesn't touch.
//
// On any pre-forward failure, broker replies with a RunChunk{Kind:failed}
// so ctl gets a clean lifecycle message rather than a NATS timeout.
func (b *Broker) handleRunReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "run" {
		b.replyRunFailed(msg, "subject_malformed")
		return
	}

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyRunFailed(msg, "actor_invalid: "+err.Error())
		return
	}

	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyRunFailed(msg, "store_error: "+err.Error())
		return
	}
	if !active {
		b.replyRunFailed(msg, "session_not_found_or_deleting")
		return
	}

	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyRunFailed(msg, "store_error: "+err.Error())
		return
	}
	if !member {
		b.replyRunFailed(msg, "not_a_member")
		return
	}

	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		b.replyRunFailed(msg, "node_not_found")
		b.pubAuditCall(sid, fp, actor, "run", nid, false, "node_not_found", msg.Reply, nil)
		return
	case err != nil:
		b.replyRunFailed(msg, "store_error: "+err.Error())
		return
	case status != node.StateOnline:
		b.replyRunFailed(msg, "node_offline: status="+string(status))
		b.pubAuditCall(sid, fp, actor, "run", nid, false, "node_offline:"+string(status), msg.Reply, nil)
		return
	}

	// Re-marshal with broker-stamped ActorFP. Same C.1 §4 single-writer
	// rule as exec — agent never invents the fp.
	var req proto.RunReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyRunFailed(msg, "json_parse: "+err.Error())
		return
	}
	req.ActorFP = fp
	body, err := json.Marshal(&req)
	if err != nil {
		b.replyRunFailed(msg, "marshal: "+err.Error())
		return
	}

	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyRunFailed(msg, "forward_failed: "+err.Error())
		return
	}

	b.cfg.Logger.Info("broker: run forwarded",
		"sid", sid, "nid", nid, "actor", actor, "fp", fp)
	b.pubAuditCall(sid, fp, actor, "run", nid, true, "", msg.Reply, nil)
}

// replyRunFailed replies on the ctl's inbox with a RunChunk{Kind:failed}.
// Same envelope shape the agent uses, so ctl has a single decoder path.
func (b *Broker) replyRunFailed(msg *nats.Msg, reason string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.RunChunk{Kind: "failed", Reason: reason})
	_ = msg.Respond(payload)
}

// handleKillReq forwards a Ctrl-C signal request. Same pre-flight as
// run.req — kill MUST hit the same auth gates because a member who lost
// authority should not be able to take down a process started by
// someone else.
func (b *Broker) handleKillReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "kill" {
		b.replyKillFailed(msg, "subject_malformed")
		return
	}

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyKillFailed(msg, "actor_invalid: "+err.Error())
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyKillFailed(msg, "store_error: "+err.Error())
		return
	}
	if !active {
		b.replyKillFailed(msg, "session_not_found_or_deleting")
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyKillFailed(msg, "store_error: "+err.Error())
		return
	}
	if !member {
		b.replyKillFailed(msg, "not_a_member")
		return
	}
	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		b.replyKillFailed(msg, "node_not_found")
		return
	case err != nil:
		b.replyKillFailed(msg, "store_error: "+err.Error())
		return
	case status != node.StateOnline:
		b.replyKillFailed(msg, "node_offline: status="+string(status))
		return
	}

	var req proto.KillReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyKillFailed(msg, "json_parse: "+err.Error())
		return
	}
	req.ActorFP = fp
	body, err := json.Marshal(&req)
	if err != nil {
		b.replyKillFailed(msg, "marshal: "+err.Error())
		return
	}

	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyKillFailed(msg, "forward_failed: "+err.Error())
		return
	}
	b.pubAuditCall(sid, fp, actor, "kill", nid, true, "", msg.Reply, nil)
}

func (b *Broker) replyKillFailed(msg *nats.Msg, reason string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.KillResp{Code: "rejected", Error: reason})
	_ = msg.Respond(payload)
}

// handlePtyFailed subscribes to `s.*.pty.*.failed` and writes
// audit.proc{kind:<reason>}. Architecture C.5.1 says broker (not agent)
// is the audit single-writer for attach_timeout / pty_alloc_failed /
// exec_failed (as for any other proc lifecycle event).
func (b *Broker) handlePtyFailed(msg *nats.Msg) {
	// Subject layout: tether.v1.s.<sid>.pty.<pid>.failed (7 tokens).
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 7 || parts[0] != "tether" || parts[1] != "v1" ||
		parts[2] != "s" || parts[4] != "pty" || parts[6] != "failed" {
		return
	}
	sid := parts[3]

	var ev proto.PtyFailedEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		b.cfg.Logger.Warn("broker: pty.failed parse", "err", err)
		return
	}
	b.cfg.Logger.Info("broker: pty failed",
		"sid", sid, "pid", ev.PID, "reason", ev.Reason, "detail", ev.Detail)
	b.pubAuditProc(sid, ev.Reason, "" /* nid unknown from this subject */, ev.PID, nil, 0, time.Now().UTC())
}
