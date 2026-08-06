package broker

import (
	"encoding/json"
	"strings"

	"github.com/LinZiyang666/tether/internal/proto"
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
// origin: line-2 review M14. exec.go's handleExecReq carries the full argument for this pair.
//
//nolint:dupl // paired with handleExecReq in exec.go; argument there.
func (b *Broker) handleRunReq(nc *nats.Conn, msg *nats.Msg) {
	ing, den, ok := b.admitSubject(msg.Subject, runSpec)
	if !ok {
		b.replyRunFailed(msg, den.reason)
		return
	}
	if b.isClusterFollower() {
		return
	}
	if den, ok := b.admitACL(&ing, runSpec); !ok {
		b.replyRunFailed(msg, den.reason)
		// Node refusals only — same asymmetry as exec, and the same one kill does NOT share.
		if den.code == "node_not_found" || den.code == "node_offline" {
			b.pubAuditCall(ing.sid, ing.fp, ing.actor, "run", ing.nid, false,
				auditRefusal(den), msg.Reply, nil)
		}
		return
	}
	sid, actor, nid, fp, verb := ing.sid, ing.actor, ing.nid, ing.fp, ing.verb

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
	b.respondBytes(msg, payload)
}

// handleKillReq forwards a Ctrl-C signal request. Same pre-flight as
// run.req — kill MUST hit the same auth gates because a member who lost
// authority should not be able to take down a process started by
// someone else.
func (b *Broker) handleKillReq(nc *nats.Conn, msg *nats.Msg) {
	ing, den, ok := b.admitSubject(msg.Subject, killSpec)
	if !ok {
		b.replyKillFailed(msg, den.reason)
		return
	}
	if b.isClusterFollower() {
		return
	}
	if den, ok := b.admitACL(&ing, killSpec); !ok {
		// NO audit row on ANY refusal — including the node refusals where run, forty lines
		// above with a doc comment claiming "same pre-flight as run.req", emits one. Nothing
		// in the history says whether that is a decision or an omission; admit() reproduces it
		// rather than quietly making the two agree. Pinned by ingress_characterization_test.go.
		b.replyKillFailed(msg, den.reason)
		return
	}
	sid, actor, nid, fp, verb := ing.sid, ing.actor, ing.nid, ing.fp, ing.verb

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
	b.respondBytes(msg, payload)
}

// handlePtyFailed subscribes to `s.*.pty.*.failed` and writes
// audit.proc{kind:<reason>}. Architecture C.5.1 says broker (not agent)
// is the audit single-writer for attach_timeout / pty_alloc_failed /
// exec_failed (as for any other proc lifecycle event).
func (b *Broker) handlePtyFailed(msg *nats.Msg) {
	// Subject layout: tether.v2.s.<sid>.pty.<pid>.failed (7 tokens).
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 7 || parts[0] != "tether" || parts[1] != proto.SubjectVersionToken ||
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
	// Audit shard 01 F8: use cfg.Now (test seam) not raw time.Now,
	// so frozen-clock tests see deterministic audit timestamps.
	b.pubAuditProc(sid, ev.Reason, "" /* nid unknown from this subject */, ev.PID, nil, 0, b.cfg.Now())
}
