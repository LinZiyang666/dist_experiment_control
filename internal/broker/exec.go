package broker

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// handleExecReq is invoked for `s.<sid>.cmd.by.<actor>.node.<nid>.exec.req`.
// Order of operations (architecture C.1, C.4):
//
//  1. parse subject; reject malformed.
//  2. C.1 §6 precheck: session exists + ACTIVE.
//  3. actor must be a member of sid (member or owner).
//  4. forward to `s.<sid>.cmd.node.<nid>.exec.req.forwarded` PRESERVING
//     the original ctl reply inbox so the agent's chunks land directly
//     on the ctl's _INBOX (and the broker doesn't sit in the data
//     stream — fewer hops, less latency, less broker memory).
//  5. write audit.call (single-writer rule, C.1 §4).
//
// On any pre-forward failure, broker replies with an `ExecChunk{kind:error}`
// so the ctl gets a clean message instead of a NATS timeout.
func (b *Broker) handleExecReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "exec" {
		b.replyExecErr(msg, "subject_malformed: "+msg.Subject)
		return
	}

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyExecErr(msg, "actor_invalid: "+err.Error())
		return
	}

	// C.1 §6 precheck.
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyExecErr(msg, "store_error: "+err.Error())
		return
	}
	if !active {
		b.replyExecErr(msg, "session_not_found_or_deleting")
		return
	}

	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyExecErr(msg, "store_error: "+err.Error())
		return
	}
	if !member {
		b.replyExecErr(msg, "not_a_member")
		return
	}

	// Forward to agent. We preserve msg.Reply so chunks go straight to ctl.
	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    msg.Data,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyExecErr(msg, "forward_failed: "+err.Error())
		return
	}

	b.cfg.Logger.Info("broker: exec forwarded",
		"sid", sid, "nid", nid, "actor", actor, "fp", fp)

	// audit.call (single-writer rule C.1 §4).
	b.pubAuditCall(sid, fp, actor, "exec", nid, true, "")
}

// replyExecErr replies on the ctl's inbox with an ExecChunk{kind:error}.
// We use the same envelope as the agent's streaming reply so the ctl
// has exactly one decoder path.
func (b *Broker) replyExecErr(msg *nats.Msg, reason string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.ExecChunk{Kind: "error", Error: reason})
	_ = msg.Respond(payload)
}

// handleProcEvent is invoked for `s.<sid>.ev.node.<nid>.proc.<pid>.<kind>`
// (kind = started | exit). The broker is the SQLite single-writer; the
// agent only ships the runtime fact, the broker turns it into a row.
func (b *Broker) handleProcEvent(msg *nats.Msg) {
	sid, nid, pid, kind, ok := proto.ParseEvProc(msg.Subject)
	if !ok {
		return
	}
	switch kind {
	case "started":
		var ev proto.ProcStartedEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			b.cfg.Logger.Warn("broker: proc.started parse", "err", err)
			return
		}
		err := proc.Insert(b.cfg.DB, proc.Process{
			PID: pid, SID: sid, NID: nid,
			Argv:        ev.Argv,
			StartedAt:   ev.StartedAt,
			StartedByFP: ev.StartedByFP,
		})
		if err != nil && !errors.Is(err, proc.ErrNodeMissing) {
			b.cfg.Logger.Warn("broker: proc.started insert", "err", err, "pid", pid)
		}
		b.pubAuditProc(sid, "start", nid, pid, ev.Argv, 0, ev.StartedAt)

	case "exit":
		var ev proto.ProcExitEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			b.cfg.Logger.Warn("broker: proc.exit parse", "err", err)
			return
		}
		if err := proc.MarkExited(b.cfg.DB, pid, ev.ExitCode, ev.EndedAt); err != nil {
			b.cfg.Logger.Warn("broker: proc.exit mark", "err", err, "pid", pid)
		}
		b.pubAuditProc(sid, "exit", nid, pid, nil, ev.ExitCode, ev.EndedAt)
	}
}

// handlePsReq replies with a PsResp built from internal/proc.ListBySession.
// Subject layout: `ctrl.by.<actor>.s.<sid>.ps.req`. Architecture F.8 says
// `tether ps` is read-only and never goes through agent forwarding.
func (b *Broker) handlePsReq(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.PsResp{Code: "subject_malformed"})
		return
	}
	// leaf = "s.<sid>.ps.req"
	parts := splitDot(leaf)
	if len(parts) != 4 || parts[0] != "s" || parts[2] != "ps" || parts[3] != "req" {
		b.replyJSON(msg, proto.PsResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !member {
		b.replyJSON(msg, proto.PsResp{Code: "not_a_member"})
		return
	}

	procs, err := proc.ListBySession(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	out := make([]proto.PsEntry, 0, len(procs))
	for _, p := range procs {
		entry := proto.PsEntry{
			PID:         p.PID,
			NID:         p.NID,
			Argv:        p.Argv,
			StartedAt:   p.StartedAt,
			Status:      string(p.Status),
			StartedByFP: p.StartedByFP,
		}
		if p.EndedAt != nil {
			entry.EndedAt = *p.EndedAt
		}
		if p.ExitCode != nil {
			entry.ExitCode = *p.ExitCode
		}
		out = append(out, entry)
	}
	b.replyJSON(msg, proto.PsResp{Processes: out})
}

// splitDot exists so we don't import "strings" just for one Split call
// (sessions.go has its own; keeping per-file independence).
func splitDot(s string) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// pubAuditCall emits an `audit.call` JetStream candidate (in P4 still
// core pub; P7 promotes to JS). Architecture H.5 schema.
func (b *Broker) pubAuditCall(sid, actorFP, actorNkey, verb, nid string, ok bool, errMsg string) {
	type auditCall struct {
		V         int       `json:"v"`
		Kind      string    `json:"kind"`
		Ts        time.Time `json:"ts"`
		ActorNkey string    `json:"actor_nkey"`
		ActorFp   string    `json:"actor_fp"`
		Session   string    `json:"session"`
		Node      string    `json:"node,omitempty"`
		Verb      string    `json:"verb"`
		OK        bool      `json:"ok"`
		Error     string    `json:"error,omitempty"`
	}
	payload, err := json.Marshal(auditCall{
		V: 1, Kind: "call", Ts: b.cfg.Now(),
		ActorNkey: actorNkey, ActorFp: actorFP,
		Session: sid, Node: nid, Verb: verb,
		OK: ok, Error: errMsg,
	})
	if err != nil {
		return
	}
	if err := b.publishOnConn(proto.SubjAuditCall(sid), payload); err != nil {
		b.cfg.Logger.Warn("broker: audit.call pub", "err", err, "sid", sid)
	}
}

// pubAuditProc emits an `audit.proc` event derived from the agent's
// runtime ev.proc.* notifications.
func (b *Broker) pubAuditProc(sid, kind, nid, pid string, argv []string, exitCode int, ts time.Time) {
	type auditProc struct {
		V        int       `json:"v"`
		Kind     string    `json:"kind"`
		Ts       time.Time `json:"ts"`
		Session  string    `json:"session"`
		Node     string    `json:"node"`
		PID      string    `json:"pid"`
		ExitCode *int      `json:"exit_code,omitempty"`
		Cmd      string    `json:"cmd,omitempty"`
	}
	rec := auditProc{
		V: 1, Kind: kind, Ts: ts,
		Session: sid, Node: nid, PID: pid,
	}
	if kind == "exit" {
		rec.ExitCode = &exitCode
	}
	if kind == "start" && len(argv) > 0 {
		// Stringify argv with simple shell-ish join. Audit is informational.
		joined := argv[0]
		for _, a := range argv[1:] {
			joined += " " + a
		}
		rec.Cmd = joined
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := b.publishOnConn(proto.SubjAuditProc(sid), payload); err != nil {
		b.cfg.Logger.Warn("broker: audit.proc pub", "err", err, "sid", sid, "pid", pid)
	}
}
