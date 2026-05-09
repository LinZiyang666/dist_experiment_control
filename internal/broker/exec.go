package broker

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
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

	// Pre-forward node check. Without this, a typoed nid or a
	// long-OFFLINE agent would land on `.req.forwarded` with no
	// subscriber and ctl would hang until its own timeout. Surface
	// the failure as an ExecChunk{kind:error} per the contract in
	// this handler's doc comment.
	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		b.replyExecErr(msg, "node_not_found")
		b.pubAuditCall(sid, fp, actor, "exec", nid, false, "node_not_found", msg.Reply, nil)
		return
	case err != nil:
		b.replyExecErr(msg, "store_error: "+err.Error())
		return
	case status != node.StateOnline:
		b.replyExecErr(msg, "node_offline: status="+string(status))
		b.pubAuditCall(sid, fp, actor, "exec", nid, false, "node_offline:"+string(status), msg.Reply, nil)
		return
	}

	// Re-marshal the body to stamp the broker-parsed actor fp. C.1 §4
	// requires actor attribution to originate at the broker — the agent
	// echoes ActorFP into ProcStartedEvent.StartedByFP, never invents one.
	var req proto.ExecReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyExecErr(msg, "json_parse: "+err.Error())
		return
	}
	req.ActorFP = fp
	body, err := json.Marshal(&req)
	if err != nil {
		b.replyExecErr(msg, "marshal: "+err.Error())
		return
	}

	// Forward to agent. We preserve msg.Reply so chunks go straight to ctl.
	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyExecErr(msg, "forward_failed: "+err.Error())
		return
	}

	b.cfg.Logger.Info("broker: exec forwarded",
		"sid", sid, "nid", nid, "actor", actor, "fp", fp)

	// audit.call (single-writer rule C.1 §4).
	b.pubAuditCall(sid, fp, actor, "exec", nid, true, "", msg.Reply, nil)
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
			Argv:           ev.Argv,
			StartedAt:      ev.StartedAt,
			StartedByFP:    ev.StartedByFP,
			BootID:         ev.BootID,
			StartTimeTicks: ev.StartTimeTicks,
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

// handleNodeListReq answers ctrl.by.<actor>.s.<sid>.node.list.req
// (architecture B.1 line 129) with the SQLite `nodes` rows for sid.
// Same membership + DELETING gate as handlePsReq; result includes
// every node ever registered (independent of process activity), so
// `tether node upgrade --all` can target ONLINE agents that have
// not exec'd anything.
func (b *Broker) handleNodeListReq(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.NodeListResp{Code: "subject_malformed"})
		return
	}
	// leaf = "s.<sid>.node.list.req"
	parts := splitDot(leaf)
	if len(parts) != 5 || parts[0] != "s" || parts[2] != "node" ||
		parts[3] != "list" || parts[4] != "req" {
		b.replyJSON(msg, proto.NodeListResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.NodeListResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.NodeListResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !active {
		b.replyJSON(msg, proto.NodeListResp{Code: "session_not_found_or_deleting"})
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.NodeListResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !member {
		b.replyJSON(msg, proto.NodeListResp{Code: "not_a_member"})
		return
	}

	all, err := node.List(b.cfg.DB)
	if err != nil {
		b.replyJSON(msg, proto.NodeListResp{Code: "store_error", Error: err.Error()})
		return
	}
	out := make([]proto.NodeListEntry, 0, len(all))
	for _, n := range all {
		if n.SID != sid {
			continue
		}
		out = append(out, proto.NodeListEntry{
			NID:             n.NID,
			Status:          n.Status,
			LastHeartbeatAt: n.LastHeartbeatAt,
			BootID:          n.BootID,
			ReleaseVersion:  n.ReleaseVersion,
			ProtoVersion:    n.ProtoVersion,
		})
	}
	b.replyJSON(msg, proto.NodeListResp{Nodes: out})
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

	// C.1 §6: every session-scoped ingress (P4 ps included, not just exec)
	// must reject DELETING sessions before touching membership / data.
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !active {
		b.replyJSON(msg, proto.PsResp{Code: "session_not_found_or_deleting"})
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
	// LOST is derived at read time per proc.go pkgdoc: a RUNNING row
	// whose owning node is OFFLINE shows as LOST in `tether ps`. The
	// underlying SQLite stays RUNNING — when the agent re-registers,
	// G.1 will resolve it to EXITED(rc=-1) (missed-exit) or back to
	// RUNNING (still alive). Cache one node-status lookup per (sid,
	// nid) seen in this list to avoid N round-trips for sessions
	// with many processes on the same node.
	nodeStatusCache := map[string]node.State{}
	procOut := make([]proto.PsEntry, 0, len(procs))
	for _, p := range procs {
		status := string(p.Status)
		if p.Status == proc.StateRunning {
			ns, cached := nodeStatusCache[p.NID]
			if !cached {
				ns, _ = node.LookupStatus(b.cfg.DB, sid, p.NID)
				nodeStatusCache[p.NID] = ns
			}
			if ns == node.StateOffline {
				status = string(proc.StateLost)
			}
		}
		entry := proto.PsEntry{
			PID:         p.PID,
			NID:         p.NID,
			Argv:        p.Argv,
			StartedAt:   p.StartedAt,
			Status:      status,
			StartedByFP: p.StartedByFP,
		}
		if p.EndedAt != nil {
			entry.EndedAt = *p.EndedAt
		}
		if p.ExitCode != nil {
			entry.ExitCode = *p.ExitCode
		}
		procOut = append(procOut, entry)
	}

	// P6 / architecture F.8 — same query also returns the ports.
	ports, err := port.ListBySession(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	portOut := make([]proto.PsPortEntry, 0, len(ports))
	for _, pa := range ports {
		portOut = append(portOut, proto.PsPortEntry{
			Port:        pa.Port,
			Name:        pa.Name,
			NID:         pa.NID,
			LocalPort:   pa.LocalPort,
			State:       string(pa.State),
			CreatedByFP: pa.CreatedByFP,
			CreatedAt:   pa.CreatedAt,
		})
	}
	b.replyJSON(msg, proto.PsResp{Processes: procOut, Ports: portOut})
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

// pubAuditCall emits an `audit.call` event using the schema package
// types so the on-the-wire JSON matches what consumers decode with
// schema.AuditCall (architecture H.5 — append-only contract). Always
// marshal through the schema struct, never an inline anonymous one;
// a future field rename then surfaces as a build error here rather
// than as a silent decoder-loses-fields bug at the consumer.
//
// reqID is the NATS reply inbox the caller is using (uniquely
// identifies the call across the whole broker process); empty
// when there's no reply context. target is verb-specific metadata
// (e.g. {"pid": "01H..."} for exec, {"port": 14022} for expose)
// so consumers of audit.call can join call→proc/port without
// guessing. Empty target is OK; the field is omitempty in JSON.
//
// Audit shard 03 F2: ReqID + Target were defined in schema but
// never populated by the broker, so `tether history` lost the
// ability to correlate call → proc / port. This populates both
// at every site.
func (b *Broker) pubAuditCall(
	sid, actorFP, actorNkey, verb, nid string,
	ok bool, errMsg string,
	reqID string, target map[string]any,
) {
	payload, err := json.Marshal(schema.AuditCall{
		V: schema.AuditSchemaVersion, Kind: "call", Ts: b.cfg.Now(),
		ActorNkey: actorNkey, ActorFp: actorFP,
		Session: sid, Node: nid, Verb: verb,
		ReqID: reqID, Target: target,
		OK: ok, Error: errMsg,
	})
	if err != nil {
		return
	}
	if err := b.publishAudit(proto.SubjAuditCall(sid), payload); err != nil {
		b.cfg.Logger.Warn("broker: audit.call pub", "err", err, "sid", sid)
	}
}

// pubAuditProc emits an `audit.proc` event using schema.AuditProc.
// `kind` ∈ {"start","exit","reconciled_closed","killed_orphan"}; for
// exit kinds the rc field is populated (json tag matches schema).
func (b *Broker) pubAuditProc(sid, kind, nid, pid string, argv []string, exitCode int, ts time.Time) {
	rec := schema.AuditProc{
		V: schema.AuditSchemaVersion, Kind: kind, Ts: ts,
		Session: sid, Node: nid, PID: pid,
	}
	if kind == "exit" || kind == "reconciled_closed" {
		ec := exitCode
		rec.RC = &ec
	}
	if kind == "start" && len(argv) > 0 {
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
	if err := b.publishAudit(proto.SubjAuditProc(sid), payload); err != nil {
		b.cfg.Logger.Warn("broker: audit.proc pub", "err", err, "sid", sid, "pid", pid)
	}
}
