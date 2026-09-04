package broker

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
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
// The dupl exemption for this handler and handleRunReq lives HERE rather than in .golangci.yml.
// origin: line-2 review M14 — the config-file form pinned the partner's LINE RANGE in a regex, and one
// added comment line in run.go turned `make lint` red in a broker hot path.
//
// This one IS real duplication: the ingress admission skeleton (admitSubject -> follower short-circuit
// -> admitACL -> node-refusal audit) copied across verb handlers, which is the debt the admit()
// consolidation exists to pay down. Exempted rather than merged because the merge is that
// consolidation's job, and because the handlers are NOT interchangeable — see the comment inside on the
// subject-shape check running BEFORE the follower short-circuit here, where expose does the opposite.
// A naive collapse would silently pick one order for all of them.
//
//nolint:dupl // paired with handleRunReq in run.go; argument above.
func (b *Broker) handleExecReq(nc *nats.Conn, msg *nats.Msg) {
	// The subject-shape check runs BEFORE the follower short circuit, matching this
	// handler's long-standing order: a follower still answers a malformed exec subject.
	// (expose deliberately does the opposite; see its own comment.)
	ing, den, ok := b.admitSubject(msg.Subject, execSpec)
	if !ok {
		b.replyExecErr(msg, den.reason)
		return
	}
	if b.isClusterFollower() {
		return
	}
	if den, ok := b.admitACL(&ing, execSpec); !ok {
		b.replyExecErr(msg, den.reason)
		// The node refusals — and ONLY those — carry an audit row here. The session and
		// membership refusals deliberately do not; that asymmetry predates admit() and is
		// pinned by ingress_characterization_test.go.
		if den.code == "node_not_found" || den.code == "node_offline" {
			b.pubAuditCall(ing.sid, ing.fp, ing.actor, "exec", ing.nid, false,
				auditRefusal(den), msg.Reply, nil)
		}
		return
	}
	sid, actor, nid, fp, verb := ing.sid, ing.actor, ing.nid, ing.fp, ing.verb

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
	b.respondBytes(msg, payload)
}

// handleProcEvent is invoked for `s.<sid>.ev.node.<nid>.proc.<pid>.<kind>`
// (kind = started | exit). The broker is the SQLite single-writer; the
// agent only ships the runtime fact, the broker turns it into a row.
//
// h1 C3: since h1 the agent's courier sends these as ACKed REQUESTS — when a
// Reply inbox is present the broker answers proto.ProcEventAck after the
// write settles, so the courier can stop retrying. A pre-h1 agent publishes
// with no Reply and takes the byte-identical old path (ackProcEvent no-ops).
//
// The ack rules encode two hard-won constraints (plan critique-1):
//
//   - unknown_pid ({OK:true} — "the row is gone, stop") may ONLY be derived
//     from the write path itself, i.e. the LEADER-COMMITTED view: in cluster
//     mode this handler can run on a follower whose RODB lags the `started`
//     insert, and a local not-found ack would delete the courier's entry for
//     a REAL process — re-creating zombie class (a) through the ack path.
//   - the LOCAL pre-read (b.read()) is used ONLY for audit dedup:
//     already_exited/already_recorded are MONOTONE facts (EXITED is terminal;
//     a row once inserted is never un-inserted short of GC), so a lagging
//     replica that sees them can trust them — and skipping the duplicate
//     pubAuditProc is what keeps a courier retry from double-writing audit.
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
		// Audit-dedup pre-read (monotone: rows are never un-inserted).
		//
		// SID-SCOPED — origin: prerelease audit round 2, E3. Unscoped, this
		// short-circuited on ANY session's row and told the sender so, which is a
		// cross-session pid-existence oracle reachable from a legal subject with
		// no write at all. The fence on the write path (proc.MarkExited) was
		// already scoped and its comment claimed the indistinguishability this
		// read was quietly handing back.
		if existing, gerr := proc.GetInSession(b.read().SQL(), sid, pid); gerr == nil && existing != nil {
			b.ackProcEvent(msg, proto.ProcEventAck{OK: true, Code: "already_recorded"})
			return
		}
		// D9 §3 (audit #4): cluster mode routes the proc record through raft; single
		// mode is the byte-identical direct mutator.
		err := b.recordProc(proc.Process{
			PID: pid, SID: sid, NID: nid,
			Argv:           ev.Argv,
			StartedAt:      ev.StartedAt,
			StartedByFP:    ev.StartedByFP,
			BootID:         ev.BootID,
			StartTimeTicks: ev.StartTimeTicks,
		})
		if err != nil {
			// audit broker-core F4: the proc row did NOT land (node missing, or — in cluster mode
			// — a forward/leadership failure), so do NOT publish a "start" audit for a process
			// with no authoritative DB record (best-effort audit must not claim a process that has
			// no row). A genuinely-running orphan is reconciled at the agent's next register
			// (reconcileOnRegister), which is the authoritative missed-proc path.
			if errors.Is(err, proc.ErrNodeMissing) {
				// Transient for the courier: the node row lands with the next
				// register, after which the retry (or the register snapshot)
				// succeeds. NOT an OK:true — dropping the entry here would
				// lose the start and turn the eventual exit into unknown_pid.
				b.ackProcEvent(msg, proto.ProcEventAck{OK: false, Code: "node_missing"})
				return
			}
			b.cfg.Logger.Warn("broker: proc.started insert", "err", err, "pid", pid)
			// origin: h1 external review F6. The store detail stays in the WARN
			// above; it must NOT ride the wire. testing-standards.md S4: a
			// reply carries the CODE, because SQLite text can leak paths and
			// schema shape, and the courier only needs OK=false to retry.
			b.ackProcEvent(msg, proto.ProcEventAck{OK: false, Code: "store_error"})
			return
		}
		b.pubAuditProc(sid, "start", nid, pid, ev.Argv, 0, ev.StartedAt)
		b.ackProcEvent(msg, proto.ProcEventAck{OK: true, Code: "recorded"})

	case "exit":
		var ev proto.ProcExitEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			b.cfg.Logger.Warn("broker: proc.exit parse", "err", err)
			return
		}
		// Audit-dedup pre-read (monotone: EXITED is terminal). Sid-scoped for the
		// same reason as the started arm above (round 2, E3): unscoped, this
		// answered `already_exited` for a FOREIGN exited pid where the fenced
		// writer one line below would have said `unknown_pid`.
		if existing, gerr := proc.GetInSession(b.read().SQL(), sid, pid); gerr == nil && existing != nil && existing.Status == proc.StateExited {
			b.ackProcEvent(msg, proto.ProcEventAck{OK: true, Code: "already_exited"})
			return
		}
		if err := b.markProcExited(pid, sid, ev.ExitCode, ev.EndedAt); err != nil {
			// Leader-committed not-found (direct in single mode; carried
			// across the forward as kind proc_not_found in cluster mode) is
			// the ONE terminal ack: the row was GC'd or never existed on the
			// authoritative view — nothing left to deliver.
			if errors.Is(err, proc.ErrNotFound) {
				b.ackProcEvent(msg, proto.ProcEventAck{OK: true, Code: "unknown_pid"})
				return
			}
			b.cfg.Logger.Warn("broker: proc.exit mark", "err", err, "pid", pid)
			// origin: h1 external review F6. The store detail stays in the WARN
			// above; it must NOT ride the wire. testing-standards.md S4: a
			// reply carries the CODE, because SQLite text can leak paths and
			// schema shape, and the courier only needs OK=false to retry.
			b.ackProcEvent(msg, proto.ProcEventAck{OK: false, Code: "store_error"})
			return
		}
		b.pubAuditProc(sid, "exit", nid, pid, nil, ev.ExitCode, ev.EndedAt)
		b.ackProcEvent(msg, proto.ProcEventAck{OK: true, Code: "recorded"})
	}
}

// ackProcEvent answers a couriered proc event (h1 C3). No Reply inbox — the
// pre-h1 fire-and-forget publish — is a silent no-op, keeping the old-agent
// wire byte-identical. The ~60B ack rides respondBytes (ErrMaxPayload is
// structurally impossible at this size; the egress logging is the point).
func (b *Broker) ackProcEvent(msg *nats.Msg, ack proto.ProcEventAck) {
	if msg.Reply == "" {
		return
	}
	b.replyJSON(msg, ack)
}

// handleNodeListReq answers ctrl.by.<actor>.s.<sid>.node.list.req
// (architecture B.1 line 129) with the SQLite `nodes` rows for sid.
// Same membership + DELETING gate as handlePsReq; result includes
// every node ever registered (independent of process activity), so
// `tether node upgrade --all` can target ONLINE agents that have
// not exec'd anything.
func (b *Broker) handleNodeListReq(msg *nats.Msg) {
	// leaf = "s.<sid>.node.list.req"
	ing, den, ok := b.admitCtrl(msg.Subject, nodeListSpec)
	if !ok {
		b.replyJSON(msg, proto.NodeListResp{Code: den.code, Error: den.detail})
		return
	}
	sid := ing.sid

	all, err := node.List(b.cfg.DB)
	if err != nil {
		b.replyJSON(msg, proto.NodeListResp{Code: "store_error", Error: err.Error()})
		return
	}
	// One scan of this session's credential bindings, so the Leased verdict below
	// costs no per-row query.
	//
	// FAIL CLOSED: when the binding set is unusable — the query failed, or the
	// session has none at all, which is the steady state on a broker running
	// without auth_callout — NOTHING is reported leased. The other direction
	// would mark every `<x>-NN` device ephemeral and silently drop it from
	// `node upgrade --all`, and `gpu-01 gpu-02 gpu-03` is this repo's own
	// example fleet.
	// len(provisioned) > 0, not the readable flag: this site means "are there bindings to
	// reason about at all". E1 (round 2) changed what the second return says — it is now
	// "the table was read" — so the intent is spelled out rather than inherited.
	provisioned, provReadable := node.ProvisionedNIDs(b.read().SQL(), sid)
	bindingsKnown := provReadable && len(provisioned) > 0

	out := make([]proto.NodeListEntry, 0, len(all))
	for _, n := range all {
		if n.SID != sid {
			continue
		}
		// THE DISTINGUISHING FACT IS CREDENTIAL-SHAPED, NOT NAME-SHAPED, and it
		// needs no column of its own.
		//
		// A real device — including one the operator named `gpu-02` — reaches
		// PIN bootstrap and owns an agent_provisioning row. A leased instance
		// never does: it is admitted by the suffix fallback against its
		// BASENAME's fingerprint, and its name is not persisted anywhere.
		//
		// This inference was briefly replaced by a stored `nodes.leased` column,
		// which was WRONG TWICE over: it required a migration, and a same-proto
		// rolling release must not add one (G5 OQ-2 — an un-migrated follower
		// fails to Apply a register command naming an unknown column), and it
		// was only needed because the suffix fallback was preempting PIN
		// bootstrap and denying real `<base>-NN` devices a binding. Fixing that
		// preemption (external review F9) restores this test's precision and
		// removes the schema change entirely.
		_, _, looksLeased := proto.SplitLeaseName(n.NID)
		leased := bindingsKnown && looksLeased && !provisioned[n.NID]
		out = append(out, proto.NodeListEntry{
			NID:             n.NID,
			Status:          n.Status,
			LastHeartbeatAt: n.LastHeartbeatAt,
			BootID:          n.BootID,
			ReleaseVersion:  n.ReleaseVersion,
			ProtoVersion:    n.ProtoVersion,
			Leased:          leased,
		})
	}
	b.replyJSON(msg, proto.NodeListResp{Nodes: out})
}

// handlePsReq replies with a PsResp built from internal/proc.ListBySession.
// Subject layout: `ctrl.by.<actor>.s.<sid>.ps.req`. Architecture F.8 says
// `tether ps` is read-only and never goes through agent forwarding.
func (b *Broker) handlePsReq(msg *nats.Msg) {
	// leaf = "s.<sid>.ps.req"; the ctrl family's own subject conventions live in admitCtrlSubject.
	ing, den, ok := b.admitCtrl(msg.Subject, psSpec)
	if !ok {
		b.replyJSON(msg, proto.PsResp{Code: den.code, Error: den.detail})
		return
	}
	sid := ing.sid

	var req proto.PsReq
	if len(msg.Data) > 0 {
		// Tolerate unknown fields (encoding/json silently drops
		// them — keeps the wire forward-compatible) and the empty
		// `{}` body from legacy v0.2.7 ctl. But surface outright
		// malformed JSON as bad_request instead of silently
		// downgrading to defaults — a corrupt client should hear
		// about it rather than see a partial result.
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			b.replyJSON(msg, proto.PsResp{
				Code:  "bad_request",
				Error: "ps: malformed PsReq body: " + err.Error(),
			})
			return
		}
	}
	opts := proc.ListBySessionOpts{
		IncludeExited: req.IncludeExited,
		Limit:         req.Limit,
	}
	// serverMaxLimit caps BOTH reply sections (h1 A1). Processes have been
	// capped since v0.2.8; ports were not, and 24k FREED rows once pushed the
	// marshaled PsResp past NATS max_payload — the reply was then silently
	// dropped (pre-h1 replyJSON swallowed ErrMaxPayload) and every `tether ps`
	// timed out for five days (2026-08-04 incident, docs/reviews/h1-plan.md).
	const serverMaxLimit = 500
	if opts.Limit <= 0 || opts.Limit > serverMaxLimit {
		opts.Limit = serverMaxLimit
	}

	procs, err := proc.ListBySessionFiltered(b.cfg.DB, sid, opts)
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	// Totals go through b.read() (not b.cfg.DB): the cfgdb ratchet pins this
	// function at exactly 3 direct sites, and these are plain bounded-stale
	// reads. A count/list pair is two non-transactional reads by design —
	// see proto.PsResp for why the ±1 skew is accepted.
	procsTotal, err := proc.CountBySession(b.read().SQL(), sid, req.IncludeExited)
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

	// P6 / architecture F.8 — same query also returns the ports. Bounded and
	// FREED-excluded by default since h1 A1 (the incident section); `-a` sets
	// IncludeFreedPorts and gets live-first ordering so a truncated view can
	// never omit a live allocation.
	ports, err := port.ListBySessionFiltered(b.read().SQL(), sid, port.ListBySessionOpts{
		IncludeFreed: req.IncludeFreedPorts,
		Limit:        serverMaxLimit,
	})
	if err != nil {
		b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
		return
	}
	portsTotal, err := port.CountBySession(b.read().SQL(), sid, req.IncludeFreedPorts)
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
			// B4: home/epoch/rebuild for `ps` + `expose explain`. All omitempty → single
			// broker / un-homed / rebuild-ON (default) rows marshal byte-identical to pre-B4.
			HomeBroker: pa.HomeBroker,
			Epoch:      pa.Epoch,
			RebuildOff: pa.RebuildOff,
		})
	}
	b.replyJSON(msg, proto.PsResp{
		Processes: procOut, Ports: portOut,
		ProcsTotal: procsTotal, ProcsTruncated: procsTotal > len(procOut),
		PortsTotal: portsTotal, PortsTruncated: portsTotal > len(portOut),
	})
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

// leasedNIDsForSession answers "which of this session's node names are leases?"
// — once, from the credential bindings, for every caller that needs it.
//
// It exists because the question was being answered independently in three
// places and they disagreed (external review F12): node ls consulted the
// bindings, proxy status parsed the name, and the allocator did a third thing.
// A device the operator named `gpu-02` was therefore a real device to one
// subsystem and an ephemeral clone to another.
//
// FAIL CLOSED, same as the node-list path: when the binding set is unusable —
// the query failed, or the session has none, which is the steady state on a
// broker with no auth_callout — NOTHING is reported leased. Marking a real
// device ephemeral is the destructive direction.
func leasedNIDsForSession(b *Broker, sid string) map[string]bool {
	out := map[string]bool{}
	// len(provisioned) > 0, not the readable flag: this site means "are there bindings to
	// reason about at all". E1 (round 2) changed what the second return says — it is now
	// "the table was read" — so the intent is spelled out rather than inherited.
	provisioned, provReadable := node.ProvisionedNIDs(b.read().SQL(), sid)
	bindingsKnown := provReadable && len(provisioned) > 0
	if !bindingsKnown {
		return out
	}
	all, err := node.List(b.read().SQL())
	if err != nil {
		return out
	}
	for _, n := range all {
		if n.SID != sid {
			continue
		}
		if _, _, looksLeased := proto.SplitLeaseName(n.NID); looksLeased && !provisioned[n.NID] {
			out[n.NID] = true
		}
	}
	return out
}
