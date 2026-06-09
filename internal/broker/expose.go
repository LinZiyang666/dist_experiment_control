// `expose` / `expose-rm` control plane on the broker side. The
// actual TCP forwarding goes through the in-process yamux tunnel
// (`internal/tunnel`); this file only owns the SQLite-state +
// audit + agent-forward layer. The architecture spec originally
// called for embedding the `frp` Go library and shipping `frpc` as
// a subprocess (architecture F.1); the in-process yamux variant is
// a documented deviation, see README "Architecture deep-dive".
//
// Architecture references:
//   - D.4   port state machine (ALLOCATED → REVOKED|FREED)
//   - F.3   expose end-to-end flow
//   - F.4   token storage rule (broker keeps only SHA256, raw is
//           agent-only after delivery)
//   - F.6   断联与恢复 — agent restart re-uses the same port via
//           state.json + tunnel auto-reconnect
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// publicHostFor returns the operator-facing host that goes into the URL
// printed by `tether expose`. v1 reads it from broker config; for the
// in-process tests we fall back to "<localhost>" because there's no
// real DNS name.
func (b *Broker) publicHostFor() string {
	if b.cfg.PublicHost != "" {
		return b.cfg.PublicHost
	}
	return "localhost"
}

// tunnelTokenLookup is the callback the internal/tunnel server invokes
// for every agent REGISTER. Authoritative answer comes from the
// port_allocations table (state must be ALLOCATED, sid/nid must
// match). Returns nil → allow; non-nil → DENY (the error message
// becomes part of the wire DENY line for operator debugging).
//
// Architecture F.4 — broker is the only side that knows token_hash
// vs raw token; agent presents raw, broker hashes & looks up.
func (b *Broker) tunnelTokenLookup(sid, nid string, publicPort int, tokenHash string) error {
	// Audit shard 02 F6: collapse "absent" and "port-mismatch" into
	// the same error code so an attacker probing with stolen
	// tokens can't tell whether the row exists at all. Internally
	// log the discrimination for operator triage; over the wire
	// only "token_unknown_or_revoked" leaks.
	a, err := port.LookupByTokenHash(b.cfg.DB, tokenHash)
	if err != nil {
		return fmt.Errorf("token_unknown_or_revoked")
	}
	if a.Port != publicPort || a.SID != sid || a.NID != nid {
		b.cfg.Logger.Warn("tunnel: token sid/nid/port mismatch",
			"want_sid", sid, "want_nid", nid, "want_port", publicPort,
			"got_sid", a.SID, "got_nid", a.NID, "got_port", a.Port)
		return fmt.Errorf("token_unknown_or_revoked")
	}
	return nil
}

// handleExposeReq is the broker entry point for `tether expose`.
// Pre-flight mirrors handleExecReq / handleRunReq exactly (same C.1 §6
// + member + node ONLINE gates), then port.Allocate runs in its own
// SQLite transaction. On success we forward (port, token, local_port,
// name) to the agent and reply ExposeResp{Port,Token,PublicHost} to
// ctl. On any pre-forward failure we reply ExposeResp{Code,Error} —
// ctl gets a clean lifecycle message instead of a NATS timeout.
func (b *Broker) handleExposeReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "expose" {
		b.replyExposeErr(msg, "subject_malformed", "")
		return
	}

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyExposeErr(msg, "actor_invalid", err.Error())
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyExposeErr(msg, "store_error", err.Error())
		return
	}
	if !active {
		b.replyExposeErr(msg, "session_not_found_or_deleting", "")
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyExposeErr(msg, "store_error", err.Error())
		return
	}
	if !member {
		b.replyExposeErr(msg, "not_a_member", "")
		return
	}
	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		b.replyExposeErr(msg, "node_not_found", "")
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "node_not_found", msg.Reply, nil)
		return
	case err != nil:
		b.replyExposeErr(msg, "store_error", err.Error())
		return
	case status != node.StateOnline:
		b.replyExposeErr(msg, "node_offline", string(status))
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "node_offline:"+string(status), msg.Reply, nil)
		return
	}

	var req proto.ExposeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyExposeErr(msg, "json_parse", err.Error())
		return
	}
	if req.Name == "" {
		b.replyExposeErr(msg, "name_required", "")
		return
	}
	if req.LocalPort <= 0 || req.LocalPort > 65535 {
		b.replyExposeErr(msg, "local_port_invalid", strconv.Itoa(req.LocalPort))
		return
	}

	alloc, err := port.Allocate(b.cfg.DB, sid, nid, req.Name, req.LocalPort, req.RemotePort, fp, b.cfg.PortAllocCfg())
	switch {
	case errors.Is(err, port.ErrNameTaken):
		b.replyExposeErr(msg, "name_taken", req.Name)
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "name_taken", msg.Reply, nil)
		return
	case errors.Is(err, port.ErrPortExhausted):
		b.replyExposeErr(msg, "port_exhausted", "")
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "port_exhausted", msg.Reply, nil)
		return
	case errors.Is(err, port.ErrPortOutOfBand):
		b.replyExposeErr(msg, "port_out_of_band", strconv.Itoa(req.RemotePort))
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "port_out_of_band", msg.Reply, nil)
		return
	case errors.Is(err, port.ErrPortTaken):
		b.replyExposeErr(msg, "port_taken", strconv.Itoa(req.RemotePort))
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "port_taken", msg.Reply, nil)
		return
	case err != nil:
		b.replyExposeErr(msg, "alloc_failed", err.Error())
		return
	}

	// Forward (port, token, local_port, name) to the agent. The token
	// is leaked over NATS exactly once, here, and never re-exposed by
	// the broker again — agent persists it to state.json so frpc can
	// re-present on restart (F.4 / F.6).
	fwdReq := proto.ExposeForwardedReq{
		Name:      req.Name,
		Port:      alloc.Port,
		LocalPort: req.LocalPort,
		Token:     alloc.Token,
		ActorFP:   fp,
	}
	fwdBody, err := json.Marshal(&fwdReq)
	if err != nil {
		// roll back the allocation — no point keeping a row no agent saw
		_ = port.Free(b.cfg.DB, alloc.Port, b.cfg.Now())
		b.replyExposeErr(msg, "marshal", err.Error())
		return
	}
	fwdSubj := proto.SubjCmdForwarded(sid, nid, "expose")
	fwdResp, err := nc.Request(fwdSubj, fwdBody, b.cfg.ExposeForwardTimeout())
	if err != nil {
		// Agent didn't ACK in time. Free the port so we don't leak it.
		// Common cause: agent process crashed between OK status read
		// and message receive.
		_ = port.Free(b.cfg.DB, alloc.Port, b.cfg.Now())
		b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "freed")
		b.replyExposeErr(msg, "agent_no_responders", err.Error())
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "agent_no_responders", msg.Reply, nil)
		return
	}
	var agentResp proto.ExposeForwardedResp
	if err := json.Unmarshal(fwdResp.Data, &agentResp); err != nil {
		_ = port.Free(b.cfg.DB, alloc.Port, b.cfg.Now())
		b.replyExposeErr(msg, "agent_malformed_resp", err.Error())
		return
	}
	if !agentResp.OK {
		_ = port.Free(b.cfg.DB, alloc.Port, b.cfg.Now())
		b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "freed")
		b.replyExposeErr(msg, "agent_rejected:"+agentResp.Code, agentResp.Error)
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "agent_rejected:"+agentResp.Code, msg.Reply, nil)
		return
	}

	// Success: tell ctl, broadcast, audit. The raw token does NOT
	// go to ctl — agent already has it via the forwarded request and
	// is the only side that needs to present it to the tunnel server
	// (architecture F.4 storage boundary).
	resp := proto.ExposeResp{
		Port:       alloc.Port,
		PublicHost: b.publicHostFor(),
		Name:       req.Name,
	}
	respBody, _ := json.Marshal(&resp)
	if msg.Reply != "" {
		_ = msg.Respond(respBody)
	}
	b.cfg.Logger.Info("broker: expose allocated",
		"sid", sid, "nid", nid, "name", req.Name,
		"port", alloc.Port, "local", req.LocalPort, "fp", fp)
	b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "allocated")
	b.pubAuditCall(sid, fp, actor, "expose", nid, true, "", msg.Reply,
		map[string]any{"port": alloc.Port, "name": req.Name, "local_port": req.LocalPort})
	b.pubAuditPort(sid, "allocated", nid, alloc.Port, req.Name, req.LocalPort, fp, b.cfg.Now())
}

func (b *Broker) replyExposeErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.ExposeResp{Code: code, Error: detail})
	_ = msg.Respond(body)
}

// handleExposeRmReq is the broker entry point for `tether expose rm`.
// Same pre-flight as expose; lookup by (sid, name), mark FREED, forward
// drop instruction to agent (best-effort: even if agent ACK fails, the
// port is back in the pool — agent will catch up via reconciliation).
func (b *Broker) handleExposeRmReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, _, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "expose-rm" {
		b.replyExposeRmErr(msg, "subject_malformed", "")
		return
	}

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyExposeRmErr(msg, "actor_invalid", err.Error())
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyExposeRmErr(msg, "store_error", err.Error())
		return
	}
	if !active {
		b.replyExposeRmErr(msg, "session_not_found_or_deleting", "")
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyExposeRmErr(msg, "store_error", err.Error())
		return
	}
	if !member {
		b.replyExposeRmErr(msg, "not_a_member", "")
		return
	}

	var req proto.ExposeRmReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyExposeRmErr(msg, "json_parse", err.Error())
		return
	}
	if req.Name == "" {
		b.replyExposeRmErr(msg, "name_required", "")
		return
	}

	alloc, err := port.LookupByName(b.cfg.DB, sid, req.Name)
	switch {
	case errors.Is(err, port.ErrNotFound):
		b.replyExposeRmErr(msg, "not_found", req.Name)
		return
	case err != nil:
		b.replyExposeRmErr(msg, "store_error", err.Error())
		return
	}

	// Architecture F.8: only the original creator (created_by_fp) OR
	// the session owner may rm an expose. Skipping this would let any
	// member disrupt another member's exposed service AND immediately
	// squat the freed port number for their own expose. The fp here
	// is broker-NATS-proven via the by.<actor> subject segment (B.1)
	// — no extra identity validation needed.
	if alloc.CreatedByFP != fp {
		isOwner, err := session.IsOwner(b.cfg.DB, sid, fp)
		if err != nil {
			b.replyExposeRmErr(msg, "store_error", err.Error())
			return
		}
		if !isOwner {
			b.replyExposeRmErr(msg, "not_owner_or_creator", req.Name)
			b.pubAuditCall(sid, fp, actor, "expose-rm", alloc.NID, false, "not_owner_or_creator", msg.Reply, nil)
			return
		}
	}

	if err := port.Free(b.cfg.DB, alloc.Port, b.cfg.Now()); err != nil {
		b.replyExposeRmErr(msg, "free_failed", err.Error())
		return
	}

	// Tell agent best-effort. We don't gate the user-visible OK on the
	// agent's response — the SQLite row is the source of truth and
	// frps/frpc will eventually catch up.
	fwdReq := proto.ExposeRmForwardedReq{Name: req.Name, Port: alloc.Port}
	fwdBody, _ := json.Marshal(&fwdReq)
	fwdSubj := proto.SubjCmdForwarded(sid, alloc.NID, "expose-rm")
	if err := nc.Publish(fwdSubj, fwdBody); err != nil {
		b.cfg.Logger.Warn("broker: expose-rm forward failed", "err", err)
	}

	if msg.Reply != "" {
		body, _ := json.Marshal(proto.ExposeRmResp{OK: true, Port: alloc.Port})
		_ = msg.Respond(body)
	}
	b.cfg.Logger.Info("broker: expose freed",
		"sid", sid, "nid", alloc.NID, "name", req.Name, "port", alloc.Port, "fp", fp)
	b.pubPortEvent(sid, alloc.Port, req.Name, alloc.NID, alloc.LocalPort, "freed")
	b.pubAuditCall(sid, fp, actor, "expose-rm", alloc.NID, true, "", msg.Reply,
		map[string]any{"port": alloc.Port, "name": req.Name})
	b.pubAuditPort(sid, "freed", alloc.NID, alloc.Port, req.Name, alloc.LocalPort, fp, b.cfg.Now())
}

func (b *Broker) replyExposeRmErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.ExposeRmResp{Code: code, Error: detail})
	_ = msg.Respond(body)
}

// reconcilePorts is the broker tick that walks ALLOCATED ports owned
// by long-OFFLINE nodes and revokes them. Architecture D.4 / F.6:
// default threshold 15min. Returns the number of ports just revoked
// (caller logs it; 0 is the common case).
func (b *Broker) reconcilePorts(now time.Time) int {
	allocs, err := port.ListAllocatedForOfflineNodes(b.cfg.DB, now, b.cfg.PortRevokeAfter())
	if err != nil {
		b.cfg.Logger.Warn("broker: reconcilePorts list", "err", err)
		return 0
	}
	revoked := 0
	for _, a := range allocs {
		if err := port.Revoke(b.cfg.DB, a.Port, now); err != nil {
			b.cfg.Logger.Warn("broker: reconcilePorts revoke", "err", err, "port", a.Port)
			continue
		}
		b.pubPortEvent(a.SID, a.Port, a.Name, a.NID, a.LocalPort, "revoked")
		b.pubAuditPort(a.SID, "revoked", a.NID, a.Port, a.Name, a.LocalPort, "" /* system, no actor */, now)
		revoked++
	}
	return revoked
}

// pubPortEvent broadcasts a port lifecycle event on s.<sid>.ev.port.<port>.<kind>.
// Members subscribe to keep their local view of `tether ps` fresh.
func (b *Broker) pubPortEvent(sid string, port int, name, nid string, localPort int, kind string) {
	body, err := json.Marshal(proto.PortEvent{
		Port: port, Name: name, NID: nid, LocalPort: localPort, Kind: kind, Ts: b.cfg.Now(),
	})
	if err != nil {
		return
	}
	if err := b.publishOnConn(proto.SubjEvPort(sid, port, kind), body); err != nil {
		b.cfg.Logger.Warn("broker: pub ev.port", "err", err, "sid", sid, "port", port, "kind", kind)
	}
}

// pubAuditPort writes audit.port using schema.AuditPort (architecture
// H.5). Same single-source-of-truth rule as pubAuditCall /
// pubAuditProc — the schema struct is the wire contract; never an
// inline anonymous one.
func (b *Broker) pubAuditPort(sid, kind, nid string, port int, name string, localPort int, actorFP string, ts time.Time) {
	body, err := json.Marshal(schema.AuditPort{
		V: schema.AuditSchemaVersion, Kind: kind, Ts: ts,
		Session: sid, Node: nid, Port: port,
		Name: name, LocalPort: localPort, ActorFp: actorFP,
	})
	if err != nil {
		return
	}
	if err := b.publishAudit(proto.SubjAuditPort(sid), body); err != nil {
		b.cfg.Logger.Warn("broker: audit.port pub", "err", err, "sid", sid)
	}
}
