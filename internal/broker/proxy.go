package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
)

// P13 — session-scoped proxy subscription handlers (architecture p13-plan §5).
//
// Owner-only: proxy.set (on/off), proxy.sub.{create,revoke}. Member-readable:
// proxy.status, proxy.sub.list reveals only redacted entries. Every handler
// follows the handleSessionRm/handlePsReq template: parse subject (exact
// length) → fingerprint actor → C.1 §6 IsActive precheck → owner/member gate.
//
// Secrets (SS PSKs, tunnel tokens) reach agents ONLY via the register reply
// _INBOX and the per-(sid,nid) proxy-keys.req.forwarded push — NEVER over
// sys.events (member-readable). sys.events carries secret-free nudges only.

// proxyErr replies a generic {code,error} body. All proxy resp types share the
// Code/Error JSON fields, so this is safe regardless of which RPC the ctl sent.
func (b *Broker) proxyErr(msg *nats.Msg, code, errMsg string) {
	b.replyJSON(msg, map[string]string{"code": code, "error": errMsg})
}

func (b *Broker) handleProxySet(nc *nats.Conn, msg *nats.Msg) {
	actor, sid, action, ok := proto.ParseCtrlProxy(msg.Subject)
	if !ok || action != "set" {
		b.proxyErr(msg, "subject_malformed", "")
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.proxyErr(msg, "actor_invalid", err.Error())
		return
	}
	// F5: reject a malformed body BEFORE any gate/mutation — otherwise the
	// zero-valued Enabled field would silently DISABLE the session.
	var req proto.ProxySetReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.proxyErr(msg, "json_parse", err.Error())
		return
	}
	if code := b.proxyActiveOwnerGate(sid, fp, actor, "proxy", msg); code != "" {
		return
	}
	// C5: in cluster mode the switch is a Raft-committed control write (the leader-gated reaper
	// performs the per-node allocation + the token-bearing directive push). proxy.set is broadcast +
	// leader-only (clusterwrite.go isBroadcastClusterSubject), so the LEADER serves this.
	if b.clusterMode {
		b.handleProxySetCluster(sid, fp, actor, req.HAPolicy, req.Enabled, msg)
		return
	}
	b.proxyOpMu.Lock()
	defer b.proxyOpMu.Unlock()
	if req.Enabled {
		b.enableProxy(nc, sid, fp, actor, msg)
	} else {
		b.disableProxy(nc, sid, fp, actor, msg)
	}
}

func (b *Broker) enableProxy(nc *nats.Conn, sid, fp, actor string, msg *nats.Msg) {
	// round-6 F6: switch + epoch flip atomically. A failed epoch bump rolls the
	// switch back, so a failed enable never leaves proxy_enabled=1 authorizing
	// stale tokens via tunnelTokenLookup.
	epoch, err := session.SetProxyEnabledAndBumpEpoch(b.cfg.DB, sid, true)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			b.proxyErr(msg, "session_not_found_or_deleting", "")
			return
		}
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	keys, err := b.activeProxyKeys(sid)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	affected := 0
	for _, nid := range b.onlineNIDs(sid) {
		// M1 keep-vs-replace: keep a node's existing port+token (token-less
		// re-push, no in-flight drop) ONLY when the agent is actually serving on
		// it — i.e. proxy_ready=1. round-4 F3: if a stale ALLOCATED row remains
		// for a node that is NOT ready (e.g. a prior OFF whose port.Free failed,
		// while the agent cleared its footprint), a token-less directive can't
		// recover it — so ROTATE: free the stale row and mint a fresh port+token
		// below so the agent gets a full token-bearing directive.
		if existing, err := port.LookupProxyByNode(b.cfg.DB, sid, nid); err == nil {
			if b.nodeProxyReady(sid, nid) {
				b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{
					Enabled: true, Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch,
				})
				affected++
				continue
			}
			if srv := b.tunnelSrv.Load(); srv != nil {
				srv.CloseProxy(existing.Port)
			}
			if err := port.Free(b.cfg.DB, existing.Port, b.cfg.Now()); err != nil {
				b.cfg.Logger.Warn("broker: proxy on rotate stale port", "sid", sid, "nid", nid, "err", err)
			}
		}
		alloc, err := port.AllocateProxy(b.cfg.DB, sid, nid, b.cfg.PortAllocCfg())
		if err != nil {
			b.cfg.Logger.Warn("broker: proxy allocate", "sid", sid, "nid", nid, "err", err)
			continue
		}
		b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{
			Enabled: true, PublicPort: alloc.Port, Token: alloc.Token,
			Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch,
		})
		b.pubPortEvent(sid, alloc.Port, port.ProxyPortName, nid, 0, "allocated")
		affected++
	}
	b.pubSysEvent("proxy_enabled", map[string]any{"sid": sid})
	b.pubAuditCall(sid, fp, actor, "proxy.on", "", true, "", msg.Reply, map[string]any{"affected_nodes": affected})
	b.replyJSON(msg, proto.ProxySetResp{OK: true, Enabled: true, AffectedNodes: affected})
}

func (b *Broker) disableProxy(nc *nats.Conn, sid, fp, actor string, msg *nats.Msg) {
	// Prefer committing switch OFF + epoch bump atomically. If the epoch mutation
	// fails, OFF is still a security kill switch: persist the switch alone on a
	// best-effort fallback, close every installed listener unconditionally, and
	// report the error. Keeping the proxy fully ON because versioning failed would
	// leave the public data plane reachable after an explicit OFF request.
	epoch, err := session.SetProxyEnabledAndBumpEpoch(b.cfg.DB, sid, false)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			b.proxyErr(msg, "session_not_found_or_deleting", "")
			return
		}
		switchErr := err
		if _, fallbackErr := session.SetProxyEnabled(b.cfg.DB, sid, false); fallbackErr != nil {
			b.cfg.Logger.Error("broker: proxy off fallback switch failed",
				"sid", sid, "epoch_err", switchErr, "switch_err", fallbackErr)
		}
		if srv := b.tunnelSrv.Load(); srv != nil {
			srv.CloseSession(sid)
		}
		for _, snap := range b.sessionNIDs(sid) {
			_ = node.SetProxyReady(b.cfg.DB, sid, snap, false)
		}
		b.cfg.Logger.Error("broker: proxy off switch/epoch commit failed", "sid", sid, "err", err)
		b.proxyErr(msg, "store_error", "proxy off: "+err.Error())
		return
	}
	// round-4 F3: kill the data plane SYNCHRONOUSLY and WITHOUT a DB query —
	// CloseSession closes every installed public listener for this session and
	// bumps their kill generations. This is the authoritative broker-side kill;
	// it must not depend on the allocation-store read below succeeding. Runs after
	// the switch is committed OFF (tunnelTokenLookup already denies new REGISTERs).
	if srv := b.tunnelSrv.Load(); srv != nil {
		srv.CloseSession(sid)
	}

	allocs, err := port.ListBySession(b.cfg.DB, sid)
	if err != nil {
		// Listeners are already closed; we just can't recycle the port numbers
		// now. The reconcile/next-ON path rotates stale rows — but report the
		// failure so OFF isn't a silent partial success.
		b.cfg.Logger.Error("broker: proxy off list ports failed", "sid", sid, "err", err)
		b.proxyErr(msg, "store_error", "proxy off: list ports failed: "+err.Error())
		return
	}
	for _, a := range allocs {
		if a.Name == port.ProxyPortName && a.State == port.StateAllocated {
			if err := port.Free(b.cfg.DB, a.Port, b.cfg.Now()); err != nil {
				// Don't emit a false "freed" event; the row stays ALLOCATED and
				// a later ON rotates it (enableProxy replaces when not ready).
				b.cfg.Logger.Error("broker: proxy off free port failed", "sid", sid, "port", a.Port, "err", err)
				continue
			}
			b.pubPortEvent(sid, a.Port, port.ProxyPortName, a.NID, 0, "freed")
		}
	}
	for _, snap := range b.sessionNIDs(sid) {
		_ = node.SetProxyReady(b.cfg.DB, sid, snap, false)
	}
	for _, nid := range b.onlineNIDs(sid) {
		b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{Enabled: false, Epoch: epoch})
	}
	b.pubSysEvent("proxy_disabled", map[string]any{"sid": sid})
	b.pubAuditCall(sid, fp, actor, "proxy.off", "", true, "", msg.Reply, nil)
	b.replyJSON(msg, proto.ProxySetResp{OK: true, Enabled: false})
}

func (b *Broker) handleProxySub(nc *nats.Conn, msg *nats.Msg) {
	actor, sid, action, ok := proto.ParseCtrlProxy(msg.Subject)
	if !ok {
		b.proxyErr(msg, "subject_malformed", "")
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.proxyErr(msg, "actor_invalid", err.Error())
		return
	}
	switch action {
	case "sub.list":
		// list is owner-only too (reveals subscriber names).
		if code := b.proxyActiveOwnerGate(sid, fp, actor, "proxy.sub.list", msg); code != "" {
			return
		}
		subs, err := proxysub.ListBySession(b.cfg.DB, sid)
		if err != nil {
			b.proxyErr(msg, "store_error", err.Error())
			return
		}
		b.replyJSON(msg, proto.ProxySubListResp{Subs: redactSubs(subs)})
	case "sub.create":
		if code := b.proxyActiveOwnerGate(sid, fp, actor, "proxy.sub.create", msg); code != "" {
			return
		}
		if b.clusterMode {
			var req proto.ProxySubCreateReq
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				b.replyJSON(msg, proto.ProxySubCreateResp{Code: "json_parse"})
				return
			}
			if !validSubName(req.Name) {
				b.replyJSON(msg, proto.ProxySubCreateResp{Code: "sub_name_invalid"})
				return
			}
			b.handleProxySubCreateCluster(sid, fp, actor, req.Name, msg)
			return
		}
		b.proxyOpMu.Lock()
		defer b.proxyOpMu.Unlock()
		b.proxySubCreate(nc, sid, fp, actor, msg)
	case "sub.revoke":
		if code := b.proxyActiveOwnerGate(sid, fp, actor, "proxy.sub.revoke", msg); code != "" {
			return
		}
		if b.clusterMode {
			var req proto.ProxySubRevokeReq
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				b.replyJSON(msg, proto.ProxySubRevokeResp{Code: "json_parse"})
				return
			}
			b.handleProxySubRevokeCluster(sid, fp, actor, req.Name, msg)
			return
		}
		b.proxyOpMu.Lock()
		defer b.proxyOpMu.Unlock()
		b.proxySubRevoke(nc, sid, fp, actor, msg)
	default:
		b.proxyErr(msg, "subject_malformed", action)
	}
}

func (b *Broker) proxySubCreate(nc *nats.Conn, sid, fp, actor string, msg *nats.Msg) {
	var req proto.ProxySubCreateReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "json_parse"})
		return
	}
	if !validSubName(req.Name) {
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "sub_name_invalid"})
		return
	}
	s, epoch, err := b.createSubAndBump(sid, req.Name, fp)
	switch {
	case errors.Is(err, proxysub.ErrNameTaken):
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "sub_name_taken"})
		b.pubAuditCall(sid, fp, actor, "proxy.sub.create", "", false, "sub_name_taken", msg.Reply, map[string]any{"name": req.Name})
		return
	case err != nil:
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	b.pushCurrentKeyset(nc, sid, epoch)
	// The raw token leaves the broker exactly once, here.
	b.pubAuditCall(sid, fp, actor, "proxy.sub.create", "", true, "", msg.Reply, map[string]any{"name": req.Name, "sub_id": s.SubID})
	b.replyJSON(msg, proto.ProxySubCreateResp{OK: true, Name: req.Name, SubURL: b.subURL(s.Token)})
}

func (b *Broker) proxySubRevoke(nc *nats.Conn, sid, fp, actor string, msg *nats.Msg) {
	var req proto.ProxySubRevokeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyJSON(msg, proto.ProxySubRevokeResp{Code: "json_parse"})
		return
	}
	epoch, err := b.revokeSubAndBump(sid, req.Name)
	switch {
	case errors.Is(err, proxysub.ErrNotFound):
		b.replyJSON(msg, proto.ProxySubRevokeResp{Code: "sub_not_found", Name: req.Name})
		return
	case errors.Is(err, proxysub.ErrAlreadyRevoked):
		b.replyJSON(msg, proto.ProxySubRevokeResp{Code: "already_revoked", Name: req.Name})
		return
	case err != nil:
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	b.pushCurrentKeyset(nc, sid, epoch)
	b.pubAuditCall(sid, fp, actor, "proxy.sub.revoke", "", true, "", msg.Reply, map[string]any{"name": req.Name})
	b.replyJSON(msg, proto.ProxySubRevokeResp{OK: true, Name: req.Name})
}

func (b *Broker) handleProxyStatus(msg *nats.Msg) {
	actor, sid, action, ok := proto.ParseCtrlProxy(msg.Subject)
	if !ok || action != "status" {
		b.proxyErr(msg, "subject_malformed", "")
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.proxyErr(msg, "actor_invalid", err.Error())
		return
	}
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	if !active {
		b.proxyErr(msg, "session_not_found_or_deleting", "")
		return
	}
	// Member-readable (status carries no secrets).
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	if !member {
		b.proxyErr(msg, "not_a_member", "")
		return
	}
	var req proto.ProxyStatusReq
	_ = json.Unmarshal(msg.Data, &req) // empty body ⇒ single-broker view (Cluster=false)
	enabled, _ := session.GetProxyEnabled(b.cfg.DB, sid)
	subs, err := proxysub.ListBySession(b.cfg.DB, sid)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	resp := proto.ProxyStatusResp{
		Enabled: enabled, Subscribers: redactSubs(subs), SubURLPrefix: b.subURLBase() + "/sub/",
	}
	if b.clusterMode && req.Cluster {
		// C5 cluster view: per-agent ready reason + home + the derived degraded state.
		nodes, nerr := b.proxyStatusNodesCluster(sid)
		if nerr != nil {
			b.proxyErr(msg, "store_error", nerr.Error())
			return
		}
		st := b.proxyClusterStatusFor(sid)
		policy, _ := session.GetProxyHAPolicy(b.cfg.DB, sid)
		resp.Nodes, resp.ClusterState, resp.HAPolicy, resp.Writable = nodes, st.State, policy, st.Writable
		b.replyJSON(msg, resp)
		return
	}
	nodes, err := b.proxyStatusNodes(sid)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return
	}
	resp.Nodes = nodes
	b.replyJSON(msg, resp)
}

// proxyStatusNodesCluster builds the C5 per-agent status rows: the readiness REASON (why a node is /
// isn't vended), its authoritative proxy home, and the keyset epoch. Secret-free. The agent's reported
// keyset epoch isn't persisted per-node, so keyset_stale is not surfaced here (a ready node serving an
// old keyset still renders); the reason precedence collapses to {no_home, catching_up, tunnel_down,
// ready} from replicated facts.
func (b *Broker) proxyStatusNodesCluster(sid string) ([]proto.ProxyNodeEntry, error) {
	sessionEpoch, _ := session.GetProxyEpoch(b.cfg.DB, sid)
	rows, err := b.cfg.DB.Query(`SELECT nid, status FROM nodes WHERE sid=? AND proxy_capable=1 ORDER BY nid`, sid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	type nodeRow struct{ nid, status string }
	var nrs []nodeRow
	for rows.Next() {
		var nr nodeRow
		if err := rows.Scan(&nr.nid, &nr.status); err != nil {
			return nil, err
		}
		nrs = append(nrs, nr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []proto.ProxyNodeEntry
	for _, nr := range nrs {
		e := proto.ProxyNodeEntry{NID: nr.nid, Status: nr.status, Epoch: sessionEpoch}
		homeBroker := ""
		if alloc, err := port.LookupProxyByNode(b.cfg.DB, sid, nr.nid); err == nil && alloc != nil {
			homeBroker = alloc.HomeBroker
			e.HomeBroker = alloc.HomeBroker
			if b.proxyHomeHealthy(alloc.HomeBroker) {
				e.PublicPort = alloc.Port
				// N3: project the HOME broker's public host (where /sub points subscribers), NOT this
				// answering broker's host — else status would disagree with the /sub render.
				if home, herr := clusternodes.LookupByNodeID(b.cfg.DB, alloc.HomeBroker); herr == nil {
					e.PublicHost = home.PublicHost
				}
			}
		}
		// BD12: the SAME shared reason builder `--homes` uses → status ⟺ /sub render-equivalence.
		e.ReadyReason = b.proxyReadyFor(sid, nr.nid, homeBroker)
		e.Ready = proxyInSubRender(e.ReadyReason)
		out = append(out, e)
	}
	return out, nil
}

// handleProxyReadyEvent records the agent's SS-bind ACK on
// s.<sid>.ev.node.<nid>.proxy.<ready|unready>.
func (b *Broker) handleProxyReadyEvent(msg *nats.Msg) {
	p := splitDot(msg.Subject)
	// 0:tether 1:v2 2:s 3:sid 4:ev 5:node 6:nid 7:proxy 8:kind
	if len(p) != 9 || p[2] != "s" || p[4] != "ev" || p[5] != "node" || p[7] != "proxy" {
		return
	}
	sid, nid, kind := p[3], p[6], p[8]
	// B.5 syntax validation, like every sibling Parse* helper.
	if proto.ValidateSID(sid) != nil || proto.ValidateNID(nid) != nil {
		return
	}
	if kind != "ready" && kind != "unready" {
		return
	}
	ready := kind == "ready"
	// Gate a ready=1 on the switch actually being ON, so a stale/late ready
	// can't mark a node serving after `proxy off`. Clearing (unready) is
	// always honored.
	if ready {
		if on, err := session.GetProxyEnabled(b.cfg.DB, sid); err != nil || !on {
			return
		}
	}
	// proxy_ready is a LIVENESS column — write the local liveness handle (in single mode
	// livenessDB() == b.cfg.DB, byte-identical; in cluster mode each broker tracks its own, never raft).
	if err := node.SetProxyReady(b.livenessDB(), sid, nid, ready); err != nil {
		b.cfg.Logger.Warn("broker: set proxy_ready", "err", err)
	}
}

// sessionNIDs returns every node id in the session (any status).
func (b *Broker) sessionNIDs(sid string) []string {
	snaps, err := node.List(b.cfg.DB)
	if err != nil {
		return nil
	}
	var out []string
	for _, s := range snaps {
		if s.SID == sid {
			out = append(out, s.NID)
		}
	}
	return out
}

// proxyDirectiveForRegister returns the authoritative directive for a
// (re)registering agent, or nil when proxy is off. Keep-vs-replace: if an
// ALLOCATED proxy row exists AND the agent re-presented its matching token
// hash, keep it (Token empty → agent reuses its persisted token); otherwise
// mint a fresh port+token.
func (b *Broker) proxyDirectiveForRegister(sid, nid string, req proto.NodeRegisterReq) *proto.ProxyDirective {
	if b.clusterMode {
		// C5: deliver the AUTHORITATIVE directive for an EXISTING reaper-allocated __proxy__ row — the
		// PRIMARY rehome-delivery path (the D6 server-id bridge co-locates home with the agent's
		// connected broker, so a home death bounces the agent → re-register → the new Home here). No
		// allocation here (the leader-gated reaper owns the token-correct mint); a missing row ⇒ nil and
		// the reaper pushes once it allocates.
		enabled, err := session.GetProxyEnabled(b.cfg.DB, sid)
		if err != nil || !enabled || !nodeHasProxyCap(req.Capabilities, req.ReleaseVersion) {
			return nil
		}
		existing, err := port.LookupProxyByNode(b.cfg.DB, sid, nid)
		if err != nil || existing == nil {
			return nil
		}
		keys, err := b.activeProxyKeys(sid)
		if err != nil {
			return nil
		}
		epoch, _ := session.GetProxyEpoch(b.cfg.DB, sid)
		// No token (the agent reuses its persisted one; Generation=0 in cluster mode → epoch-only order).
		return &proto.ProxyDirective{
			Enabled: true, PublicPort: existing.Port, Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch,
			Home: b.homeForExpose(existing),
		}
	}
	enabled, err := session.GetProxyEnabled(b.cfg.DB, sid)
	if err != nil || !enabled {
		return nil
	}
	// F7/F5 capability gate: never allocate a proxy port / push a directive to a
	// pre-P13 agent. Gate on the EXPLICIT proto.CapProxyV1 capability (release
	// semver is only a secondary signal — see nodeHasProxyCap).
	if !nodeHasProxyCap(req.Capabilities, req.ReleaseVersion) {
		return nil
	}
	keys, err := b.activeProxyKeys(sid)
	if err != nil {
		return nil
	}
	epoch, _ := session.GetProxyEpoch(b.cfg.DB, sid)

	agentHash := ""
	for _, lp := range req.LocalPorts {
		if lp.Name == port.ProxyPortName {
			agentHash = lp.TokenHash
			break
		}
	}
	if existing, err := port.LookupProxyByNode(b.cfg.DB, sid, nid); err == nil {
		if agentHash != "" && existing.TokenHash == agentHash {
			return &proto.ProxyDirective{
				Enabled: true, PublicPort: existing.Port,
				Cipher: ssproxy.Cipher, Keys: keys, Generation: b.proxyGenLoad(), Epoch: epoch,
			}
		}
	}
	alloc, err := port.AllocateProxy(b.cfg.DB, sid, nid, b.cfg.PortAllocCfg())
	if err != nil {
		b.cfg.Logger.Warn("broker: proxy allocate on register", "sid", sid, "nid", nid, "err", err)
		return nil
	}
	return &proto.ProxyDirective{
		Enabled: true, PublicPort: alloc.Port, Token: alloc.Token,
		Cipher: ssproxy.Cipher, Keys: keys, Generation: b.proxyGenLoad(), Epoch: epoch,
	}
}

// --- helpers -------------------------------------------------------------

// proxyActiveOwnerGate runs IsActive (C.1 §6) then IsOwner; on failure it
// replies the right code (+ admin_denied audit for non-owners) and returns a
// non-empty code. Empty return means "pass, proceed".
func (b *Broker) proxyActiveOwnerGate(sid, fp, actor, verb string, msg *nats.Msg) string {
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return "store_error"
	}
	if !active {
		b.proxyErr(msg, "session_not_found_or_deleting", "")
		return "session_not_found_or_deleting"
	}
	owner, err := session.IsOwner(b.cfg.DB, sid, fp)
	if err != nil {
		b.proxyErr(msg, "store_error", err.Error())
		return "store_error"
	}
	if !owner {
		b.proxyErr(msg, "not_owner", "")
		b.pubAuditCall(sid, fp, actor, verb, "", false, "not_owner", msg.Reply, nil)
		return "not_owner"
	}
	return ""
}

func (b *Broker) activeProxyKeys(sid string) ([]proto.ProxyKey, error) {
	subs, err := proxysub.ListActive(b.cfg.DB, sid)
	if err != nil {
		return nil, err
	}
	keys := make([]proto.ProxyKey, 0, len(subs))
	for _, s := range subs {
		keys = append(keys, proto.ProxyKey{SubID: s.SubID, Secret: s.PSK})
	}
	return keys, nil
}

// createSubAndBump creates a subscriber AND bumps the epoch in ONE transaction
// (round-6 F5): the credential and the version commit together, so a failed bump
// rolls the credential back instead of leaving agents on a stale keyset version.
func (b *Broker) createSubAndBump(sid, name, fp string) (*proxysub.Subscriber, int64, error) {
	tx, err := b.cfg.DB.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	s, err := proxysub.Create(tx, sid, name, fp, b.cfg.Now())
	if err != nil {
		return nil, 0, err
	}
	epoch, err := bumpEpochTx(tx, sid)
	if err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return s, epoch, nil
}

// revokeSubAndBump revokes a subscriber AND bumps the epoch in ONE transaction
// (round-6 F5): a failed bump rolls the revoke back so the RPC can't report
// success while agents keep accepting the supposedly-revoked PSK.
func (b *Broker) revokeSubAndBump(sid, name string) (int64, error) {
	tx, err := b.cfg.DB.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := proxysub.Revoke(tx, sid, name, b.cfg.Now()); err != nil {
		return 0, err
	}
	epoch, err := bumpEpochTx(tx, sid)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return epoch, nil
}

func bumpEpochTx(tx *sql.Tx, sid string) (int64, error) {
	if _, err := tx.Exec(`UPDATE sessions SET proxy_epoch=proxy_epoch+1 WHERE sid=?`, sid); err != nil {
		return 0, fmt.Errorf("bump proxy_epoch: %w", err)
	}
	var epoch int64
	if err := tx.QueryRow(`SELECT proxy_epoch FROM sessions WHERE sid=?`, sid).Scan(&epoch); err != nil {
		return 0, err
	}
	return epoch, nil
}

// pushCurrentKeyset is the best-effort NATS half after a durable epoch bump. The
// version is already committed, so heartbeat repair replays it to any agent that
// misses the push.
func (b *Broker) pushCurrentKeyset(nc *nats.Conn, sid string, epoch int64) {
	// round-7 F4: P13 is INERT while the master switch is OFF. A subscriber
	// create/revoke commits its DB change + epoch bump, but must NOT push an
	// Enabled:true keyset (which would leak the latest PSK, flip readiness/runtime
	// state, and fire keyset_changed) while proxy is off. The next `proxy on`
	// sends the full authoritative keyset.
	if enabled, err := session.GetProxyEnabled(b.cfg.DB, sid); err != nil || !enabled {
		return
	}
	keys, err := b.activeProxyKeys(sid)
	if err != nil {
		return
	}
	for _, nid := range b.onlineNIDs(sid) {
		// Keyset-only delta: no port/token (agent keeps its running server).
		b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{
			Enabled: true, Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch,
		})
	}
	b.pubSysEvent("proxy_keyset_changed", map[string]any{"sid": sid})
}

// repairProxyEpoch (M2 + F1) makes the heartbeat the proxy convergence loop. A
// node whose heartbeat-reported epoch lags the session's gets the current
// directive re-pushed — covering BOTH a dropped keyset push (switch on) AND a
// dropped OFF (switch off but the agent is still serving, agentEpoch>0). This
// makes `proxy off` a convergent kill switch, not a one-shot nudge.
// repairProxyEpoch is the epoch-only entry point (kept for direct callers /
// tests); it treats the agent generation as 0 (always mismatched) so any ON
// node it's invoked for is re-pushed.
func (b *Broker) repairProxyEpoch(sid, nid string, agentEpoch int64) {
	b.repairProxy(sid, nid, 0, agentEpoch)
}

// repairProxy drives proxy convergence from a heartbeat's (generation, epoch).
// A node is converged ONLY when ready AND its applied pair equals the broker's
// current (proxyGen, sessionEpoch). On any mismatch — including the SAME epoch
// under a different generation (round-4 F2: two DB snapshots can share an epoch
// with different keysets) — it re-pushes the current directive.
func (b *Broker) repairProxy(sid, nid string, agentGen, agentEpoch int64) {
	// D9 §16.4: P13/proxy is OUT of v1 HA — cluster mode does not serve the proxy path at
	// all (it would write port_allocations via the RODB handle anyway). No-op in cluster mode.
	if b.clusterMode {
		return
	}
	// round-6 F2: a non-P13-capable node must NOT influence proxy generation,
	// repair, or readiness — gate on the persisted capability before trusting
	// any P13 heartbeat field.
	if !b.nodeProxyCapable(sid, nid) {
		return
	}
	epoch, err := session.GetProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		return
	}
	on, err := session.GetProxyEnabled(b.cfg.DB, sid)
	if err != nil {
		return
	}
	nc := b.nc.Load()
	if nc == nil {
		return
	}

	// round-6 F1: CONVERGENCE-FIRST. A healthy ON+ready agent already on the
	// exact authoritative pair is done — return BEFORE escalating or pushing, so
	// an ordinary heartbeat doesn't storm proxy_meta writes + key re-pushes.
	ready := b.nodeProxyReady(sid, nid)
	brokerGen := b.proxyGenLoad()
	if on && ready && agentGen == brokerGen && agentEpoch == epoch {
		return
	}

	// round-5 F3 (+ self-review): a broker restored from an older DB snapshot can
	// come up with a generation STRICTLY BELOW a still-connected agent's applied
	// generation; a directive stamped at the lower generation is dropped forever.
	// Escalate past it FIRST — above the on/off split so the OFF kill switch also
	// out-orders a restored-behind agent. Only when the agent is strictly AHEAD
	// (agentGen > brokerGen): an agent at the SAME generation that just isn't
	// converged is handled by the epoch-driven re-push below (escalating there
	// would storm the generation on every not-ready heartbeat).
	if agentGen > brokerGen {
		if !b.escalateProxyGen(agentGen) {
			// Escalation rejected (implausible/unrepresentable agent value, F3) or
			// the durable store failed; any push now would be stamped at/below the
			// agent and dropped. Skip; the next heartbeat retries.
			return
		}
	}

	if !on {
		// F1: the agent is still serving past an OFF it missed — push disable.
		if agentEpoch > 0 {
			b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{Enabled: false, Epoch: epoch})
			b.cfg.Logger.Debug("broker: repaired missed proxy OFF via heartbeat",
				"sid", sid, "nid", nid, "agent_epoch", agentEpoch, "session_epoch", epoch)
		}
		return
	}

	// Fix D (proxy-tunnel-reconnect): suppress a keyset nudge whose ONLY trigger
	// is !ready at the current (gen, epoch). Such a node is either self-healing
	// its data-plane tunnel (the agent's reconnect re-publishes ready itself) or
	// authoritatively terminal (revoked/disabled exit — must stay down). A
	// keyset re-push here would hit the agent `default:` (srv!=nil) branch and
	// re-ACK ready=true every heartbeat, flapping the node in/out of /sub for
	// the whole outage. A genuine gen/epoch divergence still falls through.
	//
	// NOTE: brokerGen here is the PRE-escalation snapshot (:588). That is
	// intentional: if the escalation branch above raised b.proxyGen past agentGen,
	// then agentGen != brokerGen(snapshot), so this guard does NOT suppress and the
	// push below fires — an escalated node always converges. The guard only fires
	// for a node already at the broker's pre-escalation pair, which is the
	// self-healing / authoritative-terminal case.
	if !ready && agentGen == brokerGen && agentEpoch == epoch {
		return
	}

	// ON, not converged → push the current keyset (covers generation/epoch
	// mismatch; the not-ready-only case is handled above).
	keys, err := b.activeProxyKeys(sid)
	if err != nil {
		return
	}
	b.pushProxyDirective(nc, sid, nid, &proto.ProxyDirective{
		Enabled: true, Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch,
	})
	b.cfg.Logger.Debug("broker: repaired proxy via heartbeat",
		"sid", sid, "nid", nid, "agent_gen", agentGen, "agent_epoch", agentEpoch,
		"broker_gen", b.proxyGenLoad(), "session_epoch", epoch, "ready", ready)
}

// advanceProxyGeneration atomically raises the persisted broker generation to
// max(stored+1, nowNanos, floor) and returns it (round-4 F1 / round-5 F2,F3).
// It is strictly monotonic: even if nowNanos rolls backward, stored+1 wins, so a
// later boot always observes a higher value; floor lets repairProxy escalate
// above a connected agent's reported generation after a DB restore. The SELECT
// ... UPDATE runs in one transaction; concurrent callers serialize to an atomic
// max. An UNREADABLE counter is a hard error — the caller must fail closed, NOT
// fall back to a bare wall clock (which is the very fencing hole this fixes).
func advanceProxyGeneration(db *sql.DB, nowNanos, floor int64) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var cur int64
	if err := tx.QueryRow(`SELECT generation FROM proxy_meta WHERE id = 1`).Scan(&cur); err != nil {
		return 0, fmt.Errorf("read proxy_meta generation: %w", err)
	}
	if cur >= maxProxyGeneration {
		return 0, fmt.Errorf("proxy generation exhausted (%d)", cur)
	}
	next := cur + 1
	if nowNanos > next {
		next = nowNanos
	}
	if floor > next {
		next = floor
	}
	// round-6 F3: never persist a value that breaches the representable ceiling —
	// that would deny every future broker start (the next New computes stored+1).
	if next >= maxProxyGeneration {
		return 0, fmt.Errorf("proxy generation would exceed ceiling (next=%d)", next)
	}
	if _, err := tx.Exec(`UPDATE proxy_meta SET generation = ? WHERE id = 1`, next); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// maxProxyGeneration guards the stored+1 increment against int64 overflow. Far
// above any wall-nanos value a real deployment reaches (~year 2262 in nanos).
const maxProxyGeneration = int64(1) << 62

// proxyGenLoad reads the current (possibly runtime-escalated) generation.
func (b *Broker) proxyGenLoad() int64 {
	b.proxyGenMu.Lock()
	defer b.proxyGenMu.Unlock()
	return b.proxyGen
}

// escalateProxyGen raises the durable + in-memory generation strictly above
// agentGen (round-5 F3). A broker restored from an older DB snapshot whose
// proxy_meta rewound below a still-connected agent's applied generation would
// otherwise keep stamping directives the agent rejects forever. On any heartbeat
// where the agent's generation is at/above ours, we atomically bump the
// persisted counter past it (advanceProxyGeneration's floor) and adopt the new
// value, so the very next push out-orders the agent. Concurrent escalations for
// different agents converge through the transactional max in the store.
// escalateProxyGen returns true once the in-memory generation is strictly above
// agentGen (either it already was, or this call raised it). It returns FALSE
// only when the durable bump failed — the caller must then SKIP its push, since
// a directive stamped at the un-escalated (still ≤ agentGen) generation would be
// dropped by the agent anyway; the next heartbeat retries when the store
// recovers (self-review: don't emit a guaranteed-dropped directive).
func (b *Broker) escalateProxyGen(agentGen int64) bool {
	now := b.cfg.Now().UnixNano()
	// round-6 F3: an agent-reported generation is NOT trusted as unbounded global
	// authority. A legitimate value was issued by some broker incarnation, so it
	// can't plausibly exceed now + a generous clock-skew margin. Reject anything
	// above that (and anything that would breach the representable ceiling) so a
	// single capable agent can't escalate the global generation toward
	// exhaustion and brick every session's next broker start.
	ceiling := maxProxyGeneration - 1
	if now <= maxProxyGeneration-maxGenSkewNanos-1 {
		ceiling = now + maxGenSkewNanos
	}
	if agentGen < 0 || agentGen >= ceiling {
		b.cfg.Logger.Warn("broker: rejecting implausible agent proxy generation",
			"agent_gen", agentGen, "ceiling", ceiling)
		return false
	}
	b.proxyGenMu.Lock()
	defer b.proxyGenMu.Unlock()
	if agentGen < b.proxyGen {
		return true // already ahead (another escalation won, or never behind)
	}
	next, err := advanceProxyGeneration(b.cfg.DB, now, agentGen+1)
	if err != nil {
		b.cfg.Logger.Error("broker: proxy generation escalation failed", "agent_gen", agentGen, "err", err)
		return false
	}
	b.proxyGen = next
	b.cfg.Logger.Info("broker: escalated proxy generation past restored agent",
		"agent_gen", agentGen, "new_gen", next)
	return true
}

// maxGenSkewNanos bounds how far above wall-clock-now a broker-issued generation
// may legitimately be (forward clock skew of the issuing host). ~10 years —
// generous for skew, yet far below maxProxyGeneration so escalation can never
// approach the representable ceiling.
const maxGenSkewNanos = int64(10) * 365 * 24 * 60 * 60 * 1_000_000_000

// nodeProxyCapable reports the node's persisted proxy-v1 capability.
func (b *Broker) nodeProxyCapable(sid, nid string) bool {
	var v int
	_ = b.cfg.DB.QueryRow(`SELECT proxy_capable FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&v)
	return v == 1
}

func (b *Broker) nodeProxyReady(sid, nid string) bool {
	var v int
	_ = b.cfg.DB.QueryRow(`SELECT proxy_ready FROM nodes WHERE sid=? AND nid=?`, sid, nid).Scan(&v)
	return v == 1
}

func (b *Broker) pushProxyDirective(nc *nats.Conn, sid, nid string, d *proto.ProxyDirective) {
	// C5: in cluster mode the ordering is EPOCH-ONLY (Generation constant 0) — the register-reply
	// directives (proxyDirectiveForRegister) carry no generation, so a pushed directive must not stamp
	// b.proxyGen either, or the two delivery paths would order against different generations and the
	// agent's proxyNewer would drop one. Single mode keeps the escalatable ordering generation.
	if !b.clusterMode {
		d.Generation = b.proxyGenLoad() // round-3/5: stamp the (escalatable) ordering generation on every push
	}
	body, err := json.Marshal(d)
	if err != nil {
		return
	}
	if err := nc.Publish(proto.SubjCmdForwarded(sid, nid, "proxy-keys"), body); err != nil {
		b.cfg.Logger.Warn("broker: push proxy directive", "sid", sid, "nid", nid, "err", err)
	}
}

// onlineNIDs returns the ONLINE, P13-capable nodes of the session — the set
// the broker pushes proxy directives to (a pre-P13 node can't act on them).
// Capability comes from nodes.proxy_capable (set at register from the explicit
// proto.CapProxyV1 capability), NOT from a release-string guess.
func (b *Broker) onlineNIDs(sid string) []string {
	rows, err := b.cfg.DB.Query(
		`SELECT nid FROM nodes WHERE sid=? AND status='ONLINE' AND proxy_capable=1 ORDER BY nid`, sid)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			return out
		}
		out = append(out, nid)
	}
	return out
}

// P13 minimum *release* for proxy support — a SECONDARY production policy.
// The PRIMARY gate is the explicit proto.CapProxyV1 capability (see
// nodeHasProxyCap), because every source build shares "v0.0.0-dev" and a
// version string alone can't prove P13 support (round-2 F5). isP13Capable is
// pure semver with NO dev exception: any "-dev"/non-release string is NOT
// proof of capability.
const (
	minProxyMajor = 0
	minProxyMinor = 2
	minProxyPatch = 9
)

func isP13Capable(release string) bool {
	r := strings.TrimPrefix(release, "v")
	if r == "" {
		return false
	}
	if i := strings.IndexAny(r, "-+"); i >= 0 {
		r = r[:i] // strip pre-release / build metadata ("-dev", etc.)
	}
	parts := strings.Split(r, ".")
	if len(parts) < 3 {
		return false
	}
	maj, e1 := strconv.Atoi(parts[0])
	min, e2 := strconv.Atoi(parts[1])
	pat, e3 := strconv.Atoi(parts[2])
	if e1 != nil || e2 != nil || e3 != nil {
		return false
	}
	switch {
	case maj != minProxyMajor:
		return maj > minProxyMajor
	case min != minProxyMinor:
		return min > minProxyMinor
	default:
		return pat >= minProxyPatch
	}
}

// nodeHasProxyCap is the authoritative capability gate: the agent explicitly
// advertised proto.CapProxyV1, OR (secondary) its release semver is ≥ the P13
// release. The explicit capability is what lets a dev/source build participate
// without trusting its shared "v0.0.0-dev" string.
func nodeHasProxyCap(capabilities []string, release string) bool {
	for _, c := range capabilities {
		if c == proto.CapProxyV1 {
			return true
		}
	}
	return isP13Capable(release)
}

func (b *Broker) proxyStatusNodes(sid string) ([]proto.ProxyNodeEntry, error) {
	rows, err := b.cfg.DB.Query(`
		SELECT n.nid, n.status, n.proxy_ready, COALESCE(pa.port, 0)
		FROM nodes n
		LEFT JOIN port_allocations pa
		  ON pa.sid = n.sid AND pa.nid = n.nid
		 AND pa.name = '__proxy__' AND pa.state = 'ALLOCATED'
		WHERE n.sid = ?
		ORDER BY n.nid`, sid)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []proto.ProxyNodeEntry
	for rows.Next() {
		var e proto.ProxyNodeEntry
		var ready, pport int
		if err := rows.Scan(&e.NID, &e.Status, &ready, &pport); err != nil {
			return nil, err
		}
		e.Ready = ready == 1
		if pport != 0 {
			e.PublicPort = pport
			e.PublicHost = b.publicHostFor()
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func redactSubs(subs []proxysub.Subscriber) []proto.ProxySubEntry {
	out := make([]proto.ProxySubEntry, 0, len(subs))
	for _, s := range subs {
		out = append(out, proto.ProxySubEntry{
			Name: s.Name, State: s.State, CreatedAt: s.CreatedAt, RevokedAt: s.RevokedAt,
		})
	}
	return out
}

func (b *Broker) subURLBase() string {
	if b.cfg.SubURLBase != "" {
		return b.cfg.SubURLBase
	}
	return "https://" + b.publicHostFor()
}

func (b *Broker) subURL(token string) string {
	return b.subURLBase() + "/sub/" + token
}

// validSubName mirrors the ctl-side check: 1..64 printable ASCII, no '/'.
func validSubName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c < 0x20 || c > 0x7e || c == '/' {
			return false
		}
	}
	return true
}
