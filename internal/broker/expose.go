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
//     agent-only after delivery)
//   - F.6   断联与恢复 — agent restart re-uses the same port via
//     state.json + tunnel auto-reconnect
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// B4 `--on-broker` sentinels. errOnBrokerSingleMode: a single broker has no roster, so
// pinning a home is meaningless. errOnBrokerUnknown: the named node is not a real ELIGIBLE
// (VOTER, cert-pinned) cluster node — eligibility is checked with the SAME predicate
// homeForExpose uses (Eligible() && CertFP != ""), which already excludes draining/retiring/
// catching-up/add-failed phases (those are not phase VOTER). Both abort BEFORE any raft
// Propose, so a rejected --on-broker writes no row.
var (
	errOnBrokerSingleMode = errors.New("broker: --on-broker requires a clustered broker")
	errOnBrokerUnknown    = errors.New("broker: --on-broker target is not an eligible cluster node")
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
func (b *Broker) tunnelTokenLookup(sid, nid string, publicPort int, tokenHash string, epoch int64) error {
	// Audit shard 02 F6: collapse "absent" and "port-mismatch" into
	// the same error code so an attacker probing with stolen
	// tokens can't tell whether the row exists at all. Internally
	// log the discrimination for operator triage; over the wire
	// only "token_unknown_or_revoked" leaks.
	a, err := port.LookupByTokenHash(b.cfg.DB, tokenHash)
	if err != nil {
		if !errors.Is(err, port.ErrNotFound) {
			// A TRANSIENT store fault must NOT masquerade as a revocation: a
			// reconnecting agent treats token_unknown_or_revoked as terminal
			// (proxy off) and stops forever — the false-online incident, this
			// time DB-triggerable. Emit a distinct transient reason the agent
			// retries. This leaks nothing: try_again fires only on the broker's
			// own DB fault, never as a function of token validity — the F6
			// anti-enumeration collapse below (absent/mismatch/off → one code)
			// is preserved.
			b.cfg.Logger.Warn("tunnel: token lookup transient store error",
				"sid", sid, "nid", nid, "port", publicPort, "err", err)
			return fmt.Errorf("try_again")
		}
		// gotcha #86: "no such row" is only authoritative from a replica that has applied everything
		// the leader committed. A follower that is behind (apply lag under load — the leader has
		// committed the allocation, forwarded the expose, and the agent dialled here before this
		// FSM applied it) must answer the TRANSIENT home_catching_up, exactly as the epoch ladder
		// below does for a not-yet-applied reassign; the agent retries and the row appears. This
		// leaks nothing: the answer is a function of this replica's own lag, never of the token
		// (the F6 absent/mismatch/off collapse below is untouched), and a caught-up replica still
		// says token_unknown_or_revoked. The lag is read in the COMMAND domain (homeApplyLagging):
		// the raft-domain CaughtUp reads true while the FSM is still inside the SQLite apply, which
		// is precisely this window (round-2 review R2-F2).
		if missingTokenIsCatchingUp(b.clusterMode, homeApplyLagging(b.cl, b.cfg.Logger)) {
			b.cfg.Logger.Warn("tunnel: token unknown on a replica that is still catching up — transient",
				"sid", sid, "nid", nid, "port", publicPort)
			return fmt.Errorf("%s", proto.ReasonHomeCatchingUp)
		}
		return fmt.Errorf("token_unknown_or_revoked")
	}
	if a.Port != publicPort || a.SID != sid || a.NID != nid {
		b.cfg.Logger.Warn("tunnel: token sid/nid/port mismatch",
			"want_sid", sid, "want_nid", nid, "want_port", publicPort,
			"got_sid", a.SID, "got_nid", a.NID, "got_port", a.Port)
		return fmt.Errorf("token_unknown_or_revoked")
	}
	// round-3 F1: for the system __proxy__ port, the master switch is part of
	// the authorization boundary. Even if the ALLOCATED row is briefly visible
	// between CloseProxy and port.Free, a REGISTER must be denied while proxy is
	// OFF — otherwise the kill switch leaks an exit through a re-REGISTER race.
	if a.Name == port.ProxyPortName {
		on, err := session.GetProxyEnabled(b.cfg.DB, sid)
		if err != nil {
			// Same transient-vs-authoritative split as the lookup above (Fix C):
			// a store fault on the dominant __proxy__ reconnect path must NOT be
			// folded into the terminal token_unknown_or_revoked, or the exact
			// DB-hiccup false-terminal hole Fix C closes reopens one query later.
			// try_again leaks nothing — it fires only on the broker's own DB
			// fault, never as a function of switch state or token validity.
			b.cfg.Logger.Warn("tunnel: proxy-enabled check transient store error",
				"sid", sid, "port", publicPort, "err", err)
			return fmt.Errorf("try_again")
		}
		if !on {
			// Authoritative: the master switch is OFF — terminal kill switch (a
			// re-REGISTER must NOT resurrect a disabled exit).
			return fmt.Errorf("token_unknown_or_revoked")
		}
	}
	// D6 §7.2 home/epoch ladder. INERT when the row carries no cluster home
	// (home_broker=='', the migration-0010 default / single-node path) — this
	// branch is skipped entirely, leaving tunnelTokenLookup byte-equivalent to
	// pre-D6. When the row IS homed, the bind decision is a function of BOTH
	// (home vs self) AND (agent-presented epoch vs the LOCAL row epoch a.Epoch):
	if a.HomeBroker != "" {
		self := b.selfNodeID()
		switch {
		case a.Epoch < 0:
			// Defense-in-depth (review A5 M6): a corrupted negative stored epoch must
			// not make every honest agent (epoch >= 0) loop on home_catching_up
			// forever. Treat it as a terminal, anti-enumeration-collapsed deny.
			return fmt.Errorf("token_unknown_or_revoked")
		case epoch < a.Epoch:
			// The agent holds a SUPERSEDED directive (an OpPortReassignHome bumped
			// the row past it); its higher-epoch directive will rehome it. Terminal
			// (collapsed to the anti-enumeration code, like absent/mismatch/off).
			return fmt.Errorf("token_unknown_or_revoked")
		case epoch > a.Epoch:
			// This replica has NOT yet applied the latest OpPortReassignHome
			// (REGARDLESS of home-vs-self — a higher presented epoch can only come
			// from a leader-committed directive this replica will eventually apply).
			// TRANSIENT: the agent retries until this home catches up. This IS the
			// catch-up barrier (§7.2c, epoch-as-local-row-epoch — NOT a raft index).
			return fmt.Errorf("%s", proto.ReasonHomeCatchingUp)
		case a.HomeBroker != self:
			// Same epoch, but this node is genuinely NOT the assigned home (an
			// ex-home or never-home replica at the same epoch). Terminal.
			return fmt.Errorf("token_unknown_or_revoked")
		}
		// epoch == a.Epoch && a.HomeBroker == self → this IS the home at the right
		// epoch: allow (fall through to return nil).
	}
	return nil
}

// The expose home-applied barrier (gotcha #86). The budget stays well inside the ctl's 15 s expose
// request and does not eat into ExposeForwardTimeout (it runs BEFORE the forward). One step is one
// cursor scatter/gather window; 400 ms is enough for every broker on a LAN to answer, and 3 s covers the
// apply lags measured under the deploy tier's contention rounds without turning a dead home into a
// 15 s ctl timeout — a home that never answers is the ordinary "forward anyway" path.
const (
	exposeHomeApplyBudget = 3 * time.Second
	exposeHomeApplyStep   = 400 * time.Millisecond
)

// missingTokenIsCatchingUp is the pure half of the gotcha-#86 deny classification: a token the local
// store does not know is TRANSIENT (home_catching_up) only on a clustered replica that is LAGGING
// (homeApplyLagging); single mode (the store IS the authority) and a replica that owes no committed
// command keep the terminal answer.
func missingTokenIsCatchingUp(clusterMode, lagging bool) bool {
	return clusterMode && lagging
}

// homeApplyLagging answers "may this replica's store be missing a write the leader has already
// committed?" — the #86 home-half predicate. Two readings, either one is lag:
//
//   - !Node.CaughtUp (raft domain): never synced with a leader, or a committed batch not yet even
//     dispatched to the FSM (the large-replay backpressure case);
//   - Node.CommandApplyLagging (command domain): the SQLite command cursor trails the newest
//     committed LogCommand — the FSM is inside (or queued behind) a slow apply. This is the window
//     #86 describes and the one the raft-domain reading cannot see (CaughtUp's HONEST LIMIT #1).
//
// A cluster runtime that is not wired yet, and a read error, both read as lagging: the transient
// answer costs the agent a bounded retry, the terminal one costs a healthy expose. A package
// function rather than a Broker method: the Broker method budget is a ratchet, and this needs
// nothing of the broker but its runtime and its logger.
func homeApplyLagging(cl *clusterRuntime, logger *slog.Logger) bool {
	if cl == nil {
		return false // single mode: the store is the authority
	}
	if cl.node == nil {
		return true
	}
	return applyLagOf(cl.node, logger)
}

// applyLagSource is the two readings applyLagOf combines; *cluster.Node is the production source,
// a scripted one pins the combination (expose_test.go) without a three-node cluster.
type applyLagSource interface {
	CaughtUp() bool
	CommandApplyLagging() (bool, error)
}

func applyLagOf(node applyLagSource, logger *slog.Logger) bool {
	if !node.CaughtUp() {
		return true
	}
	lagging, err := node.CommandApplyLagging()
	if err != nil {
		logger.Warn("tunnel: command apply lag unreadable — answering transient", "err", err)
		return true
	}
	return lagging
}

// awaitHomeApplied polls `poll` (a cursor scatter/gather keyed by node id) until `home` reports a
// command-domain AppliedIndex >= need, or the budget is spent. It returns how long it waited and
// whether the home caught up. A home missing from a poll (did not answer inside the step) simply
// counts as not-yet; the wait is best-effort by design — the home's own home_catching_up answer
// (homeApplyLagging, command domain since round 2) is the correctness half, this only makes the
// common case not need it. Injected `poll` makes the ladder table-testable without NATS
// (expose_home_barrier_test.go).
func awaitHomeApplied(home string, need uint64, budget, step time.Duration, poll func() map[string]proto.ClusterHealthResp) (time.Duration, bool) {
	start := time.Now()
	for {
		if r, ok := poll()[home]; ok && r.AppliedIndex >= need {
			return time.Since(start), true
		}
		if time.Since(start) >= budget {
			return time.Since(start), false
		}
		time.Sleep(step) // the real poll also gathers for one step, so ≈2 steps per iteration
	}
}

// handleExposeReq is the broker entry point for `tether expose`.
// Pre-flight mirrors handleExecReq / handleRunReq exactly (same C.1 §6
// + member + node ONLINE gates), then port.Allocate runs in its own
// SQLite transaction. On success we forward (port, token, local_port,
// name) to the agent and reply ExposeResp{Port,Token,PublicHost} to
// ctl. On any pre-forward failure we reply ExposeResp{Code,Error} —
// ctl gets a clean lifecycle message instead of a NATS timeout.
func (b *Broker) handleExposeReq(nc *nats.Conn, msg *nats.Msg) {
	// External-review F3: expose is LEADER-LOCAL (the leak-once token can't be read back),
	// so the expose subject is BROADCAST (not queue-grouped) and handled ONLY by the leader —
	// a follower returns silently, so the leader (also a broadcast subscriber) always answers
	// and the ctl never sees a spurious not_leader from a random queue member.
	if b.clusterMode && !b.cl.node.IsLeader() {
		return
	}
	// NOTE the ordering difference from exec/run/kill: expose's leader-only short circuit
	// (above) runs BEFORE the subject is parsed, so a follower answers NOTHING for a
	// malformed expose subject while exec answers subject_malformed. That is an externally
	// observable difference and it is preserved, not unified.
	ing, den, ok := b.admit(msg.Subject, exposeSpec)
	if !ok {
		b.replyExposeErr(msg, den.code, den.detail)
		if den.code == "node_not_found" || den.code == "node_offline" {
			b.pubAuditCall(ing.sid, ing.fp, ing.actor, "expose", ing.nid, false,
				auditRefusal(den), msg.Reply, nil)
		}
		return
	}
	sid, actor, nid, fp := ing.sid, ing.actor, ing.nid, ing.fp

	var req proto.ExposeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyExposeErr(msg, "json_parse", err.Error())
		return
	}
	if req.Name == "" {
		b.replyExposeErr(msg, "name_required", "")
		return
	}
	// P13: the reserved name __proxy__ belongs to the system-managed proxy
	// port; a user expose must never collide with it.
	if req.Name == port.ProxyPortName {
		b.replyExposeErr(msg, "name_reserved", port.ProxyPortName)
		return
	}
	if req.LocalPort <= 0 || req.LocalPort > 65535 {
		b.replyExposeErr(msg, "local_port_invalid", strconv.Itoa(req.LocalPort))
		return
	}

	// D9 §3 (audit #6): cluster mode routes the allocation through the leader (Propose);
	// single mode is the byte-identical direct mutator. The handler is leader-only in cluster
	// mode (F3), so allocatePort runs on the leader; a leadership race falls through to the
	// generic store_error below (raft.ErrNotLeader) and the ctl retries.
	alloc, err := b.allocatePort(sid, nid, req.Name, req.LocalPort, req.RemotePort, req.RebuildOff, req.OnBroker, fp)
	switch {
	case errors.Is(err, errOnBrokerSingleMode):
		b.replyExposeErr(msg, "on_broker_single_mode",
			"--on-broker requires a clustered broker; this broker is single-node (every expose is homed locally, nothing to pin to)")
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "on_broker_single_mode", msg.Reply, nil)
		return
	case errors.Is(err, errOnBrokerUnknown):
		b.replyExposeErr(msg, "on_broker_unknown",
			req.OnBroker+": not a known eligible (VOTER, cert-pinned, non-draining) cluster node")
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "on_broker_unknown", msg.Reply, nil)
		return
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
	// D6 §7.2/§6.5 (C1 fix): the initial-expose path never touches NodeRegisterResp, so the
	// home directive must ride the forward. Self-gating — nil in single mode (selfID==""), so
	// the forward stays byte-identical; in a cluster it tells the agent which home to dial using
	// the COMMITTED home_broker + epoch the leader baked into the captured allocation (audit
	// dataplane F1/F3/F7 — no re-resolve, no re-query).
	fwdReq.Home = b.homeForExpose(alloc)
	// gotcha #86: the agent dials the HOME the moment it receives this forward, and the home is a
	// follower that may not have APPLIED the allocation yet (raft apply lags commit by an fsync under
	// load). Its tunnelTokenLookup then finds no row and answers token_unknown_or_revoked — the
	// terminal, anti-enumeration-collapsed code — so the agent gives up, replies frpc_failed, and the
	// operator reads "the agent couldn't start the local proxy" for a race the leader created. The
	// leader waits, bounded, until the home's command-domain AppliedIndex has reached its own (the
	// cursor probe every broker answers); a home that does not catch up inside the budget still gets
	// the forward (today's behaviour), and the home side now answers home_catching_up for that case.
	if b.clusterMode && b.cl != nil && b.cl.node != nil && alloc.HomeBroker != "" && alloc.HomeBroker != b.selfNodeID() {
		if need, aerr := b.cl.node.AppliedIndex(); aerr == nil {
			home := alloc.HomeBroker
			// The scatter ends the moment the HOME has answered with a current cursor: a home that
			// applied within a millisecond costs a millisecond, not the whole step (round-2 R2-F4).
			homeApplied := func(r map[string]proto.ClusterHealthResp) bool {
				h, ok := r[home]
				return ok && h.AppliedIndex >= need
			}
			waited, ok := awaitHomeApplied(home, need, exposeHomeApplyBudget, exposeHomeApplyStep, func() map[string]proto.ClusterHealthResp {
				return pollClusterHealthUntil(nc, proto.SubjClusterCursor, exposeHomeApplyStep, homeApplied)
			})
			// Log the waits worth reading: a home that needed more than one step, or never caught up.
			if waited > exposeHomeApplyStep || !ok {
				b.cfg.Logger.Info("broker: expose waited for the home to apply the allocation",
					"home", home, "need_applied", need, "waited", waited.Round(time.Millisecond), "caught_up", ok)
			}
		}
	}
	fwdBody, err := json.Marshal(&fwdReq)
	if err != nil {
		// roll back the allocation — no point keeping a row no agent saw
		_ = b.rollbackExposeAllocation(*alloc)
		b.replyExposeErr(msg, "marshal", err.Error())
		return
	}
	fwdSubj := proto.SubjCmdForwarded(sid, nid, "expose")
	fwdResp, err := nc.Request(fwdSubj, fwdBody, b.cfg.ExposeForwardTimeout())
	if err != nil {
		// Agent didn't ACK in time. Free the port so we don't leak it.
		// Common cause: agent process crashed between OK status read
		// and message receive.
		if b.rollbackExposeAllocation(*alloc) {
			b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "freed")
		}
		b.replyExposeErr(msg, "agent_no_responders", err.Error())
		b.pubAuditCall(sid, fp, actor, "expose", nid, false, "agent_no_responders", msg.Reply, nil)
		return
	}
	var agentResp proto.ExposeForwardedResp
	if err := json.Unmarshal(fwdResp.Data, &agentResp); err != nil {
		_ = b.rollbackExposeAllocation(*alloc)
		b.replyExposeErr(msg, "agent_malformed_resp", err.Error())
		return
	}
	if !agentResp.OK {
		if b.rollbackExposeAllocation(*alloc) {
			b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "freed")
		}
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
		// B4: surface where the broker homed this expose. Single mode → both zero → omitted →
		// byte-identical to pre-B4. These come from the COMMITTED captured allocation (home_broker
		// + epoch the leader's PlanAllocate baked), not a re-read.
		HomeBroker: alloc.HomeBroker,
		Epoch:      alloc.Epoch,
	}
	respBody, _ := json.Marshal(&resp)
	if msg.Reply != "" {
		b.respondBytes(msg, respBody)
	}
	b.cfg.Logger.Info("broker: expose allocated",
		"sid", sid, "nid", nid, "name", req.Name,
		"port", alloc.Port, "local", req.LocalPort, "fp", fp)
	b.pubPortEvent(sid, alloc.Port, req.Name, nid, req.LocalPort, "allocated")
	b.pubAuditCall(sid, fp, actor, "expose", nid, true, "", msg.Reply,
		map[string]any{"port": alloc.Port, "name": req.Name, "local_port": req.LocalPort})
	b.pubAuditPort(sid, "allocated", nid, alloc.Port, req.Name, req.LocalPort, fp, b.cfg.Now())
}

func (b *Broker) rollbackExposeAllocation(a port.Allocation) bool {
	if err := b.freePortAllocation(a); err != nil {
		if errors.Is(err, port.ErrNotFound) {
			return false
		}
		b.cfg.Logger.Warn("broker: expose rollback free failed",
			"sid", a.SID, "nid", a.NID, "name", a.Name, "port", a.Port, "err", err)
		return false
	}
	return true
}

func (b *Broker) replyExposeErr(msg *nats.Msg, code, detail string) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(proto.ExposeResp{Code: code, Error: detail})
	b.respondBytes(msg, body)
}

// handleExposeRmReq is the broker entry point for `tether expose rm`.
// Same pre-flight as expose; lookup by (sid, name), mark FREED, forward
// drop instruction to agent (best-effort: even if agent ACK fails, the
// port is back in the pool — agent will catch up via reconciliation).
func (b *Broker) handleExposeRmReq(nc *nats.Conn, msg *nats.Msg) {
	ing, den, ok := b.admitSubject(msg.Subject, exposeRmSpec)
	if !ok {
		b.replyExposeRmErr(msg, den.code, den.detail)
		return
	}
	if b.isClusterFollower() {
		return
	}
	if den, ok := b.admitACL(&ing, exposeRmSpec); !ok {
		// No audit on any refusal here, and no node refusals to audit: exposeRmSpec sets
		// skipNodeCheck because this handler resolves its target by (sid, name).
		b.replyExposeRmErr(msg, den.code, den.detail)
		return
	}
	sid, actor, fp := ing.sid, ing.actor, ing.fp

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

	if err := b.freePortAllocation(*alloc); err != nil {
		b.replyExposeRmErr(msg, "free_failed", err.Error())
		return
	}
	b.closeTunnelProxyEverywhere(*alloc)

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
		b.respondBytes(msg, body)
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
	b.respondBytes(msg, body)
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
		if err := b.revokePortAllocation(a, now); err != nil {
			b.cfg.Logger.Warn("broker: reconcilePorts revoke", "err", err, "port", a.Port)
			continue
		}
		b.closeTunnelProxyEverywhere(a)
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
