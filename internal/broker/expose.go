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
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
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
	// The previous agent cannot retry a transient REGISTER denial. Retain
	// its one request under the ordinary lookup budget, while every attempt
	// still rejects known lag and reads the full fresh authority transaction.
	// origin: hot-update S5 plan §2.1 / §4.3 / T29 / DRILL-RUN 39.F.
	return lookupLegacyTunnelToken(b, tunnel.AuthRequest{Allocation: tunnel.Allocation{
		SID: sid, NID: nid, Port: publicPort, TokenHash: tokenHash, Epoch: epoch,
	}})
}

// Only apply lag is retried: revocation and store faults retain their native
// classifications. One absolute deadline covers all attempts and pacing; no
// positive grant is cached across a committed revoke or a later transaction.
// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN 39.F.
func lookupLegacyTunnelToken(b *Broker, req tunnel.AuthRequest) error {
	return lookupLegacyTunnelTokenAttempts(req, func(next tunnel.AuthRequest) (tunnel.Authority, error) {
		return tunnelAuthoritySnapshotAttempt(b, next)
	})
}

// A synchronous attempt seam lets the expiry regression hold a real SQL pool
// only after one real read has observed lag; no authority is manufactured.
// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN run262.
func lookupLegacyTunnelTokenAttempts(req tunnel.AuthRequest, attempt func(tunnel.AuthRequest) (tunnel.Authority, error)) error {
	deadline := time.Now().Add(tunnel.LookupBudget)
	if !req.Deadline.IsZero() && req.Deadline.Before(deadline) {
		deadline = req.Deadline
	}
	req.Deadline = deadline
	sawLag := false
	for {
		select {
		case <-req.Stop:
			return errTunnelAuthorityRetry
		default:
		}
		// Expiry alone does not prove lag. Preserve a prior native lag refusal
		// even when the final SQL attempt, rather than its pacing timer, expires.
		// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN run262.
		if !time.Now().Before(deadline) {
			if sawLag {
				return errTunnelAuthorityLag
			}
			return errTunnelAuthorityRetry
		}
		_, err := attempt(req)
		if errors.Is(err, errTunnelAuthorityExpired) {
			select {
			case <-req.Stop:
				return errTunnelAuthorityRetry
			default:
			}
			if sawLag {
				return errTunnelAuthorityLag
			}
			return errTunnelAuthorityRetry
		}
		if !errors.Is(err, errTunnelAuthorityLag) {
			return err
		}
		sawLag = true
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errTunnelAuthorityLag
		}
		timer := time.NewTimer(min(20*time.Millisecond, remaining))
		select {
		case <-timer.C:
		case <-req.Stop:
			timer.Stop()
			return errTunnelAuthorityRetry
		}
	}
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
// (applyLagOf); single mode (the store IS the authority) and a replica that owes no committed
// command keep the terminal answer.
func missingTokenIsCatchingUp(clusterMode, lagging bool) bool {
	return clusterMode && lagging
}

// applyLagOf answers "may this replica's store be missing a write the leader has already
// committed?" — the #86 home-half predicate. Two readings, either one is lag:
//
//   - !Node.CaughtUp (raft domain): never synced with a leader, or a committed batch not yet even
//     dispatched to the FSM (the large-replay backpressure case);
//   - Node.CommandApplyLagging (command domain): the SQLite command cursor trails the newest
//     committed LogCommand — the FSM is inside (or queued behind) a slow apply. This is the window
//     #86 describes and the one the raft-domain reading cannot see (CaughtUp's HONEST LIMIT #1).
//
// A read error counts as lagging: a transient answer costs the agent a bounded
// retry, while a terminal answer costs a healthy expose. This package function
// needs only the command-domain source and its logger, keeping the Broker
// method budget unchanged.
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

var (
	errTunnelAuthorityRevoked = errors.New("token_unknown_or_revoked")
	errTunnelAuthorityRetry   = errors.New("try_again")
	errTunnelAuthorityLag     = errors.New(proto.ReasonHomeCatchingUp)
	// Private provenance distinguishes deadline cancellation from a store fault.
	// Neither v1 nor legacy may emit this internal classification over the wire.
	// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN run262.
	errTunnelAuthorityExpired = errors.New("authority deadline expired")
)

// These callbacks have operation deadlines, rather than caller-owned lifetimes.
// A broker has its own replay budget; it never charges the agent's reservations.
// Darwin retains the legacy carrier because it cannot own Linux replay sockets.
// origin: hot-update S5 plan §2.1 / §4.1 / §4.3 / §7.4
func configureTunnelAuthority(b *Broker, srv *tunnel.Server) error {
	if runtime.GOOS != "linux" {
		return nil
	}
	// The issuer identity belongs to this server lifetime. A restored database
	// or repeated node ID must not make a prior process's tickets transferable;
	// the image path below preserves it across broker exec.
	// origin: hot-update S5 plan §4.4 / T14
	var incarnation [16]byte
	if _, err := rand.Read(incarnation[:]); err != nil {
		return err
	}
	bootID := fmt.Sprintf("broker.%x", incarnation)
	instanceID := b.selfNodeID()
	if instanceID == "" {
		instanceID = bootID
	}
	return srv.SetFlowConfig(brokerTunnelFlowConfig(b, bootID, instanceID, 1,
		&workload.ReplayBudget{Limit: workload.DefaultReplayBudget}))
}

// Callbacks capture the current broker/store, while identity and budget belong
// to the imported owner. Calling SetFlowConfig on import would mint a new ticket
// key; only tunnel.ImportServer may install the sealed key and private fences.
// origin: hot-update S6 plan §6.3 / T28
func brokerTunnelFlowConfig(b *Broker, bootID, instanceID string, owner uint64, budget *workload.ReplayBudget) tunnel.FlowConfig {
	return tunnel.FlowConfig{
		InstanceID: instanceID, BootID: bootID, OwnerGen: owner,
		Budget:       budget,
		MaxEndpoints: tunnel.MaxEndpoints, MaxPendingFDs: flow.MaxPendingFDs,
		TokenLookup: func(req tunnel.AuthRequest) error {
			_, err := tunnelAuthorityLookup(b, req)
			return err
		},
		Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			return tunnelAuthorityLookup(b, req)
		},
	}
}

// Build before ImportServer takes any files. The shared receiving budget can
// already contain other domains; its global Used is never the server's exported
// reservation. P07 validates the actual private replay-capacity sum on import.
// OwnerGeneration is the agent-owner fence, not the daemon's image generation.
// origin: hot-update S6 plan §6.3 / T28
func brokerTunnelImportConfig(b *Broker, image tunnel.ServerImage, budget *workload.ReplayBudget) (tunnel.ServerImportConfig, error) {
	if runtime.GOOS != "linux" {
		return tunnel.ServerImportConfig{}, fmt.Errorf("broker: live tunnel image unsupported on this platform")
	}
	// Import only constructs dormant callbacks. The current DB is deliberately
	// opened later, after ACTIVE; requiring it here makes every real restored
	// serve launch refuse before taking the store lock.
	// origin: hot-update S6 plan §4.1 / §6.3 / T28
	if b == nil || image.IssuerBootID == "" || image.InstanceID == "" || image.OwnerGeneration == 0 || budget == nil {
		return tunnel.ServerImportConfig{}, fmt.Errorf("broker: missing current tunnel authority or image identity")
	}
	limit := budget.Limit
	if limit == 0 {
		limit = workload.DefaultReplayBudget
	}
	if image.ReplayLimit != limit || image.ReplayUsed < 0 || image.ReplayUsed != image.Flows.ReservedBytes || image.ReplayUsed > limit-budget.Used() {
		return tunnel.ServerImportConfig{}, fmt.Errorf("broker: tunnel image replay reservation mismatch")
	}
	// PublicHost is the listener bind host here, not the advertised expose
	// hostname. A new post-cut OPEN must use the same cold-start bind policy.
	// origin: hot-update S6 plan §6.3 / T28
	publicHost := b.cfg.TunnelPublicHost
	if publicHost == "" {
		publicHost = "0.0.0.0"
	}
	return tunnel.ServerImportConfig{
		PublicHost: publicHost, Lookup: b.tunnelTokenLookup,
		Certificate: b.tunnelCert, Logger: b.cfg.Logger,
		Flow: brokerTunnelFlowConfig(b, image.IssuerBootID, image.InstanceID, image.OwnerGeneration, budget),
	}, nil
}

// Stop's only worker cancels the synchronous SQL operation. Cleanup joins that
// worker before returning; no timeout can leave an authorization read behind.
// Pool acquisition, SQLite and all validation spend the same entry-time budget.
// origin: hot-update S5 plan §4.3 / T29
func tunnelAuthorityContext(req tunnel.AuthRequest) (context.Context, func()) {
	deadline := time.Now().Add(tunnel.LookupBudget)
	if !req.Deadline.IsZero() && req.Deadline.Before(deadline) {
		deadline = req.Deadline
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline) // ctx-none: frozen AuthRequest supplies Deadline and Stop, not a caller context.
	joined := make(chan struct{})
	if req.Stop == nil {
		close(joined)
	} else {
		select {
		case <-req.Stop:
			cancel()
		default:
		}
		go func() {
			defer close(joined)
			select {
			case <-req.Stop:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, func() { cancel(); <-joined }
}

// A committed LogCommand, unlike an election noop, belongs to the SQLite
// revision domain. Reading its ceiling separately never supplies authority:
// it only rejects a transaction whose applied cursor has not reached it.
// In particular, do not call CommandApplyLagging: its second, uncancellable SQL
// snapshot could both mix revisions and block outside this operation's budget.
// origin: hot-update S5 plan §4.3 / lead Q1 / T29
type tunnelCommandSource interface {
	CommitIndex() uint64
	RaftAppliedIndex() uint64
	CommittedCommandAt(uint64) (*cluster.Command, error)
}

func tunnelAuthorityKnownLag(ctx context.Context, src tunnelCommandSource, applied uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	commit := src.CommitIndex()
	if commit == 0 || src.RaftAppliedIndex() < commit {
		return true, nil
	}
	for index := commit; index > applied; index-- {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		_, err := src.CommittedCommandAt(index)
		switch {
		case err == nil, errors.Is(err, cluster.ErrLogPoison):
			return true, nil
		case errors.Is(err, cluster.ErrLogNonCommand):
			continue
		case errors.Is(err, cluster.ErrLogTruncated):
			return false, nil
		default:
			return false, err
		}
	}
	return false, ctx.Err()
}

// The unhomed legacy epoch ladder is intentionally inert. A v1 grant must
// instead describe the exact five-tuple that its Endpoint and ticket bind;
// otherwise a caller could authorize an invented epoch against an old row.
// Both entry points still perform the same complete bounded snapshot read.
// origin: hot-update S5 plan §2.1 / §4.3 / T29
func tunnelAuthorityLookup(b *Broker, req tunnel.AuthRequest) (tunnel.Authority, error) {
	grant, err := tunnelAuthoritySnapshot(b, req)
	if err != nil {
		return tunnel.Authority{}, err
	}
	if grant.Allocation != req.Allocation {
		return tunnel.Authority{}, errTunnelAuthorityRevoked
	}
	return grant, nil
}

func tunnelAuthoritySnapshot(b *Broker, req tunnel.AuthRequest) (tunnel.Authority, error) {
	grant, err := tunnelAuthoritySnapshotAttempt(b, req)
	if errors.Is(err, errTunnelAuthorityExpired) {
		return tunnel.Authority{}, errTunnelAuthorityRetry
	}
	return grant, err
}

// Only the legacy retry loop needs deadline provenance to retain an observed
// lag refusal. Public lookups still collapse expired reads to try_again; real
// store errors and committed revocations keep their existing classifications.
// origin: hot-update S5 plan §4.3 / T29 / DRILL-RUN run262.
func tunnelAuthoritySnapshotAttempt(b *Broker, req tunnel.AuthRequest) (tunnel.Authority, error) {
	ctx, done := tunnelAuthorityContext(req)
	defer done()
	grant, err := readTunnelAuthority(ctx, b, req.Allocation)
	if err != nil {
		// A TRANSIENT store fault must NOT masquerade as a revocation: a
		// reconnecting agent treats token_unknown_or_revoked as terminal
		// (proxy off) and stops forever — the false-online incident, this
		// time DB-triggerable. Emit a distinct transient reason the agent
		// retries. This leaks nothing: try_again fires only on the broker's
		// own DB fault, never as a function of token validity — the F6
		// anti-enumeration collapse below (absent/mismatch/off → one code)
		// is preserved.
		if errors.Is(err, errTunnelAuthorityRevoked) || errors.Is(err, errTunnelAuthorityLag) {
			return tunnel.Authority{}, err
		}
		if errors.Is(err, context.DeadlineExceeded) ||
			(errors.Is(err, sql.ErrTxDone) && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
			return tunnel.Authority{}, errTunnelAuthorityExpired
		}
		// Neither SQL driver text nor grant material may escape over DENY/logs.
		return tunnel.Authority{}, errTunnelAuthorityRetry
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return tunnel.Authority{}, errTunnelAuthorityExpired
	}
	if ctx.Err() != nil {
		return tunnel.Authority{}, errTunnelAuthorityRetry
	}
	return grant, nil
}

// All durable authorization fields come from one read transaction. busy_timeout
// is connection-local and the existing pools use five seconds; even QueryContext
// can spend that interval inside SQLite's busy handler. Use immediate refusal
// for this read, then restore the borrowed connection's original setting.
// origin: hot-update S5 plan §4.3 / T29
func readTunnelAuthority(ctx context.Context, b *Broker, want tunnel.Allocation) (tunnel.Authority, error) {
	if want.Port < 1 || want.Port > 65535 || want.Epoch < 0 || want.TokenHash == "" ||
		proto.ValidateSID(want.SID) != nil || proto.ValidateNID(want.NID) != nil {
		return tunnel.Authority{}, errTunnelAuthorityRevoked
	}
	// The accessor names this as a read; the SQL escape hatch is needed for
	// connection-local busy timeout and the single read-only transaction.
	// origin: hot-update S5 plan §4.3 / lead Q1
	conn, err := b.read().SQL().Conn(ctx)
	if err != nil {
		return tunnel.Authority{}, err
	}
	defer func() { _ = conn.Close() }()
	tx, release, err := beginBoundedAuthorityRead(ctx, conn)
	if err != nil {
		return tunnel.Authority{}, err
	}
	defer release()
	return readTunnelAuthorityTx(ctx, b, tx, want)
}

// Both authority paths must defeat the connection's longer busy handler before
// their single snapshot starts. No second SQL path or timeout worker may escape
// the operation deadline; release restores the setting before pool reuse.
// origin: hot-update S5 internal review rv-authority-2
func beginBoundedAuthorityRead(ctx context.Context, conn *sql.Conn) (*sql.Tx, func(), error) {
	var busy int
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy); err != nil {
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		return nil, nil, err
	}
	restore := func() {
		// This local PRAGMA does not wait for a database lock. Restore it even
		// when Stop cancelled the read, before returning the pooled connection.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("PRAGMA busy_timeout=%d", busy))
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		restore()
		return nil, nil, err
	}
	return tx, func() { _ = tx.Rollback(); restore() }, nil
}

func readTunnelAuthorityTx(ctx context.Context, b *Broker, tx *sql.Tx, want tunnel.Allocation) (tunnel.Authority, error) {
	grant := tunnel.Authority{Allocation: want}
	var revision string
	err := tx.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&revision)
	if err == nil {
		grant.AppliedRevision, err = strconv.ParseUint(revision, 10, 64)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return tunnel.Authority{}, err
	}
	// A positive token row on a known-lagging replica is no proof that a
	// committed revocation has applied. Check before both presence outcomes.
	// origin: hot-update S5 plan §4.3 / T29
	if b.clusterMode {
		if b.cl == nil || b.cl.node == nil {
			return tunnel.Authority{}, errTunnelAuthorityLag
		}
		lag, err := tunnelAuthorityKnownLag(ctx, b.cl.node, grant.AppliedRevision)
		if err != nil {
			return tunnel.Authority{}, err
		}
		if lag {
			return tunnel.Authority{}, errTunnelAuthorityLag
		}
	}
	var name, home, sessionState, nodeState string
	var storedEpoch int64
	var enabled, capable int
	err = tx.QueryRowContext(ctx, `SELECT a.name,a.home_broker,a.epoch,s.state,n.status,s.proxy_enabled,s.proxy_epoch,n.proxy_capable
		FROM port_allocations a JOIN sessions s ON s.sid=a.sid
		JOIN nodes n ON n.sid=a.sid AND n.nid=a.nid
		WHERE a.token_hash=? AND a.state='ALLOCATED' AND a.sid=? AND a.nid=? AND a.port=?`,
		want.TokenHash, want.SID, want.NID, want.Port).
		Scan(&name, &home, &storedEpoch, &sessionState, &nodeState, &enabled, &grant.ProxyEpoch, &capable)
	if errors.Is(err, sql.ErrNoRows) {
		return tunnel.Authority{}, errTunnelAuthorityRevoked
	}
	if err != nil {
		return tunnel.Authority{}, err
	}
	// Defense-in-depth (review A5 M6): a corrupted negative stored epoch must
	// not make every honest agent (epoch >= 0) loop on home_catching_up
	// forever. Treat it as a terminal, anti-enumeration-collapsed deny.
	if sessionState != "ACTIVE" || (nodeState != "ONLINE" && nodeState != "STALE") ||
		storedEpoch < 0 || grant.ProxyEpoch < 0 || (enabled != 0 && enabled != 1) {
		return tunnel.Authority{}, errTunnelAuthorityRevoked
	}
	// D6 home/epoch ladder: a newer presented epoch is transient, a superseded
	// epoch or wrong home is terminal. The unhomed legacy epoch remains inert.
	if home != "" {
		switch {
		case want.Epoch < storedEpoch:
			// The agent holds a SUPERSEDED directive (an OpPortReassignHome bumped
			// the row past it); its higher-epoch directive will rehome it. Terminal
			// (collapsed to the anti-enumeration code, like absent/mismatch/off).
			return tunnel.Authority{}, errTunnelAuthorityRevoked
		case want.Epoch > storedEpoch:
			// This replica has NOT yet applied the latest OpPortReassignHome
			// (REGARDLESS of home-vs-self — a higher presented epoch can only come
			// from a leader-committed directive this replica will eventually apply).
			// TRANSIENT: the agent retries until this home catches up. This IS the
			// catch-up barrier (§7.2c, epoch-as-local-row-epoch — NOT a raft index).
			return tunnel.Authority{}, errTunnelAuthorityLag
		case home != b.selfNodeID():
			// Same epoch, but this node is genuinely NOT the assigned home (an
			// ex-home or never-home replica at the same epoch). Terminal.
			return tunnel.Authority{}, errTunnelAuthorityRevoked
		}
	}
	grant.Allocation.Epoch = storedEpoch
	grant.Enabled = true // A normal expose does not depend on the SS master switch.
	if name != port.ProxyPortName {
		return grant, ctx.Err()
	}
	// round-3 F1: for the system __proxy__ port, the master switch is part of
	// the authorization boundary. Even if the ALLOCATED row is briefly visible
	// between CloseProxy and port.Free, a REGISTER must be denied while proxy is
	// OFF — otherwise the kill switch leaks an exit through a re-REGISTER race.
	// The same boundary now dominates each fresh RESUME grant as well.
	// origin: hot-update S5 plan §4.3
	// The retained allocation is insufficient after node participation changes.
	// Read that predicate from this same snapshot, matching proxy push authority.
	// origin: hot-update S5 internal review rv-authority-1
	if !proxyParticipationEnabled(enabled, capable, nodeState) {
		return tunnel.Authority{}, errTunnelAuthorityRevoked
	}
	if !b.clusterMode {
		if err := tx.QueryRowContext(ctx, `SELECT generation FROM proxy_meta WHERE id=1`).Scan(&grant.Generation); err != nil {
			return tunnel.Authority{}, err
		}
		if grant.Generation < 0 || grant.Generation >= maxProxyGeneration {
			return tunnel.Authority{}, errTunnelAuthorityRevoked
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT sub_id,psk FROM proxy_subscribers WHERE sid=? AND state='ACTIVE' ORDER BY sub_id`, want.SID)
	if err != nil {
		return tunnel.Authority{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, secret string
		if err := rows.Scan(&id, &secret); err != nil {
			return tunnel.Authority{}, err
		}
		if id == "" || secret == "" {
			return tunnel.Authority{}, errTunnelAuthorityRevoked
		}
		// Secret retains the password representation used by ProxyKey/SS Key;
		// key derivation stays in ssproxy, never in the authorization adapter.
		grant.Keys = append(grant.Keys, tunnel.AuthorizedKey{ID: id, Secret: []byte(secret)})
	}
	if err := rows.Err(); err != nil {
		return tunnel.Authority{}, err
	}
	return grant, ctx.Err()
}
