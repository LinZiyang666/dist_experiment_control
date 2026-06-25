package agent

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/proto"
)

// P13 agent-side proxy orchestration. The broker delivers a ProxyDirective
// either in the register reply (join/reconnect) or via the per-(sid,nid)
// proxy-keys.req.forwarded push (live keyset delta). applyProxyDirective is
// idempotent and epoch-guarded so out-of-order / duplicate directives converge.
//
// Lifecycle: a full directive (Token != "") (re)establishes the SS server +
// tunnel; a keyset-only directive (Token == "") swaps keys on the running
// server when the epoch differs. PSKs are never persisted — only the tunnel
// footprint (public port, local port, token, epoch) goes to state.json.

type proxyRuntime struct {
	mu         sync.Mutex
	srv        *ssproxy.Server
	publicPort int
	localPort  int
	token      string // cached tunnel token (so an epoch-only persist never blanks it)
	// gen + epoch are the last SUCCESSFULLY-applied ordering pair (round-3).
	gen   int64
	epoch int64
	// needsReestablish (round-6 F7) is set by a LOCAL fail-closed teardown. While
	// set, applyProxyDirective accepts an exact-equal full (token-bearing)
	// directive — the authoritative reconnect resync — to rebuild, even though it
	// isn't strictly newer. Cleared on any successful (re)build or disable.
	needsReestablish bool
}

// proxyNewer reports whether directive (dGen,dEpoch) is strictly newer than the
// applied (appliedGen,appliedEpoch) under lexicographic ordering: a higher
// generation always wins (DB-restore convergence); within one generation a
// higher epoch wins; equal or lower is stale.
func proxyNewer(dGen, dEpoch, appliedGen, appliedEpoch int64) bool {
	if dGen != appliedGen {
		return dGen > appliedGen
	}
	return dEpoch > appliedEpoch
}

// ensureProxyRuntime returns the lifetime-owned proxy runtime. It is created
// eagerly in New (F2) so concurrent forwarded directives never race to create
// competing runtimes — this is a plain read of an immutable pointer.
func (a *Agent) ensureProxyRuntime() *proxyRuntime {
	return a.proxy
}

// proxyGenEpoch returns the agent's last-applied (generation, epoch) ordering
// pair, or (0,0) when not serving. Reported in the heartbeat so the broker can
// detect a generation mismatch even at an equal epoch (round-4 F2).
func (a *Agent) proxyGenEpoch() (gen, epoch int64) {
	p := a.proxy
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.srv == nil {
		return 0, 0
	}
	return p.gen, p.epoch
}

func toSSKeys(keys []proto.ProxyKey) []ssproxy.Key {
	out := make([]ssproxy.Key, 0, len(keys))
	for _, k := range keys {
		out = append(out, ssproxy.Key{KeyID: k.SubID, Secret: k.Secret})
	}
	return out
}

// applyProxyDirective converges the local SS proxy to the directive. ctx is
// the agent run context (anchors the SS server lifetime). nc is used to ACK
// readiness. A nil directive means "ensure proxy off".
func (a *Agent) applyProxyDirective(ctx context.Context, nc *nats.Conn, d *proto.ProxyDirective) {
	p := a.ensureProxyRuntime()
	p.mu.Lock()
	defer p.mu.Unlock()

	// F4: a nil directive is the proxy-OFF *register* representation (kept nil
	// for byte-identity). It carries no epoch and so cannot be ordered against
	// live enable/disable pushes — treat it as "no opinion" and NEVER tear a
	// running proxy down on it. Authoritative OFF is delivered by the disable
	// push and the heartbeat OFF repair (broker repairProxyEpoch). At fresh
	// boot the agent isn't serving, so a nil is a clean no-op.
	if d == nil {
		return
	}

	// Round-7 F3: an exact-equal ENABLED directive while we are STILL SERVING is
	// an idempotent re-ACK request (the broker lost readiness — first ready
	// publish dropped, or an OFFLINE flap cleared proxy_ready — and re-sent the
	// current authoritative pair). Re-publish ready WITHOUT rebuild or pair
	// advance, instead of dropping it at the strict-newer guard below; otherwise
	// a correctly-serving node never re-enters /sub.
	if d.Enabled && p.srv != nil && d.Generation == p.gen && d.Epoch == p.epoch {
		a.pubProxyReady(nc, true)
		return
	}

	// Round-6 F7: after a LOCAL fail-closed teardown we kept the applied pair but
	// stopped serving. The broker re-sends the SAME authoritative full directive
	// on reconnect; accept that exact-equal token-bearing directive to rebuild.
	// Strictly limited to (a) needsReestablish state, (b) a full (Token) directive
	// at (c) the EXACT applied pair — a stale LOWER directive is still dropped, so
	// this can't resurrect a revoked-after-OFF state.
	reestablish := p.needsReestablish && d.Enabled && d.Token != "" &&
		d.Generation == p.gen && d.Epoch == p.epoch

	// Round-3 ordering: drop a directive that is NOT strictly newer than the
	// last SUCCESSFULLY-applied (gen, epoch). This single check covers ALL
	// sources — register reply, live push, heartbeat repair — so a stale
	// register reply can't resurrect after a newer OFF, while a higher-
	// generation restore (lower epoch) still converges. NOTE: the pair is
	// recorded only on SUCCESS below, so a transient start failure does not
	// advance it (the retry directive is still "newer" → F3 retry works).
	if !reestablish && !proxyNewer(d.Generation, d.Epoch, p.gen, p.epoch) {
		a.cfg.Logger.Debug("agent: dropping stale proxy directive",
			"dir_gen", d.Generation, "dir_epoch", d.Epoch, "applied_gen", p.gen, "applied_epoch", p.epoch)
		return
	}

	if !d.Enabled {
		a.proxyTeardownLocked(p, nc, true /* clearPersist + ack unready */)
		p.gen = d.Generation
		p.epoch = d.Epoch
		p.needsReestablish = false
		return
	}

	keys := toSSKeys(d.Keys)

	switch {
	case d.Token != "":
		// Full (re)establish: a fresh tunnel token was minted. Tear down any
		// existing server/tunnel (without an unready flap) and rebuild.
		a.proxyTeardownLocked(p, nc, false)
		a.proxyStartLocked(ctx, nc, p, d.PublicPort, 0, d.Token, d.Generation, d.Epoch, keys)

	case p.srv == nil:
		// Keyset-only push but no running server (e.g. fresh boot reconnect-
		// keep, a sub change before the full directive, or a transient-start
		// retry). Bootstrap from the persisted footprint if we have one;
		// otherwise wait for a full directive.
		ps := a.loadProxyStateSafe()
		if ps == nil || ps.PublicPort == 0 || ps.Token == "" {
			a.cfg.Logger.Warn("agent: proxy keyset push with no server and no persisted footprint; awaiting full directive")
			return
		}
		a.proxyStartLocked(ctx, nc, p, ps.PublicPort, ps.LocalPort, ps.Token, d.Generation, d.Epoch, keys)

	default:
		// Running server: swap the key set and advance the applied pair.
		if err := p.srv.SetKeys(keys); err != nil {
			a.cfg.Logger.Warn("agent: proxy SetKeys", "err", err)
			return
		}
		p.gen = d.Generation
		p.epoch = d.Epoch
		a.persistProxyEpochLocked(p)
		// M3: re-ACK readiness — a node whose first ready publish was lost (or
		// whose proxy_ready was cleared by an OFFLINE flap) is otherwise stuck
		// out of /sub forever despite serving correctly.
		a.pubProxyReady(nc, true)
	}
}

func (a *Agent) proxyStartLocked(ctx context.Context, nc *nats.Conn, p *proxyRuntime,
	publicPort, wantLocal int, token string, gen, epoch int64, keys []ssproxy.Key) {

	if a.cfg.ExposeAdapter == nil {
		a.cfg.Logger.Warn("agent: proxy tunnel adapter unavailable")
		a.proxyFailCleanupLocked(p, nc)
		return
	}

	srv := ssproxy.New(a.cfg.Logger)
	// round-6 F12: default to internet-egress only — a subscription must NOT
	// reach this agent's loopback, LAN/VPC, link-local, or cloud-metadata
	// endpoints. Operators who intend private-network access opt in via
	// Config.ProxyAllowPrivateDestinations.
	if !a.cfg.ProxyAllowPrivateDestinations {
		srv.DenyPrivateDestinations()
	}
	lp, err := srv.Start(ctx, wantLocal, keys)
	if err != nil {
		a.cfg.Logger.Warn("agent: ssproxy start", "err", err)
		a.proxyFailCleanupLocked(p, nc)
		return
	}
	// round-2 F3: persist the footprint (port+token+localPort) BEFORE opening
	// the tunnel. If AddProxy then transiently fails, the footprint survives so
	// the next keyset-only push can bootstrap-and-retry — instead of stranding a
	// connected agent unready until an unrelated reconnect.
	if a.stateStore != nil {
		if err := a.stateStore.SetProxy(&ProxyState{
			PublicPort: publicPort, LocalPort: lp, Token: token, Epoch: epoch,
		}); err != nil {
			a.cfg.Logger.Warn("agent: persist proxy state", "err", err)
		}
	}
	// Publish the proxy public port for the tunnel session-state hook BEFORE
	// opening the tunnel, so a drop/reconnect callback (fired on the supervisor
	// goroutine the instant AddProxy spawns it) is correctly attributed to the
	// proxy port rather than filtered out. Cleared on teardown / fail-cleanup.
	a.proxyPublicPort.Store(int64(publicPort))
	if err := a.cfg.ExposeAdapter.AddProxy(PortToken{
		Name: proxyTokenName, Port: publicPort, LocalPort: lp, Token: token,
	}); err != nil {
		a.cfg.Logger.Warn("agent: proxy tunnel open", "err", err)
		srv.Stop()
		a.proxyFailCleanupLocked(p, nc)
		return
	}
	p.srv = srv
	p.publicPort = publicPort
	p.localPort = lp
	p.token = token
	p.gen = gen
	p.epoch = epoch
	p.needsReestablish = false // rebuilt — clear the fail-closed resync flag (F7)
	a.pubProxyReady(nc, true)
	a.cfg.Logger.Info("agent: proxy enabled", "public_port", publicPort, "local_port", lp, "keys", len(keys))
}

// proxyFailCleanupLocked (F3) runs when a (re)build fails. It resets the
// in-memory runtime and ACKs unready so the broker stops rendering a dead node
// — but it KEEPS the persisted footprint (round-2 F3) so the next keyset-only
// push can re-bootstrap and retry the tunnel. The footprint is only wiped by an
// authoritative disable / fail-closed (proxyTeardownLocked with clearPersist).
//
// It deliberately does NOT touch p.gen/p.epoch: those are advanced ONLY on a
// successful apply (proxyStartLocked tail / keyset / disable), so a failed start
// leaves them at the PRIOR success — strictly LOWER than the retry directive's
// pair, which therefore passes proxyNewer and re-applies. Resetting them here
// would instead let a stale lower directive re-apply. (Retry is covered by
// TestExternalReviewTransientProxyStartFailureCanRecover.)
func (a *Agent) proxyFailCleanupLocked(p *proxyRuntime, nc *nats.Conn) {
	p.srv = nil
	p.publicPort = 0
	p.localPort = 0
	p.token = ""
	a.proxyPublicPort.Store(0) // stop attributing tunnel transitions to the proxy
	a.pubProxyReady(nc, false)
}

// proxyTeardownLocked stops the SS server + tunnel. When clearPersist is true
// it also wipes the persisted footprint and ACKs unready (the proxy-off path);
// when false it is a silent teardown ahead of an immediate rebuild.
func (a *Agent) proxyTeardownLocked(p *proxyRuntime, nc *nats.Conn, clearPersist bool) {
	// Close the readiness-hook filter FIRST (before RemoveProxy → Client.Close
	// cancels the supervisor), so any final supervisor edge fired during
	// teardown is filtered out and cannot publish a stale ready/unready for a
	// port we're tearing down.
	a.proxyPublicPort.Store(0)
	if p.srv != nil {
		if a.cfg.ExposeAdapter != nil {
			_ = a.cfg.ExposeAdapter.RemoveProxy(proxyTokenName, p.publicPort)
		}
		p.srv.Stop()
		p.srv = nil
	}
	p.publicPort = 0
	p.localPort = 0
	p.token = ""
	if clearPersist {
		if a.stateStore != nil {
			_ = a.stateStore.SetProxy(nil)
		}
		a.pubProxyReady(nc, false)
	}
}

func (a *Agent) persistProxyEpochLocked(p *proxyRuntime) {
	if a.stateStore == nil || p.token == "" {
		// p.token is the in-memory cache set at start time; never re-read from
		// disk (a transient read error must not blank a good footprint).
		return
	}
	_ = a.stateStore.SetProxy(&ProxyState{
		PublicPort: p.publicPort, LocalPort: p.localPort, Token: p.token, Epoch: p.epoch,
	})
}

func (a *Agent) loadProxyStateSafe() *ProxyState {
	if a.stateStore == nil {
		return nil
	}
	ps, err := a.stateStore.GetProxy()
	if err != nil {
		a.cfg.Logger.Warn("agent: load proxy state", "err", err)
		return nil
	}
	return ps
}

// pubProxyReady publishes the SS-bind ACK so the broker can set/clear
// nodes.proxy_ready (the render gate for /sub). Best-effort.
func (a *Agent) pubProxyReady(nc *nats.Conn, ready bool) {
	if nc == nil {
		return
	}
	kind := "unready"
	if ready {
		kind = "ready"
	}
	_ = nc.Publish(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, kind), nil)
}

// handleProxyKeysForwarded applies one live keyset push. seq is the arrival
// order assigned in dispatchForwarded (the NATS subscription delivers a single
// publisher's pushes in order; seq + proxyApplyMu serialize application so the
// per-message goroutines can't reorder and resurrect a revoked key — the
// ordering guarantee that lets applyProxyDirective drop its epoch guard, F2).
func (a *Agent) handleProxyKeysForwarded(nc *nats.Conn, msg *nats.Msg, seq int64) {
	var d proto.ProxyDirective
	if err := json.Unmarshal(msg.Data, &d); err != nil {
		a.cfg.Logger.Warn("agent: proxy-keys parse", "err", err)
		return
	}
	ctx := a.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	a.proxyApplyMu.Lock()
	defer a.proxyApplyMu.Unlock()
	if seq <= a.lastAppliedPushSeq {
		return // an out-of-order goroutine for an older push; already superseded
	}
	a.lastAppliedPushSeq = seq
	a.applyProxyDirective(ctx, nc, &d)
}

// proxyTokenName is the reserved state.json/port name for the proxy tunnel.
// Kept in sync with port.ProxyPortName (broker side) without importing the
// broker storage package into the agent.
const proxyTokenName = "__proxy__"

// onNATSReconnect re-registers after a NATS reconnect and re-applies the
// broker's reconciliation + proxy directive. Runs on its own goroutine
// (off the NATS callback goroutine) because register may retry.
func (a *Agent) onNATSReconnect(nc *nats.Conn) {
	a.cancelFailClosed() // B1: a reconnect cancels the fail-closed countdown
	a.ncBox.Store(nc)    // keep the session-state hook publishing on the live conn
	ctx := a.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := a.register(ctx, nc)
	if err != nil {
		a.cfg.Logger.Warn("agent: re-register on reconnect failed", "err", err)
		return
	}
	a.applyReconciliation(ctx, resp)
	a.applyProxyDirective(ctx, nc, resp.Proxy)
	// M3: re-ACK readiness if we are serving (covers a lost first ACK or a
	// proxy_ready cleared while we were partitioned).
	if p := a.proxy; p != nil {
		p.mu.Lock()
		serving := p.srv != nil
		p.mu.Unlock()
		if serving {
			a.pubProxyReady(nc, true)
		}
	}
	a.cfg.Logger.Info("agent: re-registered after reconnect")
}

// armFailClosed (B1) starts the fail-closed countdown on NATS disconnect: if
// the agent stays partitioned for ProxyFailClosedGrace without reconnecting,
// it proactively tears the SS server down so a revoked subscriber cannot keep
// egressing past the broker's OFFLINE→port-REVOKE window. Idempotent.
func (a *Agent) armFailClosed() {
	// Don't arm during graceful shutdown — the nc.Drain() on Run exit fires a
	// DisconnectErr, and a timer started here would outlive the agent.
	if a.runCtx != nil && a.runCtx.Err() != nil {
		return
	}
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	if a.flcTimer != nil {
		return // already counting down
	}
	grace := a.cfg.ProxyFailClosedGrace
	if grace <= 0 {
		grace = 15 * time.Minute
	}
	a.flcTimer = time.AfterFunc(grace, a.failClosedFire)
}

func (a *Agent) cancelFailClosed() {
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	if a.flcTimer != nil {
		a.flcTimer.Stop()
		a.flcTimer = nil
	}
}

func (a *Agent) failClosedFire() {
	a.flcMu.Lock()
	a.flcTimer = nil
	a.flcMu.Unlock()
	a.cfg.Logger.Warn("agent: fail-closed — NATS partitioned past grace, stopping embedded proxy")
	// Direct teardown (not via applyProxyDirective, whose nil path is now a
	// no-op): stop SS, drop the tunnel, clear the footprint. A reconnect
	// re-establishes from a fresh broker directive.
	p := a.proxy
	p.mu.Lock()
	a.proxyTeardownLocked(p, nil, true)
	// round-6 F7: this was a LOCAL fail-closed teardown, NOT an authoritative OFF.
	// The applied (gen,epoch) is retained (so a stale lower enable still can't
	// resurrect), but on reconnect the broker re-sends the SAME authoritative
	// pair; mark that we need to accept that exact-equal full directive to rebuild
	// (see applyProxyDirective), instead of dropping it as not-strictly-newer.
	p.needsReestablish = true
	p.mu.Unlock()
}
