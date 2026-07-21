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
	// homeAddr + homeEpoch (C5) are the last-applied proxy HOME on its OWN epoch axis (distinct from
	// the keyset epoch above). A directive with a newer Home.Epoch re-dials the tunnel via ApplyHome
	// WITHOUT a keyset rebuild — so a home failover is independent of a keyset rotation.
	homeAddr  string
	homeEpoch int64
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

// proxyBound (C5) reports whether the embedded SS proxy is currently serving — the heartbeat's
// ProxyBound flag, from which each cluster broker writes its own liveness proxy_ready (the cross-broker
// convergence signal). False when proxy is off / not yet bound.
func (a *Agent) proxyBound() bool {
	p := a.proxy
	if p == nil {
		return false
	}
	p.mu.Lock()
	srvUp := p.srv != nil
	p.mu.Unlock()
	// M2: bound == SS server up AND the reverse tunnel live. A pure tunnel drop (srv still up, home
	// alive) must report unready so a cluster broker does NOT resurrect proxy_ready over a dead tunnel.
	return srvUp && a.proxyTunnelUp.Load()
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

	// C5: the proxy HOME rides a SEPARATE epoch axis (d.Home.Epoch) from the keyset (d.Epoch). A
	// home failover re-points the home WITHOUT bumping the keyset, so a rehome directive carries the
	// SAME keyset (gen, epoch) + a strictly-newer Home.Epoch — handle it HERE, before the keyset
	// exact-equal re-ACK + the proxyNewer guard (both of which key on the keyset pair and would
	// otherwise drop/early-return a pure rehome). Re-dial in place via ApplyHome (no SS rebuild, no
	// in-flight drop); a monotone Home.Epoch guard makes a stale lower home a no-op on every replica.
	// N1: scope the rehome to the CURRENT allocation (d.Home.PublicPort == the running proxy port). The
	// home epoch is per-allocation (resets to 0 on a fresh __proxy__ row after off/on), so a runtime-
	// global epoch guard alone could let a STALE directive minted for a prior allocation (epoch > 0)
	// re-dial the new port to the old home and fence the new epoch-0 home forever.
	rehomeFailed := false
	if d.Enabled && d.Home != nil && p.srv != nil && d.Home.PublicPort == p.publicPort && d.Home.Epoch > p.homeEpoch {
		if ha, ok := a.cfg.ExposeAdapter.(homeApplier); ok {
			err := ha.ApplyHome(p.publicPort, d.Home.BrokerAddr, d.Home.Epoch, d.Home.CertPins)
			// M4: ApplyHome returns nil (success no-op) when the tunnel SESSION is absent — that is NOT
			// a real rehome (no tunnel was opened). Only advance the home epoch + persist when the
			// session actually exists; otherwise leave homeEpoch so the reaper's next push retries, and
			// publish UNREADY (N2) so /sub stops vending a home with no live tunnel until it converges.
			sessionPresent := err == nil
			if hc, ok2 := a.cfg.ExposeAdapter.(homeSessionChecker); ok2 && !hc.HasSession(p.publicPort) {
				sessionPresent = false
			}
			if err != nil {
				a.cfg.Logger.Warn("agent: proxy rehome (ApplyHome)", "err", err)
				a.pubProxyReady(nc, false)
				rehomeFailed = true
			} else if !sessionPresent {
				a.cfg.Logger.Warn("agent: proxy rehome no-op (tunnel session absent); not advancing home epoch")
				a.pubProxyReady(nc, false)
				rehomeFailed = true
			} else {
				p.homeAddr, p.homeEpoch = d.Home.BrokerAddr, d.Home.Epoch
				a.persistProxyHomeLocked(p)
				// #33 (R8a): RESTORE the tunnel-liveness flag. This branch has just
				// PROVEN a live tunnel — ApplyHome returned nil AND HasSession(port) is
				// true — yet before this line it never touched proxyTunnelUp, whose only
				// writers are the supervisor's drop/redial edges (agent.go notifyState)
				// and the proxy start/teardown paths. ApplyHome→OpenHome never calls
				// notifyState, so after a crash-rehome the flag stayed stuck at the
				// `false` the drop edge wrote, while the new tunnel was actually up.
				//
				// The consequence was the reported non-determinism, with a perverse
				// inversion: a FLAPPING tunnel self-heals (its redial edge fires
				// notifyState(true)) while a STABLE rehomed one never does. ProxyBound
				// stayed false ⇒ every heartbeat wrote proxy_ready=false, racing the ACK
				// path's true, and after proxyRehomeDwell bad reads the broker's reaper
				// minted a fresh port+token and stranded the data plane. `proxy off;
				// proxy on` worked only because it takes the token path — which is
				// exactly the manual workaround operators had found.
				a.proxyTunnelUp.Store(true)
				a.cfg.Logger.Info("agent: proxy rehomed", "home", d.Home.BrokerAddr, "home_epoch", d.Home.Epoch)
			}
		}
	}

	// Round-7 F3: an exact-equal ENABLED directive while we are STILL SERVING is
	// an idempotent re-ACK request (the broker lost readiness — first ready
	// publish dropped, or an OFFLINE flap cleared proxy_ready — and re-sent the
	// current authoritative pair). Re-publish ready WITHOUT rebuild or pair
	// advance, instead of dropping it at the strict-newer guard below; otherwise
	// a correctly-serving node never re-enters /sub.
	//
	// N2 (Stage-C closure): a FAILED rehome on this same directive already published UNREADY above. A
	// rehome carries the SAME keyset (gen, epoch) (it bumps only the per-port home epoch), so this
	// exact-equal condition is ALWAYS true after a rehome attempt — re-ACKing ready here would override
	// the unready and keep /sub vending a home with no live tunnel. Suppress the re-ACK when the rehome
	// failed; still return (the keyset is unchanged, nothing else to apply).
	// Mega-audit MAJ-3: a reaper ROTATION mints a NEW port+token at the SAME keyset (gen,epoch) — the
	// keyset axis is unchanged, only the port rotates. A serving node at the exact keyset must REBUILD on
	// the new port (the `case d.Token != ""` arm below), NOT false-re-ACK ready on the old (now freed)
	// port — that strands the data plane and black-holes /sub (the broker renders the new port the agent
	// never opened). So the re-ACK fires only for a keyset-only / same-port directive.
	portRebuild := d.Enabled && d.Token != "" && d.PublicPort != p.publicPort &&
		d.Generation == p.gen && d.Epoch == p.epoch
	if d.Enabled && p.srv != nil && d.Generation == p.gen && d.Epoch == p.epoch && !portRebuild {
		if !rehomeFailed {
			a.pubProxyReady(nc, true)
		}
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
	if !reestablish && !portRebuild && !proxyNewer(d.Generation, d.Epoch, p.gen, p.epoch) {
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
		a.proxyStartLocked(ctx, nc, p, d.PublicPort, 0, d.Token, d.Generation, d.Epoch, keys, d.Home)

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
		// C5: prefer the directive's Home (has cert pins); else replay the persisted home (no pins →
		// the dial defers until a register reply re-delivers them, like a clustered expose).
		home := d.Home
		if home == nil && ps.HomeBrokerAddr != "" {
			home = &proto.HomeDirective{BrokerAddr: ps.HomeBrokerAddr, Epoch: ps.HomeEpoch}
		}
		a.proxyStartLocked(ctx, nc, p, ps.PublicPort, ps.LocalPort, ps.Token, d.Generation, d.Epoch, keys, home)

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
	publicPort, wantLocal int, token string, gen, epoch int64, keys []ssproxy.Key, home *proto.HomeDirective) {

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
	homeAddr, homeEpoch := "", int64(0)
	var homePins proto.CertPins
	if home != nil {
		homeAddr, homeEpoch, homePins = home.BrokerAddr, home.Epoch, home.CertPins
	}
	if a.stateStore != nil {
		if err := a.stateStore.SetProxy(&ProxyState{
			PublicPort: publicPort, LocalPort: lp, Token: token, Epoch: epoch,
			HomeBrokerAddr: homeAddr, HomeEpoch: homeEpoch,
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
		HomeBrokerAddr: homeAddr, Epoch: homeEpoch, CertPins: homePins,
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
	p.homeAddr = homeAddr
	p.homeEpoch = homeEpoch
	p.needsReestablish = false  // rebuilt — clear the fail-closed resync flag (F7)
	a.proxyTunnelUp.Store(true) // M2: tunnel just opened — ProxyBound now reflects a live tunnel
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
	a.proxyTunnelUp.Store(false)
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
	a.proxyTunnelUp.Store(false)
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
	// C5: carry the home fields too — a keyset-epoch persist must NOT blank the proxy home.
	_ = a.stateStore.SetProxy(&ProxyState{
		PublicPort: p.publicPort, LocalPort: p.localPort, Token: p.token, Epoch: p.epoch,
		HomeBrokerAddr: p.homeAddr, HomeEpoch: p.homeEpoch,
	})
}

// persistProxyHomeLocked persists the proxy footprint after a rehome (the same full ProxyState write,
// now carrying the advanced home epoch). Same merge semantics as persistProxyEpochLocked.
func (a *Agent) persistProxyHomeLocked(p *proxyRuntime) { a.persistProxyEpochLocked(p) }

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
	ctx := a.loadRunCtx()
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
	// C1 §D-1 L3: a stuck-reconnect rebuild owns the conn lifecycle; if one is in flight the
	// session loop re-registers on the FRESH conn, so a late reconnect on the dying conn must not.
	if a.rebuilding.Load() {
		return
	}
	a.cancelFailClosed() // B1: a reconnect cancels the fail-closed countdown
	// Stage-C MAJOR-2: re-check rebuilding immediately before publishing nc — a rebuild may have
	// started since the entry check, and storing the (about-to-be-Closed) old conn here would
	// clobber the fresh session's ncBox and silently drop proxy-readiness signaling.
	if a.rebuilding.Load() {
		return
	}
	a.ncBox.Store(nc) // keep the session-state hook publishing on the live conn
	ctx := a.loadRunCtx()
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
	if rc := a.loadRunCtx(); rc != nil && rc.Err() != nil {
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
