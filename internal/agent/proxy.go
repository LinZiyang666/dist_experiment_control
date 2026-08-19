package agent

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/backoff"
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

	// dial + lastFail (#78) are the FIRST-DIAL backoff for the proxy tunnel.
	// The broker's heartbeat convergence loop re-pushes the directive every
	// ~5s to a not-ready node, and each push used to re-dial unconditionally
	// — a node whose tunnel path is broken (half-open :7000) dialed forever
	// at 5s, feeding the broker's `read REGISTER` WARN flood (#78, the fuel
	// of the #75/#77 disk-fill). The gate lives in proxyStartLocked (the
	// single dial choke point — repair pushes are keyset-only and arrive
	// here via the footprint-bootstrap arm) and ONLY holds back a retry of
	// the SAME resolved dial identity: any new gen/epoch/port/token/home
	// (operator proxy off/on, reaper re-mint, rehome) bypasses and resets.
	// Both fields are guarded by mu — backoff.Tracker is NOT goroutine-safe
	// on its own. dial==nil ⇒ healthy (next failure starts a fresh run at
	// Base); the supervisor backoff for ESTABLISHED sessions (tunnel.Client)
	// is a separate, untouched mechanism.
	dial     *backoff.Tracker
	lastFail proxyDialID

	// optOutAnnounced (#78): the local opt-out gate logs at Info once per
	// process, then at Debug — an old broker re-pushes every 5s and a 5s
	// Info line would just move the flood from the broker's log to ours.
	optOutAnnounced bool

	// configWarnAnnounced (#78 review Mi3): the two config-wound arms of
	// applyProxyDirective — a nil ExposeAdapter (`--tunnel-addr ""` debug
	// shape) and a keyset push with no server AND no persisted footprint —
	// do not self-heal within the push loop, yet the broker re-pushes every
	// ~5s. WARN once then Debug so they don't recreate the very disk-fill
	// flood #78 exists to end. Cleared on any successful start.
	configWarnAnnounced bool
}

// proxyDialID is the resolved dial identity the #78 backoff keys on. Fields
// mirror what proxyStartLocked is about to dial with — NOT the raw directive
// (repair pushes are keyset-only with port=0/token="", which would never
// match and defeat the backoff). Any component changing means the broker
// minted something new and the retry must go out immediately.
type proxyDialID struct {
	gen       int64
	epoch     int64
	port      int
	token     string // in-memory only, mirrors proxyRuntime.token; NEVER logged
	homeEpoch int64  // C5: a rehome bumps ONLY this — without it a failover would sit out the old home's backoff
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
	if leasedInstanceRefusesProxy(a, d) {
		return
	}
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

	// #78 opt-out gate (the N-1 belt): a #78-aware broker never pushes to an
	// opted-out node (register folds ProxyOptOut into nodes.proxy_capable),
	// so this arm exists for OLD brokers that keep pushing.
	if a.cfg.ProxyOptOut {
		refuseProxyDirectiveOptedOut(a, nc, p, d)
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
			logConfigWarnOnce(a, p, "agent: proxy keyset push with no server and no persisted footprint; awaiting full directive")
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

// refuseProxyDirectiveOptedOut (#78) is the opted-out arm of applyProxyDirective,
// extracted so the dispatcher stays under the god-function detector. It refuses
// to build anything, keeps publishing unready so /sub never renders this node,
// and never dials — the dial is exactly the 5s flood #78 documents. Info once,
// Debug thereafter (an old broker re-pushes every heartbeat). Caller holds p.mu.
func refuseProxyDirectiveOptedOut(a *Agent, nc *nats.Conn, p *proxyRuntime, d *proto.ProxyDirective) {
	if p.optOutAnnounced {
		a.cfg.Logger.Debug("agent: proxy directive ignored (proxy.participate=false)")
	} else {
		p.optOutAnnounced = true
		a.cfg.Logger.Info("agent: proxy participation disabled by agent.yaml (proxy.participate=false); ignoring proxy directives")
	}
	if d.Enabled {
		a.pubProxyReady(nc, false)
	}
}

func (a *Agent) proxyStartLocked(ctx context.Context, nc *nats.Conn, p *proxyRuntime,
	publicPort, wantLocal int, token string, gen, epoch int64, keys []ssproxy.Key, home *proto.HomeDirective) {

	homeAddr, homeEpoch := "", int64(0)
	var homePins proto.CertPins
	if home != nil {
		homeAddr, homeEpoch, homePins = home.BrokerAddr, home.Epoch, home.CertPins
	}

	// #78 backoff gate — BEFORE any side effect. Holds back ONLY an exact
	// retry of the identity that just failed; anything the broker minted
	// fresh (proxy off/on bumps epoch, a reaper rotation changes port+token,
	// a rehome bumps homeEpoch) bypasses immediately, so no operator action
	// ever waits out a backoff window. While held: no SS server, no dial, no
	// ACK — the node simply stays unready until the next push after Due.
	id := proxyDialID{gen: gen, epoch: epoch, port: publicPort, token: token, homeEpoch: homeEpoch}
	now := proxyNow(a)
	if p.dial != nil && id == p.lastFail && !p.dial.Due(now) {
		a.cfg.Logger.Debug("agent: proxy dial backoff active; retry deferred",
			"public_port", publicPort, "fails", p.dial.Fails())
		return
	}
	if p.dial != nil && id != p.lastFail {
		// A NEW identity is a NEW failure story: the deep schedule earned by
		// the old (gen,epoch,port,token,home) must not throttle a fresh mint
		// — an operator's `proxy off/on` or a reaper rotation deserves the
		// full 5s-base ladder, not a 5min hangover.
		p.dial = nil
	}

	if a.cfg.ExposeAdapter == nil {
		// Configuration wound, not a network one: deliberately OUTSIDE the
		// backoff (a missing adapter never self-heals). WARN once then Debug
		// (review Mi3) — the broker re-pushes every ~5s, so a per-push WARN
		// would itself be the flood #78 ends.
		logConfigWarnOnce(a, p, "agent: proxy tunnel adapter unavailable")
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
		proxyDialFailedLocked(a, p, id, "ssproxy_start", err)
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
		proxyDialFailedLocked(a, p, id, "tunnel_dial", err)
		srv.Stop()
		a.proxyFailCleanupLocked(p, nc)
		return
	}
	// #78: a successful dial ends the failure run — Recover both logs the
	// suppressed count and resets the tracker (a dial can only be reached
	// when Due, and the first window IS Base, so the run length is always
	// ≥ Base and Recover's anti-flap fold cannot trigger here). Dropping the
	// pointer afterwards is invariant maintenance, not a second reset:
	// "p.dial != nil ⇒ a failure run is in progress" is what the entry gate
	// reads.
	if p.dial != nil && p.dial.Failing() {
		suppressed, _ := p.dial.Recover(now)
		a.cfg.Logger.Info("agent: proxy tunnel recovered", "suppressed", suppressed)
	}
	p.dial = nil
	p.lastFail = proxyDialID{}
	p.configWarnAnnounced = false // review Mi3: a healthy start re-arms the config-wound WARN
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

// logConfigWarnOnce (#78 review Mi3) WARNs a config-wound condition once per
// runtime lifecycle then drops to Debug, so a broker re-pushing every ~5s
// cannot turn a structural (non-self-healing) condition into a log flood.
// Caller holds p.mu. Cleared by a successful proxyStartLocked.
func logConfigWarnOnce(a *Agent, p *proxyRuntime, msg string) {
	if p.configWarnAnnounced {
		a.cfg.Logger.Debug(msg + " (suppressed repeat)")
		return
	}
	p.configWarnAnnounced = true
	a.cfg.Logger.Warn(msg)
}

// proxyNow is the #78 backoff clock: Config.Now when injected (tests), else
// the wall clock — same seam the roster expiry checks use.
func proxyNow(a *Agent) time.Time {
	if a.cfg.Now != nil {
		return a.cfg.Now()
	}
	return time.Now()
}

// proxyDialPolicy resolves the #78 backoff schedule: 5s base (one heartbeat —
// the first retry is as fast as today) doubling to a 5min cap, overridable
// via Config for tests / unusual deployments.
func proxyDialPolicy(a *Agent) backoff.Policy {
	pol := backoff.Policy{Base: 5 * time.Second, Cap: 5 * time.Minute}
	if a.cfg.ProxyDialRetryBase > 0 {
		pol.Base = a.cfg.ProxyDialRetryBase
	}
	if a.cfg.ProxyDialRetryCap > 0 {
		pol.Cap = a.cfg.ProxyDialRetryCap
	}
	return pol
}

// proxyDialFailedLocked records one failed proxy dial attempt (#78): arms the
// backoff for the failed identity and applies the Tracker's log discipline —
// WARN on the first failure of a run / a class change, Debug for the
// suppressed repeats (the agent-side half of the flood; the broker-side WARN
// damping is internal/tunnel's).
func proxyDialFailedLocked(a *Agent, p *proxyRuntime, id proxyDialID, class string, err error) {
	if p.dial == nil {
		p.dial = backoff.New(proxyDialPolicy(a))
	}
	// review N2: the clock is read at the FAILURE instant, not at gate entry —
	// a dial can block for its whole timeout (D≈Base in the half-open :7000
	// case), and anchoring the next window at gate entry would shave ~D off
	// the first window (Base·2ⁿ−D). Anchoring at the failure keeps the
	// schedule honest (Base, 2·Base, …).
	now := proxyNow(a)
	p.lastFail = id
	if logNow := p.dial.Fail(now, class); logNow {
		a.cfg.Logger.Warn("agent: proxy "+class, "err", err, "public_port", id.port)
	} else {
		a.cfg.Logger.Debug("agent: proxy "+class+" (suppressed repeat)",
			"err", err, "public_port", id.port, "fails", p.dial.Fails())
	}
}

// resetProxyDialBackoff clears the #78 backoff state (a fresh NATS session /
// authoritative OFF invalidates the failure run — the next directive must
// dial immediately).
func resetProxyDialBackoff(a *Agent) {
	p := a.proxy
	if p == nil {
		return
	}
	p.mu.Lock()
	p.dial = nil
	p.lastFail = proxyDialID{}
	p.mu.Unlock()
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
		// #78: an authoritative OFF ends the failure run — the next enable
		// mints a new epoch (bypass anyway), but stale backoff state must
		// not survive into a semantically new proxy lifecycle.
		p.dial = nil
		p.lastFail = proxyDialID{}
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
	_ = nc.Publish(proto.SubjEvNodeProxyReady(a.cfg.SID, nidOf(a), kind), nil)
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
	// h1 D3: a reconnect ends a deaf window — keepalive silence accumulated
	// across it must not count against any ctl (mirror of session()'s
	// restamp after the .ka subscribe).
	a.restampProcKA()
	ctx := a.loadRunCtx()
	if ctx == nil {
		ctx = context.Background()
	}
	// h1 C4: same started-drain + replay-settlement as session()'s register.
	a.courier.drainStarted(ctx, nc)
	resp, err := a.register(ctx, nc)
	if err != nil {
		a.cfg.Logger.Warn("agent: re-register on reconnect failed", "err", err)
		return
	}
	// A CONTESTED reply is NOT a successful register, and this is the second of
	// the agent's two register sites — the one that runs on every nats.go
	// reconnect. Treating it as success here would be worse than missing the
	// rename: the reply carries no AcceptedProcesses, so courier.onRegisterSuccess
	// would settle every pending proc exit as delivered, and applyReconciliation
	// would act on empty directive arrays. Hand it to the rebuild path instead
	// and return without touching anything.
	if lease := resp.Lease; lease != nil {
		if acceptableLeaseName(a, lease.AssignedNID) {
			requestLeaseRebuild(a, lease.AssignedNID)
			return
		}
		// A REFUSAL MUST STILL RETIRE THIS CONNECTION. external review F3.
		//
		// Logging and returning was the worst of both worlds. nats.go replays
		// every subscription BEFORE it invokes this handler, so by the time the
		// verdict arrives this process is ALREADY subscribed to the incumbent's
		// forwarded subject — and the broker deliberately did not register it,
		// did not reconcile it, and gave it no ownership. It sits there
		// consuming the incumbent's exec/run/expose/upgrade traffic: the double
		// execution this whole increment exists to remove, reached by the one
		// path where the agent was told in so many words that the name is not
		// its own.
		//
		// So tear the session down. requestLeaseRebuild with an EMPTY name is
		// the "drop the connection and re-compete for the configured name"
		// signal — the same machinery as an accepted rename, minus the rename.
		// The Run loop's backoff bounds how fast that re-competition can spin.
		refusals, wait, giveUp := recordLeaseRefusal(a)
		a.cfg.Logger.Warn("agent: refused an unusable lease verdict; retiring this session so the "+
			"replayed subscriptions cannot keep serving a name this process does not hold",
			"sid", a.cfg.SID, "configured_nid", a.cfg.NID, "offered_nid", lease.AssignedNID,
			"consecutive_refusals", refusals, "backoff", wait.String(), "giving_up", giveUp)
		if giveUp {
			// SAY WHAT THE OPERATOR HAS TO DO. Only capacity clears this: another
			// instance exiting, or MaxInstancesPerBasename raised on the broker.
			a.cfg.Logger.Error("agent: giving up on competing for a name — every verdict so far has "+
				"been unusable, which means the suffix space for this credential is full. This "+
				"instance stays running but will NOT appear in `node ls`. Free an instance, or "+
				"raise the broker's max instances per basename, then restart this agent.",
				"sid", a.cfg.SID, "configured_nid", a.cfg.NID, "refusals", refusals)
		}
		// RETIRE FIRST, WAIT AFTERWARDS — including on the give-up path.
		//
		// external review F3, and the ordering matters more than the waiting.
		// nats.go replayed this connection's subscriptions before invoking the
		// handler, so right now this process is subscribed to a forwarded
		// subject the broker has just told it it does not own. Sleeping before
		// the teardown leaves that subscription serving the incumbent's traffic
		// for the whole backoff, and "give up" without a teardown leaves it
		// there FOREVER — the unbounded form of the very defect F3 is about.
		//
		// So the retirement is unconditional and immediate. The backoff is
		// carried on the agent and spent by the Run loop before it dials again
		// (see leaseRefusalWait), which is where "re-compete more slowly"
		// actually belongs.
		requestLeaseRebuild(a, "")
		return
	}
	// A real register means a name was granted: the refusal streak is over.
	resetLeaseRefusals(a)
	a.courier.onRegisterSuccess(resp)
	a.applyReconciliation(ctx, resp)
	// #78: a fresh NATS session invalidates the dial-failure run — the old
	// path may have healed with the network. Reset BEFORE applying the
	// directive: the register reply re-delivers the SAME (gen,epoch,token)
	// when nothing changed broker-side, which the identity gate would
	// otherwise still be suppressing.
	resetProxyDialBackoff(a)
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
