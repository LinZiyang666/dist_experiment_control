package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/backoff"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/tunnel"
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
	// applyDone is guarded by Agent.proxyApplyMu, not mu. A caller reserves
	// exclusive apply ownership, then releases the mutex before doing any IO or
	// joining workers. Waiters wait on this receipt outside every table lock.
	// origin: hot-update S5 plan §7.2
	applyDone          chan struct{}
	revokeWG           sync.WaitGroup
	keysetDigest       [32]byte
	digestApplied      bool
	authorityConflicts uint64
	applyFailures      uint64
	// flcState is guarded by Agent.flcMu. It outlives the timer pointer, which
	// is only a scheduler handle and disappears before cleanup can finish.
	flcState proxyFailClosedState
	planned  proxyPlannedReadiness

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
	//
	// It is ALSO a fence on the proxy-lifecycle `refootprint` hatch, and that use is
	// load-bearing. origin: proxy-lifecycle internal review MAJOR (security lane).
	// refootprint rebuilds a non-serving runtime from its footprint at an equal pair
	// WITHOUT requiring a token — which in the fail-closed state would re-open an exit
	// that F7 says may only come back on a token-bearing authoritative directive. The
	// only other thing standing in the way would be the footprint wipe, and that wipe is
	// `_ = a.stateStore.SetProxy(nil)` with the error discarded: the same
	// disk-write-as-a-fence hole ⚠R2 rejects. fail-closed does not set
	// authoritativelyOff (it is a LOCAL decision, not the operator's), so that latch does
	// not cover this path — this one does.
	needsReestablish bool

	// agentExiting (proxy-lifecycle) latches in stopProxyOnRunExit so a fail-closed timer
	// that fires concurrently with agent shutdown cannot wipe the persisted footprint.
	// time.Timer.Stop() does NOT join an AfterFunc that has already started running, so
	// cancelFailClosed alone is not enough: failClosedFire could take p.mu after the exit
	// teardown and run its clearPersist teardown, leaving the node with no footprint to
	// bootstrap from on the next start.
	agentExiting bool

	// authoritativelyOff (proxy-lifecycle ⚠R2) latches when the BROKER told us to stop
	// (the !d.Enabled arm), as opposed to a local fail-closed teardown. While set, the
	// refootprint escape hatch below refuses to rebuild at an equal pair, so an exit the
	// operator switched off can never be resurrected by a tokenless keyset push.
	//
	// It is IN MEMORY on purpose. The obvious fence — "an authoritative OFF wipes the
	// persisted footprint, so there is nothing to bootstrap from" — rests on
	// `_ = a.stateStore.SetProxy(nil)`, whose error is discarded, and on
	// loadProxyStateSafe, which likewise swallows read errors. One failed disk write
	// would turn that fence into a hole. A process restart needs no latch: there is no
	// live SS server to resurrect, and the broker re-states the authoritative pair.
	authoritativelyOff bool

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

	// corpseWarnAnnounced (proxy-lifecycle) is the reap path's OWN once-then-Debug latch.
	// It is deliberately NOT configWarnAnnounced: sharing that one-shot would let the
	// "found stopped, rebuilding" line consume it and demote the "no persisted footprint"
	// warning that follows when the rebuild cannot proceed — hiding the failure behind the
	// announcement of the attempt. Cleared on a successful start, like its sibling.
	corpseWarnAnnounced bool

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
	return ssproxy.CompareAuthority(dGen, dEpoch, 0, appliedGen, appliedEpoch, 0) > 0
}

// ensureProxyRuntime returns the lifetime-owned proxy runtime. It is created
// eagerly in New (F2) so concurrent forwarded directives never race to create
// competing runtimes — this is a plain read of an immutable pointer.
func (a *Agent) ensureProxyRuntime() *proxyRuntime {
	return a.proxy
}

// proxyGenEpoch returns the agent's last-applied (generation, epoch) ordering
// pair, or (0,0) when there is no server at all. Reported in the heartbeat so the
// broker can detect a generation mismatch even at an equal epoch (round-4 F2).
//
// This deliberately tests `p.srv == nil` and NOT servingLocked(), unlike proxyBound
// right below it. The two answer different questions: this one is ORDERING ("which
// directive pair did I last apply"), proxyBound is LIVENESS ("am I serving"). Making
// a stopped-but-present server report (0,0) here would be actively harmful: the
// broker only pushes an authoritative OFF to a node whose reported epoch is > 0
// (repairProxy, internal/broker/proxy.go), so a corpse reporting (0,0) would never
// receive the OFF — and the corpse reap runs on the directive path, so a node that
// receives no directives never gets reaped either. That is a deadlock, and reporting
// the true pair alongside ProxyBound=false is both honest and self-healing: the
// broker sees "applied epoch N, not serving" and has every repair path available.
//
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md ⚠R1).
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

// servingLocked reports whether this runtime has an SS server that can ACTUALLY serve.
// Caller holds p.mu.
//
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md). The
// runtime used a bare `p.srv != nil` as its serving predicate, and a STOPPED server
// satisfies it — which is how the node advertised a READY exit with no listener behind it
// for 7h40m. A pointer answers "do I have one", never "does it work". Every site that asks
// the SERVING question must use this; sites that ask the ORDERING question (which directive
// pair did I last apply) must not — see proxyGenEpoch.
func (p *proxyRuntime) servingLocked() bool {
	return p.srv != nil && p.srv.Serving()
}

// proxyBound (C5) reports whether the embedded SS proxy is currently serving — the heartbeat's
// ProxyBound flag, from which each cluster broker writes its own liveness proxy_ready (the cross-broker
// convergence signal). False when proxy is off / not yet bound.
//
// SCOPE: this flag is read ONLY in cluster mode (internal/broker/broker.go gates the ProxyBound
// branch on b.clusterMode and returns; single mode falls through to repairProxy, which never
// looks at it). Making it honest therefore fixes the READY lie on CLUSTER brokers. On a SINGLE
// broker, proxy_ready is driven entirely by the agent's own pubProxyReady calls.
//
// CORRECTION — origin: proxy-lifecycle EXTERNAL review F4. An earlier version of this comment
// claimed single mode had its own exit edge, reasoning that "the broker still believes
// ready=true, so repairProxy's !ready suppression does not fire, so the keyset push arrives".
// That is WRONG, and it was my error, not the reviewers': repairProxy never reaches that
// suppression guard, because the CONVERGENCE-FIRST arm above it returns as soon as
// `on && ready && agentGen == brokerGen && agentEpoch == epoch` — which is exactly a corpse's
// report (proxy_ready still true in the DB, and proxyGenEpoch reports the real pair while
// p.srv is non-nil). Three internal-review lanes found this and I dismissed all three on the
// strength of the misreading.
//
// What actually closes it now is reapProxyCorpseOnHeartbeat: the reap runs BEFORE the report,
// so a corpse is nil'd and this heartbeat carries ProxyBound=false AND (0,0) — a combination
// neither early return matches, so the repair push goes out. The edge exists in both modes; in
// single mode it is the heartbeat that supplies it, not the directive path.
func (a *Agent) proxyBound() bool {
	p := a.proxy
	if p == nil {
		return false
	}
	p.mu.Lock()
	srvUp := p.servingLocked()
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

// applyProxyDirective converges the local SS proxy to the directive. nc is used
// to ACK readiness. A nil directive means "ensure proxy off".
//
// It deliberately takes NO context. origin: 2026-08-21 weilandserver incident
// (docs/reviews/proxy-lifecycle-plan.md). The parameter used to carry the session's
// runCtx down into ssproxy.Server.Start, which made a control-plane session rebuild
// silently kill the data plane; the SS server's lifetime is now owned exclusively by
// Stop(). Do not reintroduce a ctx parameter here to "anchor" the server -- that is
// the bug, not a convenience.
func (a *Agent) applyProxyDirective(nc *nats.Conn, d *proto.ProxyDirective) {
	serializedProxyApply(a, nc, d, 0)
}

// serializedProxyApply is shared by register, reconnect and forwarded pushes.
// Reservation, not a held mutex, serializes the whole decision/IO/commit cycle.
// This keeps joins outside proxyApplyMu while preserving the apply order.
// origin: hot-update S5 plan §6.4 / §7.2
func serializedProxyApply(a *Agent, nc *nats.Conn, d *proto.ProxyDirective, seq int64) {
	// origin: hot-update S5 plan §5.2 / T36. Creating authority or a listener
	// must reserve SS admission before any apply side effect. OFF remains an
	// irreversible revocation even while a pending reader blocks creation.
	if d != nil && d.Enabled {
		release, admitted := a.gate.admit(gateSS)
		if !admitted {
			return
		}
		defer release()
	}
	release := beginProxyApply(a)
	defer release()
	if seq < 0 {
		return
	}
	if seq != 0 {
		if seq <= a.lastAppliedPushSeq {
			return
		}
		// Direct handler callers also maintain the paired sequence invariant.
		a.proxyDrainMu.Lock()
		if seq > a.proxyDispatchN {
			a.proxyDispatchN = seq
		}
		a.proxyDrainMu.Unlock()
		a.lastAppliedPushSeq = seq
	}
	applyProxyDirectiveOwned(a, nc, d)
}

func beginProxyApply(a *Agent) func() {
	release, _ := beginProxyApplyUntil(a, time.Time{})
	return release
}

// Cut callers spend the same entry deadline while waiting for an admitted
// apply. A timeout cannot create a second owner or interrupt the first one's IO.
// origin: hot-update S5 plan §7.2
func beginProxyApplyUntil(a *Agent, deadline time.Time) (func(), error) {
	if a.proxy == nil {
		return func() {}, nil
	}
	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		expired = timer.C
	}
	for {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		a.proxyApplyMu.Lock()
		done := a.proxy.applyDone
		if done == nil {
			done = make(chan struct{})
			a.proxy.applyDone = done
			a.proxyApplyMu.Unlock()
			return func() {
				a.proxyApplyMu.Lock()
				a.proxy.applyDone = nil
				close(done)
				a.proxyApplyMu.Unlock()
			}, nil
		}
		a.proxyApplyMu.Unlock()
		select {
		case <-done:
		case <-expired:
			return nil, context.DeadlineExceeded
		}
	}
}

func applyProxyDirectiveOwned(a *Agent, nc *nats.Conn, d *proto.ProxyDirective) {
	a.flcMu.Lock()
	pending := a.proxy.flcState.CleanupPending
	a.flcMu.Unlock()
	if pending {
		return
	} // fired work must retire before any authoritative rebuild
	if leasedInstanceRefusesProxy(a, d) {
		return
	}
	p := a.ensureProxyRuntime()
	p.mu.Lock()
	defer p.mu.Unlock()

	if proxyRefusedDuringShutdown(a, p) {
		return
	}

	// F4: a nil directive is the proxy-OFF *register* representation (kept nil
	// for byte-identity). It carries no epoch and so cannot be ordered against
	// live enable/disable pushes — treat it as "no opinion" and NEVER tear a
	// running proxy down on it. Authoritative OFF is delivered by the disable
	// push and the heartbeat OFF repair (broker repairProxyEpoch). At fresh
	// boot the agent isn't serving, so a nil is a clean no-op.
	if d == nil {
		return
	}

	// The digest is canonical over the complete keyset, including key material.
	// An equal ordering pair with different authority must never take the ready
	// re-ACK path or rotate the home first. No secret enters diagnostics.
	// origin: hot-update S5 plan §6.4 / TestProxyPairDigestIsNotSuppressed
	// A delayed push cannot act on home/readiness or unversioned key state
	// after a fresher RESUME. Push wire has only the watermark's pair.
	// origin: hot-update S5 internal review rv-authority-4
	if d.Generation < 0 || d.Epoch < 0 || ssproxy.CompareAuthority(d.Generation, d.Epoch, 0, p.gen, p.epoch, 0) < 0 {
		return
	}
	digest, err := proxyDirectiveDigest(d)
	if err != nil {
		a.cfg.Logger.Warn("agent: invalid proxy authority snapshot")
		return
	}
	// Only validated changes to installed authority preempt a route hold;
	// periodic equal keyset nudges are not evidence of a revoke.
	// origin: hot-update S6 internal review rv-cluster-5
	if p.digestApplied && (d.Generation != p.gen || d.Epoch != p.epoch || digest != p.keysetDigest) ||
		!d.Enabled && !p.authoritativelyOff || d.Enabled && d.Home != nil && p.publicPort > 0 && d.Home.PublicPort == p.publicPort && d.Home.Epoch >= p.homeEpoch && (d.Home.BrokerAddr != p.homeAddr || d.Home.Epoch > p.homeEpoch) {
		invalidateAgentMaintenance(a)
	}
	if p.digestApplied && d.Generation == p.gen && d.Epoch == p.epoch && digest != p.keysetDigest {
		if p.authorityConflicts < math.MaxUint64 {
			p.authorityConflicts++
		}
		a.cfg.Logger.Warn("agent: proxy authority conflict", "count", p.authorityConflicts)
		a.proxyTeardownLocked(p, nc, true)
		p.needsReestablish = true
		return
	}
	defer func() {
		if p.gen == d.Generation && p.epoch == d.Epoch &&
			((d.Enabled && p.servingLocked()) || (!d.Enabled && p.authoritativelyOff)) {
			p.keysetDigest, p.digestApplied = digest, true
		}
	}()

	// #78 opt-out gate (the N-1 belt): a #78-aware broker never pushes to an
	// opted-out node (register folds ProxyOptOut into nodes.proxy_capable),
	// so this arm exists for OLD brokers that keep pushing.
	if a.cfg.ProxyOptOut {
		refuseProxyDirectiveOptedOut(a, nc, p, d)
		return
	}

	// Reap a corpse BEFORE anything below reads p.srv, restoring the invariant the rest
	// of this function was written against: `p.srv != nil` implies it can actually serve.
	// Extracted (not inlined) to keep this dispatcher off the god-function detector, the
	// same reason refuseProxyDirectiveOptedOut lives outside.
	reapProxyCorpseLocked(a, p, nc)

	applyProxyDirectiveStateLocked(a, p, nc, d, rehomeProxyDirectiveLocked(a, p, nc, d))
}

// The state transition follows corpse reaping and home repair under the same
// serialized directive owner. Caller holds p.mu.
// origin: hot-update S6 plan §7.3 / T44
func applyProxyDirectiveStateLocked(a *Agent, p *proxyRuntime, nc *nats.Conn, d *proto.ProxyDirective, rehomeFailed bool) {
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

	// proxy-lifecycle: the same escape hatch for a runtime that stopped serving WITHOUT a
	// fail-closed (the head reap above just cleared a corpse, or a start failed). The broker's
	// steady-state push is keyset-only at the UNCHANGED pair, so without this the drop guard
	// below would reject the very directive that could rebuild us and the node would stay dark
	// until an operator ran a session-wide `proxy off/on`. That is exactly the 7h40m outage.
	//
	// Why this cannot resurrect a switched-off exit: it accepts ONLY the exact last-applied
	// pair (never a lower one), requires d.Enabled, and is refused outright once
	// authoritativelyOff is latched by an authoritative OFF. Unlike `reestablish` it does not
	// require a token, because the whole point is that the broker is NOT minting one.
	// Both latch terms are load-bearing fences, documented on their field declarations.
	refootprint := d.Enabled && !p.authoritativelyOff && !p.needsReestablish && !p.servingLocked() &&
		d.Generation == p.gen && d.Epoch == p.epoch

	// Round-3 ordering: drop a directive that is NOT strictly newer than the
	// last SUCCESSFULLY-applied (gen, epoch). This single check covers ALL
	// sources — register reply, live push, heartbeat repair — so a stale
	// register reply can't resurrect after a newer OFF, while a higher-
	// generation restore (lower epoch) still converges. NOTE: the pair is
	// recorded only on SUCCESS below, so a transient start failure does not
	// advance it (the retry directive is still "newer" → F3 retry works).
	if !reestablish && !portRebuild && !refootprint && !proxyNewer(d.Generation, d.Epoch, p.gen, p.epoch) {
		a.cfg.Logger.Debug("agent: dropping stale proxy directive",
			"dir_gen", d.Generation, "dir_epoch", d.Epoch, "applied_gen", p.gen, "applied_epoch", p.epoch)
		return
	}

	if !d.Enabled {
		a.proxyTeardownLocked(p, nc, true /* clearPersist + ack unready */)
		p.gen, p.epoch = d.Generation, d.Epoch
		p.needsReestablish = false
		// proxy-lifecycle ⚠R2: latch the AUTHORITATIVE off. This is the arm that must
		// fence the refootprint rebuild; a fail-closed teardown (failClosedFire) uses
		// the same clearPersist=true teardown but is a LOCAL decision and deliberately
		// does NOT latch here — it sets needsReestablish so the broker can rebuild it.
		p.authoritativelyOff = true
		return
	}

	keys := toSSKeys(d.Keys)

	switch {
	case d.Token != "":
		// Full (re)establish: a fresh tunnel token was minted. Tear down any
		// existing server/tunnel (without an unready flap) and rebuild.
		a.proxyTeardownLocked(p, nc, false)
		a.proxyStartLocked(nc, p, d.PublicPort, 0, d.Token, d.Generation, d.Epoch, keys, d.Home)

	case !p.servingLocked():
		// Keyset-only push but no LIVE server (fresh boot reconnect-keep, a sub change
		// before the full directive, a transient-start retry, or a corpse the head reap
		// above just cleared). Bootstrap from the persisted footprint.
		bootstrapProxyFromFootprintLocked(a, p, nc, d, keys)

	default:
		applyProxyKeysetLocked(a, p, nc, d, keys)
	}
}

// Caller holds p.mu; network work temporarily releases it and preserves the
// serialized directive owner.
// origin: hot-update S5 plan §7.2
func rehomeProxyDirectiveLocked(a *Agent, p *proxyRuntime, nc *nats.Conn, d *proto.ProxyDirective) bool {
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
	failed := false
	if d.Enabled && d.Home != nil && p.srv != nil && d.Home.PublicPort == p.publicPort && d.Home.Epoch > p.homeEpoch {
		if ha, ok := a.cfg.ExposeAdapter.(homeApplier); ok {
			port := p.publicPort
			p.mu.Unlock()
			err := ha.ApplyHome(port, d.Home.BrokerAddr, d.Home.Epoch, d.Home.CertPins)
			// M4: ApplyHome returns nil (success no-op) when the tunnel SESSION is absent — that is NOT
			// a real rehome (no tunnel was opened). Only advance the home epoch + persist when the
			// session actually exists; otherwise leave homeEpoch so the reaper's next push retries, and
			// publish UNREADY (N2) so /sub stops vending a home with no live tunnel until it converges.
			sessionPresent := err == nil
			if hc, ok2 := a.cfg.ExposeAdapter.(homeSessionChecker); ok2 && !hc.HasSession(port) {
				sessionPresent = false
			}
			p.mu.Lock()
			if err != nil {
				a.cfg.Logger.Warn("agent: proxy rehome (ApplyHome)", "err", err)
				a.pubProxyReady(nc, false)
				failed = true
			} else if !sessionPresent {
				a.cfg.Logger.Warn("agent: proxy rehome no-op (tunnel session absent); not advancing home epoch")
				a.pubProxyReady(nc, false)
				failed = true
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

	return failed
}

// reapProxyCorpseLocked (proxy-lifecycle) clears a runtime whose SS server exists but
// can no longer serve, so the caller's next arm rebuilds it. Caller holds p.mu.
//
// origin: 2026-08-21 weilandserver incident. A session rebuild cancelled the ctx the SS
// server was (wrongly) anchored to; the server latched closed while p.srv kept pointing
// at it. Every subsequent keyset push took the "running server" arm, failed SetKeys, and
// returned — 5416 times over 7.5 hours, with the broker advertising the node READY the
// whole time. Everything needed to self-heal was already present (the persisted footprint
// was intact, and the srv==nil arm knows how to bootstrap from it); the ONLY missing piece
// was setting the dead pointer back to nil.
//
// This function is unconditional-on-CAUSE, but on its own it is not unconditional-on-EDGE: it
// only runs when something calls it. Called from applyProxyDirective it covers every cause that
// produces a directive; called from reapProxyCorpseOnHeartbeat it covers the rest (an accept
// loop that died by itself produces no directive, and on a single broker none would ever come —
// external review F4). An earlier version of this comment claimed cause-independence outright;
// that over-claimed, because the edge was missing.
//
// clearPersist=false: the footprint is what the rebuild reads. Wiping it here would turn a
// recoverable corpse into a permanently dark node, which is the failure this closes.
func reapProxyCorpseLocked(a *Agent, p *proxyRuntime, nc *nats.Conn) {
	if p.srv == nil || p.srv.Serving() {
		return
	}
	// once-then-Debug, on its OWN latch. The broker re-pushes every ~5s, so a per-push WARN
	// here would recreate the 5416-line flood this increment exists to end (#78 / #75
	// disk-fill) — but sharing configWarnAnnounced with logConfigWarnOnce would let THIS
	// message consume the one-shot and silently demote the "no persisted footprint" warning
	// that fires immediately afterwards when the rebuild cannot proceed. The operator would
	// then see "found stopped, rebuilding" and never the line saying it did not.
	// origin: proxy-lifecycle internal review MAJOR (discipline lane).
	if !p.corpseWarnAnnounced {
		p.corpseWarnAnnounced = true
		a.cfg.Logger.Warn("agent: embedded proxy server found stopped; rebuilding from persisted footprint")
	} else {
		a.cfg.Logger.Debug("agent: embedded proxy server found stopped; rebuilding from persisted footprint")
	}
	a.proxyTeardownLocked(p, nc, false)
	a.pubProxyReady(nc, false)
}

// bootstrapProxyFromFootprintLocked (proxy-lifecycle) is the keyset-only arm: rebuild the SS
// server + tunnel from the persisted footprint. Extracted from applyProxyDirective's switch so
// the dispatcher stays under the god-function detector (same reason as
// refuseProxyDirectiveOptedOut). Caller holds p.mu.
func bootstrapProxyFromFootprintLocked(a *Agent, p *proxyRuntime, nc *nats.Conn, d *proto.ProxyDirective, keys []ssproxy.Key) {
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
	a.proxyStartLocked(nc, p, ps.PublicPort, ps.LocalPort, ps.Token, d.Generation, d.Epoch, keys, home)
}

// applyProxyKeysetLocked (proxy-lifecycle) is the running-server arm: swap the key set and
// advance the applied pair. Caller holds p.mu.
//
// A SetKeys failure is TERMINAL, not transient: ssproxy.Server is single-use, so ErrStopped
// means "this object is spent" and no amount of re-pushing will make it take keys. The
// pre-fix code logged a WARN and returned, which is what let a dead server sit in the runtime
// forever. Reap and rebuild in the SAME call so the unready window is one push wide — long
// enough to be honest, short enough not to accumulate the three consecutive unready
// observations that trigger the cluster reaper's port-rotating rehome dwell.
func applyProxyKeysetLocked(a *Agent, p *proxyRuntime, nc *nats.Conn, d *proto.ProxyDirective, keys []ssproxy.Key) {
	// Revoke may join raw workers and deliver a flow callback. Never hold the
	// proxy table lock across that operation.
	// origin: hot-update S5 plan §7.2
	srv := p.srv
	p.mu.Unlock()
	err := srv.Authorize(ssproxy.Grant{Enabled: d.Enabled, Generation: d.Generation, Epoch: d.Epoch, Keys: keys})
	p.mu.Lock()
	if err != nil {
		if !errors.Is(err, ssproxy.ErrStopped) {
			// S5 capacity failure may already have revoked removed IDs. Retain the
			// serving survivor owner, but never commit this pair/digest or publish a
			// success ACK. A later complete snapshot may retry the failed admission.
			// origin: hot-update S5 plan §6.4
			if p.applyFailures < math.MaxUint64 {
				p.applyFailures++
			}
			a.cfg.Logger.Warn("agent: proxy keyset apply failed", "count", p.applyFailures)
			return
		}
		reapProxyCorpseLocked(a, p, nc)
		bootstrapProxyFromFootprintLocked(a, p, nc, d, keys)
		return
	}
	p.gen, p.epoch = d.Generation, d.Epoch
	a.persistProxyEpochLocked(p)
	// M3: re-ACK readiness — a node whose first ready publish was lost (or
	// whose proxy_ready was cleared by an OFFLINE flap) is otherwise stuck
	// out of /sub forever despite serving correctly.
	a.pubProxyReady(nc, true)
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

func (a *Agent) proxyStartLocked(nc *nats.Conn, p *proxyRuntime,
	publicPort, wantLocal int, token string, gen, epoch int64, keys []ssproxy.Key, home *proto.HomeDirective) {

	// origin: hot-update S3 plan §4.0. Broker re-pushes a skipped directive.
	release, admitted := a.gate.admit(gateExpose)
	if !admitted {
		return
	}
	defer release()

	homeAddr, homeEpoch := "", int64(0)
	var homePins proto.CertPins
	if home != nil {
		homeAddr, homeEpoch, homePins = home.BrokerAddr, home.Epoch, home.CertPins
	}

	// #78 backoff gate — BEFORE any side effect. Holds back ONLY an exact
	// retry of the identity that just failed; anything the broker minted
	// fresh (proxy off/on bumps epoch, a reaper rotation changes port+token,
	// a rehome bumps homeEpoch) bypasses immediately, so no operator action
	// ever waits out a backoff window. While held: no SS server and no dial. The
	// node is ACKed UNREADY (proxy-lifecycle S7) rather than left silent — the
	// previous wording promised "no ACK … the node simply stays unready", but
	// nothing published that, so a stale ready could linger in the broker for the
	// whole backoff window.
	id := proxyDialID{gen: gen, epoch: epoch, port: publicPort, token: token, homeEpoch: homeEpoch}
	now := proxyNow(a)
	if p.dial != nil && id == p.lastFail && !p.dial.Due(now) {
		a.cfg.Logger.Debug("agent: proxy dial backoff active; retry deferred",
			"public_port", publicPort, "fails", p.dial.Fails())
		// proxy-lifecycle: the comment above promises "the node simply stays unready",
		// but nothing published that. Staying silent lets a stale ready linger in the
		// broker for the whole backoff window — the same lie this increment closes
		// elsewhere. pubProxyReady is cheap and idempotent.
		a.pubProxyReady(nc, false)
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
	// Install the stable flow revoke callback before accepting any SS client.
	// Unknown binding must fail closed rather than silently leave public owners.
	// origin: hot-update S5 plan §6.5
	if err := wireProxyFlowRevoke(a, srv); err != nil {
		a.cfg.Logger.Warn("agent: proxy flow revoke binding unavailable")
		a.proxyFailCleanupLocked(p, nc)
		return
	}
	// round-6 F12: default to internet-egress only — a subscription must NOT
	// reach this agent's loopback, LAN/VPC, link-local, or cloud-metadata
	// endpoints. Operators who intend private-network access opt in via
	// Config.ProxyAllowPrivateDestinations.
	if !a.cfg.ProxyAllowPrivateDestinations {
		srv.DenyPrivateDestinations()
	}
	p.mu.Unlock()
	lp, err := srv.Start(wantLocal, keys)
	p.mu.Lock()
	if err != nil && wantLocal != 0 && errors.Is(err, syscall.EADDRINUSE) {
		// The persisted local port is taken — retry ONCE on an OS-chosen port.
		//
		// origin: proxy-lifecycle external review F3. The footprint replays the local port the
		// SS listener happened to get last time, but that port is an implementation detail
		// between the listener and the tunnel adapter: the PUBLIC port, the token and the home
		// are all unchanged by picking a different one. Before this, a corpse whose old local
		// port had been taken by anything else (including a predecessor that outlived the reap)
		// failed to rebuild, went dark, and stayed dark — the retry then hit the #78 dial
		// backoff and slowed down, so the only way out was the session-wide `proxy off/on`
		// this whole increment exists to avoid.
		//
		// Deliberately NARROW: only when a specific port was requested, and only for
		// EADDRINUSE. Any other listen error (EACCES, EMFILE, …) still fails closed — those
		// are not fixed by choosing a different port, and papering over them would hide a real
		// fault. Retrying on the SAME Server is safe: a failed Start leaves ln nil and closed
		// false, so nothing was consumed by the attempt.
		a.cfg.Logger.Warn("agent: persisted proxy local port in use; retrying on an OS-chosen port",
			"wanted_local_port", wantLocal, "public_port", publicPort)
		p.mu.Unlock()
		lp, err = srv.Start(0, keys)
		p.mu.Lock()
	}
	if err != nil {
		proxyDialFailedLocked(a, p, id, "ssproxy_start", err)
		a.proxyFailCleanupLocked(p, nc)
		return
	}
	// The listener and subsequent pushes share the same authority watermark.
	// origin: hot-update S5 internal review rv-authority-4
	p.mu.Unlock()
	err = srv.Authorize(ssproxy.Grant{Enabled: true, Generation: gen, Epoch: epoch, Keys: keys})
	p.mu.Lock()
	if err != nil {
		p.mu.Unlock()
		srv.Stop()
		p.mu.Lock()
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
	// The local-connect reservation can run inside AddProxy's initial RESUME.
	// Publish the started SS owner and its local target together so a flow
	// arriving during initial RESUME binds to this listener. Waiting for
	// AddProxy to return would leave the local-connect hook at port zero.
	// Apply ownership still prevents a cut or a competing rebuild.
	// origin: hot-update S5 plan §6.5
	p.srv, p.localPort = srv, lp
	a.proxyPublicPort.Store(int64(publicPort))
	// The proxy tunnel's open is bounded by the tunnel client's own lifetime and nothing
	// else, exactly as before AddProxy took a ctx: this function takes no ctx by design
	// (see applyProxyDirective; dataplane_lifetime gate), nobody upstream is holding a
	// request open for the dial, and a session-scoped bound here would put the
	// 2026-08-21 coupling back one layer down.
	openCtx := context.Background() // ctx-none: proxyStartLocked must not carry a ctx (gotcha #80); no deadline by design.
	p.mu.Unlock()
	err = a.cfg.ExposeAdapter.AddProxy(openCtx, PortToken{
		Name: proxyTokenName, Port: publicPort, LocalPort: lp, Token: token,
		HomeBrokerAddr: homeAddr, Epoch: homeEpoch, CertPins: homePins,
	})
	p.mu.Lock()
	if err != nil {
		proxyDialFailedLocked(a, p, id, "tunnel_dial", err)
		p.mu.Unlock()
		srv.Stop()
		p.mu.Lock()
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
	p.corpseWarnAnnounced = false // and the corpse-reap WARN, so a LATER corpse is announced again
	p.srv = srv
	p.publicPort = publicPort
	p.localPort = lp
	p.token = token
	p.gen = gen
	p.epoch = epoch
	p.homeAddr = homeAddr
	p.homeEpoch = homeEpoch
	p.needsReestablish = false   // rebuilt — clear the fail-closed resync flag (F7)
	p.authoritativelyOff = false // serving again under an authoritative ENABLE (proxy-lifecycle ⚠R2)
	a.proxyTunnelUp.Store(true)  // M2: tunnel just opened — ProxyBound now reflects a live tunnel
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
	release := beginProxyApply(a)
	defer release()
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
	clearPlannedProxyReadiness(a)
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
	clearPlannedProxyReadiness(a)
	a.proxyTunnelUp.Store(false)
	// Detach first: callbacks and readers see no owner while cleanup joins.
	// The apply reservation prevents a competing start until these owners retire.
	// origin: hot-update S5 plan §7.2
	srv, port := p.srv, p.publicPort
	p.srv = nil
	p.publicPort, p.localPort, p.token = 0, 0, ""
	if srv != nil {
		p.mu.Unlock()
		// Revoke invalidates the original OFDs and every prepared dup; Stop alone
		// only closes wrappers and is insufficient after a sealed cut.
		if err := srv.RevokeAll("proxy_stopped"); err != nil {
			a.cfg.Logger.Warn("agent: proxy hard revoke unavailable")
		}
		srv.Stop()
		// Stop joins SS callback producers before this Wait: no Add can race a
		// zero-count Wait, and the next server cannot start until apply retires.
		p.revokeWG.Wait()
		if a.cfg.ExposeAdapter != nil {
			_ = a.cfg.ExposeAdapter.RemoveProxy(proxyTokenName, port)
		}
		p.mu.Lock()
	}
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
	// No ctx is fetched here any more: applyProxyDirective takes none, because the SS server's
	// lifetime must not be bound to a session-scoped context (proxy-lifecycle; the 2026-08-21
	// weilandserver incident). This function previously loaded runCtx purely to pass it down.
	if seq <= 0 {
		return
	}
	serializedProxyApply(a, nc, &d, seq)
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
	// B1: a reconnect cancels the fail-closed countdown — in the ReconnectHandler callback
	// (disarmPartitionTimers), NOT here. This goroutine runs after the callback queue has moved
	// on and is skipped entirely for a reconnect the single-flight drops; cancelling here erased
	// later disconnects' countdowns and missed dropped reconnects (h100 stale timer). With the
	// cancel gone the entry check and the Stage-C re-check below were back to back, so they are
	// one check now; it still sits immediately before publishing nc.
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
		ctx = context.Background() // ctx-none: nats reconnect callback has no caller context.
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
	// register settled the courier entries represented by its own request.
	a.applyReconciliation(ctx, resp)
	// #78: a fresh NATS session invalidates the dial-failure run — the old
	// path may have healed with the network. Reset BEFORE applying the
	// directive: the register reply re-delivers the SAME (gen,epoch,token)
	// when nothing changed broker-side, which the identity gate would
	// otherwise still be suppressing.
	resetProxyDialBackoff(a)
	a.applyProxyDirective(nc, resp.Proxy)
	// M3: re-ACK readiness if we are serving (covers a lost first ACK or a
	// proxy_ready cleared while we were partitioned).
	//
	// This MUST use servingLocked, not `p.srv != nil`. origin: proxy-lifecycle internal
	// review, MAJOR raised independently by four lanes — the first cut of this increment
	// converted proxyBound and left this site on the pointer check, which is the SAME
	// "READY lie" the whole increment exists to end, just on the reconnect path. It is
	// reachable: applyProxyDirective returns EARLY without reaping when the directive is
	// nil (the proxy-OFF register representation), so a corpse survives to this line and
	// would be re-advertised as a live exit.
	if p := a.proxy; p != nil {
		p.mu.Lock()
		serving := p.servingLocked()
		p.mu.Unlock()
		if serving {
			a.pubProxyReady(nc, true)
		}
	}
	// The same counts the first register prints (DOC-25): the reconnect path is where a broker that was
	// rolled back / restored answers with drop directives for processes it no longer knows, and a line
	// without the counts cannot say whether the broker asked for a kill that never happened or never asked
	// (drill 94 arm B has been unattributable on exactly this since 2026-09-03; simcluster-speed S2 triage).
	a.cfg.Logger.Info("agent: re-registered after reconnect",
		"reconciled", len(resp.ReconciledProcesses),
		"drop_procs", len(resp.DropProcesses),
		"revoke_ports", len(resp.RevokePorts))
}

// armFailClosed (B1) starts the fail-closed countdown on NATS disconnect: if
// the agent stays partitioned for ProxyFailClosedGrace without reconnecting,
// it proactively tears the SS server down so a revoked subscriber cannot keep
// egressing past the broker's OFFLINE→port-REVOKE window. Idempotent.
func (a *Agent) armFailClosed() {
	// Don't arm during graceful shutdown: nc.Drain on Run exit fires a
	// DisconnectErr, and a timer started here would outlive the agent.
	if rc := a.loadRunCtx(); rc != nil && rc.Err() != nil {
		return
	}
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	st := &a.proxy.flcState
	if a.flcTimer != nil || st.CleanupPending {
		return
	}
	grace := a.cfg.ProxyFailClosedGrace
	if grace <= 0 {
		grace = 15 * time.Minute
	}
	now, err := procrequest.BootNow()
	// Darwin keeps the legacy timer; it cannot export a Linux BOOTTIME image.
	if err == nil && now > 0 && now <= math.MaxInt64-int64(grace) {
		st.DeadlineBootNS = now + int64(grace)
	} else {
		st.DeadlineBootNS = 0
	}
	st.Fired, st.CleanupPending = false, false
	st.serial++
	serial := st.serial
	a.flcTimer = time.AfterFunc(grace, func() {
		if latchProxyFailClosed(a, serial) {
			// The timer callback submits cleanup to a separate owner, so teardown
			// cannot wait on its own callback or carry the timer lock into Stop.
			go finishProxyFailClosed(a, serial)
		}
	})
}

func (a *Agent) cancelFailClosed() {
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	if a.flcTimer != nil {
		a.flcTimer.Stop()
		a.flcTimer = nil
	}
	st := &a.proxy.flcState
	if !st.Fired {
		st.serial++ // invalidates a queued callback that lost the cancellation race
		st.DeadlineBootNS = 0
	}
	// Fired cleanup is irrevocable, even when reconnect beats the cleanup worker.
}

// disarmPartitionTimers is the ReconnectHandler's mirror of the disconnect
// handler's armFailClosed + armRedialWatchdog. It must run synchronously in
// that callback, on nats.go's serialized callback queue, before the reconnect
// single-flight.
//
// B1: a reconnect cancels the fail-closed countdown. That cancel used to live
// at the entry of onNATSReconnect, which broke both ways:
//   - a reconnect DROPPED by the single-flight never cancelled. A disconnect
//     while the in-flight pass was retrying its register armed a countdown that
//     nothing ever cancelled, so a healthy agent tore its SS exit down
//     ProxyFailClosedGrace later while the broker still reported READY (h100,
//     2026-10-03: 9h55m egress outage);
//   - the pass goroutine's cancel ran AFTER the callback queue had moved on, so
//     a disconnect landing between the reconnect callback and that goroutine
//     had its countdown erased — fail-open for a partition that then persisted.
//
// Callback order makes "armed" mean "the latest connection event was a
// disconnect", exactly as it already did for the redial watchdog.
// origin: proxy fail-closed stale-timer defect (h100); hot-update S6 proxyfix
func disarmPartitionTimers(a *Agent) {
	a.stopRedialWatchdog() // a successful reconnect means we are NOT stuck
	a.cancelFailClosed()
}

// origin: hot-update S5 plan §6.5 / §7.2 / TestProxyFiredCleanupSurvivesCut
func latchProxyFailClosed(a *Agent, serial uint64) bool {
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	st := &a.proxy.flcState
	if st.serial != serial || st.Fired {
		return false
	}
	st.Fired, st.CleanupPending = true, true
	if a.flcTimer != nil {
		a.flcTimer.Stop()
	}
	a.flcTimer = nil
	return true
}

func (a *Agent) failClosedFire() {
	a.flcMu.Lock()
	serial := a.proxy.flcState.serial
	a.flcMu.Unlock()
	if latchProxyFailClosed(a, serial) {
		finishProxyFailClosed(a, serial)
	}
}

// Timer.Stop does not join an already running AfterFunc. Such a callback can
// reach cleanup during Run's exit and wipe the footprint the next boot needs.
// The agentExiting check and teardown must therefore be one reserved apply
// operation. A previous split check/log/teardown let the entire exit fit in the
// gap: the callback read false, exit kept the footprint, then the callback wiped
// it. The reservation now spans lock-free joins as well as the latch check.
// origin: proxy-lifecycle internal review BLOCKER; hot-update S5 plan §7.2
func finishProxyFailClosed(a *Agent, serial uint64) {
	release := beginProxyApply(a)
	defer release()
	p := a.proxy
	p.mu.Lock()
	if !p.agentExiting {
		a.cfg.Logger.Warn("agent: fail-closed — NATS partitioned past grace, stopping embedded proxy")
		a.proxyTeardownLocked(p, nil, true)
		// round-6 F7: local cleanup is not authoritative broker OFF. Keep the
		// successfully applied pair so a stale lower enable cannot resurrect it;
		// reconnect re-sends the same pair and must carry a full token-bearing
		// directive to reestablish. A nil register directive remains no opinion.
		p.needsReestablish = true
	}
	p.mu.Unlock()
	a.flcMu.Lock()
	if p.flcState.serial == serial {
		p.flcState.CleanupPending = false
	}
	a.flcMu.Unlock()
}

// stopProxyOnRunExit stops the embedded SS server when the agent itself exits.
//
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md).
// The SS server used to be anchored to the per-session runCtx, which meant agent exit
// stopped it for free — and also meant every SESSION REBUILD stopped it, which is the
// bug. Removing that anchor makes agent-exit teardown something we now have to say out
// loud. This is the fourth and last member of the CLOSED set of things that may stop the
// SS server; the other three are: an authoritative proxy OFF (the !d.Enabled arm of
// applyProxyDirective), a fail-closed fire after ProxyFailClosedGrace (failClosedFire),
// and a teardown-then-rebuild (the d.Token != "" arm). A session rebuild is deliberately
// NOT on that list.
//
// clearPersist=false: an ordinary restart must still find its footprint and re-bootstrap.
// nil nc: by the time Run's defers run the connection is drained, and publishing readiness
// on a dead conn is pointless — proxyTeardownLocked tolerates a nil conn.
func stopProxyOnRunExit(a *Agent) {
	release := beginProxyApply(a)
	defer release()
	p := a.proxy
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.agentExiting = true
	a.proxyTeardownLocked(p, nil, false)
}

// proxyRefusedDuringShutdown reports whether Run's exit teardown has already latched, in which
// case no directive may build anything. Caller holds p.mu.
//
// origin: proxy-lifecycle internal review MAJOR (statemachine lane). Removing the SS server's
// ctx anchor also removed the implicit "everything dies with the process context" cleanup, so a
// directive still in flight when Run returns could start a server that NOTHING will ever stop:
// the exit teardown has already run, and all four legitimate stoppers require a live agent.
//
// Extracted rather than inlined so applyProxyDirective stays off the god-function detector —
// maintidx counts comment lines inside the body, and this explanation is the point.
func proxyRefusedDuringShutdown(a *Agent, p *proxyRuntime) bool {
	if !p.agentExiting {
		return false
	}
	a.cfg.Logger.Debug("agent: ignoring proxy directive during shutdown")
	return true
}

// reapProxyCorpseOnHeartbeat is the cause-INDEPENDENT recovery edge: it takes p.mu and reaps a
// server that has stopped serving for ANY reason, not just the ones that happen to produce a
// directive.
//
// origin: proxy-lifecycle external review F4. The directive-entry reap only fires when a
// directive arrives, which ties recovery to the control plane having something to say. An
// accept loop that exits on a non-temporary error produces no such event, and on a single
// broker the heartbeat cannot supply one either while the node still reports itself converged.
// Running the reap here breaks that: it publishes unready and nils p.srv, so the very next
// heartbeat reports (0,0) + ProxyBound=false, which is a state neither of repairProxy's early
// returns matches — the broker then pushes the keyset that rebuilds us.
//
// Cheap by construction: the common case is one mutex acquisition and one Serving() call, both
// uncontended, once per heartbeat interval.
func reapProxyCorpseOnHeartbeat(a *Agent, nc *nats.Conn) {
	// Freeze deliberately stops the SS accept worker. Heartbeat repair must
	// share cut admission, or it mistakes that safe point for a corpse and
	// destroys the borrowed descriptors before final Prepare can duplicate them.
	// origin: hot-update S5 internal review HOTUPDATE-T44/T45
	leave, admitted := a.gate.admit(gateSS)
	if !admitted {
		return
	}
	defer leave()
	release := beginProxyApply(a)
	defer release()
	p := a.proxy
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.agentExiting {
		return
	}
	reapProxyCorpseLocked(a, p, nc)
}

// proxyDirectiveDigest uses the SS authority owner's canonical keyset digest.
// origin: hot-update S5 internal review rv-authority-4
func proxyDirectiveDigest(d *proto.ProxyDirective) ([32]byte, error) {
	return ssproxy.GrantDigest(ssproxy.Grant{Enabled: d.Enabled, Generation: d.Generation, Epoch: d.Epoch, Keys: toSSKeys(d.Keys)})
}

type proxyFailClosedState struct {
	DeadlineBootNS int64
	Fired          bool
	CleanupPending bool
	serial         uint64 // scheduler generation is local, never an authority revision
}

// proxyCutState is private handoff metadata, to be merged with the SS sealed
// records by the image owner. No JSON carrier may serialize this value. In
// particular the allocation token/home and keyset digest are never diagnostics.
// Snapshot and restore do no Start, dial, register or callback installation.
// origin: hot-update S5 plan §7.2 / §12.1
type proxyCutState struct {
	dispatchSequence, lastAppliedSequence int64
	generation, epoch                     int64
	keysetDigest                          [32]byte
	digestApplied                         bool
	publicPort, localPort                 int
	token, homeAddr                       string
	allocationSID, allocationNID          string
	homeEpoch                             int64
	tunnelUp                              bool
	plannedDeadlineBootNS                 int64
	needsReestablish, authoritativelyOff  bool
	timer                                 proxyFailClosedState
}

func (proxyCutState) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprint(state, "proxy cut [redacted]")
}

func snapshotProxyCut(a *Agent) (proxyCutState, error) {
	return snapshotProxyCutUntil(a, time.Time{})
}

func snapshotProxyCutUntil(a *Agent, deadline time.Time) (proxyCutState, error) {
	release, err := beginProxyApplyUntil(a, deadline)
	if err != nil {
		return proxyCutState{}, err
	}
	defer release()
	a.proxyDrainMu.Lock()
	dispatch := a.proxyDispatchN
	a.proxyDrainMu.Unlock()
	p := a.proxy
	p.mu.Lock()
	st := proxyCutState{
		dispatchSequence: dispatch, lastAppliedSequence: a.lastAppliedPushSeq,
		generation: p.gen, epoch: p.epoch, keysetDigest: p.keysetDigest,
		digestApplied: p.digestApplied, publicPort: p.publicPort, localPort: p.localPort,
		token: p.token, homeAddr: p.homeAddr, homeEpoch: p.homeEpoch,
		needsReestablish:   p.needsReestablish,
		authoritativelyOff: p.authoritativelyOff,
		allocationSID:      a.cfg.SID, allocationNID: nidOf(a),
	}
	p.planned.mu.Lock()
	st.tunnelUp = a.proxyTunnelUp.Load()
	st.plannedDeadlineBootNS = p.planned.deadlineBootNS
	p.planned.mu.Unlock()
	srv := p.srv
	p.mu.Unlock()
	a.flcMu.Lock()
	st.timer = p.flcState
	// A scheduled timer without a BOOTTIME deadline is legacy-only, never a
	// representable Linux image. Fired work is independent of timer existence.
	unrepresentable := a.flcTimer != nil && st.timer.DeadlineBootNS == 0
	a.flcMu.Unlock()
	if unrepresentable || !validProxyCut(st) {
		return proxyCutState{}, ssproxy.ErrStateUnrepresentable
	}
	// The final Prepare re-exports SS after this snapshot. Copy the same
	// dispatch, allocation and absolute timer facts into its declared codec;
	// the private compatibility envelope cannot diverge from the SS owner.
	// origin: hot-update S5 internal review PRIVATE-STATE-2
	if srv != nil {
		if err := ssproxy.SetProxyState(srv, proxySSState(a, st)); err != nil {
			return proxyCutState{}, err
		}
	}
	return st, nil
}

func validProxyCut(st proxyCutState) bool {
	return st.dispatchSequence >= 0 && st.lastAppliedSequence >= 0 &&
		st.lastAppliedSequence <= st.dispatchSequence && st.generation >= 0 && st.epoch >= 0 &&
		st.homeEpoch >= 0 && st.publicPort >= 0 && st.publicPort <= 65535 &&
		st.localPort >= 0 && st.localPort <= 65535 && st.timer.DeadlineBootNS >= 0 && st.plannedDeadlineBootNS >= 0 &&
		(!st.timer.CleanupPending || st.timer.Fired)
}

// restoreProxyCut must run before subscription installation. It cannot replace
// a serving owner or a running timer: that would silently lose cleanup work.
// origin: hot-update S5 plan §7.2
func restoreProxyCut(a *Agent, st proxyCutState) error {
	if !validProxyCut(st) {
		return ssproxy.ErrStateUnrepresentable
	}
	release := beginProxyApply(a)
	defer release()
	p := a.proxy
	p.mu.Lock()
	defer p.mu.Unlock()
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	if p.srv != nil || a.flcTimer != nil || p.flcState.CleanupPending {
		return ssproxy.ErrStateUnrepresentable
	}
	a.proxyDrainMu.Lock()
	a.proxyDispatchN = st.dispatchSequence
	a.lastAppliedPushSeq = st.lastAppliedSequence
	a.proxyDrainMu.Unlock()
	p.gen, p.epoch = st.generation, st.epoch
	p.keysetDigest, p.digestApplied = st.keysetDigest, st.digestApplied
	p.publicPort, p.localPort, p.token = st.publicPort, st.localPort, st.token
	p.homeAddr, p.homeEpoch = st.homeAddr, st.homeEpoch
	p.needsReestablish, p.authoritativelyOff = st.needsReestablish, st.authoritativelyOff
	p.flcState = st.timer
	p.flcState.serial++
	a.proxyPublicPort.Store(int64(st.publicPort))
	// Historical readiness is valid only through the original planned grace;
	// construction restores metadata but starts no timer or other worker.
	// origin: hot-update S5 internal review rv-authority-6
	p.planned.deadlineBootNS = st.plannedDeadlineBootNS
	now, err := procrequest.BootNow()
	a.proxyTunnelUp.Store(err == nil && st.tunnelUp && st.plannedDeadlineBootNS > now)
	return nil
}

// activateProxyTimer runs after the restored SS owner has been installed and
// before fresh authorization. Expiry is absolute; exec cannot renew grace.
// Fired cleanup is processed even when the scheduler handle is nil.
// origin: hot-update S5 plan §6.5 / §7.2
func activateProxyTimer(a *Agent) error {
	activatePlannedProxyReadiness(a)
	a.flcMu.Lock()
	st := &a.proxy.flcState
	serial := st.serial
	if a.flcTimer != nil {
		a.flcMu.Unlock()
		return nil
	}
	if st.Fired {
		pending := st.CleanupPending
		a.flcMu.Unlock()
		if pending {
			finishProxyFailClosed(a, serial)
		}
		return nil
	}
	if st.DeadlineBootNS == 0 {
		a.flcMu.Unlock()
		return nil
	}
	now, err := procrequest.BootNow()
	if err != nil {
		a.flcMu.Unlock()
		return err
	}
	remaining := time.Duration(st.DeadlineBootNS - now)
	if remaining <= 0 {
		a.flcMu.Unlock()
		if latchProxyFailClosed(a, serial) {
			finishProxyFailClosed(a, serial)
		}
		return nil
	}
	a.flcTimer = time.AfterFunc(remaining, func() {
		if latchProxyFailClosed(a, serial) {
			go finishProxyFailClosed(a, serial)
		}
	})
	a.flcMu.Unlock()
	return nil
}

// wireProxyFlowRevoke hands flow cleanup to an independent worker. In
// particular a callback running in the SS actor must not join that actor via
// Client.RevokeFlow or acquire the proxy table lock.
// origin: hot-update S5 plan §6.5 / §7.2
func wireProxyFlowRevoke(a *Agent, srv *ssproxy.Server) error {
	// A restored owner already contains its authority/timer descriptor. Check
	// the compatibility envelope before installing even the first callback.
	// origin: hot-update S5 internal review PRIVATE-STATE-2
	if _, imported := ssproxy.RestoredProxyState(srv); imported {
		state, err := snapshotProxyCut(a)
		if err != nil {
			return err
		}
		if err := ssproxy.ValidateProxyState(srv, proxySSState(a, state)); err != nil {
			return err
		}
	}
	adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter)
	if !ok {
		return nil
	} // legacy/custom adapters own no v1 client binding
	if adapter.client == nil {
		return ssproxy.ErrStateUnrepresentable
	}
	return srv.SetFlowRevoke(func(id [16]byte, _ string) {
		a.proxy.revokeWG.Add(1)
		go func() {
			defer a.proxy.revokeWG.Done()
			if err := adapter.client.RevokeFlow(flow.ID(id), flow.ReasonRevoked); err != nil {
				a.cfg.Logger.Warn("agent: proxy bound flow revoke failed")
			}
		}()
	})
}

// expectProxyFlow is the BeforeLocalConnect hook for the production v1 client.
// It uses a one-shot tuple reservation; no remote-address guessing is allowed.
// Only a flow whose local target is the SS proxy's own port is reserved: every
// other expose flow is not SS traffic and must connect untouched, including
// while the proxy is disabled. The server and port are read together under
// proxy.mu so a concurrent apply cannot pair one generation's port with
// another generation's server. A proxy that has a server but no bound port yet
// refuses with ErrAuthorityPending instead of letting the flow race the bind.
// origin: hot-update S5 plan §6.5 (P14 bootstrap hook + P15 proxy hook merged
// by the lead: the two owners had each declared it, with different semantics).
func expectProxyFlow(a *Agent, id flow.ID, tuple rawsocket.Tuple) error {
	p := a.proxy
	p.mu.Lock()
	srv, local := p.srv, p.localPort
	p.mu.Unlock()
	if srv == nil {
		return nil
	}
	if local == 0 {
		return ssproxy.ErrAuthorityPending
	}
	if int(tuple.Remote.Port()) != local {
		return nil
	}
	// A zero ID is the legacy connector's explicit tuple reservation. It
	// carries no resumable flow, so the retained legacy slot remains busy.
	// origin: hot-update S5 plan §4.1 / §9.1 / DRILL-RUN
	if id == (flow.ID{}) {
		return ssproxy.ExpectLegacyFlow(srv, tuple)
	}
	return srv.ExpectFlow([16]byte(id), tuple)
}

const proxyStateRef = "proxy-state.agent"
const proxyStateLimit = 16 << 10

// The declared private descriptor codec never enters Bundle JSON. Both directions
// validate ordering, flags and exact closure before restoring any callback state.
// origin: hot-update S5 plan §6.6 / §7.2
func encodeProxyCut(st proxyCutState) ([]byte, error) {
	if !validProxyCut(st) {
		return nil, ssproxy.ErrStateUnrepresentable
	}
	return ssproxy.EncodeProxyCut(ssproxy.ProxyCut{
		State: proxySSState(nil, st), LocalPort: st.localPort, Token: st.token, HomeAddr: st.homeAddr, HomeEpoch: st.homeEpoch,
		DigestApplied: st.digestApplied, TunnelUp: st.tunnelUp, NeedsReestablish: st.needsReestablish,
	})
}

func decodeProxyCut(data []byte) (proxyCutState, error) {
	cut, err := ssproxy.DecodeProxyCut(data)
	if err != nil {
		return proxyCutState{}, err
	}
	p := cut.State
	st := proxyCutState{
		dispatchSequence: p.DispatchSequence, lastAppliedSequence: p.LastAppliedSequence,
		generation: p.Generation, epoch: p.Epoch, keysetDigest: p.KeysetDigest, digestApplied: cut.DigestApplied,
		publicPort: p.AllocationPort, localPort: cut.LocalPort, token: cut.Token, homeAddr: cut.HomeAddr, homeEpoch: cut.HomeEpoch,
		allocationSID: p.AllocationSID, allocationNID: p.AllocationNID,
		tunnelUp: cut.TunnelUp, plannedDeadlineBootNS: p.PlannedDeadlineBootNS,
		needsReestablish: cut.NeedsReestablish, authoritativelyOff: !p.Enabled,
		timer: proxyFailClosedState{DeadlineBootNS: p.DeadlineBootNS, Fired: p.Fired, CleanupPending: p.CleanupPending},
	}
	if !validProxyCut(st) {
		return proxyCutState{}, ssproxy.ErrStateUnrepresentable
	}
	return st, nil
}

// The readiness hook cannot take proxy.mu/client.mu: Open and teardown invoke
// it while owning those tables. This separate guard serializes its publication
// with expiry, without joining workers or waiting on a control-plane session.
// origin: hot-update S5 internal review rv-authority-6
type proxyPlannedReadiness struct {
	mu             sync.Mutex
	deadlineBootNS int64
	timer          *time.Timer
}

func applyProxyTunnelState(a *Agent, port int, up bool) {
	a.gate.mu.Lock()
	planned := a.gate.closed
	a.gate.mu.Unlock()
	p := a.proxy
	if p == nil {
		return
	}
	latch := &p.planned
	latch.mu.Lock()
	defer latch.mu.Unlock()
	if int64(port) != a.proxyPublicPort.Load() {
		return
	}
	if !up && (planned || latch.deadlineBootNS != 0) && a.proxyTunnelUp.Load() {
		now, err := procrequest.BootNow()
		if err == nil {
			if latch.deadlineBootNS == 0 {
				latch.deadlineBootNS = now + int64(tunnel.DetachedGrace)
			}
			if latch.timer == nil {
				schedulePlannedProxyReadinessLocked(a, port, now)
			}
			if now < latch.deadlineBootNS {
				return
			}
		}
	}
	if up {
		clearPlannedProxyReadinessLocked(latch)
	}
	a.proxyTunnelUp.Store(up)
	if nc := a.ncBox.Load(); nc != nil {
		a.pubProxyReady(nc, up)
	}
}

func clearPlannedProxyReadinessLocked(latch *proxyPlannedReadiness) {
	if latch.timer != nil {
		latch.timer.Stop()
		latch.timer = nil
	}
	latch.deadlineBootNS = 0
}
func clearPlannedProxyReadiness(a *Agent) {
	latch := &a.proxy.planned
	latch.mu.Lock()
	clearPlannedProxyReadinessLocked(latch)
	latch.mu.Unlock()
}
func schedulePlannedProxyReadinessLocked(a *Agent, port int, now int64) {
	latch := &a.proxy.planned
	deadline := latch.deadlineBootNS
	latch.timer = time.AfterFunc(time.Duration(max(0, deadline-now)), func() {
		latch.mu.Lock()
		defer latch.mu.Unlock()
		if latch.deadlineBootNS != deadline || int64(port) != a.proxyPublicPort.Load() {
			return
		}
		latch.timer = nil
		// Keep the expired deadline as evidence: another down edge cannot extend it.
		a.proxyTunnelUp.Store(false)
		if nc := a.ncBox.Load(); nc != nil {
			a.pubProxyReady(nc, false)
		}
	})
}
func activatePlannedProxyReadiness(a *Agent) {
	latch := &a.proxy.planned
	latch.mu.Lock()
	defer latch.mu.Unlock()
	if latch.deadlineBootNS == 0 || latch.timer != nil {
		return
	}
	now, err := procrequest.BootNow()
	if err != nil {
		a.proxyTunnelUp.Store(false)
		return
	}
	schedulePlannedProxyReadinessLocked(a, int(a.proxyPublicPort.Load()), now)
}

// Reserve historical readiness before PREPARE closes a carrier: its supervisor
// can publish down after Freeze returns, and final export must already retain
// the original conservative grace. Only a real ready proxy can start a latch;
// the reservation starts no timer until the carrier actually goes down.
// origin: hot-update S5 internal review rv-authority-6
func beginPlannedProxyReadiness(a *Agent) {
	if a.proxy == nil {
		return
	}
	latch := &a.proxy.planned
	latch.mu.Lock()
	defer latch.mu.Unlock()
	if !a.proxyTunnelUp.Load() || latch.deadlineBootNS != 0 {
		return
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return
	}
	latch.deadlineBootNS = now + int64(tunnel.DetachedGrace)
}

// A failed PREPARE that never detached keeps a healthy attached carrier. Drop
// only its unscheduled reservation; a real down edge owns its original expiry.
func cancelAttachedProxyReadiness(a *Agent) {
	if a.proxy == nil {
		return
	}
	latch := &a.proxy.planned
	latch.mu.Lock()
	if latch.timer == nil {
		clearPlannedProxyReadinessLocked(latch)
	}
	latch.mu.Unlock()
}

// This is only wiring: ssproxy owns the typed record builder and validator.
// The raw allocation token remains in the existing sealed routing envelope;
// the declared authority record carries the same allocation's token hash.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func proxySSState(a *Agent, st proxyCutState) ssproxy.ProxyState {
	p := ssproxy.ProxyState{
		DispatchSequence: st.dispatchSequence, LastAppliedSequence: st.lastAppliedSequence,
		Generation: st.generation, Epoch: st.epoch, KeysetDigest: st.keysetDigest,
		Enabled: !st.authoritativelyOff, DeadlineBootNS: st.timer.DeadlineBootNS, PlannedDeadlineBootNS: st.plannedDeadlineBootNS,
		Fired: st.timer.Fired, CleanupPending: st.timer.CleanupPending,
	}
	if st.publicPort != 0 {
		p.AllocationSID, p.AllocationNID, p.AllocationPort = st.allocationSID, st.allocationNID, st.publicPort
		if p.AllocationSID == "" && a != nil {
			p.AllocationSID, p.AllocationNID = a.cfg.SID, nidOf(a)
		}
		p.AllocationTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte(st.token)))
		p.AllocationEpoch = st.homeEpoch
	}
	return p
}
