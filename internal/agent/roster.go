package agent

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// roster.go (C1) — the agent-side consumer of the account-signed broker roster: it adopts the
// roster delivered on every register reply (verify → relearn dial URLs → advance the monotone
// generation → persist), refreshes it periodically without a full re-register, and rebuilds the
// session onto the freshest roster when nats.go is stuck-disconnected (live failover). See
// docs/reviews/c1-plan.md.

const (
	// defaultRosterRefreshInterval is the cadence of the online roster refresh (full-jittered).
	// 3min keeps the post-add convergence comfortably under the 5min acceptance budget.
	defaultRosterRefreshInterval = 3 * time.Minute
	// defaultRosterRefreshFailBackoff is the SHORT retry after a failed/empty refresh tick
	// (for example, a leader failover), keeping one transient failure within the 5min SLA.
	defaultRosterRefreshFailBackoff = 20 * time.Second
	// redialAfter is how long nats.go may stay disconnected (retrying its now-dead boot pool) before
	// the watchdog rebuilds the session on the freshest roster. > a normal reconnect, < a human's
	// patience; the rebuild is a no-op churn-free path for a healthy agent (it only fires when stuck).
	redialAfter = 20 * time.Second
	// maxSilentRosterRefreshes (#48) is how many CONSECUTIVE failed roster-only refreshes on an
	// otherwise-healthy NATS connection mark the current broker as a SILENT retired-broker island.
	// A retired broker answers nothing (its register handler early-returns on isClusterFollower) yet
	// the NATS connection to its still-running nats-server never drops — so neither the "current
	// broker is leaving" roster edge (it needs a reply) nor the disconnect-armed redial watchdog can
	// ever fire, and the agent starves. After this many misses (≈ N × failBackoff of silence) the
	// agent rebuilds onto a known voter instead. 3 tolerates a transient miss / one leader failover
	// (each retried on failBackoff) before concluding the broker is gone.
	maxSilentRosterRefreshes = 3
)

// agentNow is the agent clock seam (cfg.Now or time.Now).
func (a *Agent) agentNow() time.Time {
	if a.cfg.Now != nil {
		return a.cfg.Now()
	}
	return time.Now()
}

// loadRosterCacheAtBoot seeds the in-memory roster mirror from state.json once at startup so the
// FIRST connectNATS can already dial the learned set (cached → seed). An OOB cfg.AccountPub pin is
// authoritative (disables TOFU). The cached roster's URLs are used only if it still VerifyAt's
// (its expires_at bounds replay); an expired cache still contributes its pin + generation hwm so
// anti-rollback survives a restart, but falls back to seed-only dialing until a fresh roster lands.
func (a *Agent) loadRosterCacheAtBoot() {
	a.rosterMu.Lock()
	a.pinAccount = a.cfg.AccountPub // OOB override (may be "")
	a.rosterMu.Unlock()

	rc := a.loadCachedRoster()
	if rc == nil {
		return
	}
	now := a.agentNow()
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	if a.pinAccount == "" { // no OOB → adopt the persisted TOFU pin
		a.pinAccount = rc.PinAccountPub
	}
	a.rosterGen = rc.Generation
	a.seedGen = rc.SeedGen
	a.cachedRoster = rc.Roster
	a.cachedSeeds = rc.Seeds
	if a.pinAccount == "" {
		return // nothing to verify the cached roster/seeds against
	}
	// The cached roster's URLs are used only if it still VerifyAt's (its expires_at bounds replay); an
	// expired cache still contributes its pin + generation hwm so anti-rollback survives a restart.
	if rc.Roster != nil {
		if err := clusterroster.VerifyAt(rc.Roster, a.pinAccount, now); err == nil {
			if urls, derr := clusterroster.DialURLs(rc.Roster, a.cfg.NATSURL); derr == nil && len(urls) > 0 {
				a.rosterURLs = urls
			}
		}
	}
	// C2: re-verify the cached SeedBundle against the pin (anti-replay survives the restart — a
	// tampered cached bundle is NOT trusted), mirroring the roster.
	if rc.Seeds != nil {
		if err := clusterroster.VerifySeedsAt(rc.Seeds, a.pinAccount, now); err == nil {
			a.seedURLs = clusterroster.SeedDialURLs(rc.Seeds)
		}
	}
}

// loadCachedRoster reads the persisted discovery cache, preferring the C2 roster_cache.json (with a
// one-time C1 state.json migration) and falling back to the legacy stateStore for in-process tests
// with no Home. Returns nil when no cache exists.
func (a *Agent) loadCachedRoster() *RosterCache {
	if a.rosterCacheStore != nil {
		rc, err := a.rosterCacheStore.load()
		if err != nil {
			a.cfg.Logger.Warn("agent: load roster cache", "err", err)
			return nil
		}
		if rc != nil {
			return rc
		}
		if a.stateStore != nil { // migration: a C1 agent stored the cache inside state.json.Roster
			if leg, lerr := a.stateStore.GetRosterCache(); lerr == nil {
				return leg
			}
		}
		return nil
	}
	if a.stateStore != nil { // in-process tests / no Home
		if rc, err := a.stateStore.GetRosterCache(); err == nil {
			return rc
		}
	}
	return nil
}

// RosterState is the agent's consumer discovery state — the input/output of the pure single-authority
// AdoptDecision (C2 §2.5). Both the daemon (adoptManifest, holds rosterMu + persists) and the CLI
// (config refresh / join / doctor) decide through AdoptDecision over this struct, so there is exactly
// ONE verify/adopt enforcement site.
type RosterState struct {
	Pin       string
	RosterGen uint64
	SeedGen   uint64
	DialURLs  []string
	SeedURLs  []string
	Roster    *proto.ClusterRoster
	Seeds     *proto.SeedBundle
}

// AdoptDecision is the ONE consumer authority (C1 roster decision table + C2 seed mirror). It verifies
// a roster and/or a seed bundle against the pin (TOFU on the FIRST roster when pin==""), enforces the
// monotone roster/seed generations, and returns the next state + whether anything was accepted. Every
// reject keeps prior state (never wedge, never empty a pool). The pin passed to VerifyAt/VerifySeedsAt
// is the PERSISTED/OOB pin — never the object's self-claimed AccountPub — except the first-TOFU roster.
// templateURL is the agent's cfg.NATSURL (roster DialURLs templating). PURE — no I/O, no lock, the
// only clock is the passed `now`.
func AdoptDecision(prev RosterState, r *proto.ClusterRoster, s *proto.SeedBundle, templateURL string, now time.Time) (RosterState, bool) {
	next := prev
	accepted := false
	pin := prev.Pin

	if r != nil {
		tofu := pin == ""
		vp := pin
		if tofu {
			vp = r.AccountPub // row 2: trust-on-first-use
		}
		// rows 2-7: sig + account pin + schema + expiry + monotone generation. A reject leaves `next`
		// at `prev` for the roster fields (prior kept).
		if err := clusterroster.VerifyAt(r, vp, now); err == nil && r.Generation >= prev.RosterGen {
			urls, derr := clusterroster.DialURLs(r, templateURL)
			if derr == nil {
				if len(urls) == 0 {
					urls = prev.DialURLs // row E: empty-set floor — keep prior, never empty the pool
				}
				if tofu {
					pin = r.AccountPub
					next.Pin = r.AccountPub
				}
				next.RosterGen = r.Generation
				next.DialURLs = urls
				next.Roster = r
				accepted = true
			}
		}
	}

	// Seeds verify against the SAME pin (possibly just TOFU-set by the roster above). A fresh agent
	// with no pin AND no roster cannot adopt seeds — it has nothing to verify them against.
	if s != nil && pin != "" {
		if err := clusterroster.VerifySeedsAt(s, pin, now); err == nil && s.Generation >= prev.SeedGen {
			next.SeedGen = s.Generation
			next.SeedURLs = clusterroster.SeedDialURLs(s)
			next.Seeds = s
			accepted = true
		}
	}
	return next, accepted
}

// adoptManifest is the daemon's single adopt path: decide via AdoptDecision, then apply to the
// in-memory mirror (monotone re-decide under the lock for concurrent adopts) + persist. nil roster
// (single broker) AND nil seeds → no-op (byte-equivalent register reply).
func (a *Agent) adoptManifest(r *proto.ClusterRoster, s *proto.SeedBundle) bool {
	if r == nil && s == nil {
		return false
	}
	a.rosterMu.Lock()
	prev := RosterState{
		Pin: a.pinAccount, RosterGen: a.rosterGen, SeedGen: a.seedGen,
		DialURLs: a.rosterURLs, SeedURLs: a.seedURLs, Roster: a.cachedRoster, Seeds: a.cachedSeeds,
	}
	a.rosterMu.Unlock()

	next, accepted := AdoptDecision(prev, r, s, a.cfg.NATSURL, a.agentNow())
	if !accepted {
		if r != nil || s != nil {
			a.cfg.Logger.Warn("agent: roster/seed rejected (sig / account / schema / expiry / generation)")
		}
		return false
	}

	a.rosterMu.Lock()
	pinned := false
	applied := false
	if next.Pin != "" && a.pinAccount == "" { // first-writer-wins TOFU under the final lock
		a.pinAccount = next.Pin
		pinned = true
	}
	if r != nil && next.RosterGen >= a.rosterGen { // monotone re-decide vs a concurrent higher-gen adopt
		a.rosterGen = next.RosterGen
		a.rosterURLs = next.DialURLs
		a.cachedRoster = next.Roster
		applied = true
	}
	if s != nil && next.SeedGen >= a.seedGen {
		a.seedGen = next.SeedGen
		a.seedURLs = next.SeedURLs
		a.cachedSeeds = next.Seeds
		applied = true
	}
	a.rosterMu.Unlock()
	if pinned {
		a.cfg.Logger.Info("agent: roster account pinned (TOFU)", "account_pub", next.Pin)
	}
	a.persistRosterCache()
	return applied
}

// adoptRoster is the C1 roster-only entry point (boot/reconnect register + refresh). It funnels to the
// single authority with no seed bundle — byte-behavior-identical to C1 for the roster path.
func (a *Agent) adoptRoster(r *proto.ClusterRoster) bool { return a.adoptManifest(r, nil) }

// manifestFetchTimeout bounds the cold-start bootstrap manifest fetch.
const manifestFetchTimeout = 20 * time.Second

// bootstrapFetchOnce (C2 §2.4 tier 2) fetches the well-known manifest ONCE at cold-start and adopts it
// (the two children verified against the pin via AdoptDecision). Async + best-effort: it never blocks
// Run and a failure is a no-op (the cached set + cfg.NATSURL floor keep the agent connecting). It is a
// NO-OP without a pin — a manifest from an HTTP endpoint is NEVER TOFU-pinned (a new pin is only ever
// established by `agent join` with an OOB account_pub).
func (a *Agent) bootstrapFetchOnce(ctx context.Context) {
	if a.pinnedAccountPub() == "" {
		return
	}
	fetchCtx, cancel := context.WithTimeout(ctx, manifestFetchTimeout)
	defer cancel()
	m, err := clusterroster.FetchManifest(fetchCtx, a.cfg.BootstrapURL, 0)
	if err != nil {
		a.cfg.Logger.Debug("agent: bootstrap manifest fetch failed (non-fatal)", "err", err)
		return
	}
	a.adoptManifest(m.Roster, m.Seeds)
}

// persistRosterCache writes the current in-memory mirror (pin + both generations + roster + seeds) to
// the C2 roster_cache.json store (or the legacy stateStore in in-process tests with no Home). The
// cache is internally consistent — its embedded roster/seeds match the stored hwms (taken under the
// same lock). Best-effort; a failure is non-fatal (the in-memory mirror still drives this lifetime).
func (a *Agent) persistRosterCache() {
	a.rosterMu.Lock()
	rc := RosterCache{
		PinAccountPub: a.pinAccount, Generation: a.rosterGen, Roster: a.cachedRoster,
		SeedGen: a.seedGen, Seeds: a.cachedSeeds,
	}
	a.rosterMu.Unlock()
	if a.rosterCacheStore != nil {
		if err := a.rosterCacheStore.save(&rc); err != nil {
			a.cfg.Logger.Warn("agent: persist roster cache", "err", err)
		}
		return
	}
	if a.stateStore != nil { // legacy fallback (in-process tests with no Home set)
		if err := a.stateStore.SetRosterCache(&rc); err != nil {
			a.cfg.Logger.Warn("agent: persist roster cache (legacy)", "err", err)
		}
	}
}

// effectiveDialURLs returns the comma-joined dial string for connectNATS, in tiers: the learned roster
// URLs (VOTER-first, templated off cfg.NATSURL) → the verified SeedBundle endpoints (C2: a fresh agent
// rotates onto re-published endpoints once seed_generation advances) → the configured seed URL(s) as a
// PERMANENT floor (last, never removed). seedURLs/rosterURLs are empty for a non-cluster agent → the
// dial string is byte-identical to cfg.NATSURL (non-cluster byte-equivalence). Every seed endpoint was
// VerifySeedsAt-verified against the pin before it landed here, so this is an authenticated tier.
func (a *Agent) effectiveDialURLs() string {
	a.rosterMu.Lock()
	learned := append([]string(nil), a.rosterURLs...)
	seeds := append([]string(nil), a.seedURLs...)
	a.rosterMu.Unlock()
	// Delegates to the shared clusterroster.BuildDialString so the agent and the ctl/cli honor ONE
	// floor-last invariant (the ctl reuses it for broker auto-failover) with no drift.
	return clusterroster.BuildDialString(learned, seeds, a.cfg.NATSURL)
}

// cachedRosterGen / pinnedAccountPub expose the in-memory mirror (register req / tests / grep-guard).
func (a *Agent) cachedRosterGen() uint64 {
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	return a.rosterGen
}

func (a *Agent) pinnedAccountPub() string {
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	return a.pinAccount
}

// rosterRefreshLoop pulls a fresh signed roster on a jittered cadence using a roster-only register
// (RosterRefreshOnly → the broker answers from RODB with no raft write / event / reconcile). It runs
// for the lifetime of one session; ctx is the session context. A negative interval disables it.
func (a *Agent) rosterRefreshLoop(ctx context.Context, nc *nats.Conn) {
	iv := a.cfg.RosterRefreshInterval
	if iv == 0 {
		iv = defaultRosterRefreshInterval
	}
	if iv < 0 {
		return
	}
	timer := time.NewTimer(jitterDur(iv))
	defer timer.Stop()
	silent := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-a.rosterRefreshNow:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		// Single-flight: a reconnect/rebuild's own register already refreshes the roster.
		if a.reconnectInFlight.Load() || a.rebuilding.Load() {
			timer.Reset(jitterDur(iv))
			continue
		}
		next := iv
		if a.refreshRosterOnce(ctx, nc) {
			silent = 0
		} else {
			silent++
			// #48: sustained silence on a healthy connection == a retired-broker island. If the
			// cached roster still knows another dialable voter, rebuild onto it (excluding the
			// silent broker from the immediate dial) instead of polling the island forever.
			if silent >= maxSilentRosterRefreshes && a.rebuildOnBrokerSilence(nc) {
				return
			}
			next = a.rosterRefreshFailBackoff
		}
		timer.Reset(jitterDur(next))
	}
}

// refreshRosterOnce sends one roster-only register and adopts the returned roster. Returns false on a
// failed / empty pull (caller retries on the short backoff). Best-effort — never fatal.
func (a *Agent) refreshRosterOnce(ctx context.Context, nc *nats.Conn) bool {
	req := proto.NodeRegisterReq{
		ProtoVersion:      proto.ProtoVersion,
		NID:               a.cfg.NID,
		RosterGen:         a.cachedRosterGen(),
		RosterRefreshOnly: true,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return false
	}
	reqCtx, cancel := context.WithTimeout(ctx, a.cfg.RegisterTimeout)
	msg, err := nc.RequestWithContext(reqCtx, proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID), payload)
	cancel()
	if err != nil {
		return false
	}
	var resp proto.NodeRegisterResp
	if json.Unmarshal(msg.Data, &resp) != nil || !resp.OK {
		return false
	}
	if resp.Roster != nil {
		a.rosterMu.Lock()
		previous := a.cachedRoster
		a.rosterMu.Unlock()
		accepted := a.adoptRoster(resp.Roster)
		// Updating only the NEXT dial pool is insufficient when this connection is still healthy
		// at the NATS layer but its broker has entered DRAINING/RETIRING. That leaves the agent on
		// a stale route island after the broker is removed. Trigger a single session rebuild only
		// after the signed roster was actually accepted (verified, non-rollback and persisted).
		if accepted && rosterRequiresReconnect(previous, resp.Roster, nc.ConnectedUrl()) {
			a.requestRosterReconnect(nc)
		}
	}
	return true // a single/non-cluster broker (nil roster) is a successful no-op, not a retry
}

// rosterRequiresReconnect is the narrow proactive-move policy: a healthy connection is rebuilt only
// when another dialable VOTER exists and the signed roster either marks the current broker as leaving,
// or removes a broker that the previously accepted roster identified as current. The previous-roster
// fence prevents an unrelated DNS/route alias from looking like a removal on every refresh.
func rosterRequiresReconnect(previous, current *proto.ClusterRoster, connectedURL string) bool {
	cu, err := url.Parse(connectedURL)
	if err != nil || cu.Hostname() == "" || current == nil {
		return false
	}
	connectedHost := cu.Hostname()
	knownBefore := rosterContainsHost(previous, connectedHost)
	currentPresent := false
	currentLeaving := false
	otherVoter := false
	for _, b := range current.Brokers {
		isCurrent := rosterBrokerMatchesHost(b, connectedHost)
		if isCurrent {
			currentPresent = true
			switch b.Phase {
			case proto.RosterPhaseDraining, proto.RosterPhaseRetiring, proto.RosterPhaseAddFailed:
				currentLeaving = true
			}
			continue
		}
		// Dialability filters destinations, not identity. A cfg.NATSURL floor may legitimately connect
		// a local/dev agent to a loopback roster entry; skipping that entry before the identity check
		// makes an unchanged roster look like a removal and churns the session every refresh.
		if b.PublicHost == "" || clusterroster.IsUndialableHost(b.PublicHost) {
			continue
		}
		if b.Phase == proto.RosterPhaseVoter {
			otherVoter = true
		}
	}
	return otherVoter && (currentLeaving || (knownBefore && !currentPresent))
}

func rosterContainsHost(r *proto.ClusterRoster, host string) bool {
	if r == nil {
		return false
	}
	for _, b := range r.Brokers {
		if rosterBrokerMatchesHost(b, host) {
			return true
		}
	}
	return false
}

func rosterBrokerMatchesHost(b proto.RosterBroker, host string) bool {
	if strings.EqualFold(host, b.PublicHost) {
		return true
	}
	ru, err := url.Parse(b.NatsRoute)
	return err == nil && ru.Hostname() != "" && strings.EqualFold(host, ru.Hostname())
}

func (a *Agent) requestRosterReconnect(nc *nats.Conn) {
	a.rebuildOntoVoter(nc, "agent: current broker is leaving the signed roster; rebuilding NATS session on a voter")
}

// rebuildOntoVoter tears the current session down so the Run loop re-dials the freshest pool. It
// single-flights via `rebuilding` (returns false if a rebuild is already in flight); the shared
// body for both the roster-says-leaving path (requestRosterReconnect) and the broker-silence path
// (rebuildOnBrokerSilence). reason is logged.
func (a *Agent) rebuildOntoVoter(nc *nats.Conn, reason string) bool {
	if !a.rebuilding.CompareAndSwap(false, true) {
		return false
	}
	a.rebuildRequested.Store(true)
	a.cfg.Logger.Warn(reason, "connected_url", nc.ConnectedUrl())
	nc.Close()
	a.sessCancelMu.Lock()
	cancel := a.sessCancel
	a.sessCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

// rebuildOnBrokerSilence (#48) fires when the current broker has gone silent. It only acts when the
// cached roster still names ANOTHER dialable voter to fail over to — else the disconnect-armed
// redial watchdog (all brokers truly gone) or the seed floor is the right recovery, and churning
// here would be pointless. It stamps the silent broker's host as a one-shot dial exclusion so the
// rebuild lands on the survivor rather than re-sticking to the island. Returns true iff it rebuilt.
func (a *Agent) rebuildOnBrokerSilence(nc *nats.Conn) bool {
	cu, err := url.Parse(nc.ConnectedUrl())
	if err != nil || cu.Hostname() == "" {
		return false
	}
	host := cu.Hostname()
	if !a.hasOtherDialableVoter(host) {
		return false
	}
	a.setAvoidHost(host)
	if !a.rebuildOntoVoter(nc, "agent: current broker went silent (no roster reply — retired-broker island); rebuilding onto a known voter") {
		return false // a rebuild is already in flight; the avoid hint is consumed next connect
	}
	fireAfterSilenceEscape(a, host)
	return true
}

// hasOtherDialableVoter reports whether the cached signed roster names a VOTER on a host other than
// connectedHost that is actually dialable (not loopback/unspecified). Read-only over the in-memory
// mirror. It gates the #48 silence rebuild: with no survivor to move to, staying put is correct.
func (a *Agent) hasOtherDialableVoter(connectedHost string) bool {
	a.rosterMu.Lock()
	r := a.cachedRoster
	a.rosterMu.Unlock()
	if r == nil {
		return false
	}
	for _, b := range r.Brokers {
		if b.Phase != proto.RosterPhaseVoter {
			continue
		}
		if b.PublicHost == "" || clusterroster.IsUndialableHost(b.PublicHost) {
			continue
		}
		if strings.EqualFold(b.PublicHost, connectedHost) {
			continue
		}
		return true
	}
	return false
}

// setAvoidHost / takeAvoidHost carry the one-shot dial exclusion (#48) from the silence-rebuild
// decision to the next connectNATS. takeAvoidHost clears it so it applies to exactly one dial.
func (a *Agent) setAvoidHost(host string) {
	a.avoidHostMu.Lock()
	a.avoidHost = host
	a.avoidHostMu.Unlock()
}

func (a *Agent) takeAvoidHost() string {
	a.avoidHostMu.Lock()
	defer a.avoidHostMu.Unlock()
	h := a.avoidHost
	a.avoidHost = ""
	return h
}

// excludeHostFromDial drops every comma-separated dial URL whose host equals `host` (the just-silent
// island broker). If the filter would strand the agent (empty result), it returns the input
// unchanged — never trade a stuck-on-island for a stuck-on-nothing.
func excludeHostFromDial(csv, host string) string {
	if csv == "" || host == "" {
		return csv
	}
	parts := strings.Split(csv, ",")
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		u, err := url.Parse(strings.TrimSpace(p))
		if err == nil && u.Hostname() != "" && strings.EqualFold(u.Hostname(), host) {
			continue
		}
		kept = append(kept, p)
	}
	if len(kept) == 0 {
		return csv
	}
	return strings.Join(kept, ",")
}

// --- C1 §D-1 L3: stuck-reconnect session-rebuild watchdog ---

// armRedialWatchdog starts the single-arm stuck-reconnect timer on a NATS disconnect (mirrors
// armFailClosed's idempotent one-timer guard). It does NOT arm during graceful shutdown (the
// nc.Drain on Run exit fires a DisconnectErr that must not outlive the agent).
func (a *Agent) armRedialWatchdog() {
	if rc := a.loadRunCtx(); rc != nil && rc.Err() != nil {
		return
	}
	a.redialMu.Lock()
	defer a.redialMu.Unlock()
	if a.redialTimer != nil {
		return // single-arm: re-armed not stacked
	}
	a.redialTimer = time.AfterFunc(redialAfter, a.fireRedial)
}

// stopRedialWatchdog cancels + clears the timer on any successful (re)connect / rebuild.
func (a *Agent) stopRedialWatchdog() {
	a.redialMu.Lock()
	defer a.redialMu.Unlock()
	if a.redialTimer != nil {
		a.redialTimer.Stop()
		a.redialTimer = nil
	}
}

// fireRedial rebuilds the session when nats.go has been stuck-disconnected past redialAfter (its boot
// pool is dead — exactly the case a newly-added broker rescues). It single-flights via `rebuilding`
// (so onNATSReconnect no-ops on the dying conn — no double-subscribe), signals the session loop to
// return rebuild=true, Closes the dying conn (Close does NOT fire ReconnectHandler), and cancels the
// session ctx to unblock heartbeatLoop.
func (a *Agent) fireRedial() {
	if !a.rebuilding.CompareAndSwap(false, true) {
		return
	}
	a.rebuildRequested.Store(true)
	a.cfg.Logger.Warn("agent: NATS stuck-disconnected; rebuilding session on the freshest roster")
	if nc := a.ncBox.Load(); nc != nil {
		nc.Close()
	}
	a.sessCancelMu.Lock()
	cancel := a.sessCancel
	a.sessCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// setRunCtx / loadRunCtx synchronize a.runCtx across the session goroutine (which rewrites it every
// session) and the NATS-callback goroutines that read it (Stage-C MAJOR-1 — the rebuild loop turned
// runCtx into concurrently-mutated cross-session state).
func (a *Agent) setRunCtx(ctx context.Context) {
	a.runCtxMu.Lock()
	a.runCtx = ctx
	a.runCtxMu.Unlock()
}

func (a *Agent) loadRunCtx() context.Context {
	a.runCtxMu.Lock()
	defer a.runCtxMu.Unlock()
	return a.runCtx
}

func (a *Agent) setSessionCancel(c context.CancelFunc) {
	a.sessCancelMu.Lock()
	a.sessCancel = c
	a.sessCancelMu.Unlock()
}

func (a *Agent) clearSessionCancel() {
	a.sessCancelMu.Lock()
	a.sessCancel = nil
	a.sessCancelMu.Unlock()
}
