package agent

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agentstate"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"

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
	a.cachedSeeds = rc.Seeds
	if a.pinAccount == "" {
		return // nothing to verify the cached roster/seeds against
	}
	// The cached roster's URLs are used only if it still VerifyAt's (its expires_at bounds replay); an
	// expired cache still contributes its pin + generation hwm so anti-rollback survives a restart.
	if rc.Roster != nil {
		if err := clusterroster.VerifyAt(rc.Roster, a.pinAccount, now); err == nil {
			a.cachedRoster = rc.Roster
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
		rc, err := a.rosterCacheStore.Load()
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

// Preserve the public consumer API shared with the CLI.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type RosterState = clusterroster.ConsumerState

func AdoptDecision(prev RosterState, r *proto.ClusterRoster, seeds *proto.SeedBundle, templateURL string, now time.Time) (RosterState, bool) {
	return clusterroster.AdoptDecision(prev, r, seeds, templateURL, now)
}

// adoptManifest is the daemon's single adopt path: decide via AdoptDecision, then apply to the
// in-memory mirror (monotone re-decide under the lock for concurrent adopts) + persist. nil roster
// (single broker) AND nil seeds → no-op (byte-equivalent register reply).
func (a *Agent) adoptManifest(r *proto.ClusterRoster, seeds *proto.SeedBundle) bool {
	if r == nil && seeds == nil {
		return false
	}
	now := a.agentNow()
	a.rosterMu.Lock()
	prev := RosterState{Pin: a.pinAccount, RosterGen: a.rosterGen, SeedGen: a.seedGen,
		DialURLs: a.rosterURLs, SeedURLs: a.seedURLs, Roster: a.cachedRoster, Seeds: a.cachedSeeds}
	// Verification and monotone adoption share the final lock. A concurrent
	// first pin or newer roster cannot turn an earlier verification into authority.
	// origin: hot-update S8 plan §11.2 P14.
	next, accepted := AdoptDecision(prev, r, seeds, a.cfg.NATSURL, now)
	if !accepted {
		a.rosterMu.Unlock()
		a.cfg.Logger.Warn("agent: roster/seed rejected (sig / account / schema / expiry / generation)")
		return false
	}
	pinned := a.pinAccount == "" && next.Pin != ""
	a.pinAccount, a.rosterGen, a.seedGen = next.Pin, next.RosterGen, next.SeedGen
	a.rosterURLs, a.seedURLs = next.DialURLs, next.SeedURLs
	a.cachedRoster, a.cachedSeeds = next.Roster, next.Seeds
	a.rosterMu.Unlock()
	if pinned {
		a.cfg.Logger.Info("agent: roster account pinned (TOFU)", "account_pub", next.Pin)
	}
	a.persistRosterCache()
	return true
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
		if err := a.rosterCacheStore.Save(&rc); err != nil {
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
		// The wake SOURCE is logged with every refresh. Two sources exist — the jittered timer and the
		// broker's nats_topology_* nudge (rosterRefreshNow) — and an operator asking "why did this agent
		// not move to the new voter" needs to know which one ran, and which one was dropped: a nudge that
		// lands while a reconnect/rebuild is in flight is deliberately discarded below and its work only
		// happens on the next timer draw, up to a full interval later. Drill 41's fast-path arm reads
		// exactly this line to tell "the nudge was never delivered" from "it was delivered and dropped".
		// origin: simcluster-speed plan §5.3 I₁ (research R14: the interval is not compressible until
		// the two mechanisms are distinguishable in evidence).
		source := rosterWakeTimer
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-a.rosterRefreshNow:
			source = rosterWakeTopology
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		// Single-flight: a reconnect/rebuild's own register already refreshes the roster.
		if a.reconnectInFlight.Load() || a.rebuilding.Load() {
			reason := "reconnect_in_flight"
			if a.rebuilding.Load() {
				reason = "rebuilding"
			}
			next := jitterDur(iv)
			a.cfg.Logger.Info("agent: roster refresh wake dropped", "source", source, "reason", reason, "next_in", next)
			timer.Reset(next)
			continue
		}
		next := iv
		if a.refreshRosterOnce(ctx, nc, source) {
			silent = 0
		} else {
			silent++
			// #48: sustained silence on a healthy connection == a retired-broker island. If the
			// cached roster still knows another dialable voter, rebuild onto it (excluding the
			// silent broker from the immediate dial) instead of polling the island forever.
			escaped := rosterSilenceRequiresRebuild(a, nc, silent)
			// The observation describes the completed decision, so an immediate
			// next wake cannot mistake a queued escape for a protected miss.
			a.cfg.Logger.Debug("agent: roster refresh missed", "source", source, "consecutive", silent, "escaped", escaped)
			if escaped {
				return
			}
			next = a.rosterRefreshFailBackoff
		}
		timer.Reset(jitterDur(next))
	}
}

// refreshRosterOnce sends one roster-only register and adopts the returned roster. Returns false on a
// failed / empty pull (caller retries on the short backoff). Best-effort — never fatal.
// rosterWakeTimer / rosterWakeTopology name the two wake sources of rosterRefreshLoop in its log lines.
const (
	rosterWakeTimer    = "timer"
	rosterWakeTopology = "topology_event"
)

// refreshRosterOnce runs one roster-only register. source is which wake drove it (rosterWake*), carried
// into the outcome line so a refresh and the wake that caused it are one record, not two to join.
func (a *Agent) refreshRosterOnce(ctx context.Context, nc *nats.Conn, source string) bool {
	genBefore := a.cachedRosterGen()
	routeURL := nc.ConnectedUrl()
	req := proto.NodeRegisterReq{
		ServerID:                  nc.ConnectedServerName(),
		ProtoVersion:              proto.ProtoVersion,
		NID:                       nidOf(a),
		RosterGen:                 a.cachedRosterGen(),
		RosterRefreshOnly:         true,
		BrokerImageMaintenance:    brokermaint.Capability,
		BrokerImageMaintenanceKey: agentMaintenanceKey(a),
	}
	expected := agentIncarnation(a)
	payload, err := marshalAuthorityRegister(a, req)
	if err != nil {
		return false
	}
	reqCtx, cancel := context.WithTimeout(ctx, a.cfg.RegisterTimeout)
	msg, err := nc.RequestWithContext(reqCtx, proto.SubjNodeRegister(a.cfg.SID, nidOf(a)), payload)
	cancel()
	if err != nil {
		return false
	}
	var resp proto.NodeRegisterResp
	if json.Unmarshal(msg.Data, &resp) != nil || !resp.OK {
		return false
	}
	if err := checkRegisterAuthority(a, expected, msg.Data); err != nil {
		return false
	}
	rememberConnectedBroker(a, nc, req.ServerID, routeURL, resp)
	if resp.Roster != nil {
		a.rosterMu.Lock()
		previous := a.cachedRoster
		a.rosterMu.Unlock()
		accepted := a.adoptRoster(resp.Roster)
		// Updating only the NEXT dial pool is insufficient when this connection is still healthy
		// at the NATS layer but its broker has entered DRAINING/RETIRING. That leaves the agent on
		// a stale route island after the broker is removed. Trigger a single session rebuild only
		// after the signed roster was actually accepted (verified, non-rollback and persisted).
		rehome := accepted && rosterRequiresReconnect(previous, resp.Roster, nc.ConnectedUrl())
		if rehome {
			a.requestRosterReconnect()
		}
		a.cfg.Logger.Info("agent: roster refreshed", "source", source,
			"gen_before", genBefore, "gen_after", a.cachedRosterGen(), "accepted", accepted, "rehome", rehome)
		return true
	}
	a.cfg.Logger.Info("agent: roster refreshed", "source", source, "gen_before", genBefore, "gen_after", genBefore, "roster", "none")
	return true // a single/non-cluster broker (nil roster) is a successful no-op, not a retry
}

func rosterRequiresReconnect(previous, current *proto.ClusterRoster, connectedURL string) bool {
	return clusterroster.RosterRequiresReconnect(previous, current, connectedURL)
}

// nc dropped for the same reason as rebuildOntoVoter's (external review F3): nothing on this path
// may touch the *nats.Conn before the finalizer has cancelled.
func (a *Agent) requestRosterReconnect() {
	a.rebuildOntoVoter("agent: current broker is leaving the signed roster; rebuilding NATS session on a voter")
}

// rebuildOntoVoter tears the current session down so the Run loop re-dials the freshest pool. It
// single-flights via `rebuilding` (returns false if a rebuild is already in flight); the shared
// body for both the roster-says-leaving path (requestRosterReconnect) and the broker-silence path
// (rebuildOnBrokerSilence). reason is logged.
// origin: gotcha #72 — the SAME Close-before-cancel defect as fireRedial lived here (this path is
// reached from the roster-says-leaving and broker-silence callbacks, both of which run while the
// link may already be half-dead). Same fix, same reasons: bounded finalizer, cancel first.
// origin: external review F3 — `nc` is GONE from the signature on purpose. It existed only to call
// nc.ConnectedUrl() for the log line, and that observer takes the very mutex a wedged doReconnect
// holds. Keeping an unused *nats.Conn parameter here would be an open invitation to "just peek at
// the conn" again; the connected URL now comes from the lock-free healthy-period snapshot.
func (a *Agent) rebuildOntoVoter(reason string) bool {
	if !a.rebuilding.CompareAndSwap(false, true) {
		return false
	}
	a.rebuildRequested.Store(true)
	// origin: external review F3 (BLOCKER) — read the lock-free snapshot, never nc.ConnectedUrl():
	// that observer takes nc.mu.RLock, which a wedged doReconnect holds in write across a
	// no-deadline dial/handshake, so this log line alone could hang the whole teardown before
	// cancel-first ran. `nc` stays in the signature for the caller's identity checks only.
	a.cfg.Logger.Warn(reason, "connected_url", a.connectedURLSnapshot())
	// Same rollback as fireRedial (internal review CT-2 / S5): with neither a finalizer nor a
	// cancel there is no session to rebuild, and latched flags would disarm the successor.
	if finishSessionRebuild(a, a.loadSessionFinalizer()) {
		return true
	}
	a.cfg.Logger.Warn("agent: rebuild requested with no live session to tear down; rolled back the rebuild flags")
	return false
}

// rebuildOnBrokerSilence (#48) fires when the current broker has gone silent. It only acts when the
// cached roster still names ANOTHER dialable voter to fail over to — else the disconnect-armed
// redial watchdog (all brokers truly gone) or the seed floor is the right recovery, and churning
// here would be pointless. It stamps the silent broker's host as a one-shot dial exclusion so the
// rebuild lands on the survivor rather than re-sticking to the island. Returns true iff it rebuilt.
func (a *Agent) rebuildOnBrokerSilence(nc *nats.Conn) bool {
	// origin: hot-update S6 plan §7.3 / T44. A topology wake can spend the
	// third miss immediately; timer/backoff duration is not a safety proof.
	if agentupgradeport.InfraProtectsConnection(nc) || agentMaintenanceProtects(a, nc) {
		return false
	}
	cu, err := url.Parse(nc.ConnectedUrl())
	if err != nil || cu.Hostname() == "" {
		return false
	}
	host := cu.Hostname()
	avoid, other := silenceVoterDestination(a, nc, host)
	if !other {
		return false
	}
	a.setAvoidHost(avoid)
	if !a.rebuildOntoVoter("agent: current broker went silent (no roster reply — retired-broker island); rebuilding onto a known voter") {
		return false // a rebuild is already in flight; the avoid hint is consumed next connect
	}
	fireAfterSilenceEscape(a, host)
	return true
}

// rosterRouteIdentity is bound to a particular connection AND its current
// server. The same *nats.Conn can reconnect to another server without a new
// agent session, so pointer identity alone cannot fence a delayed reply.
type rosterRouteIdentity struct {
	maintenanceOrder                                       uint64
	order                                                  uint64
	nc                                                     *nats.Conn
	serverID, url, brokerID                                string
	disabled                                               bool
	ingressServerID, actualBrokerID, maintenanceCapability string
	healthGeneration                                       uint64
	aliases                                                []string
}

func rememberConnectedBroker(a *Agent, nc *nats.Conn, serverID, connectedURL string, resp proto.NodeRegisterResp) {
	order := a.rosterRouteOrder.Add(1)
	if !resp.OK || connectedURL == "" || nc.ConnectedServerName() != serverID || nc.ConnectedUrl() != connectedURL {
		return
	}
	if current := a.ncBox.Load(); current != nil && current != nc {
		return
	}
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	if current := a.ncBox.Load(); current != nil && current != nc {
		return
	}
	// A same-*Conn reconnect can complete another reply while this one waits
	// for rosterMu. Never overwrite that newer proof with the older route.
	if old := a.rosterRoute; old != nil && old.order > order {
		return
	}
	id := resp.ConnectedBrokerID
	var aliases []string
	if old := a.rosterRoute; old != nil && old.nc == nc && old.serverID == serverID && old.url == connectedURL {
		if id == "" {
			id = old.brokerID // N-1 reply cannot erase a previously proven ingress
		}
		if id == old.brokerID {
			aliases = append(aliases, old.aliases...)
		}
	}
	id, aliases = upgradeauthority.RosterAuthority(upgradeauthority.RosterConsumer{Current: rosterCurrentAt,
		Unique: clusterroster.UniqueRosterNodeAtHost, Aliases: clusterroster.AppendRosterAliases}, a.cachedRoster, a.agentNow(), connectedURL, id, aliases)
	maintenanceOrder := order
	if old := a.rosterRoute; old != nil && old.nc == nc && old.serverID == serverID && old.url == connectedURL &&
		old.ingressServerID == nc.ConnectedServerId() && old.actualBrokerID == resp.ConnectedBrokerID &&
		old.healthGeneration == a.ptyHealthGeneration.Load() && old.maintenanceCapability == resp.BrokerImageMaintenance {
		maintenanceOrder = old.maintenanceOrder
	}
	a.rosterRoute = &rosterRouteIdentity{maintenanceOrder: maintenanceOrder, order: order, nc: nc, serverID: serverID, url: connectedURL,
		brokerID: id, disabled: resp.Roster == nil, aliases: aliases,
		ingressServerID: nc.ConnectedServerId(), actualBrokerID: resp.ConnectedBrokerID,
		maintenanceCapability: resp.BrokerImageMaintenance, healthGeneration: a.ptyHealthGeneration.Load()}
}

// uniqueRosterNodeAtHost prevents a shared hostname from becoming a false
// persistent ingress identity when an N-1 broker omits connected_broker_id.

func silenceVoterDestination(a *Agent, nc *nats.Conn, host string) (string, bool) {
	serverID, connectedURL := nc.ConnectedServerName(), nc.ConnectedUrl()
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	var id string
	var aliases []string
	if route := a.rosterRoute; route != nil && route.nc == nc && route.serverID == serverID && route.url == connectedURL {
		if route.disabled {
			return "", false
		}
		id, aliases = route.brokerID, route.aliases
	}
	return rosterVoterDestination(a.cachedRoster, host, id, aliases, a.agentNow())
}

func rosterCurrentAt(r *proto.ClusterRoster, now time.Time) bool {
	return clusterroster.RosterCurrentAt(r, now)
}

func rosterVoterDestination(r *proto.ClusterRoster, host, id string, aliases []string, now time.Time) (string, bool) {
	return clusterroster.RosterVoterDestination(r, host, id, aliases, now)
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

func excludeHostFromDial(csv, host string) string {
	return clusterroster.ExcludeHostFromDial(csv, host)
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
// return rebuild=true, and hands the dying session to its BOUNDED FINALIZER, which cancels the
// session ctx FIRST and only then runs a budgeted close.
//
// origin: gotcha #72 — ORDER IS THE FIX. This used to call nc.Close() synchronously and cancel the
// session ctx only afterwards; nats.go's Close takes the same nc.mu that doReconnect holds across a
// no-deadline DNS lookup / TLS / ws handshake, so on a half-dead link the cancel this watchdog
// exists to deliver sat behind a block that lasted 10m58s in the field. Now: pin the dying session's
// finalizer (never re-read ncBox — a late close must not target the successor), then let the
// finalizer cancel FIRST and run the close under a budget with poisoning + escalation behind it.
func (a *Agent) fireRedial() {
	if !a.rebuilding.CompareAndSwap(false, true) {
		return
	}
	a.rebuildRequested.Store(true)
	a.cfg.Logger.Warn("agent: NATS stuck-disconnected; rebuilding session on the freshest roster")
	// No finalizer published (a rebuild fired before this session installed one): fall back to the
	// cancel alone. NEVER a bare nc.Close() here — that is the unbounded call this fix removed.
	// origin: internal review CT-2 (S5) — NOTHING to tear down: a stale timer fired inside the
	// successor's connect window, before that session published either a finalizer or a cancel.
	// Leaving `rebuilding`/`rebuildRequested` latched would silently disarm the NEXT session
	// (onNATSReconnect early-returns while rebuilding, the roster refresh loop skips, the redial
	// watchdog self-cancels) and would make a later clean shutdown read as a rebuild. Roll back.
	if finishSessionRebuild(a, a.loadSessionFinalizer()) {
		return
	}
	a.cfg.Logger.Warn("agent: stale redial fired with no live session to tear down; rolled back the rebuild flags")
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

// agentMaintenanceHold retains the challenge even after expiry/revocation. A
// retransmission of the same cut must not obtain another local clock origin.
// origin: hot-update S6 plan §7.1 / §7.3 / T44.
type agentMaintenanceHold struct {
	record      brokermaint.Record
	routeOrder  uint64
	routeDigest string
	nonce       string
	startedNS   int64
	expiresNS   int64
	installed   bool
	ended       bool
}

const agentMaintenanceTTL = int64(8 * time.Second)

func agentMaintenanceBootNow(a *Agent) (int64, error) {
	if a.maintenanceNow != nil {
		return a.maintenanceNow()
	}
	return procrequest.BootNow()
}

func agentMaintenanceSignedBytes(value any) ([]byte, error) {
	return upgradeauthority.MaintenanceSignedBytes(value)
}

func verifyAgentMaintenance(pub, signature string, value any) error {
	return upgradeauthority.VerifyMaintenance(pub, signature, value, auth.VerifySignature)
}

// Signing stays inside the install critical section: Accepted is a receipt for
// an installed hold with the remaining lifetime measured AFTER installation.
func signAgentMaintenanceAck(a *Agent, ack proto.BrokerMaintenanceAck) (proto.BrokerMaintenanceAck, error) {
	body, err := agentMaintenanceSignedBytes(ack)
	if err != nil {
		return proto.BrokerMaintenanceAck{}, err
	}
	sig, err := auth.SignWithSeed(a.cfg.Identity.Seed, body)
	if err != nil {
		return proto.BrokerMaintenanceAck{}, err
	}
	ack.Sig = base64.RawStdEncoding.EncodeToString(sig)
	return ack, nil
}

func validAgentMaintenanceRecord(r brokermaint.Record) bool {
	return brokermaint.ValidateRecord(r) == nil
}

func sameAgentMaintenanceCut(left, right brokermaint.Record) bool {
	left.Phase, right.Phase = brokermaint.MaintPrepared, brokermaint.MaintPrepared
	return left == right
}

// The digest includes local order and reconnect generation. The broker learns
// it from the signed challenge ACK and echoes it; no hostname fallback is used.
func agentMaintenanceRouteDigest(route *rosterRouteIdentity) string {
	return agentMaintenanceDigest(struct {
		Order            uint64 `json:"order"`
		HealthGeneration uint64 `json:"health_generation"`
		BrokerID         string `json:"connected_broker_id"`
		IngressServerID  string `json:"ingress_server_id"`
		URL              string `json:"url"`
	}{route.maintenanceOrder, route.healthGeneration, route.actualBrokerID, route.ingressServerID, route.url})
}

func agentMaintenanceRouteMatches(a *Agent, nc *nats.Conn, route *rosterRouteIdentity, serverID, ingressID, connectedURL string) bool {
	return route != nil && a.ncBox.Load() == nc && route.nc == nc && route.serverID == serverID &&
		route.ingressServerID == ingressID && ingressID != "" && route.url == connectedURL &&
		route.healthGeneration == a.ptyHealthGeneration.Load() && route.maintenanceCapability == brokermaint.Capability
}

func acceptAgentMaintenance(a *Agent, nc *nats.Conn, req proto.BrokerMaintenanceReq) (proto.BrokerMaintenanceAck, error) {
	if nc == nil || a.cfg.Identity == nil || !validAgentMaintenanceRecord(req.Record) || req.NID != nidOf(a) {
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: unsupported observer or invalid record")
	}
	sig := req.Sig
	req.Sig = ""
	if err := verifyAgentMaintenance(a.pinnedAccountPub(), sig, req); err != nil {
		return proto.BrokerMaintenanceAck{}, err
	}
	now, err := agentMaintenanceBootNow(a)
	if err != nil || now <= 0 || now > math.MaxInt64-agentMaintenanceTTL {
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: clock unavailable")
	}
	serverID, ingressID, connectedURL := nc.ConnectedServerName(), nc.ConnectedServerId(), nc.ConnectedUrl()
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	route := a.rosterRoute
	if !agentMaintenanceRouteMatches(a, nc, route, serverID, ingressID, connectedURL) ||
		req.ConnectedBrokerID != route.actualBrokerID ||
		(req.Record.Mode == proto.BrokerImageModeSingle && req.IngressServerID != ingressID) ||
		(req.Record.Mode != proto.BrokerImageModeSingle && req.IngressServerID != "") {
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: route changed or unsupported")
	}
	// origin: hot-update S6 plan §7.3 / T44. Cluster identity must come from
	// the actual register reply; single uses the live external NATS server ID.
	if req.Record.Mode == proto.BrokerImageModeSingle {
		if route.actualBrokerID != "" || req.Record.IngressServerID != ingressID || !route.disabled {
			return proto.BrokerMaintenanceAck{}, errors.New("maintenance: single ingress mismatch")
		}
	} else if route.disabled || route.actualBrokerID == "" || req.Record.Home != route.actualBrokerID {
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: home mismatch")
	}
	digest := agentMaintenanceRouteDigest(route)
	hold := a.brokerMaintenance
	if hold != nil && hold.record.CutID == req.Record.CutID {
		if !sameAgentMaintenanceCut(hold.record, req.Record) || hold.routeOrder != route.maintenanceOrder || hold.routeDigest != digest || hold.ended || req.Record.Phase < hold.record.Phase {
			return proto.BrokerMaintenanceAck{}, errors.New("maintenance: cut identity changed or ended")
		}
	} else {
		if req.Proof != (brokermaint.Proof{}) || req.Record.Phase != brokermaint.MaintPrepared ||
			(hold != nil && !hold.ended && now < hold.expiresNS) || (req.RouteDigest != "" && req.RouteDigest != digest) {
			return proto.BrokerMaintenanceAck{}, errors.New("maintenance: challenge required")
		}
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return proto.BrokerMaintenanceAck{}, err
		}
		// Local t0 is captured BEFORE a proof can be issued, never on reply.
		hold = &agentMaintenanceHold{record: req.Record, routeOrder: route.maintenanceOrder, routeDigest: digest,
			nonce: hex.EncodeToString(nonce[:]), startedNS: now, expiresNS: now + (req.Record.ExpiresBootNS - req.Record.IssuedBootNS)}
		a.brokerMaintenance = hold
	}
	ack := proto.BrokerMaintenanceAck{CutID: req.Record.CutID, Nonce: hold.nonce, NID: req.NID,
		ConnectedBrokerID: route.actualBrokerID, IngressServerID: req.IngressServerID, InstanceID: req.Record.InstanceID, RouteDigest: digest}
	if req.Proof == (brokermaint.Proof{}) {
		if req.Record.Phase != brokermaint.MaintPrepared || now >= hold.expiresNS {
			return proto.BrokerMaintenanceAck{}, errors.New("maintenance: challenge expired")
		}
		return signAgentMaintenanceAck(a, ack) // Accepted=false: this is only a challenge
	}
	proof := req.Proof
	proofSig := proof.Sig
	proof.Sig = ""
	if err := verifyAgentMaintenance(a.pinAccount, proofSig, proof); err != nil {
		return proto.BrokerMaintenanceAck{}, err
	}
	recordDigest, digestErr := brokermaint.RecordDigest(req.Record)
	if digestErr != nil || req.RouteDigest != digest || proof.Nonce != hold.nonce || proof.CutID != req.Record.CutID ||
		proof.IssuerBootID != req.Record.IssuerBootID || proof.RecordDigest != recordDigest ||
		proof.ExpiresBootNS != req.Record.ExpiresBootNS || proof.ResponseBootNS < req.Record.IssuedBootNS ||
		proof.ResponseBootNS >= proof.ExpiresBootNS || (req.Record.Mode != proto.BrokerImageModeSingle && proof.AppliedIndex == 0) {
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: proof mismatch")
	}
	// origin: hot-update S6 plan §7.1 / T44. Foreign BOOTTIME is a duration,
	// not a local timestamp. Reproofs can only shorten the original deadline.
	expiry := hold.startedNS + (proof.ExpiresBootNS - proof.ResponseBootNS)
	if expiry < hold.expiresNS {
		hold.expiresNS = expiry
	}
	if now >= hold.expiresNS || req.Record.Phase == brokermaint.MaintEnded {
		hold.installed, hold.ended = false, true
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: expired or ended")
	}
	hold.record = req.Record
	hold.installed = true
	ack.ProofDigest, err = brokermaint.ProofDigest(req.Proof)
	if err != nil {
		hold.installed = false
		return proto.BrokerMaintenanceAck{}, err
	}
	// Re-read local time after installation/signature verification, so ACK
	// cannot claim the lifetime consumed while this callback was queued.
	ackNow, err := agentMaintenanceBootNow(a)
	if err != nil || ackNow < now || ackNow >= hold.expiresNS {
		hold.installed = false
		return proto.BrokerMaintenanceAck{}, errors.New("maintenance: expired before acknowledgement")
	}
	ack.Accepted, ack.RemainingNS = true, hold.expiresNS-ackNow
	signed, err := signAgentMaintenanceAck(a, ack)
	if err != nil {
		hold.installed = false
	}
	return signed, err
}

func invalidateAgentMaintenance(a *Agent) {
	a.rosterMu.Lock()
	if hold := a.brokerMaintenance; hold != nil {
		hold.installed, hold.ended = false, true
	}
	a.rosterMu.Unlock()
}

func agentMaintenanceProtects(a *Agent, nc *nats.Conn) bool {
	now, err := agentMaintenanceBootNow(a)
	if err != nil || nc == nil {
		return false
	}
	serverID, ingressID, connectedURL := nc.ConnectedServerName(), nc.ConnectedServerId(), nc.ConnectedUrl()
	a.rosterMu.Lock()
	defer a.rosterMu.Unlock()
	hold, route := a.brokerMaintenance, a.rosterRoute
	return hold != nil && hold.installed && !hold.ended && now >= hold.startedNS && now < hold.expiresNS &&
		agentMaintenanceRouteMatches(a, nc, route, serverID, ingressID, connectedURL) &&
		hold.routeOrder == route.maintenanceOrder && hold.routeDigest == agentMaintenanceRouteDigest(route)
}

// Maintenance runs inline under the existing subscription's drain receipt.
// Ordinary mutators retain their original dispatch and authorization paths.
func dispatchAgentMaintenance(a *Agent, nc *nats.Conn, msg *nats.Msg) bool {
	if msg.Subject == proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.BrokerMaintenanceVerb) {
		ctx := a.loadRunCtx()
		if ctx == nil || ctx.Err() != nil || msg.Reply == "" || len(msg.Data) > 32*1024 {
			return true
		}
		var req proto.BrokerMaintenanceReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return true
		}
		ack, err := acceptAgentMaintenance(a, nc, req)
		if err != nil {
			return true
		} // never turn refusal into an installation ACK
		body, err := json.Marshal(ack)
		if err == nil {
			_ = msg.Respond(body)
		}
		return true
	}
	// Ordinary pushes are not revocation evidence. Their decoded handlers and
	// fresh route adoption decide whether authority actually changed.
	// origin: hot-update S6 internal review rv-cluster-5
	return false
}

// This is the decision shared by timer and immediate topology wakes; the
// failure count is retained while maintenance protects the matching route.
func rosterSilenceRequiresRebuild(a *Agent, nc *nats.Conn, silent int) bool {
	return silent >= maxSilentRosterRefreshes && a.rebuildOnBrokerSilence(nc)
}

// A decoded removal only preempts the hold when it names a currently owned
// allocation. Malformed, unrelated and stale broadcasts are not revocations.
// origin: hot-update S6 internal review rv-cluster-5
func invalidateMaintenanceForPort(a *Agent, name string, port int) {
	if port <= 0 || name == "" {
		return
	}
	sf, ok := a.loadStateBounded("maintenance authority change")
	if !ok || sf == nil {
		return
	}
	for _, owned := range sf.PortTokens {
		if owned.Name == name && owned.Port == port {
			invalidateAgentMaintenance(a)
			return
		}
	}
	if sf.Proxy != nil && name == proxyTokenName && sf.Proxy.PublicPort == port {
		invalidateAgentMaintenance(a)
	}
}

func invalidateMaintenanceForHome(a *Agent, d proto.HomeDirective) {
	sf, ok := a.loadStateBounded("maintenance home change")
	if !ok || sf == nil {
		return
	}
	for _, owned := range sf.PortTokens {
		if owned.Name == d.Name && owned.Port == d.PublicPort && d.Epoch >= owned.Epoch &&
			(d.Epoch > owned.Epoch || d.BrokerAddr != owned.HomeBrokerAddr) {
			invalidateAgentMaintenance(a)
			return
		}
	}
}

func agentMaintenanceKey(a *Agent) string {
	if a.cfg.Identity == nil {
		return ""
	}
	return a.cfg.Identity.PublicKey
}

func probeAgentControl(ctx context.Context, a *Agent, nc *nats.Conn, nonce string) error {
	return agentupgrade.ProbeControl(ctx, agentControlPort(a), nc, nonce)
}

func defaultAgentDuration(value, fallback time.Duration) time.Duration {
	return agentupgrade.DefaultDuration(value, fallback)
}

func newRosterCacheStore(home, sid string) *rosterCacheStore {
	return agentstate.NewCacheView(filepath.Join(home, "agent", sid, "roster_cache.json"), &sync.Mutex{})
}
func ReadRosterCache(home, sid string) (*RosterCache, error) {
	return agentstate.ReadRosterCache(home, sid)
}
func WriteRosterCache(home, sid string, rc *RosterCache) error {
	return agentstate.WriteRosterCache(home, sid, rc)
}

func agentControlPort(a *Agent) agentupgrade.ControlPort {
	return agentupgrade.ControlPort{InstanceID: a.instanceID, SID: a.cfg.SID, NID: nidOf(a), Current: &a.ncBox,
		Generation: &a.sessionGen, Dependencies: a.cfg.Upgrade, Anonymous: a.cfg.Identity == nil, Heartbeat: func() []byte { return heartbeatPayload(a, time.Now()) }}
}

func bindAgentInfra(a *Agent, nc *nats.Conn, fin *sessionFinalizer) error {
	return agentupgradeport.BindInfraControl(nc, a.cfg.SID, func() string { return nidOf(a) }, fin.addBoundedCleanup)
}
