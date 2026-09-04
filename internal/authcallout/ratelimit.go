package authcallout

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ratelimit.go — TWO ceilings on PIN-bootstrap work, answering two different
// questions:
//
//   - PER-CLIENT-IP (architecture E.6: "PIN 暴力: 每 broker、每 IP、每分钟 ≤ 10 次;
//     不做账户锁定" — see the CLUSTER SEMANTICS note below on the per-broker
//     qualifier): "is this source brute-forcing?" Counts FAILURES only.
//   - PER-PROCESS (pinGlobalPerSecond, added by the prerelease audit): "is this
//     broker about to spend 0.15-0.3s of its single serialized callout goroutine?"
//     Counts every verify, correct or not, and holds regardless of how many source
//     addresses the caller controls. Without it the per-IP bucket multiplies by K
//     for an attacker with K addresses, and the serialized callout turns that
//     straight into a fleet-wide authentication black hole (L1-F3).
//
// TRUST BOUNDARY of the per-IP key (client_info.host):
//
//	The IP is taken from jwt.AuthorizationRequestClaims.ClientInformation.Host,
//	which nats-server fills in from the TCP peer address of the connecting
//	client — it is NOT self-reported by the client and cannot be spoofed at the
//	auth_callout layer (the client only controls its nkey + CONNECT name/token).
//	The one caveat is a shared egress: multiple clients behind the SAME NAT or a
//	reverse proxy all present the proxy's address, so a brute-forcer sharing an
//	egress with a legitimate joiner throttles that joiner's NEW joins too. This
//	is an INTENTIONALLY accepted v1 trade-off — we would rather over-throttle a
//	shared-NAT brute-force source than let it through (better a rare false
//	positive on a first-join than an open PIN oracle). Already-provisioned
//	members/agents are unaffected: they never re-enter the PIN path, so a
//	shared-NAT attacker cannot knock existing sessions offline (see
//	handler.go ensureMember / ensureAgentProvisioned — the throttle guards only
//	the not-yet-a-member PIN-verify branch).
//
// DESIGN: single-broker, in-memory only — NO distributed state, NO new
// dependency (golang.org/x/time/rate was already vendored). A token bucket per
// IP (burst=10, refill 10/min) realises "≤10 attempts / IP / minute" as a rate
// limit rather than a permanent lockout (E.6 explicitly does "not do account
// lockout"). Idle IPs are swept lazily (no background goroutine — the repo's
// leak gate is strict) so the map cannot grow unbounded.
//
// CLUSTER SEMANTICS (external review H2, adjudicated): the budget is PER-BROKER.
// auth_callout is queue-distributed across all brokers (§6.2), so one source IP's
// attempts land on different brokers' independent buckets and an N-broker cluster
// exposes ≈ N×10/min. This is an ACCEPTED v1 trade-off, not an oversight:
//   - the PRIMARY brute-force defense is the memory-hard argon2id verify (64 MiB,
//     t=3 — internal/auth/pin.go), so throughput is bounded by memory bandwidth
//     regardless of the counter; this limiter is a secondary speed bump;
//   - PINs are ASCII-printable with no length cap (auth.ValidPIN), so a non-trivial
//     PIN is astronomically safe even at N×10/min;
//   - a cluster-consistent counter would put a distributed write (raft/JS/leader RPC)
//     on the UNAUTHENTICATED connect path — a strictly worse DoS amplification surface.
//
// architecture.md §E.6 documents the honest per-broker contract. If a future batch
// needs cluster-wide enforcement, best-effort failure gossip (a fire-and-forget pub
// per failed attempt, decision stays local) is the intended low-DoS mechanism.
const (
	// pinAttemptsPerMinute is the E.6 ceiling: 10 failed PIN attempts per IP per
	// minute before that source is refused (a correct PIN included) until the
	// bucket refills.
	pinAttemptsPerMinute = 10

	// pinLimiterIdleTTL drops an IP's bucket once it has been quiet this long, so
	// a scan of many distinct source IPs cannot pin unbounded memory.
	pinLimiterIdleTTL = 10 * time.Minute

	// pinLimiterSweepEvery bounds how often the lazy idle-sweep walks the map (at
	// most once per access-after-interval), keeping the common path O(1).
	pinLimiterSweepEvery = time.Minute

	// pinGlobalPerSecond / pinGlobalBurst bound argon2id work for the WHOLE broker,
	// independently of how many source addresses ask for it.
	//
	// origin: prerelease audit proto-auth-acl/L1-F3. The per-IP bucket above is the
	// E.6 anti-brute-force control and it is the right control for THAT threat, but
	// it is not a bound on cost: an attacker with K source addresses buys K times the
	// argon2 budget. That matters here in a way it would not elsewhere, because the
	// verify does NOT run on a worker — nats.go delivers one subscription on ONE
	// goroutine, so every auth_callout decision for the entire broker is serialized
	// behind it. Measured: 0.15-0.3s per verify (m=64 MiB, t=3) against
	// nats-server's AUTH_TIMEOUT of 2s, so roughly 7-13 queued PIN attempts are
	// enough to time out the CONNECT of every honest client behind them — agents
	// booting, ctl commands, everything.
	//
	// One per second with a burst of 10 keeps a real first-join instant (a fleet
	// bootstraps a handful of agents, not thousands) while pinning the worst-case
	// duty cycle at roughly 15-30%, no matter how many addresses the attacker owns.
	//
	// THE COST, stated plainly because it is real: while the global bucket is empty a
	// LEGITIMATE first-time join is refused too, and it must retry. That is strictly
	// better than the alternative it replaces — today there is no ceiling at all, so
	// the failure mode is every client on the broker failing to authenticate.
	// Already-provisioned members and agents are untouched: they take the no-PIN
	// branches in ensureMember / ensureAgentProvisioned and never reach this limiter.
	pinGlobalDefaultPerSecond = 1
	pinGlobalDefaultBurst     = 10
)

// pinGlobalPerSecond / pinGlobalBurst are vars, not consts, ONLY so that unit tests
// of the PER-IP bucket can raise the process-wide ceiling out of their way and keep
// measuring the one mechanism they were written for. Production never reassigns
// them. Same precedent, and the same reason, as closeBudget / poisonGrace in
// internal/agent/conn_teardown.go.
//
// Two ceilings in one code path means a test that does not neutralise one of them is
// silently measuring both; three existing per-IP tests started failing for exactly
// that reason when the process-wide ceiling was introduced, and "advance the clock"
// was the wrong repair because it refills the per-IP bucket too.
var (
	pinGlobalPerSecond = pinGlobalDefaultPerSecond
	pinGlobalBurst     = pinGlobalDefaultBurst
)

type pinLimiterEntry struct {
	lim  *rate.Limiter
	seen time.Time
}

// pinRateLimiter is a concurrency-safe map of per-IP token buckets. All time is
// caller-supplied (now) so the gate is fully testable with a fake clock and so
// the whole handler shares one monotone view of time.
type pinRateLimiter struct {
	mu        sync.Mutex
	ips       map[string]*pinLimiterEntry
	lastSweep time.Time
	// global bounds argon2id work for the whole process regardless of source
	// address count — see pinGlobalPerSecond. It is consulted and consumed
	// alongside the per-IP bucket, never instead of it: the per-IP bucket is the
	// anti-brute-force control, this one is the cost ceiling.
	global *rate.Limiter
}

func newPinRateLimiter() *pinRateLimiter {
	return &pinRateLimiter{
		ips:    make(map[string]*pinLimiterEntry),
		global: rate.NewLimiter(rate.Limit(pinGlobalPerSecond), pinGlobalBurst),
	}
}

// entry returns (creating if absent) the bucket for ip and lazily sweeps idle
// buckets. Caller holds p.mu.
func (p *pinRateLimiter) entry(ip string, now time.Time) *rate.Limiter {
	if p.lastSweep.IsZero() {
		p.lastSweep = now
	}
	if now.Sub(p.lastSweep) >= pinLimiterSweepEvery {
		for k, e := range p.ips {
			if now.Sub(e.seen) >= pinLimiterIdleTTL {
				delete(p.ips, k)
			}
		}
		p.lastSweep = now
	}
	e := p.ips[ip]
	if e == nil {
		e = &pinLimiterEntry{
			// rate.Every(6s) with burst 10 == a sustained 10/min with a 10-attempt
			// burst allowance — the E.6 rate limit, not a lockout.
			lim: rate.NewLimiter(rate.Every(time.Minute/pinAttemptsPerMinute), pinAttemptsPerMinute),
		}
		p.ips[ip] = e
	}
	e.seen = now
	return e.lim
}

// blocked reports whether ip has exhausted its PIN-attempt budget. It is a PURE
// READ (does NOT consume a token) so that a correct-PIN connect arriving while
// the source is blocked is REFUSED without itself counting as an attempt (and
// without extending the block).
func (p *pinRateLimiter) blocked(ip string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.entry(ip, now).TokensAt(now) < 1
}

// globalBlocked reports whether the PROCESS-wide argon2 budget is exhausted. A pure
// read (TokensAt does not consume), like blocked, so a correct PIN arriving while
// the process is out of budget is refused without itself counting as an attempt.
func (p *pinRateLimiter) globalBlocked(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.global != nil && p.global.TokensAt(now) < 1
}

// recordFailure consumes one unit of ip's budget for a genuine failed PIN
// verify (the Argon2-reject branch). When the bucket is already empty AllowN is
// a no-op (tokens never go negative), which is correct: the source is blocked
// already.
//
// It charges ONLY the per-IP bucket. The process-wide budget is charged exactly
// once per verify by spendGlobal, at the call site, whatever the outcome —
// charging it here as well would bill a failed attempt twice and make the ceiling
// half of what it says it is.
func (p *pinRateLimiter) recordFailure(ip string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entry(ip, now).AllowN(now, 1)
}

// spendGlobal IS GONE, and tryTakeGlobal below replaced it — origin: prerelease audit
// external review R2-M1. Its doc comment was correct about WHAT to charge (a correct PIN
// costs the same CPU as a wrong one, so the ceiling must bill both) and that reasoning
// survives verbatim in tryTakeGlobal. What did not survive is the SHAPE: a spend with no
// return value, in its own critical section, downstream of a separate globalBlocked read.
// `AllowN`'s false was discarded, so the write half could not refuse anybody even in
// principle, and 64 concurrent callers all passed a burst of 1.
//
// Note also what the unused-code linter caught here: deleting the last caller left this
// method alive and plausible-looking, one `go get` away from being wired back in by
// somebody who read only its comment. A deleted function cannot be re-adopted by accident.

// tryTakeGlobal ATOMICALLY checks and consumes one unit of the process-wide argon2 budget.
// It returns true iff this caller now owns the slot and may run Argon2.
//
// origin: prerelease audit external review R2-M1. The budget used to be a `globalBlocked`
// read followed, after the lock was released, by a separate `spendGlobal` write. With
// burst=1 and 64 concurrent callers every one of them observed the same last token before
// any of them consumed it, and all 64 entered Argon2 — the ceiling bounded nothing under
// exactly the concurrency it exists for. `AllowN`'s own false return was discarded too, so
// the write half could not have refused anybody even if it had been consulted.
//
// One critical section, one AllowN, one bool. A caller that gets false has not spent
// anything, which is what makes the refusal cheap enough to be the answer to a flood.
func (p *pinRateLimiter) tryTakeGlobal(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.global == nil {
		return true
	}
	return p.global.AllowN(now, 1)
}
