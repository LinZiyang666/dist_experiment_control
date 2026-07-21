package authcallout

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ratelimit.go — per-client-IP throttle on PIN-bootstrap brute force
// (architecture E.6: "PIN 暴力: 每 broker、每 IP、每分钟 ≤ 10 次; 不做账户锁定" —
// see the CLUSTER SEMANTICS note below on the per-broker qualifier).
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
}

func newPinRateLimiter() *pinRateLimiter {
	return &pinRateLimiter{ips: make(map[string]*pinLimiterEntry)}
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

// recordFailure consumes one unit of ip's budget for a genuine failed PIN
// verify (the Argon2-reject branch). When the bucket is already empty AllowN is
// a no-op (tokens never go negative), which is correct: the source is blocked
// already.
func (p *pinRateLimiter) recordFailure(ip string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entry(ip, now).AllowN(now, 1)
}
