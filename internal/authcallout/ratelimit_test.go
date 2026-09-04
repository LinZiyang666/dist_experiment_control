package authcallout

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// ratelimit_test.go — P7/#25 the E.6 PIN brute-force throttle (≤10 failed
// attempts / IP / minute → that source refused, a CORRECT pin included).
//
// MUTATION VERIFICATION (run manually to prove the test can go RED):
//   - Delete the `if h.pinRateLimited(clientIP) { return ErrPINRateLimited }`
//     guard in ensureMember/ensureAgentProvisioned → the 11th same-IP CORRECT
//     pin is ALLOWED again (exactly the reported bug) → the "…11th correct pin
//     is REFUSED" assertions below fail.
//   - Delete the `h.recordPINFailure(clientIP)` call in the ErrInvalidPIN branch
//     → the budget never depletes → same assertions fail.
//   - Change the per-IP key to a constant → TestPINRateLimitMultipleIPsDispersed
//     fails (dispersed IPs would falsely block).
// (Verified during development; see the R12 completion notes.)

// fixedClock returns a Now func pinned to a constant instant. With a fixed clock
// the token bucket never refills between attempts, so a run of failures is
// deterministic (the bucket only leaks as wall time advances — exercised by
// TestPINRateLimitRefillUnblocks with an explicit clock bump).
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// pinReqFrom builds a signed auth request carrying a client_info.host (the
// server-stamped TCP peer address the throttle keys on).
func pinReqFrom(t *testing.T, clientNkey, name, token, host string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()

	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = freshUserPub(t)
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: name, Nkey: clientNkey, Token: token}
	rc.ClientInformation = jwt.ClientInformation{Host: host}
	signNonceInto(t, rc, clientNkey)
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// denyReason decodes a handler response and returns its .Error ("" = allowed).
func denyReason(t *testing.T, respJWT string) string {
	t.Helper()
	resp, err := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Error
}

func freshClientPub(t *testing.T) string {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	testUserKeys.Store(pub, kp)
	return pub
}

// TestPINRateLimitSingleIPHighFrequency — the headline case: 10 wrong PINs from
// one IP exhaust the budget; the 11th connect from that IP is refused EVEN WITH
// THE CORRECT PIN, while a first connect from a different IP still succeeds.
func TestPINRateLimitSingleIPHighFrequency(t *testing.T) {
	h, _ := freshHandler(t)
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	const badIP = "203.0.113.7"
	// 10 wrong-PIN attempts from badIP — each rejected as invalid PIN, budget depletes.
	for i := 0; i < pinAttemptsPerMinute; i++ {
		reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", badIP)))
		if !strings.Contains(reason, "invalid PIN") {
			t.Fatalf("attempt %d: want invalid-PIN denial, got %q", i+1, reason)
		}
	}
	// 11th connect from badIP WITH THE CORRECT PIN must now be REFUSED (rate limited).
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", badIP)))
	if !strings.Contains(reason, "rate_limited") {
		t.Fatalf("11th correct-PIN connect from a blocked IP must be rate-limited, got %q", reason)
	}
	// A first connect from a DIFFERENT IP with the correct PIN still joins (per-IP,
	// not global — a brute-forcer must not lock the whole broker out).
	reason = denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", "198.51.100.9")))
	if reason != "" {
		t.Fatalf("a clean IP's correct-PIN join must succeed, got %q", reason)
	}
}

// TestPINRateLimitUnderThresholdDoesNotBlock — 9 failures (below the 10 ceiling)
// must NOT block the source: the next correct PIN joins. No false positives.
func TestPINRateLimitUnderThresholdDoesNotBlock(t *testing.T) {
	h, _ := freshHandler(t)
	// origin: prerelease audit round 2, A-F4. relaxGlobalPINBudget is what makes this a
	// PER-IP guard rather than a "something refused it" guard. Both budgets deny with the
	// same ErrPINRateLimited text — deliberately, so a caller cannot probe which ceiling
	// it hit — so with the process-wide bucket at its real size this assertion is
	// satisfied by EITHER, and deleting the per-IP limiter outright leaves it green.
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	const ip = "203.0.113.20"
	for i := 0; i < pinAttemptsPerMinute-1; i++ { // 9 failures
		_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ip))
	}
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", ip)))
	if reason != "" {
		t.Fatalf("under threshold (9 failures) a correct PIN must still join, got %q", reason)
	}
}

// TestPINRateLimitAgentRole — the agent PIN-bootstrap path is throttled by the
// SAME per-IP budget (the brute-force surface is identical).
func TestPINRateLimitAgentRole(t *testing.T) {
	h, _ := freshHandler(t)
	// origin: prerelease audit round 2, A-F4. relaxGlobalPINBudget is what makes this a
	// PER-IP guard rather than a "something refused it" guard. Both budgets deny with the
	// same ErrPINRateLimited text — deliberately, so a caller cannot probe which ceiling
	// it hit — so with the process-wide bucket at its real size this assertion is
	// satisfied by EITHER, and deleting the per-IP limiter outright leaves it green.
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	const ip = "203.0.113.30"
	for i := 0; i < pinAttemptsPerMinute; i++ {
		reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-agent:lab:node-x", "nope", ip)))
		if !strings.Contains(reason, "invalid PIN") {
			t.Fatalf("agent attempt %d: want invalid-PIN, got %q", i+1, reason)
		}
	}
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-agent:lab:node-y", "correct-pin", ip)))
	if !strings.Contains(reason, "rate_limited") {
		t.Fatalf("agent 11th correct-PIN from a blocked IP must be rate-limited, got %q", reason)
	}
}

// TestPINRateLimitMultipleIPsDispersed — the SAME total number of failures spread
// across many IPs (each below the ceiling) must block NONE of them ON THE PER-IP
// KEY. This is the per-IP-key correctness proof: a per-IP counter that was secretly
// global would falsely block here.
//
// THE CLOCK ADVANCES, and the reason is a real change of contract, not a test
// convenience. origin: prerelease audit proto-auth-acl/L1-F3. This test used to
// assert, with a frozen clock, that dispersing across addresses is never throttled
// AT ALL — which is precisely the property the audit identified as the
// vulnerability: the argon2 verify runs on nats.go's single per-subscription
// delivery goroutine, so an attacker who disperses across addresses saturates every
// honest client's CONNECT against nats-server's 2s AUTH_TIMEOUT. There is now a
// SECOND, process-wide ceiling that answers "how much argon2 may this broker spend",
// and it deliberately does not care how many addresses are asking. Advancing the
// clock at the global refill rate keeps this test measuring the question it was
// written for — per-IP key isolation — instead of accidentally re-asserting the
// absence of the cost ceiling. The cost ceiling has its own guard,
// TestPINGlobalBudgetBoundsArgon2RegardlessOfSourceAddress.
func TestPINRateLimitMultipleIPsDispersed(t *testing.T) {
	h, _ := freshHandler(t)
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	ips := []string{"203.0.113.41", "203.0.113.42", "203.0.113.43", "203.0.113.44", "203.0.113.45"}
	// 9 failures from each IP → 45 total failures, but no single IP crosses 10.
	for _, ip := range ips {
		for i := 0; i < pinAttemptsPerMinute-1; i++ {
			_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ip))
		}
	}
	for _, ip := range ips {
		reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", ip)))
		if reason != "" {
			t.Fatalf("dispersed IP %s (9 failures) must NOT be blocked, got %q", ip, reason)
		}
	}
}

// TestPINRateLimitBehindProxy — many DISTINCT clients that all egress through one
// shared address (a NAT / reverse proxy) share one bucket, so their COLLECTIVE
// failures shut the shared address out. This is the documented v1 trade-off: a
// brute-forcer behind a shared NAT throttles co-located first-joins.
func TestPINRateLimitBehindProxy(t *testing.T) {
	h, _ := freshHandler(t)
	// origin: prerelease audit round 2, A-F4. relaxGlobalPINBudget is what makes this a
	// PER-IP guard rather than a "something refused it" guard. Both budgets deny with the
	// same ErrPINRateLimited text — deliberately, so a caller cannot probe which ceiling
	// it hit — so with the process-wide bucket at its real size this assertion is
	// satisfied by EITHER, and deleting the per-IP limiter outright leaves it green.
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	const proxyIP = "192.0.2.250"
	// 10 distinct client identities, one failed attempt each, all via the proxy IP.
	for i := 0; i < pinAttemptsPerMinute; i++ {
		_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", proxyIP))
	}
	// An 11th distinct client behind the same proxy, with the CORRECT pin, is blocked.
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", proxyIP)))
	if !strings.Contains(reason, "rate_limited") {
		t.Fatalf("shared-egress collective budget must block the 11th connect, got %q", reason)
	}
}

// TestPINRateLimitEmptyHostFailsOpen — when nats-server did not stamp a peer
// address (not client-forceable) the PER-IP throttle fails OPEN, so real joiners
// are never collapsed into one shared empty-host bucket.
//
// THE CLOCK ADVANCES, and that is the whole difference from the original version of
// this test. origin: prerelease audit proto-auth-acl/L1-F3 added a SECOND ceiling —
// a process-wide argon2 budget — which answers a different question from this one:
// "is this broker about to spend 0.15-0.3s of its single serialized callout
// goroutine", not "is this source brute-forcing". That one deliberately does NOT
// exempt an unstamped host, because a cost ceiling that anyone can step outside of
// is not a ceiling. Under the original FIXED clock the two are indistinguishable —
// 30 attempts drain the global burst and the assertion below would be measuring the
// cost ceiling while claiming to measure the identity one. Advancing the clock one
// second per attempt (the global refill rate) isolates the identity question, which
// is the invariant this test was written for and still guards.
func TestPINRateLimitEmptyHostFailsOpen(t *testing.T) {
	h, _ := freshHandler(t)
	relaxGlobalPINBudget(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	for i := 0; i < 3*pinAttemptsPerMinute; i++ { // far past the PER-IP ceiling
		_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ""))
	}
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", "")))
	if reason != "" {
		t.Fatalf("empty host must fail open for the PER-IP throttle (no identity bucket), got %q", reason)
	}
}

// TestPINGlobalBudgetBoundsArgon2RegardlessOfSourceAddress is the L1-F3 guard.
//
// The per-IP bucket is an identity control: an attacker with K addresses buys K
// times the argon2 budget. That is not a theoretical multiplier here, because the
// verify runs on nats.go's SINGLE per-subscription delivery goroutine — every
// auth_callout decision for the whole broker queues behind it — against
// nats-server's 2s AUTH_TIMEOUT. So a process-wide ceiling has to exist, and it has
// to hold when every attempt arrives from a DIFFERENT address.
func TestPINGlobalBudgetBoundsArgon2RegardlessOfSourceAddress(t *testing.T) {
	h, _ := freshHandler(t)
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	h.Now = func() time.Time { return now } // frozen: no refill during the burst
	seedSessionWithPin(t, h, "lab", "correct-pin")

	// Every attempt from its OWN address, so the per-IP bucket never blocks: each
	// one has a full budget of its own. Only a process-wide ceiling can stop this.
	var blocked int
	for i := 0; i < 4*pinGlobalBurst; i++ {
		ip := fmt.Sprintf("198.51.100.%d", i%250)
		reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ip)))
		if strings.Contains(reason, "rate_limited") {
			blocked++
		}
	}
	if blocked == 0 {
		t.Fatal("a distinct source address per attempt was never throttled: the per-IP bucket is " +
			"the only ceiling, so an attacker with enough addresses saturates the single " +
			"serialized auth_callout goroutine and every honest CONNECT times out")
	}
	// And it is a RATE limit, not a lockout: once the budget refills, a correct PIN
	// from a fresh source is served again.
	now = now.Add(time.Minute)
	if reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", "203.0.113.9"))); reason != "" {
		t.Fatalf("after the global budget refilled a correct PIN must be served, got %q", reason)
	}
}

// TestPINRateLimitRefillUnblocks — E.6 is a RATE limit, not a lockout: after the
// bucket refills (one minute later) the previously-blocked source may attempt
// again.
func TestPINRateLimitRefillUnblocks(t *testing.T) {
	base := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	now := base
	h, _ := freshHandler(t)
	h.Now = func() time.Time { return now }
	seedSessionWithPin(t, h, "lab", "correct-pin")

	const ip = "203.0.113.60"
	for i := 0; i < pinAttemptsPerMinute; i++ {
		_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ip))
	}
	if r := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", ip))); !strings.Contains(r, "rate_limited") {
		t.Fatalf("must be blocked immediately after 10 failures, got %q", r)
	}
	// Advance a full minute → the 10/min bucket has fully refilled → unblocked.
	now = base.Add(time.Minute)
	if r := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", ip))); r != "" {
		t.Fatalf("after a minute the correct PIN must join again (rate limit, not lockout), got %q", r)
	}
}

// TestPinRateLimiterIdleSweep pins the memory-bound: buckets idle past the TTL
// are pruned so a scan of many distinct source IPs cannot grow the map forever.
func TestPinRateLimiterIdleSweep(t *testing.T) {
	base := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	l := newPinRateLimiter()
	for i := 0; i < 100; i++ {
		l.recordFailure(fromInt(i), base)
	}
	if got := len(l.ips); got != 100 {
		t.Fatalf("expected 100 buckets, got %d", got)
	}
	// A single access well past the idle TTL + sweep interval prunes all the idle
	// buckets (only the freshly-touched one survives).
	l.recordFailure("survivor", base.Add(pinLimiterIdleTTL+2*pinLimiterSweepEvery))
	if got := len(l.ips); got != 1 {
		t.Fatalf("idle sweep must prune stale buckets: got %d, want 1", got)
	}
}

func fromInt(i int) string { return "10.0." + itoa(i/256) + "." + itoa(i%256) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [3]byte
	n := 0
	for i > 0 {
		b[n] = byte('0' + i%10)
		i /= 10
		n++
	}
	// reverse
	for l, r := 0, n-1; l < r; l, r = l+1, r-1 {
		b[l], b[r] = b[r], b[l]
	}
	return string(b[:n])
}

// relaxGlobalPINBudget lifts the process-wide argon2 ceiling for the duration of one
// test so a PER-IP assertion measures the per-IP bucket alone.
//
// Not a convenience: two ceilings guard the same call path, and a per-IP test that
// leaves the process-wide one at production values is measuring whichever binds
// first — which is how three of these tests started failing when it was introduced.
// Advancing the clock instead does NOT work: it refills the per-IP bucket too, so
// the very saturation the test is trying to create never happens.
func relaxGlobalPINBudget(t *testing.T) {
	t.Helper()
	perSec, burst := pinGlobalPerSecond, pinGlobalBurst
	pinGlobalPerSecond, pinGlobalBurst = 1_000_000, 1_000_000
	t.Cleanup(func() { pinGlobalPerSecond, pinGlobalBurst = perSec, burst })
}

// origin: prerelease audit round 2, A-F3.
//
// THE ARGON2 CEILING IS CHARGED WHERE ARGON2 RUNS, NOT WHERE THE REQUEST ARRIVES.
//
// The charge used to sit at the call site, immediately before provision()/join(). That
// looks equivalent and is not: both refuse a nonexistent session, an already-provisioned
// nid, or an unwired seam BEFORE reaching a verifier. So a stranger publishing joins for
// random session names spent one token of a 10-token bucket per request at zero CPU cost
// — and once the bucket emptied, every genuine agent bootstrap and every PIN member join
// on the broker was refused. The ceiling delivered the denial it exists to prevent.
func TestTheArgon2BudgetIsNotDrainedByRequestsThatNeverReachArgon2(t *testing.T) {
	h, _ := freshHandler(t)

	// Drain attempts against sessions that DO NOT EXIST. Each one is refused long
	// before a verifier is reached, so none of them may cost budget.
	for i := 0; i < pinGlobalDefaultBurst*3; i++ {
		_ = h.ensureAgentProvisioned("no-such-session", "n1", "NKEY", "SHA256:fp", "1234", "203.0.113.9")
	}

	if h.pinLimiterFor().globalBlocked(time.Now()) {
		t.Fatal("requests that never reached argon2 exhausted the process-wide argon2 budget.\n\n" +
			"They cost the broker no CPU at all — the session lookup refuses first — so a stranger " +
			"naming random session names denies every real agent bootstrap and every PIN member " +
			"join on this broker, using the ceiling as the weapon.")
	}

	// And the charge must still happen when argon2 DOES run, or the ceiling is gone.
	// (Was h.chargedVerifyPIN; external review R2-M1 collapsed the check-then-charge pair
	// into one atomic try-spend, so VerifyPINWithBudget IS the charging verifier now. The
	// property under test — "a real verify consumes exactly one unit" — is unchanged.)
	before := h.pinLimiterFor().global.TokensAt(time.Now())
	_ = h.VerifyPINWithBudget("1234", "not-a-valid-phc")
	after := h.pinLimiterFor().global.TokensAt(time.Now())
	if after >= before {
		t.Errorf("a real verify did not charge the budget (%v -> %v); the ceiling no longer bounds "+
			"the 0.15-0.3s of serialized callout goroutine each argon2 costs", before, after)
	}
}
