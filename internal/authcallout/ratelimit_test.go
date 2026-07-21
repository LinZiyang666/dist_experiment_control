package authcallout

import (
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
	return pub
}

// TestPINRateLimitSingleIPHighFrequency — the headline case: 10 wrong PINs from
// one IP exhaust the budget; the 11th connect from that IP is refused EVEN WITH
// THE CORRECT PIN, while a first connect from a different IP still succeeds.
func TestPINRateLimitSingleIPHighFrequency(t *testing.T) {
	h, _ := freshHandler(t)
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
// across many IPs (each below the ceiling) must block NONE of them. This is the
// per-IP-key correctness proof (a global counter would falsely block here).
func TestPINRateLimitMultipleIPsDispersed(t *testing.T) {
	h, _ := freshHandler(t)
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
// address (not client-forceable) the throttle fails OPEN, so real joiners are
// never collapsed into one shared empty-host bucket.
func TestPINRateLimitEmptyHostFailsOpen(t *testing.T) {
	h, _ := freshHandler(t)
	h.Now = fixedClock(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	seedSessionWithPin(t, h, "lab", "correct-pin")

	for i := 0; i < 3*pinAttemptsPerMinute; i++ { // far past the ceiling
		_ = mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "nope", ""))
	}
	reason := denyReason(t, mustHandle(t, h, pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", "")))
	if reason != "" {
		t.Fatalf("empty host must fail open (no throttle), got %q", reason)
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
