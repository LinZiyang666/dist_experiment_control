package authcallout

import (
	"strings"
	"testing"
	"time"
)

// ADJUDICATION (main process, external review H2). Codex flagged the per-broker PIN
// budget as a cluster-wide N× dilution and asked for a cluster-consistent limiter.
// Weighed against claude's N-2 (doc-level) classification and the code, the main process
// PARTIALLY ACCEPTS: the false "≤10 per IP" contract is FIXED (architecture.md §E.6 now
// states the honest per-broker semantics), but a distributed limiter is DECLINED for v1:
//  1. the PRIMARY brute-force defense is memory-hard argon2id (64 MiB, t=3 per verify —
//     internal/auth/pin.go), so throughput is memory-bound regardless of the counter;
//     this limiter is a secondary speed bump.
//  2. a cluster-consistent counter puts a distributed write on the UNAUTHENTICATED
//     connect path — a strictly worse DoS surface (security 实用主义).
//  3. PINs are ASCII-printable with no length cap (auth.ValidPIN), so a non-trivial PIN
//     is astronomically safe even at N×10/min.
//
// This test therefore (a) pins the guarantee we DO make — per-broker enforcement — and
// (b) documents the cluster N× as a KNOWN, accepted boundary rather than deleting Codex's
// counterexample. See docs/reviews/codex-allgreen-external-review.md for the full reply.
func TestCodexReviewPINBudgetIsPerBrokerByDesign(t *testing.T) {
	base := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	// (a) per-broker enforcement is REAL and non-vacuous: 10 failures on ONE broker block
	// the 11th attempt — a correct PIN included — until the bucket refills.
	h, _ := freshHandler(t)
	h.Now = fixedClock(base)
	seedSessionWithPin(t, h, "lab", "correct-pin")
	const ip = "203.0.113.77"
	for i := 0; i < pinAttemptsPerMinute; i++ {
		reason := denyReason(t, mustHandle(t, h,
			pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "wrong-pin", ip)))
		if !strings.Contains(reason, "invalid PIN") {
			t.Fatalf("attempt %d: want invalid-PIN denial, got %q", i+1, reason)
		}
	}
	reason := denyReason(t, mustHandle(t, h,
		pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "correct-pin", ip)))
	if !strings.Contains(reason, "rate_limited") {
		t.Fatalf("per-broker budget must block the 11th attempt (a correct PIN included); got %q", reason)
	}

	// (b) KNOWN, ACCEPTED BOUNDARY: the budget is per-broker, so failures spread across two
	// brokers do NOT share a counter. Ten failures split 5/5 leave EACH broker's bucket at 5
	// (< the budget), so a further WRONG attempt on the first broker still denies with
	// "invalid PIN", not "rate_limited". Documented in architecture.md §E.6; the memory-hard
	// argon2id verify is the primary defense that makes the N× dilution acceptable for v1.
	h2a, _ := freshHandler(t)
	h2a.Now = fixedClock(base)
	seedSessionWithPin(t, h2a, "lab", "correct-pin")
	h2b, _ := freshHandler(t)
	h2b.Now = fixedClock(base)
	seedSessionWithPin(t, h2b, "lab", "correct-pin")
	handlers := []*Handler{h2a, h2b}
	const ip2 = "203.0.113.88"
	for i := 0; i < pinAttemptsPerMinute; i++ {
		_ = denyReason(t, mustHandle(t, handlers[i%2],
			pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "wrong-pin", ip2)))
	}
	boundary := denyReason(t, mustHandle(t, h2a,
		pinReqFrom(t, freshClientPub(t), "tether-cli:lab", "wrong-pin", ip2)))
	if strings.Contains(boundary, "rate_limited") {
		t.Fatalf("per-broker boundary: h2a saw only 5 of 10 spread failures and must NOT be rate-limited yet; got %q", boundary)
	}
}
