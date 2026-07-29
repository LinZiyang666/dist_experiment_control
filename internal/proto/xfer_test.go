package proto

import (
	"testing"
	"time"
)

// xfer_test.go — the tier/budget relations.
//
// EVERY LOAD-BEARING ASSERTION HERE USES A HAND-COMPUTED LITERAL, ON PURPOSE.
//
// The obvious way to write these is to restate the formula: assert
// XferTierBMaxBudget == XferPushLegs * XferLegBudget(XferMaxBytes). That is an IDENTITY — it is true
// for every possible value of every constant involved, including a leg count of 1, because the leg
// count appears on both sides and cancels. A first draft of this file did exactly that and claimed
// the assertion "catches a wrong leg count"; it does not, and nothing about running it would have
// revealed that. So the numbers below are computed by hand from the derivation in xfer.go, and a
// constant that moves has to be re-derived by hand too. That is the cost, and it is the point.

// TestXferBudgetDerivationLiterals pins the hand-computed values from the xfer.go derivation:
//
//	2 GiB / 2 MiB/s          = 1024s   (one leg at the worst admitted size)
//	2 legs x 1024s + 60s     = 2108s   = 35m08s (the broker's ceiling)
//
// Mutations: change XferMinThroughput, XferPushLegs, XferMaxBytes, or swap max/min in XferBudget —
// each reddens a different line.
func TestXferBudgetDerivationLiterals(t *testing.T) {
	if got := XferLegBudget(XferMaxBytes); got != 1024*time.Second {
		t.Errorf("one leg at the maximum size = %s, want 1024s (2 GiB / 2 MiB/s). If XferMinThroughput "+
			"or XferMaxBytes moved, re-derive this literal by hand — do not replace it with the formula, "+
			"which would make the assertion an identity", got)
	}
	// 2 legs x 1024s of data + a 60s overhead margin = 2108s = 35m08s. Re-derive by hand if any of
	// the three constants moves; do NOT replace this with the formula, which would make it an identity.
	if got := XferTierBMaxBudget; got != 2108*time.Second {
		t.Errorf("XferTierBMaxBudget = %s, want 2108s (= 35m08s: two full crossings at the worst "+
			"admitted size, PLUS the non-transfer overhead margin)", got)
	}
	if got := XferBudget("b", XferMaxBytes, XferPushLegs); got != XferTierBMaxBudget {
		t.Errorf("the budget at the maximum size (%s) must equal the declared ceiling (%s) — the "+
			"ceiling is what makes the watchdog bounded", got, XferTierBMaxBudget)
	}
	// The leg count is checked here and ONLY here, against a literal: a one-leg budget must be half
	// the two-leg one at the same size. Written as `XferPushLegs * legBudget` it would cancel out.
	if got := XferBudget("b", XferMaxBytes, 1); got != 1084*time.Second {
		t.Errorf("a single-leg budget at the maximum size = %s, want 1084s (1024s of data + the 60s "+
			"margin) — each END covers one crossing, the broker covers both", got)
	}
}

// TestXferBudgetNeverBelowTheTierFloor: the size-derived budget is a floor-RAISING device only. A
// small file must behave exactly as it did before batch C.
//
// Mutation: drop the max() in XferBudget so a 1-byte file gets a 1-second budget — reddens.
func TestXferBudgetNeverBelowTheTierFloor(t *testing.T) {
	for _, size := range []int64{0, 1, 1024, XferTierAMaxBytes, 64 * 1024 * 1024, XferMaxBytes} {
		if got := XferBudget("b", size, XferPushLegs); got < XferTimeoutTierBFloor {
			t.Errorf("size=%d: tier-B budget %s is below the floor %s", size, got, XferTimeoutTierBFloor)
		}
	}
	// size==0 means "not known here" (the pull path carries no declared size, and a ledger record
	// written before batch C has none). It must yield exactly the old fixed behaviour, NOT a zero
	// budget — a zero budget would fire the watchdog instantly and declare every such transfer failed.
	if got := XferBudget("b", 0, XferPushLegs); got != XferTimeoutTierBFloor {
		t.Errorf("an unknown size must degrade to the fixed tier-B floor exactly, got %s want %s",
			got, XferTimeoutTierBFloor)
	}
	if got := XferBudget("a", XferMaxBytes, XferPushLegs); got != XferTimeoutTierA {
		t.Errorf("tier A is fixed regardless of size, got %s want %s", got, XferTimeoutTierA)
	}
}

// TestXferBudgetIsMonotonicInSize: a larger file must never get LESS time. An off-by-one in the
// ceiling arithmetic, or an int32 truncation, shows up here and nowhere else.
func TestXferBudgetIsMonotonicInSize(t *testing.T) {
	prev := time.Duration(0)
	for size := int64(0); size <= XferMaxBytes; size += XferMaxBytes / 64 {
		got := XferBudget("b", size, XferPushLegs)
		if got < prev {
			t.Fatalf("budget decreased at size=%d: %s after %s", size, got, prev)
		}
		prev = got
	}
}

// TestXferLegBudgetRoundsUp: truncating division would give a budget one tick SHORT of what the
// declared throughput needs, which is the difference between "just enough" and "always fails".
func TestXferLegBudgetRoundsUp(t *testing.T) {
	if got := XferLegBudget(1); got != time.Second {
		t.Errorf("a 1-byte leg = %s, want 1s (rounded up); truncation would give a zero budget", got)
	}
	if got := XferLegBudget(XferMinThroughput + 1); got != 2*time.Second {
		t.Errorf("one byte past a whole second = %s, want 2s", got)
	}
	if got := XferLegBudget(0); got != 0 {
		t.Errorf("a zero-size leg = %s, want 0 (the caller's max() supplies the floor)", got)
	}
}

// TestXferBudgetClampsUntrustedInputs keeps damaged/forward-version ledger values from overflowing
// a duration into the five-minute floor.
//
// origin: batch-c external re-review D2. Production admission limits size and uses one or two legs,
// but crash recovery reads these values from durable files before it decides whether deletion is safe.
func TestXferBudgetClampsUntrustedInputs(t *testing.T) {
	if got := XferLegBudget(int64(^uint64(0) >> 1)); got != XferLegBudget(XferMaxBytes) {
		t.Fatalf("an oversized durable size produced leg budget %s, want the bounded maximum %s",
			got, XferLegBudget(XferMaxBytes))
	}
	if got := XferBudget("b", XferMaxBytes, int(^uint(0)>>1)); got != XferTierBMaxBudget {
		t.Fatalf("an oversized leg count produced budget %s, want bounded maximum %s",
			got, XferTierBMaxBudget)
	}
}

// TestXferTierCeilingsAreTheDocumentedValues: these two are a wire-adjacent contract — the broker
// REFUSES at them and the other two ends must not offer what will be refused. Moving one silently is
// how the three former copies were able to drift.
func TestXferTierCeilingsAreTheDocumentedValues(t *testing.T) {
	if XferTierAMaxBytes != 8*1024*1024 {
		t.Errorf("tier-A ceiling = %d, want 8 MiB — docs/usage.md states this to users", XferTierAMaxBytes)
	}
	if XferMaxBytes != 2*1024*1024*1024 {
		t.Errorf("global ceiling = %d, want 2 GiB — docs/usage.md states this to users", XferMaxBytes)
	}
	if XferTierAMaxBytes >= XferMaxBytes {
		t.Error("the tier-A ceiling must sit below the global one")
	}
}

// TestBudgetLeavesRoomForNonTransferWork is the external review's M3 counterexample, and it is
// deliberately NOT a comparison of the formula against itself.
//
// A link achieving EXACTLY the throughput this package promises to cover must still finish inside the
// budget, because a real transfer also spends time on things that move no bytes: the prepare
// round-trip, the object-store open, the commit RPC, the receiver's SHA and fsync, the metadata
// write, and the completion event. The pre-M3 budget was exactly legs x data-time, which asserted all
// of that takes zero seconds — so the promised worst-case link was mathematically guaranteed to fail.
//
// Mutation: set XferOverheadMargin to 0 — reddens at every size.
func TestBudgetLeavesRoomForNonTransferWork(t *testing.T) {
	for _, size := range []int64{
		XferTierAMaxBytes + 1, 128 * 1024 * 1024, 1024 * 1024 * 1024, XferMaxBytes,
	} {
		budget := XferBudget("b", size, XferPushLegs)
		// What a link running at EXACTLY the promised floor spends purely on bytes, both crossings.
		dataTime := time.Duration(XferPushLegs) * XferLegBudget(size)
		slack := budget - dataTime
		if slack <= 0 {
			t.Errorf("size=%d: budget %s leaves %s for everything that is not byte transfer — a link "+
				"achieving exactly the promised %s/s still cannot finish, because setup and finalize "+
				"would have to take zero time", size, budget, slack, HumanBytes(XferMinThroughput))
		}
		if slack < XferOverheadMargin {
			t.Errorf("size=%d: slack %s is below the declared overhead margin %s", size, slack, XferOverheadMargin)
		}
	}
	// The floor itself must also keep room: an 8 MiB+1 transfer at the promised rate needs ~8s of data
	// time, and the rest of the five-minute floor is its margin.
	small := int64(XferTierAMaxBytes + 1)
	if XferBudget("b", small, XferPushLegs) != XferTimeoutTierBFloor {
		t.Errorf("a just-over-tier-A transfer should still sit at the floor, got %s",
			XferBudget("b", small, XferPushLegs))
	}
}
