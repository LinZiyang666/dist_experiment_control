package proto

import "time"

// xfer.go (batch C / C2) — the SHARED tier sizing and the transfer time budget.
//
// WHY THESE LIVE HERE
// -------------------
// The two tier ceilings existed in THREE copies — broker, agent and ctl each declared their own
// `8 * 1024 * 1024` and `2 * 1024 * 1024 * 1024`. They are a wire-adjacent contract (the broker
// REFUSES at them and the other two ends must not offer what will be refused), so three copies is
// three chances to drift on one edit. internal/proto is the leaf both other packages already import,
// and moving a compile-time constant changes NO wire value: the numbers on the wire are the sizes
// themselves, not these bounds.
//
// WHY THE BUDGET IS A FUNCTION AND NOT A CONSTANT
// -----------------------------------------------
// transferTimeoutTierB (5m) and XferMaxBytes (2 GiB) did not derive from each other, and the pair
// silently asserted a throughput nobody had ever computed. Reading it back out:
//
//	2 legs x 2 GiB / 300s = 13.65 MiB/s = ~114 Mbit/s, END TO END, through JetStream object chunking
//	and the broker's fsync.
//
// A tier-B transfer crosses the object store TWICE (sender Put, receiver Get), so the watchdog has to
// cover both legs; the refactor roadmap's own worked example — a 100 Mbit/s link — cannot meet that
// implied rate even at the raw link layer. When the watchdog fired it wrote a WRONG code and DELETED
// the in-flight object, after which the receiver's real completion event was dropped because the
// tracker was already empty: the file could land on disk while the audit said failed.
//
// XferMinThroughput is therefore the SLOWEST link the tier-B budget promises to cover. 2 MiB/s
// (~16.8 Mbit/s) is about one seventh of the rate the old constants implied, which is a conservative
// floor for a residential/lab uplink, and 2 GiB / 2 MiB/s is EXACTLY 1024 seconds — a power of two,
// so every derived bound below is hand-checkable with no rounding.
//
// The budget is bounded because the size is: the broker's admission gate refuses anything over
// XferMaxBytes before the watchdog is ever armed, so XferTierBMaxBudget is a compile-time constant.
// An unbounded budget would not be a longer timeout, it would be no timeout.
const (
	// XferTierAMaxBytes is the tier-A (inline, in a NATS message body) ceiling.
	XferTierAMaxBytes = 8 * 1024 * 1024
	// XferMaxBytes is the GLOBAL hard upper bound for one tier-B transfer. A small-disk broker's
	// per-session bucket ceiling can be LOWER; admission refuses at the min of the two.
	XferMaxBytes = 2 * 1024 * 1024 * 1024
	// XferMinThroughput is the slowest end-to-end rate, in bytes/second, that the tier-B budget
	// promises to cover on ONE leg. See the derivation above.
	XferMinThroughput = 2 * 1024 * 1024
	// XferPushLegs is how many full-size crossings of the object store one tier-B transfer makes:
	// the sender Put and the receiver Get. The broker's watchdog is armed BEFORE the prepare is even
	// forwarded, so it must cover both; each END covers only its own one (XferLegBudget).
	XferPushLegs = 2
)

// Tier floors. A transfer never gets LESS than these, whatever its size says.
const (
	XferTimeoutTierA = 30 * time.Second
	// XferTimeoutTierBFloor is the historical fixed tier-B timeout, kept as the floor so every
	// transfer small enough not to need a size-derived budget behaves exactly as it did before.
	XferTimeoutTierBFloor = 5 * time.Minute
)

// XferOverheadMargin is the time the budget reserves for everything that is NOT byte transfer.
//
// origin: batch-c EXTERNAL review M3. The budget used to be exactly legs x ceil(size/throughput),
// which quietly asserted that every non-transfer step takes ZERO time — so a link achieving EXACTLY
// the 2 MiB/s this file promises to cover would still time out. The watchdog is armed BEFORE the
// prepare is even forwarded, and inside that window sit: the prepare round-trip to the agent, the
// object-store open/create, the ctl's commit RPC, the receiver's SHA verification and fsync, the
// object metadata write, and the ev.transfer publish back to the broker.
//
// 60s is the same order as the margins the other two ends already carry (the agent's prep-cache slack
// is 1m, the ctl default adds 2m) — this makes the one end that actually DELETES the object and
// writes the failed terminal the one that is no longer the tightest.
const XferOverheadMargin = 60 * time.Second

// XferTierBMaxBudget is the ceiling the broker watchdog can ever arm: both legs at the worst size
// admission will accept, plus the overhead margin. Written as a constant expression so it is
// checkable by hand — 2 * (2 GiB / 2 MiB/s) + 60s = 2048s + 60s = 2108s = 35m08s — and so a test can
// pin it to that literal rather than to the formula that produced it.
const XferTierBMaxBudget = XferPushLegs*(XferMaxBytes/XferMinThroughput)*time.Second + XferOverheadMargin

// XferLegBudget is the time ONE full-size crossing of the object store is allowed to take. Rounds UP
// to whole seconds: rounding down would make a budget that is exactly enough into one that is one
// tick short.
func XferLegBudget(size int64) time.Duration {
	if size <= 0 {
		return 0
	}
	// Ledger rows are local durable input and may predate validation changes or be damaged. Keep the
	// helper bounded even when called before admission: overflow must never turn a huge declared size
	// into a negative duration and silently fall back to the five-minute floor.
	if size > XferMaxBytes {
		size = XferMaxBytes
	}
	return time.Duration((size+XferMinThroughput-1)/XferMinThroughput) * time.Second
}

// XferBudget is the total time a transfer of this tier and size is allowed. legs is how many
// full-size crossings the caller is responsible for: the broker covers XferPushLegs, each END covers
// 1. tier A is fixed — its 8 MiB ceiling is inside the floor at any plausible rate.
//
// size==0 means "not known here" (the pull path carries no declared size) and yields the floor, i.e.
// exactly the pre-batch-C behaviour. That fallback is deliberate and pinned by a test: it is also
// what an OLD on-disk ledger record, written before Size existed, degrades to.
func XferBudget(tier string, size int64, legs int) time.Duration {
	if tier != "b" {
		return XferTimeoutTierA
	}
	if legs < 1 {
		legs = 1
	}
	if legs > XferPushLegs {
		legs = XferPushLegs
	}
	// The margin is added to the DATA time, not to the floor: a transfer small enough to fit inside the
	// floor already has minutes of headroom, while one that needs a derived budget is precisely the one
	// whose setup and finalize steps would otherwise have to take zero time (EXTERNAL review M3).
	if d := time.Duration(legs)*XferLegBudget(size) + XferOverheadMargin; d > XferTimeoutTierBFloor {
		return d
	}
	return XferTimeoutTierBFloor
}

// HumanBytes renders a byte bound the way an operator reads it.
//
// origin: batch-c internal review F4. It lives beside the constants, not in one consumer, because all
// three ends print these bounds at a human: the broker refuses with them, the agent refuses with them,
// and the ctl refuses with them. Every one of those messages used to hand-copy "2 GiB" / "8 MiB" next
// to a constant that can move underneath it — so lowering XferMaxBytes would have left three separate
// places still telling the operator the old number, and the operator computing the wrong limit from
// the error they were shown. The AST gate that keeps the CONSTANTS single-sourced cannot see inside a
// format string, which is exactly why this one needed a person to notice.
func HumanBytes(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib && n%gib == 0:
		return itoa64(n/gib) + " GiB"
	case n >= mib && n%mib == 0:
		return itoa64(n/mib) + " MiB"
	case n >= kib && n%kib == 0:
		return itoa64(n/kib) + " KiB"
	}
	return itoa64(n) + " B"
}

// itoa64 avoids pulling fmt/strconv into this leaf for one conversion.
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}
