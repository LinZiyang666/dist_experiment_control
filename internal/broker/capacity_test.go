package broker

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
)

// capacity_test.go (G6 #21 + smalldisk) — the OBJ_xfer bucket sizing invariants: the computed
// MaxBytes is ALWAYS in (floor, cap] and NEVER <=0 (nats treats MaxBytes<=0 as UNLIMITED → a worse
// silent re-brick), and when the store cannot even fit the floor it REFUSES rather than emit a bad
// number.
func TestXferMaxBytesForCeiling(t *testing.T) {
	const G = int64(1024 * 1024 * 1024)
	cases := []struct {
		name     string
		ceiling  int64
		sessions int
		replicas int
		wantErr  bool
		check    func(int64) bool
	}{
		{"unknown ceiling → cap", 0, 1, 1, false, func(v int64) bool { return v == xferBucketCap }},
		{"negative ceiling → cap", -1, 1, 1, false, func(v int64) bool { return v == xferBucketCap }},
		{"4 GiB store (drill 21 shape) fits below cap", 4 * G, 1, 1, false,
			func(v int64) bool { return v > xferBucketFloor && v < 4*G }},
		{"racknerd ~10.33 GiB", 10*G + G/3, 1, 1, false,
			func(v int64) bool { return v > xferBucketFloor && v <= xferBucketCap }},
		{"huge store clamps to the cap, NOT to a fraction of the disk", 500 * G, 1, 1, false,
			func(v int64) bool { return v == xferBucketCap }},
		{"too small even for the floor → refuse", 2*G + 100*1024*1024, 1, 1, true, nil},
		{"exactly the reserve → refuse (avail 0)", 2 * G, 1, 1, true, nil},
		// Enough sessions / replicas eventually exhaust the store outright.
		{"10 sessions on a 10 GiB store → refuse", 10 * G, 10, 1, true, nil},
		{"5 replicas on a 10 GiB store → refuse", 10 * G, 1, 5, true, nil},
		{"3 replicas on a 40 GiB store still sizes", 40 * G, 1, 3, false,
			func(v int64) bool { return v > xferBucketFloor && v <= xferBucketCap }},
	}
	for _, tc := range cases {
		got, err := xferMaxBytesForCeiling(tc.ceiling, tc.sessions, tc.replicas)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: want refuse, got MaxBytes=%d", tc.name, got)
				continue
			}
			// The refusal must stay structurally PERMANENT: #67's bounded provisioning proves
			// "make zero create attempts" via errors.Is, not by matching prose.
			if !errors.Is(err, errXferStoreTooSmall) {
				t.Errorf("%s: refusal lost the errXferStoreTooSmall sentinel (%v) — the #67 retry path "+
					"would start making create attempts against a store that cannot hold the floor",
					tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected err: %v", tc.name, err)
			continue
		}
		// HARD invariant: never <=0 (UNLIMITED footgun), never above the cap.
		if got <= 0 || got > xferBucketCap {
			t.Errorf("%s: MaxBytes=%d violates the (0, cap] invariant", tc.name, got)
		}
		if !tc.check(got) {
			t.Errorf("%s: MaxBytes=%d failed the case-specific check", tc.name, got)
		}
	}
}

// TestXferBucketCapHoldsOneMaxTransferPlusOverhead is the boundary this increment exists to make
// impossible: a bucket sized EXACTLY proto.XferMaxBytes would let admission accept a max-size push
// (the gate is `size > maxBytes`, and equal is not greater) and then fail it MID-PUT against the
// backing stream's DiscardNew — after the sender has hashed the file and started streaming.
// Refusing up front is strictly better than failing half way, so the cap must EXCEED one max
// transfer by enough to cover the object's chunk overhead.
//
// origin: docs/reviews/smalldisk-plan.md §2 (边界陷阱).
func TestXferBucketCapHoldsOneMaxTransferPlusOverhead(t *testing.T) {
	if int64(xferBucketCap) <= int64(proto.XferMaxBytes) {
		t.Fatalf("xferBucketCap=%d must EXCEED proto.XferMaxBytes=%d: a bucket sized to exactly one "+
			"max transfer admits it and then overflows mid-Put (128 KiB chunks × per-message overhead)",
			int64(xferBucketCap), int64(proto.XferMaxBytes))
	}
	// The margin must cover a max-size object's chunk overhead with real room — ~16384 chunks at
	// 2 GiB. A margin that merely squeaked past would be indistinguishable from luck.
	const chunk = 128 * 1024
	chunks := int64(proto.XferMaxBytes) / chunk
	margin := int64(xferBucketCap) - int64(proto.XferMaxBytes)
	if margin < chunks*256 {
		t.Fatalf("margin %d is under the %d-chunk worst case (%d bytes at 256B/chunk)",
			margin, chunks, chunks*256)
	}
	// AND AN UPPER BOUND — this half is the increment's whole point, and it was missing from the
	// first cut: a lower bound alone is satisfied by the legacy 8 GiB cap too, so restoring that cap
	// left every test green while re-creating the bug. (Found by mutating the constant back and
	// watching nothing redden.)
	//
	// The bucket may hold only IN-FLIGHT objects — both ends delete on completion and the orphan
	// reaper sweeps the rest — so capacity beyond about one max-size transfer buys nothing but a
	// RESERVATION, and the reservation is precisely what denies tier-B on a small disk. Sizing "by
	// need" therefore means the cap stays within one transfer plus overhead, never a multiple of it.
	if int64(xferBucketCap) >= 2*int64(proto.XferMaxBytes) {
		t.Fatalf("xferBucketCap=%d is >= 2x one max transfer (%d). The bucket holds only in-flight "+
			"objects, so a multiple-transfer cap reserves store nobody can use — on racknerd the old "+
			"8 GiB cap reserved 77%% of a 10.33 GiB store for a bucket holding zero bytes, which is "+
			"how tier-B became permanently unavailable there",
			int64(xferBucketCap), int64(proto.XferMaxBytes))
	}
}

// TestXferReserveTracksTheStreamConstants pins the reserve to its SOURCES. The pre-existing code
// hand-copied "2 GiB" for events+history; nothing tied that number to the two stream configs, so
// editing either one silently falsified the sizing arithmetic in this package.
//
// NOT falsifiable by editing the jsstream constants — `want` is computed from the same symbols, so
// both sides move together and the test stays green. That is by design: what it pins is the SOURCING
// (that broker asks jsstream rather than carrying its own copy), and the mutation that reddens it is
// re-introducing a hand-written literal — e.g. `return int64(replicas) * (1<<30 + int64(sessions)*(1<<30))`
// still passes, but any literal that DISAGREES with the constants fails the (3,1) and (1,3) rows.
// Recorded precisely because the first version of this note claimed a mutation that does not fire.
func TestXferReserveTracksTheStreamConstants(t *testing.T) {
	if got, want := xferReserveFor(1, 1), int64(jsstream.EventsMaxBytes+jsstream.HistoryMaxBytesPerSession); got != want {
		t.Fatalf("xferReserveFor(1,1)=%d, want %d (events + one history)", got, want)
	}
	// PER SESSION, not a constant: the old hand-copied reserve was correct only at exactly one session.
	if got, want := xferReserveFor(3, 1), int64(jsstream.EventsMaxBytes+3*jsstream.HistoryMaxBytesPerSession); got != want {
		t.Fatalf("xferReserveFor(3,1)=%d, want %d — history is PER SESSION", got, want)
	}
	// Replica-multiplied: nats charges the ACCOUNT-level reservation as Replicas*MaxBytes.
	if got, want := xferReserveFor(1, 3), 3*int64(jsstream.EventsMaxBytes+jsstream.HistoryMaxBytesPerSession); got != want {
		t.Fatalf("xferReserveFor(1,3)=%d, want %d — the account-level charge multiplies by replicas", got, want)
	}
	// Degenerate inputs must not SHRINK the reserve: a 0 would under-reserve and over-size the bucket.
	if xferReserveFor(0, 0) != xferReserveFor(1, 1) {
		t.Fatal("zero/negative sessions or replicas must floor at 1, never reserve less")
	}
	if got := xferReserveFor(math.MaxInt, math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("huge counts wrapped reserve to %d, want saturated MaxInt64", got)
	}
}

// TestXferSizingShrinksWithSessionsAndReplicas is the scaling guard, written as MONOTONICITY rather
// than as hand-computed thresholds: a threshold table only catches the bug if my arithmetic happens
// to straddle it, whereas "more claimants ⇒ never a bigger bucket, and strictly smaller before the
// cap clamps" catches a dropped multiply outright.
//
// Mutations that must redden this: drop the `int64(sessions)*` term, or drop the `int64(replicas)*`
// term, in xferReserveFor. Either one makes the bucket size stop responding to its input.
func TestXferSizingShrinksWithSessionsAndReplicas(t *testing.T) {
	const G = int64(1024 * 1024 * 1024)
	// The ceiling must be SMALL enough that the DISK binds rather than the cap — otherwise every
	// input clamps to xferBucketCap and the test compares a constant to itself. (First cut used
	// 12 GiB "to be safe" and did exactly that: three identical numbers, a green tautology waiting
	// to happen. The clamp is the reason a monotonicity test needs a disk-bound fixture.)
	const ceiling = 9 * G / 2 // 4.5 GiB: avail*0.75 stays under the cap for every case below

	size := func(sessions, replicas int) int64 {
		v, err := xferMaxBytesForCeiling(ceiling, sessions, replicas)
		if err != nil {
			t.Fatalf("fixture refused at sessions=%d replicas=%d: %v", sessions, replicas, err)
		}
		return v
	}

	one := size(1, 1)
	if got := size(2, 1); got >= one {
		t.Errorf("a SECOND session did not shrink the bucket (%d vs %d) — history is per-session, so "+
			"its reservation must scale with the session count", got, one)
	}
	if got := size(1, 2); got >= one {
		t.Errorf("doubling replicas did not shrink the bucket (%d vs %d) — nats charges the "+
			"account-level reservation as Replicas*MaxBytes", got, one)
	}
	// Never <=0, never above the cap, at every point the fixture can still serve. Past that the
	// store genuinely runs out and the REFUSAL is the correct answer — asserted separately, in the
	// table above, so this loop stops where the fixture does instead of demanding the impossible.
	for _, n := range []int{1, 2, 3} {
		v := size(n, 1)
		if v <= 0 || v > xferBucketCap {
			t.Fatalf("sessions=%d produced %d, outside (0, cap]", n, v)
		}
	}
	// And one past the edge: the same curve must REFUSE rather than emit something unusable.
	if v, err := xferMaxBytesForCeiling(ceiling, 8, 1); err == nil {
		t.Fatalf("8 sessions on a %d-byte store returned %d instead of refusing", ceiling, v)
	}
}

// TestXferSizingSurvivesMissingDependencies pins that the sizing path degrades instead of dying.
//
// It runs on the PUSH path. A nil-deref there takes the broker down over a capacity question — and
// the first cut of activeSessionCount did exactly that against a fixture with no DB handle, panicking
// through xferBucketMaxBytes. Sizing has to answer with a number under every degraded input, because
// the alternative to a slightly-wrong bucket size is no broker.
//
// Mutation that must redden this: drop the `b.read().SQL() == nil` guard in activeSessionCount —
// the call then dereferences a nil pool and panics. (The nil-Logger guard on the next lines is NOT
// reachable from here: activeSessionCount returns at the nil-handle check first. Naming a mutation
// that cannot fire is the same defect this file is full of guards against.)
func TestXferSizingSurvivesMissingDependencies(t *testing.T) {
	b := &Broker{} // no DB, no Logger, no JetStream — every dependency absent at once
	if got := b.activeSessionCount(context.Background()); got != 1 {
		t.Fatalf("activeSessionCount with no DB = %d, want the conservative 1", got)
	}
	// And the whole sizing call must still produce a usable answer rather than panicking.
	got, err := b.xferBucketMaxBytes(context.Background())
	if err != nil {
		t.Fatalf("sizing with no dependencies refused (%v); it should fall back, not fail", err)
	}
	if got <= 0 || got > xferBucketCap {
		t.Fatalf("degraded sizing produced %d, outside (0, cap]", got)
	}
}

// origin: smalldisk external review F5
// The object-store bucket is per session, while transferTracker admits many
// concurrent transfer IDs. A cap sized for one maximum object is safe only if
// admission serializes that bucket (or atomically accounts aggregate bytes).
// Otherwise two individually valid pushes are accepted and overflow the
// DiscardNew stream after data transmission has begun.
func TestOneObjectBucketCapRejectsASecondMaxTransferInTheSameSession(t *testing.T) {
	tracker := newTransferTracker()
	bucket := proto.XferBucketName("lab")
	first := &transferEntry{
		transferID: "first", sid: "lab", tier: "b", bucket: bucket,
		size: proto.XferMaxBytes,
	}
	second := &transferEntry{
		transferID: "second", sid: "lab", tier: "b", bucket: bucket,
		size: proto.XferMaxBytes,
	}
	if code := tracker.put(first); code != "" {
		t.Fatalf("first transfer unexpectedly refused: %s", code)
	}
	if code := tracker.put(second); code == "" {
		t.Fatalf("tracker admitted two %d-byte objects into one %d-byte bucket; both pass per-request "+
			"admission but their aggregate cannot fit, so one fails mid-Put", proto.XferMaxBytes, xferBucketCap)
	}
}

// origin: smalldisk external review F6
// xferMaxBytesForCeiling returns the backing stream's MaxBytes. On a
// disk-bound broker that value is below the global cap, but it still includes
// the bytes JetStream spends on chunks and metadata. Admission must reserve
// the margin instead of accepting a payload equal to the stream limit.
func TestDiskBoundBucketReservesChunkMarginFromAdmission(t *testing.T) {
	const ceiling = int64(4 * 1024 * 1024 * 1024)
	bucketMax, err := xferMaxBytesForCeiling(ceiling, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bucketMax >= xferBucketCap {
		t.Fatalf("fixture is not disk-bound: bucketMax=%d cap=%d", bucketMax, xferBucketCap)
	}
	js := &countingJS{maxStore: ceiling}
	b := newProvisionBroker(js)

	bucket, tooLarge, perr := b.provisionXferBucket(context.Background(), "lab", bucketMax)
	if perr != nil {
		t.Fatalf("capacity boundary became provisioning error: %v", perr)
	}
	if tooLarge == nil {
		t.Fatalf("admitted payload=%d equal to backing MaxBytes=%d without reserving the %d-byte "+
			"chunk margin; this will fail mid-Put", bucketMax, bucketMax, xferBucketChunkMargin)
	}
	if tooLarge.MaxBytes > bucketMax-int64(xferBucketChunkMargin) {
		t.Fatalf("reported payload ceiling=%d, want at most bucket max minus margin=%d", tooLarge.MaxBytes,
			bucketMax-int64(xferBucketChunkMargin))
	}
	if bucket != "" || js.creates != 0 {
		t.Fatalf("oversize payload should be refused before create: bucket=%q creates=%d", bucket, js.creates)
	}
}
