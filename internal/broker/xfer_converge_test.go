package broker

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// xfer_converge_test.go — the in-band repair of an OBJ_xfer bucket whose reservation the JetStream
// store can no longer honour, and the equally load-bearing guarantee that a HEALTHY bucket is left
// alone.
//
// Why the repair has to exist at all: nats-server gates STREAM.CREATE on a server-level check
// (storeReserved + requested > MaxStore) that counts the target bucket's OWN existing reservation,
// and it runs before the server looks at the name. So on a store whose reservations reach the
// ceiling, an already-present bucket cannot be re-created at ANY size — not even a smaller one. The
// update path is the only way out, because a shrink charges zero additional bytes. Fixing the sizing
// arithmetic alone therefore leaves such a broker exactly as broken as before.
//
// Why "leave it alone" is equally load-bearing: this package states that stream limits are
// operator-owned (internal/jsstream, raiseXferReplicas). Shrinking whenever tether would have picked
// a different number would clobber that. The trigger is unsatisfiability, not disagreement — and
// without the second test below, the first one is satisfied just as well by "always normalise".
// origin: docs/reviews/smalldisk-plan.md §2 裁决 + C4.

// convergeJS is a JetStream fake for the resolve → raise → converge path.
type convergeJS struct {
	jetstream.JetStream

	// store shape reported by AccountInfo
	maxStore         uint64
	reserved         uint64
	used             uint64
	accountUnlimited bool // production shape: account limit -1, server limit finite

	// the existing backing stream (nil ⇒ not found)
	existing *jetstream.StreamInfo

	updates []jetstream.ObjectStoreConfig // every UpdateObjectStore config, in order
	creates int
}

func (c *convergeJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	// accountUnlimited reproduces the PRODUCTION shape: tether renders no account JWT limits, so the
	// account reports MaxStore = -1 while the SERVER still holds a finite disk-derived limit.
	maxStore := int64(c.maxStore)
	if c.accountUnlimited {
		maxStore = -1
	}
	return &jetstream.AccountInfo{Tier: jetstream.Tier{
		Store:         c.used,
		ReservedStore: c.reserved,
		Limits:        jetstream.AccountLimits{MaxStore: maxStore},
	}}, nil
}

func (c *convergeJS) Stream(context.Context, string) (jetstream.Stream, error) {
	if c.existing == nil {
		return nil, jetstream.ErrStreamNotFound
	}
	return &fakeStream{info: c.existing}, nil
}

func (c *convergeJS) CreateObjectStore(context.Context, jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	c.creates++
	return nil, nil
}

func (c *convergeJS) UpdateObjectStore(_ context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	c.updates = append(c.updates, cfg)
	return nil, nil
}

// ObjectStore/Status are what raiseXferReplicas walks; a single replica at target means it no-ops.
func (c *convergeJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return &fakeObjStore{replicas: 1}, nil
}

type fakeStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (f *fakeStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return f.info, nil
}
func (f *fakeStream) CachedInfo() *jetstream.StreamInfo { return f.info }

type fakeObjStore struct {
	jetstream.ObjectStore
	replicas int
}

func (f *fakeObjStore) Status(context.Context) (jetstream.ObjectStoreStatus, error) {
	return &fakeObjStatus{replicas: f.replicas}, nil
}

type fakeObjStatus struct {
	jetstream.ObjectStoreStatus
	replicas int
}

func (f *fakeObjStatus) Replicas() int { return f.replicas }

func streamInfo(maxBytes int64, usedBytes uint64) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{MaxBytes: maxBytes, Replicas: 1},
		State:  jetstream.StreamState{Bytes: usedBytes},
	}
}

const gib = int64(1024 * 1024 * 1024)

func convergeBroker(js jetstream.JetStream) *Broker {
	b := &Broker{transfers: newTransferTracker()}
	b.cfg.Logger = silentLogger()
	b.js = js
	return b
}

// TestOversizedBucketIsShrunkWhenTheStoreCannotHonourIt is racknerd's exact shape: an EMPTY bucket
// carrying the legacy 8 GiB reservation on a 10.33 GiB store whose reservations already total
// 10 GiB. Every create is refused while that stands, so the bucket must be converged in place.
func TestOversizedBucketIsShrunkWhenTheStoreCannotHonourIt(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	// racknerd's real shape: reservations (10 GiB) have grown PAST what the store can carry
	// (~9 GiB), which is what makes every stream create fail. The oversized bucket is 8 GiB of
	// that, and it is empty.
	js := &convergeJS{
		maxStore: uint64(9 * gib),      // effective ceiling
		reserved: uint64(10 * gib),     // OVER it: events + history + the 8 GiB bucket
		existing: streamInfo(8*gib, 0), // empty, never used
	}
	b := convergeBroker(js)

	bucket, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target)
	if err != nil {
		t.Fatalf("ensure returned %v; an existing bucket must resolve, never fail", err)
	}
	if bucket == "" {
		t.Fatal("no bucket name returned")
	}
	if js.creates != 0 {
		t.Errorf("CreateObjectStore was called %d time(s) for an EXISTING bucket. The create path is "+
			"gated by a server-level storage check that counts this bucket's own reservation, so on a "+
			"full store it returns 10047 rather than 'already exists' — resolving first is the whole "+
			"point", js.creates)
	}
	if len(js.updates) != 1 {
		t.Fatalf("expected exactly one shrink, got %d — an unsatisfiable reservation blocks every "+
			"stream create on the broker until it is reduced", len(js.updates))
	}
	if got := js.updates[0].MaxBytes; got != target {
		t.Errorf("shrank to %d, want the computed target %d", got, target)
	}
}

// TestHealthyBucketIsNeverResized is the other half, and without it the test above is satisfied by
// "always normalise" — which would clobber an operator's deliberate `nats stream edit`. Same
// oversized bucket, but a store with room to carry it.
func TestHealthyBucketIsNeverResized(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	// A store with room, but NOT so much room that a buggy predicate and a correct one agree.
	// The first version used 500 GiB against 10 GiB reserved — 50x headroom, where
	// `reserved > ceiling` and the double-counting `reserved + cur > ceiling` give the SAME answer,
	// so the guard could not tell them apart. These numbers are chosen so they diverge: the bucket
	// (4.5 GiB) is larger than the REMAINING headroom (3.5 GiB), which is the ordinary condition for
	// a broker's biggest stream and exactly where the double-count fires on a healthy store.
	js := &convergeJS{
		maxStore: uint64(10 * gib),       // ceiling
		reserved: uint64(13 * gib / 2),   // 6.5 GiB: events 1 + history 1 + THIS bucket 4.5
		existing: streamInfo(9*gib/2, 0), // 4.5 GiB, the operator's deliberate size
	}
	b := convergeBroker(js)

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("tether resized a bucket the store can carry (%+v). Stream limits are "+
			"operator-owned; the trigger is 'the store cannot honour this', not 'tether would have "+
			"picked a different number'", js.updates)
	}
}

// origin: smalldisk external review F4
// ReservedStore already includes the existing bucket and resolve-before-create
// means no second bucket is being added. If current reservations are within the
// ceiling, the current configuration is satisfiable; adding `target` asks
// whether an imaginary additional bucket would fit and wrongly authorizes an
// operator-owned limit change.
func TestExistingBucketWithinStoreLimitIsNotShrunkForAnImaginarySecondCopy(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	js := &convergeJS{
		maxStore: uint64(10*gib + gib/3), // 10.33 GiB effective limit
		reserved: uint64(10 * gib),       // current reservations fit
		existing: streamInfo(8*gib, 0),   // already resolved; no create is needed
	}
	b := convergeBroker(js)

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target); err != nil {
		t.Fatal(err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("shrank a currently satisfiable bucket because a fictitious second target would not fit: %+v; "+
			"ReservedStore already includes this bucket and resolve-before-create issues no create", js.updates)
	}
}

// TestOversizedBucketInUseIsNotShrunkBelowItsBytes: shrinking under the stored bytes is rejected by
// UpdateObjectStore and is how replication_degraded gets latched. Staying oversized is the safe
// outcome, and it must be logged rather than silently skipped.
func TestOversizedBucketInUseIsNotShrunkBelowItsBytes(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	js := &convergeJS{
		maxStore: uint64(10*gib + gib/3),
		reserved: uint64(10 * gib),
		existing: streamInfo(8*gib, uint64(3*gib)), // 3 GiB already stored — target is BELOW that
	}
	b := convergeBroker(js)

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("shrank a bucket below its stored bytes (%+v) — UpdateObjectStore rejects that, and "+
			"the rejection latches replication_degraded", js.updates)
	}
}

// TestMissingBucketStillTakesTheCreatePath: resolve-before-create must not disable creation.
func TestMissingBucketStillTakesTheCreatePath(t *testing.T) {
	js := &convergeJS{maxStore: uint64(500 * gib), existing: nil}
	b := convergeBroker(js)

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, gib); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if js.creates != 1 {
		t.Fatalf("CreateObjectStore called %d time(s) for a bucket that does not exist, want 1", js.creates)
	}
	if len(js.updates) != 0 {
		t.Fatalf("a fresh bucket must not be updated: %+v", js.updates)
	}
}

// TestStoreCapacityProbeFailsClosedTowardDoingNothing: every unreadable input must answer "healthy",
// so an unknown store leaves the operator's configuration untouched instead of triggering a shrink.
func TestStoreCapacityProbeFailsClosedTowardDoingNothing(t *testing.T) {
	cases := []struct {
		name string
		js   jetstream.JetStream
	}{
		{"no jetstream", nil},
		{"unlimited store and no StoreDir (no ceiling at all)", &convergeJS{maxStore: 0, reserved: uint64(99 * gib)}},
		{"AccountInfo error", &erroringAccountJS{}},
	}
	for _, tc := range cases {
		b := convergeBroker(tc.js)
		if b.xferAccountIsOverCommitted(context.Background()) {
			t.Errorf("%s: reported the store cannot honour the reservation; an unreadable store must "+
				"be treated as healthy so nothing is resized on a guess", tc.name)
		}
	}
}

type erroringAccountJS struct{ jetstream.JetStream }

func (e *erroringAccountJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	return nil, errors.New("no responders")
}

// TestStorageRefusalNamesReservedVsUsed is C5: nats says only "insufficient storage resources
// available", and on the incident broker that was 99.5% RESERVED against 0.5% USED — an operator who
// reads it, checks `df`, and sees free disk has nowhere to go. The refusal must therefore carry the
// reserved-vs-used split, which is the distinction that actually explains it.
//
// Mutation that must redden this: drop the xferStoreAccounting call from the create-error wrap, or
// have it return the same text for a non-storage error.
func TestStorageRefusalNamesReservedVsUsed(t *testing.T) {
	js := &refusingJS{
		maxStore: uint64(10*gib + gib/3),
		reserved: uint64(10 * gib),
		used:     uint64(47 * 1024 * 1024),
		err:      &jetstream.APIError{ErrorCode: jsErrCodeStorageExceeded, Description: "insufficient storage resources available"},
	}
	b := convergeBroker(js)

	_, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 2*gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	msg := err.Error()
	for _, want := range []string{"RESERVED", "actually used", "nats stream ls"} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal does not mention %q; an operator cannot tell reserved-but-empty bytes "+
				"from used ones. got: %s", want, msg)
		}
	}
}

// TestNonStorageRefusalIsNotDecorated: the enrichment must be specific to the storage case. Pasting
// store accounting onto an unrelated failure would misdirect the reader — the same defect class,
// pointing the other way.
func TestNonStorageRefusalIsNotDecorated(t *testing.T) {
	js := &refusingJS{
		maxStore: uint64(10 * gib),
		reserved: uint64(1 * gib),
		err:      &jetstream.APIError{ErrorCode: 10052, Description: "something else entirely"},
	}
	b := convergeBroker(js)

	_, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	if strings.Contains(err.Error(), "RESERVED") {
		t.Fatalf("a non-storage failure was decorated with store accounting: %s", err.Error())
	}
}

// refusingJS has no existing bucket and fails every create with a chosen API error.
type refusingJS struct {
	jetstream.JetStream
	maxStore, reserved, used uint64
	accountUnlimited         bool
	err                      error
}

func (r *refusingJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	maxStore := int64(r.maxStore)
	if r.accountUnlimited {
		maxStore = -1
	}
	return &jetstream.AccountInfo{Tier: jetstream.Tier{
		Store: r.used, ReservedStore: r.reserved,
		Limits: jetstream.AccountLimits{MaxStore: maxStore},
	}}, nil
}
func (r *refusingJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, jetstream.ErrStreamNotFound
}
func (r *refusingJS) CreateObjectStore(context.Context, jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	return nil, r.err
}

// A statfs ceiling is in server reservation units, while AccountInfo.ReservedStore is in
// replica-weighted account units. It is also only a current-free-space estimate, not the exact
// server MaxStore fixed at JetStream startup. That pair cannot prove an operator-owned limit is
// unsatisfiable, so unlimited accounts must never authorize an automatic shrink. Resolve-before-
// create already makes an existing legacy bucket usable without recreating it.
func TestConvergenceDoesNotGuessFromStatfsWhenTheAccountIsUnlimited(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	js := &convergeJS{
		accountUnlimited: true,             // <- production: account says "no limit"
		reserved:         uint64(10 * gib), // but 10 GiB is already reserved — past the 9 GiB ceiling
		used:             uint64(47 * 1024 * 1024),
		existing:         streamInfo(8*gib, 0), // the legacy oversized, empty bucket
	}
	b := convergeBroker(js)
	// Production takes the statfs branch (the account limit is -1), so pin it to a KNOWN store size
	// instead of whatever this host's filesystem reports — otherwise the verdict depends on the CI
	// machine's free space rather than on the code.
	b.cfg.StoreDir = t.TempDir()
	restore := pinStoreSize(t, 16*gib, 4*gib) // total 16, used 4 => ceiling 0.75*12 = 9 GiB < 10 reserved
	defer restore()

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("statfs estimate authorized an operator-owned shrink with an unlimited account: %+v", js.updates)
	}
}

// TestStorageAccountingSurvivesAnUnlimitedAccountLimit is the same trap on the C5 message: gating the
// enrichment on the account limit would print nothing in exactly the deployment it exists for.
func TestStorageAccountingSurvivesAnUnlimitedAccountLimit(t *testing.T) {
	js := &refusingJS{
		accountUnlimited: true,
		reserved:         uint64(10 * gib),
		used:             uint64(47 * 1024 * 1024),
		err:              &jetstream.APIError{ErrorCode: jsErrCodeStorageExceeded, Description: "insufficient storage resources available"},
	}
	b := convergeBroker(js)
	b.cfg.StoreDir = t.TempDir()
	restore := pinStoreSize(t, 16*gib, 4*gib)
	defer restore()

	_, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 2*gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	if !strings.Contains(err.Error(), "RESERVED") {
		t.Fatalf("no store accounting on the production account shape: %s", err.Error())
	}
}

// pinStoreSize replaces the statfs reading with fixed numbers for the duration of a test, so a
// ceiling-dependent assertion measures the CODE and not the host's free space.
func pinStoreSize(t *testing.T, total, used int64) func() {
	t.Helper()
	prev := xferDiskUsage
	xferDiskUsage = func(string) (uint64, uint64, error) { return uint64(used), uint64(total), nil }
	return func() { xferDiskUsage = prev }
}

// ── internal review findings, made durable ──────────────────────────────────────────────────────
//
// Three MAJORs the adversarial review found in the first cut of convergeOversizedXferBucket. Each is
// reproduced here as the scenario that actually failed, not as a paraphrase.

// TestConvergenceDoesNotFireWhenOurOwnBucketIsTheReservation is the double-count regression.
//
// The trigger asked "could the store take ANOTHER `cur` bytes on top of what is reserved" — but
// ReservedStore ALREADY INCLUDES this bucket, so the question double-counted it. Measured failure: a
// 16 GiB store with 10 GiB reserved of which 8 GiB is our own bucket has 6 GiB genuinely free and
// blocks nothing, yet the repair overwrote the operator's 8 GiB with ~2 GiB. Over-firing is not a
// small mistake: the ONLY justification for touching an operator-owned limit is that the
// configuration is unsatisfiable, and here it plainly was not.
//
// Mutation that must redden this: compare `reserved + cur > ceiling` instead of `reserved > ceiling`.
func TestConvergenceDoesNotFireWhenOurOwnBucketIsTheReservation(t *testing.T) {
	js := &convergeJS{
		maxStore: uint64(16 * gib),     // room to spare
		reserved: uint64(10 * gib),     // events 1 + history 1 + THIS bucket 8 => 6 GiB free
		existing: streamInfo(8*gib, 0), // the operator's deliberate 8 GiB
	}
	b := convergeBroker(js)

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 2*gib+64*1024*1024); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("shrank an operator-owned limit on a store with 6 GiB free (%+v). ReservedStore "+
			"already contains this bucket, so adding it again asks whether a SECOND copy would fit — "+
			"a question nobody needs answered", js.updates)
	}
}

// TestConvergencePreservesTheReplicaFactor: replication is raise-only and cluster-owned. A capacity
// repair that writes the local target replica count silently DOWNGRADES a 3-replica bucket to 1.
//
// Mutation that must redden this: put `targetReplicas` back in the shrink's ObjectStoreConfig.
func TestConvergencePreservesTheReplicaFactor(t *testing.T) {
	js := &replica3JS{convergeJS{
		maxStore: uint64(9 * gib),
		reserved: uint64(10 * gib), // over-committed => convergence fires
		existing: &jetstream.StreamInfo{
			Config: jetstream.StreamConfig{MaxBytes: 8 * gib, Replicas: 3},
			State:  jetstream.StreamState{Bytes: 0},
		},
	}}
	b := convergeBroker(js)

	// Target replicas 1 — LOWER than the stream's 3, which is the trap.
	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 2*gib+64*1024*1024); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 1 {
		t.Fatalf("expected one shrink, got %d", len(js.updates))
	}
	if got := js.updates[0].Replicas; got != 3 {
		t.Fatalf("shrink wrote Replicas=%d, want the stream's existing 3. Losing redundancy as a "+
			"side effect of a capacity repair is not something this path is entitled to do", got)
	}
}

// replica3JS reports an existing bucket at 3 replicas, so raiseXferReplicas no-ops (raise-only) and
// the shrink is the only writer.
type replica3JS struct{ convergeJS }

func (c *replica3JS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return &fakeObjStore{replicas: 3}, nil
}

// TestConvergenceDefersWhileATransferIsInFlight: State.Bytes reports what is STORED, not what an
// admitted transfer is still going to send. A 1.4 GiB object 600 MiB into its Put looks like "600 MiB
// used" and sails past a bytes-only check; shrinking under it makes the upload hit DiscardNew and die
// mid-stream — the very failure this increment exists to prevent, caused by the repair itself.
//
// Mutation that must redden this: delete the b.transfers.activeOBJStreams() check.
func TestConvergenceDefersWhileATransferIsInFlight(t *testing.T) {
	js := &convergeJS{
		maxStore: uint64(3 * gib),
		reserved: uint64(4 * gib),                            // over-committed => the trigger says repair
		existing: streamInfo(3*gib/2, uint64(600*1024*1024)), // 1.5 GiB cap, 600 MiB streamed so far
	}
	b := convergeBroker(js)
	// An admitted, in-flight transfer owning this bucket.
	b.transfers.put(&transferEntry{transferID: "t1", sid: "lab", bucket: proto.XferBucketName("lab"), tier: "b"})

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 805306368); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("shrank to %+v while a transfer was in flight — the remaining bytes of an already "+
			"admitted object would hit DiscardNew", js.updates)
	}
}

// TestConvergenceIsInertWithoutATracker: an unreadable input must mean "leave it alone", never
// "assume idle". It also must not panic — this runs on the push path.
func TestConvergenceIsInertWithoutATracker(t *testing.T) {
	js := &convergeJS{maxStore: uint64(9 * gib), reserved: uint64(10 * gib), existing: streamInfo(8*gib, 0)}
	b := &Broker{} // no tracker, no logger
	b.cfg.Logger = silentLogger()
	b.js = js

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, 2*gib); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if len(js.updates) != 0 {
		t.Fatalf("shrank without being able to check for in-flight transfers: %+v", js.updates)
	}
}

// origin: smalldisk external review F1
// A capacity repair owns exactly MaxBytes. Rebuilding ObjectStoreConfig from
// zero values silently erases every other operator-owned setting when the
// shrink succeeds, even though the surrounding code promises those settings
// survive reconciliation.
func TestConvergencePreservesOperatorOwnedObjectStoreConfig(t *testing.T) {
	placement := &jetstream.Placement{Cluster: "edge", Tags: []string{"ssd", "rack-a"}}
	metadata := map[string]string{"owner": "ops", "purpose": "transfer"}
	js := &convergeJS{
		maxStore: uint64(9 * gib),
		reserved: uint64(10 * gib),
		existing: &jetstream.StreamInfo{
			Config: jetstream.StreamConfig{
				MaxBytes:    8 * gib,
				Replicas:    1,
				Storage:     jetstream.FileStorage,
				Description: "operator description",
				MaxAge:      37 * time.Minute,
				Placement:   placement,
				Compression: jetstream.S2Compression,
				Metadata:    metadata,
			},
		},
	}
	b := convergeBroker(js)
	target := 2*gib + 64*1024*1024

	if _, _, err := ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, target); err != nil {
		t.Fatal(err)
	}
	if len(js.updates) != 1 {
		t.Fatalf("updates=%d, want one shrink", len(js.updates))
	}
	got := js.updates[0]
	if got.Description != "operator description" || got.TTL != 37*time.Minute ||
		got.Storage != jetstream.FileStorage || !got.Compression ||
		!reflect.DeepEqual(got.Placement, placement) || !reflect.DeepEqual(got.Metadata, metadata) {
		t.Fatalf("shrink clobbered operator-owned config: got=%+v; placement=%+v metadata=%v", got, got.Placement, got.Metadata)
	}
}

// origin: smalldisk external review F2
// Account reservations multiply MaxBytes by replicas, but the server-level
// storeReserved limit does not. Production accounts are unlimited, so a
// statfs-derived ceiling represents the SERVER limit and must use server
// units. Multiplying it by R can reject a healthy N=3 broker outright.
func TestServerDerivedCeilingUsesServerReservationUnits(t *testing.T) {
	js := &convergeJS{accountUnlimited: true}
	b := convergeBroker(js)
	b.cfg.StoreDir = t.TempDir()
	b.AttachXferReplicas(func() int { return 3 })
	restore := pinStoreSize(t, 10*gib, 0) // server ceiling = 7.5 GiB
	defer restore()

	got, err := b.xferBucketMaxBytes(context.Background())
	if err != nil {
		t.Fatalf("healthy server-limited N=3 store was refused: %v", err)
	}
	if got != xferBucketCap {
		t.Fatalf("server-derived ceiling used account replica units: got %d, want cap %d; "+
			"server storeReserved charges events+history once, not once per replica", got, xferBucketCap)
	}
}

// origin: smalldisk external review F2
// A finite account limit is only one of nats-server's two checks. The server
// can have a much smaller disk-derived limit, so returning the account value
// early oversizes the bucket and merely defers the verdict to a 10047 create
// failure instead of making the small broker usable.
func TestSizingUsesTheTighterAccountAndServerLimits(t *testing.T) {
	js := &convergeJS{maxStore: uint64(100 * gib)} // generous account limit
	b := convergeBroker(js)
	b.cfg.StoreDir = t.TempDir()
	restore := pinStoreSize(t, 4*gib, 0) // server estimate = 3 GiB
	defer restore()

	got, err := b.xferBucketMaxBytes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want, err := xferMaxBytesForCeiling(3*gib, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("sizing ignored tighter server limit: got %d from 100 GiB account, want %d from 3 GiB server",
			got, want)
	}
}

// origin: smalldisk external review F7
// Operator-owned limits can be smaller as well as larger than tether's new
// target. The request gate must use the existing backing stream's real
// MaxBytes; otherwise a valid-looking prepare succeeds and ObjectStore.Put
// discovers the limit only after transmission has begun.
func TestExistingSmallerBucketConstrainsPrepareAdmission(t *testing.T) {
	const existingMax = int64(512 * 1024 * 1024)
	const requestSize = int64(700 * 1024 * 1024)
	js := &convergeJS{
		maxStore: uint64(10 * gib),
		reserved: uint64(3 * gib),
		existing: streamInfo(existingMax, 0),
	}
	b := convergeBroker(js)
	b.runCtx = context.Background()

	bucket, tooLarge, perr := b.provisionXferBucket(context.Background(), "lab", requestSize)
	if perr != nil {
		t.Fatalf("existing bucket lookup failed: %v", perr)
	}
	if tooLarge == nil {
		t.Fatalf("prepare admitted %d bytes against an existing %d-byte bucket; bucket=%q — Put will fail mid-stream",
			requestSize, existingMax, bucket)
	}
	if tooLarge.MaxBytes > existingMax-int64(xferBucketChunkMargin) {
		t.Fatalf("reported payload ceiling=%d exceeds existing bucket capacity after margin=%d",
			tooLarge.MaxBytes, existingMax-int64(xferBucketChunkMargin))
	}
}

// origin: smalldisk external review F7
// A crash can leave an orphan object until the periodic reaper runs. Stream
// MaxBytes is shared by all objects, so prepare must subtract State.Bytes as
// well as overhead; checking only the configured maximum admits a transfer
// that cannot coexist with the bytes already present.
func TestExistingBucketStoredBytesConstrainPrepareAdmission(t *testing.T) {
	const stored = int64(200 * 1024 * 1024)
	const requestSize = int64(proto.XferMaxBytes)
	existingMax := int64(xferBucketCap)
	js := &convergeJS{
		maxStore: uint64(10 * gib),
		reserved: uint64(4 * gib),
		existing: streamInfo(existingMax, uint64(stored)),
	}
	b := convergeBroker(js)
	b.runCtx = context.Background()

	_, tooLarge, perr := b.provisionXferBucket(context.Background(), "lab", requestSize)
	if perr != nil {
		t.Fatal(perr)
	}
	if tooLarge == nil {
		t.Fatalf("admitted %d bytes although bucket max=%d already stores %d bytes and needs %d overhead; "+
			"the orphan reaper has not freed enough capacity", requestSize, existingMax, stored, xferBucketChunkMargin)
	}
	wantMax := existingMax - stored - int64(xferBucketChunkMargin)
	if tooLarge.MaxBytes > wantMax {
		t.Fatalf("reported ceiling=%d, want <= %d after stored bytes and margin", tooLarge.MaxBytes, wantMax)
	}
}

// ── external review F3 / F4: the two fixes that shipped without a guard ─────────────────────────
//
// Both were real defects and both were fixed correctly. Neither had a test that reddens when the fix
// is removed — I checked by mutation. A fix with no guard is one refactor away from being undone
// silently, and these two are exactly the kind a later "simplification" would target: one looks like
// a redundant loop, the other like an unnecessary subtraction.

// TestTierBIsSerializedPerSessionBucket pins external review F3.
//
// The bucket is sized for ONE maximum object plus overhead, so two tier-B transfers admitted against
// the same bucket can both pass admission and then collide in DiscardNew mid-Put. Admission cannot
// catch it by arithmetic: State.Bytes does not include an in-flight object's unwritten remainder,
// and the PULL leg carries no size at all (the broker only learns it from the agent), so there is no
// number to accumulate. Serialising the bucket is the available correct answer.
//
// Mutation that must redden this: delete the bucket-collision loop in transferTracker.put.
func TestTierBIsSerializedPerSessionBucket(t *testing.T) {
	tr := newTransferTracker()
	bucket := proto.XferBucketName("lab")

	if code := tr.put(&transferEntry{transferID: "t1", sid: "lab", bucket: bucket, tier: "b"}); code != "" {
		t.Fatalf("first tier-B transfer rejected: %s", code)
	}
	if code := tr.put(&transferEntry{transferID: "t2", sid: "lab", bucket: bucket, tier: "b"}); code == "" {
		t.Fatal("a SECOND tier-B transfer was admitted for the same bucket. Both would pass admission " +
			"and then race into DiscardNew — the mid-Put failure this increment exists to prevent")
	}

	// Tier A carries no bucket and must stay concurrent: it never touches the object store.
	if code := tr.put(&transferEntry{transferID: "t3", sid: "lab", tier: "a"}); code != "" {
		t.Fatalf("tier-A transfer rejected (%s); it uses no bucket and must not be serialised", code)
	}
	// A DIFFERENT session has its own bucket and must stay concurrent too — otherwise this guard
	// would serialise the whole broker rather than one bucket.
	if code := tr.put(&transferEntry{transferID: "t4", sid: "ops", bucket: proto.XferBucketName("ops"), tier: "b"}); code != "" {
		t.Fatalf("a different session's tier-B was rejected (%s); the constraint is per BUCKET", code)
	}
	// And the slot frees when the first one finishes.
	tr.remove("t1")
	if code := tr.put(&transferEntry{transferID: "t5", sid: "lab", bucket: bucket, tier: "b"}); code != "" {
		t.Fatalf("bucket still blocked after the holder was removed (%s) — the serialisation would be "+
			"permanent, not per-transfer", code)
	}
}

// TestPayloadLimitSubtractsChunkOverheadAndUsedBytes pins external review F4.
//
// A stream's MaxBytes is not payload the caller may send: it also has to hold JetStream's 128 KiB
// chunk framing and whatever objects are already there. Admitting a payload equal to MaxBytes is the
// same mid-Put failure as F3, arrived at by arithmetic instead of by racing.
//
// Mutations that must redden this: drop the `- xferBucketChunkMargin` term, or the `- used` term.
func TestPayloadLimitSubtractsChunkOverheadAndUsedBytes(t *testing.T) {
	const margin = int64(xferBucketChunkMargin)

	// Empty bucket: payload is the reservation minus framing headroom, never the reservation itself.
	if got, want := xferPayloadLimit(gib, 0), gib-margin; got != want {
		t.Errorf("empty bucket: payload limit %d, want %d (MaxBytes minus chunk overhead)", got, want)
	}
	// Bytes already stored are not available to the next transfer.
	if got, want := xferPayloadLimit(gib, uint64(gib/2)), gib-margin-gib/2; got != want {
		t.Errorf("half-full bucket: payload limit %d, want %d (existing bytes are not payload)", got, want)
	}
	// Full enough that nothing fits ⇒ zero, never a negative that would read as "unlimited" upstream.
	if got := xferPayloadLimit(gib, uint64(gib)); got != 0 {
		t.Errorf("over-full bucket: payload limit %d, want 0", got)
	}
	// The global transfer cap still binds on a huge bucket.
	if got, want := xferPayloadLimit(1<<62, 0), int64(proto.XferMaxBytes); got != want {
		t.Errorf("huge bucket: payload limit %d, want the global cap %d", got, want)
	}
	// A non-positive MaxBytes is JetStream's UNLIMITED, so only the global cap applies — and the
	// answer must never be negative or zero, which upstream would misread.
	if got, want := xferPayloadLimit(0, 0), int64(proto.XferMaxBytes); got != want {
		t.Errorf("unlimited bucket: payload limit %d, want the global cap %d", got, want)
	}
}
