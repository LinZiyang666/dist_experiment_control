package broker

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
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
//
// SINCE HOT-UPDATE S7 every write here goes through the real configuration owner
// (xferbucket.NewWithCensus, bound by bindConvergeOwner exactly as the broker transfer factory
// binds it), and the owner writes FULL StreamConfigs: an ObjectStoreConfig projection cannot carry
// Duplicates and would erase the recovery seal (plan §2.1 / M14). So what these tests used to count
// as UpdateObjectStore calls is now counted twice, on both sides of the owner:
//
//   - shrinks: the reservation reductions the broker's repair asked the owner for (convergeOwner);
//   - writes:  the full-config UPDATEs that actually reached the stream (convergeJS), classified by
//     what they changed. The owner's ensure may raise the Duplicates recovery window (that is the
//     one change the plan mandates); any other changed field is an operator-owned limit tether
//     touched, which is what "updated"/"resized" meant in every assertion below.
//
// Asking for both keeps each test's original strength: the broker's DECISION is observable even
// where the owner's own guards (idle census, stored bytes, raise-only replicas) would also have
// refused the write, and a successful repair is proven by the bytes that reached the stream.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof; S7 lead integration

// convergeJS is a JetStream fake for the resolve → configure → converge path. It holds ONE backing
// stream whose full config every INFO reads live, so the owner's confirming readbacks see its own
// writes, as a real stream handle does. Every method the provisioning path reaches is modelled; an
// unmodelled one fails the calling test through convergeEnsure instead of aborting the package.
type convergeJS struct {
	jetstream.JetStream

	// store shape reported by AccountInfo
	maxStore         uint64
	reserved         uint64
	used             uint64
	accountUnlimited bool // production shape: account limit -1, server limit finite

	// the existing backing stream (nil ⇒ not found until a CREATE makes it)
	existing *jetstream.StreamInfo

	writes     []convergeWrite // every full-config UpdateStream, in order
	creates    int
	sdkUpdates int // UpdateObjectStore projections: the owner must never send one
}

// convergeWrite is one full-config UPDATE together with the config it replaced.
type convergeWrite struct{ before, after jetstream.StreamConfig }

// recoveryWindowOnly reports whether w changed nothing but a RAISED Duplicates window — the
// owner's mandated recovery seal, which is not an operator-owned limit.
func (w convergeWrite) recoveryWindowOnly() bool {
	after := w.after
	after.Duplicates = w.before.Duplicates
	return w.after.Duplicates > w.before.Duplicates && reflect.DeepEqual(after, w.before)
}

// operatorWrites are the writes that changed an operator-owned field (anything but the raised
// recovery window). Every "the bucket must not be updated/resized" assertion means: none of these.
func (c *convergeJS) operatorWrites() []convergeWrite {
	var out []convergeWrite
	for _, w := range c.writes {
		if !w.recoveryWindowOnly() {
			out = append(out, w)
		}
	}
	return out
}

// reservationShrinks are the writes that lowered MaxBytes.
func (c *convergeJS) reservationShrinks() []convergeWrite {
	var out []convergeWrite
	for _, w := range c.writes {
		if w.after.MaxBytes < w.before.MaxBytes {
			out = append(out, w)
		}
	}
	return out
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

func (c *convergeJS) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	if c.existing == nil || name != c.existing.Config.Name {
		return nil, jetstream.ErrStreamNotFound
	}
	return &fakeStream{js: c}, nil
}

// CreateObjectStore makes the backing stream the SDK would: the configured reservation and the
// server's default two-minute duplicate window, which the owner then raises to the recovery seal.
func (c *convergeJS) CreateObjectStore(_ context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	c.creates++
	c.existing = &jetstream.StreamInfo{Config: jetstream.StreamConfig{
		Name: "OBJ_" + cfg.Bucket, MaxBytes: cfg.MaxBytes, Replicas: cfg.Replicas, Storage: cfg.Storage,
		Duplicates: 2 * time.Minute, Discard: jetstream.DiscardNew,
	}}
	return nil, nil
}

// UpdateStream is the owner's full-config UPDATE.
func (c *convergeJS) UpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if c.existing == nil || cfg.Name != c.existing.Config.Name {
		return nil, jetstream.ErrStreamNotFound
	}
	c.writes = append(c.writes, convergeWrite{before: c.existing.Config, after: cfg})
	c.existing.Config = cfg
	return &fakeStream{js: c}, nil
}

// UpdateObjectStore is the SDK projection that drops Duplicates. It is counted and refused so a
// writer that bypasses the owner is visible rather than silently "succeeding".
func (c *convergeJS) UpdateObjectStore(context.Context, jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	c.sdkUpdates++
	return nil, errors.New("convergeJS: UpdateObjectStore bypasses the configuration owner")
}

// Conn has no server behind it; the owner then seals MaxPayload 0 (fail closed for v1).
func (c *convergeJS) Conn() *nats.Conn { return nil }

// ObjectStore/Status are what raiseXferReplicas walks; a single replica at target means it no-ops.
func (c *convergeJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return &fakeObjStore{replicas: 1}, nil
}

// fakeStream reads the CURRENT config on every Info: a snapshot taken at lookup would hide the
// owner's own UPDATE from its confirming readback.
type fakeStream struct {
	jetstream.Stream
	js *convergeJS
}

func (f *fakeStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	info := *f.js.existing
	return &info, nil
}
func (f *fakeStream) CachedInfo() *jetstream.StreamInfo {
	info := *f.js.existing
	return &info
}

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

// convergeStream is the backing stream name of session "lab"'s bucket — the one every test here
// provisions. A real INFO always carries the name, and the owner writes exactly what it read back.
const convergeStream = "OBJ_xfer-lab"

func streamInfo(maxBytes int64, usedBytes uint64) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{
		// Model an already-created/converged tether bucket. Migration from the
		// unsafe old policy is separately exercised against real JetStream;
		// these tests keep their strict resize/replica/operator-field oracles.
		// origin: S7 internal review FIX-CONVERGE F1
		Config: jetstream.StreamConfig{Name: convergeStream, MaxBytes: maxBytes, Replicas: 1, Discard: jetstream.DiscardNew},
		State:  jetstream.StreamState{Bytes: usedBytes},
	}
}

const gib = int64(1024 * 1024 * 1024)

// convergeBroker is a single broker with the legacy tracker and, whenever there is a JetStream to
// configure, the real configuration owner bound in front of it (bindConvergeOwner).
func convergeBroker(t *testing.T, js jetstream.JetStream) *Broker {
	t.Helper()
	b := &Broker{transfers: xfertrack.NewLegacyTracker()}
	b.cfg.Logger = silentLogger()
	setBrokerJS(b, js)
	if js != nil {
		bindConvergeOwner(t, b, js)
	}
	return b
}

// convergeOwner is the real configuration owner with every Shrink REQUEST recorded on the way in.
// It decides nothing: Apply/Freeze/Stop and the Shrink itself are the coordinator's.
type convergeOwner struct {
	xfercontract.BucketPort
	mu      sync.Mutex
	shrinks []xfercontract.ConfigPatch
}

func (o *convergeOwner) Shrink(p xfercontract.ConfigPatch, d controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	o.mu.Lock()
	o.shrinks = append(o.shrinks, p)
	o.mu.Unlock()
	return o.BucketPort.(xferbucket.Shrinker).Shrink(p, d)
}

// convergeLedger is a ready, complete continuation ledger with no rows: the owner's census then
// reduces to the legacy tracker these tests drive, which is exactly the responsibility they model.
type convergeLedger struct{}

func (convergeLedger) Scan(controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	return xfercontract.LedgerCensus{Ready: true, Complete: true}, nil
}
func (convergeLedger) Update(xfercontract.Key, uint64, xfercontract.LedgerMutation, controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	return xfercontract.LedgerRow{}, errors.New("convergeLedger: no continuation rows in this fixture")
}
func (convergeLedger) Retire(xfercontract.Key, string, xfercontract.RetirementProof, controlbudget.Deadline) error {
	return errors.New("convergeLedger: no continuation rows in this fixture")
}

// bindConvergeOwner installs xferbucket.NewWithCensus with this broker's own census
// (brokerBucketCensus), in the shape the broker transfer factory installs it.
func bindConvergeOwner(t *testing.T, b *Broker, js jetstream.JetStream) {
	t.Helper()
	owner, err := newProvisionOwner(b, js)
	if err != nil {
		t.Fatal(err)
	}
	b.transferRuntime.Store(&BrokerTransfers{Buckets: &convergeOwner{BucketPort: owner}, Ledger: convergeLedger{}})
}

// convergeShrinks returns every reservation shrink the broker asked the owner for.
func convergeShrinks(t *testing.T, b *Broker) []xfercontract.ConfigPatch {
	t.Helper()
	graph := b.transferRuntime.Load()
	if graph == nil {
		t.Fatal("no configuration owner is bound")
	}
	owner, ok := graph.Buckets.(*convergeOwner)
	if !ok {
		t.Fatal("the bound configuration owner does not record shrink requests")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return slices.Clone(owner.shrinks)
}

// convergeEnsure runs the provisioning path under test. A double asked for something it does not
// model panics through its nil embedded interface, and a product nil dereference panics the same
// way; either becomes THIS test's failure instead of a panic that aborts every later test in the
// package (which is how an earlier cut of this fixture hid every test after it).
func convergeEnsure(t *testing.T, b *Broker, maxBytes int64) (bucket string, payloadMax int64, err error) {
	t.Helper()
	defer failConvergePanic(t)
	return ensureXferBucketSizedWithLimit(context.Background(), b, "lab", 1, maxBytes)
}

// convergeProvision is convergeEnsure for the full prepare-admission path (provisionXferBucket).
func convergeProvision(t *testing.T, b *Broker, size int64) (bucket string, tooLarge *xferTooLarge, perr *xferProvisionErr) {
	t.Helper()
	defer failConvergePanic(t)
	return b.provisionXferBucket(context.Background(), "lab", size)
}

// failConvergePanic is deferred directly by the two helpers above, so recover() sees their panic.
func failConvergePanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Helper()
		t.Fatalf("the provisioning path panicked (%v): an unmodelled call on a JetStream double, or a "+
			"nil dereference in the code under test", r)
	}
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
	b := convergeBroker(t, js)

	bucket, _, err := convergeEnsure(t, b, target)
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
	shrink := requireOneShrink(t, b, js, "an unsatisfiable reservation blocks every stream create on the "+
		"broker until it is reduced")
	if got := shrink.after.MaxBytes; got != target {
		t.Errorf("shrank to %d, want the computed target %d", got, target)
	}
}

// requireOneShrink: the broker asked the owner for exactly one shrink, and exactly one write changed an
// operator-owned field — a full-config MaxBytes reduction to the requested size that left every other
// field (including the recovery window) as it was.
func requireOneShrink(t *testing.T, b *Broker, js *convergeJS, why string) convergeWrite {
	t.Helper()
	requests := convergeShrinks(t, b)
	writes := js.operatorWrites()
	if len(requests) != 1 || len(writes) != 1 || len(js.reservationShrinks()) != 1 {
		t.Fatalf("expected exactly one shrink, got %d request(s) and %d operator-owned write(s) %+v — %s",
			len(requests), len(writes), writes, why)
	}
	w := writes[0]
	unchanged := w.after
	unchanged.MaxBytes = w.before.MaxBytes
	if w.after.MaxBytes != requests[0].MaxBytes || !reflect.DeepEqual(unchanged, w.before) {
		t.Fatalf("the shrink write is not a pure MaxBytes reduction to the requested %d:\nbefore=%+v\nafter =%+v",
			requests[0].MaxBytes, w.before, w.after)
	}
	if js.sdkUpdates != 0 {
		t.Fatalf("%d UpdateObjectStore projection(s) bypassed the configuration owner", js.sdkUpdates)
	}
	return w
}

// assertNoOperatorChange: the broker asked the owner for no shrink, and no write changed anything but
// the owner's raised recovery window. This is what "not updated"/"not resized" means since S7.
func assertNoOperatorChange(t *testing.T, b *Broker, js *convergeJS, why string) {
	t.Helper()
	if requests := convergeShrinks(t, b); len(requests) != 0 {
		t.Fatalf("the broker asked the configuration owner to shrink (%+v) — %s", requests, why)
	}
	if writes := js.operatorWrites(); len(writes) != 0 {
		t.Fatalf("%d write(s) changed operator-owned config (%+v) — %s", len(writes), writes, why)
	}
	if js.sdkUpdates != 0 {
		t.Fatalf("%d UpdateObjectStore projection(s) bypassed the configuration owner", js.sdkUpdates)
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
	b := convergeBroker(t, js)

	if _, _, err := convergeEnsure(t, b, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "tether resized a bucket the store can carry. Stream limits are "+
		"operator-owned; the trigger is 'the store cannot honour this', not 'tether would have "+
		"picked a different number'")
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
	b := convergeBroker(t, js)

	if _, _, err := convergeEnsure(t, b, target); err != nil {
		t.Fatal(err)
	}
	assertNoOperatorChange(t, b, js, "shrank a currently satisfiable bucket because a fictitious second target "+
		"would not fit; ReservedStore already includes this bucket and resolve-before-create issues no create")
}

// TestOversizedBucketInUseIsNotShrunkBelowItsBytes: shrinking under the stored bytes is rejected by
// UpdateObjectStore and is how replication_degraded gets latched. Staying oversized is the safe
// outcome, and it must be logged rather than silently skipped.
//
// The store must be OVER-committed for the stored-bytes belt to be the guard that decides: with the
// 10.33 GiB limit this test used to carry, 10 GiB reserved is satisfiable, the repair returns before
// it ever reaches the belt (external review F4), and deleting the belt left this test green.
// origin: S7 lead integration (converge fixture rebinding; mutation found the belt unreached)
func TestOversizedBucketInUseIsNotShrunkBelowItsBytes(t *testing.T) {
	target := int64(2)*gib + 64*1024*1024
	js := &convergeJS{
		maxStore: uint64(9 * gib),                  // over-committed => the trigger says repair
		reserved: uint64(10 * gib),                 // events + history + the 8 GiB bucket
		existing: streamInfo(8*gib, uint64(3*gib)), // 3 GiB already stored — target is BELOW that
	}
	b := convergeBroker(t, js)

	if _, _, err := convergeEnsure(t, b, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "shrank a bucket below its stored bytes — the update is rejected, "+
		"and the rejection latches replication_degraded")
}

// TestMissingBucketStillTakesTheCreatePath: resolve-before-create must not disable creation.
//
// Since S7 the owner follows every CREATE with INFO -> UPDATE -> INFO to seal the recovery window
// the SDK's object-store config cannot carry (plan §2.1 / T05); that UPDATE is the one write a fresh
// bucket may see, and it must change nothing else.
func TestMissingBucketStillTakesTheCreatePath(t *testing.T) {
	js := &convergeJS{maxStore: uint64(500 * gib), existing: nil}
	b := convergeBroker(t, js)

	if _, _, err := convergeEnsure(t, b, gib); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	if js.creates != 1 {
		t.Fatalf("CreateObjectStore called %d time(s) for a bucket that does not exist, want 1", js.creates)
	}
	assertNoOperatorChange(t, b, js, "a fresh bucket must not be updated beyond its recovery window")
	if len(js.writes) != 1 || js.existing.Config.MaxBytes != gib {
		t.Fatalf("a fresh bucket must keep its created reservation %d and see only the recovery-window "+
			"seal: writes=%+v final=%+v", gib, js.writes, js.existing.Config)
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
		b := convergeBroker(t, tc.js)
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
	b := convergeBroker(t, js)

	_, _, err := convergeEnsure(t, b, 2*gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	js.requireCreateRefused(t)
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
	b := convergeBroker(t, js)

	_, _, err := convergeEnsure(t, b, gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	js.requireCreateRefused(t)
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
	creates                  int
}

// requireCreateRefused proves the refusal under test is the JetStream verdict on a CREATE that the
// configuration owner actually sent, not a refusal from somewhere before it (an owner that is not
// installed fails with no JetStream call at all, and its text carries no store accounting either).
func (r *refusingJS) requireCreateRefused(t *testing.T) {
	t.Helper()
	if r.creates == 0 {
		t.Fatal("the configuration owner never sent the CREATE; the refusal did not come from JetStream")
	}
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
	r.creates++
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
	b := convergeBroker(t, js)
	// Production takes the statfs branch (the account limit is -1), so pin it to a KNOWN store size
	// instead of whatever this host's filesystem reports — otherwise the verdict depends on the CI
	// machine's free space rather than on the code.
	b.cfg.StoreDir = t.TempDir()
	restore := pinStoreSize(t, 16*gib, 4*gib) // total 16, used 4 => ceiling 0.75*12 = 9 GiB < 10 reserved
	defer restore()

	if _, _, err := convergeEnsure(t, b, target); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "statfs estimate authorized an operator-owned shrink with an unlimited account")
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
	b := convergeBroker(t, js)
	b.cfg.StoreDir = t.TempDir()
	restore := pinStoreSize(t, 16*gib, 4*gib)
	defer restore()

	_, _, err := convergeEnsure(t, b, 2*gib)
	if err == nil {
		t.Fatal("expected the create to fail")
	}
	js.requireCreateRefused(t)
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
	b := convergeBroker(t, js)

	if _, _, err := convergeEnsure(t, b, 2*gib+64*1024*1024); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "shrank an operator-owned limit on a store with 6 GiB free. ReservedStore "+
		"already contains this bucket, so adding it again asks whether a SECOND copy would fit — "+
		"a question nobody needs answered")
}

// TestConvergencePreservesTheReplicaFactor: replication is raise-only and cluster-owned. A capacity
// repair that writes the local target replica count silently DOWNGRADES a 3-replica bucket to 1.
//
// Mutation that must redden this: put `targetReplicas` back in the shrink's ObjectStoreConfig.
//
// Since S7 the owner is raise-only on replicas too, so the stream would keep 3 even if the broker
// asked for 1. The broker's REQUEST is therefore asserted as well as the written config: a repair
// that asks the owner to downgrade redundancy is the defect, whoever happens to refuse it.
func TestConvergencePreservesTheReplicaFactor(t *testing.T) {
	js := &replica3JS{convergeJS{
		maxStore: uint64(9 * gib),
		reserved: uint64(10 * gib), // over-committed => convergence fires
		existing: &jetstream.StreamInfo{
			Config: jetstream.StreamConfig{Name: convergeStream, MaxBytes: 8 * gib, Replicas: 3, Discard: jetstream.DiscardNew},
			State:  jetstream.StreamState{Bytes: 0},
		},
	}}
	b := convergeBroker(t, js)

	// Target replicas 1 — LOWER than the stream's 3, which is the trap.
	if _, _, err := convergeEnsure(t, b, 2*gib+64*1024*1024); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	shrink := requireOneShrink(t, b, &js.convergeJS, "an over-committed store must still be repaired")
	if got := convergeShrinks(t, b)[0].Replicas; got != 3 {
		t.Fatalf("shrink asked for Replicas=%d, want the stream's existing 3. Losing redundancy as a "+
			"side effect of a capacity repair is not something this path is entitled to do", got)
	}
	if got := shrink.after.Replicas; got != 3 {
		t.Fatalf("shrink wrote Replicas=%d, want the stream's existing 3. Losing redundancy as a "+
			"side effect of a capacity repair is not something this path is entitled to do", got)
	}
}

// replica3JS reports an existing bucket at 3 replicas, so raiseXferReplicas no-ops (raise-only) and
// the shrink is the only writer. (On the owner path the replica raise is the owner's own raise-only
// field, so the stream config at 3 replicas is what keeps it a no-op.)
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
// (The owner's own census refuses the same shrink; the broker's request is what this pins.)
func TestConvergenceDefersWhileATransferIsInFlight(t *testing.T) {
	js := &convergeJS{
		maxStore: uint64(3 * gib),
		reserved: uint64(4 * gib),                            // over-committed => the trigger says repair
		existing: streamInfo(3*gib/2, uint64(600*1024*1024)), // 1.5 GiB cap, 600 MiB streamed so far
	}
	// An admitted stream must preserve acknowledged chunks before this test
	// can exercise the independent live-transfer shrink barrier.
	// origin: hot-update S7 plan §2.2 / FIX-JSN1 / INTEG-G fixture integration
	js.existing.Config.Discard = jetstream.DiscardNew
	b := convergeBroker(t, js)
	// An admitted, in-flight transfer owning this bucket.
	putLegacyTransfer(b.transfers, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t1", SID: "lab", Bucket: proto.XferBucketName("lab"), Tier: "b"}))

	if _, _, err := convergeEnsure(t, b, 805306368); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "shrank while a transfer was in flight — the remaining bytes of an "+
		"already admitted object would hit DiscardNew")
}

// TestConvergenceIsInertWithoutATracker: an unreadable input must mean "leave it alone", never
// "assume idle". It also must not panic — this runs on the push path.
func TestConvergenceIsInertWithoutATracker(t *testing.T) {
	js := &convergeJS{maxStore: uint64(9 * gib), reserved: uint64(10 * gib), existing: streamInfo(8*gib, 0)}
	b := &Broker{} // no tracker, no logger
	b.cfg.Logger = silentLogger()
	setBrokerJS(b, js)
	bindConvergeOwner(t, b, js)

	if _, _, err := convergeEnsure(t, b, 2*gib); err != nil {
		t.Fatalf("ensure returned %v", err)
	}
	assertNoOperatorChange(t, b, js, "shrank without being able to check for in-flight transfers")
}

// origin: smalldisk external review F1
// A capacity repair owns exactly MaxBytes. Rebuilding ObjectStoreConfig from
// zero values silently erases every other operator-owned setting when the
// shrink succeeds, even though the surrounding code promises those settings
// survive reconciliation.
//
// The operator TTL here is 48h, not the 37 minutes this test first used: since S7 LQ3 a bucket
// whose MaxAge is shorter than the transfer recovery horizon (35m08s + 2m) is refused with
// transfer_storage_horizon before any write, so a 37-minute TTL no longer reaches the repair at all.
// The property under test — every operator field survives the shrink — is unchanged.
func TestConvergencePreservesOperatorOwnedObjectStoreConfig(t *testing.T) {
	placement := &jetstream.Placement{Cluster: "edge", Tags: []string{"ssd", "rack-a"}}
	metadata := map[string]string{"owner": "ops", "purpose": "transfer"}
	js := &convergeJS{
		maxStore: uint64(9 * gib),
		reserved: uint64(10 * gib),
		existing: &jetstream.StreamInfo{
			Config: jetstream.StreamConfig{
				Name:        convergeStream,
				MaxBytes:    8 * gib,
				Replicas:    1,
				Storage:     jetstream.FileStorage,
				Discard:     jetstream.DiscardNew,
				Description: "operator description",
				MaxAge:      48 * time.Hour,
				Placement:   placement,
				Compression: jetstream.S2Compression,
				Metadata:    metadata,
			},
		},
	}
	b := convergeBroker(t, js)
	target := 2*gib + 64*1024*1024

	if _, _, err := convergeEnsure(t, b, target); err != nil {
		t.Fatal(err)
	}
	// requireOneShrink already proves the write changed MaxBytes and nothing else; the named fields
	// are kept explicit so a failure says which operator setting a repair erased.
	got := requireOneShrink(t, b, js, "an over-committed store must still be repaired").after
	if got.Description != "operator description" || got.MaxAge != 48*time.Hour ||
		got.Storage != jetstream.FileStorage || got.Compression != jetstream.S2Compression ||
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
	b := convergeBroker(t, js)
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
	b := convergeBroker(t, js)
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
	b := convergeBroker(t, js)
	b.runCtx = context.Background()

	bucket, tooLarge, perr := convergeProvision(t, b, requestSize)
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
	b := convergeBroker(t, js)
	b.runCtx = context.Background()

	_, tooLarge, perr := convergeProvision(t, b, requestSize)
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
	tr := xfertrack.NewLegacyTracker()
	bucket := proto.XferBucketName("lab")

	if code := putLegacyTransfer(tr, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t1", SID: "lab", Bucket: bucket, Tier: "b"})); code != "" {
		t.Fatalf("first tier-B transfer rejected: %s", code)
	}
	if code := putLegacyTransfer(tr, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t2", SID: "lab", Bucket: bucket, Tier: "b"})); code == "" {
		t.Fatal("a SECOND tier-B transfer was admitted for the same bucket. Both would pass admission " +
			"and then race into DiscardNew — the mid-Put failure this increment exists to prevent")
	}

	// Tier A carries no bucket and must stay concurrent: it never touches the object store.
	if code := putLegacyTransfer(tr, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t3", SID: "lab", Tier: "a"})); code != "" {
		t.Fatalf("tier-A transfer rejected (%s); it uses no bucket and must not be serialised", code)
	}
	// A DIFFERENT session has its own bucket and must stay concurrent too — otherwise this guard
	// would serialise the whole broker rather than one bucket.
	if code := putLegacyTransfer(tr, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t4", SID: "ops", Bucket: proto.XferBucketName("ops"), Tier: "b"})); code != "" {
		t.Fatalf("a different session's tier-B was rejected (%s); the constraint is per BUCKET", code)
	}
	// And the slot frees when the first one finishes.
	tr.Remove(tr.Get("t1"))
	if code := putLegacyTransfer(tr, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "t5", SID: "lab", Bucket: bucket, Tier: "b"})); code != "" {
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
