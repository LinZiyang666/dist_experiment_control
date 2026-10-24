package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
)

// TestPullBucketPayloadCeilingHonorsBrokerCapacity pins the additive broker→agent field used when
// only the pull sender can stat the file. A missing field preserves N-1 behavior; a present zero is
// intentionally distinct and closes tier B when existing objects leave no usable bucket space.
func TestPullBucketPayloadCeilingHonorsBrokerCapacity(t *testing.T) {
	zero := int64(0)
	small := int64(512 * 1024 * 1024)
	huge := agentTransferMaxBytes + 1
	negative := int64(-1)
	for _, tc := range []struct {
		name string
		in   *int64
		want int64
	}{
		{"old broker omits field", nil, agentTransferMaxBytes},
		{"small disk", &small, small},
		{"no capacity", &zero, 0},
		{"invalid negative is closed", &negative, 0},
		{"broker cannot widen global cap", &huge, agentTransferMaxBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S7 plan §11.4 / P31; assert the real leaf
			// and the retained Agent adapter independently against the same oracle.
			if got := filetransfer.PullBucketPayloadCeiling(tc.in); got != tc.want {
				t.Fatalf("leaf got %d want %d", got, tc.want)
			}
			if got := pullBucketPayloadCeiling(tc.in); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

// validateFn is the shared shape of ValidateForRead / ValidateForWrite,
// used by the table tests that exercise both directions.
type validateFn func(string, []string, transferMode) (*ValidatedPath, error)

// origin: hot-update S7 plan §11.4 / P31; retained Agent entry points
// and direct path leaves must enforce identical all-dropped-root policy.
func bothValidators() []validateFn {
	return []validateFn{
		ValidateForRead, ValidateForWrite,
		func(path string, roots []string, mode transferMode) (*ValidatedPath, error) {
			return transferpath.ValidateForRead(path, roots, int(mode))
		},
		func(path string, roots []string, mode transferMode) (*ValidatedPath, error) {
			return transferpath.ValidateForWrite(path, roots, int(mode))
		},
	}
}

// resolveTransferMode is the load-bearing security invariant: mode is
// decided from RAW config presence, NEVER from len(canonAllowRoots), so a
// narrow config whose roots all drop during canonicalization stays
// modeNarrow (reject-all) and never collapses to whole-FS open.
func TestResolveTransferMode(t *testing.T) {
	tests := []struct {
		name            string
		rootsConfigured bool
		rawRoots        []string
		want            transferMode
	}{
		{"absent key (fresh install)", false, nil, modeOpen},
		{"absent key, empty slice (not configured)", false, []string{}, modeOpen},
		{"explicit empty list", true, []string{}, modeDisabled},
		{"non-empty, configured", true, []string{"/x"}, modeNarrow},
		{"non-empty, unflagged still narrow", false, []string{"/x"}, modeNarrow},
		// The fail-closed guarantee: a narrow config is decided on the RAW
		// list, so even roots that will all drop in CanonAllowRoots resolve
		// to modeNarrow here — never modeOpen.
		{"non-empty but will-all-drop stays narrow", true, []string{"/no/such/dir"}, modeNarrow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S7 plan §11.4 / P31; raw policy resolution moved to the leaf.
			if got := filetransfer.ResolveTransferMode(tc.rootsConfigured, tc.rawRoots); got != int(tc.want) {
				t.Errorf("leaf ResolveTransferMode(%v, %v) = %d, want %d", tc.rootsConfigured, tc.rawRoots, got, tc.want)
			}
			if got := resolveTransferMode(tc.rootsConfigured, tc.rawRoots); got != tc.want {
				t.Errorf("resolveTransferMode(%v, %v) = %d, want %d", tc.rootsConfigured, tc.rawRoots, got, tc.want)
			}
		})
	}
}

// End-to-end fail-closed: a narrow config whose roots ALL drop during
// canonicalization must, when run through agent.New, still reject every path
// (modeNarrow + empty canon), never fall open. Ties resolveTransferMode →
// CanonAllowRoots → Validate in the single flow New actually wires (Round-1
// review: the two halves were pinned separately but never together).
func TestNew_AllDroppedNarrowFailsClosed(t *testing.T) {
	a, err := New(Config{
		NATSURL:         "nats://127.0.0.1:4222",
		SID:             "lab",
		NID:             "a100",
		RootsConfigured: true,
		AllowRoots:      []string{"/no/such/dir", "relative", ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.transferMode != modeNarrow {
		t.Fatalf("transferMode = %d, want modeNarrow (all-dropped narrow must not fall open)", a.transferMode)
	}
	if len(a.canonAllowRoots) != 0 {
		t.Fatalf("canonAllowRoots = %v, want empty (all dropped)", a.canonAllowRoots)
	}
	for _, fn := range bothValidators() {
		_, err := fn("/etc/hosts", a.canonAllowRoots, a.transferMode)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
			t.Errorf("got %v, want path_outside_roots (fail-closed)", err)
		}
	}
}

// Partial-drop narrow: a config with SOME valid + SOME dropping roots must
// keep the survivors working AND still reject paths outside them — neither
// fall open nor over-reject. This is the typo'd / transiently-unmounted-root
// footgun the RAW-vs-canon design exists to handle (plan §"Rejected 1a").
func TestNew_PartialDropNarrow(t *testing.T) {
	good := t.TempDir()
	a, err := New(Config{
		NATSURL:         "nats://127.0.0.1:4222",
		SID:             "lab",
		NID:             "a100",
		RootsConfigured: true,
		AllowRoots:      []string{good, "/no/such/dir", "relative"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.transferMode != modeNarrow {
		t.Fatalf("transferMode = %d, want modeNarrow", a.transferMode)
	}
	if len(a.canonAllowRoots) != 1 {
		t.Fatalf("canonAllowRoots = %v, want exactly the surviving good root", a.canonAllowRoots)
	}
	// Survivor root still works.
	src := filepath.Join(good, "f.bin")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if vp, err := ValidateForRead(src, a.canonAllowRoots, a.transferMode); err != nil {
		t.Errorf("survivor root path rejected: %v", err)
	} else if vp.AllowRoot == "" {
		t.Errorf("survivor AllowRoot empty, want the good root")
	}
	// A real path OUTSIDE the survivor is still rejected by containment.
	other := t.TempDir()
	otherFile := filepath.Join(other, "f.bin")
	if err := os.WriteFile(otherFile, []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = ValidateForRead(otherFile, a.canonAllowRoots, a.transferMode)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
		t.Errorf("path outside survivor: got %v, want path_outside_roots", err)
	}
}

func TestPushCommitCacheExpiresAndStaysBounded(t *testing.T) {
	now := time.Now()
	// Expiry is PER ENTRY, keyed on that entry's own size, so a tiny transfer's prep is short-lived.
	// origin: hot-update S7 plan §11.4 / P31; the real leaf owns cache admission.
	var mu sync.Mutex
	cache := map[string]filetransfer.PushCommitEntry{
		"stale": {Added: now.Add(-filetransfer.PushCommitEntryTTL(0) - time.Second)},
	}
	if !filetransfer.RememberPrepared(&mu, &cache, "fresh", filetransfer.PushCommitEntry{Added: now}, now, filetransfer.PushCommitEntryExpired) {
		t.Fatal("remember refused with a nearly-empty cache")
	}
	if _, ok := cache["stale"]; ok {
		t.Fatal("stale push commit entry was not purged")
	}
	if _, ok := cache["fresh"]; !ok {
		t.Fatal("fresh push commit entry missing")
	}
}

// TestPushCommitCacheNeverEvictsALiveLargeTransfer pins the batch-C behaviour change, and it is the
// test that has to be BEHAVIOURAL rather than a constant inequality.
//
// The pre-batch-C cache had TWO eviction paths: the TTL sweep, and "at capacity, delete the entry
// with the earliest `added`". Making a large transfer's prep entry live longer — which the size-
// derived budget REQUIRES, since the ctl now takes ~17 minutes to upload 2 GiB — makes that second
// path fire on precisely the entry being protected: the oldest live entry is by construction the
// longest-running, largest transfer. A test asserting only `TTL > uploadBudget` would stay green
// while the capacity path silently killed the transfer.
//
// Mutations: (a) restore evict-oldest instead of refusing; (b) make pushCommitEntryTTL a single
// global constant again. Either reddens.
func TestPushCommitCacheNeverEvictsALiveLargeTransfer(t *testing.T) {
	now := time.Now()
	// origin: hot-update S7 plan §11.4 / P31; no private Agent cache implementation.
	var mu sync.Mutex
	cache := map[string]filetransfer.PushCommitEntry{}

	// The entry we must not lose: a maximum-size transfer whose upload has been running for a quarter
	// of an hour. Under the old flat 6-minute TTL it would already be gone.
	big := filetransfer.PushCommitEntry{Size: proto.XferMaxBytes, Added: now.Add(-15 * time.Minute)}
	cache["big"] = big
	if filetransfer.PushCommitEntryExpired(big, now) {
		t.Fatalf("a %d-byte transfer's prep expired after 15m; its TTL is %s and the ctl needs %s just "+
			"to upload it", proto.XferMaxBytes, filetransfer.PushCommitEntryTTL(big.Size), proto.XferLegBudget(big.Size))
	}

	// Now flood the cache to capacity with small, still-live entries.
	for i := 0; len(cache) < pushCommitCacheMaxEntries; i++ {
		cache[fmt.Sprintf("id-%04d", i)] = filetransfer.PushCommitEntry{Added: now}
	}
	if filetransfer.RememberPrepared(&mu, &cache, "newest", filetransfer.PushCommitEntry{Added: now}, now, filetransfer.PushCommitEntryExpired) {
		t.Fatal("remember accepted an entry while the cache was full of LIVE entries — the only way to " +
			"make room is to evict one, and the victim would be the oldest, i.e. the biggest transfer " +
			"in flight")
	}
	if _, ok := cache["big"]; !ok {
		t.Fatal("the long-running large transfer's prep entry was evicted to make room for a new one")
	}
	if _, ok := cache["newest"]; ok {
		t.Fatal("the refused entry was stored anyway")
	}

	// Once the small entries age out, the cache accepts again — the refusal is retriable, not a wedge.
	for id, e := range cache {
		if id == "big" {
			continue
		}
		e.Added = now.Add(-filetransfer.PushCommitEntryTTL(0) - time.Second)
		cache[id] = e
	}
	if !filetransfer.RememberPrepared(&mu, &cache, "newest", filetransfer.PushCommitEntry{Added: now}, now, filetransfer.PushCommitEntryExpired) {
		t.Fatal("the cache never recovers: expired entries were not swept on the next insert")
	}
	if _, ok := cache["big"]; !ok {
		t.Fatal("the sweep took the live large entry with it")
	}
}

// TestPushCommitTTLCoversTheUploadItWaitsOn: the prep entry has to outlive the ctl's upload of that
// exact file, or push-commit arrives to a cache miss and the agent answers transfer_unknown.
//
// Mutation: drop pushCommitCacheSlack, or key the TTL on something other than size — reddens.
func TestPushCommitTTLCoversTheUploadItWaitsOn(t *testing.T) {
	for _, size := range []int64{0, 1, 8 * 1024 * 1024, 512 * 1024 * 1024, proto.XferMaxBytes} {
		ttl, upload := pushCommitEntryTTL(size), proto.XferLegBudget(size)
		// origin: hot-update S5 plan §10 / AGENT-SLIM-S5; preserve the documented commit margin.
		if want := proto.XferBudget("b", size, 1) + time.Minute; ttl != want {
			t.Errorf("size=%d: prep TTL %s, want upload budget plus one minute (%s)", size, ttl, want)
		}
		if ttl <= upload {
			t.Errorf("size=%d: prep TTL %s does not exceed the ctl's own upload budget %s — the entry "+
				"can expire before push-commit is even sent", size, ttl, upload)
		}
	}
}
