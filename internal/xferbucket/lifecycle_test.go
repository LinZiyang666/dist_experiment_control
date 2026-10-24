package xferbucket

// Embedded NATS provides real file-store configuration effects and teardown.
// Reply barriers exercise join, deferred cuts and unresolved response retention.
// Worker/courier, same-PID exec and cross-broker forwarding are other owners' exits.

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §11.3 / P04 / T05
func TestTransferLifecycleNoLeaks(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer bucket coordinator", goroutines)
	testharness.AssertNoFDLeak(t, "transfer bucket coordinator", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	t.Run("configuration_lifetime", func(t *testing.T) {
		ctx, js := bucketFixture(t, false, 0)
		owner := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-success"}, bucketDeadline(t, time.Second))
		if err != nil || seal.Duplicates < objectRecoveryHorizon {
			t.Fatalf("successful lifetime lacks confirmed config: seal=%+v err=%v", seal, err)
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatalf("second stop failed: %v", err)
		}
		t.Run("cut_timeout_preserves_owner_then_joins", func(t *testing.T) {
			bucket := "xfer-deferred"
			operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			// Cleanup releases the reply before the containing server is stopped,
			// including a failed test path; every spawned goroutine has a join.
			var released atomic.Bool
			unblock := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			wrapped := &interceptedJS{JetStream: js}
			wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
				stream, err := js.UpdateStream(callCtx, cfg)
				if err != nil {
					return nil, err
				}
				if cfg.Name != "OBJ_"+bucket {
					return stream, nil
				}
				close(entered)
				select {
				case <-release:
					return stream, nil
				case <-callCtx.Done():
					return nil, callCtx.Err()
				}
			}
			owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
			result := make(chan error, 1)
			deadline := bucketDeadline(t, 3*time.Second)
			go func() {
				_, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket}, deadline)
				result <- err
			}()
			joined := false
			defer func() {
				unblock()
				if !joined {
					<-result
				}
			}()
			select {
			case <-entered:
			case err := <-result:
				joined = true
				t.Fatalf("UPDATE did not reach reply barrier: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			state, err := owner.Freeze(bucketDeadline(t, 20*time.Millisecond))
			if !errors.Is(err, context.DeadlineExceeded) || state.Admission.Closed {
				t.Fatalf("cut claimed joined requests at blocked reply: state=%+v err=%v", state, err)
			}
			// Deferred Freeze restored admission: a different bucket remains
			// usable while this original operation still owns its reply.
			seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-after-defer"}, bucketDeadline(t, time.Second))
			if err != nil || seal.Proof.Generation != 1 {
				t.Fatalf("deferred cut lost original owner: seal=%+v err=%v", seal, err)
			}
			unblock()
			if err := <-result; err != nil {
				joined = true
				t.Fatal(err)
			}
			joined = true
			state, err = owner.Freeze(bucketDeadline(t, time.Second))
			if err != nil || !state.Admission.Closed || state.Admission.Inflight != 0 || len(state.Proofs) != 2 {
				t.Fatalf("joined current state lacks both bucket owners: state=%+v err=%v", state, err)
			}
			current := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 2, &state)
			if _, err := current.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			js.Conn().Close()
			if err := current.Stop(bucketDeadline(t, time.Second)); err != nil {
				t.Fatalf("stop required data transport after requests joined: %v", err)
			}
		})
	})
}

// origin: hot-update S7 plan §2.1 / T05 / old-owner sent-unknown handoff
func TestTransferBucketConfigUnresolvedWriteBlocksHandoff(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	bucket := "xfer-unknown"
	expected := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
	wrapped := &interceptedJS{JetStream: js}
	wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
		if _, err := js.UpdateStream(callCtx, cfg); err != nil {
			return nil, err
		}
		// The effect is real; only its response is withheld until timeout.
		<-callCtx.Done()
		return nil, callCtx.Err()
	}
	owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
	seal, err := owner.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, 100*time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeResultUnknown) || seal != (xfercontract.BucketSeal{}) {
		t.Fatalf("lost response yielded a first-byte seal: seal=%+v err=%v", seal, err)
	}
	expected.Duplicates = objectRecoveryHorizon
	before, _ := configDigest(expected)
	after, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config)
	if before != after {
		t.Fatal("lost-response fixture did not perform the real UPDATE")
	}
	lookups := wrapped.lookups.Load()
	seal, err = owner.Apply(xfercontract.ConfigPatch{Bucket: bucket, Duplicates: objectRecoveryHorizon + time.Minute}, bucketDeadline(t, time.Second))
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeResultUnknown) || seal != (xfercontract.BucketSeal{}) || wrapped.updates.Load() != 1 || wrapped.lookups.Load() != lookups {
		t.Fatalf("equal INFO/retry bypassed unresolved UPDATE: seal=%+v err=%v updates=%d", seal, err, wrapped.updates.Load())
	}
	state, err := owner.Freeze(bucketDeadline(t, time.Second))
	if err == nil || len(state.Attempts) != 1 || !state.Attempts[0].SentUnknown || state.Admission.Closed || state.Proofs[0].RequestsJoined {
		t.Fatalf("unresolved intent/proof was lost: state=%+v err=%v", state, err)
	}
	if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
		t.Fatal("new leader acquired authority without previous retirement")
	}
	// Both witnesses are necessary; a closed gate alone cannot join an old
	// request, even when a caller incorrectly marks the proof as joined.
	state.Admission.Closed = true
	state.Proofs[0].GateClosed, state.Proofs[0].RequestsJoined = true, true
	if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
		t.Fatal("sent-unknown intent was erased by forged joined flags")
	}
}

func requireResultUnknown(t *testing.T, seal xfercontract.BucketSeal, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeResultUnknown) || seal != (xfercontract.BucketSeal{}) {
		t.Fatalf("unknown configuration request was not reported as unknown: seal=%+v err=%v", seal, err)
	}
}

// withheldReply blocks until the caller's own deadline, as a leaderless meta does: the request may or
// may not have taken effect, and no response ever says which.
func withheldReply(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// The same writer generation retrying the same ensure after a CREATE timeout is the G67 retry. Its
// definite answer plus the full readback resolves the unknown CREATE, whether the lost request had
// created the stream or never reached the server; only then may a successor take over.
// origin: hot-update S7 plan §2.1 / T05 / lead decision on G67 replay
func TestTransferBucketConfigSameIntentRetryResolvesUnknownCreate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		applied bool
		creates int32
	}{
		{name: "reply_lost_after_create", applied: true, creates: 1},
		{name: "request_dropped_before_create", applied: false, creates: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, js := bucketFixture(t, false, 0)
			var calls atomic.Int32
			wrapped := &interceptedJS{JetStream: js}
			wrapped.create = func(callCtx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
				if calls.Add(1) > 1 {
					return js.CreateObjectStore(callCtx, cfg)
				}
				if tc.applied {
					if _, err := js.CreateObjectStore(callCtx, cfg); err != nil {
						return nil, err
					}
				}
				return nil, withheldReply(callCtx)
			}
			owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
			patch := xfercontract.ConfigPatch{Bucket: "xfer-retry-create", MaxBytes: 256 << 20}
			seal, err := owner.Apply(patch, bucketDeadline(t, 200*time.Millisecond))
			requireResultUnknown(t, seal, err)
			seal, err = owner.Apply(patch, bucketDeadline(t, 5*time.Second))
			if err != nil {
				t.Fatalf("same-generation retry of the same ensure was refused: %v", err)
			}
			if wrapped.creates.Load() != tc.creates || seal.MaxBytes != 256<<20 || seal.Duplicates < objectRecoveryHorizon || seal.Proof.Generation != 1 {
				t.Fatalf("retry did not converge on one confirmed bucket: creates=%d seal=%+v", wrapped.creates.Load(), seal)
			}
			state, err := owner.Freeze(bucketDeadline(t, time.Second))
			if err != nil || len(state.Attempts) != 1 || state.Attempts[0].SentUnknown || state.Attempts[0].Seal == nil {
				t.Fatalf("resolved request still blocks handoff: state=%+v err=%v", state, err)
			}
			successor := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 2, &state)
			if _, err := successor.Apply(patch, bucketDeadline(t, time.Second)); err != nil {
				t.Fatalf("successor could not use the resolved bucket: %v", err)
			}
		})
	}
}

// While a request is unknown, a different desired config (including a shrink of the same patch) and a
// different writer generation stay refused, without any storage IO. The same intent is what resolves it.
// origin: hot-update S7 plan §2.1 / T05 / lead decision on G67 replay
func TestTransferBucketConfigUnknownRequestRefusesOtherIntentAndOwner(t *testing.T) {
	_, js := bucketFixture(t, false, 0)
	var dropped atomic.Bool
	dropped.Store(true)
	wrapped := &interceptedJS{JetStream: js}
	wrapped.create = func(callCtx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
		if dropped.Load() {
			return nil, withheldReply(callCtx)
		}
		return js.CreateObjectStore(callCtx, cfg)
	}
	owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
	bucket := "xfer-unknown-intent"
	patch := xfercontract.ConfigPatch{Bucket: bucket, MaxBytes: 256 << 20}
	seal, err := owner.Apply(patch, bucketDeadline(t, 200*time.Millisecond))
	requireResultUnknown(t, seal, err)
	dropped.Store(false)
	lookups, creates := wrapped.lookups.Load(), wrapped.creates.Load()
	for name, other := range map[string]xfercontract.ConfigPatch{
		"replicas":    {Bucket: bucket, MaxBytes: 256 << 20, Replicas: 3},
		"reservation": {Bucket: bucket, MaxBytes: 512 << 20},
		"duplicates":  {Bucket: bucket, MaxBytes: 256 << 20, Duplicates: objectRecoveryHorizon + time.Minute},
	} {
		seal, err := owner.Apply(other, bucketDeadline(t, time.Second))
		if err == nil || !strings.Contains(err.Error(), xfercontract.CodeResultUnknown) || seal != (xfercontract.BucketSeal{}) {
			t.Fatalf("%s: a different desired config was admitted while a request is unknown: seal=%+v err=%v", name, seal, err)
		}
	}
	seal, err = owner.(Shrinker).Shrink(patch, bucketDeadline(t, time.Second))
	requireResultUnknown(t, seal, err)
	if wrapped.lookups.Load() != lookups || wrapped.creates.Load() != creates {
		t.Fatalf("refused intents performed storage IO: lookups %d->%d creates %d->%d", lookups, wrapped.lookups.Load(), creates, wrapped.creates.Load())
	}
	state, err := owner.Freeze(bucketDeadline(t, time.Second))
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeResultUnknown) || len(state.Attempts) != 1 || !state.Attempts[0].SentUnknown {
		t.Fatalf("unknown request did not block retirement: state=%+v err=%v", state, err)
	}
	if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
		t.Fatal("a successor generation was authorized while the request is unknown")
	}
	if _, err := owner.Apply(patch, bucketDeadline(t, 5*time.Second)); err != nil {
		t.Fatalf("same intent could not resolve the unknown request: %v", err)
	}
	state, err = owner.Freeze(bucketDeadline(t, time.Second))
	if err != nil || state.Attempts[0].SentUnknown {
		t.Fatalf("resolved request still blocks retirement: state=%+v err=%v", state, err)
	}
	if _, err := NewAuthority(bucketOwner(2), &state); err != nil {
		t.Fatalf("successor refused after resolution: %v", err)
	}
}

// An UPDATE whose reply was lost may still land, so a retry may only converge on the identical full
// config. A changed snapshot (operator edit in between) keeps the request unknown, whether converging
// would need a different UPDATE or none at all; it never clobbers the newer config.
// origin: hot-update S7 plan §2.1 / T05 / lead decision on G67 replay
func TestTransferBucketConfigUnknownUpdateRetriesOnlyIdenticalConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		applied  bool
		operator func(*jetstream.StreamConfig)
		matches  bool
		updates  int32
	}{
		{name: "reply_lost_after_update", applied: true, matches: true, updates: 1},
		{name: "request_dropped_resent_identically", matches: true, updates: 2},
		{name: "operator_edit_needs_another_update", operator: func(cfg *jetstream.StreamConfig) {
			cfg.Description = "operator edit while unknown"
		}, updates: 1},
		{name: "operator_edit_needs_no_update", operator: func(cfg *jetstream.StreamConfig) {
			cfg.Description = "operator edit while unknown"
			cfg.Duplicates = objectRecoveryHorizon
		}, updates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, js := bucketFixture(t, false, 0)
			bucket := "xfer-unknown-update"
			operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			var sent []string
			wrapped := &interceptedJS{JetStream: js}
			wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
				digest, err := configDigest(cfg)
				if err != nil {
					return nil, err
				}
				sent = append(sent, digest)
				if len(sent) > 1 {
					return js.UpdateStream(callCtx, cfg)
				}
				if tc.applied {
					if _, err := js.UpdateStream(callCtx, cfg); err != nil {
						return nil, err
					}
				}
				return nil, withheldReply(callCtx)
			}
			owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
			patch := xfercontract.ConfigPatch{Bucket: bucket}
			seal, err := owner.Apply(patch, bucketDeadline(t, 200*time.Millisecond))
			requireResultUnknown(t, seal, err)
			if tc.operator != nil {
				current := bucketInfo(ctx, t, js, bucket).Config
				tc.operator(&current)
				if _, err := js.UpdateStream(ctx, current); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config)
			seal, err = owner.Apply(patch, bucketDeadline(t, 5*time.Second))
			if wrapped.updates.Load() != tc.updates {
				t.Fatalf("retry sent %d UPDATEs, want %d", wrapped.updates.Load(), tc.updates)
			}
			if len(sent) == 2 && sent[0] != sent[1] {
				t.Fatal("retry of an unknown UPDATE sent a different full configuration")
			}
			state, freezeErr := owner.Freeze(bucketDeadline(t, time.Second))
			if !tc.matches {
				requireResultUnknown(t, seal, err)
				after, _ := configDigest(bucketInfo(ctx, t, js, bucket).Config)
				if after != before {
					t.Fatal("unresolved retry overwrote the operator's newer configuration")
				}
				if freezeErr == nil || !state.Attempts[0].SentUnknown {
					t.Fatalf("unresolved UPDATE no longer blocks retirement: state=%+v err=%v", state, freezeErr)
				}
				return
			}
			if err != nil || seal.Duplicates < objectRecoveryHorizon {
				t.Fatalf("identical retry did not confirm the same configuration: seal=%+v err=%v", seal, err)
			}
			// Matching retry ACKs authorize only this same config; they never
			// answer for the original full UPDATE still in transit.
			// origin: S7 internal review rv-jsnats F1
			if freezeErr == nil || !state.Attempts[0].SentUnknown || state.Admission.Closed || state.Proofs[0].RequestsJoined {
				t.Fatalf("retry ACK retired the original UPDATE: state=%+v err=%v", state, freezeErr)
			}
		})
	}
}

// A definite answer to the retry is only half of the resolution proof: if the full readback does not
// match, the earlier unknown request may still land and keeps blocking handoff.
// origin: hot-update S7 plan §2.1 / T05 / lead decision on G67 replay
func TestTransferBucketConfigRetryWithoutMatchingReadbackStaysUnknown(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	bucket := "xfer-unmatched-readback"
	operatorBucket(ctx, t, js, bucket, 48*time.Hour)
	var calls atomic.Int32
	wrapped := &interceptedJS{JetStream: js}
	wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
		if calls.Add(1) == 1 {
			return nil, withheldReply(callCtx)
		}
		stream, err := js.UpdateStream(callCtx, cfg)
		if err != nil {
			return nil, err
		}
		cfg.Description = "operator changed after the retry"
		_, err = js.UpdateStream(callCtx, cfg)
		return stream, err
	}
	owner := bucketCoordinator(t, wrapped, xfercontract.StorageLimits{}, 1, nil)
	patch := xfercontract.ConfigPatch{Bucket: bucket}
	seal, err := owner.Apply(patch, bucketDeadline(t, 200*time.Millisecond))
	requireResultUnknown(t, seal, err)
	seal, err = owner.Apply(patch, bucketDeadline(t, 5*time.Second))
	if err == nil || !strings.Contains(err.Error(), xfercontract.CodeObjectConflict) || seal != (xfercontract.BucketSeal{}) || calls.Load() != 2 {
		t.Fatalf("retry with a conflicting readback produced a seal: seal=%+v err=%v updates=%d", seal, err, calls.Load())
	}
	state, err := owner.Freeze(bucketDeadline(t, time.Second))
	if err == nil || len(state.Attempts) != 2 || !state.Attempts[0].SentUnknown || state.Attempts[1].SentUnknown {
		t.Fatalf("a definite retry answer alone resolved the unknown request: state=%+v err=%v", state, err)
	}
}

// origin: hot-update S7 plan §2.1 / T05 / admission and proof guards
func TestTransferBucketConfigRejectsInvalidAuthorityAndDeadlines(t *testing.T) {
	_, js := bucketFixture(t, false, 0)
	authority, err := NewAuthority(bucketOwner(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &interceptedJS{JetStream: js}
	cfg := Config{Base: xfercontract.Base{Clock: bucketClock}, JS: wrapped, Authority: authority}
	owner, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, deadline := range []controlbudget.Deadline{0, 1, -1} {
		if _, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-invalid"}, deadline); err == nil {
			t.Fatalf("invalid/expired deadline %d admitted", deadline)
		}
	}
	for _, patch := range []xfercontract.ConfigPatch{{Bucket: "other"}, {Bucket: "xfer-bad", Replicas: 4}, {Bucket: "xfer-bad", MaxBytes: -1}, {Bucket: "xfer-bad", Duplicates: -1}} {
		if _, err := owner.Apply(patch, bucketDeadline(t, time.Second)); err == nil {
			t.Fatalf("invalid patch admitted: %+v", patch)
		}
	}
	var changed atomic.Bool
	cfg.Clock = func() (string, int64, error) {
		boot, now, err := bucketClock()
		if changed.Load() {
			boot = "different-boot"
		}
		return boot, now, err
	}
	clockOwner, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	changed.Store(true)
	if _, err := clockOwner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-clock"}, bucketDeadline(t, time.Second)); err == nil {
		t.Fatal("different boot clock admitted IO")
	}
	if wrapped.lookups.Load() != 0 || wrapped.updates.Load() != 0 || wrapped.creates.Load() != 0 {
		t.Fatal("invalid admission performed storage IO")
	}
	proof, err := authority.Acquire("xfer-proof", bucketDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Acquire(proof.Bucket, bucketDeadline(t, time.Second)); err == nil {
		t.Fatal("second coordinator acquired the same live configuration authority")
	}
	if err := authority.Retire(proof, bucketDeadline(t, time.Second)); err == nil {
		t.Fatal("open unjoined proof retired")
	}
	proof.GateClosed, proof.RequestsJoined = true, true
	wrong := proof
	wrong.Nonce = "replacement"
	if err := authority.Retire(wrong, bucketDeadline(t, time.Second)); err == nil {
		t.Fatal("different writer retired original authority")
	}
	if err := authority.Retire(proof, bucketDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Acquire(proof.Bucket, bucketDeadline(t, time.Second)); err == nil {
		t.Fatal("retired authority reacquired bucket")
	}
	for name, previous := range map[string]*xfercontract.ConfigWriterState{
		"missing":  nil,
		"open":     {Admission: xfercontract.AdmissionState{Generation: 1}},
		"inflight": {Admission: xfercontract.AdmissionState{Closed: true, Generation: 1, Inflight: 1}},
		"unjoined": {Admission: xfercontract.AdmissionState{Closed: true, Generation: 1}, Proofs: []xfercontract.ConfigWriterProof{bucketOwner(1)}},
	} {
		if _, err := NewAuthority(bucketOwner(2), previous); err == nil {
			t.Fatalf("%s predecessor proof permitted handoff", name)
		}
	}
	t.Run("partial_retirement_retains_closed_gate_and_can_retry", func(t *testing.T) {
		authority, err := NewAuthority(bucketOwner(1), nil)
		if err != nil {
			t.Fatal(err)
		}
		failure := errors.New("retirement response unavailable")
		var failOnce atomic.Bool
		failOnce.Store(true)
		retiring := &interceptedAuthority{ConfigAuthority: authority, retire: func(proof xfercontract.ConfigWriterProof, deadline controlbudget.Deadline) error {
			if proof.Bucket == "xfer-retire-b" && failOnce.CompareAndSwap(true, false) {
				return failure
			}
			return authority.Retire(proof, deadline)
		}}
		current, err := New(Config{Base: xfercontract.Base{Clock: bucketClock}, JS: js, Authority: retiring})
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range []string{"xfer-retire-a", "xfer-retire-b"} {
			if _, err := current.Apply(xfercontract.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		state, err := current.Freeze(bucketDeadline(t, time.Second))
		if !errors.Is(err, failure) || !state.Admission.Closed || len(state.Proofs) != 2 || !state.Proofs[0].RequestsJoined || state.Proofs[1].RequestsJoined {
			t.Fatalf("partial retirement lost original proof set: state=%+v err=%v", state, err)
		}
		if _, err := current.Apply(xfercontract.ConfigPatch{Bucket: "xfer-retire-b"}, bucketDeadline(t, time.Second)); err == nil {
			t.Fatal("partial retirement reopened configuration admission")
		}
		if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
			t.Fatal("partial retirement allowed successor")
		}
		state, err = current.Freeze(bucketDeadline(t, time.Second))
		if err != nil || !state.Proofs[0].RequestsJoined || !state.Proofs[1].RequestsJoined {
			t.Fatalf("retirement retry did not join original authority: state=%+v err=%v", state, err)
		}
	})
}

type interceptedAuthority struct {
	xfercontract.ConfigAuthority
	retire func(xfercontract.ConfigWriterProof, controlbudget.Deadline) error
}

func (a *interceptedAuthority) Retire(proof xfercontract.ConfigWriterProof, deadline controlbudget.Deadline) error {
	return a.retire(proof, deadline)
}
