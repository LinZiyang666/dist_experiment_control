package xferbucket

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S7 internal review FIX-CONVERGE F1
func TestTransferBucketMigratesDiscardWithRecoveryWindow(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseDiscardMigration(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer discard migration", g)
	testharness.AssertNoFDLeak(t, "transfer discard migration", fd, 0)
}

func exerciseDiscardMigration(t *testing.T) {
	t.Helper()
	t.Run("existing_bucket", func(t *testing.T) {
		ctx, js := bucketFixture(t, false, 0)
		bucket := "xfer-discard-migration"
		expected := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
		// Existing deployments can carry the server default independently of
		// the current SDK, which explicitly creates new stores as DiscardNew.
		expected.Discard = jetstream.DiscardOld
		if _, err := js.UpdateStream(ctx, expected); err != nil {
			t.Fatal(err)
		}
		before := bucketInfo(ctx, t, js, bucket)
		if before.Config.Discard != jetstream.DiscardOld {
			t.Fatalf("pre-existing fixture did not retain DiscardOld: %+v", before.Config)
		}
		wrapped := &interceptedJS{JetStream: js}
		owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
		seal, err := owner.Apply(x.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
		if err != nil {
			t.Fatalf("tether bucket cannot migrate DiscardOld: %v", err)
		}
		expected.Discard, expected.Duplicates = jetstream.DiscardNew, objectRecoveryHorizon
		assertBucketConfig(ctx, t, js, seal, expected)
		if wrapped.creates.Load() != 0 || wrapped.updates.Load() != 1 || !bucketInfo(ctx, t, js, bucket).Created.Equal(before.Created) {
			t.Fatalf("migration must be one authoritative UPDATE of the original stream: creates=%d updates=%d", wrapped.creates.Load(), wrapped.updates.Load())
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	})
}

// The retry may confirm this intent, but it cannot join a delayed original
// UPDATE or authorize another owner. Discard and Duplicates share that fence.
// origin: S7 internal review FIX-CONVERGE F1
func TestTransferDiscardMigrationRetainsUnknownUpdate(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnknownDiscardUpdate(t)
	}
	testharness.AssertNoGoroutineLeak(t, "unknown discard migration", g)
	testharness.AssertNoFDLeak(t, "unknown discard migration", fd, 0)
}

func exerciseUnknownDiscardUpdate(t *testing.T) {
	t.Helper()
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "request_delayed", true: "reply_lost"}[applied], func(t *testing.T) {
			ctx, js := bucketFixture(t, false, 0)
			bucket := "xfer-unknown-discard"
			cfg := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			cfg.Discard = jetstream.DiscardOld
			if _, err := js.UpdateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			wrapped := &interceptedJS{JetStream: js}
			var sent []jetstream.StreamConfig
			wrapped.update = func(callCtx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
				sent = append(sent, cfg)
				if len(sent) != 1 || applied {
					if _, err := js.UpdateStream(callCtx, cfg); err != nil {
						return nil, err
					}
				}
				if len(sent) == 1 {
					return nil, context.DeadlineExceeded
				}
				return js.Stream(callCtx, cfg.Name)
			}
			owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
			patch := x.ConfigPatch{Bucket: bucket}
			seal, err := owner.Apply(patch, bucketDeadline(t, time.Second))
			requireResultUnknown(t, seal, err)
			if !errors.Is(err, context.DeadlineExceeded) || len(sent) != 1 || sent[0].Discard != jetstream.DiscardNew || sent[0].Duplicates != objectRecoveryHorizon {
				t.Fatalf("migration did not retain the complete unknown UPDATE: sent=%+v err=%v", sent, err)
			}
			seal, err = owner.Apply(patch, bucketDeadline(t, time.Second))
			if err != nil || seal.Duplicates != objectRecoveryHorizon || bucketInfo(ctx, t, js, bucket).Config.Discard != jetstream.DiscardNew {
				t.Fatalf("identical retry did not confirm migration: seal=%+v err=%v", seal, err)
			}
			if !applied && (len(sent) != 2 || !reflect.DeepEqual(sent[0], sent[1])) || applied && len(sent) != 1 {
				t.Fatalf("retry changed the original snapshot: %+v", sent)
			}
			patch.Duplicates = objectRecoveryHorizon + time.Second
			seal, err = owner.Apply(patch, bucketDeadline(t, time.Second))
			requireResultUnknown(t, seal, err)
			state, err := owner.Freeze(bucketDeadline(t, time.Second))
			if err == nil || len(state.Attempts) == 0 || !state.Attempts[0].SentUnknown || state.Admission.Closed {
				t.Fatalf("retry retired an unresolved migration: state=%+v err=%v", state, err)
			}
			if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
				t.Fatal("successor took authority while original migration could still land")
			}
			// Deliver the original at its original digest. It must preserve the
			// confirmed configuration, without granting a retirement proof.
			if _, err := js.UpdateStream(ctx, sent[0]); err != nil {
				t.Fatal(err)
			}
			if bucketInfo(ctx, t, js, bucket).Config.Discard != jetstream.DiscardNew {
				t.Fatal("late original undid the confirmed migration")
			}
		})
	}
}

// origin: S7 internal review FIX-CONVERGE F1
func TestTransferDiscardMigrationLeavesOperatorStreamsUntouched(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseOperatorDiscard(t)
	}
	testharness.AssertNoGoroutineLeak(t, "operator discard policy", g)
	testharness.AssertNoFDLeak(t, "operator discard policy", fd, 0)
}

func exerciseOperatorDiscard(t *testing.T) {
	t.Helper()
	t.Run("operator_bucket", func(t *testing.T) {
		ctx, js := bucketFixture(t, false, 0)
		wrapped := &interceptedJS{JetStream: js}
		owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
		bound := Bind(js, owner)
		if _, err := bound.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "operator", MaxBytes: 256 << 20}); err != nil {
			t.Fatal(err)
		}
		before := bucketInfo(ctx, t, js, "operator").Config
		before.Discard = jetstream.DiscardOld
		if _, err := js.UpdateStream(ctx, before); err != nil {
			t.Fatal(err)
		}
		seal, err := owner.Apply(x.ConfigPatch{Bucket: "operator"}, bucketDeadline(t, time.Second))
		if err == nil || seal != (x.BucketSeal{}) || wrapped.updates.Load() != 0 || !reflect.DeepEqual(before, bucketInfo(ctx, t, js, "operator").Config) || before.Discard != jetstream.DiscardOld {
			t.Fatalf("non-tether bucket was migrated: before=%+v seal=%+v err=%v", before, seal, err)
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	})
}
