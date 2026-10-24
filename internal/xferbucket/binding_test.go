package xferbucket

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
	"github.com/LinZiyang666/tether/internal/xferforward"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §2.1 / T05 / M14
func TestTransferLegacyConfigUsesSingleCoordinator(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLegacyConfig(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy config owner", beforeG)
	testharness.AssertNoFDLeak(t, "legacy config owner", beforeFD, 0)
}
func exerciseTransferLegacyConfig(t *testing.T) {
	t.Helper()
	t.Run("shared_writer", func(t *testing.T) {
		ctx, js := bucketFixture(t, false, 0)
		_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-shared", MaxBytes: 1 << 30, TTL: 48 * time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := js.Stream(ctx, "OBJ_xfer-shared")
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stale := info.Config
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var updates atomic.Int32
		intercepted := &interceptedJS{JetStream: js}
		intercepted.update = func(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
			if updates.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return js.UpdateStream(ctx, cfg)
		}
		owner := bucketCoordinator(t, intercepted, xfercontract.StorageLimits{}, 1, nil)
		bound := Bind(js, owner)
		go func() {
			_, err := owner.Apply(xfercontract.ConfigPatch{Bucket: "xfer-shared"}, bucketDeadline(t, 3*time.Second))
			done <- err
		}()
		joined := false
		defer func() {
			if !joined {
				close(release)
				<-done
			}
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		object := jetstream.ObjectStoreConfig{Bucket: "xfer-shared", MaxBytes: stale.MaxBytes}
		for _, call := range []func(context.Context) error{
			func(ctx context.Context) error { _, err := bound.UpdateStream(ctx, stale); return err },
			func(ctx context.Context) error { _, err := bound.CreateStream(ctx, stale); return err },
			func(ctx context.Context) error { _, err := bound.CreateOrUpdateStream(ctx, stale); return err },
			func(ctx context.Context) error { _, err := bound.CreateObjectStore(ctx, object); return err },
			func(ctx context.Context) error { _, err := bound.UpdateObjectStore(ctx, object); return err },
			func(ctx context.Context) error { _, err := bound.CreateOrUpdateObjectStore(ctx, object); return err },
		} {
			short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			err = call(short)
			cancel()
			if (!errors.Is(err, context.DeadlineExceeded) && !controlbudget.IsExpired(err)) || updates.Load() != 1 {
				t.Fatalf("legacy caller bypassed the in-flight coordinator: updates=%d err=%v", updates.Load(), err)
			}
		}
		close(release)
		err = <-done
		joined = true
		if err != nil {
			t.Fatal(err)
		}
		current, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		current.Config.Description = "operator-current"
		current.Config.Metadata = map[string]string{"operator": "preserved"}
		if _, err := js.UpdateStream(ctx, current.Config); err != nil {
			t.Fatal(err)
		}
		if _, err := bound.UpdateStream(ctx, stale); err != nil {
			t.Fatal(err)
		}
		after, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if after.Config.Description != "operator-current" || after.Config.Metadata["operator"] != "preserved" || after.Config.Duplicates < objectRecoveryHorizon || after.Config.MaxAge != 48*time.Hour {
			t.Fatalf("legacy snapshot replaced current configuration: %+v", after.Config)
		}
		// No frozen idle-proof field exists. A caller must not report a size repair
		// as successful when the current reservation has not actually changed.
		stale.MaxBytes /= 2
		if _, err := bound.UpdateStream(ctx, stale); err == nil {
			t.Fatal("unproved reservation shrink succeeded")
		}
		if _, err := Guard(js).UpdateStream(ctx, current.Config); err == nil {
			t.Fatal("unbound writer changed transfer configuration")
		}
		if _, err := Guard(js).CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-unbound", MaxBytes: 1 << 30}); err == nil {
			t.Fatal("unbound writer created transfer bucket")
		}
		if _, err := js.Stream(ctx, "OBJ_xfer-unbound"); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("unbound write had a side effect: %v", err)
		}
		if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	})
}

// origin: hot-update S7 plan lead LQ3 / §2.1 / T05
func TestTransferBucketRefusalSurvivesForwarding(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	_, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-short", MaxBytes: 1 << 30, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	owner := bucketCoordinator(t, js, xfercontract.StorageLimits{}, 1, nil)
	patch := xfercontract.ConfigPatch{Bucket: "xfer-short"}
	_, err = owner.Apply(patch, bucketDeadline(t, time.Second))
	var coded interface{ TransferCode() string }
	if !errors.As(err, &coded) || coded.TransferCode() != xfercontract.CodeStorageHorizon {
		t.Fatalf("untyped storage refusal: %v", err)
	}
	reply := xferforward.Dispatch(xferforward.Config{Buckets: owner}, xferforward.Envelope{Verb: xferforward.VerbTransferBucketConfig}, xferforward.Request{Patch: &patch, BudgetNS: int64(time.Second)})
	if reply.Code != xfercontract.CodeStorageHorizon || !strings.Contains(reply.Error, "Duplicates >= 37m8s") || !strings.Contains(reply.Error, "MaxAge >= 37m8s") || reply.Seal != nil {
		t.Fatalf("forwarding erased required horizon: %+v", reply)
	}
	if err := owner.Stop(bucketDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, refusal := range []error{&Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "capacity"}, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "retained owner"}, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "response unknown", cause: errors.New("private diagnostic")}} {
		if !errors.As(refusal, &coded) || coded.TransferCode() == "" || strings.Contains(refusal.Error(), "private diagnostic") {
			t.Fatalf("invalid public refusal: %v", refusal)
		}
	}
}
