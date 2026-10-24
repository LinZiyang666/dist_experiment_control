package xferbucket

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// Copy into internal/xferbucket. Existing package fixtures only start embedded
// NATS servers and register Shutdown/WaitForShutdown with the test.
// origin: S7 internal review rv-jsnats F1
func TestConfigDelayedUpdateCannotOutliveRetirement(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	bucket := "xfer-late-config"
	operatorBucket(ctx, t, js, bucket, 48*time.Hour)
	wrapped := &interceptedJS{JetStream: js}
	var late jetstream.StreamConfig
	first := true
	wrapped.update = func(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
		if first {
			first = false
			late = cfg // Request retained in transport/API queue; no response yet.
			return nil, context.DeadlineExceeded
		}
		return js.UpdateStream(ctx, cfg)
	}
	owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
	patch := x.ConfigPatch{Bucket: bucket}
	if _, err := owner.Apply(patch, bucketDeadline(t, time.Second)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first UPDATE did not become sent-unknown: %v", err)
	}
	if _, err := owner.Apply(patch, bucketDeadline(t, time.Second)); err != nil {
		return // Keeping the unresolved request fenced is a valid repair.
	}
	previous, err := owner.Freeze(bucketDeadline(t, time.Second))
	if err != nil {
		return // A refusal to certify retirement also preserves safety.
	}
	next := bucketCoordinator(t, js, x.StorageLimits{}, 2, &previous)
	patch.Duplicates = objectRecoveryHorizon + time.Minute
	seal, err := next.Apply(patch, bucketDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// The originally timed-out request finally executes against the real server.
	if _, err := js.UpdateStream(ctx, late); err != nil {
		t.Fatal(err)
	}
	actual := bucketInfo(ctx, t, js, bucket).Config.Duplicates
	if actual < seal.Duplicates {
		t.Fatalf("retired UPDATE overwrote successor seal: sealed=%s actual=%s proofs=%+v", seal.Duplicates, actual, previous.Proofs)
	}
}

// A retry ACK is evidence about that request only, even when every delayed
// original carries the same digest. Both the same owner and its successor must
// retain all unresolved sends before any different configuration is authorized.
// origin: S7 internal review rv-jsnats F1
func TestConfigDelayedUpdateCannotOverwriteConsecutivePatch(t *testing.T) {
	for _, change := range []string{"duplicates", "replicas"} {
		t.Run(change, func(t *testing.T) {
			ctx, js := bucketFixture(t, change == "replicas", 0)
			bucket := "xfer-consecutive"
			operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			wrapped := &interceptedJS{JetStream: js}
			var delayed []jetstream.StreamConfig
			wrapped.update = func(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
				if len(delayed) < 2 {
					delayed = append(delayed, cfg)
					return nil, context.DeadlineExceeded
				}
				return js.UpdateStream(ctx, cfg)
			}
			owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
			patch := x.ConfigPatch{Bucket: bucket}
			for i := 0; i < 2; i++ {
				seal, err := owner.Apply(patch, bucketDeadline(t, time.Second))
				requireResultUnknown(t, seal, err)
			}
			// Exercise a genuine retry ACK and full readback from the real server.
			seal, err := owner.Apply(patch, bucketDeadline(t, 5*time.Second))
			if err != nil || seal.ConfigDigest == "" || wrapped.updates.Load() != 3 {
				t.Fatalf("same-intent retry did not reach a definite response: seal=%+v err=%v", seal, err)
			}
			state, err := owner.Freeze(bucketDeadline(t, time.Second))
			if err == nil || !strings.Contains(err.Error(), x.CodeResultUnknown) || state.Admission.Closed {
				t.Fatalf("retry ACK retired original unresolved UPDATEs: state=%+v err=%v", state, err)
			}
			unknown := 0
			for _, attempt := range state.Attempts {
				if attempt.SentUnknown {
					unknown++
				}
			}
			if unknown != len(delayed) || len(state.Attempts) != len(delayed)+1 || state.Proofs[0].RequestsJoined {
				t.Fatalf("retirement lost original attempts: pending=%d delayed=%d state=%+v", unknown, len(delayed), state)
			}
			if _, err := NewAuthority(bucketOwner(2), &state); err == nil {
				t.Fatal("successor acquired authority with unresolved originals")
			}
			if change == "replicas" {
				patch.Replicas = 3
			} else {
				patch.Duplicates = objectRecoveryHorizon + time.Minute
			}
			lookups, updates := wrapped.lookups.Load(), wrapped.updates.Load()
			seal, err = owner.Apply(patch, bucketDeadline(t, 5*time.Second))
			requireResultUnknown(t, seal, err)
			if wrapped.lookups.Load() != lookups || wrapped.updates.Load() != updates {
				t.Fatal("a consecutive patch issued IO despite unresolved original UPDATEs")
			}
			for i := len(delayed) - 1; i >= 0; i-- {
				if _, err := js.UpdateStream(ctx, delayed[i]); err != nil {
					t.Fatal(err)
				}
			}
			actual := bucketInfo(ctx, t, js, bucket).Config
			if actual.Replicas != 1 || actual.Duplicates != objectRecoveryHorizon {
				t.Fatalf("unresolved intent changed unexpectedly: %+v", actual)
			}
			// The test knows the delayed packets landed. The owner has no reply
			// for them, so equal INFO still cannot authorize its next patch.
			seal, err = owner.Apply(patch, bucketDeadline(t, time.Second))
			requireResultUnknown(t, seal, err)
		})
	}
}

// origin: S7 internal review rv-jsnats F1
func TestConfigUnknownUpdateAttemptsRemainBounded(t *testing.T) {
	ctx, js := bucketFixture(t, false, 0)
	bucket := "xfer-attempt-capacity"
	operatorBucket(ctx, t, js, bucket, 48*time.Hour)
	wrapped := &interceptedJS{JetStream: js, update: func(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
		return nil, context.DeadlineExceeded
	}}
	owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
	for i := 0; i < configAttemptCapacity+1; i++ {
		seal, err := owner.Apply(x.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
		requireResultUnknown(t, seal, err)
	}
	state, err := owner.Freeze(bucketDeadline(t, time.Second))
	if err == nil || len(state.Attempts) != configAttemptCapacity || wrapped.updates.Load() != configAttemptCapacity {
		t.Fatalf("retry capacity sent or lost unresolved responsibility: attempts=%d sends=%d err=%v", len(state.Attempts), wrapped.updates.Load(), err)
	}
	for _, attempt := range state.Attempts {
		if !attempt.SentUnknown {
			t.Fatal("capacity exhaustion retired an unresolved attempt")
		}
	}
}

// Every rejection is before CREATE/UPDATE and first-byte admission. Counts and
// per-subject limits need a complete version reservation, which a bucket patch
// cannot provide. DiscardNew alone still evicts at MaxMsgsPerSubject in NATS.
// origin: S7 internal review rv-jsnats F2
func TestConfigRetentionRefusesEvictionBeforeFirstChunk(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*jetstream.StreamConfig)
	}{
		{"discard_old_bytes", func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld }},
		{"discard_old_count", func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld; c.MaxMsgs = 1 }},
		{"discard_old_subject", func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld; c.MaxMsgsPerSubject = 1 }},
		{"discard_new_count", func(c *jetstream.StreamConfig) { c.MaxMsgs = 1 }},
		{"discard_new_subject", func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }},
		{"interest", func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }},
		{"workqueue", func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, js := bucketFixture(t, false, 0)
			bucket := "xfer-retention"
			cfg := operatorBucket(ctx, t, js, bucket, 48*time.Hour)
			tc.change(&cfg)
			// Retention policy is immutable after CREATE. Recreate only this
			// empty fixture, before there is any owner, seal or object version.
			if err := js.DeleteStream(ctx, cfg.Name); err != nil {
				t.Fatal(err)
			}
			if _, err := js.CreateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			original := bucketInfo(ctx, t, js, bucket).Config
			before, _ := configDigest(original)
			wrapped := &interceptedJS{JetStream: js}
			owner := bucketCoordinator(t, wrapped, x.StorageLimits{}, 1, nil)
			seal, err := owner.Apply(x.ConfigPatch{Bucket: bucket}, bucketDeadline(t, time.Second))
			// The lead's FIX-CONVERGE verdict permits exactly this tether-owned
			// migration; count/subject/retention/TTL limits still fail unchanged.
			// Keep the original case and check every field of its single UPDATE.
			// origin: S7 internal review FIX-CONVERGE F1 / lead Discard decision
			if tc.name == "discard_old_bytes" {
				if err != nil {
					t.Fatalf("tether-owned DiscardOld was not migrated: %v", err)
				}
				original.Discard, original.Duplicates = jetstream.DiscardNew, objectRecoveryHorizon
				assertBucketConfig(ctx, t, js, seal, original)
				if wrapped.updates.Load() != 1 || wrapped.creates.Load() != 0 {
					t.Fatal("discard migration did not use exactly one full UPDATE")
				}
				return
			}
			assertHorizonRefusal(t, seal, err)
			var refusal *Refusal
			if !errors.As(err, &refusal) || refusal.TransferCode() != x.CodeStorageHorizon {
				t.Fatalf("retention refusal lost retryable storage-horizon code: %v", err)
			}
			info := bucketInfo(ctx, t, js, bucket)
			after, _ := configDigest(info.Config)
			if after != before || wrapped.updates.Load() != 0 || wrapped.creates.Load() != 0 || info.State.Msgs != 0 {
				t.Fatal("retention admission rewrote operator config or published a chunk")
			}
		})
	}
}
