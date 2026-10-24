package xferserve

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go/jetstream"
)

type lifetimeListStore struct {
	jetstream.ObjectStore
	entered chan context.Context
}

func (s lifetimeListStore) List(ctx context.Context, _ ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	s.entered <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

type lifetimeListJS struct {
	jetstream.JetStream
	store lifetimeListStore
}

func (s lifetimeListJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return s.store, nil
}

// Both List owners must retain the earlier pass deadline, honor its stop signal
// while blocked in the SDK, and join their cancellation watchers before return.
// origin: S7 internal review GATES-PROD F1 / hot-update S7 plan lead LQ3
func TestLegacyReaperDeadlineAndStopJoinBothLists(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyReaperLifetime(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy reaper cancellation", g)
	testharness.AssertNoFDLeak(t, "legacy reaper cancellation", fd, 0)
}

func exerciseLegacyReaperLifetime(t *testing.T) {
	t.Helper()
	// Exercise the timer race deterministically: after the original deadline,
	// its stop signal must leave the deadline timer in charge of the error.
	canceled := false
	cancelLegacyReapBeforeDeadline(time.Now().Add(-time.Second), func() { canceled = true })
	if canceled {
		t.Error("expired pass stop signal replaced deadline with cancellation")
	}
	for _, aged := range []bool{false, true} {
		for _, preCanceled := range []bool{false, true} {
			store := lifetimeListStore{entered: make(chan context.Context, 1)}
			reaper := LegacyReaper{JS: lifetimeListJS{store: store}, Now: time.Now, Logger: testharness.SilentLog()}
			stop := make(chan struct{})
			if preCanceled {
				close(stop)
			}
			// The long fallback cannot satisfy the cancellation assertion. The
			// SDK must return because the original scheduler closed its signal.
			until := time.Now().Add(time.Minute)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if aged {
					reaper.HasAgedObjects(until, stop, "OBJ_xfer-lifetime", time.Hour)
				} else {
					reaper.ReapBucketObjects(until, stop, "OBJ_xfer-lifetime", func(int64) time.Duration { return time.Hour }, nil, nil)
				}
			}()
			ctx := <-store.entered
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 5*time.Second {
				t.Errorf("SDK List lost five-second operation ceiling: %v, %v", deadline, ok)
			}
			if !preCanceled {
				close(stop)
			}
			select {
			case <-done:
				if ctx.Err() != context.Canceled {
					t.Errorf("List returned without original stop cancellation: %v", ctx.Err())
				}
			case <-time.After(2 * time.Second):
				t.Error("List ignored original stop signal")
				<-done // Join even a mutant's bounded fallback before leak checks.
			}
		}
	}
	// An inherited earlier deadline must survive conversion without a new grant.
	until := time.Now().Add(30 * time.Millisecond)
	ctx, cancel := legacyReapContext(until, nil)
	defer cancel()
	if got, ok := ctx.Deadline(); !ok || !got.Equal(until) {
		t.Errorf("original pass deadline refreshed: got %v, want %v", got, until)
	}
	<-ctx.Done()
	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("original pass expiry lost: %v", ctx.Err())
	}
}
