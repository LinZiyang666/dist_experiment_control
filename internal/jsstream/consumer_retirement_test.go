package jsstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Copy into internal/jsstream; exercises real consumer creation followed by
// physical connection loss, then checks the global cleanup owner's state.
// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupHasOwnerAfterPhysicalConnectionClose(t *testing.T) {
	url := startJSNATS(t)
	js := newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, js, "closed-cleanup")
	spec := transferReaderSpec(ctx, t, js, store, stream, make([]byte, 64<<10))
	reader, err := NewTransferReader(js, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	r := reader.(*transferResumeReader)
	t.Cleanup(func() { settleConsumerCleanup(t, js.Conn(), newJS(t, url)) })
	if _, err := reader.ReadChunk(transferReaderDeadline(t, 2*time.Second)); err != nil {
		t.Fatal(err)
	}
	nc := js.Conn()
	nc.Close()
	if err := reader.Stop(transferReaderDeadline(t, time.Second)); err == nil {
		t.Fatal("closed connection unexpectedly confirmed consumer deletion")
	}
	objectWindows.Lock()
	done := r.window.cleanupDone
	objectWindows.Unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup process did not join")
	}
	objectWindows.Lock()
	w := objectWindows.byConn[nc]
	orphaned := w != nil && w.refs == 0 && len(w.deletes) > 0 && !w.cleanupRunning
	count := len(r.window.deletes)
	// Test-only restoration of global bookkeeping prevents cross-test pollution.
	delete(objectWindows.byConn, nc)
	objectWindows.Unlock()
	if orphaned {
		t.Fatalf("closed Conn retains %d consumer reservations with refs=0 and no cleanup owner; no rebind/export port exists", count)
	}
	owner := ConsumerCleanupFor(nc)
	if owner == nil {
		t.Fatal("closed connection lost its independently reachable cleanup duty")
	}
	state, err := owner.Freeze(transferReaderDeadline(t, time.Second))
	if err != nil || len(state.Deletes) != 1 || state.Deletes[0].Stream != spec.Stream ||
		state.Deletes[0].StreamCreated != spec.StreamCreated || state.Deletes[0].Consumer == "" {
		t.Fatalf("closed connection lost its exportable consumer identity: %+v, %v", state, err)
	}
}

func settleConsumerCleanup(t *testing.T, old *nats.Conn, current jetstream.JetStream) {
	t.Helper()
	owner := ConsumerCleanupFor(old)
	if owner == nil {
		return
	}
	if err := owner.Bind(current, transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Error(err)
		return
	}
	if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Error(err)
	}
}

func consumerReservations() int {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	return objectReservationsLocked()
}

// This adapter leaves the real consumer and Stream INFO untouched. Only DELETE
// fails, so a successful bind alone cannot serve as the deletion receipt.
type unavailableConsumerDelete struct {
	jetstream.JetStream
	attempted chan struct{}
	once      sync.Once
}

func (j *unavailableConsumerDelete) DeleteConsumer(context.Context, string, string) error {
	j.once.Do(func() { close(j.attempted) })
	return nats.ErrTimeout
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupRebindDeletesAndRecoversCapacity(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseConsumerCleanupRebind(t)
	}
	testharness.AssertNoGoroutineLeak(t, "consumer cleanup owner", goroutines)
	testharness.AssertNoFDLeak(t, "consumer cleanup owner", fds, 0)
}

func exerciseConsumerCleanupRebind(t *testing.T) {
	t.Helper()
	t.Run("close_cut_rebind", func(t *testing.T) {
		url := startJSNATS(t)
		js, management := newJS(t, url), newJS(t, url)
		ctx, store, stream := transferObjectFixture(t, js, "cleanup-rebind")
		spec := transferReaderSpec(ctx, t, js, store, stream, make([]byte, 64<<10))
		before := consumerReservations()
		reader, err := NewTransferReader(js, spec, x.SinkCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.ReadChunk(transferReaderDeadline(t, 2*time.Second)); err != nil {
			t.Fatal(err)
		}
		consumer := reader.(*transferResumeReader).consumer
		owner := ConsumerCleanupFor(js.Conn())
		if owner == nil {
			t.Fatal("reader did not register a cleanup owner")
		}
		t.Cleanup(func() { settleConsumerCleanup(t, js.Conn(), management) })
		if _, err := owner.Freeze(transferReaderDeadline(t, time.Second)); err == nil {
			t.Fatal("export accepted a reader that can still add deletion duties")
		}
		js.Conn().Close()
		if err := reader.Stop(transferReaderDeadline(t, time.Second)); err == nil {
			t.Fatal("closed socket confirmed deletion")
		}
		if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err == nil {
			t.Fatal("closed management binding reported all duties retired")
		}
		for cut := 0; cut < 3; cut++ {
			state, err := owner.Freeze(transferReaderDeadline(t, time.Second))
			if err != nil || len(state.Deletes) != 1 || state.Deletes[0].Consumer != consumer {
				t.Fatalf("cut %d lost deletion: %+v %v", cut, state, err)
			}
			// A value/codec round trip exercises detached bytes, not a mutable
			// map alias. The integration owner must place these in sealed storage.
			encoded, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			var imported ConsumerCleanupState
			if err := json.Unmarshal(encoded, &imported); err != nil {
				t.Fatal(err)
			}
			owner, err = ImportConsumerCleanup(imported)
			if err != nil {
				t.Fatal(err)
			}
			state.Deletes[0].Consumer = "mutatedCopy"
			if owner.window.nc != js.Conn() || consumerReservations() != before+1 {
				t.Fatal("cut released the old connection or its pending reservation")
			}
			// Replace and physically close several management sockets while the
			// same server duty remains unconfirmed across successive cuts.
			attachment := newJS(t, url)
			failed := &unavailableConsumerDelete{JetStream: attachment, attempted: make(chan struct{})}
			if err := owner.Bind(failed, transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-failed.attempted:
			case <-ctx.Done():
				t.Fatal("replacement management connection did not attempt cleanup")
			}
			attachment.Conn().Close()
			if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err == nil {
				t.Fatal("replacement management close discharged the server duty")
			}
		}
		unavailable := &unavailableConsumerDelete{JetStream: management, attempted: make(chan struct{})}
		if err := owner.Bind(unavailable, transferReaderDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-unavailable.attempted:
		case <-ctx.Done():
			t.Fatal("current management binding did not attempt deletion")
		}
		state, err := owner.Freeze(transferReaderDeadline(t, time.Second))
		if err != nil || len(state.Deletes) != 1 || consumerReservations() != before+1 {
			t.Fatalf("unconfirmed DELETE released responsibility: %+v %v", state, err)
		}
		var readers []ResumeReader
		defer func() {
			for _, reader := range readers {
				if err := reader.Stop(transferReaderDeadline(t, time.Second)); err != nil {
					t.Error(err)
				}
			}
		}()
		for consumerReservations() < objectWindowLimit {
			reader, err := NewTransferReader(management, spec, x.SinkCheckpoint{})
			if err != nil {
				t.Fatal(err)
			}
			readers = append(readers, reader)
		}
		if extra, err := NewTransferReader(management, spec, x.SinkCheckpoint{}); err == nil {
			readers = append(readers, extra)
			t.Fatal("pending cleanup did not consume reader capacity")
		} else if !strings.Contains(err.Error(), "transfer_capacity_exceeded") {
			t.Fatal(err)
		}
		importAttempt := state
		importAttempt.OwnerID = nuid.Next()
		if extra, err := ImportConsumerCleanup(importAttempt); err == nil {
			forgetFrozenConsumerCleanup(t, extra)
			t.Fatal("cleanup import bypassed the shared reservation limit")
		}
		if err := owner.Bind(management, transferReaderDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := management.Consumer(ctx, spec.Stream, consumer); !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("current management connection did not delete consumer: %v", err)
		}
		if owner.window.nc != nil || ConsumerCleanupFor(js.Conn()) != nil || consumerReservations() != objectWindowLimit-1 {
			t.Fatal("confirmed deletion retained the closed connection or capacity")
		}
		if err := reader.Stop(transferReaderDeadline(t, time.Second)); err != nil {
			t.Fatalf("repeated Stop resurrected cleanup on the closed binding: %v", err)
		}
		extra, err := NewTransferReader(management, spec, x.SinkCheckpoint{})
		if err != nil {
			t.Fatalf("confirmed deletion did not restore admission capacity: %v", err)
		}
		readers = append(readers, extra)
		currentStream, err := management.Stream(ctx, spec.Stream)
		if err != nil {
			t.Fatal(err)
		}
		requireNoTransferObjectConsumers(ctx, t, currentStream)
	})
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupImportsDetachedDuties(t *testing.T) {
	for _, gone := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "already_gone"}[gone], func(t *testing.T) {
			js := newJS(t, startJSNATS(t))
			ctx, _, stream := transferObjectFixture(t, js, "cleanup-import")
			name := nuid.Next()
			if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: name, AckPolicy: jetstream.AckNonePolicy}); err != nil {
				t.Fatal(err)
			}
			if gone {
				if err := stream.DeleteConsumer(ctx, name); err != nil {
					t.Fatal(err)
				}
			}
			state := ConsumerCleanupState{OwnerID: nuid.Next(), Deletes: []ConsumerDelete{{Stream: stream.CachedInfo().Config.Name,
				StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano), Consumer: name}}}
			before := consumerReservations()
			owner, err := ImportConsumerCleanup(state)
			if err != nil {
				t.Fatal(err)
			}
			if owner.window.nc != nil || owner.window.management != nil || owner.window.cleanupRunning || consumerReservations() != before+1 {
				t.Fatal("import did IO or failed to reserve the detached duty")
			}
			snapshot, err := owner.Freeze(transferReaderDeadline(t, time.Second))
			if err != nil || !reflect.DeepEqual(snapshot, state) {
				t.Fatalf("detached duty did not survive another cut: %+v %v", snapshot, err)
			}
			state.Deletes[0].Consumer = "mutatedCopy"
			if err := owner.Bind(js, transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
				t.Fatal(err)
			}
			if consumerReservations() != before {
				t.Fatal("imported duty did not release its reservation")
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
			// A handle retained by old rollback code must not erase an imported
			// owner that reused its image identity after the old duty settled.
			nextName := nuid.Next()
			if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: nextName, AckPolicy: jetstream.AckNonePolicy}); err != nil {
				t.Fatal(err)
			}
			snapshot.Deletes[0].Consumer = nextName
			successor, err := ImportConsumerCleanup(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := owner.Bind(js, transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
				t.Fatal(err)
			}
			if consumerReservations() != before+1 {
				t.Fatal("stale cleanup handle retired the imported successor's reservation")
			}
			if err := successor.Bind(js, transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := successor.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
				t.Fatal(err)
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
		})
	}
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupRejectsInvalidOrConflictingImages(t *testing.T) {
	valid := ConsumerCleanupState{OwnerID: nuid.Next(), Deletes: []ConsumerDelete{{Stream: "OBJ_valid",
		StreamCreated: time.Now().UTC().Format(time.RFC3339Nano), Consumer: nuid.Next()}}}
	for _, mutate := range []func(*ConsumerCleanupState){
		func(s *ConsumerCleanupState) { s.OwnerID = "" },
		func(s *ConsumerCleanupState) { s.Deletes[0].Stream = "other" },
		func(s *ConsumerCleanupState) { s.Deletes[0].StreamCreated = "" },
		func(s *ConsumerCleanupState) { s.Deletes[0].Consumer = "*" },
		func(s *ConsumerCleanupState) { s.Deletes = append(s.Deletes, s.Deletes[0]) },
		func(s *ConsumerCleanupState) { s.Deletes = make([]ConsumerDelete, objectWindowLimit+1) },
	} {
		bad := valid
		bad.Deletes = append([]ConsumerDelete(nil), valid.Deletes...)
		mutate(&bad)
		before := consumerReservations()
		if _, err := ImportConsumerCleanup(bad); err == nil {
			t.Fatal("invalid cleanup image admitted")
		}
		if consumerReservations() != before {
			t.Fatal("rejected cleanup image changed capacity")
		}
	}
	owner, err := ImportConsumerCleanup(valid)
	if err != nil {
		t.Fatal(err)
	}
	defer forgetFrozenConsumerCleanup(t, owner)
	valid.Deletes[0].Consumer = "different"
	if _, err := ImportConsumerCleanup(valid); err == nil {
		t.Fatal("conflicting image overwrote the current duty")
	}
}

// Conflicting/malformed-state tests deliberately cannot obtain deletion proof.
// Join first, then remove only their own inert bookkeeping to isolate tests.
func forgetFrozenConsumerCleanup(t *testing.T, owner *ConsumerCleanupOwner) {
	t.Helper()
	if _, err := owner.Freeze(transferReaderDeadline(t, 3*time.Second)); err != nil {
		t.Error(err)
		return
	}
	objectWindows.Lock()
	delete(objectWindows.owners, owner.window.identity)
	delete(objectWindows.byConn, owner.window.nc)
	objectWindows.Unlock()
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupRequiresCurrentStreamIncarnation(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx, _, original := transferObjectFixture(t, js, "cleanup-incarnation")
	info := original.CachedInfo()
	name := nuid.Next()
	state := ConsumerCleanupState{OwnerID: nuid.Next(), Deletes: []ConsumerDelete{{Stream: info.Config.Name,
		StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Consumer: name}}}
	if err := js.DeleteStream(ctx, info.Config.Name); err != nil {
		t.Fatal(err)
	}
	if err := deleteObjectConsumer(ctx, js, state.Deletes[0]); err == nil {
		t.Fatal("missing stream incorrectly proved consumer absence in its original incarnation")
	}
	current, err := js.CreateStream(ctx, info.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: name, AckPolicy: jetstream.AckNonePolicy}); err != nil {
		t.Fatal(err)
	}
	owner, err := ImportConsumerCleanup(state)
	if err != nil {
		t.Fatal(err)
	}
	defer forgetFrozenConsumerCleanup(t, owner)
	// Invoke the same deletion primitive as the loop so the mismatch has a
	// deterministic receipt, then require the loop to retain the same duty.
	if err := deleteObjectConsumer(ctx, js, state.Deletes[0]); err == nil {
		t.Fatal("recreated stream discharged the old incarnation's deletion")
	}
	if err := owner.Bind(js, transferReaderDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Wait(transferReaderDeadline(t, 300*time.Millisecond)); err == nil {
		t.Fatal("cleanup owner retired a duty using a different stream incarnation")
	}
	if _, err := current.Consumer(ctx, name); err != nil {
		t.Fatalf("old duty deleted the new incarnation's consumer: %v", err)
	}
	retained, err := owner.Freeze(transferReaderDeadline(t, time.Second))
	if err != nil || !reflect.DeepEqual(retained, state) {
		t.Fatalf("incarnation mismatch lost the original duty: %+v %v", retained, err)
	}
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupLegacyReaderUsesCurrentBinding(t *testing.T) {
	url := startJSNATS(t)
	js, management := newJS(t, url), newJS(t, url)
	ctx, store, _ := transferObjectFixture(t, js, "cleanup-legacy")
	putTransferObject(ctx, t, store, make([]byte, 64<<10))
	reader, err := GetTransferObject(ctx, js, store, "cleanup-legacy", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	js.Conn().Close()
	if err := reader.Close(); err == nil {
		t.Fatal("legacy reader confirmed deletion on a closed socket")
	}
	owner := ConsumerCleanupFor(js.Conn())
	if owner == nil {
		t.Fatal("legacy reader lost its deletion duty")
	}
	settleConsumerCleanup(t, js.Conn(), management)
	stream, err := management.Stream(ctx, "OBJ_cleanup-legacy")
	if err != nil {
		t.Fatal(err)
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}

// This port blocks after cancellation, as a management adapter that has not
// finished joining its request would. The real server consumer remains live.
type joiningConsumerDelete struct {
	jetstream.JetStream
	entered, canceled, release chan struct{}
	enterOnce, cancelOnce      sync.Once
}

func (j *joiningConsumerDelete) DeleteConsumer(ctx context.Context, _, _ string) error {
	j.enterOnce.Do(func() { close(j.entered) })
	<-ctx.Done()
	j.cancelOnce.Do(func() { close(j.canceled) })
	<-j.release
	return ctx.Err()
}

// origin: S7 internal review rv-jsnats F3
func TestConsumerCleanupBindingJoinsPreviousAttempt(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "bind_joins", true: "freeze_timeout_keeps_owner"}[timeout], func(t *testing.T) {
			url := startJSNATS(t)
			js, management := newJS(t, url), newJS(t, url)
			ctx, store, stream := transferObjectFixture(t, js, "cleanup-join")
			spec := transferReaderSpec(ctx, t, js, store, stream, make([]byte, 64<<10))
			reader, err := NewTransferReader(js, spec, x.SinkCheckpoint{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reader.ReadChunk(transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			js.Conn().Close()
			if err := reader.Stop(transferReaderDeadline(t, time.Second)); err == nil {
				t.Fatal("closed socket confirmed cleanup")
			}
			owner := ConsumerCleanupFor(js.Conn())
			if owner == nil {
				t.Fatal("cleanup owner missing")
			}
			blocked := &joiningConsumerDelete{JetStream: management, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			defer release()
			t.Cleanup(func() { settleConsumerCleanup(t, js.Conn(), management) })
			if err := owner.Bind(blocked, transferReaderDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-blocked.entered:
			case <-ctx.Done():
				t.Fatal("old binding did not enter DELETE")
			}
			if timeout {
				if _, err := owner.Freeze(transferReaderDeadline(t, 100*time.Millisecond)); err == nil {
					t.Fatal("freeze exported while the previous attempt was unjoined")
				}
				objectWindows.Lock()
				retained := objectWindows.owners[owner.window.identity] == owner.window && len(owner.window.deletes) == 1 && !owner.window.paused
				objectWindows.Unlock()
				if !retained {
					t.Fatal("freeze timeout lost the original owner or left it frozen")
				}
			}
			bound := make(chan error, 1)
			deadline := transferReaderDeadline(t, 3*time.Second)
			go func() { bound <- owner.Bind(management, deadline) }()
			select {
			case <-blocked.canceled:
			case <-bound:
				t.Fatal("management binding changed before cancel/join of its predecessor")
			case <-ctx.Done():
				t.Fatal("binding did not cancel the old DELETE")
			}
			objectWindows.Lock()
			stillOld := owner.window.management == blocked
			objectWindows.Unlock()
			if !stillOld {
				t.Fatal("new management binding overtook an unjoined DELETE")
			}
			release()
			if err := <-bound; err != nil {
				t.Fatal(err)
			}
			if err := owner.Wait(transferReaderDeadline(t, 3*time.Second)); err != nil {
				t.Fatal(err)
			}
			current, err := management.Stream(ctx, spec.Stream)
			if err != nil {
				t.Fatal(err)
			}
			requireNoTransferObjectConsumers(ctx, t, current)
		})
	}
}
