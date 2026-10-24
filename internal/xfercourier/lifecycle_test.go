package xfercourier

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.2 / §12.2 P09 / T16
func TestTransferCourierLifecycleNoLeak(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "courier lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "courier lifecycle", beforeFDs, 0)
}

// exerciseTransferLifecycle carries the canonical transfer-owner exercise name
// so T52's direct-cycle join can recognise this package's lifecycle receipt.
// origin: hot-update S7 plan §11.3 / T52
func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newCourierFixture(t, "complete")
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	f.transport.handle = func(r xfercontract.Request, d xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		close(entered)
		<-release
		// An adapter accidentally modifying its own pointer must not mutate the
		// canonical receiver decision retained by the owner through the cut.
		r.Outcome.Bytes++
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true}, errors.New("lost ACK")
	}
	go func() { done <- f.owner.Step(f.deadline()) }()
	awaitCourierSignal(t, entered)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err := f.owner.Step(f.deadline()); !errors.Is(err, ErrRecovering) {
		t.Fatalf("concurrent terminal request admitted: %v", err)
	}
	short := xfercontract.Deadline(f.now.Load() + int64(20*time.Millisecond))
	if _, err := courierFreezeWithin(t, f.owner, short); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("freeze did not join active request: %v", err)
	}
	if f.capacity.outstanding() != 2 {
		t.Fatal("freeze timeout released original owner")
	}
	close(release)
	if err := awaitCourierResult(t, done); err == nil {
		t.Fatal("lost ACK stopped original courier")
	}
	state, err := f.owner.Freeze(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	if state.Stopped || state.Outcome != f.config.Outcome || !bytes.Equal(state.Canonical.Bytes, f.record.Bytes) {
		t.Fatal("timeout thaw did not preserve owner")
	}
	f.advance(retryMinimum)
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		copyReceipt := f.receipt
		return xfercontract.Response{Receipt: &copyReceipt}, xfercontract.Attempt{Sent: true}, nil
	}
	if err := f.owner.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if f.capacity.outstanding() != 0 || f.transport.count() != 2 {
		t.Fatal("lifecycle did not join/release exactly once")
	}
}

// origin: hot-update S7 plan §8.2 / §12.2 P09 / T16
func TestTransferCourierFreezeAndStopJoinCallbacks(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCourierCallbackJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "courier callback join", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "courier callback join", beforeFDs, 0)
}

func exerciseCourierCallbackJoin(t *testing.T) {
	t.Helper()
	f := newCourierFixture(t, "failed")
	entered, release := make(chan struct{}), make(chan struct{})
	observer := f.config.Observe
	f.owner.(*courier).config.Observe = func(o xfercontract.Observation) {
		observer(o)
		if o.Kind == xfercontract.ObserveAcknowledge {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.owner.Acknowledge(f.receipt) }()
	awaitCourierSignal(t, entered)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	short := xfercontract.Deadline(f.now.Load() + int64(20*time.Millisecond))
	if _, err := courierFreezeWithin(t, f.owner, short); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("freeze skipped entered ACK callback: %v", err)
	}
	if err := f.owner.Stop(short); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("stop skipped entered ACK callback: %v", err)
	}
	if f.capacity.outstanding() != 2 {
		t.Fatal("callback timeout retired capacity")
	}
	close(release)
	if err := awaitCourierResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if f.capacity.outstanding() != 0 {
		t.Fatal("joined callback retained capacity")
	}
	// The snapshot's own observation is also a callback. Stop cannot retire its
	// owner while that final producer receipt is still being delivered.
	frozen := newCourierFixture(t, "failed")
	freezeEntered, freezeRelease := make(chan struct{}), make(chan struct{})
	freezeObserver := frozen.config.Observe
	frozen.owner.(*courier).config.Observe = func(o xfercontract.Observation) {
		freezeObserver(o)
		if o.Kind == xfercontract.ObserveFreeze {
			close(freezeEntered)
			<-freezeRelease
		}
	}
	freezeDone := make(chan error, 1)
	go func() { _, err := frozen.owner.Freeze(frozen.deadline()); freezeDone <- err }()
	awaitCourierSignal(t, freezeEntered)
	defer func() {
		select {
		case <-freezeRelease:
		default:
			close(freezeRelease)
		}
	}()
	if err := frozen.owner.Stop(xfercontract.Deadline(frozen.now.Load() + int64(20*time.Millisecond))); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("stop skipped final freeze observation: %v", err)
	}
	close(freezeRelease)
	if err := awaitCourierResult(t, freezeDone); err != nil {
		t.Fatal(err)
	}
	if err := frozen.owner.Stop(frozen.deadline()); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.2 / T16
func TestTransferCourierStopClosesAdmissionBeforeJoin(t *testing.T) {
	f := newCourierFixture(t, "failed")
	entered, release := make(chan struct{}), make(chan struct{})
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		close(entered)
		<-release
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true}, errors.New("offline")
	}
	done := make(chan error, 1)
	go func() { done <- f.owner.Step(f.deadline()) }()
	awaitCourierSignal(t, entered)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err := f.owner.Stop(xfercontract.Deadline(f.now.Load() + int64(20*time.Millisecond))); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("stop returned before active request joined: %v", err)
	}
	if err := f.owner.Step(f.deadline()); !errors.Is(err, ErrRecovering) || f.transport.count() != 1 {
		t.Fatal("stop timeout reopened admission")
	}
	close(release)
	if err := awaitCourierResult(t, done); err == nil {
		t.Fatal("offline request returned false success")
	}
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		return xfercontract.Response{}, xfercontract.Attempt{}, errors.New("offline")
	}
	f.advance(retryMinimum)
	if err := f.owner.Step(f.deadline()); !errors.Is(err, ErrRecovering) || f.transport.count() != 1 {
		t.Fatal("stop timeout lost admission closure after callback joined")
	}
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Step(f.deadline()); err != nil || f.transport.count() != 1 {
		t.Fatal("stopped owner sent another terminal")
	}
	state, err := f.owner.Freeze(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	if !state.Stopped || state.Receipt != nil || state.Outcome != f.config.Outcome || state.Capsule != f.config.Capsule {
		t.Fatal("explicit stop erased pending delivery evidence")
	}
}

// origin: hot-update S7 plan §1.2 / §8.1 / T16
func TestTransferCourierRetryDeadlineAndBootGuards(t *testing.T) {
	f := newCourierFixture(t, "failed")
	if err := f.owner.Step(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline accepted: %v", err)
	}
	if err := f.owner.Step(xfercontract.Deadline(f.now.Load())); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("expired attempt accepted: %v", err)
	}
	if f.transport.count() != 0 {
		t.Fatal("invalid deadline reached transport")
	}
	short := xfercontract.Deadline(f.now.Load() + int64(50*time.Millisecond))
	if err := f.owner.Step(short); err == nil {
		t.Fatal("offline transport returned success")
	}
	if calls := f.transport.requests(); len(calls) != 1 || calls[0].deadline != short {
		t.Fatal("settlement attempt exceeded caller deadline")
	}
	c := f.owner.(*courier)
	f.advance(retryMinimum)
	originalClock := c.config.Clock
	c.config.Clock = func() (string, int64, error) { return "different-boot", f.now.Load(), nil }
	if err := c.Step(f.deadline()); err == nil || f.transport.count() != 1 {
		t.Fatal("retry crossed a boot without original authority")
	}
	c.config.Clock = originalClock
	if retryDelay(1) != retryMinimum || retryDelay(2) != 2*retryMinimum || retryDelay(3) != retryMaximum || retryDelay(100) != retryMaximum {
		t.Fatal("unbounded settlement backoff")
	}
}

// origin: hot-update S7 plan §8.3 / T16
func TestTransferCourierReservationReleaseRetries(t *testing.T) {
	f := newCourierFixture(t, "failed")
	f.capacity.refuseRelease.Store(true)
	if err := f.owner.Stop(f.deadline()); err == nil {
		t.Fatal("release failure hidden")
	}
	if f.capacity.outstanding() != 2 {
		t.Fatal("failed release forgot reservation identity")
	}
	f.capacity.refuseRelease.Store(false)
	if err := f.owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Stop(f.deadline()); err != nil || f.capacity.outstanding() != 0 {
		t.Fatal("reservation not retired exactly once")
	}
}

func awaitCourierSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("courier barrier not reached")
	}
}
func awaitCourierResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("courier callback not joined")
		return nil
	}
}

// A broken join must become an assertion, not strand the whole test inside the
// same blocked observer. The caller's deferred barrier release joins this
// invocation even when the assertion exits the exercise early.
func courierFreezeWithin(t *testing.T, owner Courier, deadline xfercontract.Deadline) (xfercontract.CourierState, error) {
	t.Helper()
	type result struct {
		state xfercontract.CourierState
		err   error
	}
	done := make(chan result, 1)
	go func() { state, err := owner.Freeze(deadline); done <- result{state, err} }()
	select {
	case value := <-done:
		return value.state, value.err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("freeze ignored its bounded join deadline")
		return xfercontract.CourierState{}, nil
	}
}

// origin: hot-update S7 plan §4 / §8.3 / T16
func TestTransferCourierRejectsUnboundedReceiptEvidence(t *testing.T) {
	f := newCourierFixture(t, "complete")
	bad := f.receipt
	bad.CanonicalDigest = string(make([]byte, 1<<20))
	if err := f.owner.Acknowledge(bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("unbounded reply accepted: %v", err)
	}
	c := f.owner.(*courier)
	c.mu.Lock()
	retained := c.conflict != nil
	c.mu.Unlock()
	if retained {
		t.Fatal("unbounded conflict evidence escaped capacity accounting")
	}
	if err := f.owner.Acknowledge(f.receipt); err != nil {
		t.Fatalf("malformed reply poisoned exact settlement: %v", err)
	}
}

// origin: hot-update S7 plan §10.4 / §12.2 P09 / T16
func TestTransferCourierObservationsKeepProducerOrder(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCourierObservationOrder(t)
	}
	testharness.AssertNoGoroutineLeak(t, "courier observation order", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "courier observation order", beforeFDs, 0)
}

func exerciseCourierObservationOrder(t *testing.T) {
	t.Helper()
	f := newCourierFixture(t, "complete")
	entered, release := make(chan struct{}), make(chan struct{})
	observer := f.config.Observe
	c := f.owner.(*courier)
	c.config.Observe = func(o xfercontract.Observation) {
		if o.Kind == xfercontract.ObserveCallbackEnter {
			close(entered)
			<-release
		}
		observer(o)
	}
	stepDone, ackDone := make(chan error, 1), make(chan error, 1)
	go func() { stepDone <- c.Step(f.deadline()) }()
	awaitCourierSignal(t, entered)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	go func() { ackDone <- c.Acknowledge(f.receipt) }()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.state.Receipt != nil
	}) {
		t.Fatal("concurrent ACK did not close admission")
	}
	select {
	case err := <-ackDone:
		t.Fatalf("ACK observation overtook the entered request: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := awaitCourierResult(t, ackDone); err != nil {
		t.Fatal(err)
	}
	if err := awaitCourierResult(t, stepDone); err == nil {
		t.Fatal("offline request unexpectedly succeeded")
	}
	rows := f.observations()
	if len(rows) < 4 {
		t.Fatal("courier observation window was empty")
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].EventSeq <= rows[i-1].EventSeq {
			t.Fatal("courier producer emitted out-of-order observations")
		}
	}
	if err := c.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
}

type courierCapacity struct {
	mu            sync.Mutex
	live          map[xfercontract.ReservationID]bool
	ordinal       uint64
	refuse        atomic.Bool
	refuseRelease atomic.Bool
}

func (c *courierCapacity) Reserve(r xfercontract.Reservation) (xfercontract.ReservationID, error) {
	if c.refuse.Load() {
		return "", errors.New("account full")
	}
	if r.NonReplayBytes <= 0 || r.SealedBytes <= 0 {
		return "", errors.New("unaccounted private state")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ordinal++
	id := xfercontract.ReservationID(fmt.Sprintf("courier-%d", c.ordinal))
	c.live[id] = true
	return id, nil
}
func (c *courierCapacity) Release(id xfercontract.ReservationID) error {
	if c.refuseRelease.Load() {
		return errors.New("release unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.live[id] {
		return errors.New("reservation released twice")
	}
	delete(c.live, id)
	return nil
}
func (c *courierCapacity) outstanding() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.live) }

// origin: hot-update S7 plan §6.4 / §8.3 / T16
func TestTransferCourierRejectsInvalidSettlementBinding(t *testing.T) {
	f := newCourierFixture(t, "failed")
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Capsule.Identity.EntryNonce = "other" },
		func(c *Config) { c.Capsule.ReceiverNkey = "" },
		func(c *Config) { c.Capsule.Nonce = "" },
		func(c *Config) { c.Capsule.Signature = "" },
		func(c *Config) { c.Capsule.Origin.Generation = 0 },
		func(c *Config) { c.Identity.SID = "bad.>" },
		func(c *Config) { c.Outcome.Outcome = "" },
		func(c *Config) { c.Outcome.Primitive = "unknown-primitive" },
		func(c *Config) { c.Outcome.Grant.Identity.TransferID = "other" },
		func(c *Config) { c.Clock = nil },
		func(c *Config) { c.Transport = nil },
	} {
		bad := f.config
		mutate(&bad)
		before := f.capacity.outstanding()
		if owner, err := New(bad); err == nil || owner != nil {
			t.Fatal("invalid settlement admitted")
		}
		if f.capacity.outstanding() != before {
			t.Fatal("invalid admission allocated capacity")
		}
	}
	f.capacity.refuse.Store(true)
	if owner, err := New(f.config); err == nil || owner != nil {
		t.Fatal("capacity refusal admitted courier")
	}
	f.capacity.refuse.Store(false)
}
