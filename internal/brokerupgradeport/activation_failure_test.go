package brokerupgradeport

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type unqueuedArmFailure struct{ error }

func (unqueuedArmFailure) ActivationUnstarted() bool { return true }
func (e unqueuedArmFailure) Unwrap() error           { return e.error }

// origin: S8 internal review roll-recovery-stall F5
func TestBrokerUnqueuedArmRetiresReservationWithoutReturned(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerUnqueuedArm(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker unqueued arm reservation", beforeG)
	testharness.AssertNoFDLeak(t, "broker unqueued arm reservation", beforeFD, 0)
}

func exerciseBrokerUnqueuedArm(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	cut := f.prepareCut(t)
	failure := errors.New("maintenance admission refused before queue")
	f.arm = func(controlbudget.Deadline) error { return unqueuedArmFailure{failure} }
	_, err := f.runtime.Activate(cut)
	if !errors.Is(err, failure) || activationDispatched(err) || f.runtime.armed.Load() != nil || f.runtime.held != nil || !f.leases[0].closed {
		t.Fatalf("unqueued refusal retained activation custody: err=%v armed=%t held=%t closed=%t", err, f.runtime.armed.Load() != nil, f.runtime.held != nil, f.leases[0].closed)
	}
	if _, dispatched := Returned(f.runtime); dispatched {
		t.Fatal("unqueued refusal invented a Run return")
	}
}

// origin: S8 internal review roll-recovery-stall F2
func TestBrokerActivationRetainsArmFailure(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerArmFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker arm refusal", beforeG)
	testharness.AssertNoFDLeak(t, "broker arm refusal", beforeFD, 0)
}

// origin: S8 internal review roll-recovery-stall F4
func TestBrokerArmingRefusalCannotHideRecoveryAsTransient(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerArmingRecoveryObservation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker armed recovery observation", beforeG)
	testharness.AssertNoFDLeak(t, "broker armed recovery observation", beforeFD, 0)
}

func exerciseBrokerArmingRecoveryObservation(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	cut := f.prepareCut(t)
	f.arm = func(controlbudget.Deadline) error {
		return errors.New("maintenance admission refused before Run queued")
	}
	_, err := f.runtime.Activate(cut)
	if err == nil || activationDispatched(err) {
		t.Fatalf("missing accepted arming refusal: %v", err)
	}
	defer Returned(f.runtime)
	stamp := upgradetx.Stamp{Attempt: cut.Attempt, Nonce: nonce(), ScopeDigest: f.grant.ScopeDigest}
	// Until the source journals its failure, an armed cut is the ordinary
	// transient exec window. The durable unknown must end that classification.
	if _, err = f.runtime.Observe(stamp); err == nil {
		t.Fatal("an unresolved armed cut reported an ordinary census")
	}
	f.journal.mu.Lock()
	r := &f.journal.view.Historical
	r.Forward.Phase, r.Forward.CutStage, r.Forward.Outcome = upgradetx.PhaseRecoveryRequired, upgradetx.CutPartialActive, upgradetx.OutcomeUnknown
	r.Revision++
	r.Forward.Handle.Revision = r.Revision
	stamp.Attempt = r.Forward.Handle
	f.journal.mu.Unlock()
	_, err = f.runtime.Observe(stamp)
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || typed.Code != upgradetx.CodeUnsettled || !typed.Accepted {
		t.Fatalf("durable recovery was hidden as a retryable cut window: %v", err)
	}
}

func exerciseBrokerArmFailure(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	cut := f.prepareCut(t)
	failure := errors.New("maintenance expired before store cut")
	f.arm = func(controlbudget.Deadline) error { return failure }
	_, err := f.runtime.Activate(cut)
	// The Run loop still owns an uncertain queued cut until it returns. This
	// assertion checks diagnosis, without treating an arming error as proof that
	// the store was never closed or releasing custody before that readback.
	_, dispatched := Returned(f.runtime)
	if !errors.Is(err, failure) || activationDispatched(err) || dispatched || !f.leases[0].closed {
		t.Fatalf("lost original arm failure or custody: error=%v dispatched=%t closed=%t", err, dispatched, f.leases[0].closed)
	}
}
