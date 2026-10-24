package xferworker

import (
	"sync/atomic"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// backoffCourier models a courier inside its settlement backoff: Step sends
// nothing and returns nil, and Freeze reports no receipt yet.
type backoffCourier struct {
	id             x.Identity
	steps, freezes atomic.Int32
}

func (c *backoffCourier) Step(x.Deadline) error { c.steps.Add(1); return nil }
func (c *backoffCourier) Freeze(x.Deadline) (x.CourierState, error) {
	c.freezes.Add(1)
	return x.CourierState{Identity: c.id}, nil
}
func (*backoffCourier) Stop(x.Deadline) error               { return nil }
func (*backoffCourier) Acknowledge(x.TerminalReceipt) error { return nil }

// A same-store JetStream restart fails the first settlement attempt. The
// runner then spun on the backing-off courier about once per millisecond,
// freezing it each time and flooding its observer until the backoff elapsed.
// A push receiver retained beside its courier re-ensured that courier and
// erased the wait a second way.
// origin: S7 FIX-CUT courier backoff spin / hot-update S7 plan §1.2 / §4 / T16 / T46
func TestTransferWorkerWaitsOutCourierBackoff(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "courier-only"
		if retained {
			name = "push-receiver-retained"
		}
		t.Run(name, func(t *testing.T) { exerciseCourierBackoff(t, retained) })
	}
}

func exerciseCourierBackoff(t *testing.T, retained bool) {
	o, p := reviewWorker(t)
	e := o.entries[key(p.Identity)]
	outcome := x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}
	if retained {
		outcome = x.ReceiverOutcome{Outcome: x.OutcomeLanded, Bytes: p.Intent.Size, SHA256: p.Intent.SHA256, ParentSynced: true}
		o.mu.Lock()
		e.receiver = &receiverProbe{state: x.ReceiverState{Identity: p.Identity, Outcome: outcome, GateClosed: true}}
		o.mu.Unlock()
	}
	courier := &backoffCourier{id: p.Identity}
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) { return courier, nil }
	if err := o.ensureCourier(e, outcome); err != nil {
		t.Fatal(err)
	}
	// One runner wait is 500ms; this window admits the first step and at
	// most one wake-up, never a busy loop.
	window := time.NewTimer(700 * time.Millisecond) // sleep-fixture: fixed observation window for a retry cadence.
	<-window.C
	steps, freezes := courier.steps.Load(), courier.freezes.Load()
	if steps < 1 || steps > 3 || freezes > 3 {
		t.Fatalf("worker runner spun on a backing-off courier: steps=%d freezes=%d in 700ms", steps, freezes)
	}
}
