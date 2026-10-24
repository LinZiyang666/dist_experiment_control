package execsession

import (
	"errors"
	"runtime"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
)

// admitHandleUntil returns with mu held only on success. Unlike Cond.Wait,
// this wait never needs to reacquire a contended state lock after its cutoff.
// A planned cut still queues transparently; a failed or stalled recovery gets
// a distinct recovery-pending refusal plus its retained-ownership receipt, without changing
// authority, accounting, admission, or the recovery owner's lifetime.
// origin: hot-update S4 internal review rv-fd rv-fd-3 activation recovery stall
func admitHandleUntil(r *Runtime, op string, deadline time.Time) error {
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for {
		if !lockAdmissionBefore(r, deadline) {
			return errors.Join(procrequest.ErrBusy, ErrRollbackPending)
		}
		if r.closed {
			r.mu.Unlock()
			return ErrClosed
		}
		if !admissionBlocked(r, op) {
			return nil
		}
		if r.activation.changed == nil {
			r.activation.changed = make(chan struct{})
		}
		changed := r.activation.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			return errors.Join(procrequest.ErrBusy, ErrRollbackPending)
		}
	}
}

// admissionBlocked reports, under mu, whether the image fence still holds this
// op back: while Freeze waits for in-flight creations only create waits, and
// every op waits while the image is freezing, frozen, activating or only
// partially active.
func admissionBlocked(r *Runtime, op string) bool {
	return (r.quiescing && op == "create") || r.frozen || r.freezing || r.activating || r.partialActive
}

// Protocol admission is a hot path, unlike Freeze's safe-point polling.
// Yield briefly for an ordinary short critical section before backing off;
// charging every contention Freeze's 5 ms poll made a large request sequence
// outlive its capability. Long contention still consumes one bounded budget.
func lockAdmissionBefore(r *Runtime, deadline time.Time) bool {
	for attempts := 0; time.Now().Before(deadline); attempts++ {
		if r.mu.TryLock() {
			return true
		}
		if attempts < 8 {
			runtime.Gosched()
		} else {
			time.Sleep(min(time.Millisecond, time.Until(deadline)))
		}
	}
	return false
}

// broadcastAdmissionLocked publishes a state transition to bounded Handle
// waiters. Snapshot and registration share mu, so a thaw/close cannot race
// between checking the predicate and subscribing to this generation's wakeup.
func broadcastAdmissionLocked(r *Runtime) {
	r.cond.Broadcast()
	if r.activation.changed != nil {
		close(r.activation.changed)
		r.activation.changed = nil
	}
}
