// Package budgetclock binds host admission and cancellation to one clock.
// It does not own a journal, backend effect or business resource.
// origin: S10 internal review ARCH-REDS R12
package budgetclock

import (
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
)

// Each journal and its workers share one clock. Tests can hold admission time
// while durable IO enters the intended effect window, then expire the original
// deadline without racing filesystem latency. Production always uses BOOTTIME.
// origin: S10 internal review ARCH-REDS R12
type Clock interface {
	Now() (int64, error)
	After(time.Duration) (controlbudget.Deadline, error)
	Remaining(controlbudget.Deadline) (time.Duration, error)
	Timer(time.Duration) (<-chan time.Time, func())
}

type System struct{}

func (System) Now() (int64, error) { return controlbudget.Now() }
func (System) Timer(d time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(d)
	return timer.C, func() { timer.Stop() }
}

func (System) After(budget time.Duration) (controlbudget.Deadline, error) {
	return controlbudget.After(budget)
}
func (System) Remaining(d controlbudget.Deadline) (time.Duration, error) {
	return controlbudget.Remaining(d)
}

// A single owned relay converts deadline/worker/caller cancellation into the
// frozen Stop type. It is joined even when the backend fails synchronously.
func RelayStop(clock Clock, d controlbudget.Deadline, caller, worker <-chan struct{}) (<-chan struct{}, func()) {
	stop, done, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	remaining, err := clock.Remaining(d)
	if err != nil {
		remaining = 0
	}
	expired, cancelTimer := clock.Timer(remaining)
	go func() {
		defer close(joined)
		defer cancelTimer()
		select {
		case <-done:
		case <-expired:
		case <-caller:
		case <-worker:
		}
		close(stop)
	}()
	return stop, func() { close(done); <-joined }
}
