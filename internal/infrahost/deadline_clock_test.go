package infrahost

import (
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

// The clock and its timers advance together. Freezing only Now would still let
// the real cancellation timer expire while fsync admits the intended fixture.
// Each fixture owns its clock; no package global or concurrent worker changes.
// origin: S10 internal review ARCH-REDS R12
type manualHostClock struct {
	mu     sync.Mutex
	at     int64
	timers []*manualHostTimer
}

type manualHostTimer struct {
	at      int64
	channel chan time.Time
	stopped bool
}

func newJournalClock(t *testing.T, j *Journal) *manualHostClock {
	t.Helper()
	at, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	clock := &manualHostClock{at: at}
	j.clock = clock
	return clock
}

func (c *manualHostClock) Now() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at, nil
}

func (c *manualHostClock) Timer(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &manualHostTimer{at: c.at + int64(d), channel: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	if d <= 0 {
		timer.channel <- time.Time{}
		timer.stopped = true
	}
	return timer.channel, func() {
		c.mu.Lock()
		timer.stopped = true
		c.mu.Unlock()
	}
}

func (c *manualHostClock) deadline(d time.Duration) infracontract.Deadline {
	c.mu.Lock()
	defer c.mu.Unlock()
	return infracontract.Deadline(c.at + int64(d))
}

func (c *manualHostClock) After(d time.Duration) (infracontract.Deadline, error) {
	if d <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	return c.deadline(d), nil
}

func (c *manualHostClock) Remaining(d infracontract.Deadline) (time.Duration, error) {
	if d <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at >= int64(d) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(d) - c.at), nil
}

func (c *manualHostClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at += int64(d)
	for _, timer := range c.timers {
		if !timer.stopped && timer.at <= c.at {
			timer.channel <- time.Time{}
			timer.stopped = true
		}
	}
}
