package xferauth

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Stop must cancel the SQL/SDK child and join its watcher. Settlement also
// inherits the original deadline, and a late Stop cannot authorize a cascade.
// origin: S7 internal review INTEG-M F4
func TestSessionControlLifetimeNoLeaks(t *testing.T) {
	// One P makes a cancellation without join observable before its watcher
	// gets another turn. Restore the process setting before leaving this test.
	parallelism := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(parallelism)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSessionControlLifetime(t)
	}
	testharness.AssertNoGoroutineLeak(t, "session control adapter", g)
	testharness.AssertNoFDLeak(t, "session control adapter", fd, 0)
}

type sessionDeadlineBarrier func(controlbudget.Deadline) error

func (f sessionDeadlineBarrier) Settle(_ string, _ bool, d controlbudget.Deadline) error {
	return f(d)
}

func exerciseSessionControlLifetime(t *testing.T) {
	t.Helper()
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	_, join, err := sessionContext(d, nil)
	if err != nil {
		t.Fatal(err)
	}
	join()
	if runtime.NumGoroutine() > before {
		t.Fatal("adapter release returned before its watcher joined")
	}
	stop := make(chan struct{})
	ctx, release, err := sessionContext(d, stop)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	until, ok := ctx.Deadline()
	remaining, err := controlbudget.Remaining(d)
	if err != nil || !ok || time.Until(until) > remaining+10*time.Millisecond {
		t.Fatal("SQL/SDK adapter renewed the original admission deadline")
	}
	close(stop)
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("Stop did not cancel the SQL/SDK context")
		}
	case <-time.After(time.Second):
		t.Fatal("SQL/SDK adapter ignored Stop")
	}
	release()
	if _, _, err := sessionContext(d, stop); !errors.Is(err, context.Canceled) {
		t.Fatal("closed Stop started a new adapter")
	}
	settleStop := make(chan struct{})
	barrier := sessionDeadlineBarrier(func(got controlbudget.Deadline) error {
		if got != d {
			t.Fatal("settlement renewed the original deadline")
		}
		close(settleStop)
		return nil
	})
	if err := SettleSession(d, settleStop, barrier, true, "session", false); !errors.Is(err, context.Canceled) {
		t.Fatal("late Stop released the settlement barrier")
	}
	dropped := false
	if err := CascadeSession(d, settleStop, nil, "session", func(string) error { dropped = true; return nil }); !errors.Is(err, context.Canceled) || dropped {
		t.Fatal("canceled cascade deleted session rows")
	}
}
