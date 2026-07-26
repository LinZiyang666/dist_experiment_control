package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Batch-A A3. These pin the two properties that were WRONG before the poll
// loops were collapsed into pollUntilStep:
//
//   - an interrupted wait must return promptly (the old waitForOp never read
//     cmd.Context(), so `cluster join approve --wait` could not be Ctrl-C'd at
//     all — signal.NotifyContext had swallowed the signal, and the loop it was
//     supposed to cancel was not reading the context);
//   - an interrupted or timed-out wait must exit 75, not 69/70. All three
//     values were in use for the same situation.

func exitClassOf(t *testing.T, err error) int {
	t.Helper()
	var ee *ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("error %v (%T) is not an *ExitError, so it falls through to exitInternal(70)", err, err)
	}
	return ee.Class
}

func TestPollUntilStepCancellation(t *testing.T) {
	tests := []struct {
		name string
		// how long after cancellation we still tolerate before calling it a hang
		grace time.Duration
		// interval is deliberately far longer than grace: a loop that only
		// notices cancellation on the next tick would fail this.
		interval time.Duration
	}{
		{name: "cancel is observed without waiting for the next tick", grace: 2 * time.Second, interval: 30 * time.Second},
		{name: "cancel is observed with a short interval too", grace: 2 * time.Second, interval: 10 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			calls := 0
			done := make(chan error, 1)
			go func() {
				done <- pollUntilStep(ctx, tc.interval, time.Time{}, func() (bool, error) {
					calls++
					return false, nil
				})
			}()
			// Let the first step run, then interrupt.
			time.Sleep(20 * time.Millisecond)
			cancel()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled wait returned nil; the caller would report success for work it stopped watching")
				}
				if got := exitClassOf(t, err); got != exitTransient {
					t.Errorf("cancelled wait exited %d, want %d (EX_TEMPFAIL): the operation is still running "+
						"on the broker and re-running the verb is the documented remedy", got, exitTransient)
				}
			case <-time.After(tc.grace):
				t.Fatalf("pollUntilStep did not return within %s of cancellation (interval=%s) — this is the "+
					"exact bug A3 fixes: the operator presses Ctrl-C and nothing happens", tc.grace, tc.interval)
			}
			if calls == 0 {
				t.Error("step was never called; a wait that is already satisfied must not sleep first")
			}
		})
	}
}

func TestPollUntilStepSemantics(t *testing.T) {
	sentinel := errors.New("step blew up")
	tests := []struct {
		name     string
		deadline time.Duration // 0 => no deadline
		step     func(calls *int) (bool, error)
		wantErr  error
		wantNil  bool
		minCalls int
	}{
		{
			name:     "already satisfied returns without sleeping",
			deadline: time.Hour,
			step:     func(c *int) (bool, error) { *c++; return true, nil },
			wantNil:  true,
			minCalls: 1,
		},
		{
			name:     "step error is propagated verbatim, not swallowed into a timeout",
			deadline: time.Hour,
			step:     func(c *int) (bool, error) { *c++; return false, sentinel },
			wantErr:  sentinel,
			minCalls: 1,
		},
		{
			name:     "expired deadline yields errPollDeadline for the caller to phrase",
			deadline: -time.Second, // already in the past
			step:     func(c *int) (bool, error) { *c++; return false, nil },
			wantErr:  errPollDeadline,
			minCalls: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var dl time.Time
			if tc.deadline != 0 {
				dl = time.Now().Add(tc.deadline)
			}
			err := pollUntilStep(context.Background(), time.Millisecond, dl, func() (bool, error) {
				return tc.step(&calls)
			})
			switch {
			case tc.wantNil && err != nil:
				t.Fatalf("want nil, got %v", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			if calls < tc.minCalls {
				t.Errorf("step called %d time(s), want at least %d — the deadline must be checked AFTER a "+
					"step, so an already-satisfied wait never reports a timeout", calls, tc.minCalls)
			}
		})
	}
}

// TestPollUntilDeadlineIsCheckedAfterStep guards a specific ordering bug: if the
// deadline were checked before calling step, a wait whose condition became true
// exactly at the deadline would report a spurious timeout — and for `cluster
// join approve --wait` that means telling the operator a completed join is
// "still in flight".
func TestPollUntilDeadlineIsCheckedAfterStep(t *testing.T) {
	err := pollUntilStep(context.Background(), time.Millisecond, time.Now().Add(-time.Hour), func() (bool, error) {
		return true, nil // satisfied, despite the deadline being long past
	})
	if err != nil {
		t.Fatalf("a wait satisfied on its first step must succeed even past the deadline, got %v", err)
	}
}

// TestPollUntilWrapperPreservesTimeoutClass pins the compatibility wrapper used
// by the upgrade orchestrator: its timeout stays 69 (that verb documents
// "unavailable"), but a CANCELLED wait now exits 75 like every other one
// instead of falling through to 70 on a bare ctx.Err().
func TestPollUntilWrapperPreservesTimeoutClass(t *testing.T) {
	err := pollUntil(context.Background(), -time.Second, func() bool { return false }, "converge timed out")
	if got := exitClassOf(t, err); got != exitUnavailable {
		t.Errorf("wrapper timeout exited %d, want %d (unchanged from before A3)", got, exitUnavailable)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = pollUntil(ctx, time.Hour, func() bool { return false }, "converge timed out")
	if got := exitClassOf(t, err); got != exitTransient {
		t.Errorf("cancelled wrapper wait exited %d, want %d — before A3 this returned a bare ctx.Err() "+
			"and fell through to exitInternal(70)", got, exitTransient)
	}
}
