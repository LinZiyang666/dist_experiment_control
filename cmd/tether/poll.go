package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// errPollDeadline is returned by pollUntil when the caller's deadline passes.
// Callers translate it into their own message (each verb documents a different
// remedy) but must keep the exit class at exitTransient — see below.
var errPollDeadline = errors.New("poll deadline exceeded")

// pollUntil runs step every interval until it reports done, the deadline
// passes, or ctx is cancelled.
//
// Batch-A A3. This exists because the hand-written poll loops did not agree on
// two things that matter to an operator:
//
//   - Ctrl-C. cmd/tether/main.go installs a signal-aware context and its
//     comment states that "the signal-aware ctx is observed by every cobra
//     command". waitForOp was a bare `time.Sleep(2*time.Second)` loop that never
//     read cmd.Context(), so `cluster join approve --wait` (5 min default) and
//     `cluster retire --wait` (10 min) could not be interrupted at all: because
//     signal.NotifyContext takes over SIGINT, the first Ctrl-C only cancels a
//     context nobody was reading. The operator had to SIGKILL. Installing
//     signal handling had made these commands HARDER to stop than before.
//
//   - The timeout exit class. waitForOp exited 75 (EX_TEMPFAIL, "re-run me"),
//     while `cluster reconcile nats --wait` returned a bare fmt.Errorf and so
//     fell through to 70 — which docs/usage.md §9.13 tells automation is
//     retryable-but-a-tether-bug. Same situation, two answers.
//
// A cancelled or timed-out wait is EX_TEMPFAIL in both cases: the underlying
// operation is still running on the broker, and re-running the verb is the
// documented way to keep watching it. Neither is a failure of the operation.
//
// A third answer existed too: the pre-A3 pollUntil (cluster_upgrade_drive.go)
// returned unavailErr (69) on timeout and a bare ctx.Err() on cancel, which
// fell through to 70. So the same "I stopped waiting" outcome produced 69, 70
// or 75 depending on which loop the operator happened to be in. That helper is
// now the thin wrapper below, delegating here, so the class is decided once.
//
// step is called immediately, before the first tick — a wait that is already
// satisfied must not sleep for an interval first.
func pollUntilStep(ctx context.Context, interval time.Duration, deadline time.Time, step func() (done bool, err error)) error {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		done, err := step()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return errPollDeadline
		}
		select {
		case <-ctx.Done():
			return &ExitError{
				Class: exitTransient,
				Err:   fmt.Errorf("interrupted while waiting; the operation is still running on the broker — re-run to keep watching: %w", ctx.Err()),
			}
		case <-t.C:
		}
	}
}

// pollUntil keeps the pre-A3 predicate-shaped call sites (the upgrade
// orchestrator's three convergence waits) working while routing them through
// the single implementation above. Behaviour is unchanged except that a
// cancelled wait now exits 75 like every other interrupted wait, instead of
// falling through to 70 on a bare ctx.Err().
func pollUntil(ctx context.Context, timeout time.Duration, pred func() bool, timeoutMsg string) error {
	err := pollUntilStep(ctx, upgradeConvergePoll, time.Now().Add(timeout), func() (bool, error) {
		return pred(), nil
	})
	if errors.Is(err, errPollDeadline) {
		return unavailErr("%s", timeoutMsg)
	}
	return err
}
