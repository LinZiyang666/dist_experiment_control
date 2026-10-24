package cli

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// ControlResultReserve leaves time for joined cancellation, the terminal result
// and observer close inside the caller's five-second operation budget.
// origin: hot-update S6 plan §8.2 / T30 / EXITS-2-3
const ControlResultReserve = 100 * time.Millisecond

var ErrControlResultUnknown = errors.New("agent_no_responders: control_result_unknown")

// This is an engine control value, not evidence that a transfer failed. The
// invocation must query its original owner before replaying the same CallID.
// origin: hot-update S7 plan §7 / T32
var ErrTransferReconcileRequired = errors.New(xfercontract.CodeReconcileRequired)

// AttemptError supplies facts the callback alone can know. Retryable cannot
// override explicit authority failures; Sent prevents unsafe mutation replay.
type AttemptError struct {
	Err       error
	Sent      bool
	Retryable bool
}

func (e *AttemptError) Error() string { return e.Err.Error() }
func (e *AttemptError) Unwrap() error { return e.Err }

// AdmissionDeadline is called at RunE entry. It never renews a deadline already
// passed through the operation; callers pass the result to every helper.
// origin: hot-update S6 plan §8.2 / T30
func AdmissionDeadline(ctx context.Context) (controlbudget.Deadline, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	budget := upgradebudget.RequestLatency
	if until, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(until))
	}
	return controlbudget.After(budget)
}

// AdmissionContext binds cancellation to a phase without making the long task
// inherit its admission lifetime. The caller cancels and joins phase work before
// switching to its original user context at true_started.
func AdmissionContext(ctx context.Context, deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	phaseCtx, cancel := context.WithTimeout(ctx, remaining)
	return phaseCtx, cancel, nil
}

// DoWithin preserves the frozen P00 signature. Each callback executes
// synchronously and must bound its actual I/O by the supplied remaining budget.
// Cancellation-aware RunE callers use DoPhaseWithin with their original ctx.
func DoWithin(cfg RetryConfig, attempt func(time.Duration) error) error {
	// ctx-none: the frozen DoWithin callback contract has no caller context.
	return DoPhaseWithin(context.Background(), cfg, controlbudget.PhaseRequest, attempt)
}

// DoPhaseWithin never spawns a timeout goroutine around blocking work: returning
// means the callback has returned, so cancellation cannot leave an old attempt
// performing late side effects after a retry or CLI exit.
// origin: hot-update S6 plan §8.2 / T35 / T57
func DoPhaseWithin(ctx context.Context, cfg RetryConfig, phase controlbudget.AttemptPhase, attempt func(time.Duration) error) error {
	if !validRetryFamily(cfg.Family) || !controlbudget.ValidPhase(phase) || !validLogicalID(cfg.LogicalID) || attempt == nil {
		return errors.New("cli: invalid admission policy")
	}
	if cfg.Family == RetryTransfer {
		// Byte and settlement lifetimes cannot enlarge a logical control
		// operation, even when a caller uses the shared helper directly.
		// origin: hot-update S7 plan §1.2 / §7 / T32
		bounded, err := transferControlDeadline(cfg.Deadline)
		if err != nil {
			return err
		}
		cfg.Deadline = bounded
	}
	backoff := 250 * time.Millisecond
	var last error
	for ordinal := 0; ordinal < AttemptLimit; ordinal++ {
		_, err := controlbudget.Remaining(cfg.Deadline)
		if err != nil || ctx.Err() != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return finishRetry(cfg, lastOr(last, err), controlbudget.PhaseCancel)
		}
		a, err := beginAttempt(cfg.Observer, cfg.LogicalID, phase)
		if err != nil {
			return err
		}
		// Observer allocation can block behind other attempts. Re-read the
		// same deadline before I/O rather than handing out a stale duration.
		// origin: hot-update S6 plan §8.2 / §8.3 / T30 / T35
		remaining, err := controlbudget.Remaining(cfg.Deadline)
		if err != nil || ctx.Err() != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			recordErr := completeAttempt(cfg.Observer, a, false, errorClass(err), 0)
			return finishRetry(cfg, errors.Join(lastOr(last, err), ctx.Err(), recordErr), controlbudget.PhaseCancel)
		}
		last = attempt(remaining)
		sent, retry, class := retryDisposition(cfg, phase, last)
		if last == nil {
			// A late response is evidence, not a successful <=5s admission.
			_, last = controlbudget.Remaining(cfg.Deadline)
			if last == nil {
				last = ctx.Err()
			}
			if last != nil {
				class, retry = "timeout", false
			}
		}
		if err = completeAttempt(cfg.Observer, a, sent, class, 0); err != nil {
			if cfg.Family == RetryTransfer && sent {
				return errors.Join(ErrTransferReconcileRequired, last, err)
			}
			return errors.Join(last, err)
		}
		// Cancellation, authority errors and malformed replies can also follow
		// a publish. Inspect Sent before the generic terminal/retry predicates:
		// none of those errors proves the original mutation did not execute.
		// origin: hot-update S7 plan §7 / T32
		if cfg.Family == RetryTransfer && sent && last != nil {
			return finishRetry(cfg, errors.Join(ErrTransferReconcileRequired, last), controlbudget.PhaseResult)
		}
		if last == nil || !retry {
			return finishRetry(cfg, last, controlbudget.PhaseResult)
		}
		// origin: hot-update S6 plan §8.2 / T36
		if cfg.Family == RetryUnsafe && sent {
			return finishRetry(cfg, errors.Join(ErrControlResultUnknown, last), controlbudget.PhaseResult)
		}
		remaining, err = controlbudget.Remaining(cfg.Deadline)
		if err != nil {
			return finishRetry(cfg, last, controlbudget.PhaseResult)
		}
		wait := min(backoff, remaining)
		a, err = beginAttempt(cfg.Observer, cfg.LogicalID, controlbudget.PhaseBackoff)
		if err != nil {
			return errors.Join(last, err)
		}
		remaining, err = controlbudget.Remaining(cfg.Deadline)
		if err != nil || ctx.Err() != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			recordErr := completeAttempt(cfg.Observer, a, false, errorClass(err), 0)
			return finishRetry(cfg, errors.Join(lastOr(last, err), ctx.Err(), recordErr), controlbudget.PhaseCancel)
		}
		wait = min(wait, remaining)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			err = ctx.Err()
		case <-timer.C:
		}
		if recordErr := completeAttempt(cfg.Observer, a, false, errorClass(err), wait); recordErr != nil {
			return errors.Join(last, recordErr)
		}
		if err != nil {
			return finishRetry(cfg, err, controlbudget.PhaseCancel)
		}
		backoff = min(backoff*2, time.Second)
	}
	return errors.Join(last, ErrAttemptOverflow)
}

func validRetryFamily(family RetryFamily) bool {
	switch family {
	case RetryReadOnly, RetryStableProcess, RetryUnsafe, RetryTransfer:
		return true
	default:
		return false
	}
}

func retryDisposition(cfg RetryConfig, phase controlbudget.AttemptPhase, err error) (bool, bool, string) {
	if err == nil {
		// CONNECT success is not a transfer mutation publish. A late CONNECT
		// must retain an unsent outcome instead of inventing reconciliation work.
		// origin: hot-update S7 plan §7 / T32
		return cfg.Family != RetryTransfer || phase != controlbudget.PhaseConnect, false, "ok"
	}
	var detail *AttemptError
	known := errors.As(err, &detail)
	sent := !errors.Is(err, nats.ErrNoServers) && !errors.Is(err, nats.ErrConnectionClosed)
	if known {
		sent = detail.Sent
	}
	if phase == controlbudget.PhaseConnect {
		sent = false
	}
	class := errorClass(err)
	if class == "auth" {
		return sent, phase == controlbudget.PhaseConnect && cfg.HasAuthReceipt, class
	}
	if class == "denied" || class == "canceled" || class == "terminal" {
		return sent, false, class
	}
	retry := class == "timeout" || class == "no_responders" || class == "connection"
	if known {
		retry = retry || detail.Retryable
	}
	return sent, retry, class
}

// errorClass intentionally never includes an error's text (which can include a
// URL, token, certificate or payload). Unknown errors are terminal by default.
func errorClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrTransferReconcileRequired):
		return "result_unknown"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAuthExpired), errors.Is(err, nats.ErrPermissionViolation):
		return "denied"
	case errors.Is(err, nats.ErrAuthorization):
		return "auth"
	case controlbudget.IsExpired(err), errors.Is(err, nats.ErrTimeout):
		return "timeout"
	case errors.Is(err, nats.ErrNoResponders):
		return "no_responders"
	case errors.Is(err, nats.ErrNoServers), errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionReconnecting):
		return "connection"
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "timeout"
		}
		return "terminal"
	}
}

func lastOr(last, fallback error) error {
	if last != nil {
		return last // exhaustion must preserve the original auth refusal
	}
	return fallback
}

func finishRetry(cfg RetryConfig, result error, phase controlbudget.AttemptPhase) error {
	a, err := beginAttempt(cfg.Observer, cfg.LogicalID, phase)
	if err != nil {
		return errors.Join(result, err)
	}
	return errors.Join(result, completeAttempt(cfg.Observer, a, false, errorClass(result), 0))
}
