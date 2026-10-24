package controlbudget

import (
	"context"
	"errors"
	"math"
	"time"
)

var (
	ErrInvalidDeadline = errors.New("controlbudget: invalid absolute deadline")
	ErrExpired         = errors.New("controlbudget: admission deadline exceeded")
)

// Now reads the local admission clock. Linux uses CLOCK_BOOTTIME so suspend
// consumes the budget too; timestamps must never be compared across hosts.
// origin: hot-update S6 plan §2.1 / §8.2
func Now() (int64, error) { return clockNow() }

// After constructs a deadline once, at operation entry, rather than per phase.
func After(budget time.Duration) (Deadline, error) {
	now, err := Now()
	if err != nil {
		return 0, err
	}
	if budget <= 0 || now <= 0 || int64(budget) > math.MaxInt64-now {
		return 0, ErrInvalidDeadline
	}
	return Deadline(now + int64(budget)), nil
}

// Remaining rejects zero and expiration before any network/store side effect.
func Remaining(deadline Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, ErrInvalidDeadline
	}
	now, err := Now()
	if err != nil {
		return 0, err
	}
	if now >= int64(deadline) {
		return 0, ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

// IsExpired keeps callers from converting an admission failure into authority.
func IsExpired(err error) bool {
	return errors.Is(err, ErrExpired) || errors.Is(err, context.DeadlineExceeded)
}

// ValidPhase is exhaustive: an unknown producer phase cannot vanish in a
// collector's default branch and create a false zero-blackout receipt.
// origin: hot-update S6 plan §8.3 / T35
func ValidPhase(phase AttemptPhase) bool {
	switch phase {
	case PhaseDNS, PhaseProxy, PhaseTLS, PhaseWS, PhaseConnect, PhaseInboxProbe,
		PhaseInboxFallback, PhaseDiscovery, PhaseBanner, PhaseProbe, PhaseCreate,
		PhaseRequest, PhaseBackoff, PhaseCancel, PhaseResult, PhaseFreeze,
		PhaseExec, PhaseImport, PhaseAuth, PhaseReady, PhaseMaintenance:
		return true
	default:
		return false
	}
}

// Bind lets the frozen deep-connect signatures share the RunE's logical ID.
// Producers lacking an ID parameter call Begin("", phase); explicit IDs must
// still match, so a retry cannot silently become a second logical operation.
func Bind(observer Observer, logicalID string) Observer {
	if observer == nil {
		return nil
	}
	return boundObserver{Observer: observer, id: logicalID}
}

type boundObserver struct {
	Observer
	id string
}

func (o boundObserver) Begin(id string, phase AttemptPhase) (Attempt, error) {
	if id != "" && id != o.id {
		return Attempt{}, errors.New("controlbudget: logical identity changed")
	}
	return o.Observer.Begin(o.id, phase)
}

// WithoutPhases delegates these scopes to an enclosing owner. The child still
// records every other phase on the same ordinal stream; excluded scopes reserve
// no ordinal because the enclosing owner records that very attempt once.
// origin: hot-update S6 internal review rv-ctl-2 / rv-ctl-8
func WithoutPhases(observer Observer, phases ...AttemptPhase) Observer {
	if observer == nil {
		return nil
	}
	return delegatedObserver{Observer: observer, phases: append([]AttemptPhase(nil), phases...)}
}

type delegatedObserver struct {
	Observer
	phases []AttemptPhase
}

func (o delegatedObserver) Begin(id string, phase AttemptPhase) (Attempt, error) {
	for _, owned := range o.phases {
		if phase == owned {
			return Attempt{}, nil
		}
	}
	return o.Observer.Begin(id, phase)
}

func (o delegatedObserver) Record(a Attempt) error {
	if a.Ordinal == 0 && a.Phase == "" {
		return nil // Begin deliberately delegated this scope, including its completion.
	}
	return o.Observer.Record(a)
}
