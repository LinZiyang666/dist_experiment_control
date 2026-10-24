package natsinbox

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/nats-io/nats.go"
)

// inboxRemaining uses the existing platform clock rather than interpreting an
// absolute BOOTTIME value as wall time. A clock failure cannot buy another dial.
// origin: hot-update S6 plan §8.2 / T34
func inboxRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, errors.New("natsinbox: invalid admission deadline")
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return 0, err
	}
	if int64(deadline) <= now {
		return 0, nats.ErrTimeout
	}
	return time.Duration(int64(deadline) - now), nil
}

func beginInboxAttempt(observer controlbudget.Observer, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if observer == nil {
		return controlbudget.Attempt{}, nil
	}
	// The observer is bound to the logical operation by the admission owner;
	// these lower layers allocate no new request identity or ordinal space.
	return observer.Begin("", phase)
}

func finishInboxAttempt(observer controlbudget.Observer, attempt controlbudget.Attempt, sent bool, class string, operationErr error) error {
	if observer == nil {
		return operationErr
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return errors.Join(operationErr, err)
	}
	attempt.FinishedMonoNS = now
	attempt.Sent = sent
	attempt.Class = class
	// Observation overflow is an admission failure even when the connection
	// succeeded. Otherwise a successful retry could hide its failed predecessor.
	// origin: hot-update S6 plan §8.3 / T35
	return errors.Join(operationErr, observer.Record(attempt))
}

func inboxAttemptClass(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, nats.ErrTimeout) {
		return "timeout"
	}
	if errors.Is(err, nats.ErrAuthorization) {
		return "auth"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	// Never emit free-form transport errors: proxy URLs and auth material can
	// appear in them, while the observer is a public, secret-free receipt.
	return "error"
}

func connectInboxWithin(url string, opts []nats.Option, timeout time.Duration, deadline controlbudget.Deadline,
	observer controlbudget.Observer, phase controlbudget.AttemptPhase) (*nats.Conn, error) {
	// origin: hot-update S6 internal review rv-ctl-4 / rv-ctl-8
	// One NATS CONNECT owns each URL attempt, including INFO and auth replies.
	// Dividing what remains reserves time for untried endpoints in fixed order.
	urls := strings.Split(url, ",")
	var last error
	for index, address := range urls {
		remaining, err := inboxRemaining(deadline)
		if err != nil {
			return nil, errors.Join(last, err)
		}
		slice := remaining / time.Duration(len(urls)-index)
		if timeout > 0 {
			slice = min(slice, timeout)
		}
		inspect, err := inspectConnectOptions(opts)
		if err != nil {
			return nil, err
		}
		if dialer, ok := inspect.CustomDialer.(interface{ SetConnectDeadline(time.Time) error }); ok {
			if err = dialer.SetConnectDeadline(time.Now().Add(slice)); err != nil {
				return nil, err
			}
		}
		nc, err := connectInboxURLWithin(strings.TrimSpace(address), opts, slice, deadline, observer, phase)
		if err == nil {
			return nc, nil
		}
		last = err
	}
	return nil, last
}

// ConnectTransportWithin also serves anonymous development connections; they
// share the URL budget and observation owner without negotiating a private inbox.
func ConnectTransportWithin(url string, opts []nats.Option, deadline controlbudget.Deadline, observer controlbudget.Observer) (*nats.Conn, error) {
	inspect, err := inspectConnectOptions(opts)
	if err != nil {
		return nil, err
	}
	return connectInboxWithin(url, opts, inspect.Timeout, deadline, observer, controlbudget.PhaseConnect)
}

func connectInboxURLWithin(url string, opts []nats.Option, timeout time.Duration, deadline controlbudget.Deadline,
	observer controlbudget.Observer, phase controlbudget.AttemptPhase) (*nats.Conn, error) {
	attempt, err := beginInboxAttempt(observer, phase)
	if err != nil {
		return nil, err
	}
	remaining, err := inboxRemaining(deadline)
	var nc *nats.Conn
	if err == nil {
		if timeout > 0 {
			remaining = min(remaining, timeout)
		}
		bounded := append([]nats.Option(nil), opts...)
		bounded = append(bounded, nats.Timeout(remaining))
		nc, err = nats.Connect(url, bounded...)
		if err == nil {
			_, err = inboxRemaining(deadline)
		}
	}
	err = finishInboxAttempt(observer, attempt, false, inboxAttemptClass(err), err)
	if err != nil && nc != nil {
		nc.Close()
		nc = nil
	}
	return nc, err
}
