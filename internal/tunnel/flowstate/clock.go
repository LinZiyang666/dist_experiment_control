package flowstate

import (
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
)

func ClientDeadlineBoot(t time.Time) (uint64, error) {
	if t.IsZero() {
		return 0, nil
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		return 0, err
	}
	return uint64(max(1, now+int64(time.Until(t)))), nil
}

// BOOTTIME is an absolute deadline across exec; an expired cut never receives
// a fresh grace interval merely because a new process parsed it.
// origin: hot-update S5 plan §4.4 / §5.3 / T13
func ClientBootDeadline(boot uint64) (time.Time, error) {
	if boot == 0 {
		return time.Time{}, nil
	}
	if boot > uint64(1<<63-1) {
		return time.Time{}, ErrFlowState
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(time.Duration(int64(boot) - now)), nil
}
