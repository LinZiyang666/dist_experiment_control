package flowstate

import (
	"time"
)

func FlowLockBefore(mu interface {
	TryLock() bool
	Unlock()
}, deadline time.Time) bool {
	for !mu.TryLock() {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return false
		}
		timer := time.NewTimer(time.Millisecond)
		<-timer.C
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		mu.Unlock()
		return false
	}
	return true
}

func BoolNumber(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}
