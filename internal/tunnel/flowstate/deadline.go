package flowstate

import (
	"time"
)

func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
