//go:build !linux

package controlbudget

import (
	"errors"
	"os"
	"time"
)

func ReceiptFile(fd uintptr) (*os.File, error) {
	if fd <= 2 {
		return nil, errors.New("controlbudget: invalid receipt descriptor")
	}
	// origin: hot-update S6 plan §9.3; receipt FD flags require the Linux leaf.
	// Refuse an explicitly requested sink rather than accept a blocking descriptor
	// that could hold ordinary control admission past its absolute deadline.
	return nil, errors.New("controlbudget: receipt descriptors require Linux")
}

// Non-Linux control requests still have a local elapsed budget. This clock is
// process-local, cannot import Linux image deadlines, and makes no hot-image
// support claim. The monotonic component prevents wall-clock adjustments from
// granting a new admission window.
var clockOrigin = time.Now()

func clockNow() (int64, error) { return int64(time.Since(clockOrigin)) + 1, nil }

func ReceiptCloseOnExec(_ *os.File) error {
	return errors.New("controlbudget: receipt descriptors require Linux")
}
