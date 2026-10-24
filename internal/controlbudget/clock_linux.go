//go:build linux

package controlbudget

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// ReceiptFile adopts a dedicated inherited receipt descriptor. A pipe supplied
// by the shell is initially blocking; set O_NONBLOCK before os.NewFile so the
// Go poller can enforce the sink's write deadline and join a stalled collector.
// origin: hot-update S6 plan §8.3 / T35 / T57
func ReceiptFile(fd uintptr) (*os.File, error) {
	if fd <= 2 {
		return nil, errors.New("controlbudget: invalid receipt descriptor")
	}
	flags, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if err != nil {
		return nil, err
	}
	if _, err = unix.FcntlInt(fd, unix.F_SETFL, flags|unix.O_NONBLOCK); err != nil {
		return nil, err
	}
	return os.NewFile(fd, "control-attempt-receipt"), nil
}

func clockNow() (int64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	return ts.Nano(), nil
}

// ReceiptCloseOnExec is kept at the OS boundary alongside the admission clock.
// The dedicated receipt FD must not reach an exec'd business child. Control
// pins the descriptor while fcntl runs and propagates flag failures.
// origin: hot-update S6 plan §8.3 / T35
func ReceiptCloseOnExec(file *os.File) error {
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var flagErr error
	if err = raw.Control(func(fd uintptr) {
		flags, e := unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e != nil {
			flagErr = e
			return
		}
		_, flagErr = unix.FcntlInt(fd, unix.F_SETFD, flags|unix.FD_CLOEXEC)
	}); err != nil {
		return err
	}
	return flagErr
}
