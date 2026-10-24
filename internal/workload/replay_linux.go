//go:build linux

package workload

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const replaySeals = unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

func newReplayFile(capacity int64) (*os.File, error) {
	fd, err := unix.MemfdCreate("tether-replay", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("workload: create replay file: %w", err)
	}
	if err := unix.Ftruncate(fd, capacity); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, replaySeals); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), "tether-replay"), nil
}

func validateReplayFile(file *os.File, capacity int64) error {
	if file == nil {
		return fmt.Errorf("workload: missing replay file")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var checkErr error
	err = raw.Control(func(fd uintptr) {
		var info unix.Stat_t
		if err := unix.Fstat(int(fd), &info); err != nil {
			checkErr = err
			return
		}
		seals, e1 := unix.FcntlInt(fd, unix.F_GET_SEALS, 0)
		flags, e2 := unix.FcntlInt(fd, unix.F_GETFL, 0)
		fdflags, e3 := unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e1 != nil || e2 != nil || e3 != nil || info.Mode&unix.S_IFMT != unix.S_IFREG ||
			info.Size != capacity || seals&replaySeals != replaySeals ||
			seals&(unix.F_SEAL_WRITE|unix.F_SEAL_FUTURE_WRITE) != 0 || flags&unix.O_ACCMODE != unix.O_RDWR ||
			flags&unix.O_APPEND != 0 || fdflags&unix.FD_CLOEXEC == 0 {
			checkErr = fmt.Errorf("workload: incompatible replay descriptor")
		}
	})
	if err != nil {
		return err
	}
	return checkErr
}
