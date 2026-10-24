//go:build linux

package procrequest

import (
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func currentBootID() (string, error) {
	body, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(body))
	if !validToken(id) {
		return "", ErrIdentity
	}
	return id, nil
}

func bootNow() (int64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	return ts.Nano(), nil
}
