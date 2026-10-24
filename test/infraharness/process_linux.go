//go:build linux

package infraharness

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func supervisionSupported() error { return nil }

// ReadIdentity double-checks the incarnation around boot evidence. It does not
// treat a zombie as absent: disappearance is proof of Wait, not merely death.
func ReadIdentity(pid int) (Identity, error) {
	ticks, err := readStartTicks(pid)
	if err != nil {
		return Identity{}, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return Identity{}, err
	}
	again, err := readStartTicks(pid)
	if err != nil || ticks != again || len(strings.TrimSpace(string(boot))) == 0 {
		return Identity{}, errors.New("infra harness: process incarnation changed")
	}
	return Identity{BootID: strings.TrimSpace(string(boot)), PID: pid, StartTicks: ticks}, nil
}

func readStartTicks(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, errors.New("infra harness: invalid process identity")
	}
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	end := strings.LastIndexByte(string(body), ')')
	if end < 0 {
		return 0, errors.New("infra harness: malformed process stat")
	}
	fields := strings.Fields(string(body)[end+1:])
	if len(fields) < 20 {
		return 0, errors.New("infra harness: incomplete process stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

// ProcessExists checks an exact incarnation, counting unreaped zombies. Unknown
// procfs state is an error so an incomplete census cannot sign zero survivors.
func ProcessExists(id Identity) (bool, error) {
	if id.PID <= 0 || id.StartTicks == 0 || id.BootID == "" {
		return false, errors.New("infra harness: incomplete process identity")
	}
	current, err := ReadIdentity(id.PID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return current == id, nil
}

// origin: hot-update S9 plan §9.5 / T57
func killIdentity(id Identity) error {
	fd, err := unix.PidfdOpen(id.PID, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	exists, err := ProcessExists(id)
	if err != nil || !exists {
		return err
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}
