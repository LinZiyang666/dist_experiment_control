//go:build linux

package workload

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// checkControlIdentity checks one retained child, never a /proc census. Holding
// Manager's guard pins its unreaped PID throughout the subsequent group signal.
// Old callers may omit boot/ticks; ownership still requires our original child
// and its original group. Supplied identity fields must agree with the kernel.
// origin: hot-update S4 plan §3.4 / T15
func checkControlIdentity(id Identity) (Identity, error) {
	_, boot, err := currentProcessOwner()
	if err != nil {
		return Identity{}, err
	}
	actual, err := readProcessIdentity(id.OSPID)
	if err != nil {
		return Identity{}, err
	}
	if id.OSPID <= 0 || actual.parent != os.Getpid() || actual.group != id.OSPID ||
		(id.BootID != "" && id.BootID != boot) ||
		(id.StartTimeTicks != 0 && id.StartTimeTicks != actual.startTicks) {
		return Identity{}, fmt.Errorf("workload: control child identity changed")
	}
	id.BootID, id.StartTimeTicks = boot, actual.startTicks
	return id, nil
}

// ScanSession returns a bounded census of live members of the original child's
// session, across all process groups. The caller retains the leader's reap hold
// to pin the SID, performs census outside Manager/runtime locks, and requires
// two stable empty censuses before releasing its cleanup obligation.
//
// A failed or oversized census is never an empty-success receipt. A process
// disappearing while /proc is read is ordinary churn; other read failures keep
// cleanup pending. Zombies cannot fork, hold a slave, or receive a signal and
// are omitted (in particular the deliberately unreaped session leader).
// origin: hot-update S4 plan §6.2 / T15 / T16
func ScanSession(leader Identity) ([]SessionMember, error) {
	if leader.OSPID <= 0 || leader.BootID == "" || leader.StartTimeTicks <= 0 {
		return nil, fmt.Errorf("workload: incomplete session leader identity")
	}
	_, boot, err := currentProcessOwner()
	if err != nil {
		return nil, err
	}
	if boot != leader.BootID {
		return nil, fmt.Errorf("workload: session boot identity changed")
	}
	actual, err := readProcessIdentity(leader.OSPID)
	if err != nil {
		return nil, err
	}
	if actual.startTicks != leader.StartTimeTicks || actual.session != leader.OSPID || actual.parent != os.Getpid() {
		return nil, fmt.Errorf("workload: session leader identity changed")
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("workload: scan session: %w", err)
	}
	var members []SessionMember
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		current, err := readProcessIdentity(pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if current.session != leader.OSPID || current.state == "Z" || current.state == "X" {
			continue
		}
		if len(members) == maxProcessRecords {
			return nil, fmt.Errorf("workload: session census exceeds member limit")
		}
		// A member is not another public task. ID associates the obligation
		// with its owner; the boot/PID/ticks tuple identifies the kernel member.
		id := Identity{ID: leader.ID, OSPID: pid, BootID: boot, StartTimeTicks: current.startTicks}
		if pid == leader.OSPID {
			id.StartedAt = leader.StartedAt
		}
		members = append(members, SessionMember{Identity: id, SessionID: leader.OSPID})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Identity.OSPID < members[j].Identity.OSPID })
	return members, nil
}

// SignalIdentity pins a single member with a pidfd before rechecking identity.
// There is deliberately no kill(pid) fallback: reaped descendants can have
// their numeric PID reused between a census and this call. ESRCH means this
// particular member is gone, never that the whole session has been cleaned.
// origin: hot-update S4 plan §6.2 / T15
func SignalIdentity(member SessionMember, sig syscall.Signal) error {
	id := member.Identity
	if id.OSPID <= 0 || id.StartTimeTicks <= 0 || id.BootID == "" || member.SessionID <= 0 {
		return fmt.Errorf("workload: incomplete session member identity")
	}
	fd, err := unix.PidfdOpen(id.OSPID, 0)
	if err != nil {
		return fmt.Errorf("workload: open session member pidfd: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	_, boot, err := currentProcessOwner()
	if err != nil {
		return err
	}
	actual, err := readProcessIdentity(id.OSPID)
	if errors.Is(err, os.ErrNotExist) {
		return syscall.ESRCH
	}
	if err != nil {
		return err
	}
	// The check must follow pidfd_open; checking only the census (or checking
	// before acquiring the pidfd) would give a recycled PID signal authority.
	if boot != id.BootID || actual.startTicks != id.StartTimeTicks || actual.session != member.SessionID {
		return fmt.Errorf("workload: session member identity changed")
	}
	if err := unix.PidfdSendSignal(fd, unix.Signal(sig), nil, 0); err != nil {
		return fmt.Errorf("workload: signal session member: %w", err)
	}
	return nil
}
