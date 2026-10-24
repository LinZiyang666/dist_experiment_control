//go:build darwin

package transferharness

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// Procfs PID/start-ticks evidence is Linux-only. The portable exercise still
// requires a native child PID handshake, its exact exit status and joined Stop;
// Darwin's shared supervisor owns its independent kernel-start-time census.
type processIdentity struct {
	PID   int
	Start uint64
}

func captureSupervisedCensus(t *testing.T, p *Process, _ string) []processIdentity {
	t.Helper()
	child, parent, err := readProcessIdentity(p.Receipt().PID)
	if err != nil || child.Start == 0 || parent <= 1 || parent == os.Getpid() {
		t.Fatalf("process has no external supervisor: child=%+v parent=%d err=%v", child, parent, err)
	}
	owner, _, err := readProcessIdentity(parent)
	if err != nil || owner.Start == 0 {
		t.Fatalf("supervisor has no live kernel identity: %+v: %v", owner, err)
	}
	return []processIdentity{child, owner}
}

func readProcessIdentity(pid int) (processIdentity, int, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EIO) {
		return processIdentity{}, 0, os.ErrNotExist
	}
	if err != nil {
		return processIdentity{}, 0, err
	}
	return processIdentity{PID: int(info.Proc.P_pid),
		Start: uint64(info.Proc.P_starttime.Sec)*1_000_000 + uint64(info.Proc.P_starttime.Usec)}, int(info.Eproc.Ppid), nil
}

func assertSupervisedCensusRetired(t *testing.T, census []processIdentity) {
	t.Helper()
	for _, previous := range census {
		current, _, err := readProcessIdentity(previous.PID)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || current == previous {
			t.Errorf("supervised process survived join or census failed: %+v: %v", previous, err)
		}
	}
}
