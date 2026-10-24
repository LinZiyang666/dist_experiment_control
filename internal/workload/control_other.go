//go:build !linux

package workload

import (
	"fmt"
	"syscall"

	"github.com/LinZiyang666/tether/internal/handoff"
)

// Legacy terminal control still shares the portable Manager barrier. The sole
// wait owner pins the child PID; portable platforms do not import Linux images.
func checkControlIdentity(id Identity) (Identity, error) {
	group, err := syscall.Getpgid(id.OSPID)
	if err != nil {
		return Identity{}, err
	}
	if id.OSPID <= 0 || group != id.OSPID {
		return Identity{}, fmt.Errorf("workload: control child identity changed")
	}
	return id, nil
}

// ScanSession requires Linux /proc identity and pidfd semantics. Legacy run
// remains available, but must not claim resumable session cleanup elsewhere.
func ScanSession(Identity) ([]SessionMember, error) { return nil, handoff.ErrUnsupported }

// SignalIdentity has no numeric-PID fallback on unsupported platforms.
func SignalIdentity(SessionMember, syscall.Signal) error { return handoff.ErrUnsupported }
