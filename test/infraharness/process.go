// Package infraharness adapts the shared FIX-ORPHAN supervisor for infra tests.
// It owns fixture processes, never product process retirement or host units.
package infraharness

import (
	"errors"
	"os/exec"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Identity is an incarnation, including zombies until their owner has waited.
type Identity struct {
	BootID     string
	PID        int
	StartTicks uint64
}

// Process owns the shared wait closure and its lease; Cmd.Wait must not be used.
// The command's context cancels the supervisor lease rather than its reaper.
type Process struct {
	identity Identity
	wait     func() error
	stopOnce sync.Once
	stopErr  error
}

// StartProcess installs supervision before the target's first instruction.
// A short-lived target may already be a zombie at the identity handshake.
// origin: hot-update S9 plan §9.5 / T57 / lead L22
func StartProcess(cmd *exec.Cmd) (*Process, error) {
	if err := supervisionSupported(); err != nil {
		return nil, err
	}
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		return nil, err
	}
	id, err := ReadIdentity(cmd.Process.Pid)
	if err != nil {
		// Even a failed receipt must relinquish the lease and collect the tree.
		_ = cmd.Process.Kill()
		return nil, errors.Join(err, wait())
	}
	return &Process{identity: id, wait: wait}, nil
}

// Identity returns a copy; callers cannot replace the incarnation Stop signals.
func (p *Process) Identity() Identity { return p.identity }

// Wait joins target and descendants, preserving the command's native result.
func (p *Process) Wait() error { return p.wait() }

// Stop signals only the recorded incarnation; shared supervision collects all
// descendants, including setsid grandchildren and children of dead parents.
func (p *Process) Stop() error {
	return stopProcess(p, 10*time.Second)
}

func stopProcess(p *Process, limit time.Duration) error {
	p.stopOnce.Do(func() {
		signalErr := killIdentity(p.identity)
		joined := make(chan error, 1)
		go func() { joined <- p.Wait() }()
		timer := time.NewTimer(limit)
		defer timer.Stop()
		select {
		case waitErr := <-joined:
			p.stopErr = errors.Join(signalErr, waitErr)
		case <-timer.C:
			// Keep the shared reaper alive to complete custody. An expired join
			// budget is failure, never permission to kill the reaper or sign zero.
			p.stopErr = errors.Join(signalErr, errors.New("infra harness: supervisor join deadline exceeded"))
		}
	})
	return p.stopErr
}
