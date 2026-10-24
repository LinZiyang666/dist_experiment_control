//go:build !linux

package workload

import "github.com/LinZiyang666/tether/internal/handoff"

func (m *Manager) ExportProcesses() (ProcessState, error) {
	return ProcessState{}, handoff.ErrUnsupported
}

func (m *Manager) RestoreProcesses(_ ProcessState) error {
	return handoff.ErrUnsupported
}
