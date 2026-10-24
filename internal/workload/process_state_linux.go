//go:build linux

package workload

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const maxProcessRecords = 16384

// ExportProcesses copies a frozen process table and verifies that every live
// row still names an unreaped original child. A zombie is deliberately valid:
// its exact exit status remains in the kernel, including while ReapHeld keeps
// an exited shell's process group reserved for descendants draining output.
func (m *Manager) ExportProcesses() (ProcessState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.frozen {
		return ProcessState{}, fmt.Errorf("workload: export requires frozen ownership")
	}
	owner, boot, err := currentProcessOwner()
	if err != nil {
		return ProcessState{}, err
	}
	state := ProcessState{Version: processStateVersion, OwnerPID: os.Getpid(),
		OwnerStartTimeTicks: owner.startTicks, BootID: boot, Records: m.snapshotLocked()}
	for i := range state.Records {
		r := &state.Records[i]
		if r.Exit != nil {
			continue
		}
		actual, err := readProcessIdentity(r.OSPID)
		if err != nil {
			return ProcessState{}, err
		}
		if actual.parent != state.OwnerPID || actual.group != r.OSPID ||
			(r.StartTimeTicks != 0 && actual.startTicks != r.StartTimeTicks) ||
			(r.BootID != "" && r.BootID != boot) {
			return ProcessState{}, fmt.Errorf("workload: child %q identity changed", r.ID)
		}
		r.BootID, r.StartTimeTicks = boot, actual.startTicks
	}
	if err := validateProcessRecords(state); err != nil {
		return ProcessState{}, err
	}
	return state, nil
}

// RestoreProcesses installs a validated table in an unused manager after a
// same-PID exec. Success leaves the manager frozen: it cannot reap, signal or
// accept tasks until the runtime has restored all other domains and calls
// Thaw. No validation error consumes a child status or changes the receiver.
//
// The caller must use the authenticated local handoff bundle and must have
// replaced the old image; this table is not a cross-process ownership grant.
func (m *Manager) RestoreProcesses(state ProcessState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.tasks) != 0 || m.polling || m.frozen {
		return fmt.Errorf("workload: restore requires unused manager")
	}
	owner, boot, err := currentProcessOwner()
	if err != nil {
		return err
	}
	if state.Version != processStateVersion || state.OwnerPID != os.Getpid() ||
		state.OwnerStartTimeTicks != owner.startTicks || state.BootID != boot {
		return fmt.Errorf("workload: incompatible process state or parent identity")
	}
	if err := validateProcessRecords(state); err != nil {
		return err
	}
	// Complete all identity checks before allocating handles. Unreaped child
	// PIDs cannot be recycled while the sole parent owns their wait statuses.
	for _, r := range state.Records {
		if r.Exit == nil {
			actual, err := readProcessIdentity(r.OSPID)
			if err != nil {
				return err
			}
			if actual.parent != state.OwnerPID || actual.group != r.OSPID ||
				actual.startTicks != r.StartTimeTicks {
				return fmt.Errorf("workload: restored child %q identity changed", r.ID)
			}
		}
	}
	tasks := make(map[string]*child, len(state.Records))
	for _, r := range state.Records {
		c := &child{record: r, done: make(chan struct{})}
		if r.Exit != nil {
			result := *r.Exit
			c.record.Exit = &result
			close(c.done)
		} else {
			c.proc, err = os.FindProcess(r.OSPID)
			if err != nil {
				for _, previous := range tasks {
					if previous.proc != nil {
						_ = previous.proc.Release()
					}
				}
				return fmt.Errorf("workload: restore child handle: %w", err)
			}
		}
		tasks[r.ID] = c
	}
	m.tasks, m.frozen, m.restored = tasks, true, true
	return nil
}

func validateProcessRecords(state ProcessState) error {
	if len(state.Records) > maxProcessRecords {
		return fmt.Errorf("workload: too many process records")
	}
	ids := make(map[string]bool, len(state.Records))
	livePIDs := make(map[int]bool, len(state.Records))
	for _, r := range state.Records {
		if r.ID == "" || len(r.ID) > 256 || r.OSPID <= 0 || r.StartedAt.IsZero() || ids[r.ID] {
			return fmt.Errorf("workload: invalid or duplicate process record")
		}
		ids[r.ID] = true
		if r.Exit == nil {
			if livePIDs[r.OSPID] || r.BootID != state.BootID || r.StartTimeTicks <= 0 {
				return fmt.Errorf("workload: invalid or duplicate live child identity")
			}
			livePIDs[r.OSPID] = true
			continue
		}
		e := r.Exit
		status := syscall.WaitStatus(e.Status)
		if r.ReapHeld || e.EndedAt.IsZero() || (e.WaitError != "" &&
			(e.Code != -1 || e.Signal != 0 || e.Status != 0)) {
			return fmt.Errorf("workload: invalid terminal process record")
		}
		if e.WaitError == "" {
			signal := 0
			if status.Signaled() {
				signal = int(status.Signal())
			}
			if (!status.Exited() && !status.Signaled()) || e.Code != status.ExitStatus() || e.Signal != signal {
				return fmt.Errorf("workload: inconsistent terminal wait status")
			}
		}
	}
	return nil
}

type processIdentity struct {
	parent     int
	group      int
	session    int
	state      string
	startTicks int64
}

func currentProcessOwner() (processIdentity, string, error) {
	owner, err := readProcessIdentity(os.Getpid())
	if err != nil {
		return processIdentity{}, "", err
	}
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return processIdentity{}, "", fmt.Errorf("workload: read boot identity: %w", err)
	}
	boot := strings.TrimSpace(string(data))
	if boot == "" {
		return processIdentity{}, "", fmt.Errorf("workload: empty boot identity")
	}
	return owner, boot, nil
}

func readProcessIdentity(pid int) (processIdentity, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return processIdentity{}, fmt.Errorf("workload: read child identity: %w", err)
	}
	// comm is parenthesized and can itself contain spaces, newlines or ')'.
	// Fields after its final ')' start at state (field 3); starttime is 22.
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return processIdentity{}, fmt.Errorf("workload: malformed process identity")
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		return processIdentity{}, fmt.Errorf("workload: incomplete process identity")
	}
	parent, e1 := strconv.Atoi(fields[1])
	group, e2 := strconv.Atoi(fields[2])
	start, e3 := strconv.ParseInt(fields[19], 10, 64)
	session, e4 := strconv.Atoi(fields[3])
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || start <= 0 || len(fields[0]) != 1 {
		return processIdentity{}, fmt.Errorf("workload: invalid process identity values")
	}
	return processIdentity{parent: parent, group: group, session: session, state: fields[0], startTicks: start}, nil
}
