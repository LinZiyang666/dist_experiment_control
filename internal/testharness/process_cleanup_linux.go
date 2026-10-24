//go:build linux

package testharness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// GuardProcessDirectory registers a final check for a fixture's private directory.
// Register it before the fixture's own Cleanup callbacks: Cleanup runs in reverse
// order, so this guard observes teardown rather than becoming its replacement.
// Any surviving process fails the test even when the emergency kill succeeds.
// origin: hot-update S5 PROC-LEAK / normal and refused-import teardown.
func GuardProcessDirectory(t *testing.T, dir string) string {
	t.Helper()
	root, err := filepath.Abs(dir)
	if err != nil || root == "/" || root == os.TempDir() || root == "." {
		t.Fatalf("process leak guard requires a private directory: %q (%v)", dir, err)
	}
	t.Cleanup(func() {
		leaks, err := cleanFixtureProcesses(root)
		if err != nil {
			t.Errorf("process leak scan: %v", err)
		}
		if len(leaks) != 0 {
			t.Errorf("process leak after fixture teardown in %s: %s", root, strings.Join(leaks, "; "))
		}
	})
	return dir
}

type fixtureProcess struct {
	pid, parent int
	ticks       string
	exe         string
	matched     bool
}

// Check argument boundaries as well as path boundaries: /tmp/a must not match
// /tmp/another-test. A shell command may embed the path in its program text.
func fixturePathReference(text, root string) bool {
	for offset := 0; offset < len(text); {
		index := strings.Index(text[offset:], root)
		if index < 0 {
			return false
		}
		end := offset + index + len(root)
		if end == len(text) || strings.ContainsRune("/\x00 \t\n\r\"'();", rune(text[end])) {
			return true
		}
		offset = end
	}
	return false
}

func fixtureProcessSnapshot(root string) (map[int]fixtureProcess, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	processes := make(map[int]fixtureProcess)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		base := filepath.Join("/proc", entry.Name())
		stat, err := os.ReadFile(filepath.Join(base, "stat"))
		if err != nil {
			// A process can exit or revoke /proc access between the directory
			// and stat reads. It cannot be signalled safely without identity.
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) || errors.Is(err, os.ErrPermission) {
				continue
			}
			return nil, err
		}
		end := strings.LastIndexByte(string(stat), ')')
		fields := strings.Fields(string(stat)[end+1:])
		if end < 0 || len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
			continue // A zombie has no running code or live cwd/argv descriptors.
		}
		parent, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, err
		}
		argv, _ := os.ReadFile(filepath.Join(base, "cmdline"))
		cwd, _ := os.Readlink(filepath.Join(base, "cwd"))
		exe, _ := os.Readlink(filepath.Join(base, "exe"))
		processes[pid] = fixtureProcess{pid: pid, parent: parent, ticks: fields[19], exe: exe,
			matched: !strings.Contains(string(argv), "codex") &&
				(fixturePathReference(string(argv), root) || fixturePathReference(cwd, root) || fixturePathReference(exe, root))}
	}
	// Helpers can themselves run in the private cwd. Never kill this test or
	// its ancestors; parallel fixtures elsewhere remain outside this boundary.
	for pid := os.Getpid(); pid > 0; {
		process, ok := processes[pid]
		if !ok {
			break
		}
		process.matched = false
		processes[pid] = process
		pid = process.parent
	}
	return processes, nil
}

func cleanFixtureProcesses(root string) ([]string, error) {
	return cleanFixtureProcessesUntil(root, time.Now().Add(3*time.Second))
}

func cleanFixtureProcessesUntil(root string, deadline time.Time) ([]string, error) {
	seen := make(map[int]fixtureProcess)
	empty := false
	for {
		processes, err := fixtureProcessSnapshot(root)
		if err != nil {
			return fixtureProcessDescriptions(seen), err
		}
		owned := make(map[int]fixtureProcess)
		for pid, process := range processes {
			previous, tracked := seen[pid]
			if process.matched || (tracked && previous.ticks == process.ticks) {
				owned[pid] = process
			}
		}
		// A child can exec a binary without the path or change cwd before its
		// parent is killed. Capture the whole tree before reparenting hides it.
		for changed := true; changed; {
			changed = false
			for pid, process := range processes {
				if _, ok := owned[process.parent]; !ok {
					continue
				}
				if _, ok := owned[pid]; !ok {
					owned[pid], changed = process, true
				}
			}
		}
		if len(owned) == 0 {
			if empty {
				return fixtureProcessDescriptions(seen), nil
			}
			empty = true
		} else {
			empty = false
		}
		for pid, process := range owned {
			seen[pid] = process
			if err := signalFixtureProcess(process); err != nil {
				return fixtureProcessDescriptions(seen), err
			}
		}
		// A slow procfs scan can consume the budget without finding any owned
		// process. Still require two empty censuses; only an observed survivor
		// can exhaust the kill budget. Seen leaks remain reportable after exit.
		// origin: S8 internal review FIX-AGENTRACE-A F3
		if len(owned) != 0 && time.Now().After(deadline) {
			return fixtureProcessDescriptions(seen), fmt.Errorf("fixture processes survived SIGKILL")
		}
		time.Sleep(10 * time.Millisecond) // Poll process exit/reparenting, not fixture readiness.
	}
}

// Pin the PID before signalling so recycling cannot kill a different fixture.
// Both normal cleanup and the fatal-exit subreaper verify the recorded start
// ticks after acquiring the pidfd; process names never establish ownership.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN request 5.
func signalFixtureProcess(process fixtureProcess) error {
	fd, err := unix.PidfdOpen(process.pid, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(process.pid), "stat"))
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	end := strings.LastIndexByte(string(stat), ')')
	fields := strings.Fields(string(stat)[end+1:])
	if end < 0 || len(fields) < 20 {
		return errors.New("process identity unavailable")
	}
	if fields[19] != process.ticks {
		return nil
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func fixtureProcessDescriptions(processes map[int]fixtureProcess) []string {
	ids := make([]int, 0, len(processes))
	for pid := range processes {
		ids = append(ids, pid)
	}
	sort.Ints(ids)
	var descriptions []string
	for _, pid := range ids {
		p := processes[pid]
		// Avoid dumping argv: data-plane secrets must never enter diagnostics.
		descriptions = append(descriptions, fmt.Sprintf("pid=%d ppid=%d exe=%s", pid, p.parent, p.exe))
	}
	return descriptions
}
