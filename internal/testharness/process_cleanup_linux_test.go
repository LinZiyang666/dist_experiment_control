//go:build linux

package testharness

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Real subprocesses test Cleanup failure reporting, rather than just the path
// matcher. Leaks exercise argv, cwd, exe and a descendant that drops all three
// references and starts a new session. A private sibling must remain alive.
// origin: hot-update S5 PROC-LEAK / shared guard negative controls.
func TestProcessCleanupRejectsAndKillsFixtureSurvivors(t *testing.T) {
	const mode = "_TETHER_TEST_PROCESS_CLEANUP"
	if scenario := os.Getenv(mode); scenario != "" {
		root := os.Getenv(mode + "_ROOT")
		var cmd *exec.Cmd
		switch scenario {
		case "argv":
			cmd = exec.Command("/bin/sh", "-c", "sleep 60; :", filepath.Join(root, "marker"))
		case "cwd", "clean":
			cmd = exec.Command("/bin/sleep", "60")
			cmd.Dir = root
		case "exe":
			path := filepath.Join(root, "private-sleep")
			data, err := os.ReadFile("/bin/sleep")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0700); err != nil {
				t.Fatal(err)
			}
			cmd = exec.Command(path, "60")
			cmd.Args[0] = "/bin/sleep" // Only exe references the private path.
		case "descendant":
			// setsid gives the descendant its own group. Its cwd and executable
			// contain no private path, so the ancestry scan is necessary.
			cmd = exec.Command("/bin/sh", "-c", "cd /; setsid /bin/sleep 60 & echo $! > \"$1/member\"; wait", "fixture", root)
			cmd.Dir = root
		default:
			t.Fatal("unknown process guard control")
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "leader"), []byte(strconv.Itoa(cmd.Process.Pid)), 0600); err != nil {
			t.Fatal(err)
		}
		// The guard kills and reports the leaked child. Wait afterwards so the
		// control itself leaves no zombies in its parent, even on assertion failure.
		t.Cleanup(func() { _ = cmd.Wait() })
		// Cleanup runs LIFO, so the guard must run before the blocking Wait.
		GuardProcessDirectory(t, root)
		if scenario == "clean" {
			t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
		}
		if scenario == "descendant" {
			if !WaitFor(t, 2*goroutineLeakPollAttempts*goroutineLeakPollInterval, goroutineLeakPollInterval, func() bool {
				_, err := os.Stat(filepath.Join(root, "member"))
				return err == nil
			}) {
				t.Fatal("descendant did not start")
			}
		}
		return
	}
	for round := 0; round < 5; round++ {
		for _, scenario := range []string{"argv", "cwd", "exe", "descendant", "clean"} {
			t.Run(scenario+strconv.Itoa(round), func(t *testing.T) { exerciseProcessCleanup(t, scenario) })
		}
	}
}

// A scan can finish after the kill budget while finding no fixture process.
// Expire the budget before scanning to exercise that scheduling boundary
// deterministically; the real procfs census must still confirm emptiness.
// origin: S8 internal review FIX-AGENTRACE-A F3
func TestProcessCleanupConfirmsEmptyCensusAfterDeadline(t *testing.T) {
	leaks, err := cleanFixtureProcessesUntil(t.TempDir(), time.Now().Add(-time.Second))
	if err != nil || len(leaks) != 0 {
		t.Fatalf("empty census reported survivors after its scan deadline: leaks=%v err=%v", leaks, err)
	}
}

func exerciseProcessCleanup(t *testing.T, scenario string) {
	t.Helper()
	root := t.TempDir()
	GuardProcessDirectory(t, root)
	// Prefix matching must not sweep a different test's directory.
	sibling := root + "-sibling"
	if err := os.Mkdir(sibling, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sibling) })
	witness := exec.Command("/bin/sleep", "60")
	witness.Dir = sibling
	witness.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := witness.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = witness.Process.Kill(); _ = witness.Wait() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCleanupRejectsAndKillsFixtureSurvivors$", "-test.count=1")
	// Keep mutation controls hermetic even if the scanner itself is broken.
	t.Cleanup(func() {
		for _, name := range []string{"leader", "member"} {
			data, err := os.ReadFile(filepath.Join(root, name))
			if err != nil {
				continue
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	cmd.Env = append(os.Environ(), "_TETHER_TEST_PROCESS_CLEANUP="+scenario, "_TETHER_TEST_PROCESS_CLEANUP_ROOT="+root)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	out, err := cmd.CombinedOutput()
	if scenario == "clean" {
		if err != nil {
			t.Fatalf("clean teardown rejected: %v\n%s", err, out)
		}
	} else if err == nil || !bytes.Contains(out, []byte("process leak after fixture teardown")) {
		t.Fatalf("leak control did not fail: %v\n%s", err, out)
	}
	if err := witness.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("guard killed sibling fixture: %v", err)
	}
	processes, err := fixtureProcessSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, process := range processes {
		if process.matched {
			t.Errorf("control retained pid %d", process.pid)
		}
	}
	if scenario == "descendant" {
		data, err := os.ReadFile(filepath.Join(root, "member"))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		if _, alive := processes[pid]; alive {
			t.Fatalf("escaped descendant remains alive: %d", pid)
		}
	}
}

// origin: hot-update S5 PROC-LEAK / private-directory boundary.
func TestProcessCleanupMatchesOnlyPrivatePathBoundaries(t *testing.T) {
	for _, sample := range []struct {
		text string
		want bool
	}{
		{"/tmp/private/marker", true}, {"/tmp/private (deleted)", true},
		{"sh -c wait '/tmp/private/marker';", true}, {"/tmp/private\x00arg", true},
		{"/tmp/private-sibling/marker", false}, {"/tmp/private2", false},
	} {
		if got := fixturePathReference(sample.text, "/tmp/private"); got != sample.want {
			t.Errorf("reference %q: got=%t want=%t", sample.text, got, sample.want)
		}
	}
}
