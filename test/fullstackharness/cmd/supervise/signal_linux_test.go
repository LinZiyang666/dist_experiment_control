//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
)

// A real TERM/INT handler must receive its original signal and join its child.
// An escaping child still earns native125 even when the target reports143.
// origin: S10 internal review FIX-SUPERVISE-2 F2
func TestSupervisorForwardsSignalsAndKeepsEscapeFailure(t *testing.T) {
	launcher := buildSignalLauncher(t)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorSignalForwarding(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor signal forwarding x5", g)
	testharness.AssertNoFDLeak(t, "supervisor signal forwarding x5", f, 0)
}

// A target ignoring TERM cannot keep the lease indefinitely. The shared helper
// still owns escalation and the descendant census rather than a group kill.
// origin: S10 internal review FIX-SUPERVISE-2 F3
func TestSupervisorEscalatesIgnoredSignal(t *testing.T) {
	launcher := buildSignalLauncher(t)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorIgnoredSignal(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor ignored TERM x5", g)
	testharness.AssertNoFDLeak(t, "supervisor ignored TERM x5", f, 0)
}

func buildSignalLauncher(t *testing.T) string {
	t.Helper()
	launcher := filepath.Join(t.TempDir(), "supervise")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", launcher, ".")
	if out, err := fullstackharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build launcher: %v\n%s", err, out)
	}
	return launcher
}

func exerciseSupervisorSignalForwarding(t *testing.T, launcher string) {
	t.Helper()
	for _, tc := range []struct {
		mode   string
		signal syscall.Signal
		want   int
	}{
		{"join", syscall.SIGTERM, 143},
		{"join", syscall.SIGINT, 130},
		{"escape", syscall.SIGTERM, 125},
	} {
		runSignalFixture(t, launcher, tc.mode, tc.signal, tc.want)
	}
}

func exerciseSupervisorIgnoredSignal(t *testing.T, launcher string) {
	t.Helper()
	runSignalFixture(t, launcher, "ignore", syscall.SIGTERM, 125)
}

const signalFixture = `import json,os,pathlib,signal,subprocess,sys
root=pathlib.Path(sys.argv[1]); mode=sys.argv[2]
child=subprocess.Popen([sys.executable,'-c','import signal; signal.pause()'],start_new_session=True)
def identity(pid):
    fields=pathlib.Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()
    return dict(pid=pid,ticks=fields[19])
def stop(number,frame):
    if mode=='ignore':
        (root/'handled').write_text(str(number))
        return
    if mode=='join':
        child.terminate(); child.wait()
        (root/'joined').write_text(str(number))
    signal.signal(number,signal.SIG_DFL); os.kill(os.getpid(),number)
signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop)
# Child exec is fenced before READY; an early TERM must not hit Python startup.
# The subprocess exec error pipe's CLOEXEC EOF supplies that independent fence.
(root/'ready').write_text(json.dumps([identity(os.getpid()),identity(child.pid)]))
while True: signal.pause()
`

func runSignalFixture(t *testing.T, launcher, mode string, sig syscall.Signal, want int) {
	t.Helper()
	root := t.TempDir()
	log, err := os.Create(filepath.Join(root, "output"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	receipt := filepath.Join(root, "native.rc")
	cmd := exec.Command(launcher, "--receipt", receipt, "--", "python3", "-I", "-B", "-c", signalFixture, root, mode)
	cmd.Stdout, cmd.Stderr = log, log
	wait, err := fullstackharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	joined := false
	defer func() {
		if !joined {
			_ = cmd.Process.Kill()
			<-done // Outer custody removes descendants of a broken launcher.
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	var identities []struct {
		PID   int    `json:"pid"`
		Ticks string `json:"ticks"`
	}
	for {
		body, readErr := os.ReadFile(filepath.Join(root, "ready"))
		if readErr == nil && json.Unmarshal(body, &identities) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("target did not install its signal handler before READY")
		}
		time.Sleep(10 * time.Millisecond) // Poll the actual handler/child-exec fence.
	}
	if err := cmd.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		joined = true
	case <-time.After(15 * time.Second):
		t.Fatal("ignored signal did not escalate and join within the bounded lease")
	}
	output, readErr := os.ReadFile(log.Name())
	if readErr != nil {
		t.Fatal(readErr)
	}
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != want || fullstackharness.ProcessExitCode(err) != want {
		t.Fatalf("signal %d mode=%s: target=%v rc=%d want=%d output=%s", sig, mode, cmd.ProcessState, fullstackharness.ProcessExitCode(err), want, output)
	}
	body, readErr := os.ReadFile(receipt)
	if readErr != nil || string(body) != strconv.Itoa(want)+"\n" {
		t.Fatalf("signal cleanup receipt=%q err=%v want=%d", body, readErr, want)
	}
	if mode == "join" || mode == "ignore" {
		name := "joined"
		if mode == "ignore" {
			name = "handled"
		}
		body, readErr := os.ReadFile(filepath.Join(root, name))
		if readErr != nil || string(body) != strconv.Itoa(int(sig)) {
			t.Fatalf("original signal handler not reached: mode=%s evidence=%q err=%v", mode, body, readErr)
		}
	}
	for _, id := range identities {
		body, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", id.PID))
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal(readErr)
		}
		if readErr == nil && strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])[19] == id.Ticks {
			t.Fatalf("signal return abandoned pid=%d start_ticks=%s", id.PID, id.Ticks)
		}
		if mode != "join" && id != identities[0] && !strings.Contains(string(output), fmt.Sprintf("pid=%d start_ticks=%s state=", id.PID, id.Ticks)) {
			t.Fatalf("escape failure lost descendant identity: %s", output)
		}
	}
}
