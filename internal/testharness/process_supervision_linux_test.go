//go:build linux

package testharness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const supervisionRole = "_TETHER_TEST_SUPERVISION_ROLE"
const supervisionRoot = "_TETHER_TEST_SUPERVISION_ROOT"

type supervisionIdentity struct {
	PID   int
	Ticks string
}

type supervisionReceipt struct {
	Before, After             supervisionIdentity
	LeaderSignal, AfterSignal int
}

// Pin the original leader during init so the worker-exec negative premise is
// deterministic: Linux clones clear pdeath_signal even for CLONE_THREAD, and
// de_thread transfers PID/start time but never that per-task setting.
func init() {
	role := os.Getenv(supervisionRole)
	if role != "before" && role != "after" && role != "intermediate" {
		return
	}
	root := os.Getenv(supervisionRoot)
	if role == "intermediate" {
		cmd := exec.Command("/bin/sleep", "60")
		cmd.Dir = "/"
		cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		writeSupervisionReceipt(filepath.Join(root, "grandchild.json"), supervisionProcessIdentity(cmd.Process.Pid))
		os.Exit(0) // The grandchild has lost every private path and its original parent.
	}
	runtime.LockOSThread()
	if unix.Gettid() != os.Getpid() {
		panic("supervision proof did not start on the leader thread")
	}
	if role == "before" {
		receipt := supervisionReceipt{Before: supervisionProcessIdentity(os.Getpid()), LeaderSignal: supervisionDeathSignal()}
		writeSupervisionReceipt(filepath.Join(root, "before.json"), receipt)
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessSupervisionReceiver$", "-test.count=1")
		cmd.Env = supervisionEnvironment("intermediate", root)
		if err := cmd.Run(); err != nil {
			panic(err)
		}
		go func() {
			runtime.LockOSThread()
			if unix.Gettid() == os.Getpid() || supervisionDeathSignal() != 0 {
				panic("supervision proof requires a worker without Pdeathsig")
			}
			if err := syscall.Exec(os.Args[0], os.Args, supervisionEnvironment("after", root)); err != nil {
				panic(err)
			}
		}()
	} else {
		body, err := os.ReadFile(filepath.Join(root, "before.json"))
		if err != nil {
			panic(err)
		}
		var receipt supervisionReceipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			panic(err)
		}
		receipt.After, receipt.AfterSignal = supervisionProcessIdentity(os.Getpid()), supervisionDeathSignal()
		writeSupervisionReceipt(filepath.Join(root, "after.json"), receipt)
	}
	<-time.NewTimer(time.Hour).C // The actor waits for supervision, never a readiness sleep.
	os.Exit(127)
}

func supervisionDeathSignal() int {
	var signal int32
	if err := unix.Prctl(unix.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&signal)), 0, 0, 0); err != nil {
		panic(err)
	}
	return int(signal)
}

func supervisionProcessIdentity(pid int) supervisionIdentity {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		panic(err)
	}
	fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
	return supervisionIdentity{PID: pid, Ticks: fields[19]}
}

func writeSupervisionReceipt(path string, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(path+".tmp", body, 0600); err != nil {
		panic(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		panic(err)
	}
}

func supervisionEnvironment(role, root string) []string {
	var env []string
	for _, entry := range processEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, supervisionRole+"=") && !strings.HasPrefix(entry, supervisionRoot+"=") && !strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}
	return append(env, supervisionRole+"="+role, supervisionRoot+"="+root, "GORACE=atexit_sleep_ms=0")
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN panic regression.
func TestProcessSupervisionReapsAfterPanic(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessSupervision(t, "panic")
	}
	AssertNoGoroutineLeak(t, "panic supervision x5", beforeG)
	AssertNoFDLeak(t, "panic supervision x5", beforeFD, 0)
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN package timeout regression.
func TestProcessSupervisionReapsAfterTimeout(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessSupervision(t, "timeout")
	}
	AssertNoGoroutineLeak(t, "timeout supervision x5", beforeG)
	AssertNoFDLeak(t, "timeout supervision x5", beforeFD, 0)
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN SIGKILL regression.
func TestProcessSupervisionReapsAfterSIGKILL(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessSupervision(t, "sigkill")
	}
	AssertNoGoroutineLeak(t, "SIGKILL supervision x5", beforeG)
	AssertNoFDLeak(t, "SIGKILL supervision x5", beforeFD, 0)
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN same-PID worker exec regression.
func TestProcessSupervisionReapsAfterSamePIDExec(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessSupervision(t, "exec")
	}
	AssertNoGoroutineLeak(t, "same-PID exec supervision x5", beforeG)
	AssertNoFDLeak(t, "same-PID exec supervision x5", beforeFD, 0)
}

// This raw subprocess is intentionally the victim test process. Its fixtures
// use StartProcess, but it must be killable independently of their reaper.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN fatal-exit receiver.
func TestProcessSupervisionReceiver(t *testing.T) {
	role, root := os.Getenv(supervisionRole), os.Getenv(supervisionRoot)
	if role == "" {
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessSupervisionReceiver$", "-test.count=1")
	cmd.Env = supervisionEnvironment("before", root)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		_, err := os.Stat(filepath.Join(root, "after.json"))
		return err == nil
	}) {
		_ = cmd.Process.Kill()
		_ = wait()
		t.Fatal("worker exec did not reach its receipt")
	}
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "stat"))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	writeSupervisionReceipt(filepath.Join(root, "supervisor.json"), supervisionProcessIdentity(parent))
	if !WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		_, err := os.Stat(filepath.Join(root, "release"))
		return err == nil
	}) && role != "timeout" {
		t.Fatal("owner release barrier missing")
	}
	switch role {
	case "panic":
		go func() { panic("supervision owner panic") }()
	case "exec":
		os.Exit(0) // Normal process exit also closes the kernel lease, without Cleanup.
	}
	<-time.NewTimer(time.Hour).C
}

func exerciseProcessSupervision(t *testing.T, scenario string) {
	t.Helper()
	root := t.TempDir()
	// The outer test adopts the fixture supervisor after killing its owner, and
	// reaps only that recorded PID. It never consumes another test's wait status.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0) }()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessSupervisionReceiver$", "-test.count=1", "-test.timeout=2s")
	if scenario != "timeout" {
		cmd.Args[len(cmd.Args)-1] = "-test.timeout=10s"
	}
	cmd.Env = supervisionEnvironment(scenario, root)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var identities []supervisionIdentity
	defer func() {
		_ = cmd.Process.Kill()
		for _, id := range identities {
			_ = signalFixtureProcess(fixtureProcess{pid: id.PID, ticks: id.Ticks})
		}
		// Kill/wait the exact adopted tree even when a mutation breaks the helper.
		if len(identities) != 0 {
			_, _ = unix.Wait4(identities[len(identities)-1].PID, nil, 0, nil)
		}
		for _, id := range identities[:max(0, len(identities)-1)] {
			_, _ = unix.Wait4(id.PID, nil, 0, nil)
		}
		for _, id := range identities {
			if state := supervisionIdentityState(id); state != "" {
				t.Errorf("recorded descendant remains after reaping: pid=%d ticks=%s state=%s", id.PID, id.Ticks, state)
			}
		}
	}()
	if !WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		_, err := os.Stat(filepath.Join(root, "supervisor.json"))
		return err == nil
	}) {
		_ = cmd.Process.Kill()
		err := <-done
		t.Fatalf("owner did not publish its supervised tree: %v\n%s", err, output.String())
	}
	var receipt supervisionReceipt
	read := func(name string, value any) {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, value); err != nil {
			t.Fatal(err)
		}
	}
	read("after.json", &receipt)
	identities = append(identities, receipt.After)
	for _, name := range []string{"grandchild.json", "supervisor.json"} {
		var id supervisionIdentity
		read(name, &id)
		identities = append(identities, id)
	}
	if receipt.Before != receipt.After || receipt.LeaderSignal != int(syscall.SIGKILL) || receipt.AfterSignal != 0 {
		t.Fatalf("same-PID exec did not demonstrate loss of the per-task death signal: %+v", receipt)
	}
	for _, id := range identities {
		if !supervisionIdentityAlive(id) {
			t.Fatalf("supervision census has no live pre-fault pid=%d ticks=%s", id.PID, id.Ticks)
		}
	}
	if scenario == "sigkill" {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	} else if scenario != "timeout" {
		if err := os.WriteFile(filepath.Join(root, "release"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("victim test process did not exit")
	}
	if scenario == "exec" && err != nil {
		t.Fatalf("owner normal exit: %v\n%s", err, output.String())
	}
	if scenario != "exec" && err == nil {
		t.Fatal("fatal-exit control unexpectedly succeeded")
	}
	if scenario == "panic" && !strings.Contains(output.String(), "panic: supervision owner panic") {
		t.Fatalf("panic control did not reach the injected panic: %s", output.String())
	}
	if scenario == "timeout" && !strings.Contains(output.String(), "panic: test timed out after 2s") {
		t.Fatalf("timeout control did not reach testing's alarm: %s", output.String())
	}
	if !WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		for _, id := range identities {
			if supervisionIdentityAlive(id) {
				return false
			}
		}
		return true
	}) {
		for _, id := range identities {
			if supervisionIdentityAlive(id) {
				t.Errorf("surviving descendant pid=%d ticks=%s after %s", id.PID, id.Ticks, scenario)
			}
		}
	}
}

func supervisionIdentityAlive(id supervisionIdentity) bool {
	state := supervisionIdentityState(id)
	return state != "" && state != "Z" && state != "X"
}

func supervisionIdentityState(id supervisionIdentity) string {
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(id.PID), "stat"))
	if os.IsNotExist(err) || errors.Is(err, unix.ESRCH) {
		return ""
	}
	if err != nil {
		panic(fmt.Sprintf("supervision identity census: %v", err))
	}
	fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
	if fields[19] != id.Ticks {
		return ""
	}
	return fields[0]
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN cancellation and normal-exit controls.
func TestProcessSupervisionPreservesCommandResults(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisedCommand(t)
	}
	AssertNoGoroutineLeak(t, "supervised command results x5", beforeG)
	AssertNoFDLeak(t, "supervised command results x5", beforeFD, 0)
}

// origin: S10 internal review rv-harness F5
func TestProcessSupervisionRetainsDescendantCensus(t *testing.T) {
	g, f := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorCensus(t)
	}
	AssertNoGoroutineLeak(t, "supervisor census lifecycle", g)
	AssertNoFDLeak(t, "supervisor census lifecycle", f, 0)
}

func exerciseSupervisorCensus(t *testing.T) {
	t.Helper()
	for _, code := range []int{0, 1} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `setsid /bin/sleep 60 >/dev/null 2>&1 & printf '%s\n' "$!"; exit "$1"`, "fixture", strconv.Itoa(code))
		output, err := Output(cmd)
		cancel()
		pid := strings.TrimSpace(string(output))
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 125 || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != code {
			t.Fatalf("supervisor/target statuses conflated: target=%v err=%v", cmd.ProcessState, err)
		}
		if pid == "" || !strings.Contains(err.Error(), "pid="+pid+" ") || !strings.Contains(err.Error(), "start_ticks=") {
			t.Fatalf("descendant census lost: pid=%q err=%v", pid, err)
		}
		if _, err := os.Stat("/proc/" + pid); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reported descendant survived cleanup: pid=%s err=%v", pid, err)
		}
	}
}

func exerciseSupervisedCommand(t *testing.T) {
	t.Helper()
	for _, fd := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("stdio-eof-%d", fd), func(t *testing.T) { exerciseSupervisedStdioEOF(t, fd) })
	}
	root := t.TempDir()
	inherited, err := os.Create(filepath.Join(root, "inherited"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inherited.Close() }()
	command := exec.Command("/bin/sh", "-c", `printf '%s\n' "$PWD" "$ORIGINAL_ENV" "$_TETHER_TEST_PROCESS_SUPERVISOR" "$GORACE" "$1" >&3; printf '%s\n' "$$"`, "fixture", "original argument")
	command.Dir, command.Env = root, []string{"ORIGINAL_ENV=original value", "GORACE=history_size=4"}
	command.ExtraFiles = []*os.File{inherited}
	pidOutput, err := Output(command)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(pidOutput)) != strconv.Itoa(command.Process.Pid) {
		t.Fatal("supervision exposed a helper PID instead of the original target")
	}
	fileOutput, err := os.ReadFile(inherited.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(fileOutput) != root+"\noriginal value\n\nhistory_size=4\noriginal argument\n" {
		t.Fatalf("supervision changed cwd, environment, arguments or fd 3: %q", fileOutput)
	}
	for _, code := range []string{"0", "23"} {
		cmd := exec.Command("/bin/sh", "-c", "cat; echo diagnostic >&2; exit \"$1\"", "fixture", code)
		cmd.Stdin = strings.NewReader("original stdin\n")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := Output(cmd)
		if string(stdout) != "original stdin\n" || stderr.String() != "diagnostic\n" {
			t.Fatal("supervision changed command stdio")
		}
		if (code == "0" && err != nil) || (code == "23" && (err == nil || cmd.ProcessState.ExitCode() != 23)) {
			t.Fatalf("supervision changed exit result: %s %v", code, err)
		}
	}
	cmd := exec.Command("/bin/sh", "-c", "setsid /bin/sleep 60 >/dev/null 2>&1 &")
	output, err := CombinedOutput(cmd)
	if err == nil || !bytes.Contains(output, []byte("process exited with surviving descendants")) {
		t.Fatalf("normal-exit descendant leak was hidden: %v %s", err, output)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cmd = exec.CommandContext(ctx, "/bin/sleep", "60")
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	id := supervisionProcessIdentity(cmd.Process.Pid)
	cancel()
	if err := wait(); err == nil {
		t.Fatal("canceled supervised command succeeded")
	}
	if supervisionIdentityAlive(id) {
		t.Fatal("canceled supervised command is still alive")
	}
	// A short WaitDelay must only bound stale output pipes after the reaper has
	// joined its tree. Killing the reaper itself leaves escaped descendants,
	// including zombies that are invisible to a live-process-only census.
	// origin: hot-update S6 plan §9.3 / FIX-ORPHAN cancellation seam.
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	cmd = exec.CommandContext(ctx, "/bin/sh", "-c", "setsid /bin/sleep 60 >/dev/null 2>&1 & printf '%s\\n' \"$!\" >&3; read stop")
	// Contain a deliberately killed-reaper mutation even if it beats lease EOF:
	// the shell leader dies, while its escaped grandchild remains the oracle.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.WaitDelay = time.Nanosecond
	cmd.ExtraFiles = []*os.File{inherited}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	if err := inherited.Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := inherited.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	wait, err = StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = wait() }()
	var descendantPID int
	if !WaitFor(t, 2*time.Second, time.Millisecond, func() bool {
		body, err := os.ReadFile(inherited.Name())
		if err != nil {
			return false
		}
		descendantPID, err = strconv.Atoi(strings.TrimSpace(string(body)))
		return err == nil && descendantPID > 0
	}) {
		t.Fatal("cancel control did not start its escaped descendant")
	}
	descendant := supervisionProcessIdentity(descendantPID)
	defer func() {
		_ = signalFixtureProcess(fixtureProcess{pid: descendant.PID, ticks: descendant.Ticks})
		_, _ = unix.Wait4(descendant.PID, nil, 0, nil)
	}()
	cancel()
	if err := wait(); err == nil {
		t.Fatal("canceled descendant command returned success")
	}
	if supervisionIdentityState(descendant) != "" {
		t.Fatal("WaitDelay killed the reaper before its escaped descendant was reaped")
	}
	if _, err := StartProcess(exec.Command(filepath.Join(t.TempDir(), "missing"))); err == nil {
		t.Fatal("missing target returned success")
	}
}

// EOF belongs to the target's descriptor lifetime, not its process lifetime.
// The real SSH cohort closes stdout while retaining its original child PID.
// origin: hot-update S6 plan §6.3 / T48 / TESTFIX
func exerciseSupervisedStdioEOF(t *testing.T, fd int) {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("printf original >&%d; exec %d>&-; read release", fd, fd))
	var release io.Writer
	if fd == 0 {
		// Keep the target alive on a separate inherited pipe after it closes
		// stdin. The supervisor must not conceal that closure from its writer.
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = read.Close() }()
		defer func() { _ = write.Close() }()
		cmd = exec.Command("/bin/sh", "-c", "exec 0<&-; printf original; exec 1>&-; read release <&3")
		cmd.ExtraFiles = []*os.File{read}
		release = write
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	if release == nil {
		release = input
	}
	var output io.ReadCloser
	if fd != 2 {
		output, err = cmd.StdoutPipe()
	} else {
		output, err = cmd.StderrPipe()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = wait() }()
	identity := supervisionProcessIdentity(cmd.Process.Pid)
	done := make(chan error, 1)
	go func() {
		data, err := io.ReadAll(output)
		if err == nil && string(data) != "original" {
			err = fmt.Errorf("stdio changed: %q", data)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = output.Close()
		<-done
		t.Fatal("supervisor retained the live target's closed stdio endpoint")
	}
	if !supervisionIdentityAlive(identity) {
		t.Fatal("stdio EOF was obtained by terminating the target")
	}
	if fd == 0 {
		if _, err := io.WriteString(input, "closed-input"); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("supervisor retained the live target's closed stdin endpoint: %v", err)
		}
	}
	if _, err := io.WriteString(release, "release\n"); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §9.3 / FIX-ORPHAN identity-safe emergency signals.
func TestProcessSignalRequiresRecordedStartTicks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), FDCount()
	for round := 0; round < 5; round++ {
		exerciseProcessSignalIdentity(t)
	}
	AssertNoGoroutineLeak(t, "recorded process identity x5", beforeG)
	AssertNoFDLeak(t, "recorded process identity x5", beforeFD, 0)
}

func exerciseProcessSignalIdentity(t *testing.T) {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "read release; printf 'identity-alive\\n'; read stop")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	output, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close(); _ = writer.Close() }()
	cmd.Stdout = writer
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	defer func() { _ = cmd.Process.Kill(); _ = wait() }()
	id := supervisionProcessIdentity(cmd.Process.Pid)
	if err := signalFixtureProcess(fixtureProcess{pid: id.PID, ticks: id.Ticks + "-stale"}); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Write([]byte("release\n")); err != nil {
		t.Fatalf("stale start ticks killed an unrelated process incarnation: %v", err)
	}
	if err := output.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var reply [15]byte
	n, err := output.Read(reply[:])
	if err != nil || string(reply[:n]) != "identity-alive\n" || !supervisionIdentityAlive(id) {
		t.Fatal("stale start ticks killed an unrelated process incarnation")
	}
	if err := signalFixtureProcess(fixtureProcess{pid: id.PID, ticks: id.Ticks}); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err == nil || supervisionIdentityAlive(id) {
		t.Fatalf("recorded identity was not killed and joined: %v", err)
	}
}
