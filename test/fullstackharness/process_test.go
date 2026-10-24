//go:build linux

// process_test.go independently checks fatal supervision and remote lease custody.
package fullstackharness

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"unsafe"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const processRole = "_TETHER_FULLSTACK_PROCESS_ROLE"
const processRoot = "_TETHER_FULLSTACK_PROCESS_ROOT"

type execReceipt struct {
	Before, After             ProcessIdentity
	LeaderSignal, AfterSignal int
}

// Fixture dispatch precedes flag parsing. The leader remains pinned so exec
// must run on a different task whose Pdeathsig was cleared by Linux cloning.
func init() {
	role, root := os.Getenv(processRole), os.Getenv(processRoot)
	if role != "before" && role != "after" {
		return
	}
	runtime.LockOSThread()
	if unix.Gettid() != os.Getpid() {
		panic("worker exec fixture did not pin the original leader")
	}
	if role == "before" {
		id, err := processIdentity(os.Getpid())
		mustProcessFixture(err)
		writeProcessReceipt(root, "before", execReceipt{Before: id, LeaderSignal: processDeathSignal()})
		python, err := exec.LookPath("python3")
		mustProcessFixture(err)
		// This adversarial business child deliberately escapes its intermediate
		// parent. Supervising that intermediate would remove the fault premise.
		pid, err := syscall.ForkExec(python, []string{python, "-c", escapedChild, root}, &syscall.ProcAttr{
			Dir: "/", Env: processFixtureEnvironment("", ""), Files: []uintptr{0, 1, 2},
		})
		mustProcessFixture(err)
		var status syscall.WaitStatus
		_, err = syscall.Wait4(pid, &status, 0, nil)
		mustProcessFixture(err)
		if !status.Exited() || status.ExitStatus() != 0 {
			panic("escaped descendant fixture failed")
		}
		go func() {
			runtime.LockOSThread()
			if unix.Gettid() == os.Getpid() || processDeathSignal() != 0 {
				panic("exec fixture requires a worker without Pdeathsig")
			}
			mustProcessFixture(syscall.Exec(os.Args[0], os.Args, processFixtureEnvironment("after", root)))
		}()
	} else {
		var receipt execReceipt
		readProcessReceipt(root, "before", &receipt)
		id, err := processIdentity(os.Getpid())
		mustProcessFixture(err)
		receipt.After, receipt.AfterSignal = id, processDeathSignal()
		writeProcessReceipt(root, "after", receipt)
	}
	<-time.NewTimer(time.Hour).C
	os.Exit(127)
}

// The intermediate waits for CLOEXEC EOF, so the grandchild has actually exec'd
// sleep and dropped cwd/argv references before publishing the owner READY fence.
const escapedChild = `import json, os, sys
r, w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(r)
    os.setsid()
    os.chdir('/')
    base = '/proc/' + str(os.getpid())
    fields = open(base + '/stat').read().rsplit(')', 1)[1].split()
    identity = dict(pid=os.getpid(), start_ticks=int(fields[19]),
                    pid_namespace=os.readlink(base + '/ns/pid'), cgroup=open(base + '/cgroup').read())
    path = sys.argv[1] + '/grandchild.json'
    with open(path + '.tmp', 'w') as out:
        json.dump(identity, out)
    os.rename(path + '.tmp', path)
    null = os.open('/dev/null', os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(null, fd)
    os.close(null)
    os.execv('/bin/sleep', ['sleep', '60'])
os.close(w)
assert os.read(r, 1) == b''
`

func processDeathSignal() int {
	var signal int32
	mustProcessFixture(unix.Prctl(unix.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&signal)), 0, 0, 0))
	return int(signal)
}

func processFixtureEnvironment(role, root string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, processRole+"=") && !strings.HasPrefix(entry, processRoot+"=") &&
			!strings.HasPrefix(entry, "_TETHER_TEST_PROCESS_SUPERVISOR=") && !strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}
	return append(env, processRole+"="+role, processRoot+"="+root, "GORACE=atexit_sleep_ms=0")
}

func mustProcessFixture(err error) {
	if err != nil {
		panic(err)
	}
}

func writeProcessReceipt(root, name string, value any) {
	body, err := json.Marshal(value)
	mustProcessFixture(err)
	path := filepath.Join(root, name+".json")
	mustProcessFixture(os.WriteFile(path+".tmp", body, 0600))
	mustProcessFixture(os.Rename(path+".tmp", path))
}

func readProcessReceipt(root, name string, value any) {
	body, err := os.ReadFile(filepath.Join(root, name+".json"))
	mustProcessFixture(err)
	mustProcessFixture(json.Unmarshal(body, value))
}

// origin: hot-update S10 plan §6 / §10.2 P05 / §11.2 fatal census.
func TestFullstackSupervisorReapsOnOwnerDeath(t *testing.T) {
	if os.Getenv(processRole) == "owner" {
		runSupervisionOwner(t)
		return
	}
	started := time.Now()
	launcher := filepath.Join(t.TempDir(), "supervise")
	build := exec.Command("go", "build", "-o", launcher, "./cmd/supervise")
	if output, err := CombinedOutput(build); err != nil {
		t.Fatalf("launcher build: %v\n%s", err, output)
	}
	for _, artifact := range []string{launcher, os.Args[0]} {
		body, err := os.ReadFile(artifact)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("supervision artifact=%s sha256=%x build_flags_digest=%x", filepath.Base(artifact), sha256.Sum256(body), sha256.Sum256([]byte(os.Getenv("GOFLAGS"))))
	}
	t.Logf("startup/build cost: %s", time.Since(started))
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisedProcess(t, launcher)
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack supervision x5", beforeG)
	testharness.AssertNoFDLeak(t, "fullstack supervision x5", beforeFD, 0)
	t.Logf("five-cycle/fatal census/lease/settle/join cost: %s", time.Since(started))
}

func exerciseSupervisedProcess(t *testing.T, launcher string) {
	t.Helper()
	started := time.Now()
	for _, scenario := range []string{"exit", "panic", "timeout", "sigkill", "launcher-parent"} {
		exerciseFatalOwner(t, launcher, scenario)
	}
	for _, scenario := range []string{"eof", "silence", "pulse", "ready-error", "partial", "invalid-pulse", "cancel"} {
		exerciseRemoteLease(t, launcher, scenario)
	}
	// Ordinary output, original status and cleanup-red are distinct results.
	for _, code := range []int{0, 23, 75} {
		cmd := exec.Command(launcher, "--", "/bin/sh", "-c", "printf 'stdout'; printf 'stderr' >&2; exit \"$1\"", "fixture", strconv.Itoa(code))
		output, err := CombinedOutput(cmd)
		if string(output) != "stdoutstderr" && code == 0 {
			t.Fatalf("launcher changed stdio: %q", output)
		}
		if ProcessExitCode(err) != code {
			t.Fatalf("launcher changed child status: got=%d want=%d output=%s", ProcessExitCode(err), code, output)
		}
	}
	root := t.TempDir()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(launcher, "--", python, "-c", escapedChild, root)
	output, err := CombinedOutput(cmd)
	var leaked ProcessIdentity
	readProcessReceipt(root, "grandchild", &leaked)
	defer emergencyReap(t, []ProcessIdentity{leaked})
	if ProcessExitCode(err) != 125 || !bytes.Contains(output, []byte("process exited with surviving descendants")) {
		t.Fatalf("cleanup 125 was swallowed: rc=%d err=%v output=%s", ProcessExitCode(err), err, output)
	}
	if recordedProcessState(leaked) != "" {
		t.Fatal("cleanup-red descendant was not reaped")
	}
	// Returned waiters are idempotent; their ownership must not be replaced with
	// Cmd.Wait, which waits outside the lease/reaper protocol.
	cmd = exec.Command("/bin/true")
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil || cmd.ProcessState == nil || !cmd.ProcessState.Success() {
		t.Fatal("returned waiter did not join the actual target")
	}
	if _, err := StartProcess(exec.Command(filepath.Join(root, "missing-target"))); err == nil {
		t.Fatal("missing target returned a successful start")
	}
	cmd = exec.Command("/bin/sh", "-c", "cat; printf 'diagnostic' >&2")
	cmd.Stdin = strings.NewReader("original input")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := Output(cmd)
	if err != nil || string(stdout) != "original input" || stderr.String() != "diagnostic" {
		t.Fatalf("stdout adapter changed command I/O: %q %q %v", stdout, stderr.String(), err)
	}
	t.Logf("exercise cost: %s", time.Since(started))
}

func runSupervisionOwner(t *testing.T) {
	root := os.Getenv(processRoot)
	var config struct{ Scenario, Launcher string }
	readProcessReceipt(root, "config", &config)
	var cmd *exec.Cmd
	if config.Scenario == "launcher-parent" {
		// This is the victim launcher whose parent is about to die. It cannot be
		// supervised by that parent, which would mask a broken parent deathwatch.
		cmd = exec.Command(config.Launcher, "--ready-fd=3", "--", os.Args[0], "-test.run=^TestFullstackSupervisorReapsOnOwnerDeath$", "-test.count=1")
		ready, err := os.Create(filepath.Join(root, "launcher-ready"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ready.Close() }()
		cmd.ExtraFiles = []*os.File{ready}
		cmd.Env = processFixtureEnvironment("before", root)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
	} else {
		cmd = exec.Command(os.Args[0], "-test.run=^TestFullstackSupervisorReapsOnOwnerDeath$", "-test.count=1")
		cmd.Env = processFixtureEnvironment("before", root)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		wait, err := StartProcess(cmd)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cmd.Process.Kill(); _ = wait() }()
	}
	if !pollProcess(2*time.Second, func() bool { _, err := os.Stat(filepath.Join(root, "after.json")); return err == nil }) {
		t.Fatal("worker exec did not reach its nonempty fence")
	}
	if config.Scenario == "launcher-parent" {
		writeProcessReceipt(root, "launcher", mustProcessIdentity(cmd.Process.Pid))
	}
	var receipt execReceipt
	readProcessReceipt(root, "after", &receipt)
	writeProcessReceipt(root, "supervisor", mustProcessIdentity(processParent(receipt.After.PID)))
	writeProcessReceipt(root, "owner-ready", config.Scenario)
	if config.Scenario == "timeout" {
		<-time.NewTimer(time.Hour).C
	}
	if !pollProcess(5*time.Second, func() bool { _, err := os.Stat(filepath.Join(root, "release")); return err == nil }) {
		t.Fatal("owner release fence missing")
	}
	if config.Scenario == "panic" {
		// An unrelated goroutine panic bypasses the owner's returned waiter and
		// testing cleanup; a recovered tRunner panic would exercise normal teardown.
		go func() { panic("fullstack supervision owner panic") }()
		<-time.NewTimer(time.Hour).C
	}
	os.Exit(0) // Deliberately bypass Cleanup and the returned waiter.
}

func exerciseFatalOwner(t *testing.T, launcher, scenario string) {
	t.Helper()
	root := t.TempDir()
	writeProcessReceipt(root, "config", struct{ Scenario, Launcher string }{scenario, launcher})
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFullstackSupervisorReapsOnOwnerDeath$", "-test.count=1", "-test.timeout=3s")
	cmd.Env = processFixtureEnvironment("owner", root)
	output, err := os.Create(filepath.Join(root, "owner-output"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	cmd.Stdout, cmd.Stderr = output, output
	// Only the faulted owner is raw, as in shared FIX-ORPHAN's receiver. An outer
	// supervisor would repair a broken inner reaper and invalidate this oracle.
	// Its waiter and exact adopted census remain owned by this test.
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	var identities []ProcessIdentity
	defer func() {
		_ = cmd.Process.Kill()
		if !joined {
			<-done
		}
		emergencyReap(t, identities)
	}()
	if !pollProcess(2*time.Second, func() bool { _, err := os.Stat(filepath.Join(root, "owner-ready.json")); return err == nil }) {
		_ = cmd.Process.Kill()
		<-done
		joined = true
		body, _ := os.ReadFile(output.Name())
		t.Fatalf("owner did not publish its tree: %s", body)
	}
	var receipt execReceipt
	readProcessReceipt(root, "after", &receipt)
	if receipt.Before != receipt.After || receipt.AfterSignal != 0 ||
		(scenario != "launcher-parent" && receipt.LeaderSignal != int(syscall.SIGKILL)) {
		t.Fatalf("worker exec did not demonstrate the same identity and lost Pdeathsig: %+v", receipt)
	}
	identities = append(identities, receipt.After)
	for _, name := range []string{"grandchild", "supervisor"} {
		var id ProcessIdentity
		readProcessReceipt(root, name, &id)
		identities = append(identities, id)
	}
	if scenario == "launcher-parent" {
		var id ProcessIdentity
		readProcessReceipt(root, "launcher", &id)
		identities = append(identities, id)
	}
	for _, id := range identities {
		if state := recordedProcessState(id); state == "" || state == "Z" || state == "X" {
			t.Fatalf("fatal census lacks a live pre-fault member: %+v state=%s", id, state)
		}
	}
	if scenario == "sigkill" || scenario == "launcher-parent" {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
	} else if scenario != "timeout" {
		if err := os.WriteFile(filepath.Join(root, "release"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var ownerErr error
	select {
	case ownerErr = <-done:
		joined = true
	case <-time.After(5 * time.Second):
		t.Fatal("faulted owner did not exit")
	}
	ownerOutput, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	if (scenario == "exit") != (ownerErr == nil) {
		t.Fatalf("owner fault returned wrong status: scenario=%s err=%v output=%s", scenario, ownerErr, ownerOutput)
	}
	if scenario == "panic" && !bytes.Contains(ownerOutput, []byte("panic: fullstack supervision owner panic")) {
		t.Fatal("owner panic was not reached")
	}
	if scenario == "timeout" && !bytes.Contains(ownerOutput, []byte("panic: test timed out after 3s")) {
		t.Fatal("owner testing timeout was not reached")
	}
	assertReapedCensus(t, scenario, identities, true)
	t.Logf("fatal census scenario=%s owner_rc=%d identities=%s", scenario, ProcessExitCode(ownerErr), mustProcessJSON(identities))
}

func exerciseRemoteLease(t *testing.T, launcher, scenario string) {
	t.Helper()
	root := t.TempDir()
	leaseR, leaseW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = leaseR.Close(); _ = leaseW.Close() }()
	ready, err := os.Create(filepath.Join(root, "ready"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ready.Close() }()
	self := mustProcessIdentity(os.Getpid())
	cgroup := strings.SplitN(strings.Split(strings.TrimSpace(self.Cgroup), "\n")[0], ":", 3)[2]
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := exec.CommandContext(ctx, launcher, "--lease-fd=3", "--ready-fd=4", "--lease-timeout=400ms", "--cgroup="+cgroup, "--", os.Args[0], "-test.run=^TestFullstackSupervisorReapsOnOwnerDeath$", "-test.count=1")
	cmd.Env = processFixtureEnvironment("before", root)
	cmd.ExtraFiles = []*os.File{leaseR, ready}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if scenario == "partial" {
		cmd.Args[4] = "--cgroup=/missing-fullstack-registration"
	}
	if scenario == "ready-error" {
		readOnly, err := os.Open(ready.Name())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = readOnly.Close() }()
		cmd.ExtraFiles[1] = readOnly
	}
	pulse := byte('.')
	if scenario == "invalid-pulse" {
		pulse = '?'
	}
	if _, err := leaseW.Write([]byte{pulse}); err != nil {
		t.Fatal(err)
	}
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = leaseR.Close()
	defer func() { cancel(); _ = wait() }()
	if scenario == "partial" || scenario == "ready-error" || scenario == "invalid-pulse" {
		err := wait()
		if err == nil {
			t.Fatal("partial launcher startup succeeded")
		}
		if scenario == "invalid-pulse" && ProcessExitCode(err) != 126 {
			t.Fatalf("invalid initial lease pulse reached a target: rc=%d", ProcessExitCode(err))
		}
		if scenario == "partial" || scenario == "invalid-pulse" {
			if _, err := os.Stat(filepath.Join(root, "before.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unregistered remote child started before refusal")
			}
		}
		return
	}
	if !pollProcess(2*time.Second, func() bool { _, err := os.Stat(filepath.Join(root, "after.json")); return err == nil }) {
		cancel()
		_ = wait()
		t.Fatalf("remote child did not enter worker exec: scenario=%s output=%s", scenario, output.String())
	}
	var receipt execReceipt
	readProcessReceipt(root, "after", &receipt)
	var registered ProcessReady
	body, err := os.ReadFile(ready.Name())
	if err != nil || json.Unmarshal(body, &registered) != nil || registered.Target != receipt.After {
		t.Fatalf("remote launcher READY does not bind target identity: %q %v", body, err)
	}
	// Check the actual child's FD table while it is alive. Exit cleanup would
	// conceal a leaked private control descriptor.
	// origin: hot-update S10 plan §6 / §8 M11K; P05 lease/READY custody.
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", receipt.After.PID))
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range []*os.File{leaseW, ready} {
		controlInfo, err := control.Stat()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			childInfo, err := os.Stat(fmt.Sprintf("/proc/%d/fd/%s", receipt.After.PID, entry.Name()))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err == nil && os.SameFile(controlInfo, childInfo) {
				t.Errorf("remote child inherited private lease/READY descriptor: fd=%s", entry.Name())
			}
		}
	}
	var grandchild ProcessIdentity
	readProcessReceipt(root, "grandchild", &grandchild)
	identities := []ProcessIdentity{receipt.After, grandchild, mustProcessIdentity(processParent(receipt.After.PID))}
	defer emergencyReap(t, identities)
	if scenario == "pulse" {
		// Cross two original deadlines; fresh pulses, rather than an enlarged TTL,
		// keep the original child alive. Each pulse is externally fenced by census.
		for pulse := 0; pulse < 6; pulse++ {
			time.Sleep(150 * time.Millisecond)
			if _, err := leaseW.Write([]byte(".")); err != nil || recordedProcessState(receipt.After) == "" {
				t.Fatalf("valid remote pulse retired original child: %v", err)
			}
		}
	}
	switch scenario {
	case "eof", "pulse":
		_ = leaseW.Close()
	case "cancel":
		cancel()
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("remote owner loss returned success")
		}
	case <-time.After(2 * time.Second):
		// Capture assertion-red before emergency cancellation, so a mutation
		// cannot be credited merely for reaching the package watchdog.
		for _, id := range identities {
			if state := recordedProcessState(id); state != "" {
				t.Errorf("remote child survived owner lease loss: scenario=%s identity=%s state=%s", scenario, mustProcessJSON(id), state)
			}
		}
		cancel()
		emergencyReap(t, identities)
		<-done
	}
	assertReapedCensus(t, "remote-"+scenario, identities, false)
	t.Logf("remote census scenario=%s identities=%s", scenario, mustProcessJSON(identities))
}

func mustProcessIdentity(pid int) ProcessIdentity {
	id, err := processIdentity(pid)
	mustProcessFixture(err)
	return id
}

func mustProcessJSON(value any) string {
	body, err := json.Marshal(value)
	mustProcessFixture(err)
	return string(body)
}

func processParent(pid int) int {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	mustProcessFixture(err)
	fields := strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])
	parent, err := strconv.Atoi(fields[1])
	mustProcessFixture(err)
	return parent
}

// Zombies remain census members until the exact responsible owner has reaped
// them. A zero live-process count is insufficient, including after emergency kill.
func recordedProcessState(id ProcessIdentity) string {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", id.PID))
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
		return ""
	}
	mustProcessFixture(err)
	fields := strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])
	if fields[19] != strconv.FormatUint(id.StartTicks, 10) {
		return ""
	}
	return fields[0]
}

func assertReapedCensus(t *testing.T, scenario string, identities []ProcessIdentity, adopted bool) {
	t.Helper()
	if !pollProcess(3*time.Second, func() bool {
		if adopted {
			for _, id := range identities {
				_, _ = unix.Wait4(id.PID, nil, unix.WNOHANG, nil)
			}
		}
		for _, id := range identities {
			if recordedProcessState(id) != "" {
				return false
			}
		}
		return true
	}) {
		for _, id := range identities {
			if state := recordedProcessState(id); state != "" {
				t.Errorf("surviving descendant after %s: identity=%s state=%s", scenario, mustProcessJSON(id), state)
			}
		}
	}
}

func emergencyReap(t *testing.T, identities []ProcessIdentity) {
	t.Helper()
	for _, id := range identities {
		if recordedProcessState(id) == "" {
			continue
		}
		fd, err := unix.PidfdOpen(id.PID, 0)
		if err != nil {
			continue
		}
		// Revalidate after opening the pidfd so a recycled PID is never signalled.
		if recordedProcessState(id) != "" {
			_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
		}
		_ = unix.Close(fd)
	}
	assertReapedCensus(t, "emergency cleanup (prior failure retained)", identities, true)
}

func pollProcess(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}
