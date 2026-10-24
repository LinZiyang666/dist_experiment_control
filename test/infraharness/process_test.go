//go:build linux

package infraharness

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

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const harnessRole = "_TETHER_INFRA_HARNESS_ROLE"
const harnessRoot = "_TETHER_INFRA_HARNESS_ROOT"

// Only fixture actors dispatch in init. The shared supervisor's dependency init
// runs first and consumes its own mode; these keys never replace that dispatch.
func init() {
	role := os.Getenv(harnessRole)
	if role != "target" && role != "escaped" && role != "orphaner" {
		return
	}
	root := os.Getenv(harnessRoot)
	if role == "orphaner" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestInfraSupervisorReapsAfterTestParentDeath$", "-test.count=1")
		cmd.Env, cmd.Dir = actorEnvironment("escaped", root), "/"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		os.Exit(0) // Adopted by the shared subreaper while the target is still live.
	}
	if role == "target" {
		id, err := ReadIdentity(os.Getppid())
		if err != nil {
			panic(err)
		}
		writeActorReceipt(root, "supervisor", id)
		// This unregistered spawn is the adversarial escaped descendant, not a
		// fixture launcher. Its death/reap must be performed by FIX-ORPHAN.
		cmd := exec.Command(os.Args[0], "-test.run=^TestInfraSupervisorReapsAfterTestParentDeath$", "-test.count=1")
		cmd.Env = actorEnvironment("orphaner", root)
		cmd.Dir = "/"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Run(); err != nil {
			panic(err)
		}
	}
	id, err := ReadIdentity(os.Getpid())
	if err != nil {
		panic(err)
	}
	writeActorReceipt(root, role, id)
	<-time.NewTimer(time.Hour).C // Real supervisor kill, never fixture teardown.
	os.Exit(127)
}

func actorEnvironment(role, root string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, harnessRole+"=") && !strings.HasPrefix(entry, harnessRoot+"=") &&
			!strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}
	return append(env, harnessRole+"="+role, harnessRoot+"="+root, "GORACE=atexit_sleep_ms=0")
}

func writeActorReceipt(root, name string, value Identity) {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	file := filepath.Join(root, name+".json")
	if err := os.WriteFile(file+".tmp", body, 0600); err != nil {
		panic(err)
	}
	if err := os.Rename(file+".tmp", file); err != nil {
		panic(err)
	}
}

// origin: hot-update S9 plan §9.5 / T57 / M29
func TestInfraSupervisorReapsAfterTestParentDeath(t *testing.T) {
	if role := os.Getenv(harnessRole); strings.HasPrefix(role, "owner-") {
		runVictim(t, strings.TrimPrefix(role, "owner-"), os.Getenv(harnessRoot))
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseParentDeath(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra parent death x5", beforeG)
	testharness.AssertNoFDLeak(t, "infra parent death x5", beforeFD, 0)
}

func runVictim(t *testing.T, scenario, root string) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestInfraSupervisorReapsAfterTestParentDeath$", "-test.count=1")
	cmd.Env, cmd.Dir = actorEnvironment("target", root), root
	// No Pdeathsig: this premise specifically rejects the insufficient fallback.
	p, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	writeActorReceipt(root, "returned", p.Identity())
	if scenario == "panic" {
		waitFile(t, filepath.Join(root, "release"))
		panic("infra test parent panic")
	}
	select {} // SIGKILL or testing's real package timeout bypasses Cleanup.
}

func exerciseParentDeath(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"kill", "timeout", "panic", "start"} {
		t.Run(scenario, func(t *testing.T) { exerciseFatalParent(t, scenario) })
	}
}

func exerciseFatalParent(t *testing.T, scenario string) {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	// The oracle adopts the private reaper after killing the test victim, but
	// waits only recorded PIDs. It must not consume other fixtures' wait status.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestInfraSupervisorReapsAfterTestParentDeath$", "-test.count=1", "-test.timeout=10s")
	if scenario == "timeout" {
		cmd.Args[len(cmd.Args)-1] = "-test.timeout=2s"
	}
	cmd.Env, cmd.Dir = actorEnvironment("owner-"+scenario, root), root
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	// This raw Start is the victim test process, deliberately outside the
	// fixture supervisor: supervising it would conceal an inner orphan defect.
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	var supervisor Identity
	var ids []Identity
	defer func() {
		_ = cmd.Process.Kill()
		if supervisor.PID != 0 {
			signalActor(t, supervisor, unix.SIGCONT)
		}
		if !joined {
			<-done
		}
		// An early assertion may precede receipt collection. Read every actor
		// here as well so an assertion-red mutant cannot leak its own fixtures.
		for _, name := range []string{"target", "escaped"} {
			if _, err := os.Stat(filepath.Join(root, name+".json")); err == nil {
				ids = append(ids, readActor(t, root, name))
			}
		}
		// Emergency cleanup occurs after the assertions and never converts a
		// survivor into success. Mutant descendants are adopted by this oracle.
		for _, id := range ids {
			_ = killIdentity(id)
		}
		if supervisor.PID != 0 {
			_ = killIdentity(supervisor)
			_, _ = unix.Wait4(supervisor.PID, nil, 0, nil)
		}
		for _, id := range ids {
			_, _ = unix.Wait4(id.PID, nil, 0, nil)
		}
	}()
	if scenario == "start" {
		// Freeze the shared helper as soon as it is forked, before target-ready.
		// Race instrumentation makes init nontrivial, but the receipt check is
		// the premise: a completed handshake is never counted as this window.
		if !testharness.WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
			for _, pid := range actorChildren(cmd.Process.Pid) {
				id, err := ReadIdentity(pid)
				if err == nil {
					supervisor = id
					return true
				}
			}
			return false
		}) {
			t.Fatal("startup supervisor was not forked")
		}
		signalActor(t, supervisor, unix.SIGSTOP)
		if _, err := os.Stat(filepath.Join(root, "returned.json")); !os.IsNotExist(err) {
			t.Fatal("startup fault missed the pre-handshake window")
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		signalActor(t, supervisor, unix.SIGCONT)
	} else {
		for _, name := range []string{"returned", "target", "escaped", "supervisor"} {
			waitFile(t, filepath.Join(root, name+".json"))
			id := readActor(t, root, name)
			if name == "supervisor" {
				supervisor = id
			} else if name != "returned" {
				ids = append(ids, id)
			}
		}
		for _, id := range append(append([]Identity{}, ids...), supervisor) {
			exists, err := ProcessExists(id)
			if err != nil || !exists || !actorLive(id) {
				t.Fatalf("pre-fault actor not present: %+v %v", id, err)
			}
		}
		// The intermediate has already exited. This adopted, session-detached
		// grandchild has no surviving original parent or process-group relation.
		escaped := readActor(t, root, "escaped")
		body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", escaped.PID))
		if err != nil {
			t.Fatal(err)
		}
		fields := strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])
		parent, err := strconv.Atoi(fields[1])
		session, sessionErr := unix.Getsid(escaped.PID)
		if err != nil || sessionErr != nil || parent != supervisor.PID || session != escaped.PID {
			t.Error("escaped orphan was not adopted by the live shared subreaper")
		}
		if scenario == "kill" {
			_ = cmd.Process.Kill()
		}
		if scenario == "panic" {
			if err := os.WriteFile(filepath.Join(root, "release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("fatal test victim returned success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test victim failed to exit")
	}
	if scenario == "timeout" && !strings.Contains(output.String(), "panic: test timed out after 2s") {
		t.Fatalf("package timeout was not the injected failure: %s", output.String())
	}
	if scenario == "panic" && !strings.Contains(output.String(), "panic: infra test parent panic") {
		t.Fatalf("panic was not the injected failure: %s", output.String())
	}
	// Reap the supervisor after its tree is empty; zombie descendants must
	// already be gone before this oracle performs any emergency wait/kill.
	if !testharness.WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		n, err := unix.Wait4(supervisor.PID, nil, unix.WNOHANG, nil)
		return n == supervisor.PID || errors.Is(err, unix.ECHILD)
	}) {
		t.Error("shared supervisor failed to reap after parent death")
	}
	if scenario == "start" {
		for _, name := range []string{"target", "escaped"} {
			if _, err := os.Stat(filepath.Join(root, name+".json")); err == nil {
				ids = append(ids, readActor(t, root, name))
			}
		}
	}
	var watch Deathwatch
	for _, id := range append(append([]Identity{}, ids...), supervisor) {
		if err := watch.Register(id, Scope{}); err != nil {
			t.Fatal(err)
		}
	}
	censusErr := watch.CheckReaped()
	if censusErr != nil {
		t.Errorf("zero-descendant oracle: %v", censusErr)
	}
	t.Logf("%s: original victim PID=%d, census_error=%v, original incarnations=%+v", scenario, cmd.Process.Pid, censusErr, watch.Snapshot())
}

func actorLive(id Identity) bool {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", id.PID))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])
	return len(fields) >= 20 && fields[0] != "Z" && fields[0] != "X"
}

func actorChildren(pid int) []int {
	threads, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	var children []int
	for _, thread := range threads {
		body, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", pid, thread.Name()))
		for _, value := range strings.Fields(string(body)) {
			child, err := strconv.Atoi(value)
			if err == nil {
				children = append(children, child)
			}
		}
	}
	return children
}

func signalActor(t *testing.T, id Identity, signal unix.Signal) {
	t.Helper()
	fd, err := unix.PidfdOpen(id.PID, 0)
	if errors.Is(err, unix.ESRCH) {
		return
	}
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = unix.Close(fd) }()
	if exists, err := ProcessExists(id); err == nil && exists {
		if err := unix.PidfdSendSignal(fd, signal, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			t.Error(err)
		}
	}
}

func readActor(t *testing.T, root, name string) Identity {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var id Identity
	if err := json.Unmarshal(body, &id); err != nil {
		t.Fatal(err)
	}
	return id
}

func waitFile(t *testing.T, file string) {
	t.Helper()
	if !testharness.WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		_, err := os.Stat(file)
		return err == nil
	}) {
		t.Fatalf("missing actor receipt: %s", filepath.Base(file))
	}
}

// origin: hot-update S9 plan §12 P27 / lifecycle resource row
func TestInfraHarnessLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseHarness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra harness lifecycle x5", beforeG)
	testharness.AssertNoFDLeak(t, "infra harness lifecycle x5", beforeFD, 0)
}

func exerciseHarness(t *testing.T) {
	t.Helper()
	for _, code := range []string{"0", "23", "75"} {
		cmd := exec.Command("/bin/sh", "-c", "exit \"$1\"", "fixture", code)
		p, err := StartProcess(cmd)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = killIdentity(p.Identity()); _ = p.wait() }()
		err = p.Wait()
		if cmd.ProcessState == nil || strconv.Itoa(cmd.ProcessState.ExitCode()) != code || (err == nil) != (code == "0") {
			t.Fatalf("native result changed: rc=%s err=%v", code, err)
		}
		if next := p.Wait(); next != err {
			t.Fatal("repeated Wait changed result")
		}
		if exists, err := ProcessExists(p.Identity()); err != nil || exists {
			t.Fatal("Wait did not reap target")
		}
	}
	if _, err := StartProcess(exec.Command(filepath.Join(t.TempDir(), "missing"))); err == nil {
		t.Fatal("missing binary succeeded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "read release; printf 'identity-alive\\n'; read stop")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	reply, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reply.Close(); _ = writer.Close() }()
	cmd.Stdout = writer
	p, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = killIdentity(p.Identity()); _ = p.wait() }()
	_ = writer.Close()
	stale := p.Identity()
	stale.StartTicks++
	if err := killIdentity(stale); err != nil {
		t.Fatal(err)
	}
	// Existence alone would accept a killed-but-unreaped zombie. Demand an
	// original target's post-signal response before considering it still live.
	if _, err := in.Write([]byte("release\n")); err != nil {
		t.Fatal("stale identity killed live target")
	}
	if err := reply.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var response [15]byte
	if _, err := io.ReadFull(reply, response[:]); err != nil || string(response[:]) != "identity-alive\n" {
		t.Fatal("stale identity killed live target before original response")
	}
	var watch Deathwatch
	if err := watch.CheckReaped(); err == nil {
		t.Fatal("empty deathwatch signed zero")
	}
	scope := Scope{Unit: "infra-test.service", Cgroup: "/sandbox/infra-test"}
	if err := watch.Register(p.Identity(), scope); err != nil {
		t.Fatal(err)
	}
	if err := watch.Register(p.Identity(), Scope{}); err == nil {
		t.Fatal("scope conflict overwritten")
	}
	if err := watch.Register(Identity{}, Scope{}); err == nil {
		t.Fatal("empty incarnation accepted")
	}
	if err := watch.Register(stale, Scope{Unit: "bad.service", Cgroup: "/"}); err == nil {
		t.Fatal("host cgroup accepted")
	}
	copy := watch.Snapshot()
	delete(copy, p.Identity())
	if len(watch.Snapshot()) != 1 {
		t.Fatal("snapshot aliases custody")
	}
	if err := watch.CheckReaped(); err == nil {
		t.Fatal("live target signed as reaped")
	}
	cancel()
	if err := p.Wait(); err == nil {
		t.Fatal("canceled target succeeded")
	}
	if err := watch.CheckReaped(); err != nil {
		t.Fatal(err)
	}
	if _, err := ProcessExists(Identity{}); err == nil {
		t.Fatal("unknown incarnation signed as absent")
	}
	cmd = exec.Command("/bin/sh", "-c", "read stop")
	stopInput, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stopInput.Close() }()
	stopped, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = killIdentity(stopped.Identity()); _ = stopped.wait() }()
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", stopped.Identity().PID))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(body)[strings.LastIndexByte(string(body), ')')+1:])
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	reaper, err := ReadIdentity(parent)
	if err != nil {
		t.Fatal(err)
	}
	// Hold the shared helper briefly to independently observe a zombie. A
	// live-process-only census must fail before the helper can conceal it.
	signalActor(t, reaper, unix.SIGSTOP)
	defer signalActor(t, reaper, unix.SIGCONT)
	if err := killIdentity(stopped.Identity()); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 2*time.Second, time.Millisecond, func() bool { return !actorLive(stopped.Identity()) }) {
		t.Fatal("zombie control remained live")
	}
	var zombieWatch Deathwatch
	if err := zombieWatch.Register(stopped.Identity(), Scope{}); err != nil {
		t.Fatal(err)
	}
	if err := zombieWatch.CheckReaped(); err == nil {
		t.Fatal("zombie was signed as reaped before Wait")
	}
	started := time.Now()
	stopErr := stopProcess(stopped, 20*time.Millisecond)
	if stopErr == nil || !strings.Contains(stopErr.Error(), "join deadline exceeded") || time.Since(started) > time.Second {
		t.Fatal("stalled shared reaper did not enforce bounded Stop")
	}
	signalActor(t, reaper, unix.SIGCONT)
	_ = stopped.Wait() // Join the retained reaper after the real deadline error.
	if stopped.Stop() != stopErr {
		t.Fatal("repeated Stop changed native result")
	}
	if cmd.ProcessState == nil {
		t.Fatal("Stop omitted native Wait")
	}
	if exists, err := ProcessExists(stopped.Identity()); err != nil || exists {
		t.Fatal("Stop left an unreaped incarnation")
	}
}
