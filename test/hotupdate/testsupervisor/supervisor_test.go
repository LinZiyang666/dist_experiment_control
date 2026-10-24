package testsupervisor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const fixtureRole = "_TETHER_S8_SUPERVISION_ROLE"
const fixtureRoot = "_TETHER_S8_SUPERVISION_ROOT"

// The workload deliberately forks outside the fixture launcher: these are the
// uncontrolled product descendants the supervisor must own, including after
// both intermediate parents exit and the leaf escapes the original group.
// origin: hot-update S8 plan §9.1 / P21 / M21
func init() {
	role, root := os.Getenv(fixtureRole), os.Getenv(fixtureRoot)
	if role != "tree" && role != "middle" && role != "leaf" {
		return
	}
	if role == "leaf" {
		id, _, err := processIdentity(os.Getpid())
		if err != nil {
			panic(err)
		}
		writeFixture(filepath.Join(root, "leaf.json"), id)
		_ = os.Chdir("/")
		_ = os.Unsetenv(fixtureRole)
		_ = os.Unsetenv(fixtureRoot)
		<-time.NewTimer(time.Hour).C
		os.Exit(127)
	}
	childRole := "middle"
	if role == "middle" {
		childRole = "leaf"
	}
	cmd := exec.Command(os.Args[0])
	cmd.Env, cmd.Dir, cmd.SysProcAttr = fixtureEnvironment(childRole, root), "/", fixtureAttributes(true)
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	if role == "middle" {
		// Confirm the leaf was born before the double-fork parent exits. This
		// is a receipt barrier, not a delay that guesses fork scheduling.
		if !fixtureUntil(3*time.Second, func() bool { return fixturePresent(root, "leaf.json") }) {
			panic("escaped leaf has no identity receipt")
		}
		os.Exit(0)
	}
	if err := cmd.Wait(); err != nil {
		panic(err)
	}
	id, parent, err := processIdentity(os.Getpid())
	if err != nil {
		panic(err)
	}
	supervisor, _, err := processIdentity(parent)
	if err != nil {
		panic(err)
	}
	writeFixture(filepath.Join(root, "tree.json"), []identity{id, supervisor})
	<-time.NewTimer(time.Hour).C
	os.Exit(127)
}

func fixtureEnvironment(role, root string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, fixtureRole+"=") && !strings.HasPrefix(entry, fixtureRoot+"=") && !strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}
	return append(env, fixtureRole+"="+role, fixtureRoot+"="+root, "GORACE=atexit_sleep_ms=0")
}

func writeFixture(path string, value any) {
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

func fixturePresent(root, name string) bool {
	_, err := os.Stat(filepath.Join(root, name))
	return err == nil
}

func fixtureUntil(budget time.Duration, predicate func() bool) bool {
	deadline := time.Now().Add(budget)
	for {
		if predicate() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond) // Poll a real receipt/exit, never assume readiness from elapsed time.
	}
}

// This receiver is a real test runner: testing's alarm and an unhandled panic
// bypass Cleanup. Its fixture is launched by the shared entry point.
// origin: hot-update S8 plan §9.1 / TestChildSupervisorSurvivesTestTimeout
func TestChildSupervisorReceiver(t *testing.T) {
	role, root := os.Getenv(fixtureRole), os.Getenv(fixtureRoot)
	if !strings.HasPrefix(role, "owner-") {
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0])
	cmd.Env, cmd.SysProcAttr = fixtureEnvironment("tree", root), fixtureAttributes(false)
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = wait() })
	if !fixtureUntil(3*time.Second, func() bool { return fixturePresent(root, "tree.json") }) {
		t.Fatal("supervised tree did not reach its identity barrier")
	}
	writeFixture(filepath.Join(root, "ready.json"), cmd.Process.Pid)
	if role != "owner-timeout" && !fixtureUntil(5*time.Second, func() bool { return fixturePresent(root, "release") }) {
		t.Fatal("fault release barrier missing")
	}
	switch role {
	case "owner-panic":
		go func() { panic("supervision owner fault") }()
	case "owner-exit":
		os.Exit(0)
	}
	<-time.NewTimer(time.Hour).C
}

// Killing only the direct child or its process group leaves the double-forked
// setsid leaf alive. Each fault is exercised five times and all recorded kernel
// identities must disappear, including the external supervisor's zombie.
// origin: hot-update S8 plan §9.1 / P21 / M21
func TestChildSupervisorSurvivesTestTimeout(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for _, scenario := range []string{"timeout", "sigkill", "panic", "exit"} {
		t.Run(scenario, func(t *testing.T) {
			for round := 0; round < 5; round++ {
				exerciseChildSupervisor(t, scenario)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "fatal owner supervision x5", beforeG)
	testharness.AssertNoFDLeak(t, "fatal owner supervision x5", beforeFD, 0)
}

func exerciseChildSupervisor(t *testing.T, scenario string) {
	t.Helper()
	restore, err := adoptTestOrphans()
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	root := t.TempDir()
	log, err := os.Create(filepath.Join(root, "owner.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	beforeRoundFD := testharness.FDCount()
	cmd := exec.Command(os.Args[0], "-test.run=^TestChildSupervisorReceiver$", "-test.count=1", "-test.timeout=8s")
	if scenario == "timeout" {
		cmd.Args[len(cmd.Args)-1] = "-test.timeout=1s"
	}
	cmd.Env, cmd.Stdout, cmd.Stderr = fixtureEnvironment("owner-"+scenario, root), log, log
	// Keep the outer guard alive until AFTER the survivor assertion. Its shared
	// supervisor protects fatal outer-runner exits without repairing the inner
	// tree under observation.
	// origin: hot-update S8 plan §9.1 / P21 / L21
	waitOwner, closeGuard, err := startRetainedRunner(t.Context(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- waitOwner() }()
	var ownerErr error
	joined := false
	readIDs := func() []identity {
		var ids []identity
		if body, err := os.ReadFile(filepath.Join(root, "tree.json")); err == nil {
			_ = json.Unmarshal(body, &ids)
		}
		if body, err := os.ReadFile(filepath.Join(root, "leaf.json")); err == nil {
			var leaf identity
			if json.Unmarshal(body, &leaf) == nil {
				ids = append(ids, leaf)
			}
		}
		return ids
	}
	defer func() {
		if !joined {
			_ = cmd.Process.Kill()
			<-done
		}
		// Release only after the oracle has run. In a mutation run this is an
		// emergency cleanup, never evidence that the inner supervisor worked.
		_ = closeGuard()
		ids := readIDs()
		for _, id := range ids {
			if id.PID != cmd.Process.Pid {
				if err := killIdentity(id); err != nil {
					t.Errorf("emergency identity signal: %v", err)
				}
			}
		}
		if !fixtureUntil(3*time.Second, func() bool {
			empty := true
			for _, id := range ids {
				if id.PID != cmd.Process.Pid {
					reapIdentity(id)
					empty = empty && !identityExists(id)
				}
			}
			return empty
		}) {
			t.Error("emergency cleanup did not reap recorded process identities")
		}
		// Keep the caller's Process reference alive through the FD observation.
		// A finalizer must not conceal failure to release its retained pidfd.
		afterRoundFD := testharness.FDCount()
		runtime.KeepAlive(cmd)
		if beforeRoundFD >= 0 && afterRoundFD > beforeRoundFD {
			t.Errorf("retained guard leaked descriptors: before=%d after=%d", beforeRoundFD, afterRoundFD)
		}
	}()
	if !fixtureUntil(4*time.Second, func() bool { return fixturePresent(root, "ready.json") }) {
		t.Fatal("fault runner did not publish a supervised tree")
	}
	ids := readIDs()
	if len(ids) != 3 || ids[0].PID != readFixturePID(t, root) || ids[0] == ids[1] || ids[0] == ids[2] {
		t.Fatalf("incomplete or aliased original identity census: %+v", ids)
	}
	for _, id := range ids {
		if id.PID <= 0 || id.Start == 0 || !identityExists(id) {
			t.Fatalf("missing live pre-fault process identity: %+v", id)
		}
	}
	leafSession, err := unix.Getsid(ids[2].PID)
	if err != nil || leafSession != ids[2].PID {
		t.Fatalf("original leaf did not escape the target's session: sid=%d error=%v", leafSession, err)
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
	select {
	case ownerErr = <-done:
		joined = true
	case <-time.After(5 * time.Second):
		t.Fatal("fault runner did not terminate")
	}
	output, err := os.ReadFile(filepath.Join(root, "owner.log"))
	if err != nil {
		t.Fatal(err)
	}
	if scenario == "exit" && ownerErr != nil || scenario != "exit" && ownerErr == nil {
		t.Fatalf("fault runner exit does not match %s: %v\n%s", scenario, ownerErr, output)
	}
	if scenario == "timeout" && !bytes.Contains(output, []byte("panic: test timed out after 1s")) {
		t.Fatalf("testing timeout was not reached: %s", output)
	}
	if scenario == "panic" && !bytes.Contains(output, []byte("panic: supervision owner fault")) {
		t.Fatalf("injected unhandled panic was not reached: %s", output)
	}
	// Only reap the supervisor after it exits. No signal/cleanup runs here, so
	// a defect cannot pass by borrowing this test's emergency cleanup.
	if !fixtureUntil(3*time.Second, func() bool {
		reapIdentity(ids[1])
		return !identityExists(ids[0]) && !identityExists(ids[1]) && !identityExists(ids[2])
	}) {
		t.Fatalf("supervised descendant survived owner %s: %+v", scenario, ids)
	}
}

func readFixturePID(t *testing.T, root string) int {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, "ready.json"))
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := json.Unmarshal(body, &pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

// A context deadline has to join the entire tree, and a start/exit error must
// stay an error. The same waiter can be joined concurrently without a second
// waitpid or a second close of the lifeline.
// origin: hot-update S8 plan §9.1 / P21
func TestChildSupervisorJoinsCancelledAndFailedCommands(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseChildSupervisorCommands(t)
	}
	testharness.AssertNoGoroutineLeak(t, "cancelled and failed command supervision x5", beforeG)
	testharness.AssertNoFDLeak(t, "cancelled and failed command supervision x5", beforeFD, 0)
}

func exerciseChildSupervisorCommands(t *testing.T) {
	t.Helper()
	restore, err := adoptTestOrphans()
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	root := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0])
	cmd.Env, cmd.SysProcAttr, cmd.WaitDelay = fixtureEnvironment("tree", root), fixtureAttributes(false), time.Nanosecond
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = wait()
		var recorded []identity
		if body, err := os.ReadFile(filepath.Join(root, "tree.json")); err == nil {
			_ = json.Unmarshal(body, &recorded)
		}
		if body, err := os.ReadFile(filepath.Join(root, "leaf.json")); err == nil {
			var leaf identity
			if json.Unmarshal(body, &leaf) == nil {
				recorded = append(recorded, leaf)
			}
		}
		for _, id := range recorded {
			if id.PID != os.Getpid() {
				_ = killIdentity(id)
			}
		}
		if !fixtureUntil(3*time.Second, func() bool {
			empty := true
			for _, id := range recorded {
				if id.PID != os.Getpid() {
					reapIdentity(id)
					empty = empty && !identityExists(id)
				}
			}
			return empty
		}) {
			t.Error("cancel mutation cleanup left a recorded descendant")
		}
	}()
	if !fixtureUntil(3*time.Second, func() bool { return fixturePresent(root, "tree.json") }) {
		t.Fatal("cancelled tree did not reach its receipt")
	}
	var ids []identity
	body, err := os.ReadFile(filepath.Join(root, "tree.json"))
	if err != nil || json.Unmarshal(body, &ids) != nil || len(ids) != 2 {
		t.Fatalf("cancelled tree receipt: %v", err)
	}
	var leaf identity
	body, err = os.ReadFile(filepath.Join(root, "leaf.json"))
	if err != nil || json.Unmarshal(body, &leaf) != nil {
		t.Fatalf("cancelled leaf receipt: %v", err)
	}
	cancel()
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 2)
	for caller := 0; caller < 2; caller++ {
		wg.Go(func() { errorsSeen <- wait() })
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err == nil {
			t.Error("cancelled target returned success")
		}
	}
	for _, id := range append(ids, leaf) {
		if identityExists(id) {
			t.Fatalf("cancel waiter returned before tree reaping: %+v", id)
		}
	}
	if _, err := StartProcess(&exec.Cmd{}); err == nil {
		t.Fatal("empty command was accepted")
	}
	invalid := exec.Command("/bin/sh")
	invalid.Err = errors.New("fixture command setup failed")
	if _, err := StartProcess(invalid); err != invalid.Err {
		t.Fatal("command setup error was lost")
	}
	missing := exec.CommandContext(t.Context(), filepath.Join(root, "missing-executable"))
	if wait, err := StartProcess(missing); err == nil || wait != nil {
		t.Fatal("missing executable returned a successful waiter")
	}
	failed := exec.CommandContext(t.Context(), "/bin/sh", "-c", "exit 23")
	var exit interface{ ExitCode() int }
	if err := RunProcess(failed); !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("actual target exit status lost: %v", err)
	}
	if _, err := StartProcess(nil); err == nil {
		t.Fatal("nil command was accepted")
	}
	if _, err := CombinedOutput(nil); err == nil {
		t.Fatal("nil output command was accepted")
	}
	if _, err := Output(nil); err == nil {
		t.Fatal("nil stdout command was accepted")
	}
	if _, err := StartProcess(cmd); err == nil {
		t.Fatal("already started command was accepted")
	}
	// Transport the caller's inherited descriptor without renumbering it.
	file, err := os.Create(filepath.Join(root, "inherited"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	var stderr bytes.Buffer
	command := exec.CommandContext(t.Context(), "/bin/sh", "-c", `printf '%s\n' "$PWD" "$S8_FIXTURE_VALUE" "$1"; printf 'descriptor' >&3; printf 'stderr' >&2`, "fixture", "original argument")
	command.Env = append(os.Environ(), "S8_FIXTURE_VALUE=original environment")
	command.Dir, command.Stderr, command.ExtraFiles = root, &stderr, []*os.File{file}
	out, err := Output(command)
	if err != nil || string(out) != root+"\noriginal environment\noriginal argument\n" || stderr.String() != "stderr" {
		t.Fatalf("command transport changed: output=%q stderr=%q error=%v", out, stderr.String(), err)
	}
	content, err := os.ReadFile(file.Name())
	if err != nil || string(content) != "descriptor" {
		t.Fatalf("inherited descriptor was not preserved: %q %v", content, err)
	}
	combined := exec.CommandContext(t.Context(), "/bin/sh", "-c", "printf 'out'; printf 'err' >&2")
	if out, err := CombinedOutput(combined); err != nil || string(out) != "outerr" {
		t.Fatalf("combined output: %q %v", out, err)
	}
	for _, capture := range []func(*exec.Cmd) ([]byte, error){Output, CombinedOutput} {
		failedCapture := exec.CommandContext(t.Context(), "/bin/sh", "-c", "printf 'failure'; exit 24")
		out, err := capture(failedCapture)
		var exit interface{ ExitCode() int }
		if string(out) != "failure" || !errors.As(err, &exit) || exit.ExitCode() != 24 {
			t.Fatalf("captured output hid target failure: output=%q error=%v", out, err)
		}
	}
	configured := exec.CommandContext(t.Context(), "/bin/sh", "-c", "exit 0")
	configured.Stdout = &stderr
	if _, err := Output(configured); err == nil {
		t.Fatal("configured stdout was overwritten")
	}
	if _, err := CombinedOutput(configured); err == nil {
		t.Fatal("configured combined output was overwritten")
	}
	configured.Stdout, configured.Stderr = nil, &stderr
	if _, err := CombinedOutput(configured); err == nil {
		t.Fatal("configured stderr was overwritten")
	}
	exerciseChildSupervisorIdentity(t)
}

// A recycled or stale PID must not authorize a signal. The original command
// answers a fresh nonce AFTER the forged identity is rejected; a zombie's mere
// presence in the process table cannot satisfy this witness.
// origin: hot-update S8 plan §9.1 / P21 PID/start identity
func exerciseChildSupervisorIdentity(t *testing.T) {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inR.Close(); _ = inW.Close() }()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outR.Close(); _ = outW.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `while IFS= read -r line; do printf '%s\n' "$line"; done`)
	cmd.Stdin, cmd.Stdout = inR, outW
	wait, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = wait() }()
	_ = inR.Close()
	_ = outW.Close()
	id, _, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	stale := id
	stale.Start++
	if identityExists(stale) {
		t.Fatal("stale start time matched current process identity")
	}
	if err := killIdentity(stale); err != nil {
		t.Fatal(err)
	}
	if _, err := inW.WriteString("fresh identity witness\n"); err != nil {
		t.Fatalf("stale identity killed live target: %v", err)
	}
	if err := outR.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(outR).ReadString('\n')
	if err != nil || line != "fresh identity witness\n" {
		t.Fatalf("stale identity killed live target: output=%q error=%v", line, err)
	}
}

// Pin the three launch sites owned by P21. A missing site or a direct return to
// the legacy launcher fails here; broader fixture inventories belong to P31.
// origin: hot-update S8 plan §11.2 / P21
func TestChildSupervisorHarnessUsesSharedLaunchers(t *testing.T) {
	body, err := os.ReadFile("../harness_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []string{"testsupervisor.StartProcess(p.Cmd)", "testsupervisor.CombinedOutput(cmd)", "testsupervisor.Output(commit)"} {
		if !strings.Contains(string(body), call) {
			t.Errorf("shared supervised launch missing: %s", call)
		}
	}
}
