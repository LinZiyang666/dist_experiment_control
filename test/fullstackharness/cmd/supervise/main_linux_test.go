//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"golang.org/x/sys/unix"
)

// Native assertion/setup statuses must not claim that the process census failed.
// The original run-all tail made four business failures look like lost waits.
// origin: S10 internal review FIX-SUPERVISE-2 F1
func TestSupervisorDiagnosticsDistinguishCommandFailure(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorFailureDiagnostic(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor failure diagnostic x5", g)
	testharness.AssertNoFDLeak(t, "supervisor failure diagnostic x5", f, 0)
}

func exerciseSupervisorFailureDiagnostic(t *testing.T) {
	t.Helper()
	for _, status := range []int{1, 2, 23, 125, 143} {
		got, _, stderr := supervisorInvocation(t, t.TempDir(), []string{"--", "/bin/sh", "-c", "exit " + strconv.Itoa(status)})
		if got != status || !strings.Contains(stderr, "supervise: command exited with status "+strconv.Itoa(status)) || strings.Contains(stderr, "process cleanup failed") {
			t.Fatalf("native command failure misclassified: got=%d want=%d stderr=%q", got, status, stderr)
		}
	}
}

// A returning launcher must release its borrowed READY descriptor before main
// exits, while preserving both successful and unsuccessful child statuses.
// origin: hot-update S10 plan §6 / LINT-s10
func TestSupervisorReturnClosesReadyDescriptor(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorReturn(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor READY return x5", beforeG)
	testharness.AssertNoFDLeak(t, "supervisor READY return x5", beforeFD, 0)
}

// origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / supervisor evidence protocol.
func TestSupervisorLogAndReceiptPreserveChildContract(t *testing.T) {
	t.Setenv("SUPERVISOR_TEST_VALUE", "inherited-environment")
	t.Chdir(t.TempDir())
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorEvidence(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor evidence x5", beforeG)
	testharness.AssertNoFDLeak(t, "supervisor evidence x5", beforeFD, 0)
}

func exerciseSupervisorEvidence(t *testing.T) {
	t.Helper()
	for _, want := range []int{0, 23, 143} {
		root := t.TempDir()
		log, receipt := filepath.Join(root, "child.log"), filepath.Join(root, "child.rc")
		for _, path := range []string{log, receipt} {
			if err := os.WriteFile(path, []byte("stale evidence\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		script := `read -r input; printf 'stdout:%s:%s:%s:%s\n' "$input" "$SUPERVISOR_TEST_VALUE" "$PWD" "$1"; printf 'stderr:business\n' >&2; `
		if want == 143 {
			script += "kill -TERM $$"
		} else {
			script += "exit " + strconv.Itoa(want)
		}
		got, stdout, stderr := supervisorInvocation(t, root, []string{"--log", log, "--receipt", receipt, "--", "/bin/sh", "-c", script, "child", "literal argument"})
		if got != want {
			t.Fatalf("native child status=%d want=%d", got, want)
		}
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		line := "stdout:business-input:inherited-environment:" + cwd + ":literal argument\n"
		if stdout != line || !strings.Contains(stderr, "stderr:business\n") {
			t.Fatalf("inherited streams/cwd/env/argv changed: stdout=%q stderr=%q", stdout, stderr)
		}
		data, err := os.ReadFile(log)
		if err != nil || !strings.Contains(string(data), line) || !strings.Contains(string(data), "stderr:business\n") || strings.Contains(string(data), "stale evidence") {
			t.Fatalf("combined log missing business output or retaining stale evidence: %q %v", data, err)
		}
		assertSupervisorReceipt(t, receipt, want)
	}
}

// The receipt is written after joining the reaper, rather than from the child's
// successful Wait alone. Otherwise a leaked descendant becomes a false kill.
// origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / supervisor descendant verdict.
func TestSupervisorReceiptIncludesDescendantCleanup(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorDescendant(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor descendants x5", beforeG)
	testharness.AssertNoFDLeak(t, "supervisor descendants x5", beforeFD, 0)
}

func exerciseSupervisorDescendant(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	pidPath, receipt := filepath.Join(root, "descendant.pid"), filepath.Join(root, "child.rc")
	// This child deliberately outlives its shell. The shared reaper must reap it
	// even with log pipes present, before run returns its cleanup status.
	script := `sleep 60 >/dev/null 2>&1 & printf '%s\n' "$!" > "$1"; exit 0`
	got, _, _ := supervisorInvocation(t, root, []string{"--log", filepath.Join(root, "child.log"), "--receipt", receipt, "--", "/bin/sh", "-c", script, "child", pidPath})
	data, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		t.Fatalf("descendant identity: %q %v", data, err)
	}
	if err := unix.Kill(pid, 0); !errors.Is(err, unix.ESRCH) {
		// A mutant must leave no process behind after its red assertion.
		_ = unix.Kill(pid, unix.SIGKILL)
		t.Errorf("descendant survived launcher return: pid=%d err=%v", pid, err)
	}
	if got != 125 {
		t.Fatalf("cleanup status=%d want=125", got)
	}
	assertSupervisorReceipt(t, receipt, 125)
}

// origin: hot-update S6 plan §10.1 / T58; DRILL-ASSETS / evidence setup before child admission.
func TestSupervisorArtifactFailurePreventsLaunch(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupervisorArtifactFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supervisor setup refusal x5", beforeG)
	testharness.AssertNoFDLeak(t, "supervisor setup refusal x5", beforeFD, 0)
}

func exerciseSupervisorArtifactFailure(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	shared := filepath.Join(root, "same-file")
	alias := filepath.Join(root, "alias")
	if err := os.WriteFile(shared, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(shared, alias); err != nil {
		t.Fatal(err)
	}
	for _, paths := range [][2]string{
		{filepath.Join(root, "missing", "log"), filepath.Join(root, "status")},
		{filepath.Join(root, "log"), filepath.Join(root, "missing", "status")},
		{shared, shared}, {shared, alias},
	} {
		marker := filepath.Join(root, "started")
		got, _, _ := supervisorInvocation(t, root, []string{"--log", paths[0], "--receipt", paths[1], "--", "/bin/sh", "-c", `printf started > "$1"`, "child", marker})
		if got != 126 {
			t.Errorf("artifact setup status=%d want=126", got)
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("child ran before evidence paths were validated: %v", err)
		}
	}
}

func assertSupervisorReceipt(t *testing.T, path string, want int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != strconv.Itoa(want)+"\n" {
		t.Fatalf("native receipt=%q want=%d err=%v", data, want, err)
	}
}

func supervisorInvocation(t *testing.T, root string, args []string) (int, string, string) {
	t.Helper()
	files := make([]*os.File, 3)
	for i, name := range []string{"stdin", "stdout", "stderr"} {
		file, err := os.CreateTemp(root, name+"-*")
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file
		defer func() {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	if _, err := files[0].WriteString("business-input\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := files[0].Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	oldFlags, oldArgs := flag.CommandLine, os.Args
	oldIn, oldOut, oldErr := os.Stdin, os.Stdout, os.Stderr
	defer func() {
		flag.CommandLine, os.Args = oldFlags, oldArgs
		os.Stdin, os.Stdout, os.Stderr = oldIn, oldOut, oldErr
	}()
	flag.CommandLine = flag.NewFlagSet("supervise", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	os.Args = append([]string{"supervise"}, args...)
	os.Stdin, os.Stdout, os.Stderr = files[0], files[1], files[2]
	code := run()
	output := make([]string, 2)
	for i, file := range files[1:] {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		output[i] = string(data)
	}
	return code, output[0], output[1]
}

func exerciseSupervisorReturn(t *testing.T) {
	t.Helper()
	for _, want := range []int{0, 23} {
		ready, err := os.CreateTemp(t.TempDir(), "ready-*")
		if err != nil {
			t.Fatal(err)
		}
		fd, err := unix.FcntlInt(ready.Fd(), unix.F_DUPFD_CLOEXEC, 3)
		if err != nil {
			if closeErr := ready.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			t.Fatal(err)
		}
		// The raw duplicate belongs to run. This fallback also closes a mutant's
		// leaked descriptor after recording the assertion, before leak checks.
		func() {
			defer func() {
				if err := unix.Close(fd); err != nil && !errors.Is(err, unix.EBADF) {
					t.Error(err)
				}
				if err := ready.Close(); err != nil {
					t.Error(err)
				}
			}()
			oldFlags, oldArgs := flag.CommandLine, os.Args
			defer func() { flag.CommandLine, os.Args = oldFlags, oldArgs }()
			flag.CommandLine = flag.NewFlagSet("supervise", flag.ContinueOnError)
			flag.CommandLine.SetOutput(io.Discard)
			os.Args = []string{"supervise", "-ready-fd", strconv.Itoa(fd), "--", "/bin/sh", "-c", "exit " + strconv.Itoa(want)}
			if got := run(); got != want {
				t.Errorf("child status: got=%d want=%d", got, want)
			}
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
				t.Errorf("READY descriptor survived launcher return: %v", err)
			}
			if _, err := ready.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			var receipt fullstackharness.ProcessReady
			if err := json.NewDecoder(ready).Decode(&receipt); err != nil {
				t.Fatal(err)
			}
			if receipt.Launcher.PID != os.Getpid() || receipt.Target.PID <= 0 || receipt.Target.StartTicks == 0 {
				t.Fatal("missing real launcher/target census")
			}
		}()
	}
}
