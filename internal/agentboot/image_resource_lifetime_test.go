//go:build linux

package agentboot

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const lifetimeMode = "_RESOURCE_LIFETIME_MODE"

func lifetimeEnv(mode string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, lifetimeMode+"=") {
			env = append(env, entry)
		}
	}
	return append(env, lifetimeMode+"="+mode)
}

// origin: hot-update S3 plan §1.3 / §1.5; resources verifier
// A real exec transfers ownership; the parent probes BEFORE letting the
// receiver exit. Both activation and explicit abandonment share the fixture.
func TestImageCompletionClosesUntakenPipeBeforeExit(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(lifetimeMode); mode {
	case "dispatch-ready", "dispatch-abandon":
		unix.CloseOnExec(3)
		reader := os.NewFile(3, "auxiliary-reader")
		defer func() { _ = reader.Close() }()
		f := newCutFixture(t, "completion-fixture")
		frozen, err := agentimage.Freeze(agentimage.Owners{Runtime: f.rt, Manager: f.manager,
			Lineage: agentimage.Lineage{Generation: 1}}, time.Now().Add(imageTestBudget))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = frozen.Thaw() }()
		prepared, err := frozen.Prepare(handoff.Resource{ID: "auxiliary", Kind: "pipe", Files: map[string]*os.File{"read": reader}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = prepared.Close() }()
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		err = prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestImageCompletionClosesUntakenPipeBeforeExit$", "-test.count=1"},
			lifetimeEnv(strings.TrimPrefix(mode, "dispatch-")))
		t.Fatal(err)
	case "ready", "abandon":
		img := Begin(nil)
		if img == nil || img.Phase() != PhaseHeld {
			t.Fatal("real dispatch did not reach held")
		}
		defer img.Abandon(nil, CrashWindowNoRollbackTarget, "fixture cleanup")
		if mode == "ready" {
			target := newCutFixture(t, "unused-target")
			imp, err := img.Restore(target.target())
			if err != nil {
				t.Fatal("empty valid bundle refused:", err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), imageTestBudget)
				defer cancel()
				if err := imp.Runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			if _, _, _, err := img.Activate(); err != nil {
				t.Fatal(err)
			}
			retained := img.restored != nil
			t.Cleanup(func() {
				if retained {
					t.Error("activation retained the dispatch owner")
				}
			})
			img.MarkReady()
			if img.Phase() != PhaseReady {
				t.Fatal("activation did not reach ready")
			}
		} else {
			img.Abandon(nil, CrashWindowNonDaemon, "fixture non-daemon")
		}
		// origin: hot-update S3 plan §1.3 / §1.5. The parent must receive the
		// phase before it can probe descriptors while this process is alive.
		if _, err := fmt.Fprintln(os.Stdout, "LIFETIME_PHASE="+string(img.Phase())); err != nil {
			t.Fatal("write phase receipt:", err)
		}
		var release [1]byte
		if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
			t.Fatal("parent did not release receiver:", err)
		}
		return
	}
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for _, arm := range []string{"abandon", "ready"} {
		for round := 0; round < 5; round++ {
			t.Run(arm+"/"+strconv.Itoa(round), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				reader, writer, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reader.Close() }()
				defer func() { _ = writer.Close() }()
				ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestImageCompletionClosesUntakenPipeBeforeExit$", "-test.count=1")
				cmd.Env = lifetimeEnv("dispatch-" + arm)
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
				cmd.WaitDelay = time.Second
				cmd.ExtraFiles = []*os.File{reader}
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				in, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				out, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = in.Write([]byte{1})
					_ = in.Close()
					_, _ = io.Copy(io.Discard, out)
					if err := cmd.Wait(); err != nil {
						t.Errorf("helper failed: %v; %s", err, stderr.String())
					}
				}()
				_ = reader.Close()
				buffered := bufio.NewReader(out)
				line, err := buffered.ReadString('\n')
				want := "LIFETIME_PHASE=ready\n"
				if arm == "abandon" {
					want = "LIFETIME_PHASE=lost\n"
				}
				if err != nil || line != want {
					rest, _ := io.ReadAll(buffered)
					t.Fatalf("phase receipt %q, %v; %s", line, err, rest)
				}
				if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
					t.Fatal("receiver exited before pipe probe")
				}
				n, err := writer.Write([]byte("x"))
				if !errors.Is(err, syscall.EPIPE) {
					t.Errorf("live %s receiver retained untaken pipe: write=(%d, %v), want EPIPE", arm, n, err)
				}
			})
		}
	}
	testharness.AssertNoFDLeak(t, "image completion x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "image completion x5", beforeG)
}

// origin: hot-update S3 plan §1.3 / §1.5; resources verifier
// Exhaustion must not silently turn W1 cleanup into a no-op. Lowering only
// this isolated helper's soft limit deterministically makes the census fail.
func TestCrashWindowClosesInheritedDescriptorsWhenCensusFails(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Getenv(lifetimeMode) == "dispatch-exhausted" {
		unix.CloseOnExec(3)
		reader := os.NewFile(3, "exhaustion-input")
		defer func() { _ = reader.Close() }()
		prepared, err := handoff.Prepare(handoff.State{Role: "probe", Instance: "exhaustion", Generation: 1},
			[]handoff.Resource{{ID: "probe", Kind: "pipe", Files: map[string]*os.File{"read": reader}}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = prepared.Close() }()
		_ = reader.Close()
		t.Fatal(prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestCrashWindowClosesInheritedDescriptorsWhenCensusFails$", "-test.v"}, lifetimeEnv("exhausted")))
	}
	if os.Getenv(lifetimeMode) == "exhausted" {
		inherited, censusErr := foreignDescriptors()
		if censusErr != nil {
			t.Fatal(censusErr)
		}
		if len(inherited) != 2 {
			t.Fatalf("want real manifest plus resource, got %d inherited descriptors", len(inherited))
		}
		defer func() {
			for _, fd := range inherited {
				_ = unix.Close(fd.fd)
			}
		}()
		manifestFD, _, ok := strings.Cut(os.Getenv("_TETHER_HANDOFF"), ":")
		if !ok {
			t.Fatal("missing real dispatch")
		}
		var original unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
			t.Fatal(err)
		}
		if err := os.Setenv("_TETHER_HANDOFF", manifestFD+":"+strings.Repeat("0", 64)); err != nil {
			t.Fatal(err)
		}
		limit := original
		limit.Cur = 0
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &original) }()
		_, censusErr = os.ReadDir("/proc/self/fd")
		img := Begin(nil)
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(censusErr, unix.EMFILE) || img == nil || img.Phase() != PhaseLost {
			t.Fatalf("premises: census=%v image=%v", censusErr, img != nil)
		}
		for _, fd := range inherited {
			flags, fdErr := unix.FcntlInt(uintptr(fd.fd), unix.F_GETFD, 0)
			if !errors.Is(fdErr, unix.EBADF) {
				t.Errorf("W1 census EMFILE left handoff fd %d open: flags=%d err=%v (CLOEXEC=%t)", fd.fd, flags, fdErr, flags&unix.FD_CLOEXEC != 0)
			}
		}
		return
	}
	f, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashWindowClosesInheritedDescriptorsWhenCensusFails$", "-test.count=1", "-test.v")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	cmd.Env = lifetimeEnv("dispatch-exhausted")
	cmd.ExtraFiles = []*os.File{f}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper: %v\n%s", err, out)
	}
}

// origin: hot-update S3 plan §1.5 / RB-5. A saved number cannot authorize
// closing a different file after validation has closed and reused that slot.
func TestCrashWindowPreservesReusedDescriptor(t *testing.T) {
	original, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = original.Close() }()
	var st unix.Stat_t
	if err := unix.Fstat(int(original.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	saved := []foreignDescriptor{{int(original.Fd()), uint64(st.Dev), st.Ino}}
	replacement, err := os.Open("/dev/zero")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	if err := unix.Dup3(int(replacement.Fd()), int(original.Fd()), unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	if closed := closeForeignDescriptors(saved); closed != 0 {
		t.Errorf("closed reused descriptor: %d", closed)
	}
	if _, err := unix.FcntlInt(original.Fd(), unix.F_GETFD, 0); err != nil {
		t.Errorf("replacement was closed: %v", err)
	}
}

// origin: hot-update S3 plan §1.5 / RESOURCES-V1. An unusable limit must
// leave an explicit diagnostic rather than reporting an empty census.
func TestCrashWindowReportsUnavailableDescriptorCensus(t *testing.T) {
	if os.Getenv(lifetimeMode) == "census-unavailable" {
		if err := os.Setenv("_TETHER_HANDOFF", "invalid-dispatch"); err != nil {
			t.Fatal(err)
		}
		// Hard-limit changes cannot be undone; confine them to this helper.
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{}); err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		img := Begin(slog.New(slog.NewTextHandler(&logs, nil)))
		if img == nil || img.Phase() != PhaseLost {
			t.Fatal("unreadable dispatch did not enter W1")
		}
		if !strings.Contains(logs.String(), "crash_window=handoff_restore") ||
			!strings.Contains(logs.String(), `fd_cleanup_error="descriptor census failed with unusable descriptor limit"`) {
			t.Fatalf("census failure missing from crash-window log: %s", logs.String())
		}
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census. The inner
	// process deliberately removes its ability to open /proc descriptors.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashWindowReportsUnavailableDescriptorCensus$", "-test.count=1")
	cmd.Env = lifetimeEnv("census-unavailable")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v\n%s", err, out)
	}
}
