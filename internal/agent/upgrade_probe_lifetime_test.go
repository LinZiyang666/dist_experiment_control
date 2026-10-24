//go:build linux

package agent

import (
	"context"
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

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S3 plan §2.1 / §2.3 (RS-3).
// The isolated fixture adopts escaped descendants solely to clean up mutations.
// Production must reap its own tree and leave the business child's status alone.
func TestCandidateABIProbeLeavesNoDescendant(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	const helperKey = "_TETHER_ABI_RESOURCE_TEST"
	if os.Getenv(helperKey) != "1" {
		// The subreaper and five-round leak counts belong to the isolated
		// helper; the parent shares no mutable globals with other tests.
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 25*(upgradeSmokeTimeout+imageProbeCleanupBudget))
		defer cancel()
		dir := t.TempDir()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCandidateABIProbeLeavesNoDescendant$", "-test.count=1", "-test.v")
		cmd.Dir = dir
		cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "TMPDIR="+dir, helperKey+"=1")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		cmd.WaitDelay = time.Second
		defer func() {
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}()
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated candidate probe: %v\n%s", err, out)
		}
		t.Log(strings.TrimSpace(string(out)))
		return
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"accepted", "refused", "nonzero", "inherited-output", "timeout"} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				// A child of the agent fixture must remain alive and waitable.
				business := exec.Command("/bin/sh", "-c", "read release; exit 23")
				business.Env = handoff.CleanEnvironment(os.Environ())
				in, err := business.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := business.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = in.Write([]byte("done\n"))
					_ = in.Close()
					err := business.Wait()
					var exit *exec.ExitError
					if !errors.As(err, &exit) || exit.ExitCode() != 23 {
						t.Errorf("business child status stolen or killed: %v", err)
					}
				}()
				pidFile := filepath.Join(t.TempDir(), "descendant.pid")
				line, tail, redirect := proto.VersionLine(), "", " >/dev/null 2>&1"
				if arm == "refused" {
					line = "invalid version"
				}
				if arm == "nonzero" {
					tail = "exit 1\n"
				}
				if arm == "timeout" {
					tail = "wait\n"
				}
				if arm == "inherited-output" {
					redirect = ""
				}
				body := fmt.Sprintf("#!/bin/sh\n/bin/sleep 60%s &\nprintf '%%s' \"$!\" > '%s'\nprintf '%%s\\n' '%s' '%s'\n%s", redirect, pidFile, line, proto.ImageABILine(), tail)
				started := time.Now()
				_, abi, probeErr := smokeVersionImageABI(writeScript(t, body))
				data, err := os.ReadFile(pidFile)
				if err != nil {
					t.Fatal("candidate descendant receipt missing:", err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
				if err != nil || pid <= 1 {
					t.Fatal("invalid descendant receipt")
				}
				defer func() {
					_ = syscall.Kill(pid, syscall.SIGKILL)
					for {
						_, err := syscall.Wait4(pid, nil, 0, nil)
						if err != syscall.EINTR {
							break
						}
					}
				}()
				if arm == "accepted" || arm == "inherited-output" {
					if probeErr != nil || abi != proto.ImageABIVersion {
						t.Errorf("valid candidate refused: abi=%d err=%v", abi, probeErr)
					}
				} else if probeErr == nil {
					t.Error("invalid or timed-out candidate accepted")
				}
				if time.Since(started) > upgradeSmokeTimeout+imageProbeCleanupBudget {
					t.Error("probe exceeded execution and cleanup budget")
				}
				if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
					t.Errorf("probe left running or unreaped descendant %d: %v", pid, err)
				}
			})
		}
	}
	testharness.AssertNoFDLeak(t, "candidate probe x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "candidate probe x5", beforeG)
}

const imageProbeCleanupBudget = 3 * time.Second
