//go:build linux

package spawnsafe

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/handoff"
	"golang.org/x/sys/unix"
)

const probeHelperEnv = "_TETHER_VERSION_PROBE_HELPER"
const probeRaceEnv = "_TETHER_VERSION_PROBE_GORACE"

// Dispatch here so both the product and every importing test binary enter the
// same supervisor before main or testing parses arguments. The mode never
// reaches the candidate, including when the candidate is this same binary.
func init() {
	if os.Getenv(probeHelperEnv) == "1" {
		os.Exit(runProbeHelper())
	}
}

func probeEnvironment() []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, probeHelperEnv+"=") && !strings.HasPrefix(entry, probeRaceEnv+"=") {
			env = append(env, entry)
		}
	}
	return env
}

// RunVersionProbe runs a caller-bounded version command in an isolated reaper.
// The caller retains output capture and its deadline; cancellation asks the
// helper to kill and reap the candidate group before the helper exits.
// origin: hot-update S3 plan §2.1 / §2.3 (RS-3).
func RunVersionProbe(cmd *exec.Cmd) error {
	cmd.Args = append([]string{"tether-version-probe", cmd.Path}, cmd.Args[1:]...)
	cmd.Path = "/proc/self/exe"
	cmd.Env = append(probeEnvironment(), probeHelperEnv+"=1")
	// This private reaper joins every child before exit, so another race exit
	// grace period only consumes the image admission budget. Keep all other
	// diagnostics and restore the candidate's original options in the helper.
	// origin: hot-update S8 plan §5.2 / S8 internal review ROOT-STAGE-LATENCY F1
	raceOptions, raceSet := os.LookupEnv("GORACE")
	cmd.Env = append(cmd.Env, "GORACE="+raceOptions+" atexit_sleep_ms=0")
	if raceSet {
		cmd.Env = append(cmd.Env, probeRaceEnv+"="+raceOptions)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	return cmd.Run()
}

func runProbeHelper() int {
	if len(os.Args) < 3 {
		return 1
	}
	// Never enable subreaping in the agent: wait4(-1) there could consume
	// business-child statuses. This process owns only the candidate tree.
	if unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != nil {
		return 1
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	defer signal.Stop(stop)
	ctx := context.Background() // ctx-root: version-probe helper process root
	cmd := exec.CommandContext(ctx, os.Args[1], os.Args[2:]...)
	cmd.Env = probeEnvironment()
	// The helper's zero-delay exit is not an override of candidate behavior.
	// Absence and an explicitly empty GORACE remain distinct, and neither
	// private dispatch variable may leak into a candidate of the same binary.
	// origin: hot-update S8 plan §5.2 / S8 internal review ROOT-STAGE-LATENCY F1
	cmd.Env = restoreProbeRaceOptions(cmd.Env)
	// origin: hot-update S6 plan §3.1. Pure smoke input stays off argv/env.
	cmd.Stdin = os.Stdin
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 1
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var err error
	select {
	case err = <-waited:
	case <-stop:
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		err = <-waited
		if err == nil {
			err = errors.New("probe canceled")
		}
	}
	// Success also cleans up: a version command may exit after starting a
	// background child that inherited neither stdout nor stderr.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	for {
		_, reapErr := syscall.Wait4(-1, nil, 0, nil)
		if errors.Is(reapErr, syscall.EINTR) {
			continue
		}
		if errors.Is(reapErr, syscall.ECHILD) {
			break
		}
		if reapErr != nil {
			return 1
		}
	}
	if err != nil {
		return 1
	}
	return 0
}

func restoreProbeRaceOptions(env []string) []string {
	out := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, "GORACE=") {
			out = append(out, entry)
		}
	}
	if original, ok := os.LookupEnv(probeRaceEnv); ok {
		out = append(out, "GORACE="+original)
	}
	return out
}
