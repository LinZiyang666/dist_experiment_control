//go:build linux

package spawnsafe

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// The supervisor has joined its entire tree before exiting. Its race exit
// grace period adds no coverage; the candidate must retain the caller's exact
// diagnostics, including an explicitly requested exit delay. Real: the shared
// subreaper and shell candidate. Not covered: the broker cut or image decoder.
// origin: S8 internal review ROOT-STAGE-LATENCY F1
func TestVersionProbeSupervisorAvoidsExitDelayAndPreservesCandidateOptions(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseVersionProbeRaceOptions(t)
	}
	testharness.AssertNoGoroutineLeak(t, "version probe race options", g)
	testharness.AssertNoFDLeak(t, "version probe race options", fd, 0)
}

func exerciseVersionProbeRaceOptions(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name, options string
		set           bool
	}{
		{name: "unset"},
		{name: "empty", set: true},
		{name: "configured", set: true, options: "halt_on_error=1 exitcode=73 atexit_sleep_ms=1234"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GORACE", tc.options)
			if !tc.set {
				if err := os.Unsetenv("GORACE"); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `printf '%s\n%s\n%s\n' "${GORACE-unset}" "${_TETHER_VERSION_PROBE_HELPER-unset}" "${_TETHER_VERSION_PROBE_GORACE-unset}"`)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := RunVersionProbe(cmd); err != nil {
				t.Fatalf("supervised probe: %v: %s", err, output.String())
			}
			want := tc.options
			if !tc.set {
				want = "unset"
			}
			if output.String() != want+"\nunset\nunset\n" {
				t.Fatalf("candidate race options or private environment changed: %q", output.String())
			}
			var helperOptions string
			for _, entry := range cmd.Env {
				if value, ok := strings.CutPrefix(entry, "GORACE="); ok {
					helperOptions = value
				}
			}
			if helperOptions != tc.options+" atexit_sleep_ms=0" {
				t.Fatalf("reaped supervisor retains a redundant race exit delay: %q", helperOptions)
			}
		})
	}
}
