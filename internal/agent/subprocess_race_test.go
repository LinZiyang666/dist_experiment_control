//go:build race

package agent

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 plan §8.4. Reducing a helper's exit delay must retain
// the detector and its failure status; the deliberate race lives only in the
// subprocess, whose diagnostic and nonzero exit are the parent's oracle.
func TestRaceSubprocessRetainsDiagnostics(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	const helperKey = "_TETHER_RACE_DIAGNOSTIC_TEST"
	if os.Getenv(helperKey) == "1" {
		var value int
		var writers sync.WaitGroup
		start := make(chan struct{})
		for n := range 2 {
			writers.Go(func() {
				<-start
				value = n
			})
		}
		close(start)
		writers.Wait()
		runtime.KeepAlive(&value)
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			dir := t.TempDir()
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRaceSubprocessRetainsDiagnostics$", "-test.count=1")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), helperKey+"=1")
			out, err := cmd.CombinedOutput()
			if err == nil || !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
				t.Fatalf("race subprocess lost failure/diagnostic: err=%v\n%s", err, out)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "race diagnostic subprocess x5", beforeG)
	testharness.AssertNoFDLeak(t, "race diagnostic subprocess x5", beforeFD, 0)
}
