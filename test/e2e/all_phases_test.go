//go:build e2e_matrix

// Package e2e_test runs the full P1-P10 e2e suite as one matrix
// (architecture P11 line 2141: "P2-P10 各 phase 测试合并到 test/
// e2e/, CI 每夜跑"). Implemented as subtests that go-test
// subprocess-invoke each phase package, so:
//
//   - one entry point: `go test -tags e2e_matrix ./test/e2e/...`
//     runs everything (or `make e2e` via the Makefile target);
//   - the per-phase packages stay intact for fast iteration
//     (`go test ./test/p7/...` is much shorter than `make e2e`);
//   - a hang or long e2e in one phase doesn't drag the others —
//     each subtest is its own subprocess with its own timeout;
//   - failures bubble up with the phase name in t.Run output, so
//     CI logs show which phase regressed without re-runs.
//
// Behind the e2e_matrix build tag so a bare `go test ./...` (and
// therefore `make test`) does NOT recurse into this file: this
// test forks `go test ./test/pX/...` per phase, which would
// itself re-enter `./test/e2e/...` and explode if not gated.
package e2e_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// phaseTimeout caps each subprocess. test/p3 is the slow one
// (~22s for the full auth_callout matrix); 90s gives 4× headroom.
const phaseTimeout = 90 * time.Second

// allPhases is the architecture-mandated set. P0 has no e2e
// (scaffold only); P1 covers proto + identity + makefile, kept
// for completeness so a stale dependency in proto immediately
// surfaces here too.
var allPhases = []string{
	"p1", "p2", "p3", "p4", "p5",
	"p6", "p7", "p8", "p9", "p10",
}

func TestAllPhases(t *testing.T) {
	// Phases run SERIALLY. The first cut had t.Parallel() and
	// finished in ~24s, but P5's PTY attach handshake (architecture
	// C.5.1, 3s deadline) is contention-sensitive: with 10 phase
	// subprocesses each spawning embedded NATS + multiple
	// goroutines per test, attach_timeout fires under load and
	// `make e2e` flakes. P11 is a release gate — repeatability
	// beats wall-clock. Sequential is ~80s on this box, still
	// well inside any nightly budget.
	for _, phase := range allPhases {
		t.Run(phase, func(t *testing.T) {
			runPhase(t, phase)
		})
	}
}

func runPhase(t *testing.T, phase string) {
	t.Helper()
	// go test sets cwd to the package dir (test/e2e/), so the
	// `./test/pX/...` path the subprocess receives must be
	// resolved against the repo root, not the test pkg dir. Climb
	// two levels off this source file's path to find the repo.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	// -count=1 disables go's test cache so each `make e2e` run is
	// a real exercise, not a cached PASS replay (architecture
	// P11 acceptance: "CI 夜跑稳定 ≥ 7 天" — we want each night
	// to actually re-run, not stamp the same cached pass).
	cmd := exec.Command("go", "test", "-count=1", "-timeout", phaseTimeout.String(),
		"./test/"+phase+"/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("phase %s failed: %v\n--- output ---\n%s", phase, err, buf.String())
	}
}
