package broker

import (
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Each helper keeps its process-global hooks and five-round leak baseline private.
// Bound overlapping election/expiry waits without changing production deadlines or
// multiplying this package's fixture population by the host's GOMAXPROCS.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
var brokerFixtureProcesses = make(chan struct{}, 8)

func runIsolatedBrokerTest(t *testing.T) bool {
	t.Helper()
	const modeKey = "_TETHER_BROKER_TEST_HELPER"
	name := t.Name()
	if os.Getenv(modeKey) == name {
		// Nested business children must never dispatch another test body.
		if err := os.Unsetenv(modeKey); err != nil {
			t.Fatal(err)
		}
		return false
	}
	// Keep subtest filters and caller fixture/profiling flags on Go's original
	// execution path; an exact root selector cannot preserve those arguments.
	if !brokerFixtureIsolationAllowed(flag.CommandLine) {
		return false
	}
	t.Parallel()
	select {
	case brokerFixtureProcesses <- struct{}{}:
		defer func() { <-brokerFixtureProcesses }()
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	dir := t.TempDir()
	helper := filepath.Join(dir, "fixture.test")
	if err := os.Link(os.Args[0], helper); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, helper, "-test.run=^"+regexp.QuoteMeta(name)+"$", "-test.count=1", "-test.timeout=600s", "-test.parallel="+flag.Lookup("test.parallel").Value.String(), "-test.v")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), modeKey+"="+name,
		"TMPDIR="+dir, "HOME="+t.TempDir(), "TETHER_HOME="+t.TempDir())
	// The shared lease supervisor joins every descendant on failure and timeout;
	// the hard link keeps the parent's exact test binary and race instrumentation.
	out, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("isolated %s: %v\n%s", name, err, out)
	}
	// An empty selection exits zero. Require the original body's exact receipt
	// so scheduling cannot silently turn a missing or skipped test into success.
	if !bytes.Contains(out, []byte("=== RUN   "+name+"\n")) || !bytes.Contains(out, []byte("--- PASS: "+name+" (")) {
		t.Fatalf("isolated %s did not complete its test body:\n%s", name, out)
	}
	t.Logf("%s", out)
	return true
}

// Ordinary matrix flags preserve the exact child selection; every other
// invocation must retain its original runner and caller arguments.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
func brokerFixtureIsolationAllowed(options *flag.FlagSet) bool {
	isolate := len(options.Args()) == 0
	options.Visit(func(option *flag.Flag) {
		switch option.Name {
		case "test.run":
			if strings.Contains(option.Value.String(), "/") {
				isolate = false
			}
		case "test.count", "test.timeout", "test.parallel", "test.v", "test.paniconexit0", "test.testlogfile", "test.outputdir":
		default:
			isolate = false
		}
	})
	return isolate
}

// These cases keep a root-only child from silently dropping a requested case or
// a fixture argument. The flag sets are private and never alter the test runner.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET
func TestFixtureIsolationKeepsCallerSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		isolate bool
	}{
		{"package", []string{"-test.count=1", "-test.timeout=600s", "-test.parallel=8", "-test.v=test2json", "-test.paniconexit0=true", "-test.testlogfile=/tmp/testlog", "-test.outputdir=/tmp"}, true},
		{"root", []string{"-test.run=^TestExample$"}, true},
		{"subtest", []string{"-test.run=^TestExample$/^child$"}, false},
		{"slash-class", []string{"-test.run=^Test[AB/]$"}, false},
		{"fixture", []string{"-fixture-root=/fixture"}, false},
		{"short", []string{"-test.short=true"}, false},
		{"skip", []string{"-test.skip=child"}, false},
		{"cpu", []string{"-test.cpu=2,4"}, false},
		{"shuffle", []string{"-test.shuffle=on"}, false},
		{"profile", []string{"-test.cpuprofile=/tmp/profile"}, false},
		{"failfast", []string{"-test.failfast=true"}, false},
		{"positional", []string{"fixture-mode"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := flag.NewFlagSet("fixture", flag.ContinueOnError)
			for _, name := range []string{"test.run", "test.count", "test.timeout", "test.parallel", "test.v", "test.paniconexit0", "test.testlogfile", "test.outputdir", "test.short", "test.skip", "test.cpu", "test.shuffle", "test.cpuprofile", "test.failfast", "fixture-root"} {
				options.String(name, "", "")
			}
			if err := options.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			if got := brokerFixtureIsolationAllowed(options); got != tc.isolate {
				t.Fatalf("isolation=%t, want %t for %v", got, tc.isolate, tc.args)
			}
		})
	}
}
