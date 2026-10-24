package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Independent daemon lifecycles overlap their real waits in separate test
// processes. Global command options and each five-round leak census remain
// private; the shared supervisor still reports any descendant it has to reap.
// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
var commandFixtureLanes = make(chan struct{}, 24)

const signedRollArtifactRootKey = "_TETHER_SIGNED_ROLL_ARTIFACT_ROOT"

// Race diagnostics remain enabled, while descendants omit the race runtime's
// one-second exit padding after their supervisor has already joined them.
// The bounded lanes keep long lifecycle roots schedulable together even in a
// two-CPU matrix worker;
// an explicit caller parallel limit still wins.
// origin: S10 internal review POLISH-B F1 / plan L17
func TestMain(m *testing.M) {
	flag.Parse()
	explicit := false
	flag.Visit(func(option *flag.Flag) {
		if option.Name == "test.parallel" {
			explicit = true
		}
	})
	if !explicit {
		if err := flag.Set("test.parallel", "24"); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := os.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Sibling fixture processes share only immutable build outputs. The package
	// owns their directory until every test and supervised child has joined;
	// a per-test TempDir would retire bytes still needed by another roll root.
	// origin: S10 internal review CMD-TETHER-BUDGET F1; hot-update S10 plan §11.1
	artifactRoot := os.Getenv(signedRollArtifactRootKey)
	owned := artifactRoot == ""
	if owned {
		var err error
		artifactRoot, err = os.MkdirTemp("", "tcmd-artifacts-")
		if err == nil {
			err = os.Setenv(signedRollArtifactRootKey, artifactRoot)
		}
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	code := m.Run()
	if owned {
		if err := os.RemoveAll(artifactRoot); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}
	os.Exit(code)
}

func runIsolatedBinaryTest(t *testing.T) bool {
	t.Helper()
	const modeKey = "_TETHER_COMMAND_TEST_HELPER"
	name := t.Name()
	if os.Getenv(modeKey) == name {
		if err := os.Unsetenv(modeKey); err != nil {
			t.Fatal(err)
		}
		return false
	}
	if !commandFixtureIsolationAllowed(flag.CommandLine) {
		return false
	}
	t.Parallel()
	commandFixtureLanes <- struct{}{}
	defer func() { <-commandFixtureLanes }()
	// Use the run's scratch root when it is as short as a RAM slot (/dev/shm/tNN):
	// test scratch must stay off the system SSD, and a longer root would push the
	// isolated child's socket paths past sockaddr_un.
	// origin: 2026-10-23 system-SSD wear remediation (4.5 TB written through /tmp)
	root := "/tmp"
	if scratch := os.TempDir(); len(scratch) <= len("/dev/shm/t00") {
		root = scratch
	}
	dir, err := os.MkdirTemp(root, "tcmd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	testharness.GuardProcessDirectory(t, dir)
	// A Go tool may launch a detached telemetry uploader even for go env.
	// Confine its configuration to this fixture and disable that background
	// service; a surviving product or compiler child must still fail supervision.
	telemetry := filepath.Join(dir, "telemetry")
	if err := os.Mkdir(telemetry, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(telemetry, "mode"), []byte("off"), 0600); err != nil {
		t.Fatal(err)
	}
	probe := exec.CommandContext(t.Context(), "go", "env", "GOCACHE", "GOPATH")
	probe.Env = append(os.Environ(), "TEST_TELEMETRY_DIR="+telemetry)
	cache, err := testharness.Output(probe)
	if err != nil {
		t.Fatalf("read Go cache locations: %v", err)
	}
	paths := strings.Split(strings.TrimSpace(string(cache)), "\n")
	if len(paths) != 2 {
		t.Fatalf("unexpected Go cache locations: %q", cache)
	}
	helper := filepath.Join(dir, "helper.test")
	if err := os.Link(os.Args[0], helper); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, helper, "-test.run=^"+name+"$", "-test.count=1", "-test.v", "-test.parallel="+flag.Lookup("test.parallel").Value.String())
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), modeKey+"="+name,
		"TMPDIR="+dir, "HOME="+dir, "TETHER_HOME="+dir,
		"GOCACHE="+paths[0], "GOPATH="+paths[1], "TEST_TELEMETRY_DIR="+telemetry)
	// Match the matrix's process budget on large hosts. Giving every private
	// daemon all host CPUs multiplies GC workers across concurrent clusters;
	// an explicit caller allocation still takes precedence.
	// origin: S10 internal review CMD-TETHER-BUDGET F2; hot-update S10 plan §11.1
	if os.Getenv("GOMAXPROCS") == "" {
		cmd.Env = append(cmd.Env, "GOMAXPROCS=2")
	}
	// Match the existing CLI isolation contract: group signals belong only to
	// this fixture, while the shared lease supervisor owns escaped descendants.
	// origin: S10 internal review POLISH-B F1 / plan L15
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	out, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("isolated %s: %v\n%s", name, err, out)
	}
	if !bytes.Contains(out, []byte("=== RUN   "+name+"\n")) || !bytes.Contains(out, []byte("--- PASS: "+name+" (")) {
		t.Fatalf("isolated %s did not complete its test body:\n%s", name, out)
	}
	t.Log(strings.TrimSpace(string(out)))
	return true
}

// Exact root dispatch cannot preserve subtest selection or custom fixture
// arguments. Those invocations keep the original in-process execution path.
// origin: hot-update S10 plan §11.1 / POLISH-B
func commandFixtureIsolationAllowed(options *flag.FlagSet) bool {
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

// origin: S10 internal review POLISH-B F1 / plan L17
func TestCommandFixtureIsolationKeepsCallerSelection(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, true},
		{[]string{"-test.run=^TestExample$", "-test.count=1", "-test.timeout=600s", "-test.parallel=2", "-test.v=test2json", "-test.paniconexit0=true"}, true},
		{[]string{"-test.run=^TestExample$/child"}, false},
		{[]string{"-fixture-root=/fixture"}, false},
		{[]string{"-test.shuffle=on"}, false},
		{[]string{"positional"}, false},
	} {
		options := flag.NewFlagSet("fixture", flag.ContinueOnError)
		for _, name := range []string{"test.run", "test.count", "test.timeout", "test.parallel", "test.v", "test.paniconexit0", "test.shuffle", "fixture-root"} {
			options.String(name, "", "")
		}
		if err := options.Parse(tc.args); err != nil {
			t.Fatal(err)
		}
		if got := commandFixtureIsolationAllowed(options); got != tc.want {
			t.Fatalf("isolation=%t, want %t for %v", got, tc.want, tc.args)
		}
	}
}

// The supervisor's successful exit also needs the exact selected body's
// receipt. In particular, an empty selection must never sign a lifecycle.
// origin: S10 internal review POLISH-B F1 / plan L17
func TestCommandFixtureRunsSelectedBody(t *testing.T) {
	wasIsolated := os.Getenv("_TETHER_COMMAND_TEST_HELPER") == t.Name()
	if runIsolatedBinaryTest(t) {
		return
	}
	if os.Getenv("_TETHER_COMMAND_TEST_HELPER") != "" {
		t.Fatal("test dispatch escaped into the selected body")
	}
	if !strings.HasSuffix(os.Getenv("GORACE"), "atexit_sleep_ms=0") {
		t.Fatal("fixture descendants retained race exit padding")
	}
	if wasIsolated {
		if os.Getenv("GOMAXPROCS") == "" {
			t.Fatal("fixture CPU budget was not explicit")
		}
		pgid, err := syscall.Getpgid(os.Getpid())
		if err != nil || pgid != os.Getpid() {
			t.Fatalf("isolated fixture does not own its signal group: pid=%d pgid=%d err=%v", os.Getpid(), pgid, err)
		}
	}
}
