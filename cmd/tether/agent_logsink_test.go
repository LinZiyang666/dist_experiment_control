package main

// agent_logsink_test.go — the h1 F agent sink: where slog goes, and where a
// PANIC goes. The panic half is the mechanism that rescues the fleet's
// frozen-fd agents (they self-upgrade via syscall.Exec, so their inherited
// fd 2 follows the rotation chain into an unlinked inode), and it was
// shipping untested.
// origin: internal review (gate/test-quality lens).

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/logrotate"
)

func silentTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRedirectStderrToLandsPanicText runs a CHILD process that re-points its
// own fd 2 through the production helper and then panics. Asserting on a real
// panic requires a real process: the runtime writes stacktraces to fd 2
// directly, bypassing every writer this package could inject.
func TestRedirectStderrToLandsPanicText(t *testing.T) {
	if os.Getenv("TETHER_TEST_PANIC_SINK") != "" {
		// Child half.
		redirectStderrTo(os.Getenv("TETHER_TEST_PANIC_SINK"), silentTestLogger())
		panic("h1-panic-sink-canary")
	}
	dir := t.TempDir()
	sink := filepath.Join(dir, "agent.boot.err")

	cmd := exec.Command(os.Args[0], "-test.run=TestRedirectStderrToLandsPanicText")
	cmd.Env = append(os.Environ(), "TETHER_TEST_PANIC_SINK="+sink)
	// Child stderr goes to a DIFFERENT file: if the dup2 does not happen, the
	// panic lands here and the sink stays empty — which is the failure this
	// test exists to catch.
	inherited := filepath.Join(dir, "inherited.err")
	f, err := os.Create(inherited)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = f
	_ = cmd.Run() // expected to fail: it panics
	_ = f.Close()

	got, err := os.ReadFile(sink)
	if err != nil {
		t.Fatalf("panic sink %s was never created: %v", sink, err)
	}
	if !strings.Contains(string(got), "h1-panic-sink-canary") {
		t.Fatalf("panic text did not land in the process-owned sink; sink=%q inherited=%q",
			truncateForMsg(string(got)), truncateForMsg(readOrEmpty(inherited)))
	}
	if !strings.Contains(string(got), "goroutine ") {
		t.Fatalf("sink has the panic line but no stacktrace — fd 2 was only partly redirected: %q",
			truncateForMsg(string(got)))
	}
}

// TestResolveAgentLogSinkPrecedence pins the sink resolution: explicit "-"
// means stderr (and NO panic-sink redirect), a yaml path is honored, and the
// default is the per-session agent.log with agent.boot.err beside it — the
// binary default being the only channel that reaches the frozen-argv fleet.
func TestResolveAgentLogSinkPrecedence(t *testing.T) {
	home := t.TempDir()

	t.Run("default is the per-session file plus a sibling boot.err", func(t *testing.T) {
		cmd := newAgentCmd()
		w, bootErr := resolveAgentLogSink(cmd, home, "lab", "", agentYAML{})
		rw, ok := w.(*logrotate.Writer)
		if !ok {
			t.Fatalf("default sink is %T, want *logrotate.Writer (the cap must be on by default)", w)
		}
		wantLog := filepath.Join(home, "agent", "lab", "agent.log")
		if rw.Path() != wantLog {
			t.Fatalf("default log path = %q, want %q", rw.Path(), wantLog)
		}
		if want := filepath.Join(home, "agent", "lab", "agent.boot.err"); bootErr != want {
			t.Fatalf("panic sink = %q, want %q (sibling of the rotating log)", bootErr, want)
		}
	})

	t.Run("dash opts out to stderr and disables the redirect", func(t *testing.T) {
		cmd := newAgentCmd()
		w, bootErr := resolveAgentLogSink(cmd, home, "lab", "-", agentYAML{})
		if w != os.Stderr {
			t.Fatalf("'-' sink is %T, want os.Stderr", w)
		}
		if bootErr != "" {
			t.Fatalf("'-' must not arm a panic sink, got %q — fd 2 IS the destination already", bootErr)
		}
	})

	t.Run("yaml path is honored", func(t *testing.T) {
		cmd := newAgentCmd()
		custom := filepath.Join(t.TempDir(), "nested", "custom.log")
		w, bootErr := resolveAgentLogSink(cmd, home, "lab", "", agentYAML{LogFile: custom})
		rw, ok := w.(*logrotate.Writer)
		if !ok {
			t.Fatalf("yaml sink is %T, want *logrotate.Writer", w)
		}
		if rw.Path() != custom {
			t.Fatalf("yaml log path = %q, want %q", rw.Path(), custom)
		}
		if want := filepath.Join(filepath.Dir(custom), "agent.boot.err"); bootErr != want {
			t.Fatalf("panic sink = %q, want %q", bootErr, want)
		}
	})
}

// TestAgentDaemonArmsPanicSink pins the WIRING, not just the helper: the
// daemon's RunE must actually call redirectStderrTo with the path
// resolveAgentLogSink returned. Deleting that one line is invisible to every
// behavioral test (running the real daemon needs NATS + an identity), and it
// would silently un-arm the mechanism that rescues the frozen-fd fleet — so
// this is a source-level assertion, in the same spirit as the reply-egress
// census. It catches the deletion, which is the mutation that matters.
func TestAgentDaemonArmsPanicSink(t *testing.T) {
	src, err := os.ReadFile("agent.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, "logSink, bootErrPath := resolveAgentLogSink(") {
		t.Fatal("the agent daemon no longer resolves its log sink + panic-sink path together — " +
			"resolveAgentLogSink's second return IS the panic destination")
	}
	if !strings.Contains(body, "redirectStderrTo(bootErrPath, logger)") {
		t.Fatal("the agent daemon no longer re-points fd 2 at the panic sink. A fleet agent's " +
			"inherited fd 2 follows the rotation chain into an unlinked inode, so without this " +
			"call every future panic/stacktrace lands where nobody can read it (h1 F).")
	}
	if !strings.Contains(body, "newLoggerTo(logLevel, logJSON, logSink)") {
		t.Fatal("the agent daemon no longer writes slog through the resolved (size-capped) sink")
	}
}

// TestPanicSinkStaysBoundedAcrossRestarts is the h1 external review F4
// regression: the panic sink is opened O_APPEND and fd 2 must stay a RAW fd
// (the Go runtime writes panics straight to it), so the only place a bound can
// be applied is at OPEN — which is also where a crash loop passes on every
// restart. Simulate many restarts, each writing to the sink, and assert the
// on-disk footprint stays bounded instead of growing without limit.
// origin: docs/reviews/h1-external-review.md F4.
func TestPanicSinkStaysBoundedAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	sink := filepath.Join(dir, "agent.boot.err")
	// One "crash" writes ~256KB — four restarts would already blow a 1MiB cap
	// if nothing rotated.
	blob := strings.Repeat("stacktrace line\n", 16*1024)

	realStderr, err := syscallDupStderr()
	if err != nil {
		t.Skipf("cannot save fd 2 on this platform: %v", err)
	}
	t.Cleanup(func() { _ = restoreStderr(realStderr) })

	for restart := 0; restart < 20; restart++ {
		redirectStderrTo(sink, silentTestLogger())
		if _, err := os.Stderr.WriteString(blob); err != nil {
			t.Fatalf("restart %d: write to redirected stderr: %v", restart, err)
		}
	}
	_ = restoreStderr(realStderr)

	total := int64(0)
	for _, p := range []string{sink, sink + ".1"} {
		if st, err := os.Stat(p); err == nil {
			total += st.Size()
		}
	}
	// Bound: live ≤ cap at open + this process's own write, plus one retained
	// predecessor of the same shape.
	limit := int64(2*bootErrMaxBytes + 2*len(blob))
	if total > limit {
		t.Fatalf("panic sink grew to %d bytes over 20 restarts (limit %d) — an append-only sink "+
			"re-fills the disk h1 exists to protect, one crash-loop restart at a time", total, limit)
	}
	if _, err := os.Stat(sink); err != nil {
		t.Fatalf("live panic sink missing after the run: %v", err)
	}
}

func readOrEmpty(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	return string(b)
}

func truncateForMsg(s string) string {
	if len(s) > 300 {
		return s[:300] + "…"
	}
	return s
}

// syscallDupStderr / restoreStderr save and restore the test process's own
// fd 2 around a test that re-points it. Without this, the redirect would
// swallow the test binary's remaining output (including failure messages).
func syscallDupStderr() (*os.File, error) {
	fd, err := dupFD(2)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "saved-stderr"), nil
}

func restoreStderr(saved *os.File) error {
	if saved == nil {
		return nil
	}
	if err := dupOntoStderr(int(saved.Fd())); err != nil {
		return err
	}
	return saved.Close()
}
