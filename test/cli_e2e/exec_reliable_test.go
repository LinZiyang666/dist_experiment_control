//go:build linux

// exec_reliable_test.go — `tether exec` on the reliable path through a REAL
// ctl binary, a REAL agent subprocess and a real NATS wire, with the faults
// the S1 exit criteria name injected for real (server-side disconnects, a
// nats-server restart on the same port, a broker restart, a TCP relay that
// cuts the connection right after a chosen request frame).
//
// Real: the tether binary as ctl and agent, embedded nats-server, in-process
// broker, real children and pipes on the agent, real auth_callout where the
// test says so. Stubbed: nothing on the exec path. Not covered: PTY (`run`),
// tier-B transfers, Darwin, and any image switch (S2+).
// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.
package cli_e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

const reliableTestBudget = 90 * time.Second

// reliableLine is the agent's audit line for a spawn on the reliable path.
const reliableLine = `msg="agent: exec".*reliable=true`

func requireReliableOnce(t *testing.T, s *realStack, before int) {
	t.Helper()
	if n := s.agentLogCount(reliableLine) - before; n != 1 {
		t.Fatalf("expected exactly one reliable spawn on the agent, saw %d new %q lines", n, reliableLine)
	}
}

// origin: S1 exit #1 — the baseline shape every other test builds on: bytes on
// both streams delivered in order, the remote exit code passed through, and
// the agent log proving the reliable path (not legacy) ran the command once.
func TestExecReliableRealBinaryStreamsAndExitCode(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "marker")
	script, wantOut, wantErr := producer(40, 5*time.Millisecond, marker, 23)
	before := s.agentLogCount(reliableLine)
	res := s.exec(ctx, s.url, nil, nil, "sh", "-c", script)
	if res.rc != 23 || res.stdout != string(wantOut) || res.stderr != string(wantErr) {
		t.Fatalf("rc=%d\nstdout=%q\nstderr=%q", res.rc, res.stdout, res.stderr)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 {
		t.Fatalf("command ran %d times", markerCount(t, marker))
	}
	// Signal death → 128+sig; a large stdout over many batches; --stdin.
	res = s.exec(ctx, s.url, nil, nil, "sh", "-c", "kill -9 $$")
	if res.rc != 137 {
		t.Fatalf("signal death rc=%d stderr=%q", res.rc, res.stderr)
	}
	res = s.exec(ctx, s.url, nil, nil, "sh", "-c", "head -c 200000 /dev/zero | tr '\\0' a; exit 0")
	if res.rc != 0 || len(res.stdout) != 200000 || strings.Trim(res.stdout, "a") != "" {
		t.Fatalf("large stdout: rc=%d len=%d", res.rc, len(res.stdout))
	}
	stdin := []byte(strings.Repeat("0123456789abcdef", 3072)) // 48 KiB
	res = s.exec(ctx, s.url, stdin, []string{"--stdin"}, "sh", "-c", "cat; printf done >&2")
	if res.rc != 0 || res.stdout != string(stdin) || res.stderr != "done" {
		t.Fatalf("--stdin: rc=%d out=%d stderr=%q", res.rc, len(res.stdout), res.stderr)
	}
	if n := s.agentLogCount(reliableLine) - before; n != 4 {
		t.Fatalf("expected 4 reliable spawns, saw %d", n)
	}
}

// runProducerInBackground starts a real `tether exec` of producer(...) in a
// goroutine and returns the channel its result lands on plus the expected
// streams; the caller injects a fault once the marker proves the remote
// command is running.
func runProducerInBackground(t *testing.T, s *realStack, url string, n int, period time.Duration, rc int, extra ...string) (<-chan ctlResult, string, []byte, []byte) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "marker")
	script, wantOut, wantErr := producer(n, period, marker, rc)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	t.Cleanup(cancel)
	done := make(chan ctlResult, 1)
	go func() { done <- s.exec(ctx, url, nil, extra, "sh", "-c", script) }()
	waitMarker(t, marker)
	return done, marker, wantOut, wantErr
}

func awaitProducer(t *testing.T, done <-chan ctlResult) ctlResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(reliableTestBudget):
		t.Fatal("ctl did not return")
	}
	return ctlResult{}
}

// origin: S1 exit #1 (agent NATS session interruption) — the agent's own
// connection is kicked twice while the exec streams; the same daemon (same
// pid) re-registers, nothing is orphan-killed, and the ctl still gets every
// byte and the real exit code.
func TestExecReliableAgentConnKickMidStream(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	before := s.agentLogCount(reliableLine)
	agentPID := s.agentPID()
	done, marker, wantOut, wantErr := runProducerInBackground(t, s, s.url, 300, 20*time.Millisecond, 23)
	time.Sleep(500 * time.Millisecond)
	first := s.kickAgentConn()
	// Wait for the agent's NEW connection (a different client id) to be on
	// the server before kicking again — nats.go's ReconnectWait is 2 s plus
	// jitter, and a bare sleep here was a T1 wall-clock window that a loaded
	// runner would miss (S1 review E8).
	if !testharnessWait(15*time.Second, func() bool { cid, ok := s.agentConnID(); return ok && cid != first }) {
		t.Fatal("the agent never reconnected after the first kick")
	}
	s.kickAgentConn()
	res := awaitProducer(t, done)
	if res.rc != 23 || res.stdout != string(wantOut) || res.stderr != string(wantErr) {
		t.Fatalf("rc=%d stdout=%d/%d bytes stderr=%d/%d bytes\nstderr=%q", res.rc, len(res.stdout), len(wantOut), len(res.stderr), len(wantErr), res.stderr)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 || s.agentPID() != agentPID {
		t.Fatalf("command ran %d times, agent pid %d→%d", markerCount(t, marker), agentPID, s.agentPID())
	}
	// Non-emptiness: the agent really lost its connection twice (nats.go's
	// DisconnectErrHandler logs each loss) and nothing was orphan-killed.
	// origin: hot-update S4 close-out e2e run. That handler runs on nats.go's
	// async callback goroutine, so the second line can land after the producer
	// has already finished; a single read under load saw 1. Wait for the
	// evidence instead of sampling it once — it must still be two losses.
	var disconnects int
	if !testharnessWait(10*time.Second, func() bool {
		disconnects = s.agentLogCount(`msg="agent: NATS disconnected"`)
		return disconnects >= 2
	}) {
		t.Fatalf("the agent connection was not kicked twice (disconnect lines=%d)", disconnects)
	}
	if n := s.agentLogCount(`orphan`); n != 0 {
		t.Fatalf("an orphan kill happened during the reconnects: %d lines", n)
	}
}

// origin: S1 exit #1 (broker restart) — the broker is down for longer than
// StaleAfter, so the node is provably STALE while the exec is in flight;
// attach ops are tolerated and the ctl finishes with every byte.
func TestExecReliableBrokerRestartMidStream(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	before := s.agentLogCount(reliableLine)
	done, marker, wantOut, wantErr := runProducerInBackground(t, s, s.url, 600, 20*time.Millisecond, 23)
	time.Sleep(500 * time.Millisecond)
	s.stopBroker()
	// The reconciler is gone with the broker, so the heartbeat column is
	// the only clock: it must stop advancing for longer than StaleAfter,
	// which is exactly the STALE window the attach spec tolerates.
	time.Sleep(6 * time.Second)
	var lastHB string
	if err := s.db.QueryRow(`SELECT COALESCE(last_heartbeat_at, '') FROM nodes WHERE sid=? AND nid=?`, s.sid, s.nid).Scan(&lastHB); err != nil {
		t.Fatal(err)
	}
	// The column is a Go time.String() (`2006-01-02 15:04:05.999999999 -0700 MST`).
	if hb, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", lastHB); err != nil || time.Since(hb) < 5*time.Second {
		t.Fatalf("the heartbeat kept advancing while the broker was down (last=%q, err=%v): the STALE window was never opened", lastHB, err)
	}
	s.startBroker()
	// Non-emptiness (S1 review E9): the broker's start-up reconcile sees the
	// 6 s gap and marks the row STALE before the next 5 s heartbeat repairs
	// it. This is asserted, not logged — the attach-spec tolerance (gotcha
	// #90) is what this test exists to exercise, and a run that never
	// crossed STALE proved nothing about it.
	sawStale := testharnessWait(3*time.Second, func() bool {
		var status string
		return s.db.QueryRow(`SELECT status FROM nodes WHERE sid=? AND nid=?`, s.sid, s.nid).Scan(&status) == nil && status == "STALE"
	})
	res := awaitProducer(t, done)
	if res.rc != 23 || res.stdout != string(wantOut) || res.stderr != string(wantErr) {
		t.Fatalf("rc=%d stdout=%d/%d stderr=%d/%d\nstderr=%q", res.rc, len(res.stdout), len(wantOut), len(res.stderr), len(wantErr), res.stderr)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 {
		t.Fatalf("command ran %d times", markerCount(t, marker))
	}
	if !sawStale {
		t.Fatal("the node was never observed STALE after the broker restart: the exec did not cross the window this test is about")
	}
}

// origin: S1 exit #1 (nats-server restart) — the server itself goes away for
// a second; ctl, agent and broker all reconnect on their own and the exec
// completes with every byte and the real exit code.
func TestExecReliableNATSRestartMidStream(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	before := s.agentLogCount(reliableLine)
	agentPID := s.agentPID()
	done, marker, wantOut, wantErr := runProducerInBackground(t, s, s.url, 400, 20*time.Millisecond, 23)
	time.Sleep(500 * time.Millisecond)
	s.restartNATS(time.Second)
	res := awaitProducer(t, done)
	if res.rc != 23 || res.stdout != string(wantOut) || res.stderr != string(wantErr) {
		t.Fatalf("rc=%d stdout=%d/%d stderr=%d/%d\nstderr=%q", res.rc, len(res.stdout), len(wantOut), len(res.stderr), len(wantErr), res.stderr)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 || s.agentPID() != agentPID {
		t.Fatalf("command ran %d times, agent pid %d→%d", markerCount(t, marker), agentPID, s.agentPID())
	}
	// Injection self-proof (S1 review e2e-V4): the server really went away
	// under the exec — the agent logged the loss — so a harness that stopped
	// restarting, or a producer that finished before the bounce, cannot
	// pass this test.
	if n := s.agentLogCount(`msg="agent: NATS disconnected"`); n < 1 {
		t.Fatalf("the agent never saw the nats-server restart (disconnect lines=%d)", n)
	}
}

// origin: S1 exit #1 + §1.5 — with the real auth_callout, the broker (the
// only callout responder) comes back well after the server does; every
// reconnecting client hits the black window. The ctl must survive TWO
// consecutive `Authorization Violation`s on the same server — the exact
// condition under which nats.go closes a connection for good unless
// IgnoreAuthErrorAbort is set — and the exec completes.
//
// Shape (S1 review E2): the server's AUTH_TIMEOUT is 0.5 s so an unanswered
// CONNECT is refused with -ERR before the client's own 2 s deadline (with the
// default 2 s tie the client sees `i/o timeout`, which never reaches the
// auth-abort logic and the option is never consulted); the window is longer
// than two reconnect attempts (ReconnectWait 2 s + jitter + 0.5 s each), so
// the second refusal is guaranteed; and ≥2 refusals are asserted from the
// ctl's own stderr. Mutation: dropping IgnoreAuthErrorAbort from
// ctlConnOptions must make this test red (evidence §12).
func TestExecReliableCalloutBlackWindowMidStream(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStackFastAuthTimeout(t, 0.5)
	before := s.agentLogCount(reliableLine)
	done, marker, wantOut, wantErr := runProducerInBackground(t, s, s.url, 700, 20*time.Millisecond, 23)
	time.Sleep(500 * time.Millisecond)
	s.stopBroker()
	s.restartNATS(300 * time.Millisecond)
	time.Sleep(6500 * time.Millisecond) // server up, no callout responder: two refused reconnects
	s.startBroker()
	res := awaitProducer(t, done)
	// The ctl's async NATS error handler prints the black-window refusals
	// (`tether: nats async error: nats: Authorization Violation`) on its own
	// stderr — the documented h1 D2 behaviour, and the non-emptiness proof
	// that the ctl really reconnected into the window. The remote stderr
	// bytes must be complete once those lines are removed.
	remoteErr, ctlLines := splitCtlStderr(res.stderr)
	if res.rc != 23 || res.stdout != string(wantOut) || remoteErr != string(wantErr) {
		t.Fatalf("rc=%d stdout=%d/%d stderr=%d/%d\nstderr=%q", res.rc, len(res.stdout), len(wantOut), len(remoteErr), len(wantErr), res.stderr)
	}
	if n := strings.Count(strings.ToLower(ctlLines), "authorization violation"); n < 2 {
		t.Fatalf("the ctl saw %d authorization violation(s) in the black window, want >= 2 (the abort condition): ctl stderr lines=%q", n, ctlLines)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 {
		t.Fatalf("command ran %d times", markerCount(t, marker))
	}
}

// origin: S1 exit #1 (lost create / terminal-ack reply) — the relay drops the
// reply direction before forwarding the chosen request, then consumes the
// real reply without delivery; the retry keeps the original handle and
// the command still runs exactly once and settles exactly once.
func TestExecReliableLostReplyRunsOnceAndSettlesOnce(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, tc := range []struct {
		name string
		op   string
	}{{"lost_create_reply", `"op":"create"`}, {"lost_terminal_ack_reply", `"op":"terminal-ack"`}} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			s := newRealStack(t, false)
			relay, url := startNATSRelay(t, s.url, func(subject string, payload []byte) bool {
				return strings.Contains(subject, "process-resume.req") && strings.Contains(string(payload), tc.op)
			})
			before := s.agentLogCount(reliableLine)
			marker := filepath.Join(t.TempDir(), "marker")
			script, wantOut, wantErr := producer(60, 20*time.Millisecond, marker, 23)
			ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
			defer cancel()
			res := s.exec(ctx, url, nil, nil, "sh", "-c", script)
			relay.requireLoss(t)
			if res.rc != 23 || res.stdout != string(wantOut) || res.stderr != string(wantErr) {
				t.Fatalf("rc=%d stdout=%d/%d stderr=%d/%d\nstderr=%q", res.rc, len(res.stdout), len(wantOut), len(res.stderr), len(wantErr), res.stderr)
			}
			requireReliableOnce(t, s, before)
			if markerCount(t, marker) != 1 {
				t.Fatalf("command ran %d times", markerCount(t, marker))
			}
			if res.elapsed < 2*time.Second {
				t.Fatalf("the ctl finished in %s: a lost reply must cost at least one ReconnectWait", res.elapsed)
			}
		})
	}
}

// splitCtlStderr separates the ctl's own diagnostics (lines the CLI prints
// as "\r\ntether: ...\r\n" from its NATS error handler) from the remote
// command's stderr bytes.
func splitCtlStderr(stderr string) (remote, ctl string) {
	var remoteB, ctlB strings.Builder
	rest := stderr
	for {
		i := strings.Index(rest, "\r\ntether: ")
		if i < 0 {
			remoteB.WriteString(rest)
			break
		}
		remoteB.WriteString(rest[:i])
		rest = rest[i+2:]
		j := strings.Index(rest, "\r\n")
		if j < 0 {
			ctlB.WriteString(rest)
			break
		}
		ctlB.WriteString(rest[:j+2])
		rest = rest[j+2:]
	}
	return remoteB.String(), ctlB.String()
}

// origin: S1 exit #1 (--timeout) + usage §9.13.2 — a ctl that gives up after
// create is 74 (never the retryable 75), says the remote task was NOT
// killed, and the task really keeps running to its own exit, visible in
// `ps` as EXITED(0).
func TestExecReliableTimeoutAbandonsWithoutRemoteKill(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	done := filepath.Join(t.TempDir(), "done")
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	res := s.exec(ctx, s.url, nil, []string{"--timeout", "2s"}, "sh", "-c", "sleep 4; : > "+done+"; exit 0")
	if res.rc != 74 || res.elapsed > 4*time.Second || !strings.Contains(res.stderr, "NOT killed") {
		t.Fatalf("timeout after create: rc=%d elapsed=%s stderr=%q", res.rc, res.elapsed, res.stderr)
	}
	if !testharnessWait(6*time.Second, func() bool { _, err := os.Stat(done); return err == nil }) {
		t.Fatal("the abandoned task did not keep running to completion")
	}
	if !testharnessWait(10*time.Second, func() bool {
		for _, p := range s.psEntries(ctx, true) {
			if p.Status == "EXITED" && p.ExitCode == 0 && strings.Contains(strings.Join(p.Argv, " "), done) {
				return true
			}
		}
		return false
	}) {
		t.Fatal("ps never showed the abandoned task EXITED(0)")
	}
}

// origin: S1 exit #1 (`ps` visibility) — a reliable task is a first-class
// row: RUNNING while it runs, EXITED with its real code afterwards.
func TestExecReliablePsShowsTaskRunningThenExited(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	nonce := "nonce-" + filepath.Base(t.TempDir())
	before := s.agentLogCount(reliableLine)
	done := make(chan ctlResult, 1)
	go func() { done <- s.exec(ctx, s.url, nil, nil, "sh", "-c", "echo "+nonce+"; sleep 3; exit 23") }()
	if !testharnessWait(10*time.Second, func() bool {
		for _, p := range s.psEntries(ctx, false) {
			if p.Status == "RUNNING" && strings.Contains(strings.Join(p.Argv, " "), nonce) {
				return true
			}
		}
		return false
	}) {
		t.Fatal("ps never showed the running reliable task")
	}
	res := awaitProducer(t, done)
	if res.rc != 23 {
		t.Fatalf("rc=%d stderr=%q", res.rc, res.stderr)
	}
	// Non-emptiness (S1 review E1): the row above was a RELIABLE task's row.
	// A legacy exec also shows up in ps, so without this the test proved
	// nothing about the path it is named for.
	requireReliableOnce(t, s, before)
	if !testharnessWait(10*time.Second, func() bool {
		for _, p := range s.psEntries(ctx, true) {
			if p.Status == "EXITED" && p.ExitCode == 23 && strings.Contains(strings.Join(p.Argv, " "), nonce) {
				return true
			}
		}
		return false
	}) {
		t.Fatal("ps never showed the task EXITED(23)")
	}
}

// origin: S1 exit #3 (a) — a member revoked mid-stream gets the permission
// refusal (77), never a legacy fallback and never a second child.
func TestExecReliableRevokedMemberMidStreamExitsNoPerm(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	before := s.agentLogCount(reliableLine)
	done, marker, _, _ := runProducerInBackground(t, s, s.url, 300, 20*time.Millisecond, 23)
	time.Sleep(300 * time.Millisecond)
	if _, err := s.db.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, s.sid, s.ctlID.Fingerprint); err != nil {
		t.Fatal(err)
	}
	res := awaitProducer(t, done)
	if res.rc != 77 || !strings.Contains(res.stderr, "not_a_member") {
		t.Fatalf("revoked member: rc=%d stderr=%q", res.rc, res.stderr)
	}
	requireReliableOnce(t, s, before)
	if markerCount(t, marker) != 1 || s.agentLogCount(`msg="agent: exec" pid=[^ ]+ argv=`)-s.agentLogCount(reliableLine) != 0 {
		t.Fatalf("a second (legacy) execution happened: markers=%d", markerCount(t, marker))
	}
}

// origin: S1 exit #3 (b) / exit #4 — `session rm` mid-stream: the ctl exits
// 74, the agent kills the session's child (its completion marker never
// appears) and logs the revocation; the daemon stays up.
func TestExecReliableSessionRmMidStreamKillsChild(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	agentPID := s.agentPID()
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	started := filepath.Join(t.TempDir(), "started")
	finished := filepath.Join(t.TempDir(), "finished")
	done := make(chan ctlResult, 1)
	go func() {
		done <- s.exec(ctx, s.url, nil, nil, "sh", "-c", ": > "+started+"; sleep 6; : > "+finished+"; exit 0")
	}()
	if !testharnessWait(10*time.Second, func() bool { _, err := os.Stat(started); return err == nil }) {
		t.Fatal("remote command never started")
	}
	rm := s.ctlCommand(ctx, "session", "rm", s.sid)
	if rm.rc != 0 {
		t.Fatalf("session rm: rc=%d stderr=%q stdout=%q", rm.rc, rm.stderr, rm.stdout)
	}
	res := awaitProducer(t, done)
	if res.rc != 74 {
		t.Fatalf("session rm mid-stream: rc=%d stderr=%q", res.rc, res.stderr)
	}
	if !testharnessWait(5*time.Second, func() bool { return s.agentLogCount(`session revoked`) >= 1 }) {
		t.Fatal("the agent never logged the revocation")
	}
	time.Sleep(7 * time.Second)
	if _, err := os.Stat(finished); err == nil {
		t.Fatal("the revoked child ran to completion")
	}
	if s.agentPID() != agentPID {
		t.Fatal("the agent daemon restarted on session_destroyed")
	}
}

// origin: S1 exit #1 (legacy fallback, quadrant new ctl × old agent) — a node
// served by a legacy responder that ignores process-resume: the ctl probes,
// pays the 2 s tax, falls back to legacy exec and still gets the result;
// no reliable create is ever seen by the legacy node.
func TestExecReliableLegacyFallbackOnOldAgent(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	sim := startLegacyAgentSim(t, s, "old1")
	res := s.execOnNode(ctx, "old1", nil, "echo", "legacy-hello")
	if res.rc != 7 || res.stdout != "legacy-hello\n" || res.elapsed < 2*time.Second {
		t.Fatalf("legacy fallback: rc=%d stdout=%q elapsed=%s stderr=%q", res.rc, res.stdout, res.elapsed, res.stderr)
	}
	if sim.creates.Load() != 0 || sim.execs.Load() != 1 || sim.probes.Load() < 1 {
		t.Fatalf("legacy node saw creates=%d execs=%d probes=%d", sim.creates.Load(), sim.execs.Load(), sim.probes.Load())
	}
}

// origin: S1 exit #1 (legacy fallback, quadrant new ctl × old broker, plan
// §2.8 Q1) — the broker has the v0.6.0 subscription surface: nobody
// subscribes to `process-resume.req`, so the ctl's probe gets the server's
// own no-responders 503. By the S1 ruling (architecture §22.4 item 11) a 503
// is "maybe a restarting broker", so the ctl keeps probing for its full 2 s
// budget and only then falls back to the legacy exec verb, which the broker
// forwards unchanged; today's agent then runs it on the legacy path —
// exactly one `agent: exec` line, none with reliable=true. The old surface
// is proven from the server's connection table before the ctl runs
// (injection self-proof).
func TestExecReliableLegacyFallbackOnOldBroker(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStackLegacyBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	if s.brokerSubscribes(proto.ProcessResumeVerb + ".req") {
		t.Fatal("the broker still subscribes to process-resume.req: the old-broker surface was not installed")
	}
	if !s.brokerSubscribes(".exec.req") {
		t.Fatal("the broker lost its legacy exec subscription too: this is not an old broker, it is a broken one")
	}
	res := s.exec(ctx, s.url, nil, nil, "sh", "-c", "echo legacy-hello; exit 7")
	if res.rc != 7 || res.stdout != "legacy-hello\n" {
		t.Fatalf("legacy fallback on old broker: rc=%d stdout=%q elapsed=%s stderr=%q", res.rc, res.stdout, res.elapsed, res.stderr)
	}
	if res.elapsed < 2*time.Second || res.elapsed > 10*time.Second {
		t.Fatalf("the ctl must spend its whole 2 s probe budget on a 503 (a restarting broker looks the same) and no more: elapsed=%s", res.elapsed)
	}
	if n := s.agentLogCount(`msg="agent: exec".*reliable=true`); n != 0 {
		t.Fatalf("reliable exec lines=%d on an old broker (the reliable create cannot have been forwarded)", n)
	}
	if n := s.agentLogCount(`msg="agent: exec"`); n != 1 {
		t.Fatalf("legacy exec lines=%d, want exactly 1 (executed once, on the legacy path)", n)
	}
}

// origin: S1 exit #1 — closing the ctl's stdout early ends the ctl with
// SIGPIPE (141); the remote truth is unchanged: the command ran once and
// exited with its own code.
func TestExecReliableStdoutClosedEarlyKeepsRemoteTruthful(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "marker")
	script, _, _ := producer(200, 10*time.Millisecond, marker, 23)
	before := s.agentLogCount(reliableLine)
	beforeAny := s.agentLogCount(`msg="agent: exec" pid=`)
	res := s.execWithStdoutClosedAfter(ctx, 10, "sh", "-c", script)
	if res.rc != 141 {
		t.Fatalf("closed stdout: rc=%d stderr=%q", res.rc, res.stderr)
	}
	// Non-emptiness (S1 review E1): SIGPIPE→141 and an EXITED(23) row are
	// what the legacy path produces too; the claim here is that the RELIABLE
	// attach did not respawn the command when its stream broke, so the spawn
	// must be reliable and exactly one — and no legacy spawn beside it.
	requireReliableOnce(t, s, before)
	if n := s.agentLogCount(`msg="agent: exec" pid=`) - beforeAny; n != 1 {
		t.Fatalf("expected exactly one spawn of any kind, saw %d", n)
	}
	if !testharnessWait(15*time.Second, func() bool {
		for _, p := range s.psEntries(ctx, true) {
			if p.Status == "EXITED" && p.ExitCode == 23 {
				return true
			}
		}
		return false
	}) {
		t.Fatal("the remote command's real exit never reached ps")
	}
	if markerCount(t, marker) != 1 {
		t.Fatalf("command ran %d times", markerCount(t, marker))
	}
}

// origin: S1 §2.4 (gotcha #90) negative control — STALE is tolerated, OFFLINE
// is not: with a short OfflineAfter and the agent SIGSTOPped past it, the
// in-flight exec ends 74 (result unknown, handle on stderr) instead of
// hanging to --timeout.
func TestExecReliableNodeOfflineAfterCreateIsUnsettled(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	// Both windows explicit and ordered (S1 review E13): StaleAfter 1 s then
	// OfflineAfter 2 s, so the row really passes through STALE (tolerated)
	// into OFFLINE (refused) — an OfflineAfter below the default 5 s
	// StaleAfter would make STALE unreachable and the test say nothing
	// about the boundary.
	s := newRealStack(t, false, func(cfg *broker.Config) { cfg.StaleAfter = time.Second; cfg.OfflineAfter = 2 * time.Second })
	done, _, _, _ := runProducerInBackground(t, s, s.url, 2000, 20*time.Millisecond, 23)
	time.Sleep(300 * time.Millisecond)
	pid := s.agentPID()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(pid, syscall.SIGCONT) }()
	res := awaitProducer(t, done)
	_ = syscall.Kill(pid, syscall.SIGCONT)
	if res.rc != 74 || !strings.Contains(res.stderr, "node_offline") || !strings.Contains(res.stderr, "task ") {
		t.Fatalf("OFFLINE after create: rc=%d elapsed=%s stderr=%q", res.rc, res.elapsed, res.stderr)
	}
	if res.elapsed > 20*time.Second {
		t.Fatalf("the ctl waited %s for an OFFLINE node", res.elapsed)
	}
}

// origin: S1 exit #2 — the ctl's bounded connect retry: an identity that
// authenticated for this session before rides through a 1.5 s auth_callout
// black window (broker down, server up) and the command succeeds; a fresh
// identity with no receipt makes exactly one attempt and fails fast.
func TestCtlConnectRetriesThroughCalloutBlackWindow(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	s := newRealStack(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), reliableTestBudget)
	defer cancel()
	// Any successful activated command writes the receipt.
	if warm := s.ctlCommand(ctx, "ps", "--json"); warm.rc != 0 {
		t.Fatalf("warm-up ps: rc=%d stderr=%q", warm.rc, warm.stderr)
	}
	if _, err := os.Stat(filepath.Join(s.ctlHome, "auth_receipts", s.sid)); err != nil {
		t.Fatalf("no auth receipt after a successful command: %v", err)
	}
	// Heartbeat phase anchor (S1 review E17): the exec's probe needs the
	// node ONLINE when the broker is back. A heartbeat lost inside the gap
	// leaves the row >StaleAfter old at restart → STALE → the probe (strict
	// spec) is refused with node_offline. Stop the broker right after a
	// heartbeat landed, so the next one falls after the restart.
	s.waitFreshHeartbeat(t, 500*time.Millisecond)
	s.stopBroker()
	done := make(chan ctlResult, 1)
	go func() { done <- s.exec(ctx, s.url, nil, nil, "true") }()
	time.Sleep(1500 * time.Millisecond)
	s.startBroker()
	res := awaitProducer(t, done)
	if res.rc != 0 || res.elapsed > 12*time.Second || !strings.Contains(res.stderr, "retrying in") {
		t.Fatalf("receipted identity through the black window: rc=%d elapsed=%s stderr=%q", res.rc, res.elapsed, res.stderr)
	}
	// Negative control: a fresh home (keys, session, no receipt) during the
	// same kind of window makes one attempt and stops, and exits with the
	// class its mechanism promises (69 unavailable / 77 refused) — never a
	// 70 or a 64 (S1 review ctl-V4).
	fresh := t.TempDir()
	if _, err := cli.EnsureIdentity(fresh); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fresh, "current_session"), []byte(s.sid+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.stopBroker()
	t.Cleanup(s.startBroker)
	args := []string{"exec", "--home", fresh, "--nats-url", s.url, s.nid, "--", "true"}
	cmd := exec.CommandContext(ctx, s.bin, args...)
	cmd.Env = ctlEnv()
	start := time.Now()
	out, _ := testharness.CombinedOutput(cmd)
	elapsed := time.Since(start)
	if strings.Contains(string(out), "retrying in") || elapsed > 5*time.Second {
		t.Fatalf("an unreceipted identity retried: elapsed=%s out=%q", elapsed, out)
	}
	if cmd.ProcessState == nil || (cmd.ProcessState.ExitCode() != 69 && cmd.ProcessState.ExitCode() != 77) {
		t.Fatalf("an unreceipted identity against a down broker exited %v, want 69 or 77: %q", cmd.ProcessState, out)
	}
	// Second negative control (S1 review C3): the RECEIPTED home re-running
	// `login --pin <wrong>` for the same session must fail on the first
	// attempt — the activation verb is excluded from the retry — even though
	// its receipt would make any other verb eligible. The broker is still
	// down, so any retry would show as `retrying in`.
	loginArgs := []string{"login", "--home", s.ctlHome, "--nats-url", s.url, "-s", s.sid, "--pin", "not-the-pin"}
	login := exec.CommandContext(ctx, s.bin, loginArgs...)
	login.Env = ctlEnv()
	start = time.Now()
	loginOut, _ := testharness.CombinedOutput(login)
	if strings.Contains(string(loginOut), "retrying in") || time.Since(start) > 5*time.Second {
		t.Fatalf("login with a wrong PIN retried through the black window: elapsed=%s out=%q", time.Since(start), loginOut)
	}
	if login.ProcessState == nil || login.ProcessState.ExitCode() == 0 {
		t.Fatalf("login with a wrong PIN succeeded against a broker that is down: %q", loginOut)
	}
}
