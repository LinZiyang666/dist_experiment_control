//go:build linux

// broker_control_test.go checks the real RunE/transport/agent seams, keeping
// attempt receipts separate from target effects and using the shared collector.
// origin: hot-update S6 plan §8 / §12.1 P27
package cli_e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S6 plan §8.2 / T30
func TestControlAdmissionAbsoluteBudget(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	s := newRealStack(t, false)
	t.Run("TLS", func(t *testing.T) { exerciseBrokerSecureAdmission(t, s.bin, false) })
	t.Run("WSS", func(t *testing.T) { exerciseBrokerSecureAdmission(t, s.bin, true) })
	t.Run("DNS-is-budgeted-before-NATS", func(t *testing.T) {
		deadline, err := controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		options, err := proxydial.OptionsWithin(func(string) (string, bool) { return "", false }, deadline, nil)
		if err != nil {
			t.Fatal(err)
		}
		applied := nats.GetDefaultOptions()
		for _, option := range options {
			if err := option(&applied); err != nil {
				t.Fatal(err)
			}
		}
		if !applied.SkipHostLookup || applied.CustomDialer == nil || applied.Timeout > time.Second {
			t.Fatal("NATS may enter unbudgeted host lookup or dial")
		}
	})
	t.Run("success", func(t *testing.T) {
		result := runBrokerControl(t.Context(), s, s.ctlHome, strings.Replace(s.url, "127.0.0.1", "localhost", 1), brokerReceiptPath(t), nil, "node", "ls", "--json")
		requireBrokerControl(t, result)
		if result.rc != 0 || brokerReadResult(result.stdout, s.nid) != nil {
			t.Fatalf("node ls: rc=%d out=%q stderr=%q", result.rc, result.stdout, result.stderr)
		}
		for _, phase := range []controlbudget.AttemptPhase{controlbudget.PhaseDNS, controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe, controlbudget.PhaseRequest} {
			if brokerAttemptCount(result.attempts, phase, "") == 0 {
				t.Fatalf("missing real %s phase", phase)
			}
		}
	})
	t.Run("proxy-info-request-share-deadline", func(t *testing.T) {
		proxy := brokerBudgetProxy(t, s.url, 1800*time.Millisecond, 1800*time.Millisecond)
		// Hold the real pool, so admission reaches an actual blocked SQL read
		// after consuming proxy and server INFO time. Release before teardown.
		held, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), 2*upgradebudget.RequestLatency)
		defer cancel()
		result := runBrokerControl(ctx, s, s.ctlHome, strings.Replace(s.url, "127.0.0.1", "broker.invalid", 1), brokerReceiptPath(t), []string{"ALL_PROXY=" + proxy}, "node", "ls", "--json")
		_ = held.Close()
		requireBrokerControl(t, result)
		if result.rc == 0 {
			t.Fatal("blocked current-store read falsely succeeded")
		}
		if result.elapsed >= 2*upgradebudget.RequestLatency {
			t.Fatalf("per-phase budget reset: %s", result.elapsed)
		}
		if brokerAttemptCount(result.attempts, controlbudget.PhaseProxy, "") == 0 || brokerAttemptCount(result.attempts, controlbudget.PhaseRequest, "") == 0 {
			t.Fatal("deep admission did not reach both proxy and request")
		}
	})
}

// A historical receipt permits retry through a real missing callout, but a
// first login remains terminal. The successful operation must carry all failed
// CONNECTs, and the target's /proc identity must exist independently of stdout.
// origin: hot-update S6 plan §8.1–§8.3 / T29 / T30 / T50
func TestBrokerControlAuthBlackoutKeepsAttemptsAndOneEffect(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	s := newRealStack(t, true)
	warm := runBrokerControl(t.Context(), s, s.ctlHome, s.url, brokerReceiptPath(t), nil, "node", "ls", "--json")
	requireBrokerControl(t, warm)
	if warm.rc != 0 || !cli.HasAuthReceipt(s.ctlHome, s.sid, s.ctlID.Fingerprint) {
		t.Fatal("real activated CONNECT did not produce the retry receipt")
	}
	s.waitFreshHeartbeat(t, 500*time.Millisecond)
	s.stopBroker()
	fresh := t.TempDir()
	if _, err := cli.EnsureIdentity(fresh); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fresh, "current_session"), []byte(s.sid+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first := runBrokerControl(t.Context(), s, fresh, s.url, brokerReceiptPath(t), nil, "node", "ls", "--json")
	requireBrokerControl(t, first)
	// CONNECT is observed at several nested layers. Retry is a real backoff,
	// not the number of nested observations for the same socket handshake.
	if first.rc == 0 || brokerAttemptCount(first.attempts, controlbudget.PhaseBackoff, "") != 0 {
		t.Fatal("unreceipted identity retried or authenticated during auth blackout")
	}
	s.startBroker()
	s.waitFreshHeartbeat(t, 500*time.Millisecond)
	s.stopBroker()
	path := filepath.Join(t.TempDir(), "effects.jsonl")
	before := s.agentLogCount(reliableLine)
	ctx, cancel := context.WithTimeout(t.Context(), 2*upgradebudget.RequestLatency)
	defer cancel()
	done := make(chan brokerControlResult, 1)
	go func() {
		done <- runBrokerControl(ctx, s, s.ctlHome, s.url, path+".attempts", nil, "exec", s.nid, "--", "python3", "-I", "-B", "-c", brokerTargetProgram, "blackout-effect", path)
	}()
	// sleep-fixture: measured missing-callout fault duration, not startup readiness.
	timer := time.NewTimer(2100 * time.Millisecond)
	<-timer.C
	s.startBroker()
	result := <-done
	requireBrokerControl(t, result)
	if result.rc != 0 {
		t.Fatalf("blackout admission: rc=%d elapsed=%s stderr=%q", result.rc, result.elapsed, result.stderr)
	}
	if brokerAttemptCount(result.attempts, controlbudget.PhaseConnect, "") < 2 || brokerAttemptCount(result.attempts, controlbudget.PhaseBackoff, "") == 0 {
		t.Fatal("failed CONNECT/backoff samples disappeared before success")
	}
	effects, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateBrokerTarget(result.stdout, effects, "blackout-effect"); err != nil {
		t.Fatal(err)
	}
	// The task may continue after admission. Bound the independent target's
	// actual PID/start witness, while still waiting/reaping its final result.
	var started brokerTargetWitness
	if err := json.Unmarshal([]byte(result.stdout), &started); err != nil {
		t.Fatal(err)
	}
	if started.AtNS < result.startedMonoNS || time.Duration(started.AtNS-result.startedMonoNS) > upgradebudget.RequestLatency {
		t.Fatal("true target start exceeded the original command slot budget")
	}
	requireReliableOnce(t, s, before)
	if brokerAttemptCount(result.attempts, controlbudget.PhaseCreate, "") != 1 {
		t.Fatal("blackout changed the reliable create count")
	}
}

// Use P29's scheduler and judge directly: an independent local scheduler would
// let its slot denominator or attempt binding disagree with the drill collector.
// This cold callout blackout checks collector interoperability. A same-PID cut
// and its freeze events are supplied by P26, never invented by this test.
// origin: hot-update S6 plan §8.3 / T50 / §12.1 P27
func TestBrokerControlBlackoutMixed(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	fixture, err := clusterharness.StartBrokerControlFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: "single"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	configRow, err := fixture.Checkpoint("control-probe-config")
	if err != nil {
		t.Fatal(err)
	}
	var supplied struct {
		Binary, Home, URL, NID, Root string
		ShortNID                     string `json:"short_nid"`
	}
	if err = json.Unmarshal(configRow.JSON, &supplied); err != nil {
		t.Fatal(err)
	}
	if supplied.ShortNID == "" || supplied.ShortNID == supplied.NID {
		t.Fatal("normal mixed profile requires two distinct real agent targets")
	}
	s := &realStack{t: t, bin: supplied.Binary, ctlHome: supplied.Home, url: supplied.URL, nid: supplied.NID, sid: "product"}
	if warm := s.ctlCommand(t.Context(), "node", "ls", "--json"); warm.rc != 0 {
		t.Fatalf("warm-up: %+v", warm)
	}
	registeredID := "0123456789abcdef0123456789abcdef-binding"
	binding := runBrokerControl(t.Context(), s, s.ctlHome, s.url, brokerReceiptPath(t), []string{cli.AttemptLogicalIDEnv + "=" + registeredID}, "node", "ls", "--json")
	if binding.err != nil || binding.rc != 0 || len(binding.attempts) == 0 {
		t.Fatal("collector binding probe did not produce real attempts")
	}
	for _, attempt := range binding.attempts {
		if attempt.LogicalID != registeredID {
			t.Errorf("production sink ignored collector logical ID: got=%q want=%q", attempt.LogicalID, registeredID)
			break
		}
	}
	root := t.TempDir()
	effects, events := filepath.Join(root, "effects.jsonl"), filepath.Join(root, "events.jsonl")
	base := []string{s.bin, "exec", "--home", s.ctlHome, "--nats-url", s.url, s.nid, "--", "python3", "-I", "-B", "-c", brokerTargetProgram, "{logical_id}", effects}
	config := map[string]any{
		"attempt_fd_env": cli.AttemptReceiptFDEnv, "logical_id_env": cli.AttemptLogicalIDEnv, "read_kind": "nodes",
		"commands":       map[string]any{"read": []string{s.bin, "node", "ls", "--home", s.ctlHome, "--nats-url", s.url, "--json"}, "write": append(append([]string(nil), base...), "write"), "short": append([]string{s.bin, "exec", "--home", s.ctlHome, "--nats-url", s.url, supplied.ShortNID, "--", "python3", "-I", "-B", "-c", brokerTargetProgram, "{logical_id}", effects}, "short")},
		"read_alternate": map[string]any{"argv": []string{s.bin, "ps", "--home", s.ctlHome, "--nats-url", s.url, "--json"}, "kind": "processes"},
	}
	body, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "config.json")
	if err := os.WriteFile(configPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	judge, err := filepath.Abs("../simcluster/drills/lib/hot_update_broker_probe.py")
	if err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(root, "evidence")
	// Retain the same raw collector receipts on demand, including failures;
	// copying observations after the run cannot change the timed workload.
	// origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT
	if outputRoot := os.Getenv("TETHER_BLACKOUT_EVIDENCE_DIR"); outputRoot != "" {
		destination, err := os.MkdirTemp(outputRoot, "mixed-")
		if err != nil {
			t.Fatal(err)
		}
		defer retainBlackoutEvidence(t, destination, evidence, supplied.Root, effects)
	}
	scheduler, err := filepath.Abs("../simcluster/drills/lib/hot_update_mixed_probe.py")
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S6 plan §8.3 / T50 / FIX-BLACKOUT-2.
	// One physical core with race instrumentation cannot sustain the live
	// drill's offered rate on every host. Measure the same real operations in
	// joined batches first, then fix the timed rate at half that capacity. A
	// failing calibration is a failure; it cannot discard slow or failed work.
	calibrationRoot := evidence + "-calibration"
	calibrationConfig := filepath.Join(root, "calibration-config.json")
	if err := os.WriteFile(calibrationConfig, bytes.ReplaceAll(body, []byte(effects), []byte(effects+".calibration")), 0o600); err != nil {
		t.Fatal(err)
	}
	calibrate := exec.CommandContext(t.Context(), "python3", "-I", "-B", scheduler, "broker-run", "--calibrate", "--config", calibrationConfig, "--evidence", calibrationRoot, "--events", filepath.Join(root, "no-cut-events.jsonl"), "--duration", "24", "--late-tail", "5", "--nonce", "fedcba9876543210fedcba9876543210", "--hop", "calibration")
	calibrate.Env = ctlEnv()
	calibrate.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	// SIGINT enters the collector's lexical child cleanup and waitpid path.
	calibrate.Cancel = func() error { return calibrate.Process.Signal(os.Interrupt) }
	calibrate.WaitDelay = 5 * time.Second
	// Each collector/judge owns a supervisor lease, including short commands;
	// successful completion must join descendants as well as the Python parent.
	// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
	if output, err := testharness.CombinedOutput(calibrate); err != nil {
		t.Fatalf("mixed control calibration collector: %v\n%s", err, output)
	}
	calibrationJudge := exec.CommandContext(t.Context(), "python3", "-I", "-B", judge, "judge", "--evidence", calibrationRoot, "--effects", effects+".calibration")
	calibrationVerdict, calibrationErr := testharness.CombinedOutput(calibrationJudge)
	if err := os.WriteFile(filepath.Join(calibrationRoot, "verdict.json"), calibrationVerdict, 0o600); err != nil {
		t.Fatal(err)
	}
	if calibrationErr != nil {
		t.Fatalf("mixed control calibration oracle: %v\n%s", calibrationErr, calibrationVerdict)
	}
	calibrationPath := filepath.Join(calibrationRoot, "calibration.json")
	calibrationBody, err := os.ReadFile(calibrationPath)
	if err != nil {
		t.Fatal(err)
	}
	var calibration struct {
		IntervalNS int64 `json:"interval_ns"`
	}
	if err := json.Unmarshal(calibrationBody, &calibration); err != nil || calibration.IntervalNS < int64(100*time.Millisecond) {
		t.Fatalf("invalid mixed control calibration: %v", err)
	}
	// Keep at least twenty completed operations per class on BOTH sides of
	// the cut. This lengthens the sample horizon, never an operation's budget.
	baselineDuration := max(7*time.Second, 66*time.Duration(calibration.IntervalNS))
	workloadDuration := 2*baselineDuration + 8*time.Second
	if workloadDuration+upgradebudget.RequestLatency >= time.Minute {
		t.Fatal("calibrated host capacity cannot supply both cohorts within the collector deadline")
	}
	t.Logf("mixed control calibration: interval=%s rate=%.3f/s fraction=1/2 baseline=%s workload=%s", time.Duration(calibration.IntervalNS), float64(time.Second)/float64(calibration.IntervalNS), baselineDuration, workloadDuration)
	watcher := exec.CommandContext(t.Context(), "python3", "-I", "-B", judge, "watch-local", "--log-file", filepath.Join(supplied.Root, "product.stderr"), "--events", events, "--hop", "callout-cut", "--duration", "40")
	var watchOutput bytes.Buffer
	watcher.Stdout, watcher.Stderr = &watchOutput, &watchOutput
	watchWait, err := testharness.StartProcess(watcher)
	if err != nil {
		t.Fatal(err)
	}
	watchDone := make(chan error, 1)
	go func() { watchDone <- watchWait() }()
	watchJoined := false
	defer func() {
		if !watchJoined {
			_ = watcher.Process.Kill()
			<-watchDone
		}
	}()
	if !testharnessWait(5*time.Second, func() bool { _, err := os.Stat(strings.TrimSuffix(events, ".jsonl") + ".started"); return err == nil }) {
		t.Fatal("phase collector did not start")
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", scheduler, "broker-run", "--config", configPath, "--evidence", evidence, "--events", events, "--duration", strconv.FormatFloat(workloadDuration.Seconds(), 'f', 9, 64), "--calibration", calibrationPath, "--late-tail", "5", "--nonce", "0123456789abcdef0123456789abcdef", "--hop", "callout-cut")
	cmd.Env = ctlEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	joined := false
	defer func() {
		if !joined {
			// SIGINT runs the collector's lexical child cleanup. SIGKILLing
			// the scheduler cannot reap CLI children with separate sessions.
			_ = cmd.Process.Signal(os.Interrupt)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
		}
	}()
	if !testharnessWait(5*time.Second, func() bool { _, err := os.Stat(filepath.Join(evidence, "started.json")); return err == nil }) {
		t.Fatal("collector failed to start")
	}
	// Wait for actual pre-cut results, not an assumed startup sleep. A collector
	// failure is observed immediately and the exact native error is preserved.
	baseline := time.NewTimer(baselineDuration)
	defer baseline.Stop()
	select {
	case err := <-done:
		joined = true
		t.Fatalf("production collector interoperability: %v\n%s", err, &output)
	case <-baseline.C:
	}
	// origin: hot-update S6 plan §8.1 / T50
	// The real cut emits all three windows. No restart-time interval is supplied
	// as a substitute for a freeze or authentication receipt.
	// Establish connecting OPEN only after the calibrated control baseline;
	// its real operation retains the five-second business deadline.
	if _, err := fixture.Checkpoint("before-reload"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Checkpoint("reload-control-ready"); err != nil {
		t.Fatal(err)
	}
	watchErr := <-watchDone
	watchJoined = true
	if watchErr != nil {
		t.Fatalf("product phase collector: %v %s", watchErr, &watchOutput)
	}
	if _, err := fixture.Checkpoint("write/control-current"); err != nil {
		t.Fatal(err)
	}
	err = <-done
	joined = true
	if err != nil {
		t.Fatalf("collector: %v\n%s", err, &output)
	}
	judge, err = filepath.Abs("../simcluster/drills/lib/hot_update_broker_probe.py")
	if err != nil {
		t.Fatal(err)
	}
	oracle := exec.CommandContext(ctx, "python3", "-I", "-B", judge, "judge", "--evidence", evidence, "--effects", effects)
	verdict, err := testharness.CombinedOutput(oracle)
	if writeErr := os.WriteFile(filepath.Join(evidence, "verdict.json"), verdict, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil {
		attemptData, _ := os.ReadFile(filepath.Join(evidence, "attempts.jsonl"))
		for _, line := range bytes.Split(attemptData, []byte{'\n'}) {
			var row controlbudget.Attempt
			if json.Unmarshal(line, &row) == nil && strings.HasSuffix(row.LogicalID, "-109") {
				t.Logf("109 attempt: %+v", row)
			}
		}
		productLog, _ := os.ReadFile(filepath.Join(supplied.Root, "product.stderr"))
		agentLog, _ := os.ReadFile(filepath.Join(supplied.Root, "agent.stderr"))
		t.Logf("agent exec records=%d", bytes.Count(agentLog, []byte(`"msg":"agent: exec"`)))
		shortLog, _ := os.ReadFile(filepath.Join(supplied.Root, "short-agent.stderr"))
		for _, data := range [][]byte{productLog, agentLog, shortLog} {
			for _, line := range bytes.Split(data, []byte{'\n'}) {
				var row struct {
					Msg   string `json:"msg"`
					Event string `json:"event"`
					Boot  int64  `json:"boot_ns"`
					Level string `json:"level"`
					Err   string `json:"err"`
				}
				if json.Unmarshal(line, &row) == nil && (row.Event != "" || row.Level == "ERROR" || row.Level == "WARN") {
					t.Logf("product event: %s %s %d %s", row.Msg, row.Event, row.Boot, row.Err)
				}
			}
		}
		diagnostics, _ := os.ReadFile(filepath.Join(evidence, "child-output.jsonl"))
		for _, line := range bytes.Split(diagnostics, []byte{'\n'}) {
			var row struct {
				LogicalID string `json:"logical_id"`
				RC        int    `json:"rc"`
				Stderr    string `json:"stderr"`
			}
			if json.Unmarshal(line, &row) == nil && row.RC != 0 {
				t.Logf("failed logical operation %s: rc=%d %s", row.LogicalID, row.RC, row.Stderr)
			}
		}
		t.Fatalf("mixed control oracle: %v\n%s", err, verdict)
	}
}

// origin: hot-update S6 plan §8 / T57Integration
func TestBrokerControlIntegrationLeakCycles(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	buildTether(t) // The immutable build is setup, not a per-cycle leaked owner.
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRealCalloutCut(t)
	}
	testharness.AssertNoGoroutineLeak(t, "real callout control cycles", beforeG)
	testharness.AssertNoFDLeak(t, "real callout control cycles", beforeFD, 0)
}

func exerciseRealCalloutCut(t *testing.T) {
	t.Helper()
	t.Run("callout-cut", func(t *testing.T) {
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
		s := newRealStack(t, true)
		read := runBrokerControl(t.Context(), s, s.ctlHome, s.url, brokerReceiptPath(t), nil, "node", "ls", "--json")
		requireBrokerControl(t, read)
		if read.rc != 0 {
			t.Fatal("initial real read failed")
		}
		s.stopBroker()
		fresh := t.TempDir()
		if _, err := cli.EnsureIdentity(fresh); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fresh, "current_session"), []byte(s.sid+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		refusal := runBrokerControl(t.Context(), s, fresh, s.url, brokerReceiptPath(t), nil, "node", "ls", "--json")
		requireBrokerControl(t, refusal)
		if refusal.rc == 0 {
			t.Fatal("missing callout admitted a new identity")
		}
		s.startBroker()
		proxy := brokerBudgetProxy(t, s.url, time.Second, 0)
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		canceled := runBrokerControl(ctx, s, s.ctlHome, strings.Replace(s.url, "127.0.0.1", "broker.invalid", 1), brokerReceiptPath(t), []string{"ALL_PROXY=" + proxy}, "node", "ls", "--json")
		cancel()
		if canceled.rc == 0 && canceled.err == nil {
			t.Fatal("canceled proxy admission succeeded")
		}
		path := filepath.Join(t.TempDir(), "effect.jsonl")
		result := runBrokerControl(t.Context(), s, s.ctlHome, s.url, brokerReceiptPath(t), nil, "exec", s.nid, "--", "python3", "-I", "-B", "-c", brokerTargetProgram, "cycle", path)
		requireBrokerControl(t, result)
		if result.rc != 0 {
			t.Fatalf("normal reliable admission failed: %s", result.stderr)
		}
		effect, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateBrokerTarget(result.stdout, effect, "cycle"); err != nil {
			t.Fatal(err)
		}
	})
}

// These controls keep receipt loss and acceptance-only compensation observable
// even if a product regression prevents the expensive real stack from starting.
// origin: hot-update S6 plan §8.3 / T50
func TestBrokerControlEvidenceRejectsCompensation(t *testing.T) {
	rows := []controlbudget.Attempt{
		{LogicalID: "slot", Ordinal: 1, Phase: controlbudget.PhaseConnect, StartedMonoNS: 1, FinishedMonoNS: 2, Class: "auth"},
		{LogicalID: "slot", Ordinal: 2, Phase: controlbudget.PhaseConnect, StartedMonoNS: 3, FinishedMonoNS: 4, Class: "success"},
		{LogicalID: "slot", Ordinal: 3, Phase: controlbudget.PhaseResult, StartedMonoNS: 5, FinishedMonoNS: 6, Class: "success"},
	}
	if err := validateBrokerAttempts(rows); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	for _, row := range rows {
		if err := json.NewEncoder(&encoded).Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	closeRow := `{"kind":"observer_close","logical_id":"slot","attempt_count":3,"overflow":false}`
	if decoded, err := decodeBrokerControlAttempts(strings.NewReader(encoded.String() + closeRow)); err != nil || len(decoded) != len(rows) {
		t.Fatalf("valid production close receipt: %v", err)
	}
	for _, closeMutation := range []string{"", strings.Replace(closeRow, `"slot"`, `"foreign"`, 1), strings.Replace(closeRow, `:3`, `:2`, 1), strings.Replace(closeRow, `false`, `true`, 1), closeRow + "\n" + closeRow} {
		if _, err := decodeBrokerControlAttempts(strings.NewReader(encoded.String() + closeMutation)); err == nil {
			t.Fatal("forged, overflowing or duplicate close receipt was accepted")
		}
	}
	for name, mutant := range map[string][]controlbudget.Attempt{"lost-failure": rows[1:], "lost-CONNECT": rows[2:], "lost-result": rows[:2]} {
		t.Run(name, func(t *testing.T) {
			if validateBrokerAttempts(mutant) == nil {
				t.Fatal("missing production attempt was compensated")
			}
		})
	}
	identity := brokerTargetWitness{LogicalID: "slot", PID: 1, StartTicks: 2, BootID: "boot", AtNS: 3}
	stdout := `{"kind":"broker-probe-started-v1","logical_id":"slot","pid":1,"start_ticks":2,"boot_id":"boot","at_ns":3}`
	effect, _ := json.Marshal(identity)
	if err := validateBrokerTarget(stdout, effect, "slot"); err != nil {
		t.Fatal(err)
	}
	if validateBrokerTarget(stdout, nil, "slot") == nil {
		t.Fatal("CLI receipt compensated for a missing independent target effect")
	}
	for _, mutant := range []string{`{"accepted":true}`, `{}`, strings.Replace(stdout, `"pid":1`, `"pid":2`, 1)} {
		if validateBrokerTarget(mutant, effect, "slot") == nil {
			t.Fatal("acceptance or changed identity replaced the true target")
		}
	}
	if validateBrokerTarget(stdout, append(append(effect, '\n'), effect...), "slot") == nil {
		t.Fatal("duplicate target effect was accepted")
	}
	late := append([]controlbudget.Attempt(nil), rows...)
	late[2].FinishedMonoNS = int64(upgradebudget.RequestLatency) + 2
	if validateBrokerAttempts(late) == nil {
		t.Fatal("per-phase budget reset was accepted")
	}
}

// The actual agent advertises support and consumes the actual register reply.
// A type field or a locally manufactured reply cannot establish installation.
// Observe both frames on the existing bus before launching the real agent.
// origin: hot-update S6 plan §7.3 / §9.2 / T29 / T50
func TestBrokerMaintenanceDeliveryUsesInstalledCapability(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	var mu sync.Mutex
	var request proto.NodeRegisterReq
	var replySubject string
	replies := make(map[string]proto.NodeRegisterResp)
	s := bootRealStack(t, &realStack{callout: true, beforeAgent: func(s *realStack) {
		key, err := nkeys.FromSeed(s.brokerSeed)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(key.Wipe)
		pub, err := key.PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		monitor, err := nats.Connect(s.url, nats.Name("maintenance-wire-observer"), nats.Nkey(pub, key.Sign), nats.MaxReconnects(0))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(monitor.Close)
		if _, err := monitor.Subscribe(proto.SubjNodeRegister(s.sid, s.nid), func(msg *nats.Msg) {
			var decoded proto.NodeRegisterReq
			if json.Unmarshal(msg.Data, &decoded) == nil && decoded.NID == s.nid {
				mu.Lock()
				request, replySubject = decoded, msg.Reply
				mu.Unlock()
			}
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := monitor.Subscribe(auth.InboxRoot+".>", func(msg *nats.Msg) {
			var decoded proto.NodeRegisterResp
			if json.Unmarshal(msg.Data, &decoded) == nil && decoded.OK {
				mu.Lock()
				replies[msg.Subject] = decoded
				mu.Unlock()
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := monitor.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
	}})
	if !testharnessWait(time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, found := replies[replySubject]
		return replySubject != "" && found
	}) {
		t.Fatal("missing actual agent register request/reply pair")
	}
	mu.Lock()
	offered, accepted := request.BrokerImageMaintenance, replies[replySubject].BrokerImageMaintenance
	mu.Unlock()
	if offered != brokermaint.Capability {
		t.Fatal("real agent did not advertise maintenance support")
	}
	if accepted != brokermaint.Capability {
		t.Fatalf("broker did not install/echo the offered maintenance capability: offered=%q accepted=%q", offered, accepted)
	}
	// Ordinary work must remain available after negotiating the additive field.
	read := runBrokerControl(t.Context(), s, s.ctlHome, s.url, brokerReceiptPath(t), nil, "node", "ls", "--json")
	requireBrokerControl(t, read)
	if read.rc != 0 || brokerReadResult(read.stdout, s.nid) != nil {
		t.Fatal("maintenance negotiation changed ordinary control admission")
	}
}

func retainBlackoutEvidence(t *testing.T, destination, evidence, product, effects string) {
	t.Helper()
	paths := []string{effects, effects + ".calibration"}
	for _, name := range []string{"attempts.jsonl", "results.jsonl", "slots.jsonl", "windows.jsonl", "run.json", "child-output.jsonl", "calibration.json", "verdict.json"} {
		paths = append(paths, filepath.Join(evidence, name), filepath.Join(evidence+"-calibration", name))
	}
	for _, name := range []string{"product.stderr", "agent.stderr", "short-agent.stderr"} {
		paths = append(paths, filepath.Join(product, name))
	}
	for _, path := range paths {
		source, err := os.Open(path)
		if os.IsNotExist(err) {
			continue // An early failure may precede the collector's first receipt.
		}
		if err != nil {
			t.Error(err)
			continue
		}
		name := filepath.Base(path)
		if filepath.Dir(path) == evidence+"-calibration" {
			name = "calibration-" + name
		}
		target, err := os.OpenFile(filepath.Join(destination, name), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if err == nil {
			_, err = io.Copy(target, source)
			if closeErr := target.Close(); err == nil {
				err = closeErr
			}
		}
		_ = source.Close()
		if err != nil {
			t.Error(err)
		}
	}
	t.Logf("mixed control receipts: %s", destination)
}
