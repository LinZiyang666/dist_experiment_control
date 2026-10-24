//go:build linux

// Real node upgrade receipts: the ctl, exporter and importer are product
// binaries. Only artifact delivery and the broker/NATS hosts are in-process.
// origin: hot-update S3 plan §7 T16.
package cli_e2e_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// These tests are serial, like buildTether's callers. Keeping variants under
// tetherBinDir gives TestMain ownership even when an assertion fails.
var upgradeArtifacts = map[string]upgradeArtifact{}

type upgradeArtifact struct {
	tarball []byte
	tarSHA  string
	binSHA  string
}

func buildUpgradeArtifact(t *testing.T, version string) upgradeArtifact {
	t.Helper()
	if testharness.ReleaseBudgetRun(t) {
		return buildReleaseUpgradeArtifact(t, version)
	}
	if a, ok := upgradeArtifacts[version]; ok {
		return a
	}
	buildTether(t)
	out := filepath.Join(tetherBinDir, "tether-"+version)
	cmd := tetherBuildCmd(out) // inherits race instrumentation AND GOFLAGS=-overlay
	cmd.Args = append(cmd.Args[:len(cmd.Args)-1], "-ldflags",
		"-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion="+version, "./cmd/tether")
	cmd.Dir = "../.."
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build upgrade candidate: %v\n%s", err, output)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	a := packUpgradeArtifact(t, body)
	upgradeArtifacts[version] = a
	return a
}

func packUpgradeArtifact(t *testing.T, body []byte) upgradeArtifact {
	t.Helper()
	var buf bytes.Buffer
	// Compression ratio is not under test. Even BestSpeed spends 8–10 seconds
	// scanning an instrumented binary under -race; stored gzip blocks retain
	// the real tar/gzip download/extraction contract without that fixture work.
	// origin: hot-update S4 plan §8.4; CLI-LATENCY artifact phase profile.
	gz, err := gzip.NewWriterLevel(&buf, gzip.NoCompression)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(gz)
	// extractTetherBinary accepts the literal root entry "tether".
	if err := tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0o755, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return upgradeArtifact{tarball: buf.Bytes(), tarSHA: fmt.Sprintf("%x", sha256.Sum256(buf.Bytes())), binSHA: fmt.Sprintf("%x", sha256.Sum256(body))}
}

func serveUpgradeArtifacts(t *testing.T, artifacts ...upgradeArtifact) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	for i, a := range artifacts {
		mux.HandleFunc(fmt.Sprintf("/%d.tar.gz", i), func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(a.tarball)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newUpgradeStack(t *testing.T, srv *httptest.Server, configure ...func(*realStack)) *realStack {
	t.Helper()
	src, err := os.Open(buildTether(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	bin := filepath.Join(t.TempDir(), "tether")
	dst, err := os.OpenFile(bin, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(dst, src)
	closeErr := dst.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy sandbox binary: copy=%v close=%v", copyErr, closeErr)
	}
	s := &realStack{agentBin: bin,
		agentArgs: []string{"--upgrade-url-allow", srv.URL + "/"},
		brokerOpts: []func(*broker.Config){func(c *broker.Config) {
			c.UpgradeURLAllowlist = []string{srv.URL + "/"}
			// Download, smoke probe and fsync use the product forward budget.
			c.UpgradeForwardTimeoutDur = 0
		}},
	}
	for _, f := range configure {
		f(s)
	}
	return bootRealStack(t, s)
}

func upgradeFileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// A closed completion channel lets assertions and cleanup observe the same
// original ctl without competing to reap it after an early test failure.
type upgradeCtl struct {
	cmd         *exec.Cmd
	out, errOut string
	done        chan struct{}
	err         error
}

func startUpgradeCtl(t *testing.T, s *realStack, verb string, argv ...string) *upgradeCtl {
	t.Helper()
	return startUpgradeCtlBinary(t, s, s.bin, verb, argv...)
}

// Publish the original ctl result only after the shared supervisor has joined
// its descendants; keep the target PID for cross-image identity assertions.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures.
func startUpgradeCtlBinary(t *testing.T, s *realStack, bin, verb string, argv ...string) *upgradeCtl {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*upgradebudget.ComponentHandover+30*time.Second)
	args := append([]string{verb, "--home", s.ctlHome, "--nats-url", s.url, s.nid, "--"}, argv...)
	p := &upgradeCtl{cmd: exec.CommandContext(ctx, bin, args...), done: make(chan struct{}),
		out: filepath.Join(t.TempDir(), "stdout"), errOut: filepath.Join(t.TempDir(), "stderr")}
	p.cmd.Env = append(ctlEnv(), "TETHER_HOME="+s.ctlHome)
	for _, stream := range []struct {
		path   string
		target *io.Writer
	}{{p.out, &p.cmd.Stdout}, {p.errOut, &p.cmd.Stderr}} {
		f, err := os.Create(stream.path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		*stream.target = f
	}
	wait, err := testharness.StartProcess(p.cmd)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() { p.err = wait(); close(p.done) }()
	t.Cleanup(func() { cancel(); <-p.done })
	return p
}

func (p *upgradeCtl) result(t *testing.T) ctlResult {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(reliableTestBudget):
		t.Fatal("original ctl did not return")
	}
	if p.cmd.ProcessState == nil {
		t.Fatalf("ctl could not be waited: %v", p.err)
	}
	out, err := os.ReadFile(p.out)
	if err != nil {
		t.Fatal(err)
	}
	errOut, err := os.ReadFile(p.errOut)
	if err != nil {
		t.Fatal(err)
	}
	return ctlResult{rc: p.cmd.ProcessState.ExitCode(), stdout: string(out), stderr: string(errOut)}
}

// /proc stat field 22 survives exec and protects cleanup from PID reuse.
func upgradeProcessIdentity(pid int) (string, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	_, tail, ok := strings.Cut(string(body), ") ")
	fields := strings.Fields(tail)
	if !ok || len(fields) < 20 || fields[0] == "Z" {
		return "", fmt.Errorf("process %d is absent, malformed or a zombie: %s", pid, body)
	}
	return fields[19], nil
}

type upgradeProducer struct {
	stack    *realStack
	ctl      *upgradeCtl
	marker   string
	gate     string
	pid      int
	identity string
	wantOut  []byte
	wantErr  []byte
}

func startUpgradeProducer(t *testing.T, s *realStack) *upgradeProducer {
	t.Helper()
	p := &upgradeProducer{stack: s, marker: filepath.Join(t.TempDir(), "producer"), gate: filepath.Join(t.TempDir(), "continue")}
	script, out, errOut := producer(80, 5*time.Millisecond, p.marker, 23)
	p.wantOut, p.wantErr = out, errOut
	// The deterministic producer pauses at three byte checkpoints. The test
	// releases each only after observing the cut/refusal, never on a timer.
	gate := fmt.Sprintf("if [ $(( (i+1) %% 20 )) -eq 0 ] && [ $i -lt 79 ]; then while [ ! -f '%s.'$i ]; do sleep 0.02; done; fi; ", p.gate)
	script = strings.Replace(script, "i=$((i+1));", gate+"i=$((i+1));", 1)
	p.ctl = startUpgradeCtl(t, s, "exec", "sh", "-c", script)
	waitMarker(t, p.marker)
	matches, _ := filepath.Glob(p.marker + ".*")
	var err error
	p.pid, err = strconv.Atoi(strings.TrimPrefix(matches[0], p.marker+"."))
	if err != nil {
		t.Fatal(err)
	}
	p.identity, err = upgradeProcessIdentity(p.pid)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately broken importer may lose ownership; clean that child
	// explicitly as well as the ctl/agent, without signaling a recycled PID.
	t.Cleanup(func() {
		if identity, err := upgradeProcessIdentity(p.pid); err == nil && identity == p.identity {
			_ = syscall.Kill(-p.pid, syscall.SIGKILL)
		}
	})
	p.checkpoint(t, 20)
	requireReliableOnce(t, s, 0)
	return p
}

func (p *upgradeProducer) checkpoint(t *testing.T, n int) {
	t.Helper()
	_, out, errOut := producer(n, 0, p.marker, 23)
	reached := testharnessWait(15*time.Second, func() bool {
		select {
		case <-p.ctl.done:
			return true
		default:
		}
		gotOut, _ := os.ReadFile(p.ctl.out)
		gotErr, _ := os.ReadFile(p.ctl.errOut)
		return bytes.Equal(gotOut, out) && bytes.Equal(gotErr, errOut)
	})
	select {
	case <-p.ctl.done:
		t.Fatalf("original ctl exited before release: %+v", p.ctl.result(t))
	default:
	}
	if !reached {
		gotOut, _ := os.ReadFile(p.ctl.out)
		gotErr, _ := os.ReadFile(p.ctl.errOut)
		t.Fatalf("producer checkpoint %d: stdout=%q stderr=%q", n, gotOut, gotErr)
	}
	if identity, err := upgradeProcessIdentity(p.pid); err != nil || identity != p.identity {
		t.Fatalf("producer identity changed: before=%s after=%s err=%v", p.identity, identity, err)
	}
	rows := p.stack.psEntries(t.Context(), true)
	matches := 0
	for _, row := range rows {
		if strings.Contains(strings.Join(row.Argv, " "), p.marker) {
			if row.Status != "RUNNING" {
				t.Fatalf("live producer reconciled to %s(%d): %+v", row.Status, row.ExitCode, row)
			}
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("expected one running producer row, found %d: %+v", matches, rows)
	}
}

func (p *upgradeProducer) release(t *testing.T, n int) {
	t.Helper()
	if err := os.WriteFile(fmt.Sprintf("%s.%d", p.gate, n-1), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (p *upgradeProducer) finish(t *testing.T, s *realStack) {
	t.Helper()
	for _, n := range []int{20, 40, 60} {
		p.release(t, n)
	}
	res := p.ctl.result(t)
	if res.rc != 23 || res.stdout != string(p.wantOut) || res.stderr != string(p.wantErr) {
		t.Fatalf("original ctl: rc=%d stdout=%q stderr=%q; want rc=23 stdout=%q stderr=%q",
			res.rc, res.stdout, res.stderr, p.wantOut, p.wantErr)
	}
	requireReliableOnce(t, s, 0)
	if n := markerCount(t, p.marker); n != 1 {
		t.Fatalf("producer executed %d times", n)
	}
	// G.1 can mark a task EXITED(-1) even when its pipes survived. Check the
	// broker's terminal receipt, not only the ctl's independent runtime view.
	if !testharnessWait(15*time.Second, func() bool {
		rows := s.psEntries(t.Context(), true)
		matches := 0
		for _, row := range rows {
			if strings.Contains(strings.Join(row.Argv, " "), p.marker) {
				if row.Status != "EXITED" || row.ExitCode != 23 {
					return false
				}
				matches++
			}
		}
		return matches == 1
	}) {
		t.Fatalf("broker lost the producer's terminal receipt: %+v", s.psEntries(t.Context(), true))
	}
}

// drill-pin: mirrors internal/agent/upgrade_state.go upgradeMarker.
type upgradeImageMarker struct {
	State             string `json:"state"`
	Generation        uint64 `json:"generation"`
	ResourcePhase     string `json:"resource_phase"`
	ImportedResources int    `json:"imported_resources"`
	FreezeWindowMS    int64  `json:"freeze_window_ms"`
}

func upgradeMarkerFile(s *realStack) string {
	return filepath.Join(filepath.Dir(s.agentBin), ".tether-upgrade.json")
}

func nodeUpgrade(t *testing.T, s *realStack, srv *httptest.Server, index int, a upgradeArtifact, extra ...string) ctlResult {
	t.Helper()
	// Upgrade is a strict ONLINE admission. Phase-anchor it to a real
	// heartbeat: hashing and a race-instrumented ctl's exit delay can otherwise
	// place the second hop on the product's five-second STALE boundary.
	s.waitFreshHeartbeat(t, time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.ComponentHandover+30*time.Second)
	defer cancel()
	args := []string{"node", "upgrade", s.nid, "--url", fmt.Sprintf("%s/%d.tar.gz", srv.URL, index), "--sha256", a.tarSHA}
	return s.ctlCommand(ctx, append(args, extra...)...)
}

func requireUpgradeReceipt(t *testing.T, s *realStack, pid int, identity, sha string, generation uint64) {
	t.Helper()
	gotIdentity, err := upgradeProcessIdentity(pid)
	if err != nil || s.agentPID() != pid || gotIdentity != identity {
		t.Fatalf("same-PID exec lost identity: pid=%d/%d ticks=%s/%s err=%v", pid, s.agentPID(), identity, gotIdentity, err)
	}
	if got := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid)); got != sha {
		t.Fatalf("running image SHA=%s want=%s", got, sha)
	}
	if got := upgradeFileSHA(t, s.agentBin); got != sha {
		t.Fatalf("installed image SHA=%s want=%s", got, sha)
	}
	body, err := os.ReadFile(upgradeMarkerFile(s))
	if err != nil {
		t.Fatal(err)
	}
	var m upgradeImageMarker
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if m.State != "committed" || m.Generation != generation || m.ResourcePhase != "active" || m.ImportedResources != 1 || m.FreezeWindowMS <= 0 {
		t.Errorf("upgrade marker: %+v; want committed generation=%d active imported_resources=1 freeze_window_ms>0", m, generation)
	}
	log, err := os.ReadFile(s.agentOut)
	if err != nil {
		t.Fatal(err)
	}
	lines := regexp.MustCompile(`agent: image activated generation=(\d+) tasks=(\d+) freeze_window_ms=(\d+) shim_ms=(\d+) import_ms=(\d+)`).FindAllStringSubmatch(string(log), -1)
	total := s.agentLogCount(`agent: image activated`)
	if len(lines) != int(generation) || total != int(generation) {
		t.Errorf("image activation lines: complete=%d total=%d want=%d: %s", len(lines), total, generation, log)
		return
	}
	line := lines[len(lines)-1]
	if line[1] != strconv.FormatUint(generation, 10) || line[2] != "1" || line[3] != strconv.FormatInt(m.FreezeWindowMS, 10) {
		t.Errorf("activation and marker disagree: %s marker=%+v", line[0], m)
	}
	t.Logf("calibration: %s", line[0]) // samples, never a freeze-window bound
	requireUpgradeFDs(t, pid)
}

// origin: hot-update S3 plan §5.2. fdinfo reports O_CLOEXEC in octal flags;
// merely counting descriptors would miss a leaked inheritable pipe or secret.
func requireUpgradeFDs(t *testing.T, pid int) {
	t.Helper()
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if fd < 3 {
			continue
		}
		target, err := os.Readlink(filepath.Join(dir, entry.Name()))
		if os.IsNotExist(err) { // a concurrently closed network socket
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(target, "memfd:tether-handoff") {
			t.Errorf("handoff descriptor survived activation: fd=%d target=%s", fd, target)
		}
		body, err := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/%d", pid, fd))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		flags := regexp.MustCompile(`(?m)^flags:\s+([0-7]+)$`).FindSubmatch(body)
		if flags == nil {
			t.Fatalf("fd %d missing flags: %s", fd, body)
		}
		value, err := strconv.ParseUint(string(flags[1]), 8, 64)
		if err != nil || value&syscall.O_CLOEXEC == 0 {
			t.Errorf("NONCLOEXEC fd=%d target=%s flags=%s err=%v", fd, target, flags[1], err)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("FD audit inspected no descriptors")
	}
}

func requireUpgradeUnchanged(t *testing.T, s *realStack, sha string) {
	t.Helper()
	if _, err := os.Stat(upgradeMarkerFile(s)); !os.IsNotExist(err) {
		t.Errorf("refused upgrade wrote a marker: err=%v", err)
	}
	if got := upgradeFileSHA(t, s.agentBin); got != sha {
		t.Errorf("refused upgrade changed disk: %s want=%s", got, sha)
	}
	if got := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", s.agentPID())); got != sha {
		t.Errorf("refused upgrade changed running image: %s want=%s", got, sha)
	}
}

// Same-PID exec, original child identity, byte continuity and terminal receipt
// now use target-issued Prepare, explicit root stage, Start and fresh Wait.
// origin: hot-update S3 plan §7 T16 / §5.2; hot-update S8 plan §5.2 / §7.1
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS
func TestNodeUpgradeCarriesLiveExecAcrossExec(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			_, candidate := buildUpgradeTransactionCandidate(t)
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			for round := 0; round < 5; round++ {
				t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseNodeUpgradeLiveExec(t, candidate, false) })
			}
			testharness.AssertNoGoroutineLeak(t, "transaction live exec", g)
			testharness.AssertNoFDLeak(t, "transaction live exec", fd, 0)

		})
	}
}

// The two-release limit keeps A→B→A on actual source bytes. Each forward
// transaction has its own grant and generation; the same child crosses both.
// origin: hot-update S3 plan §7 T16 / §5.4; hot-update S8 plan §6.3 / §7.1
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS
func TestNodeUpgradeGenerationChainKeepsLiveExec(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			_, candidate := buildUpgradeTransactionCandidate(t)
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			for round := 0; round < 5; round++ {
				t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseNodeUpgradeLiveExec(t, candidate, true) })
			}
			testharness.AssertNoGoroutineLeak(t, "transaction generation chain", g)
			testharness.AssertNoFDLeak(t, "transaction generation chain", fd, 0)

		})
	}
}

// Five full legacy PTY lifecycles check parent-side cleanup. Busy legacy wire
// now refuses 64; an authorized hot Start still defers on the retained PTY.
// origin: hot-update S3 plan §7 T16 / §4.1; hot-update S8 plan §7.2 / §6.1
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS
func TestNodeUpgradeDefersOnBusyResource(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			legacyCtl := buildS3PTYBinary(t)
			_, candidate := buildUpgradeTransactionCandidate(t)
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			for round := 0; round < 5; round++ {
				t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseNodeUpgradeBusyResource(t, legacyCtl, candidate) })
			}
			testharness.AssertNoGoroutineLeak(t, "PTY upgrade refusal/retry", g)
			testharness.AssertNoFDLeak(t, "PTY upgrade refusal/retry", fd, 0)

		})
	}
}

// This script executes version successfully; the missing ABI line is the
// incompatibility. It proves pre-activation refusal, never a real N-1 cut.
// origin: hot-update S3 plan §7 T16 / §2.1 Q1; hot-update S8 plan §6.1 / §7.2
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS
func TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseNodeUpgradeLegacyCandidate(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy candidate refusal", g)
	testharness.AssertNoFDLeak(t, "legacy candidate refusal", fd, 0)
}
