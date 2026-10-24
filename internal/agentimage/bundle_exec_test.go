//go:build linux

// bundle_exec_test.go — the composed bundle across a real same-PID exec:
// runtime, process table, spawn verdicts and courier outbox packed by the
// before half, restored by the after half (same PID), every domain
// checked, the child driven to its terminal. No NATS: the runtime is driven
// directly, which keeps the parent's descriptor baseline stable enough for
// the five-round resource gate.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §5.1.
package agentimage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const (
	bundleExecMode   = "TETHER_TEST_BUNDLE_EXEC_MODE"
	bundleExecReport = "TETHER_TEST_BUNDLE_EXEC_REPORT"
	bundleExecWork   = "TETHER_TEST_BUNDLE_EXEC_WORKDIR"
	bundleExecPrefix = "TETHER_TEST_BUNDLE_EXEC_"
)

type bundleExecFixture struct {
	AdmissionAt int64               `json:"admission_at"`
	Handle      proto.ProcessHandle `json:"handle"`
	PID         string              `json:"pid"`
	OSPID       int                 `json:"os_pid"`
	OwnerPID    int                 `json:"owner_pid"`
	Secret      string              `json:"-"`
}

type bundleExecReceipt struct {
	OwnerPID        int    `json:"owner_pid"`
	AfterPID        int    `json:"after_pid"`
	OSPID           int    `json:"os_pid"`
	Instance        string `json:"instance"`
	Generation      uint64 `json:"generation"`
	Stdout          string `json:"stdout"`
	ExitCode        int    `json:"exit_code"`
	CourierEvents   int    `json:"courier_events"`
	CourierParked   bool   `json:"courier_parked"`
	ProbesAfter     int    `json:"probes_after"`
	FreezeWindowMS  int64  `json:"freeze_window_ms"`
	SecondRestore   bool   `json:"second_restore_nil"`
	HandoffMemfd    int    `json:"handoff_memfd"`
	SpawnedAfter    int    `json:"spawned_after"`
	EnvelopeDataNil bool   `json:"envelope_data_nil"`
	// The after half's own descriptor hygiene, by category (S2 internal
	// review R1: the parent's baseline proves nothing about the exec'd image).
	ReplayMemfdAfterImport int `json:"replay_memfd_after_import"`
	PidFDsAfterImport      int `json:"pidfds_after_import"`
	ReplayMemfdAfterClose  int `json:"replay_memfd_after_close"`
	PipesAfterClose        int `json:"pipes_after_close"`
	PidFDsAfterClose       int `json:"pidfds_after_close"`
	FDTotalBeforeImport    int `json:"fd_total_before_import"`
	FDTotalAfterClose      int `json:"fd_total_after_close"`
}

// bundleExecCensus classifies the helper's descriptors by link target
// (fd 0-2 excluded: the parent captures both on one pipe).
func bundleExecCensus(t *testing.T) (total, pipes, replayMemfds, handoffMemfd, pidfds int) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if fd, err := strconv.Atoi(e.Name()); err == nil && fd <= 2 {
			continue
		}
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		total++
		switch {
		case strings.HasPrefix(target, "pipe:["):
			pipes++
		case strings.Contains(target, "memfd:tether-replay"):
			replayMemfds++
		case strings.Contains(target, "memfd:tether-handoff"):
			handoffMemfd++
		case strings.HasPrefix(target, "anon_inode:[pidfd]"):
			pidfds++
		}
	}
	return total, pipes, replayMemfds, handoffMemfd, pidfds
}

func bundleExecEnvironment(mode string, extra map[string]string) []string {
	out := bundleExecChildEnvironment()
	out = append(out, bundleExecMode+"="+mode)
	for k, v := range extra {
		out = append(out, k+"="+v)
	}
	return out
}

func bundleExecChildEnvironment() []string {
	env := handoff.CleanEnvironment(os.Environ())
	out := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, bundleExecPrefix) {
			out = append(out, entry)
		}
	}
	return out
}

func bundleExecArgs() []string {
	return []string{os.Args[0], "-test.run=^TestBundleExecRestoresEveryDomain$", "-test.count=1"}
}

// TestBundleExecRestoresEveryDomain is the bundle-level same-PID exec proof.
func TestBundleExecRestoresEveryDomain(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(bundleExecMode); mode {
	case "before":
		bundleExecBefore(t)
		return
	case "after":
		bundleExecAfter(t)
		return
	case "":
	default:
		t.Fatalf("unknown bundle exec mode %q", mode)
	}
	receipt, output := exerciseBundleExec(t)
	if strings.Contains(output, `"secret"`) {
		t.Fatal("helper output carries the registry secret")
	}
	if receipt.OwnerPID != receipt.AfterPID || receipt.OSPID <= 0 || receipt.Instance != "exec-instance" || receipt.Generation != 5 ||
		receipt.Stdout != "beforeafter" || receipt.ExitCode != 23 || receipt.CourierEvents != 2 || !receipt.CourierParked ||
		receipt.ProbesAfter != 0 || receipt.FreezeWindowMS < 0 || !receipt.SecondRestore || receipt.HandoffMemfd != 0 ||
		receipt.SpawnedAfter != 0 || !receipt.EnvelopeDataNil {
		t.Fatalf("receipt: %+v", receipt)
	}
	if receipt.ReplayMemfdAfterImport == 0 || receipt.PidFDsAfterImport == 0 ||
		receipt.ReplayMemfdAfterClose != 0 || receipt.PipesAfterClose != 0 || receipt.PidFDsAfterClose != 0 ||
		receipt.FDTotalAfterClose >= receipt.FDTotalBeforeImport {
		t.Fatalf("after-half descriptor hygiene: %+v", receipt)
	}
	t.Logf("calibration sample, NOT a budget claim: bundle freeze→thaw across exec = %d ms", receipt.FreezeWindowMS)
}

// origin: hot-update S2 plan exit #5 — five bundle execs from one parent
// return every descriptor and goroutine to the parent's baseline.
// origin: hot-update S3 plan §5.4 — exercise both cuts of the generation
// chain under the same five-round leak gate, including each helper's census.
func TestBundleRepeatedExecReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Getenv(bundleExecMode) != "" {
		t.Skip("helper role")
	}
	exerciseBundleExec(t) // warm-up
	exerciseGenerationChain(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseBundleExec(t)
		exerciseGenerationChain(t)
	}
	testharness.AssertNoGoroutineLeak(t, "bundle-exec", beforeG)
	testharness.AssertNoFDLeak(t, "bundle-exec", beforeFD, 0)
}

func exerciseBundleExec(t *testing.T) (bundleExecReceipt, string) {
	t.Helper()
	work := t.TempDir()
	report := filepath.Join(work, "receipt.json")
	ctx, cancel := context.WithTimeout(context.Background(), 4*bundleTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], bundleExecArgs()[1:]...)
	cmd.Dir = work
	cmd.Env = bundleExecEnvironment("before", map[string]string{bundleExecReport: report, bundleExecWork: work})
	cmd.Env = append(cmd.Env, "TMPDIR="+work)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		t.Fatalf("bundle exec helper: %v\n%s", err, output.String())
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatalf("receipt: %v\n%s", err, output.String())
	}
	var receipt bundleExecReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt, output.String()
}

func bundleExecBefore(t *testing.T) {
	t.Helper()
	if restored, err := handoff.Restore(); err != nil || restored != nil {
		t.Fatalf("before half sees a dispatch: %v %v", restored, err)
	}
	work := os.Getenv(bundleExecWork)
	admission, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	f := newBundleFixture(t, "exec-instance")
	marker := filepath.Join(work, "go")
	h, pid := f.create(t, "live", []string{"/bin/sh", "-c", "printf before; while [ ! -e " + marker + " ]; do sleep 0.02; done; printf after; exit 23"}, nil)
	f.awaitRunning(t, h)
	probe := &bundleProbe{}
	policy := bundleSpawnPolicy(t, "/exec-nfs", probe, time.Now)
	if _, err := policy.Prepare([]string{"/bin/true"}, "/exec-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	courier := &courierStub{state: courierEvents()}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier, Lineage: Lineage{Generation: 5}, FrozenAtBootNS: admission}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	fixture := bundleExecFixture{AdmissionAt: admission, Handle: h, PID: pid, OSPID: b.Processes.Records[0].OSPID, OwnerPID: os.Getpid()}
	extra, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := frozen.Prepare(handoff.Resource{ID: "fixture", Kind: "test", Data: extra})
	if err != nil {
		t.Fatal(err)
	}
	env := bundleExecEnvironment("after", map[string]string{bundleExecReport: os.Getenv(bundleExecReport), bundleExecWork: work})
	if err := prepared.Exec(os.Args[0], bundleExecArgs(), env); err != nil {
		t.Fatalf("exec: %v", err)
	}
	t.Fatal("successful exec returned to the old image")
}

func bundleExecAfter(t *testing.T) {
	t.Helper()
	src, err := handoff.Restore()
	if err != nil || src == nil {
		t.Fatalf("restore: %v", err)
	}
	again, err := handoff.Restore()
	secondRestoreNil := err == nil && again == nil
	var fixture bundleExecFixture
	for _, res := range src.Resources {
		if res.ID == "fixture" {
			if err := json.Unmarshal(res.Data, &fixture); err != nil {
				t.Fatal(err)
			}
		}
	}
	work := os.Getenv(bundleExecWork)
	f := newBundleConfig(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), bundleTestBudget)
	defer cancel()
	totalBefore, _, _, _, _ := bundleExecCensus(t) // the bundle's descriptors are still inside src
	probe := &bundleProbe{}
	policy := bundleSpawnPolicy(t, "/exec-nfs", probe, time.Now)
	courier := &courierStub{}
	cfg := f.cfg
	cfg.Instance = ""
	cfg.Start = func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
		f.mu.Lock()
		f.spawned++
		f.mu.Unlock()
		return nil, "", fmt.Errorf("no spawn after import")
	}
	exerciseCalibrationMetadata(t, src)
	imported, salvage, err := Restore(src, Target{Runtime: cfg, Spawn: policy, Courier: courier})
	if err != nil {
		_ = salvage.Close()
		t.Fatalf("restore bundle: %v", err)
	}
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	// Serialized restoration must preserve the outer window and the later
	// task-accounting cut independently, including across a real exec.
	if imported.FrozenAt != fixture.AdmissionAt || imported.Bundle.Runtime.FrozenAtBootNS <= fixture.AdmissionAt {
		t.Fatal("serialized calibration lost admission or overwrote the accounting cut")
	}
	envelopeDataNil := src.State.Data == nil
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	f.rt = imported.Runtime
	_, _, replayAfterImport, handoffMemfd, pidfdsAfterImport := bundleExecCensus(t)
	if err := imported.Thaw(); err != nil {
		t.Fatalf("thaw: %v", err)
	}
	if _, err := policy.Prepare([]string{"/bin/true"}, "/exec-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "go"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out, reply := f.drain(t, fixture.Handle)
	f.mu.Lock()
	spawned := f.spawned
	f.mu.Unlock()
	receipt := bundleExecReceipt{OwnerPID: fixture.OwnerPID, AfterPID: os.Getpid(), OSPID: fixture.OSPID,
		Instance: imported.Bundle.Instance, Generation: imported.Bundle.Lineage.Generation, Stdout: out, ExitCode: reply.ExitCode,
		CourierEvents: len(courier.imported.Events), CourierParked: courier.imported.Events[1].Parked, ProbesAfter: probe.count(),
		FreezeWindowMS: imported.FreezeWindow().Milliseconds(), SecondRestore: secondRestoreNil, HandoffMemfd: handoffMemfd,
		SpawnedAfter: spawned, EnvelopeDataNil: envelopeDataNil}
	if strings.Contains(fmt.Sprint(imported.Bundle), base64.StdEncoding.EncodeToString(imported.Bundle.Runtime.Registry.Secret)) {
		t.Fatal("bundle rendering carries the registry secret")
	}
	if err := f.rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	receipt.ReplayMemfdAfterImport, receipt.PidFDsAfterImport, receipt.FDTotalBeforeImport = replayAfterImport, pidfdsAfterImport, totalBefore
	receipt.FDTotalAfterClose, receipt.PipesAfterClose, receipt.ReplayMemfdAfterClose, _, receipt.PidFDsAfterClose = bundleExecCensus(t)
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(bundleExecReport), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

type bundleChainHop struct {
	OwnerPID         int                         `json:"owner_pid"`
	OwnerStartTicks  int64                       `json:"owner_start_ticks"`
	OSPID            int                         `json:"os_pid"`
	StartTicks       int64                       `json:"start_ticks"`
	Pipes            [2]execsession.FileIdentity `json:"pipes"`
	PipeRefs         [2]int                      `json:"pipe_refs"`
	StdoutBase       uint64                      `json:"stdout_base"`
	StdoutEnd        uint64                      `json:"stdout_end"`
	StderrBase       uint64                      `json:"stderr_base"`
	StderrEnd        uint64                      `json:"stderr_end"`
	SecondRestoreNil bool                        `json:"second_restore_nil"`
	Spawned          int                         `json:"spawned"`
	Exited           int                         `json:"exited"`
}

type bundleChainReceipt struct {
	Hops                    [3]bundleChainHop `json:"hops"`
	Generations             [2]uint64         `json:"generations"`
	FreezeWindows           [2]time.Duration  `json:"freeze_windows_ns"`
	Stdout                  string            `json:"stdout"`
	Stderr                  string            `json:"stderr"`
	ExitCode                int               `json:"exit_code"`
	Settled                 bool              `json:"settled"`
	FDTotalBeforeImport     int               `json:"fd_total_before_import"`
	ReplayMemfdsAfterImport int               `json:"replay_memfds_after_import"`
	PidFDsAfterImport       int               `json:"pidfds_after_import"`
	FDTotalAfterClose       int               `json:"fd_total_after_close"`
	PipesAfterClose         int               `json:"pipes_after_close"`
	ReplayMemfdsAfterClose  int               `json:"replay_memfds_after_close"`
	HandoffMemfdsAfterClose int               `json:"handoff_memfds_after_close"`
	PidFDsAfterClose        int               `json:"pidfds_after_close"`
}

// Only this fixture crosses the cuts; observed byte prefixes and kernel
// identities are captured before exec, never reconstructed from expectations.
type bundleChainFixture struct {
	Handle  proto.ProcessHandle `json:"handle"`
	PID     string              `json:"pid"`
	Receipt bundleChainReceipt  `json:"receipt"`
}

func bundleChainArgs() []string {
	return []string{os.Args[0], "-test.run=^TestGenerationChainExportImportExport$", "-test.count=1"}
}

// origin: hot-update S3 plan §5.4, §7 T9 — an imported runtime must itself
// become the next exporter, after consuming only part of each live stream.
func TestGenerationChainExportImportExport(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(bundleExecMode); mode {
	case "chain-hop1":
		bundleChainBefore(t)
	case "chain-hop2":
		bundleChainImport(t, 1)
	case "chain-hop3":
		bundleChainImport(t, 2)
	case "":
		exerciseGenerationChain(t)
	default:
		t.Fatalf("unknown generation chain mode %q", mode)
	}
}

func exerciseGenerationChain(t *testing.T) {
	t.Helper()
	work := t.TempDir()
	report := filepath.Join(work, "receipt.json")
	ctx, cancel := context.WithTimeout(t.Context(), 4*bundleTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], bundleChainArgs()[1:]...)
	cmd.Dir = work
	cmd.Env = bundleExecEnvironment("chain-hop1", map[string]string{bundleExecReport: report, bundleExecWork: work})
	cmd.Env = append(cmd.Env, "TMPDIR="+work)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	output, err := cmd.CombinedOutput()
	if bytes.Contains(output, []byte(`"secret"`)) {
		t.Fatal("generation chain helper output carries the registry secret")
	}
	if err != nil {
		t.Fatalf("generation chain helper: %v\n%s", err, output)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatalf("generation chain receipt: %v\n%s", err, output)
	}
	var r bundleChainReceipt
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Generations != [2]uint64{1, 2} {
		t.Fatalf("imported generations = %v, want [1 2]", r.Generations)
	}
	if r.Stdout != "beforemiddleafter" || r.Stderr != "before-err\nmiddle-err\nafter-err\n" || r.ExitCode != 23 || !r.Settled {
		t.Fatalf("generation chain streams or terminal: %+v", r)
	}
	first := r.Hops[0]
	if first.OwnerPID <= 0 || first.OwnerStartTicks <= 0 || first.OSPID <= 0 || first.StartTicks <= 0 ||
		first.Pipes[0].Ino == 0 || first.Pipes[1].Ino == 0 || first.Pipes[0] == first.Pipes[1] {
		t.Fatalf("empty generation chain identity: %+v", first)
	}
	for hop, got := range r.Hops {
		if got.OwnerPID != first.OwnerPID || got.OwnerStartTicks != first.OwnerStartTicks ||
			got.OSPID != first.OSPID || got.StartTicks != first.StartTicks || got.Pipes != first.Pipes ||
			got.PipeRefs != [2]int{1, 1} || !got.SecondRestoreNil {
			t.Fatalf("hop %d lost same-PID ownership: first=%+v got=%+v", hop+1, first, got)
		}
		wantSpawned, wantExited := 0, 0
		if hop == 0 {
			wantSpawned = 1
		}
		if hop == 2 {
			wantExited = 1
		}
		if got.Spawned != wantSpawned || got.Exited != wantExited {
			t.Fatalf("hop %d lifecycle callbacks: spawned=%d exited=%d", hop+1, got.Spawned, got.Exited)
		}
	}
	// Nonempty acknowledged AND replayable tails at both cuts prevent a
	// fresh, empty stream from accidentally satisfying the continuity oracle.
	for hop, want := range [3][4]uint64{{3, 6, 4, 11}, {3, 6, 4, 11}, {9, 12, 15, 22}} {
		got := r.Hops[hop]
		if offsets := [4]uint64{got.StdoutBase, got.StdoutEnd, got.StderrBase, got.StderrEnd}; offsets != want {
			t.Fatalf("hop %d replay offsets = %v, want %v", hop+1, offsets, want)
		}
	}
	for hop, window := range r.FreezeWindows {
		if window <= 0 {
			t.Fatalf("hop %d missing freeze window sample: %s", hop+2, window)
		}
		t.Logf("calibration sample, NOT a budget claim: generation chain hop %d freeze→thaw = %s", hop+2, window)
	}
	// The completed stdin retains its replay alongside the two live outputs.
	if r.ReplayMemfdsAfterImport != 3 || r.PidFDsAfterImport != 1 || r.PipesAfterClose != 0 ||
		r.ReplayMemfdsAfterClose != 0 || r.HandoffMemfdsAfterClose != 0 || r.PidFDsAfterClose != 0 ||
		r.FDTotalAfterClose >= r.FDTotalBeforeImport {
		t.Fatalf("generation chain descriptor hygiene: %+v", r)
	}
}

func bundleChainBefore(t *testing.T) {
	t.Helper()
	for range 2 {
		if src, err := handoff.Restore(); err != nil || src != nil {
			t.Fatalf("hop1 unexpectedly received a dispatch: %v", err)
		}
	}
	f := newBundleConfig(t, "chain-instance")
	// The shared fixture's adapter predates the mode filter. This child must
	// receive neither harness keys nor handoff keys, including explicit env.
	f.cfg.Start = func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		stdio, err := workload.NewStdio()
		if err != nil {
			return nil, "", err
		}
		cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
		cmd.Dir = os.Getenv(bundleExecWork)
		cmd.Env = bundleExecChildEnvironment()
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdio.Bind(cmd)
		if err := cmd.Start(); err != nil {
			stdio.Close()
			return nil, "", err
		}
		stdio.CloseChild()
		if err := f.manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			stdio.Close()
			return nil, "", err
		}
		f.mu.Lock()
		f.spawned++
		f.mu.Unlock()
		return stdio, "", nil
	}
	var err error
	f.rt, err = execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Marker existence gates progress, not elapsed time. The owner check
	// lets the child stop even if the parent's timeout kills the exec helper.
	// Readiness is published after each stderr redirection is restored, and
	// the wait loop never redirects fd 1/2: /proc must see the canonical child
	// pipes when the frozen owner samples their identities.
	// origin: hot-update S5 plan §8 / testing-standards §0.5
	script := `printf '%s' "${TETHER_TEST_BUNDLE_EXEC_MODE}${TETHER_TEST_BUNDLE_EXEC_REPORT}${TETHER_TEST_BUNDLE_EXEC_WORKDIR}${_TETHER_HANDOFF}"
printf before; printf 'before-err\n' >&2
: 3>"$1/before-ready"
while [ ! -e "$1/middle" ]; do kill -0 "$2" || exit 98; sleep 0.02; done
printf middle; printf 'middle-err\n' >&2
: 3>"$1/middle-ready"
while [ ! -e "$1/after" ]; do kill -0 "$2" || exit 98; sleep 0.02; done
printf after; printf 'after-err\n' >&2; exit 23`
	h, pid := f.create(t, "chain", []string{"/bin/sh", "-c", script, "chain-child", os.Getenv(bundleExecWork), strconv.Itoa(os.Getpid())}, nil)
	fixture := bundleChainFixture{Handle: h, PID: pid}
	bundleChainReadPrefix(t, f, &fixture, "before", "before-err\n", "before-ready", 3, 4)
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: &courierStub{state: CourierState{Version: CourierStateVersion}},
		Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	fixture.Receipt.Hops[0] = bundleChainObserve(t, frozen.Bundle(), &fixture)
	fixture.Receipt.Hops[0].SecondRestoreNil = true
	bundleChainCallbacks(f, &fixture.Receipt.Hops[0], pid)
	bundleChainExec(t, frozen, fixture, "chain-hop2")
}

func bundleChainExec(t *testing.T, frozen *Frozen, fixture bundleChainFixture, mode string) {
	t.Helper()
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := frozen.Prepare(handoff.Resource{ID: "chain-fixture", Kind: "test", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	env := bundleExecEnvironment(mode, map[string]string{bundleExecReport: os.Getenv(bundleExecReport), bundleExecWork: os.Getenv(bundleExecWork)})
	if err := prepared.Exec(os.Args[0], bundleChainArgs(), env); err != nil {
		t.Fatalf("generation chain exec: %v", err)
	}
	t.Fatal("successful generation chain exec returned")
}

func bundleChainImport(t *testing.T, hop int) {
	t.Helper()
	src, err := handoff.Restore()
	if err != nil || src == nil {
		t.Fatalf("hop%d restore: %v", hop+1, err)
	}
	defer func() { _ = src.Close() }()
	again, err := handoff.Restore()
	if err != nil || again != nil {
		t.Fatalf("hop%d consumed the dispatch twice: %v", hop+1, err)
	}
	var fixture bundleChainFixture
	for _, res := range src.Resources {
		if res.ID == "chain-fixture" {
			if err := json.Unmarshal(res.Data, &fixture); err != nil {
				t.Fatal(err)
			}
		}
	}
	if fixture.PID == "" {
		t.Fatal("generation chain fixture missing")
	}
	f := newBundleConfig(t, "") // fresh Manager and no independently minted instance
	f.cfg.Start = func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
		f.mu.Lock()
		f.spawned++
		f.mu.Unlock()
		return nil, "", fmt.Errorf("generation chain must not respawn an imported child")
	}
	r := &fixture.Receipt
	r.FDTotalBeforeImport, _, _, _, _ = bundleExecCensus(t)
	imported, salvage, err := Restore(src, Target{Runtime: f.cfg, Courier: &courierStub{}})
	if err != nil {
		_ = salvage.Close()
		t.Fatalf("hop%d restore bundle: %v", hop+1, err)
	}
	f.rt = imported.Runtime
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	r.Hops[hop] = bundleChainObserve(t, imported.Bundle, &fixture)
	r.Hops[hop].SecondRestoreNil = true
	r.Generations[hop-1] = imported.Bundle.Lineage.Generation
	_, _, r.ReplayMemfdsAfterImport, _, r.PidFDsAfterImport = bundleExecCensus(t)
	if err := imported.Thaw(); err != nil {
		t.Fatalf("hop%d thaw: %v", hop+1, err)
	}
	r.FreezeWindows[hop-1] = imported.FreezeWindow()
	if hop == 1 {
		if err := os.WriteFile(filepath.Join(os.Getenv(bundleExecWork), "middle"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		bundleChainReadPrefix(t, f, &fixture, "beforemiddle", "before-err\nmiddle-err\n", "middle-ready", 9, 15)
		frozen, err := Freeze(Owners{Runtime: imported.Runtime, Manager: imported.Manager,
			Courier: &courierStub{state: imported.Bundle.Courier}, Lineage: Lineage{Generation: imported.Bundle.Lineage.Generation + 1}},
			time.Now().Add(bundleTestBudget))
		if err != nil {
			t.Fatalf("hop2 re-freeze: %v", err)
		}
		bundleChainCallbacks(f, &r.Hops[hop], fixture.PID)
		bundleChainExec(t, frozen, fixture, "chain-hop3")
		return
	}
	if err := os.WriteFile(filepath.Join(os.Getenv(bundleExecWork), "after"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	bundleChainDrain(t, f, &fixture)
	ctx, cancel := context.WithTimeout(t.Context(), bundleTestBudget)
	defer cancel()
	if err := f.rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	bundleChainCallbacks(f, &r.Hops[hop], fixture.PID)
	r.FDTotalAfterClose, r.PipesAfterClose, r.ReplayMemfdsAfterClose, r.HandoffMemfdsAfterClose, r.PidFDsAfterClose = bundleExecCensus(t)
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(bundleExecReport), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func bundleChainCallbacks(f *bundleFixture, hop *bundleChainHop, pid string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hop.Spawned, hop.Exited = f.spawned, f.exited[pid]
}

func bundleChainStartTicks(t *testing.T, pid int) int64 {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
	if len(fields) < 20 {
		t.Fatalf("short process stat for pid %d", pid)
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return ticks
}

func bundleChainObserve(t *testing.T, b Bundle, fixture *bundleChainFixture) bundleChainHop {
	t.Helper()
	if b.Instance != "chain-instance" || len(b.Processes.Records) != 1 || len(b.Runtime.Tasks) != 1 {
		t.Fatalf("generation chain lost its runtime or child: %s", b)
	}
	row, task := b.Processes.Records[0], b.Runtime.Tasks[0]
	hop := bundleChainHop{OwnerPID: os.Getpid(), OwnerStartTicks: bundleChainStartTicks(t, os.Getpid()),
		OSPID: row.OSPID, StartTicks: bundleChainStartTicks(t, row.OSPID)}
	if row.ID != fixture.PID || task.PID != fixture.PID || row.Exit != nil || row.StartTimeTicks != hop.StartTicks ||
		b.Processes.OwnerPID != hop.OwnerPID || b.Processes.OwnerStartTimeTicks != hop.OwnerStartTicks ||
		task.Stdout == nil || task.Stderr == nil {
		t.Fatal("generation chain process table does not match the live child")
	}
	hop.StdoutBase, hop.StdoutEnd = task.Stdout.Replay.Base, task.Stdout.Replay.End
	hop.StderrBase, hop.StderrEnd = task.Stderr.Replay.Base, task.Stderr.Replay.End
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for stream, name := range []string{"stdout-source", "stderr-source"} {
		var stat unix.Stat_t
		if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/%d", row.OSPID, stream+1), &stat); err != nil {
			t.Fatal(err)
		}
		hop.Pipes[stream] = execsession.FileIdentity{Dev: uint64(stat.Dev), Ino: stat.Ino}
		if stat.Mode&unix.S_IFMT != unix.S_IFIFO || task.Files[name] != hop.Pipes[stream] {
			t.Fatalf("%s no longer names the child's original pipe", name)
		}
		for _, entry := range entries {
			fd, err := strconv.Atoi(entry.Name())
			if err != nil || fd <= 2 {
				continue
			}
			if target, err := os.Readlink("/proc/self/fd/" + entry.Name()); err == nil && target == fmt.Sprintf("pipe:[%d]", stat.Ino) {
				hop.PipeRefs[stream]++
			}
		}
	}
	return hop
}

func bundleChainReadPrefix(t *testing.T, f *bundleFixture, fixture *bundleChainFixture, stdout, stderr, readyMarker string, outACK, errACK int) {
	t.Helper()
	// origin: hot-update S3 plan §5.4 — a skipped hop2 Thaw must fail at
	// the middle segment, rather than block Handle until the helper timeout.
	if f.rt.Frozen() || f.manager.Frozen() {
		t.Fatal("generation chain segment cannot advance while the runtime or process table is frozen")
	}
	r := &fixture.Receipt
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &fixture.Handle, AttachEpoch: 1,
			StdoutACK: uint64(len(r.Stdout)), StderrACK: uint64(len(r.Stderr))})
		if err != nil || reply.Kind == "error" || reply.Phase == "terminal" {
			t.Fatalf("generation chain prefix: %+v %v", reply, err)
		}
		_, readyErr := os.Stat(filepath.Join(os.Getenv(bundleExecWork), readyMarker))
		if readyErr != nil && !os.IsNotExist(readyErr) {
			t.Fatal(readyErr)
		}
		if readyErr == nil && reply.Stdout != nil && reply.Stderr != nil &&
			len(r.Stdout)+len(reply.Stdout.Data) >= len(stdout) && len(r.Stderr)+len(reply.Stderr.Data) >= len(stderr) {
			if reply.Stdout.Offset != uint64(len(r.Stdout)) || reply.Stderr.Offset != uint64(len(r.Stderr)) ||
				r.Stdout+string(reply.Stdout.Data) != stdout || r.Stderr+string(reply.Stderr.Data) != stderr ||
				reply.Stdout.FIN || reply.Stderr.FIN || reply.Stdout.Error != "" || reply.Stderr.Error != "" {
				t.Fatalf("generation chain prefix offsets or bytes: %+v stdout=%+v stderr=%+v", reply, reply.Stdout, reply.Stderr)
			}
			r.Stdout += string(reply.Stdout.Data[:outACK-len(r.Stdout)])
			r.Stderr += string(reply.Stderr.Data[:errACK-len(r.Stderr)])
			ack, err := f.handle(proto.ProcessResumeReq{Op: "ack", Handle: &fixture.Handle, AttachEpoch: 1,
				StdoutACK: uint64(outACK), StderrACK: uint64(errACK)})
			if err != nil || ack.Kind == "error" {
				t.Fatalf("generation chain partial ack: %+v %v", ack, err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("generation chain segment missing: stdout=%+v stderr=%+v", reply.Stdout, reply.Stderr)
		}
		time.Sleep(time.Millisecond)
	}
}

func bundleChainDrain(t *testing.T, f *bundleFixture, fixture *bundleChainFixture) {
	t.Helper()
	r := &fixture.Receipt
	var outFIN, errFIN bool
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &fixture.Handle, AttachEpoch: 1,
			StdoutACK: uint64(len(r.Stdout)), StderrACK: uint64(len(r.Stderr)), StdoutFIN: outFIN, StderrFIN: errFIN})
		if err != nil || reply.Kind == "error" {
			t.Fatalf("generation chain drain: %+v %v", reply, err)
		}
		for stream, output := range []*proto.ProcessOutput{reply.Stdout, reply.Stderr} {
			if output == nil {
				continue
			}
			data, fin := &r.Stdout, &outFIN
			if stream == 1 {
				data, fin = &r.Stderr, &errFIN
			}
			if output.Offset != uint64(len(*data)) || output.Error != "" {
				t.Fatalf("generation chain stream %d gap, repeat or abort: %+v", stream, output)
			}
			*data += string(output.Data)
			*fin = output.FIN
		}
		if reply.Phase == "terminal" && outFIN && errFIN {
			r.ExitCode = reply.ExitCode
			ack, err := f.handle(proto.ProcessResumeReq{Op: "terminal-ack", Handle: &fixture.Handle, AttachEpoch: 1,
				TerminalID: reply.TerminalID, StdoutACK: uint64(len(r.Stdout)), StderrACK: uint64(len(r.Stderr)), StdoutFIN: true, StderrFIN: true})
			if err != nil || ack.Kind == "error" || ack.Phase != "settled" {
				t.Fatalf("generation chain settlement: %+v %v", ack, err)
			}
			r.Settled = true
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("generation chain never ended: %+v", reply)
		}
		time.Sleep(time.Millisecond)
	}
}

// The real exec receiver still owns every FD while metadata is checked.
// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
func exerciseCalibrationMetadata(t *testing.T, src *handoff.Restored) {
	t.Helper()
	base, err := Decode(src.State)
	if err != nil {
		t.Fatal(err)
	}
	if err := restoreCalibration(&base, src); err != nil || base.admissionAt <= 0 {
		t.Fatal("valid calibration was refused")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*handoff.Resource)
	}{
		{"zero", func(r *handoff.Resource) { r.Data = []byte("0") }},
		{"negative", func(r *handoff.Resource) { r.Data = []byte("-1") }},
		{"future", func(r *handoff.Resource) { r.Data = strconv.AppendInt(nil, base.Runtime.FrozenAtBootNS+1, 10) }},
		{"noncanonical", func(r *handoff.Resource) { r.Data = append([]byte("+"), r.Data...) }},
		{"not-integer", func(r *handoff.Resource) { r.Data = []byte("invalid") }},
		{"wrong-kind", func(r *handoff.Resource) { r.Kind = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := src.Resources
			defer func() { src.Resources = original }()
			src.Resources = append([]handoff.Resource(nil), original...)
			for i := range src.Resources {
				if src.Resources[i].ID == calibrationResourceID {
					tc.mutate(&src.Resources[i])
				}
			}
			b := base
			if err := restoreCalibration(&b, src); err == nil {
				t.Fatal("invalid calibration was accepted")
			}
		})
	}
	original := src.Resources
	for _, resource := range original {
		if resource.ID == calibrationResourceID {
			src.Resources = append(append([]handoff.Resource(nil), original...), resource)
		}
	}
	b := base
	err = restoreCalibration(&b, src)
	src.Resources = original
	if err == nil {
		t.Fatal("duplicate calibration was accepted")
	}
	if err := restoreCalibration(&b, &handoff.Restored{Resources: original}); err == nil {
		t.Fatal("calibration without descriptor custody was accepted")
	}
	b.admissionAt = 0
	if err := restoreCalibration(&b, &handoff.Restored{}); err != nil || admissionInstant(b) != b.Runtime.FrozenAtBootNS {
		t.Fatal("legacy accounting calibration was refused")
	}
}
