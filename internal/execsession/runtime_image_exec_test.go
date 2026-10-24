//go:build linux

// runtime_image_exec_test.go — the same-PID exec proof of the runtime
// image ABI (hot-update S2 exit #2). One test binary plays three roles:
//
//   - the PARENT runs an embedded NATS server and a real execclient.Run (the
//     ctl), spawns the helper and, when the helper exits, checks the bytes,
//     order and exit code the ctl received plus the helper's receipt;
//   - the helper's BEFORE half owns a real Runtime with three live tasks,
//     serves the ctl over NATS, cuts the runtime at a chosen protocol instant
//     (bytes delivered, ACK not yet received), packs the cut with
//     internal/handoff and execs ITSELF;
//   - the helper's AFTER half (same PID) restores the bundle, imports the
//     runtime, thaws, serves the ctl's resend, drives every task to its
//     terminal, settles and writes the receipt.
//
// Real: same-PID exec, the original children (live, ReapHeld zombie, raw
// SIGKILL zombie), their pipes, memfd replays, a NATS carrier and the
// production client. Stubbed: the broker (the helper stamps ActorFP itself)
// and the agent's bootstrap wiring (S3). Dispatch shape: one test function,
// a mode env, exact -test.run, two-sided environment stripping, no second
// fork inside the helper (S2 plan exit #6).
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §5.2.
package execsession

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const (
	runtimeImageMode     = runtimeImageEnvPrefix + "MODE"
	runtimeImageReport   = runtimeImageEnvPrefix + "REPORT"
	runtimeImageNATS     = runtimeImageEnvPrefix + "NATS_URL"
	runtimeImageActor    = runtimeImageEnvPrefix + "ACTOR"
	runtimeImageMutation = runtimeImageEnvPrefix + "MUTATION"
	runtimeImageWork     = runtimeImageEnvPrefix + "WORKDIR"

	runtimeImageSID      = "lab"
	runtimeImageNID      = "node"
	runtimeImageInstance = "image-original"
	runtimeImageBudget   = 60 * time.Second
	runtimeImageStdinLen = 192 << 10
)

// runtimeImageFixture is what the before half hands the after half through
// handoff.State.Data: the image, the process table, and the receipts only
// the exporting image could take.
type runtimeImageFixture struct {
	Image           Image                 `json:"image"`
	Processes       workload.ProcessState `json:"processes"`
	TaskA           string                `json:"task_a"`
	TaskB           string                `json:"task_b"`
	TaskC           string                `json:"task_c"`
	ScopeB          procrequest.Scope     `json:"scope_b"`
	HandleB         proto.ProcessHandle   `json:"handle_b"`
	HandleC         proto.ProcessHandle   `json:"handle_c"`
	OwnerPID        int                   `json:"owner_pid"`
	OSPIDs          map[string]int        `json:"os_pids"`
	StartTicks      map[string]int64      `json:"start_ticks"`
	DescendantPID   int                   `json:"descendant_pid"`
	StdoutBase      uint64                `json:"stdout_base"`
	StdoutEnd       uint64                `json:"stdout_end"`
	StderrBase      uint64                `json:"stderr_base"`
	StderrEnd       uint64                `json:"stderr_end"`
	InputBase       uint64                `json:"input_base"`
	InputEnd        uint64                `json:"input_end"`
	BudgetUsed      int64                 `json:"budget_used"`
	BlockedRequests int                   `json:"blocked_requests"`
	Markers         [2]string             `json:"markers"`
}

// runtimeImageReceipt is what the after half writes for the parent.
type runtimeImageReceipt struct {
	OwnerPID              int                `json:"owner_pid"`
	HelperPIDAfter        int                `json:"helper_pid_after"`
	OSPIDsBefore          map[string]int     `json:"os_pids_before"`
	OSPIDsAfter           map[string]int     `json:"os_pids_after"`
	StartTicksBefore      map[string]int64   `json:"start_ticks_before"`
	StartTicksAfter       map[string]int64   `json:"start_ticks_after"`
	StdoutBaseAtFreeze    uint64             `json:"stdout_base_at_freeze"`
	StdoutEndAtFreeze     uint64             `json:"stdout_end_at_freeze"`
	StderrBaseAtFreeze    uint64             `json:"stderr_base_at_freeze"`
	StderrEndAtFreeze     uint64             `json:"stderr_end_at_freeze"`
	InputBaseAtFreeze     uint64             `json:"input_base_at_freeze"`
	InputEndAtFreeze      uint64             `json:"input_end_at_freeze"`
	BlockedRequests       int                `json:"blocked_requests"`
	BudgetBefore          int64              `json:"budget_before"`
	BudgetAfterImport     int64              `json:"budget_after_import"`
	DescendantAliveAtBoot bool               `json:"descendant_alive_at_boot"`
	SecondRestoreNil      bool               `json:"second_restore_nil"`
	HandoffMemfdAfter     int                `json:"handoff_memfd_after"`
	PipeRefsAfterImport   map[string]int     `json:"pipe_refs_after_import"`
	SpawnedAfter          int32              `json:"spawned_after"`
	TargetInstance        string             `json:"target_instance"`
	BStdout               string             `json:"b_stdout"`
	BExitCode             int                `json:"b_exit_code"`
	CSignal               int                `json:"c_signal"`
	ExitedAfter           map[string]int     `json:"exited_after"`
	FreezeWindowMS        int64              `json:"freeze_window_ms"`
	FDBeforeImport        runtimeImageCensus `json:"fd_before_import"`
	FDAfterImport         runtimeImageCensus `json:"fd_after_import"`
	FDAfterClose          runtimeImageCensus `json:"fd_after_close"`
}

// runtimeImageEnvironment is the helper's environment for one mode: the
// process environment minus the handoff dispatch and minus every harness
// key, plus the harness keys for this mode (the exec'd image inherits
// exactly what it needs and nothing of the old image's role).
func runtimeImageEnvironment(mode string, extra map[string]string) []string {
	env := runtimeChildEnvironment()
	env = append(env, runtimeImageMode+"="+mode)
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

func runtimeImageHelperArgs() []string {
	return []string{os.Args[0], "-test.run=^TestRuntimeImagePreservesLiveChildAcrossExec$", "-test.count=1"}
}

// TestRuntimeImagePreservesLiveChildAcrossExec is the literal exit #2 proof;
// see the file comment for the three roles.
func TestRuntimeImagePreservesLiveChildAcrossExec(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(runtimeImageMode); mode {
	case "before":
		runtimeImageBefore(t)
		return
	case "after":
		runtimeImageAfter(t)
		return
	case "":
	default:
		t.Fatalf("unknown runtime image test mode %q", mode)
	}
	runtimeImageParent(t, "", func(t *testing.T, receipt runtimeImageReceipt, result execclient.Result, stdout, stderr []byte, helperErr error) {

		if helperErr != nil {
			t.Fatalf("helper failed: %v", helperErr)
		}
		wantOut := "out:first\nsecond" + "descendant-tail"
		wantErr := "degraded-path\nerr:x" + "err-tail\n"
		if string(stdout) != wantOut || string(stderr) != wantErr || result.ExitCode != 23 || !result.Executed ||
			result.StdoutAbort != "" || result.StderrAbort != "" || result.TerminalError != "" {
			t.Fatalf("ctl across the exec: stdout=%q stderr=%q result=%+v", stdout, stderr, result)
		}
		if receipt.OwnerPID != receipt.HelperPIDAfter {
			t.Fatalf("exec changed the owner pid: %d → %d", receipt.OwnerPID, receipt.HelperPIDAfter)
		}
		for _, id := range []string{"A", "B", "C"} {
			if receipt.OSPIDsBefore[id] <= 0 || receipt.OSPIDsBefore[id] != receipt.OSPIDsAfter[id] ||
				receipt.StartTicksBefore[id] <= 0 || receipt.StartTicksBefore[id] != receipt.StartTicksAfter[id] {
				t.Fatalf("child %s identity changed across the exec: %+v", id, receipt)
			}
		}
		if receipt.StdoutBaseAtFreeze != 10 || receipt.StdoutEndAtFreeze != 16 || receipt.StderrBaseAtFreeze != 14 || receipt.StderrEndAtFreeze != 19 {
			t.Fatalf("the cut was not taken at the partial-ACK instant: %+v", receipt)
		}
		if receipt.InputBaseAtFreeze == 0 || receipt.InputBaseAtFreeze >= receipt.InputEndAtFreeze || receipt.InputEndAtFreeze != runtimeImageStdinLen {
			t.Fatalf("stdin was not mid-delivery at the cut: base=%d end=%d", receipt.InputBaseAtFreeze, receipt.InputEndAtFreeze)
		}
		if receipt.BlockedRequests < 1 {
			t.Fatal("no ctl request was in flight under the fence: the resend path was never exercised")
		}
		if receipt.BudgetBefore != receipt.BudgetAfterImport || receipt.BudgetBefore == 0 {
			t.Fatalf("replay budget not conserved: %d → %d", receipt.BudgetBefore, receipt.BudgetAfterImport)
		}
		if !receipt.DescendantAliveAtBoot || !receipt.SecondRestoreNil || receipt.HandoffMemfdAfter != 0 || receipt.SpawnedAfter != 0 {
			t.Fatalf("after-half invariants: %+v", receipt)
		}
		for name, refs := range receipt.PipeRefsAfterImport {
			if refs != 1 {
				t.Fatalf("pipe %s referenced %d times after import (bundle not closed or a second owner)", name, refs)
			}
		}
		if receipt.TargetInstance != runtimeImageInstance {
			t.Fatalf("instance after import %q", receipt.TargetInstance)
		}
		if receipt.BStdout != "heldheld-tail" || receipt.BExitCode != 37 || receipt.CSignal != int(syscall.SIGKILL) {
			t.Fatalf("B/C after the exec: %+v", receipt)
		}
		for id, n := range receipt.ExitedAfter {
			if n != 1 {
				t.Fatalf("task %s exited %d times after the exec", id, n)
			}
		}
		if len(receipt.ExitedAfter) != 3 {
			t.Fatalf("exits after the exec: %+v", receipt.ExitedAfter)
		}
		// After Close the helper holds no replay memfd, no pipe, no pidfd and
		// no handoff snapshot: the categories are exact, and the total must be
		// back UNDER the pre-import figure (the bundle's descriptors were still
		// inside the source then). A Close that leaks every replay/pipe would
		// pass a "total did not grow" check — this is the R1 lesson.
		after := receipt.FDAfterClose
		if after.ReplayMemfds != 0 || after.Pipes != 0 || after.PidFDs != 0 || after.HandoffMemfd != 0 {
			t.Fatalf("descriptors left after the after half closed everything: %+v (before import %+v, after import %+v)", after, receipt.FDBeforeImport, receipt.FDAfterImport)
		}
		if after.Total >= receipt.FDBeforeImport.Total || receipt.FDAfterImport.ReplayMemfds == 0 || receipt.FDAfterImport.PidFDs == 0 {
			t.Fatalf("exec-path descriptor bookkeeping is not what the harness assumes: before=%+v import=%+v close=%+v", receipt.FDBeforeImport, receipt.FDAfterImport, after)
		}
		t.Logf("calibration sample, NOT a budget claim: freeze→thaw across exec = %d ms", receipt.FreezeWindowMS)
	})
}

// origin: hot-update S2 plan §5.1 — the committed cross-exec negative
// control: a bundle that lost one stream's replay must be refused by the
// import with nothing installed; the ctl never gets a result. Without this
// arm the exec proof could pass on an importer that ignores the bundle.
func TestRuntimeImageOracleRejectsMissingStream(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	runtimeImageParent(t, "drop-stderr-replay", func(t *testing.T, receipt runtimeImageReceipt, result execclient.Result, stdout, stderr []byte, helperErr error) {

		if helperErr == nil {
			t.Fatal("helper succeeded although its bundle lost a stream")
		}
		text := helperErr.Error()
		if !strings.Contains(text, "IMAGE_ORACLE_REJECT: import:") || !strings.Contains(text, "stderr-replay") {
			t.Fatalf("helper failed, but not with the import refusal of the missing stream: %v", helperErr)
		}
		if receipt.OwnerPID != 0 {
			t.Fatal("a receipt was written by a refused import")
		}
		if result.ExitCode == 23 || (result.Executed && result.PID != "" && result.TerminalError == "" && result.StdoutAbort == "" && len(stdout) > 16) {
			t.Fatalf("ctl obtained a result across a refused import: %+v stdout=%q", result, stdout)
		}
	})
}

// runtimeImageParent runs the embedded NATS server, the ctl and the helper
// and hands the outcome to check.
func runtimeImageParent(t *testing.T, mutation string, check func(t *testing.T, receipt runtimeImageReceipt, result execclient.Result, stdout, stderr []byte, helperErr error)) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	s := natstest.RunServer(&opts)
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(s.ClientURL(), natsinbox.InboxConnectOptions(actor)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	work := t.TempDir()

	// origin: hot-update S5 PROC-LEAK. Tasks have independent groups;
	// killing only the helper leaves refused imports waiting forever.
	t.Cleanup(func() {
		groups, err := filepath.Glob(filepath.Join(work, "task-group-*"))
		if err != nil {
			t.Error(err)
			return
		}
		for _, group := range groups {
			pid, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(group), "task-group-"))
			if err == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
	})
	report := filepath.Join(work, "receipt.json")
	ctx, cancel := context.WithTimeout(t.Context(), runtimeImageBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], runtimeImageHelperArgs()[1:]...)
	cmd.Env = runtimeImageEnvironment("before", map[string]string{runtimeImageReport: report, runtimeImageNATS: s.ClientURL(),
		runtimeImageActor: actor, runtimeImageMutation: mutation, runtimeImageWork: work})
	cmd.Dir = work
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	var helperOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &helperOut, &helperOut
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })

	// The ctl: a plain execclient.Run against the helper's subject.
	var stdout, stderr bytes.Buffer
	spec := proto.ExecReq{Argv: []string{"/bin/sh", "-c", runtimeImageTaskA(work)}, Stdin: runtimeImageStdin()}
	type outcome struct {
		result execclient.Result
		err    error
	}
	ctlDone := make(chan outcome, 1)
	ctlCtx, ctlCancel := context.WithCancel(ctx)
	defer ctlCancel()
	go func() {
		result, err := execclient.Run(ctlCtx, nc, proto.SubjCmdBy(runtimeImageSID, actor, runtimeImageNID, proto.ProcessResumeVerb), spec, &stdout, &stderr)
		ctlDone <- outcome{result, err}
	}()

	helperErr := cmd.Wait()
	if helperErr != nil {
		helperErr = fmt.Errorf("%w\n--- helper output ---\n%s", helperErr, helperOut.String())
	}
	var ctl outcome
	select {
	case ctl = <-ctlDone:
	case <-time.After(3 * time.Second):
		// A refused import leaves the ctl waiting on a runtime that no longer
		// exists; the parent ends it.
		ctlCancel()
		ctl = <-ctlDone
	}
	if strings.Contains(helperOut.String(), `"secret"`) || strings.Contains(helperOut.String(), "[redacted]") {
		t.Fatalf("helper output carries a registry key rendering:\n%s", helperOut.String())
	}
	var receipt runtimeImageReceipt
	if data, err := os.ReadFile(report); err == nil {
		if err := json.Unmarshal(data, &receipt); err != nil {
			t.Fatalf("receipt: %v", err)
		}
	}
	if helperErr == nil && ctl.err != nil {
		t.Fatalf("ctl: %v (stdout=%q stderr=%q)\n--- helper output ---\n%s", ctl.err, stdout.Bytes(), stderr.Bytes(), helperOut.String())
	}
	check(t, receipt, ctl.result, stdout.Bytes(), stderr.Bytes(), helperErr)
}

// runtimeImageTaskA is the ctl-driven child: it prints, waits for control
// lines (fd 3, written by the helper), spawns a descendant that holds
// stdout across the exec and waits for a marker file, consumes its stdin
// and exits 23. It also prints the harness keys it can see (must be empty).
func runtimeImageTaskA(work string) string {
	return "printf 'out:first\\n'; " +
		"( echo $$ > " + filepath.Join(work, "descendant.pid") + "; while [ ! -e " + filepath.Join(work, "go-descendant") + " ]; do sleep 0.02; done; printf descendant-tail ) & " +
		"read -r go1 <&3; printf second; printf 'err:x' >&2; " +
		"read -r go2 <&3; printf \"env:${" + runtimeImageMode + "}${_TETHER_HANDOFF}:\" > " + filepath.Join(work, "child.env") + "; " +
		"cat >/dev/null; wait; printf 'err-tail\\n' >&2; exit 23"
}

func runtimeImageStdin() []byte {
	payload := make([]byte, runtimeImageStdinLen)
	for i := range payload {
		payload[i] = 'a' + byte(i%26)
	}
	payload[len(payload)-1] = '\n'
	return payload
}

// runtimeImageResponder serves the runtime over NATS the way the agent's
// forwarded handler does, minus the broker: it stamps ActorFP itself.
// received counts every request at entry (before Handle may block under
// the fence); hold, when set, is closed by the responder after the reply
// that carried A's second batch and blocks the handler until released, so
// the ctl's ACK for that batch is provably in flight when the cut is taken.
type runtimeImageResponder struct {
	rt       *Runtime
	fp       string
	received atomic.Int64
	mu       sync.Mutex
	trigger  func(reply proto.ProcessResumeReply) bool
	held     chan struct{}
	release  chan struct{}
	sub      *nats.Subscription
}

func (h *runtimeImageResponder) handle(msg *nats.Msg) {
	h.received.Add(1)
	var req proto.ProcessResumeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return
	}
	req.ActorFP = h.fp
	lineage := req.ClientLineage
	if req.Handle != nil {
		lineage = req.Handle.Lease.ClientLineage
	}
	scope := procrequest.Scope{SID: runtimeImageSID, NID: runtimeImageNID, ActorFP: h.fp, ClientLineage: lineage}
	reply, err := h.rt.Handle(scope, req, msg.Reply)
	if err != nil {
		reply.Kind, reply.Error = "error", err.Error()
	}
	body, _ := json.Marshal(reply)
	_ = msg.Respond(body)
	h.mu.Lock()
	trigger, held, release := h.trigger, h.held, h.release
	if trigger != nil && err == nil && trigger(reply) {
		h.trigger = nil
		h.mu.Unlock()
		close(held)
		<-release
		return
	}
	h.mu.Unlock()
}

func runtimeImageConnect(t *testing.T, rt *Runtime, actor string) (*nats.Conn, *runtimeImageResponder) {
	t.Helper()
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(os.Getenv(runtimeImageNATS))
	if err != nil {
		t.Fatalf("helper connect: %v", err)
	}
	h := &runtimeImageResponder{rt: rt, fp: fp, held: make(chan struct{}), release: make(chan struct{})}
	sub, err := nc.Subscribe(proto.SubjCmdBy(runtimeImageSID, actor, runtimeImageNID, proto.ProcessResumeVerb), h.handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(runtimeImageBudget); err != nil {
		t.Fatal(err)
	}
	h.sub = sub
	return nc, h
}

func runtimeImageStartTicks(t *testing.T, pid int) int64 {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatalf("stat of child %d: %v", pid, err)
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
	if len(fields) < 20 {
		t.Fatalf("short stat: %q", data)
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return ticks
}

// runtimeImageProcFDs counts descriptors by link target: pipe inodes and
// the handoff snapshot memfd.
// runtimeImageCensus is one category count of the helper's descriptors.
type runtimeImageCensus struct {
	Total        int `json:"total"`
	Pipes        int `json:"pipes"`
	ReplayMemfds int `json:"replay_memfds"`
	HandoffMemfd int `json:"handoff_memfd"`
	PidFDs       int `json:"pidfds"`
}

// runtimeImageFDCensus classifies /proc/self/fd by link target: the exec
// path's leak oracle must count by CATEGORY (a Close that leaks every replay
// memfd and pipe still lowers the total — S2 internal review R1).
func runtimeImageFDCensus(t *testing.T) runtimeImageCensus {
	t.Helper()
	var c runtimeImageCensus
	for _, e := range mustReadDir(t, "/proc/self/fd") {
		if fd, err := strconv.Atoi(e.Name()); err == nil && fd <= 2 {
			continue
		}
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		c.Total++
		switch {
		case strings.HasPrefix(target, "pipe:["):
			c.Pipes++
		case strings.Contains(target, "memfd:tether-replay"):
			c.ReplayMemfds++
		case strings.Contains(target, "memfd:tether-handoff"):
			c.HandoffMemfd++
		case strings.HasPrefix(target, "anon_inode:[pidfd]"):
			c.PidFDs++
		}
	}
	return c
}

func runtimeImageProcFDs(t *testing.T) (pipes map[string]int, handoffMemfd int) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	pipes = make(map[string]int)
	for _, e := range entries {
		if fd, err := strconv.Atoi(e.Name()); err == nil && fd <= 2 {
			continue // the helper's own stdio (the parent captures both on one pipe)
		}
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, "pipe:[") {
			pipes[target]++
		}
		if strings.Contains(target, "memfd:tether-handoff") {
			handoffMemfd++
		}
	}
	return pipes, handoffMemfd
}

func runtimeImageBefore(t *testing.T) {
	t.Helper()
	if restored, err := handoff.Restore(); err != nil || restored != nil {
		t.Fatalf("before half sees a dispatch: %v %v", restored, err)
	}
	actor, work := os.Getenv(runtimeImageActor), os.Getenv(runtimeImageWork)
	f, cfg := runtimeFixtureConfig(t, 256)
	cfg.Instance = runtimeImageInstance
	f.warning = "degraded-path"
	control := imageControlPipe(t, f)
	rt, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = rt
	killAll := func() { _ = f.manager.SignalAll(syscall.SIGKILL) }
	t.Cleanup(killAll) // only reached on failure: a successful exec never returns
	nc, responder := runtimeImageConnect(t, rt, actor)
	t.Cleanup(nc.Close)

	// B and C are the helper's own tasks (a second lineage of the same actor).
	scopeB := procrequest.Scope{SID: runtimeImageSID, NID: runtimeImageNID, ActorFP: responder.fp, ClientLineage: "helper-lineage"}
	call := uint64(1000)
	direct := func(req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
		call++
		req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, call, scopeB.ActorFP
		return rt.Handle(scopeB, req, "_TINBOX.helper")
	}
	probe, err := direct(proto.ProcessResumeReq{Op: "probe", ClientLineage: scopeB.ClientLineage})
	if err != nil || probe.Lease == nil {
		t.Fatalf("helper probe: %+v %v", probe, err)
	}
	handleB := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "held"}
	markerB := filepath.Join(work, "go-held")
	specB := proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf held; ( while [ ! -e " + markerB + " ]; do sleep 0.02; done; printf held-tail ) & exit 37"}}
	createdB, err := direct(proto.ProcessResumeReq{Op: "create", Handle: &handleB, AttachEpoch: 1, Exec: &specB})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	handleC := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "zombie"}
	specC := proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}
	createdC, err := direct(proto.ProcessResumeReq{Op: "create", Handle: &handleC, AttachEpoch: 1, Exec: &specC})
	if err != nil {
		t.Fatalf("create C: %v", err)
	}
	// A arrives from the ctl. Wait for it, then for the ctl's ACK of the
	// first line (base 10) so the cut lands on a genuinely partial ACK.
	var taskA string
	f.waitFor(t, "A created and first line acknowledged", func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		for pid, task := range rt.tasks {
			if pid == createdB.PID || pid == createdC.PID || task.logs[1] == nil {
				continue
			}
			taskA = pid
			return task.logs[1].Snapshot().Base >= 10
		}
		return false
	})
	f.waitFor(t, "B held as a zombie with its descendant on stdout", func() bool {
		for _, row := range f.manager.Snapshot() {
			if row.ID == createdB.PID {
				return row.ReapHeld && imageProcState(t, row.OSPID) == "Z"
			}
		}
		return false
	})
	descendantPID := 0
	f.waitFor(t, "A's descendant pid", func() bool {
		data, err := os.ReadFile(filepath.Join(work, "descendant.pid"))
		if err != nil {
			return false
		}
		descendantPID, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		return descendantPID > 0
	})
	// Arm the cut on the reply that carries A's second batch, release go1.
	responder.mu.Lock()
	responder.trigger = func(reply proto.ProcessResumeReply) bool {
		return reply.PID == taskA && reply.Stdout != nil && reply.Stdout.Offset+uint64(len(reply.Stdout.Data)) == 16
	}
	responder.mu.Unlock()
	if _, err := control.WriteString("go1\n"); err != nil {
		t.Fatal(err)
	}
	failureAwait(t, responder.held, "the reply carrying A's second batch")

	// The cut. Handler is held (its ACK-bearing successor is in flight).
	imageFreeze(t, rt)
	rows, err := f.manager.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	// C: raw SIGKILL under the frozen table → a zombie only the next image reaps.
	var osPIDs = map[string]int{}
	var ticks = map[string]int64{}
	for _, row := range rows {
		id := map[string]string{taskA: "A", createdB.PID: "B", createdC.PID: "C"}[row.ID]
		osPIDs[id], ticks[id] = row.OSPID, runtimeImageStartTicks(t, row.OSPID)
	}
	if err := syscall.Kill(osPIDs["C"], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	f.waitFor(t, "C zombie", func() bool { return imageProcState(t, osPIDs["C"]) == "Z" })
	processes, err := f.manager.ExportProcesses()
	if err != nil {
		t.Fatalf("export processes: %v", err)
	}
	img, resources, err := rt.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	// The ctl's next request is queued behind the held handler: in flight
	// under the fence, lost with this image, resent to the next.
	f.waitFor(t, "a ctl request in flight under the fence", func() bool {
		pending, _, _ := responder.sub.Pending()
		return pending >= 1
	})
	pending, _, _ := responder.sub.Pending()
	rt.mu.Lock()
	a := rt.tasks[taskA]
	fixture := runtimeImageFixture{Image: img, Processes: processes, TaskA: taskA, TaskB: createdB.PID, TaskC: createdC.PID,
		ScopeB: scopeB, HandleB: handleB, HandleC: handleC, OwnerPID: os.Getpid(), OSPIDs: osPIDs, StartTicks: ticks,
		DescendantPID: descendantPID, StdoutBase: a.frozen.replay[1].Base, StdoutEnd: a.frozen.replay[1].End,
		StderrBase: a.frozen.replay[2].Base, StderrEnd: a.frozen.replay[2].End, InputBase: a.frozen.replay[0].Base, InputEnd: a.frozen.replay[0].End,
		BudgetUsed: rt.budget.Used(), BlockedRequests: pending, Markers: [2]string{filepath.Join(work, "go-descendant"), markerB}}
	rt.mu.Unlock()
	if fixture.StdoutBase != 10 || fixture.StdoutEnd != 16 {
		t.Fatalf("cut not at the partial-ACK instant: %+v", fixture)
	}
	// Negative control: drop A's stderr replay from the bundle.
	if os.Getenv(runtimeImageMutation) == "drop-stderr-replay" {
		for i := range resources {
			if resources[i].ID == resourceID(taskA) {
				delete(resources[i].Files, fileStderrReplay)
			}
		}
	}
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	resources = append(resources, handoff.Resource{ID: "control", Kind: "pipe", Files: map[string]*os.File{"w": control}})
	prepared, err := handoff.Prepare(handoff.State{Role: "runtime-test", Instance: runtimeImageInstance, Generation: 1, Data: data}, resources)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	env := runtimeImageEnvironment("after", map[string]string{runtimeImageReport: os.Getenv(runtimeImageReport), runtimeImageNATS: os.Getenv(runtimeImageNATS),
		runtimeImageActor: actor, runtimeImageMutation: os.Getenv(runtimeImageMutation), runtimeImageWork: work})
	if err := prepared.Exec(os.Args[0], runtimeImageHelperArgs(), env); err != nil {
		t.Fatalf("exec: %v", err)
	}
	t.Fatal("successful exec returned to the old image")
}

func runtimeImageAfter(t *testing.T) {
	t.Helper()
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "IMAGE_ORACLE_REJECT: "+format+"\n", args...)
		os.Exit(1)
	}
	src, err := handoff.Restore()
	if err != nil || src == nil {
		fail("restore: %v", err)
	}
	again, err := handoff.Restore()
	secondRestoreNil := err == nil && again == nil
	var fixture runtimeImageFixture
	if err := json.Unmarshal(src.State.Data, &fixture); err != nil {
		fail("fixture: %v", err)
	}
	src.State.Data = nil
	if fixture.OwnerPID != os.Getpid() {
		fail("owner pid %d, now %d", fixture.OwnerPID, os.Getpid())
	}
	actor, work := os.Getenv(runtimeImageActor), os.Getenv(runtimeImageWork)
	f, cfg := runtimeFixtureConfig(t, 256)
	cfg.Instance = "" // the image supplies the identity every earlier lease is bound to
	f.refuseSpawn = "no spawn after import"
	fdBefore := runtimeImageFDCensus(t) // the bundle's descriptors are still inside src here
	rt, salvage, err := Import(cfg, fixture.Image, fixture.Processes.Records, src)
	if err != nil {
		for _, res := range salvage {
			for _, file := range res.Files {
				_ = file.Close()
			}
		}
		_ = src.Close()
		fail("import: %v", err)
	}
	f.r = rt
	if err := f.manager.RestoreProcesses(fixture.Processes); err != nil {
		fail("restore processes: %v", err)
	}
	control, err := src.TakeFile("control", "w")
	if err != nil {
		fail("control pipe: %v", err)
	}
	if err := src.Close(); err != nil {
		fail("bundle close: %v", err)
	}
	pipeRefs, handoffMemfd := runtimeImageProcFDs(t)
	fdAfterImport := runtimeImageFDCensus(t)
	budgetAfterImport := rt.ReplayBudgetUsed() // measured on the imported runtime, before Thaw (review P5)
	killAll := func() {
		f.manager.Thaw()
		_ = f.manager.SignalAll(syscall.SIGKILL)
	}
	t.Cleanup(killAll)
	// Thaw-time invariants: the pumps are paused (a resume parks), stdin is
	// where the cut left it, the budget is conserved.
	rt.mu.Lock()
	a := rt.tasks[fixture.TaskA]
	inputBase := a.input.Snapshot().Replay.Base
	rt.mu.Unlock()
	if inputBase != fixture.InputBase {
		fail("stdin base %d after import, %d at the cut", inputBase, fixture.InputBase)
	}
	if rt.ReplayBudgetUsed() != fixture.BudgetUsed {
		fail("budget %d after import, %d before", rt.ReplayBudgetUsed(), fixture.BudgetUsed)
	}
	descendantAlive := syscall.Kill(fixture.DescendantPID, 0) == nil
	parked := make(chan error, 1)
	go func() {
		_, err := rt.Handle(fixture.ScopeB, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 5000, Op: "resume",
			ActorFP: fixture.ScopeB.ActorFP, Handle: &fixture.HandleC, AttachEpoch: 1}, "_TINBOX.helper")
		parked <- err
	}()
	select {
	case err := <-parked:
		fail("resume answered before Thaw: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	nc, _ := runtimeImageConnect(t, rt, actor)
	t.Cleanup(nc.Close)
	f.manager.Thaw()
	if err := rt.Thaw(); err != nil {
		fail("thaw: %v", err)
	}
	thawedAt, _ := procrequest.BootNow()
	if err := <-parked; err != nil {
		fail("parked resume after Thaw: %v", err)
	}
	// Release the descendants and A's second control line; the ctl (in the
	// parent) drives A to its terminal and settles it.
	for _, marker := range fixture.Markers {
		if err := os.WriteFile(marker, nil, 0o644); err != nil {
			fail("marker: %v", err)
		}
	}
	if _, err := control.WriteString("go2\n"); err != nil {
		fail("control: %v", err)
	}
	call := uint64(6000)
	direct := func(req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
		call++
		req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, call, fixture.ScopeB.ActorFP
		return rt.Handle(fixture.ScopeB, req, "_TINBOX.helper")
	}
	collect := func(handle proto.ProcessHandle) (string, proto.ProcessResumeReply) {
		var out []byte
		var outFIN, errFIN bool
		var errLen uint64
		deadline := time.Now().Add(runtimeImageBudget)
		for {
			reply, err := direct(proto.ProcessResumeReq{Op: "resume", Handle: &handle, AttachEpoch: 1,
				StdoutACK: uint64(len(out)), StdoutFIN: outFIN, StderrACK: errLen, StderrFIN: errFIN})
			if err != nil {
				fail("resume %s: %v", handle.RequestID, err)
			}
			if reply.Stdout != nil {
				out = append(out, reply.Stdout.Data...)
				outFIN = reply.Stdout.FIN
			}
			if reply.Stderr != nil {
				errLen += uint64(len(reply.Stderr.Data))
				errFIN = reply.Stderr.FIN
			}
			if reply.Phase == "terminal" && outFIN && errFIN {
				ack := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &handle, AttachEpoch: 1, TerminalID: reply.TerminalID,
					StdoutACK: uint64(len(out)), StdoutFIN: true, StderrACK: errLen, StderrFIN: true}
				if settled, err := direct(ack); err != nil || settled.Phase != "settled" {
					fail("settle %s: %+v %v", handle.RequestID, settled, err)
				}
				return string(out), reply
			}
			if time.Now().After(deadline) {
				fail("%s never ended: %+v", handle.RequestID, reply)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	bOut, bReply := collect(fixture.HandleB)
	_, cReply := collect(fixture.HandleC)
	if bReply.TargetInstance != runtimeImageInstance {
		fail("instance after import: %q", bReply.TargetInstance)
	}
	// A is settled by the ctl; wait for the table to empty.
	f.waitFor(t, "A settled by the ctl", func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return len(rt.tasks) == 0
	})
	f.waitFor(t, "process table empty", func() bool { return len(f.manager.Snapshot()) == 0 })
	childEnv, _ := os.ReadFile(filepath.Join(work, "child.env"))
	if string(childEnv) != "env::" {
		fail("business child saw harness keys: %q", childEnv)
	}
	if !secondRestoreNil {
		fail("second Restore did not report no dispatch")
	}
	f.mu.Lock()
	exited := map[string]int{}
	for pid, codes := range f.exited {
		exited[map[string]string{fixture.TaskA: "A", fixture.TaskB: "B", fixture.TaskC: "C"}[pid]] = len(codes)
	}
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
	defer cancel()
	if err := rt.Close(ctx); err != nil {
		fail("close: %v", err)
	}
	_ = control.Close()
	nc.Close()
	after := map[string]int{}
	afterTicks := map[string]int64{}
	for _, row := range fixture.Processes.Records {
		id := map[string]string{fixture.TaskA: "A", fixture.TaskB: "B", fixture.TaskC: "C"}[row.ID]
		after[id], afterTicks[id] = row.OSPID, row.StartTimeTicks
	}
	// The exported table stamped ticks the before half read itself; both
	// must agree, and the after half re-reads nothing (the children are gone).
	receipt := runtimeImageReceipt{OwnerPID: fixture.OwnerPID, HelperPIDAfter: os.Getpid(),
		OSPIDsBefore: fixture.OSPIDs, OSPIDsAfter: after, StartTicksBefore: fixture.StartTicks, StartTicksAfter: afterTicks,
		StdoutBaseAtFreeze: fixture.StdoutBase, StdoutEndAtFreeze: fixture.StdoutEnd, StderrBaseAtFreeze: fixture.StderrBase, StderrEndAtFreeze: fixture.StderrEnd,
		InputBaseAtFreeze: fixture.InputBase, InputEndAtFreeze: fixture.InputEnd, BlockedRequests: fixture.BlockedRequests,
		BudgetBefore: fixture.BudgetUsed, BudgetAfterImport: budgetAfterImport, DescendantAliveAtBoot: descendantAlive,
		SecondRestoreNil: secondRestoreNil, HandoffMemfdAfter: handoffMemfd, PipeRefsAfterImport: pipeRefs, SpawnedAfter: f.spawned.Load(),
		TargetInstance: bReply.TargetInstance, BStdout: bOut, BExitCode: bReply.ExitCode, CSignal: cReply.Signal, ExitedAfter: exited,
		FreezeWindowMS: (thawedAt - fixture.Image.FrozenAtBootNS) / int64(time.Millisecond),
		FDBeforeImport: fdBefore, FDAfterImport: fdAfterImport, FDAfterClose: runtimeImageFDCensus(t)}
	data, err := json.Marshal(receipt)
	if err != nil {
		fail("receipt: %v", err)
	}
	if err := os.WriteFile(os.Getenv(runtimeImageReport), data, 0o644); err != nil {
		fail("write receipt: %v", err)
	}
}

func mustReadDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

var _ = errors.New
