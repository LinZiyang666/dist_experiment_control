//go:build linux

// repack_exec_test.go — the re-dispatch primitive across three real execs of
// the same PID: hop 1 prepares a live child, hop 2 takes only part of the
// bundle, fails an exec on purpose (the bundle must stay usable — Borrow
// lends, it never transfers), then repacks the taken half and the borrowed
// half under the SAME State and execs hop 3, which activates the child.
// Real: subprocesses, wait status, pipes, memfd snapshots. Stubbed: no
// agent, no broker. origin: hot-update S3 plan §1.4 P1–P3.
// The external test package keeps the shared harness's auth/proto/xfervalue
// dependency from forming a test import cycle back into handoff.
// origin: S8 internal review FIX-VETTAGS F1
package handoff_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

type repackFixture struct {
	ParentPID int    `json:"parent_pid"`
	ChildPID  int    `json:"child_pid"`
	Nonce     string `json:"nonce"`
}

type repackReceipt struct {
	Hop        int    `json:"hop"`
	PID        int    `json:"pid"`
	ChildPID   int    `json:"child_pid"`
	StateSHA   string `json:"state_sha"`
	Generation uint64 `json:"generation"`
	Continued  string `json:"continued"`
	ExitStatus int    `json:"exit_status"`
	FailedExec string `json:"failed_exec"`
}

// TestRestoredBorrowRepacksUntakenFiles drives the three hops from a parent
// go test process and reads the receipt hop 3 leaves behind.
func TestRestoredBorrowRepacksUntakenFiles(t *testing.T) {
	dir := t.TempDir()
	nonce := filepath.Base(dir)
	ctx, cancel := context.WithTimeout(t.Context(), handoffTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHandoffRepackSubprocess$", "-test.count=1")
	cmd.Env = handoffTestEnv("repack-hop1", map[string]string{handoffTestDirEnv: dir, handoffTestIDEnv: nonce})
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := testharness.RunProcess(cmd); err != nil {
		t.Fatalf("three-hop subprocess: %v\n%s", err, output.String())
	}
	raw, err := os.ReadFile(filepath.Join(dir, "receipt.json"))
	if err != nil {
		t.Fatalf("hop 3 left no receipt: %v\n%s", err, output.String())
	}
	var receipt repackReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	fixtureRaw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(fixtureRaw)
	if receipt.Hop != 3 || receipt.PID != cmd.Process.Pid || receipt.Generation != 7 || receipt.StateSHA != hex.EncodeToString(sum[:]) {
		t.Fatalf("hop 3 did not receive the very State hop 1 prepared: %+v (pid %d)", receipt, cmd.Process.Pid)
	}
	if want := fmt.Sprintf("continued:%d:resume-%s", receipt.ChildPID, nonce); receipt.Continued != want || receipt.ExitStatus != 23 {
		t.Fatalf("child continuity across three hops: %+v want %q rc 23", receipt, want)
	}
	if !strings.Contains(receipt.FailedExec, "no such file") {
		t.Fatalf("hop 2 did not record the deliberate exec failure: %+v", receipt)
	}
	if strings.Contains(output.String(), "FAIL") {
		t.Fatalf("subprocess output:\n%s", output.String())
	}
}

func TestHandoffRepackSubprocess(t *testing.T) {
	switch os.Getenv(handoffTestModeEnv) {
	case "":
		return
	case "repack-hop1":
		repackHop1(t)
	case "repack-hop2":
		repackHop2(t)
	case "repack-hop3":
		repackHop3(t)
	default:
		t.Fatalf("unknown subprocess mode %q", os.Getenv(handoffTestModeEnv))
	}
}

func repackHop1(t *testing.T) {
	t.Helper()
	inputR, inputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outputR, outputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("/bin/sh", "-c", `printf 'ready:%s\n' "$$"; IFS= read -r token; printf 'continued:%s:%s\n' "$$" "$token"; exit 23`)
	child.Stdin, child.Stdout, child.Stderr = inputR, outputW, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	_ = inputR.Close()
	_ = outputW.Close()
	if got := handoffReadLine(t, bufio.NewReader(outputR)); got != fmt.Sprintf("ready:%d", child.Process.Pid) {
		t.Fatalf("child did not start: %q", got)
	}
	nonce := os.Getenv(handoffTestIDEnv)
	fixture := repackFixture{ParentPID: os.Getpid(), ChildPID: child.Process.Pid, Nonce: nonce}
	data := handoffJSON(t, fixture)
	if err := os.WriteFile(filepath.Join(os.Getenv(handoffTestDirEnv), "state.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	state := handoff.State{Role: "test-runtime", Instance: nonce, Generation: 7, Data: data}
	prepared, err := handoff.Prepare(state, []handoff.Resource{
		{ID: "child", Kind: "process", Data: json.RawMessage(`{"stdio":"pipe"}`), Files: map[string]*os.File{"stdin": inputW, "stdout": outputR}},
	})
	if err != nil {
		t.Fatal(err)
	}
	args := []string{os.Args[0], "-test.run=^TestHandoffRepackSubprocess$", "-test.count=1"}
	if err := prepared.Exec(os.Args[0], args, handoffTestEnv("repack-hop2", repackHopEnv())); err != nil {
		t.Fatalf("hop 1 exec: %v", err)
	}
	t.Fatal("successful Exec returned")
}

func repackHop2(t *testing.T) {
	t.Helper()
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("hop 2 Restore = (%v, %v)", restored, err)
	}
	// Take only half of the resource: the receiver's import stopped midway.
	stdin := handoffTake(t, restored, "child", "stdin")
	if len(restored.Borrow()) != 1 || len(restored.Borrow()[0].Files) != 1 || restored.Borrow()[0].Files["stdout"] == nil {
		t.Fatalf("Borrow must return exactly the untaken stdout: %+v", restored.Borrow())
	}
	taken := handoff.Resource{ID: "child", Kind: "process", Data: json.RawMessage(`{"stdio":"pipe"}`), Files: map[string]*os.File{"stdin": stdin}}
	prepared, err := restored.Repack(taken)
	if err != nil {
		t.Fatalf("repack with the taken half: %v", err)
	}
	// The exec that fails: the bundle must remain exactly as usable.
	args := []string{os.Args[0], "-test.run=^TestHandoffRepackSubprocess$", "-test.count=1"}
	env := handoffTestEnv("repack-hop3", repackHopEnv())
	failed := prepared.Exec(filepath.Join(os.Getenv(handoffTestDirEnv), "no-such-binary"), args, env)
	if failed == nil {
		t.Fatal("exec of a missing binary succeeded")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	// Still borrowable, still takeable: nothing moved.
	if len(restored.Borrow()) != 1 {
		t.Fatalf("failed exec consumed the borrowed files: %+v", restored.Borrow())
	}
	stdout := handoffTake(t, restored, "child", "stdout")
	if len(restored.Borrow()) != 0 {
		t.Fatalf("Borrow still lists a taken file: %+v", restored.Borrow())
	}
	// A refused import's salvage would carry both halves now; merge and
	// re-dispatch the whole thing under the same State.
	whole := handoff.Resource{ID: "child", Kind: "process", Data: json.RawMessage(`{"stdio":"pipe"}`), Files: map[string]*os.File{"stdin": stdin, "stdout": stdout}}
	if _, err := handoff.MergeResources([]handoff.Resource{taken}, []handoff.Resource{whole}); err == nil {
		t.Fatal("two owners of child/stdin merged silently")
	}
	prepared, err = restored.Repack(whole)
	if err != nil {
		t.Fatalf("second repack: %v", err)
	}
	values := repackHopEnv()
	values["TETHER_HANDOFF_TEST_FAILED_EXEC"] = failed.Error()
	env = handoffTestEnv("repack-hop3", values)
	if err := prepared.Exec(os.Args[0], args, env); err != nil {
		t.Fatalf("hop 2 exec: %v", err)
	}
	t.Fatal("successful Exec returned")
}

// repackHopEnv carries the fixture directory and nonce to the next hop:
// handoffTestEnv strips every test key from the inherited environment (the
// dispatch keys must never leak into a business child), so each hop names
// them again explicitly.
func repackHopEnv() map[string]string {
	return map[string]string{handoffTestDirEnv: os.Getenv(handoffTestDirEnv), handoffTestIDEnv: os.Getenv(handoffTestIDEnv)}
}

func repackHop3(t *testing.T) {
	t.Helper()
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("hop 3 Restore = (%v, %v)", restored, err)
	}
	defer func() { _ = restored.Close() }()
	var fixture repackFixture
	if err := json.Unmarshal(restored.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if os.Getpid() != fixture.ParentPID {
		t.Fatalf("pid changed across hops: %d vs %d", os.Getpid(), fixture.ParentPID)
	}
	stdin := handoffTake(t, restored, "child", "stdin")
	stdout := handoffTake(t, restored, "child", "stdout")
	handoffWriteLine(t, stdin, "resume-"+fixture.Nonce)
	continued := handoffReadLine(t, bufio.NewReader(stdout))
	var status syscall.WaitStatus
	for {
		pid, err := syscall.Wait4(fixture.ChildPID, &status, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil || pid != fixture.ChildPID || !status.Exited() {
			t.Fatalf("wait4 across three hops: pid=%d status=%v err=%v", pid, status, err)
		}
		break
	}
	sum := sha256.Sum256(restored.State.Data)
	receipt := repackReceipt{Hop: 3, PID: os.Getpid(), ChildPID: fixture.ChildPID, StateSHA: hex.EncodeToString(sum[:]),
		Generation: restored.State.Generation, Continued: continued, ExitStatus: status.ExitStatus(),
		FailedExec: os.Getenv("TETHER_HANDOFF_TEST_FAILED_EXEC")}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv(handoffTestDirEnv), "receipt.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestMergeResourcesRefusesDoubleOwner pins the merge rules on their own:
// union by ID, kind/data must agree, one owner per file name.
func TestMergeResourcesRefusesDoubleOwner(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close(); _ = w.Close() }()
	a := []handoff.Resource{{ID: "x", Kind: "process", Files: map[string]*os.File{"stdin": w}}}
	b := []handoff.Resource{{ID: "x", Kind: "process", Files: map[string]*os.File{"stdout": r}}, {ID: "y", Kind: "file"}}
	merged, err := handoff.MergeResources(a, b)
	if err != nil || len(merged) != 2 || len(merged[0].Files) != 2 || merged[1].ID != "y" {
		t.Fatalf("merge: %+v %v", merged, err)
	}
	if _, err := handoff.MergeResources(a, []handoff.Resource{{ID: "x", Kind: "socket"}}); err == nil {
		t.Fatal("kind disagreement merged")
	}
	if _, err := handoff.MergeResources(a, a); err == nil {
		t.Fatal("double owner merged")
	}
	if _, err := handoff.MergeResources(a, []handoff.Resource{{ID: "x", Kind: "process", Data: json.RawMessage(`{}`)}}); err == nil {
		t.Fatal("data disagreement merged")
	}
	if got := (*handoff.Restored)(nil).Borrow(); got != nil {
		t.Fatalf("nil bundle borrowed %+v", got)
	}
}
