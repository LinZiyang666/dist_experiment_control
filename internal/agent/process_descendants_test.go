//go:build linux

// Real: shell parent/descendant, inherited pipes, Linux zombies, agent exec and NATS kill.
// Stubbed: broker authorization and external evict delivery; uses the administrative cleanup hook.
// Does not cover image replacement or descendants which deliberately leave the process group.
// origin: docs/reviews/hot-update-plan.md §4.2.
package agent

import (
	"context"
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

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

const descendantTestBudget = 10 * time.Second
const descendantHelperEnv = "TETHER_TEST_EXEC_DESCENDANT_MODE"

// A private subreaper is necessary: the fixture really exits its shell before
// killing the descendant, which otherwise leaves the descendant for host PID 1.
// Keeping that setting in a subprocess avoids changing other tests' wait owners.
func TestExecDescendantsRemainKillable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if mode := os.Getenv(descendantHelperEnv); mode != "" {
		exerciseExecDescendants(t, mode)
		return
	}
	for _, mode := range []string{"kill", "evict"} {
		t.Run(mode, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			ctx, cancel := context.WithTimeout(context.Background(), 4*descendantTestBudget)
			defer cancel()
			dir := t.TempDir()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecDescendantsRemainKillable$", "-test.count=1")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "TMPDIR="+dir, descendantHelperEnv+"="+mode)
			cmd.SysProcAttr = testProcessAttributes()
			cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
			defer func() {
				if cmd.Process != nil {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				}
			}()
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s descendant helper: %v\n%s", mode, err, output)
			}
		})
	}
}

func exerciseExecDescendants(t *testing.T, mode string) {
	t.Helper()
	if mode != "kill" && mode != "evict" {
		t.Fatalf("unknown descendant helper mode %q", mode)
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil)
	a.ncBox.Store(nc)
	inbox := nc.NewInbox()
	out, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = out.Unsubscribe() })
	killSubject := nc.NewInbox()
	killSub, err := nc.Subscribe(killSubject, func(msg *nats.Msg) { a.handleKillForwarded(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = killSub.Unsubscribe() })
	if err := nc.FlushTimeout(descendantTestBudget); err != nil {
		t.Fatal(err)
	}

	const id = "shell-with-live-descendant"
	pidFile := filepath.Join(t.TempDir(), "owned-pids")
	type execResult struct {
		code int
		err  error
	}
	result := make(chan execResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		rc, err := a.runChild(nc, inbox, id, &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{
			"/bin/sh", "-c",
			`sleep 60 & descendant=$!; printf '%s %s\n' "$$" "$descendant" > "$1"; printf '%s %s\n' "$$" "$descendant"; exit 37`,
			"descendant-fixture", pidFile,
		}})
		result <- execResult{rc, err}
	}()
	var leader, descendant int
	descendantReaped := false
	t.Cleanup(func() {
		// The subreaper owns this exact descendant and no other code waits for it.
		// Its PID cannot recycle before our Wait4, even if it already became a zombie.
		if descendant == 0 {
			if contents, err := os.ReadFile(pidFile); err == nil {
				_, _ = fmt.Sscanf(string(contents), "%d %d", &leader, &descendant)
			}
		}
		a.reapManagedChildren()
		if descendant > 0 && !descendantReaped {
			_ = syscall.Kill(descendant, syscall.SIGKILL)
			if _, err := waitOwnedDescendant(descendant); err != nil {
				t.Errorf("cleanup descendant %d: %v", descendant, err)
			}
		}
		select {
		case <-done:
		case <-time.After(descendantTestBudget):
			t.Error("descendant cleanup left the exec handler blocked")
		}
	})

	var stdout string
	deadline := time.Now().Add(descendantTestBudget)
	for !strings.Contains(stdout, "\n") {
		msg, err := out.NextMsg(time.Until(deadline))
		if err != nil {
			t.Fatal(err)
		}
		var chunk proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &chunk); err != nil || chunk.Kind != "stdout" {
			t.Fatalf("fixture output: %+v err=%v", chunk, err)
		}
		stdout += string(chunk.Data)
	}
	if n, err := fmt.Sscanf(stdout, "%d %d", &leader, &descendant); err != nil || n != 2 || leader <= 0 || descendant <= 0 {
		t.Fatalf("fixture identities %q: %v", stdout, err)
	}
	for {
		state, parent, group, err := descendantProcessState(leader)
		if err != nil {
			t.Fatalf("leader was reaped before output EOF: %v", err)
		}
		if state == "Z" {
			if parent != os.Getpid() || group != leader {
				t.Fatalf("leader identity: state=%s parent=%d group=%d", state, parent, group)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shell did not exit into a retained zombie")
		}
		time.Sleep(time.Millisecond)
	}
	state, parent, group, err := descendantProcessState(descendant)
	if err != nil || state == "Z" || parent != os.Getpid() || group != leader {
		t.Fatalf("live adopted descendant: state=%s parent=%d group=%d err=%v", state, parent, group, err)
	}
	for _, fd := range []int{1, 2} {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", descendant, fd))
		if err != nil || !strings.HasPrefix(target, "pipe:[") {
			t.Fatalf("descendant lost output pipe %d: %q %v", fd, target, err)
		}
	}
	// Give the independent reaper repeated scheduling opportunities while the
	// real output pipes remain open. This is a fixture observation window, not
	// an assertion about the product's exact polling interval. WNOWAIT never
	// competes with the manager for consuming the leader's exit status.
	observeUntil := time.Now().Add(descendantTestBudget / 100)
	for time.Now().Before(observeUntil) {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, leader, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil || info.Signo == 0 {
			t.Fatalf("leader no longer waitable while descendant holds output: signo=%d err=%v", info.Signo, err)
		}
		time.Sleep(time.Millisecond)
	}
	rows := a.workloads.Snapshot()
	if len(rows) != 1 || rows[0].ID != id || rows[0].Exit != nil || !rows[0].ReapHeld {
		t.Fatalf("output-lifetime ownership not retained: %+v", rows)
	}
	select {
	case <-done:
		t.Fatal("exec finished while the descendant still owned its output pipes")
	default:
	}

	if mode == "kill" {
		body, err := json.Marshal(proto.KillReq{PID: id, Signal: int(syscall.SIGKILL)})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := nc.Request(killSubject, body, descendantTestBudget)
		if err != nil {
			t.Fatal(err)
		}
		var response proto.KillResp
		if err := json.Unmarshal(msg.Data, &response); err != nil || !response.OK {
			t.Fatalf("kill after leader exit: %+v err=%v", response, err)
		}
	} else {
		a.reapManagedChildren()
	}
	select {
	case got := <-result:
		if got.err != nil || got.code != 37 {
			t.Fatalf("original leader exit changed after descendant kill: %+v", got)
		}
	case <-time.After(descendantTestBudget):
		t.Fatal("kill/evict did not release descendant-held pipes")
	}
	status, err := waitOwnedDescendant(descendant)
	if err != nil {
		t.Fatal(err)
	}
	descendantReaped = true
	if !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("descendant was not killed by the administrative path: %v", status)
	}
	if rows := a.workloads.Snapshot(); len(rows) != 0 {
		t.Fatalf("manager retained a completed exec: %+v", rows)
	}
	final := localProcessSnapshot(a)
	if len(final) != 1 || final[0].PID != id || final[0].State != "exited" || final[0].RC == nil || *final[0].RC != 37 {
		t.Fatalf("courier lost the original exit: %+v", final)
	}
}

func descendantProcessState(pid int) (state string, parent, group int, err error) {
	contents, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, 0, err
	}
	_, err = fmt.Sscanf(string(contents)[strings.LastIndexByte(string(contents), ')')+1:], " %s %d %d", &state, &parent, &group)
	return
}

func waitOwnedDescendant(pid int) (syscall.WaitStatus, error) {
	deadline := time.Now().Add(descendantTestBudget)
	for time.Now().Before(deadline) {
		var status syscall.WaitStatus
		got, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.ECHILD) {
			// Failure cleanup can reach us just before the killed shell exits
			// and reparents its child to this helper's subreaper.
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil || got == pid {
			return status, err
		}
		time.Sleep(time.Millisecond)
	}
	return 0, fmt.Errorf("descendant %d did not become waitable", pid)
}
