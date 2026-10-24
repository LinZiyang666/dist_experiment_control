//go:build linux

// agent_sys_event_test.go — the sys.events dispatcher (handleSysEvent) as a
// unit: which event family reaches which action. The revocation itself is
// tested in process_admission_test.go by calling revokeSessionWorkloads from
// a subscription the TEST owns, which cannot see whether the agent's own
// dispatcher ever routes session_destroyed there; the real-binary e2e does,
// but only inside the -race matrix. This pins the routing in `make test`.
package agent

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S1 internal review, lane e2e (Q5) — the plan's mutation
// "do not dispatch session_destroyed → red" was green against the unit suite
// because the unit test subscribed its own handler. Dispatch is one `if` in
// handleSysEvent; this is the test that reads it.
func TestHandleSysEventRoutesSessionDestroyedToRevocationAndKeepsTheRun(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url)
	// The direct child stays a zombie until its owner reaps it, so liveness
	// is read on a grandchild of the same process group: kill(-pgid) takes it
	// too, init reaps it, and kill(pid, 0) then reports ESRCH.
	spawn := func(pid string) int {
		t.Helper()
		pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
		stdio, warning, err := startExecPipes(a, pid, &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "sleep 30 & echo $! > " + pidFile + "; wait"}})
		if err != nil || warning != "" {
			t.Fatalf("startExecPipes(%s): %v %q", pid, err, warning)
		}
		t.Cleanup(func() {
			stdio.Close()
			a.reapManagedChildren()
			_ = a.workloads.AllowReap(pid)
			_, _ = a.workloads.Wait(pid)
			_ = a.workloads.Forget(pid)
		})
		deadline := time.Now().Add(5 * time.Second)
		for {
			if body, err := os.ReadFile(pidFile); err == nil {
				if gpid, err := strconv.Atoi(strings.TrimSpace(string(body))); err == nil && gpid > 0 {
					t.Cleanup(func() { _ = syscall.Kill(gpid, syscall.SIGKILL) })
					return gpid
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("grandchild pid of %s never appeared", pid)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	alive := func(gpid int) bool { return !errors.Is(syscall.Kill(gpid, 0), syscall.ESRCH) }
	waitDead := func(gpid int) bool {
		deadline := time.Now().Add(5 * time.Second)
		for alive(gpid) {
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(5 * time.Millisecond)
		}
		return true
	}
	event := func(kind, sid, nid string) *nats.Msg {
		body, _ := json.Marshal(map[string]string{"type": kind, "sid": sid, "nid": nid})
		return &nats.Msg{Subject: proto.SubjSysEvents, Data: body}
	}
	runEnded := false
	cancelRun := func() { runEnded = true }

	// A destroyed session of another name is not ours: nothing dies.
	foreign := spawn("sysev-foreign")
	handleSysEvent(a, event("session_destroyed", "not-"+a.cfg.SID, ""), cancelRun)
	time.Sleep(100 * time.Millisecond)
	if !alive(foreign) || runEnded {
		t.Fatalf("a foreign session_destroyed acted on this agent: alive=%v runEnded=%v", alive(foreign), runEnded)
	}
	// Ours: the subtree is killed through revokeSessionWorkloads, the daemon's
	// run is NOT ended (Q3 ruling: exiting would take the whole unit's cgroup).
	mine := spawn("sysev-mine")
	handleSysEvent(a, event("session_destroyed", a.cfg.SID, ""), cancelRun)
	if !waitDead(mine) {
		t.Fatal("session_destroyed for this session did not reach revokeSessionWorkloads: the subtree is still alive")
	}
	if runEnded {
		t.Fatal("session_destroyed ended the daemon's run; only agent_evicted may")
	}
	// Positive control for the cancelRun wiring: eviction of THIS node ends the run.
	handleSysEvent(a, event("agent_evicted", a.cfg.SID, nidOf(a)), cancelRun)
	if !runEnded {
		t.Fatal("agent_evicted for this node did not end the run")
	}
}
