//go:build linux

package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

type ptyBootTask struct {
	bootTask
	memberPID   int
	memberTicks int64
}

func startPTYBootTask(t *testing.T, a *Agent) ptyBootTask {
	t.Helper()
	task := ptyBootTask{bootTask: bootTask{scope: ptyTestScope(a)}}
	task.handle = ptyProbe(t, a, task.scope)
	memberFile := filepath.Join(t.TempDir(), "member")
	// Job control places the stopped child in a DIFFERENT pgrp within the
	// original terminal session. Killing only the leader pgrp cannot pass.
	run := proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c",
		"stty raw -echo; set -m; /bin/sh -c 'trap \"\" HUP TERM; printf \"%s\" \"$$\" > \"$1\"; kill -STOP \"$$\"; exec sleep 60' member \"$1\" & while [ ! -s \"$1\" ]; do sleep 0.01; done; printf ready; exec sleep 60", "fixture", memberFile}}
	create := ptyTaskRequest(task.scope, task.handle, "create")
	create.Run = &run
	reply, err := a.processRuntime.Handle(task.scope, ownProcessRun(task.scope, create), "_TINBOX.pty")
	if err != nil || reply.PID == "" {
		t.Fatalf("PTY create: %v", err)
	}
	task.pid = reply.PID
	waitFor(t, bootTestBudget, func() bool {
		r, err := a.processRuntime.Handle(task.scope, ptyTaskRequest(task.scope, task.handle, "resume"), "_TINBOX.pty")
		return err == nil && r.Phase == "running" && r.Stdout != nil && strings.Contains(string(r.Stdout.Data), "ready")
	})
	var ok bool
	task.row, ok = a.workloads.Lookup(task.pid)
	if !ok || task.row.StartTimeTicks == 0 || !task.row.ReapHeld {
		t.Fatal("PTY has no draining kernel owner")
	}
	data, err := os.ReadFile(memberFile)
	if err != nil {
		t.Fatal(err)
	}
	task.memberPID, err = strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	task.memberTicks, err = readStartTimeTicks(task.memberPID)
	if err != nil {
		t.Fatal(err)
	}
	// The HUP-immune member stops only after publishing its identity. Otherwise
	// orphan-group HUP/CONT can kill the fixture and falsely bless PGID-only
	// cleanup, even though no product census signalled the stopped descendant.
	// origin: hot-update S4 plan §6.2 / T26 mutation: only original PGID.
	waitFor(t, bootTestBudget, func() bool {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", task.memberPID))
		if err != nil {
			return false
		}
		fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
		return len(fields) > 0 && fields[0] == "T"
	})
	sid, err := unix.Getsid(task.memberPID)
	if err != nil {
		t.Fatal(err)
	}
	pgid, err := syscall.Getpgid(task.memberPID)
	if err != nil || sid != task.row.OSPID || pgid == task.row.OSPID {
		t.Fatalf("fixture must have a stopped same-session member in another pgrp: sid=%d pgid=%d: %v", sid, pgid, err)
	}
	t.Cleanup(func() {
		// This fixture-owned identity is a cleanup backstop, never the oracle.
		// The assertion below must observe its death BEFORE teardown.
		if ticks, err := readStartTimeTicks(task.memberPID); err == nil && ticks == task.memberTicks {
			_ = syscall.Kill(task.memberPID, syscall.SIGKILL)
		}
	})
	return task
}

func ptyMemberAlive(pid int, ticks int64) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		return true
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) < 20 {
		return true
	}
	current, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return true
	}
	return current == ticks && fields[0] != "Z" && fields[0] != "X"
}

func waitPTYRevoked(t *testing.T, a *Agent, task ptyBootTask) proto.ProcessResumeReply {
	t.Helper()
	var terminal proto.ProcessResumeReply
	waitFor(t, bootTestBudget, func() bool {
		r, err := a.processRuntime.Handle(task.scope, ptyTaskRequest(task.scope, task.handle, "resume"), "_TINBOX.pty")
		terminal = r
		return err == nil && r.Phase == "terminal" && strings.HasPrefix(r.Error, "revoked:") &&
			errors.Is(syscall.Kill(task.row.OSPID, 0), syscall.ESRCH) && !ptyMemberAlive(task.memberPID, task.memberTicks)
	})
	if terminal.TaskKind != proto.ProcessKindPTY || terminal.TerminalID == "" || terminal.InputError == "" {
		t.Fatal("revocation lost terminal kind or input responsibility")
	}
	return terminal
}

func ptyEvent(t *testing.T, a *Agent) {
	t.Helper()
	var canceled atomic.Bool
	handleSysEvent(a, &nats.Msg{Data: []byte(`{"type":"session_destroyed","sid":"lab"}`)}, func() { canceled.Store(true) })
	if canceled.Load() {
		t.Fatal("session_destroyed proactively exited the daemon")
	}
}

func ptyRegisterResponder(t *testing.T, url, code string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	_, err = nc.Subscribe(proto.SubjNodeRegister("lab", "node"), func(m *nats.Msg) {
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: code == "", Code: code})
		_ = m.Respond(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S4 plan §6.2 / §6.3 / T26. Events do not retire the
// daemon; authoritative register-gone replies revoke first and then exit.
// Both must clear a stopped foreground-capable member outside the leader PGID.
func TestSessionDestroyedRevokesPTYAcrossCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if isolatePTYAuthorityTest(t, "TestSessionDestroyedRevokesPTYAcrossCut") {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		for _, arm := range []string{"before-freeze", "after-freeze", "before-activate", "active", proto.CodeSessionNotFound, proto.CodeSessionNotFoundOrDeleting} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exercisePTYRevocationCut(t, arm)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY revoke across cut x5", beforeG)
	testharness.AssertNoFDLeak(t, "PTY revoke across cut x5", beforeFD, 0)
}

func exercisePTYRevocationCut(t *testing.T, arm string) {
	oldClock := processBootClock
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(now)
	processBootClock = func() (int64, error) { return clock.Load(), nil }
	t.Cleanup(func() { processBootClock = oldClock })
	url := startNATS(t)
	gone := strings.HasPrefix(arm, "session_")
	code := ""
	if gone {
		code = arm
	}
	ptyRegisterResponder(t, url, code)
	a := newBootAgent(t, url, "node")
	task := startPTYBootTask(t, a)
	clock.Add(int64(time.Millisecond)) // authority loss must follow lease issuance
	if arm == "before-freeze" {
		ptyEvent(t, a)
		waitPTYRevoked(t, a, task)
	}
	f := freezeBootAgent(t, a)
	if arm == "after-freeze" {
		ptyEvent(t, a)
	}
	prepared, err := f.Prepare() // production's final export must carry the late event
	if err != nil {
		t.Fatal(err)
	}
	_ = prepared.Close()
	img, _ := cutBootImage(t, a, f)
	b, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), BootImage: img, RemoteFSMode: "off", Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, b)
	if arm == "before-activate" {
		ptyEvent(t, b)
	}
	if err := f.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := activateImage(b); err != nil {
		t.Fatal(err)
	}
	if gone {
		// Observe the revoked terminal before Run's exit funnel owns Close.
		// Otherwise a correct Close removes the very record this oracle reads.
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		if _, err := b.register(t.Context(), nc); !errors.Is(err, errSessionGone) {
			t.Fatalf("register-gone lost authority classification: %v", err)
		}
		waitPTYRevoked(t, b, task)
		done := runBootAgent(t, b)
		select {
		case err := <-done:
			if !errors.Is(err, errSessionGone) {
				t.Fatalf("gone daemon exit lost authority classification: %v", err)
			}
		case <-time.After(bootTestBudget):
			t.Fatal("register-gone did not exit the daemon")
		}
		return
	}
	done := runBootAgent(t, b)
	if arm == "active" {
		waitFor(t, bootTestBudget, func() bool { return b.loadRunCtx() != nil })
		ptyEvent(t, b)
	}
	terminal := waitPTYRevoked(t, b, task)
	select {
	case err := <-done:
		t.Fatalf("event exited the daemon: %v", err)
	default:
	}
	if rc := b.loadRunCtx(); rc != nil && rc.Err() != nil {
		t.Fatal("event canceled the daemon context")
	}
	for _, op := range []string{"input", "resize", "signal", "keepalive", "hangup"} {
		r := ptyTaskRequest(task.scope, task.handle, op)
		if op == "input" {
			r.Input = []byte("late")
		} else {
			r.ControlSeq = 1
		}
		if op == "resize" {
			r.Resize = &proto.PtyResizeEvent{Cols: 99, Rows: 33}
		}
		if op == "signal" {
			r.Signal = int(syscall.SIGINT)
		}
		_, _ = b.processRuntime.Handle(task.scope, r, "_TINBOX.pty")
	}
	if ptyMemberAlive(task.row.OSPID, task.row.StartTimeTicks) || ptyMemberAlive(task.memberPID, task.memberTicks) {
		t.Fatal("late operation revived a revoked process")
	}
	// Advance the one shared boot clock, create a real discarded tombstone,
	// and carry it through another production bootstrap to prove no resurrection.
	clock.Add(int64(time.Hour))
	if _, err := b.processRuntime.Sweep(); err != nil {
		t.Fatal(err)
	}
	g := freezeBootAgent(t, b)
	if len(g.Bundle().Runtime.Tombstones) == 0 {
		t.Fatal("fixture did not create a revoked PTY tombstone")
	}
	p, err := g.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	tombImage, _ := cutBootImage(t, b, g)
	c, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), BootImage: tombImage, RemoteFSMode: "off", Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, c)
	if err := g.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := activateImage(c); err != nil {
		t.Fatal(err)
	}
	r, err := c.processRuntime.Handle(task.scope, ptyTaskRequest(task.scope, task.handle, "resume"), "_TINBOX.pty")
	if err != nil && !errors.Is(err, execsession.ErrOutputDiscarded) {
		t.Fatal(err)
	}
	if r.TaskKind != proto.ProcessKindPTY || r.TerminalID != terminal.TerminalID || r.InputACK != terminal.InputACK || r.InputAccepted != terminal.InputAccepted || r.InputError != terminal.InputError {
		t.Fatal("imported tombstone lost final PTY responsibility")
	}
	late := ptyTaskRequest(task.scope, task.handle, "create")
	late.Run = &proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/true"}}
	_, _ = c.processRuntime.Handle(task.scope, late, "_TINBOX.pty")
	if len(c.workloads.Snapshot()) != 0 {
		t.Fatal("imported tombstone recreated a child")
	}
}

// origin: hot-update S8 plan §6.2 / P24. Losing the deletion event must
// still revoke an imported PTY before a replacement session can authorize it.
// origin: S8 internal review FIX-AGENTRACE-B F1
func TestPTYSessionRecreationBoundary(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	if isolatePTYAuthorityTest(t, "TestPTYSessionRecreationBoundary") {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"before-freeze", "after-freeze", "after-prepare", "before-activate", "reconnect"} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) { exercisePTYRecreatedAuthority(t, arm) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY recreated authority x5", beforeG)
	testharness.AssertNoFDLeak(t, "PTY recreated authority x5", beforeFD, 0)
}

// Once a register has confirmed the inherited session, a subsequent broker
// rejection must revoke the terminal before the caller handles the error.
// This isolates the live cleanup obligation from the legacy image's missing
// S8 adjunct in the other four recreation windows.
// origin: S8 internal review FIX-AGENTRACE-B F1
func TestPTYIncarnationRejectionRevokesSession(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	if isolatePTYAuthorityTest(t, "TestPTYIncarnationRejectionRevokesSession") {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exercisePTYRecreatedAuthority(t, "reconnect") })
	}
	testharness.AssertNoGoroutineLeak(t, "PTY incarnation rejection x5", beforeG)
	testharness.AssertNoFDLeak(t, "PTY incarnation rejection x5", beforeFD, 0)
}

func exercisePTYRecreatedAuthority(t *testing.T, arm string) {
	t.Helper()
	url, db, nc := bootBroker(t)
	// The old bootstrap helper's placeholder is intentionally not a PHC hash.
	// Give this real authority writer valid creation material without running
	// a guessed-PIN verifier or logging any of its input.
	pin := func(value byte) string {
		return "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{value}, 16)) + "$" + base64.RawStdEncoding.EncodeToString(make([]byte, 32))
	}
	if _, err := db.Exec(`UPDATE sessions SET pin_hash=? WHERE sid='lab'`, pin(1)); err != nil {
		t.Fatal(err)
	}
	a := newBootAgent(t, url, "node")
	if _, err := a.register(t.Context(), nc); err != nil {
		t.Fatal(err)
	}
	// Check the actual broker reply, rather than synthesizing a carried digest.
	body, _ := json.Marshal(proto.NodeRegisterReq{ProtoVersion: proto.ProtoVersion, NID: "node", InstanceID: a.instanceID})
	msg, err := nc.Request(proto.SubjNodeRegister("lab", "node"), body, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var issued struct {
		Incarnation string `json:"incarnation"`
	}
	if err := json.Unmarshal(msg.Data, &issued); err != nil || len(issued.Incarnation) != 64 {
		t.Fatal("NOT_RUN pending P13/P15/P16: capable register must issue authority incarnation")
	}
	if agentIncarnation(a) != issued.Incarnation {
		t.Fatal("the source did not learn the incarnation its broker issued")
	}
	task := startPTYBootTask(t, a)
	// An S8 cut carries the authority only in the required upgrade-cut-v1
	// adjunct (plan §6.1/§6.2): a real granted Start of THIS agent packs it
	// from the live incarnation, and its returned exec leaves the journal
	// holding exactly that accepted cut for the receiver to import.
	// origin: S8 FIX-DESCRIBE (FIX-AGENTRACE-B request: P24 fixture over the real adjunct)
	route, receipt, state := recreationAdjunct(t, a)
	if len(state.Authorities) != 1 || state.Authorities[0].SID != "lab" || state.Authorities[0].Incarnation != issued.Incarnation {
		t.Fatal("the cut's adjunct does not carry the incarnation the broker issued")
	}
	recreate := func() {
		// Owner/SID/NID stay equal. A fresh creation salt alone must distinguish
		// the replacement; no session_destroyed event is delivered in this arm.
		if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
			t.Fatal(err)
		}
		stackharness.SeedSession(t, db, "lab", task.scope.ActorFP)
		if _, err := db.Exec(`UPDATE sessions SET pin_hash=? WHERE sid='lab'`, pin(2)); err != nil {
			t.Fatal(err)
		}
	}
	if arm == "before-freeze" {
		recreate()
	}
	f := freezeBootAgent(t, a)
	if arm == "after-freeze" {
		recreate()
	}
	prepared, err := f.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	_ = prepared.Close()
	if arm == "after-prepare" {
		recreate()
	}
	img := recreationImage(t, a, f, state)
	b, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), BootImage: img, RemoteFSMode: "off", Logger: testharness.SilentLog(),
		Upgrade: &UpgradeDependencies{Key: route.key, Store: route.store}, UpgradeRunningImagePath: receipt.TargetPath, UpgradeNoExit: true,
		UpgradeExecutablePath: filepath.Join(t.TempDir(), "tether")})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, b)
	if b.cfg.Upgrade == nil || b.cfg.Upgrade.Cut == nil || agentIncarnation(b) != issued.Incarnation {
		t.Fatal("the receiver did not inherit the cut's incarnation before its first registration")
	}
	if arm == "before-activate" {
		recreate()
	}
	if err := f.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := activateImage(b); err != nil {
		t.Fatal(err)
	}
	// Activation handed the inherited reservation to the commit owner. A real
	// daemon whose register is rejected exits and closes it; this in-process
	// receiver releases it at teardown instead.
	t.Cleanup(func() {
		if ports := b.cfg.Upgrade; ports != nil && ports.Lease != nil {
			_ = ports.Lease.Close()
			ports.Lease = nil
		}
	})
	if arm == "reconnect" {
		if _, err := b.register(t.Context(), nc); err != nil {
			t.Fatal(err)
		}
		recreate()
	}
	_, err = b.register(t.Context(), nc)
	if err == nil || !strings.Contains(err.Error(), "session_incarnation_mismatch") {
		t.Fatalf("old image registered in recreated session: %v", err)
	}
	waitPTYRevoked(t, b, task)
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE sid='lab'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected old image wrote a replacement-session node")
	}
	for _, op := range []string{"input", "resize", "signal", "keepalive", "hangup"} {
		req := ptyTaskRequest(task.scope, task.handle, op)
		req.ControlSeq = 1
		if op == "input" {
			req.Input = []byte("late")
		}
		if op == "resize" {
			req.Resize = &proto.PtyResizeEvent{Cols: 99, Rows: 33}
		}
		if op == "signal" {
			req.Signal = int(syscall.SIGINT)
		}
		_, _ = b.processRuntime.Handle(task.scope, req, "_TINBOX.pty")
	}
	if ptyMemberAlive(task.row.OSPID, task.row.StartTimeTicks) || ptyMemberAlive(task.memberPID, task.memberTicks) {
		t.Fatal("old incarnation regained PTY execution authority")
	}
}

// recreationAdjunct drives one real granted Start of the registered source to
// its final re-export and captures the adjunct that exec would carry: the
// production runtime port packs this agent's live incarnation into it. The
// exec returns, so the source keeps serving its PTY and the journal keeps the
// accepted cut (partial_active) the receiver must import; the source's
// transaction wiring is then detached so the fixture's freeze below remains
// the cut of the business owners.
// origin: S8 FIX-DESCRIBE (FIX-AGENTRACE-B request: P24 fixture over the real adjunct)
func recreationAdjunct(t *testing.T, a *Agent) (*grantedRoute, upgradeartifact.Receipt, upgradecut.State) {
	t.Helper()
	exe := filepath.Join(t.TempDir(), "tether")
	if err := os.WriteFile(exe, []byte("original image"), 0o755); err != nil {
		t.Fatal(err)
	}
	a.cfg.UpgradeExecutablePath, a.cfg.UpgradeNoExit = exe, true
	nc := cutNATS(t)
	r := openGrantedRoute(t, a, false)
	grant, receipt := r.publish(t, a, nc, s8Script("recreation target "+a.instanceID, s8Domains))
	var state upgradecut.State
	a.cfg.UpgradeExecFn = func(string) error {
		state, _ = preparedAdjunct(t)
		return syscall.ENOEXEC
	}
	defer func() { a.cfg.UpgradeExecFn = nil }()
	// The reply is the early accepted-unknown one, sent right before the exec
	// (agentupgradeport.ActivateNotifying), not the broker's lost-reply timeout.
	if resp := r.start(t, a, nc, grant, receipt); resp.Code != upgradetx.CodeUnsettled || !strings.Contains(resp.Error, "start accepted and activating") ||
		state.Attempt.TxID != grant.Handle.TxID {
		t.Fatalf("source did not reach its final re-export: code=%s error=%s", resp.Code, resp.Error)
	}
	r.detach(a)
	return r, receipt, state
}

// recreationImage is what the handoff delivers to the receiver: the source's
// own frozen owners and their descriptors (the business PTY), the accepted
// cut's adjunct, and a duplicate of the re-acquired domain lease as the
// adjunct's only auxiliary file (adjunctImage, over the real source).
// origin: S8 FIX-DESCRIBE (FIX-AGENTRACE-B request: P24 fixture over the real adjunct)
func recreationImage(t *testing.T, a *Agent, f *agentimage.Frozen, state upgradecut.State) *agentboot.Image {
	t.Helper()
	_, resources, err := a.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.SafePointWait)
	defer cancel()
	lease, err := upgradelock.Acquire(ctx, state.Attempt.Key, "")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := os.NewFile(uintptr(duplicate), "exported-lease")
	adjunct, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: borrowed})
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, append(append([]handoff.Resource(nil), resources...), adjunct))
	_ = borrowed.Close()
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	b := f.Bundle()
	img := agentboot.FromCut(b, b.Processes.Records, src, src.Borrow())
	t.Cleanup(func() {
		if img.Phase() != agentboot.PhaseActive && img.Phase() != agentboot.PhaseReady {
			img.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
		}
	})
	return img
}

// The entire isolated runtime tree, including differently grouped PTY jobs,
// belongs to the shared external supervisor even if the test runner dies.
// origin: hot-update S8 plan §9.1 / L21 / P24
func isolatePTYAuthorityTest(t *testing.T, name string) bool {
	t.Helper()
	const modeKey = "_TETHER_PTY_AUTHORITY_TEST"
	if os.Getenv(modeKey) == name {
		if err := os.Unsetenv(modeKey); err != nil {
			t.Fatal(err)
		}
		return false
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	dir := t.TempDir()
	helper := filepath.Join(dir, "authority.test")
	if err := os.Link(os.Args[0], helper); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, helper, "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "TMPDIR="+dir, modeKey+"="+name)
	out, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("isolated %s: %v\n%s", name, err, out)
	}
	if !bytes.Contains(out, []byte("=== RUN   "+name+"\n")) {
		t.Fatal("isolated authority selector did not exercise its body")
	}
	t.Log(strings.TrimSpace(string(out)))
	return true
}

// origin: hot-update S4 plan §6.2 / T26. The event branch must preserve its
// caller's daemon context while revoking workloads; register-gone owns exit.
func TestSessionDestroyedEventKeepsDaemonContext(t *testing.T) {
	if isolatePTYAuthorityTest(t, "TestSessionDestroyedEventKeepsDaemonContext") {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := newBootAgent(t, startNATS(t), "node")
			task := startBootTask(t, a)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			handleSysEvent(a, &nats.Msg{Data: []byte(`{"type":"session_destroyed","sid":"lab"}`)}, cancel)
			waitBootRevoked(t, a, task)
			if ctx.Err() != nil {
				t.Fatal("session_destroyed canceled the daemon context")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "session_destroyed event context x5", beforeG)
	testharness.AssertNoFDLeak(t, "session_destroyed event context x5", beforeFD, 0)
}
