//go:build linux

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 plan §2.1 / §8 T28. The nested identity is untrusted,
// and every rejected shape must leave the original terminal's byte/control
// and keepalive state untouched, including requests that lie about task kind.
func TestPTYScopeAndActorStamp(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("ownership", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		scope := procrequest.Scope{ActorFP: "broker-stamped-actor"}
		original := proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "original"}, Env: map[string]string{"MARK": "original"}, ActorFP: "forged-nested-actor"}
		owned := ownProcessRun(scope, proto.ProcessResumeReq{Run: &original})
		original.Argv[1], original.Env["MARK"] = "mutated", "mutated"
		if owned.Run.ActorFP != scope.ActorFP || owned.Run.Argv[1] != "original" || owned.Run.Env["MARK"] != "original" {
			t.Fatal("asynchronous run retained caller storage or trusted the nested actor")
		}
		owned.Run.Argv[0], owned.Run.Env["NEW"] = "owned", "owned"
		if original.Argv[0] != "/bin/sh" || original.Env["NEW"] != "" || original.ActorFP != "forged-nested-actor" {
			t.Fatal("stamping mutated the caller's request")
		}
	})
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprintf("protocol/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exercisePTYScope(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "PTY scope matrix x5", beforeG)
	testharness.AssertNoFDLeak(t, "PTY scope matrix x5", beforeFD, 0)
}

// origin: hot-update S4 plan §2.1/§3.3; P10 integration request — a refused
// PTY request still answers in the PTY envelope (TaskKind=pty, fixed empty
// stderr), or the reliable client rejects the shape and loses the real code.
func TestPTYRefusalEnvelopeNamesTaskKind(t *testing.T) {
	a := courierAgent(t, startNATS(t))
	nc := a.ncBox.Load()
	t.Cleanup(func() { stopProcessRuntime(t.Context(), a) })
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	actor := "SHA256:" + strings.Repeat("C", 43)
	for _, kind := range []string{proto.ProcessKindPTY, ""} {
		foreign := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 11, Op: "resume", AttachEpoch: 1,
			Kind: kind, ActorFP: actor, ClientLineage: "x",
			Handle: &proto.ProcessHandle{Lease: proto.ProcessLease{Version: 1, TargetInstance: "not-this-runtime", BootID: "b",
				SID: a.cfg.SID, NID: nidOf(a), ActorFP: actor, ClientLineage: "x", LeaseID: strings.Repeat("0", 32), ExpiresBootNS: 1},
				RequestID: "r"}}
		body, _ := json.Marshal(foreign)
		handleProcessResumeForwarded(a, nc, &nats.Msg{Subject: proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.ProcessResumeVerb),
			Reply: inbox, Data: body})
		msg, err := sub.NextMsg(3 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var reply proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.Kind != "error" || reply.Code != proto.CodeProcessInstanceLost {
			t.Fatalf("kind %q: refusal lost its code: %+v err=%v", kind, reply, err)
		}
		if kind == proto.ProcessKindPTY {
			if reply.TaskKind != proto.ProcessKindPTY || reply.Stderr == nil || !reply.Stderr.FIN || reply.Stderr.Offset != 0 {
				t.Fatalf("PTY refusal left the PTY envelope: %+v", reply)
			}
		} else if reply.TaskKind != "" || reply.Stderr != nil {
			// The exec refusal stays byte-identical to S1: no PTY fields leak in.
			t.Fatalf("exec refusal gained PTY envelope fields: %+v", reply)
		}
	}
}

func ptyTestScope(a *Agent) procrequest.Scope {
	return procrequest.Scope{SID: a.cfg.SID, NID: nidOf(a), ActorFP: "SHA256:" + strings.Repeat("A", 43), ClientLineage: "ptyclient"}
}

func ptyProbe(t *testing.T, a *Agent, scope procrequest.Scope) proto.ProcessHandle {
	t.Helper()
	reply, err := a.processRuntime.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion,
		CallID: 1, Op: "probe", Kind: proto.ProcessKindPTY, ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "_TINBOX.pty")
	if err != nil || reply.Lease == nil || reply.PTYVersion != proto.PTYVersion {
		t.Fatalf("complete PTY capability missing (requires S4 P6): version=%d lease=%t: %v", reply.PTYVersion, reply.Lease != nil, err)
	}
	return proto.ProcessHandle{Lease: *reply.Lease, RequestID: "ptytask"}
}

func ptyTaskRequest(scope procrequest.Scope, h proto.ProcessHandle, op string) proto.ProcessResumeReq {
	return proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 3, Op: op, Kind: proto.ProcessKindPTY,
		ActorFP: scope.ActorFP, Handle: &h, AttachEpoch: 1}
}

// ptyState reads only one task's metadata, without copying or printing the
// registry secret. Export remains borrowed; no receiver takes these descriptors.
func ptyState(t *testing.T, a *Agent, pid string) ptysession.Image {
	t.Helper()
	if err := a.processRuntime.Freeze(time.Now().Add(bootTestBudget)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.processRuntime.Thaw(); err != nil {
			t.Error(err)
		}
	}()
	image, _, err := a.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range image.Tasks {
		if task.PID != pid {
			continue
		}
		data, err := json.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Kind string            `json:"kind"`
			PTY  *ptysession.Image `json:"pty"`
		}
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		if state.Kind != proto.ProcessKindPTY || state.PTY == nil {
			t.Fatal("PTY task has no actor metadata")
		}
		return *state.PTY
	}
	t.Fatal("original PTY owner disappeared")
	return ptysession.Image{}
}

func exercisePTYScope(t *testing.T) {
	a := newBootAgent(t, startNATS(t), "node")
	cfg := processRuntimeConfig(a)
	cfg.Instance = "ptyscope"
	var starts atomic.Int32
	realStart := cfg.StartPTY
	cfg.StartPTY = func(pid string, run *proto.RunReq) (*pty.Session, string, error) {
		starts.Add(1)
		return realStart(pid, run)
	}
	rt, err := execsession.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.processRuntime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	a.processRuntime = rt
	scope := ptyTestScope(a)
	h := ptyProbe(t, a, scope)
	create := ptyTaskRequest(scope, h, "create")
	create.Run = &proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "stty raw -echo; printf ready; exec cat"},
		Env: map[string]string{"AUDIT": "preserved"}, ActorFP: "forged-nested-actor", KAIntervalMS: 1000}
	create = ownProcessRun(scope, create)
	reply, err := rt.Handle(scope, create, "_TINBOX.pty")
	if err != nil || reply.PID == "" {
		t.Fatalf("create: %v", err)
	}
	pid := reply.PID
	waitFor(t, bootTestBudget, func() bool {
		r, err := rt.Handle(scope, ptyTaskRequest(scope, h, "resume"), "_TINBOX.pty")
		return err == nil && r.Phase == "running" && r.Stdout != nil && string(r.Stdout.Data) == "ready"
	})
	pending := receiptPending(t, a.courier, pid, procEventStarted)
	var started proto.ProcStartedEvent
	if pending != nil {
		if err := json.Unmarshal(pending.payload, &started); err != nil {
			t.Fatal(err)
		}
	}
	if pending == nil || started.StartedByFP != scope.ActorFP || starts.Load() != 1 {
		t.Fatal("PTY start was not audited once with the broker actor")
	}
	baseline := ptyState(t, a, pid)
	checkScope := func(name string, requestScope procrequest.Scope, req proto.ProcessResumeReq, inbox string) {
		t.Helper()
		if _, err := rt.Handle(requestScope, req, inbox); err == nil {
			t.Fatalf("%s was accepted", name)
		}
		state := ptyState(t, a, pid)
		if starts.Load() != 1 || state.Input.Base != baseline.Input.Base || state.Input.End != baseline.Input.End ||
			state.ControlSeq != baseline.ControlSeq || state.LastKASeq != baseline.LastKASeq ||
			state.KADeadlineBootNS != baseline.KADeadlineBootNS || state.MasterClosed != baseline.MasterClosed ||
			!reflect.DeepEqual(state.Observation, baseline.Observation) {
			t.Fatalf("%s changed spawn/input/control/keepalive/kernel state", name)
		}
	}
	check := func(name string, req proto.ProcessResumeReq, inbox string) {
		t.Helper()
		checkScope(name, scope, req, inbox)
	}
	for _, field := range []string{"sid", "nid", "actor", "lineage"} {
		wrong := scope
		switch field {
		case "sid":
			wrong.SID = "other"
		case "nid":
			wrong.NID = "other"
		case "actor":
			wrong.ActorFP = "SHA256:" + strings.Repeat("B", 43)
		case "lineage":
			wrong.ClientLineage = "other"
		}
		checkScope("scope/"+field, wrong, ptyTaskRequest(scope, h, "resume"), "_TINBOX.pty")
	}
	mutators := []struct {
		name   string
		mutate func(*proto.ProcessResumeReq)
	}{
		{"lease-version", func(r *proto.ProcessResumeReq) { r.Handle.Lease.Version++ }},
		{"lease-id", func(r *proto.ProcessResumeReq) { r.Handle.Lease.LeaseID += "x" }},
		{"instance", func(r *proto.ProcessResumeReq) { r.Handle.Lease.TargetInstance += "x" }},
		{"boot", func(r *proto.ProcessResumeReq) { r.Handle.Lease.BootID += "x" }},
		{"sid", func(r *proto.ProcessResumeReq) { r.Handle.Lease.SID = "other" }},
		{"nid", func(r *proto.ProcessResumeReq) { r.Handle.Lease.NID = "other" }},
		{"actor", func(r *proto.ProcessResumeReq) { r.Handle.Lease.ActorFP = "SHA256:" + strings.Repeat("B", 43) }},
		{"lineage", func(r *proto.ProcessResumeReq) { r.Handle.Lease.ClientLineage += "x" }},
		{"issued", func(r *proto.ProcessResumeReq) { r.Handle.Lease.IssuedBootNS++ }},
		{"expires", func(r *proto.ProcessResumeReq) { r.Handle.Lease.ExpiresBootNS++ }},
		{"mac", func(r *proto.ProcessResumeReq) { r.Handle.Lease.MAC += "x" }},
		{"request-id", func(r *proto.ProcessResumeReq) { r.Handle.RequestID += "x" }},
		{"missing-handle", func(r *proto.ProcessResumeReq) { r.Handle = nil }},
		{"zero-epoch", func(r *proto.ProcessResumeReq) { r.AttachEpoch = 0 }},
		{"stamped-actor-mismatch", func(r *proto.ProcessResumeReq) { r.ActorFP = "SHA256:" + strings.Repeat("B", 43) }},
		{"exec-kind-on-pty", func(r *proto.ProcessResumeReq) { r.Kind = "" }},
		{"unknown-kind", func(r *proto.ProcessResumeReq) { r.Kind = "exec" }},
		{"unknown-op", func(r *proto.ProcessResumeReq) { r.Op = "unknown" }},
	}
	for _, m := range mutators {
		r := ptyTaskRequest(scope, h, "resume")
		m.mutate(&r)
		check(m.name, r, "_TINBOX.pty")
	}
	check("invalid-inbox", ptyTaskRequest(scope, h, "resume"), "public.subject")
	// Every operation's otherwise legal request gets each foreign payload.
	// This makes a missed validation visible through zero side effects as well
	// as refusal, rather than relying on an error produced after an ioctl.
	payloads := []struct {
		name    string
		set     func(*proto.ProcessResumeReq)
		allowed string
	}{
		{"run", func(r *proto.ProcessResumeReq) { r.Run = create.Run }, "create"},
		{"exec", func(r *proto.ProcessResumeReq) {
			r.Exec = &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/true"}}
		}, ""},
		{"input", func(r *proto.ProcessResumeReq) { r.Input = []byte("unwanted") }, "input"},
		{"input-seq", func(r *proto.ProcessResumeReq) { r.InputSeq = 1 }, "input"},
		{"input-fin", func(r *proto.ProcessResumeReq) { r.InputFIN = true }, "input"},
		{"resize", func(r *proto.ProcessResumeReq) { r.Resize = &proto.PtyResizeEvent{Cols: 99, Rows: 55} }, "resize"},
		{"signal", func(r *proto.ProcessResumeReq) { r.Signal = int(syscall.SIGTERM) }, "signal"},
		{"control-seq", func(r *proto.ProcessResumeReq) { r.ControlSeq = 1 }, "controls"},
	}
	for _, op := range []string{"create", "resume", "ack", "terminal-ack", "input", "resize", "signal", "keepalive", "hangup"} {
		for _, p := range payloads {
			control := op == "resize" || op == "signal" || op == "keepalive" || op == "hangup"
			if p.allowed == op || (p.allowed == "controls" && control) {
				continue
			}
			r := ptyTaskRequest(scope, h, op)
			if op == "create" {
				r.Run = create.Run
			}
			if control {
				r.ControlSeq = 1
			}
			if op == "resize" {
				r.Resize = &proto.PtyResizeEvent{Cols: 80, Rows: 24}
			}
			if op == "signal" {
				r.Signal = int(syscall.SIGINT)
			}
			p.set(&r)
			check(op+"/"+p.name, r, "_TINBOX.pty")
		}
	}
	// A valid resend remains the same owner and never spends another spawn.
	if _, err := rt.Handle(scope, create, "_TINBOX.pty"); err != nil || starts.Load() != 1 {
		t.Fatalf("duplicate create changed ownership: %v", err)
	}
	// A newer attach fences every byte/control request from the old ctl.
	// Zero alone does not exercise the stale, nonzero epoch guard.
	newAttach := ptyTaskRequest(scope, h, "resume")
	newAttach.AttachEpoch = 2
	if _, err := rt.Handle(scope, newAttach, "_TINBOX.pty.new"); err != nil {
		t.Fatalf("new attachment: %v", err)
	}
	staleInput := ptyTaskRequest(scope, h, "input")
	staleInput.Input = []byte("stale input")
	check("stale-input-epoch", staleInput, "_TINBOX.pty")
	for _, op := range []string{"resize", "signal", "keepalive", "hangup"} {
		stale := ptyTaskRequest(scope, h, op)
		stale.ControlSeq = 1
		if op == "resize" {
			stale.Resize = &proto.PtyResizeEvent{Cols: 99, Rows: 55}
		}
		if op == "signal" {
			stale.Signal = int(syscall.SIGTERM)
		}
		check("stale-control-epoch/"+op, stale, "_TINBOX.pty")
	}
	if err := checkPTYAdmission(a); errors.Is(err, procrequest.ErrBusy) {
		t.Fatal("unrelated marker closed ordinary PTY admission")
	}
}
