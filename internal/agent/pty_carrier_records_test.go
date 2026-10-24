//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

type ptyCarrierAudit struct {
	mu     sync.Mutex
	states []map[string]any
}

func (*ptyCarrierAudit) Enabled(context.Context, slog.Level) bool { return true }
func (s *ptyCarrierAudit) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "agent: pty image state" {
		return nil
	}
	fields := make(map[string]any)
	record.Attrs(func(a slog.Attr) bool { fields[a.Key] = a.Value.Any(); return true })
	s.mu.Lock()
	s.states = append(s.states, fields)
	s.mu.Unlock()
	return nil
}
func (s *ptyCarrierAudit) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *ptyCarrierAudit) WithGroup(string) slog.Handler      { return s }
func (s *ptyCarrierAudit) snapshot() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.states...)
}

func carrierExpectedState(phase, pid string, st ptysession.Image) map[string]any {
	return map[string]any{"phase": phase, "generation": uint64(0), "pid": pid,
		"input_applied": st.Input.Base, "input_accepted": st.Input.End, "input_fin": st.InputFIN, "input_ended": st.InputEnded,
		"output_base": st.Output.Base, "output_end": st.Output.End, "output_fin": st.Output.FIN,
		"control_seq": st.ControlSeq, "ka_seq": st.LastKASeq, "ka_deadline_boot_ns": st.KADeadlineBootNS,
		"revoked": st.Revoked != "", "master_closed": st.MasterClosed}
}

// origin: hot-update S4 plan §6.1 / §9.1; P18 carrier receipts. Real NATS
// sessions and a real PTY, with failed register attempts between loss/recovery.
func TestPTYCarrierReceiptsFollowEstablishedSessions(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exercisePTYCarrierReceipts(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "PTY established-session receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY established-session receipts", beforeFD, 0)
}

func exercisePTYCarrierReceipts(t *testing.T) {
	t.Helper()
	url := startNATS(t)
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stub.Close)
	var refuse atomic.Bool
	var attempts, heartbeats atomic.Int32
	_, err = stub.Subscribe(proto.SubjNodeRegister("lab", "node"), func(m *nats.Msg) {
		attempts.Add(1)
		if refuse.Load() {
			return
		} // real request timeout, not a fabricated success
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", func(*nats.Msg) { heartbeats.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	_, err = stub.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.proc.*", func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond([]byte(`{"ok":true}`))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stub.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	sink := &ptyCarrierAudit{}
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Logger: slog.New(sink),
		HeartbeatInterval: 25 * time.Millisecond, RegisterTimeout: 100 * time.Millisecond,
		RegisterRetryInitial: 5 * time.Millisecond, RegisterRetryMax: 10 * time.Millisecond, RosterRefreshInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				t.Errorf("agent shutdown: %v", err)
			}
		case <-time.After(bootTestBudget):
			t.Error("carrier fixture did not stop")
		}
	})
	waitFor(t, bootTestBudget, func() bool { return heartbeats.Load() > 0 })
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("no PTY tasks emitted state: %v", got)
	}

	scope := ptyTestScope(a)
	handle := ptyProbe(t, a, scope)
	create := ptyTaskRequest(scope, handle, "create")
	create.Run = &proto.RunReq{Cwd: t.TempDir(), KAIntervalMS: 1000, Argv: []string{"/bin/sh", "-c",
		"stty raw -echo; printf ready; while IFS= read -r line; do printf 'seen:%s\\n' \"$line\"; done # private-argv"},
		Env: map[string]string{"PRIVATE_ENV": "private-env"}}
	created, err := a.processRuntime.Handle(scope, ownProcessRun(scope, create), "_TINBOX.pty")
	if err != nil {
		t.Fatal(err)
	}
	pid := created.PID
	request := ptyTaskRequest(scope, handle, "resume")
	waitFor(t, bootTestBudget, func() bool {
		status, err := a.processRuntime.Handle(scope, request, "_TINBOX.pty")
		return err == nil && status.Phase == "running" && status.Stdout != nil && string(status.Stdout.Data) == "ready"
	})
	input := request
	input.Op, input.Input = "input", []byte("private-input\n")
	if _, err := a.processRuntime.Handle(scope, input, "_TINBOX.pty"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, bootTestBudget, func() bool {
		status, err := a.processRuntime.Handle(scope, request, "_TINBOX.pty")
		return err == nil && status.InputACK == uint64(len(input.Input)) && status.Stdout != nil && string(status.Stdout.Data) == "readyseen:private-input\n"
	})
	st := ptyState(t, a, pid)
	if st.Input.Base == 0 || st.KADeadlineBootNS == 0 {
		t.Fatal("carrier fixture lacks applied input or KA authority")
	}
	identity, ok := a.workloads.Lookup(pid)
	if !ok {
		t.Fatal("missing original child identity")
	}
	runtimeBefore, connBefore := a.processRuntime, a.ncBox.Load()

	refuse.Store(true)
	before := attempts.Load()
	a.fireRedial()
	waitFor(t, bootTestBudget, func() bool { return attempts.Load() >= before+3 })
	want := []map[string]any{carrierExpectedState("carrier-lost", pid, st)}
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("failed setup emitted a restoration or repeated loss: %v want %v", got, want)
	}
	hbBefore := heartbeats.Load()
	refuse.Store(false)
	waitFor(t, bootTestBudget, func() bool { return len(sink.snapshot()) >= 2 })
	want = append(want, carrierExpectedState("carrier-restored", pid, st))
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored state: %v want %v", got, want)
	}
	if a.processRuntime != runtimeBefore || a.ncBox.Load() == connBefore {
		t.Fatal("session did not rebuild independently of the original runtime")
	}
	next, ok := a.workloads.Lookup(pid)
	if !ok || next.Identity != identity.Identity {
		t.Fatal("carrier rebuild recreated the PTY child")
	}
	live := ptyState(t, a, pid)
	if live.KADeadlineBootNS != st.KADeadlineBootNS || live.LastKASeq != st.LastKASeq {
		t.Fatal("carrier rebuild renewed KA authority")
	}

	// A second completed session must re-arm the pair rather than suppress
	// every future loss. Wait for its handlers before asking it to rebuild.
	waitFor(t, bootTestBudget, func() bool { return heartbeats.Load() > hbBefore && !a.rebuilding.Load() })
	a.fireRedial()
	waitFor(t, bootTestBudget, func() bool { return len(sink.snapshot()) >= 4 })
	want = append(want, carrierExpectedState("carrier-lost", pid, st), carrierExpectedState("carrier-restored", pid, st))
	if got := sink.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("second session state/count: %v want %v", got, want)
	}
}
