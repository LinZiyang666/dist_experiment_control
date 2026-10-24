//go:build linux

package execsession

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/workload"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type ptyAuditSink struct {
	mu      sync.Mutex
	records []slog.Record
}

func (*ptyAuditSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *ptyAuditSink) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r.Clone())
	return nil
}
func (s *ptyAuditSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *ptyAuditSink) WithGroup(string) slog.Handler      { return s }
func (s *ptyAuditSink) matching(message string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, r := range s.records {
		if r.Message != message {
			continue
		}
		fields := map[string]any{}
		r.Attrs(func(a slog.Attr) bool { fields[a.Key] = a.Value.Any(); return true })
		out = append(out, fields)
	}
	return out
}

func auditedPTY(t *testing.T) (*ptyRuntimeFixture, *ptyAuditSink, proto.ProcessResumeReq, *task) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	sink := &ptyAuditSink{}
	f.cfg.Logger = slog.New(sink)
	f.r.cfg.Logger = f.cfg.Logger
	q := ptyRequest(t, f, "private-request-identity", []string{"/bin/sh", "-c", "stty raw -echo; interrupted=0; trap 'interrupted=1; printf signal-ready' INT; printf ready; while :; do interrupted=0; if IFS= read -r line; then printf 'seen:%s\\n' \"$line\"; elif [ \"$interrupted\" = 0 ]; then break; fi; done # private-argv-value"})
	q.Run.KAIntervalMS = 1000
	q.Run.Env = map[string]string{"PRIVATE_ENV": "private-env-value"}
	result, err := f.r.Handle(f.scope, q, "pty-inbox")
	requirePTYDependency(t, err)
	f.r.mu.Lock()
	owner := f.r.tasks[result.PID]
	f.r.mu.Unlock()
	q.Op, q.Run = "resume", nil
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
	return f, sink, q, owner
}

func auditInput(t *testing.T, f *ptyRuntimeFixture, q proto.ProcessResumeReq, seq uint64, data string) proto.ProcessResumeReply {
	t.Helper()
	input := q
	input.Op, input.InputSeq, input.Input = "input", seq, []byte(data)
	_, err := f.r.Handle(f.scope, input, "pty-inbox")
	requirePTYDependency(t, err)
	return ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
		return p.InputACK == seq+uint64(len(data)) && p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "seen:"+strings.TrimSuffix(data, "\n"))
	})
}

func auditControl(t *testing.T, f *ptyRuntimeFixture, q proto.ProcessResumeReq, seq uint64, op string) proto.ProcessResumeReq {
	t.Helper()
	q.Op, q.ControlSeq = op, seq
	if op == "resize" {
		q.Resize = &proto.PtyResizeEvent{Cols: 91, Rows: 37}
	}
	if op == "signal" {
		q.Signal = int(syscall.SIGINT)
	}
	_, err := f.r.Handle(f.scope, q, "pty-inbox")
	requirePTYDependency(t, err)
	return q
}

func imageReceiptFields(phase string, generation uint64, pid string, s ptysession.Image) map[string]any {
	return map[string]any{"phase": phase, "generation": generation, "pid": pid,
		"input_applied": s.Input.Base, "input_accepted": s.Input.End, "input_fin": s.InputFIN, "input_ended": s.InputEnded,
		"output_base": s.Output.Base, "output_end": s.Output.End, "output_fin": s.Output.FIN,
		"control_seq": s.ControlSeq, "ka_seq": s.LastKASeq, "ka_deadline_boot_ns": s.KADeadlineBootNS,
		"revoked": s.Revoked != "", "master_closed": s.MasterClosed}
}

// origin: hot-update S4 plan §3 / §4.2 / §6.1 / §9.1; P18.
func TestPTYImageStateReceiptsMatchExportAndImportedOwners(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		f, sink, q, owner := auditedPTY(t)
		status := auditInput(t, f, q, 0, "first-private-payload\n")
		q.StdoutACK = status.Stdout.Offset + uint64(len(status.Stdout.Data))
		auditControl(t, f, q, 1, "resize")
		auditControl(t, f, q, 2, "keepalive")
		old, _, _ := ptyCut(t, f)
		LogPTYImage(f.cfg.Logger, "cut", 7, old)
		got := sink.matching("agent: pty image state")
		want := imageReceiptFields("cut", 7, owner.pid, *old.Tasks[0].PTY)
		if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
			t.Fatalf("cut receipt: %v want %v", got, want)
		}
		f.manager.Thaw()
		requirePTYDependency(t, f.r.Thaw())
		status = auditInput(t, f, q, status.InputACK, "second-private-payload\n")
		q.StdoutACK = status.Stdout.Offset + uint64(len(status.Stdout.Data))
		auditControl(t, f, q, 3, "resize")
		f.clock.Add(1_000_000_000)
		auditControl(t, f, q, 4, "keepalive")
		current, _, _ := ptyCut(t, f)
		st := *current.Tasks[0].PTY
		if st.Input.Base <= old.Tasks[0].PTY.Input.Base || st.Output.Base <= old.Tasks[0].PTY.Output.Base || st.KADeadlineBootNS <= old.Tasks[0].PTY.KADeadlineBootNS {
			t.Fatal("current-state fixture did not advance")
		}
		f.manager.Thaw()
		requirePTYDependency(t, f.r.Thaw())
		ptyRoundTrip(t, f)
		// The imported owner may advance as soon as activation starts its pumps.
		// Exercise that boundary so a cached imported bundle cannot pass as live state.
		auditInput(t, f, q, st.Input.Base, "third-private-payload\n")
		f.r.LogPTYActivation(8)
		imageFreeze(t, f.r)
		activated, _, err := f.r.Export()
		requirePTYDependency(t, err)
		live := *activated.Tasks[0].PTY
		if live.Input.Base <= st.Input.Base || live.KADeadlineBootNS != st.KADeadlineBootNS {
			t.Fatal("imported owner did not advance without renewing its KA deadline")
		}
		got = sink.matching("agent: pty image state")
		want = imageReceiptFields("activated", 8, owner.pid, live)
		if len(got) != 2 || !reflect.DeepEqual(got[1], want) {
			t.Fatalf("activation receipt: %v want %v", got, want)
		}
		f.close(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY image receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY image receipts", beforeFD, 0)
}

// The real master ioctl is held at its boundary, so an audit before the commit
// can be distinguished from a completed effect without timing guesses.
type ptyAuditResize struct {
	pty.Master
	entered, release chan struct{}
	fail             bool
}

func (m *ptyAuditResize) Resize(cols, rows uint16) error {
	close(m.entered)
	<-m.release
	if err := m.Master.Resize(cols, rows); err != nil {
		return err
	}
	if m.fail {
		return syscall.EIO
	}
	return nil
}

func holdAuditResize(t *testing.T, f *ptyRuntimeFixture, owner *task, fail bool) *ptyAuditResize {
	t.Helper()
	imageFreeze(t, f.r)
	st := *owner.frozen.pty
	files, err := owner.pty.Detach()
	requirePTYDependency(t, err)
	row, _ := f.manager.Lookup(owner.pid)
	master, err := pty.RestoreMaster(files[filePTYMaster], st.Identity, row)
	requirePTYDependency(t, err)
	barrier := &ptyAuditResize{Master: master, entered: make(chan struct{}), release: make(chan struct{}), fail: fail}
	t.Cleanup(func() {
		select {
		case <-barrier.release:
		default:
			close(barrier.release)
		}
	})
	input, err := workload.RestoreReplay(&f.r.budget, st.Input, files[filePTYInput])
	requirePTYDependency(t, err)
	output, err := workload.RestoreReplay(&f.r.budget, st.Output, files[filePTYOutput])
	requirePTYDependency(t, err)
	actor, err := ptysession.Import(ptysession.Config{Manager: &f.manager, PID: owner.pid, Master: barrier,
		Identity: st.Identity, Input: input, Output: output, Clock: f.cfg.Clock}, st)
	requirePTYDependency(t, err)
	f.r.mu.Lock()
	owner.pty, owner.logs[0], owner.logs[1] = actor, input, output
	f.r.mu.Unlock()
	requirePTYDependency(t, f.r.Thaw())
	return barrier
}

// origin: hot-update S4 plan §3.4 / §9.1; P18.
func TestPTYControlCommitReceiptDeduplicatesRetries(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		f, sink, q, owner := auditedPTY(t)
		barrier := holdAuditResize(t, f, owner, round%2 != 0)
		control := q
		control.Op, control.ControlSeq = "resize", 1
		control.Resize = &proto.PtyResizeEvent{Cols: 91, Rows: 37}
		completed := make(chan error, 1)
		go func() { _, err := f.r.Handle(f.scope, control, "pty-inbox"); completed <- err }()
		failureAwait(t, barrier.entered, "blocked resize commit")
		if got := sink.matching("agent: pty control committed"); len(got) != 0 {
			close(barrier.release)
			<-completed
			t.Fatalf("uncommitted resize was audited: %v", got)
		}
		close(barrier.release)
		requirePTYDependency(t, <-completed)
		ptyRoundTrip(t, f) // import must retain the already audited ControlSeq
		_, err := f.r.Handle(f.scope, control, "pty-inbox")
		requirePTYDependency(t, err)
		conflict := control
		conflict.Resize = &proto.PtyResizeEvent{Cols: 92, Rows: 37}
		if _, err := f.r.Handle(f.scope, conflict, "pty-inbox"); !errors.Is(err, ptysession.ErrConflict) {
			t.Fatalf("conflicting retry: %v", err)
		}
		auditControl(t, f, q, 2, "keepalive")
		signal := auditControl(t, f, q, 3, "signal")
		_, err = f.r.Handle(f.scope, signal, "pty-inbox")
		requirePTYDependency(t, err)
		// A trapped SIGINT interrupts read. The shell must retry that read and
		// acknowledge handling the signal before Hangup, otherwise natural
		// child exit can close the master before the control transaction.
		// origin: hot-update S4 plan §3.4 / §9.1; GATES-FINAL-2.
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Phase == "running" && p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "signal-ready")
		})
		auditInput(t, f, q, 0, "after-signal\n")
		hangup := auditControl(t, f, q, 4, "hangup")
		_, err = f.r.Handle(f.scope, hangup, "pty-inbox")
		requirePTYDependency(t, err)
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
		got := sink.matching("agent: pty control committed")
		want := []map[string]any{
			{"pid": owner.pid, "seq": uint64(1), "op": "resize", "outcome": "ok"},
			{"pid": owner.pid, "seq": uint64(3), "op": "signal", "outcome": "ok"},
			{"pid": owner.pid, "seq": uint64(4), "op": "hangup", "outcome": "ok"},
		}
		if barrier.fail {
			want[0]["outcome"] = "ioctl_error"
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("control receipts: %v want %v", got, want)
		}
		f.close(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY control receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY control receipts", beforeFD, 0)
}

// origin: hot-update S4 plan §3 / §9.1; P18.
func TestPTYReliableLifecycleReceiptsExcludeUserData(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		f, sink, q, owner := auditedPTY(t)
		status := auditInput(t, f, q, 0, "private-input-payload\n")
		fin := q
		fin.Op, fin.InputSeq, fin.InputFIN = "input", status.InputACK, true
		_, err := f.r.Handle(f.scope, fin, "pty-inbox")
		requirePTYDependency(t, err)
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.InputFIN && p.InputEnded })
		auditControl(t, f, q, 1, "hangup")
		terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
		imageFreeze(t, f.r)
		img, _, err := f.r.Export()
		requirePTYDependency(t, err)
		st := *img.Tasks[0].PTY
		starts := sink.matching("agent: run reliable=true started")
		if len(starts) != 1 {
			t.Fatalf("start receipts: %v", starts)
		}
		row := img.Tasks[0].PTY.Identity
		wantStart := map[string]any{"pid": owner.pid, "request_id_hash": fmt.Sprintf("%x", sha256.Sum256([]byte(q.Handle.RequestID))), "os_pid": int64(row.LeaderPID), "start_time_ticks": row.StartTimeTicks}
		if !reflect.DeepEqual(starts[0], wantStart) {
			t.Fatalf("start: %v want %v", starts, wantStart)
		}
		exits := sink.matching("agent: run reliable=true exited")
		wantExit := map[string]any{"pid": owner.pid, "code": int64(terminal.ExitCode), "signal": int64(terminal.Signal),
			"input_applied": st.Input.Base, "input_accepted": st.Input.End, "input_fin": st.InputFIN, "input_ended": st.InputEnded,
			"output_base": st.Output.Base, "output_end": st.Output.End, "output_fin": st.Output.FIN}
		if st.Input.Base != status.InputACK || len(exits) != 1 || !reflect.DeepEqual(exits[0], wantExit) {
			t.Fatalf("exit: %v want %v", exits, wantExit)
		}
		for _, secret := range []string{"private-request-identity", "private-argv-value", "private-env-value", "PRIVATE_ENV", "private-input-payload", "argv", "nonce"} {
			if strings.Contains(fmt.Sprint(starts, exits, sink.matching("agent: pty control committed")), secret) {
				t.Fatalf("receipt disclosed %q", secret)
			}
		}
		f.close(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY lifecycle receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY lifecycle receipts", beforeFD, 0)
}

// origin: hot-update S4 plan §6.1 / §9.1; P18 carrier receipts.
func TestPTYCarrierReceiptsKeepCurrentStateAndSuppressAttempts(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var unavailable *Runtime
	unavailable.LogPTYCarrier("carrier-lost", 0)
	unavailable.LogPTYCarrier("carrier-restored", 0)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		f, sink, q, owner := auditedPTY(t)
		auditInput(t, f, q, 0, "private-carrier-input\n")
		auditControl(t, f, q, 1, "keepalive")
		img, _, _ := ptyCut(t, f)
		st := *img.Tasks[0].PTY
		f.manager.Thaw()
		requirePTYDependency(t, f.r.Thaw())
		f.r.LogPTYCarrier("carrier-restored", 9) // cold establishment has no preceding loss
		if got := sink.matching("agent: pty image state"); len(got) != 0 {
			t.Fatalf("cold establishment: %v", got)
		}
		f.r.LogPTYCarrier("carrier-lost", 9)
		f.clock.Add(int64(time.Second))
		for attempt := 0; attempt < 3; attempt++ {
			f.r.LogPTYCarrier("carrier-lost", 9)
		}
		f.r.LogPTYCarrier("carrier-restored", 9)
		for attempt := 0; attempt < 3; attempt++ {
			f.r.LogPTYCarrier("carrier-restored", 9)
		}
		want := []map[string]any{imageReceiptFields("carrier-lost", 9, owner.pid, st), imageReceiptFields("carrier-restored", 9, owner.pid, st)}
		if got := sink.matching("agent: pty image state"); !reflect.DeepEqual(got, want) {
			t.Fatalf("carrier receipt state/count: %v want %v", got, want)
		}
		if f.starts.Load() != 1 {
			t.Fatal("carrier observation recreated the PTY")
		}
		f.close(t)
		empty := newPTYRuntime(t, 0)
		empty.r.cfg.Logger = f.cfg.Logger
		empty.r.LogPTYCarrier("carrier-lost", 9)
		empty.r.LogPTYCarrier("carrier-restored", 9)
		if got := sink.matching("agent: pty image state"); !reflect.DeepEqual(got, want) {
			t.Fatalf("empty runtime emitted a carrier record: %v", got)
		}
		empty.close(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY carrier receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY carrier receipts", beforeFD, 0)
}

type ptySettlementAudit struct {
	*ptyAuditSink
	observe func()
}

func (s *ptySettlementAudit) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "agent: run reliable=true settled" {
		s.observe()
	}
	return s.ptyAuditSink.Handle(ctx, r)
}

// origin: hot-update S4 plan §3 / §9.1; P18 settlement receipts.
func TestPTYSettlementReceiptFollowsAckCommitExactlyOnce(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, discarded := range []bool{false, true} {
			f, sink, q, owner := auditedPTY(t)
			status := auditInput(t, f, q, 0, "private-settlement-input\n")
			logger := slog.New(&ptySettlementAudit{ptyAuditSink: sink, observe: func() {
				rec, err := f.r.registry.Lookup(f.scope, *q.Handle)
				if err != nil || rec.Phase != "settled" {
					t.Errorf("settlement emitted before ack commit: phase=%s err=%v", rec.Phase, err)
				}
			}})
			f.r.mu.Lock()
			f.r.cfg.Logger = logger
			f.r.mu.Unlock()
			auditControl(t, f, q, 1, "hangup")
			terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
			failureAwait(t, owner.done, "terminal observer completion")
			out := owner.logs[1].Snapshot()
			if out.Base == out.End {
				t.Fatal("settlement fixture must retain unacknowledged output")
			}
			q.Op, q.TerminalID = "terminal-ack", terminal.TerminalID
			q.StdoutFIN, q.StderrFIN = out.FIN, true
			if discarded {
				f.clock.Add(int64(AbandonedTerminalGrace + time.Minute))
				res, err := f.r.Sweep()
				if err != nil || res.Discarded != 1 {
					t.Fatalf("terminal sweep: %+v %v", res, err)
				}
				if got := sink.matching("agent: run reliable=true settled"); len(got) != 0 {
					t.Fatalf("GC fabricated an ACK: %v", got)
				}
			}
			// Incomplete output responsibility must never produce a settlement receipt.
			if _, err := f.r.Handle(f.scope, q, "pty-inbox"); err == nil {
				t.Fatal("incomplete terminal ACK succeeded")
			}
			if got := sink.matching("agent: run reliable=true settled"); len(got) != 0 {
				t.Fatalf("rejected ACK was audited: %v", got)
			}
			q.StdoutACK = out.End
			settled, err := f.r.Handle(f.scope, q, "pty-inbox")
			requirePTYDependency(t, err)
			if settled.Phase != "settled" {
				t.Fatalf("ACK phase: %s", settled.Phase)
			}
			_, err = f.r.Handle(f.scope, q, "pty-inbox")
			requirePTYDependency(t, err)
			want := []map[string]any{{"pid": owner.pid, "code": int64(terminal.ExitCode), "signal": int64(terminal.Signal),
				"input_applied": status.InputACK, "input_accepted": status.InputAccepted, "output_base": out.End, "output_end": out.End}}
			got := sink.matching("agent: run reliable=true settled")
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("settlement state/count: %v want %v", got, want)
			}
			f.r.mu.Lock()
			_, retained := f.r.tasks[owner.pid]
			_, tombstone := f.r.discarded[owner.pid]
			f.r.mu.Unlock()
			if retained || tombstone {
				t.Fatal("successful settlement retained its replay owner")
			}
			f.close(t)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY settlement receipts", beforeG)
	testharness.AssertNoFDLeak(t, "PTY settlement receipts", beforeFD, 0)
}
