//go:build linux

package execsession

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 arm A
// A late control belongs to the retained task even after its Manager row is
// retired. Neither a local nor an imported terminal may turn into unknown.
func TestPTYRetainedTerminalAnswersLateRequests(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, imported := range []bool{false, true} {
			t.Run(fmt.Sprintf("round_%d/imported_%v", round, imported), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				f := newPTYRuntime(t, 0)
				sink := &ptyAuditSink{}
				f.cfg.Logger = slog.New(sink)
				f.r.cfg.Logger = f.cfg.Logger
				gate := filepath.Join(t.TempDir(), "exit")
				q, owner := ptyCreate(t, f, "retained", []string{"/bin/sh", "-c", "printf ready; while [ ! -f \"$1\" ]; do sleep .01; done; printf tail; exit 23", "fixture", gate})
				ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
				if imported {
					ptyRoundTrip(t, f)
				}
				if err := os.WriteFile(gate, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" && p.Stdout != nil && p.Stdout.FIN })
				if _, ok := f.manager.Lookup(owner.pid); ok {
					t.Fatal("fixture did not retire the Manager row")
				}
				if terminal.ExitCode != 23 || terminal.TerminalID == "" || string(terminal.Stdout.Data) != "readytail" {
					t.Fatalf("wrong retained result: %+v", terminal)
				}
				for _, op := range []string{"keepalive", "resize", "signal", "hangup", "input", "resume", "ack"} {
					late := q
					late.Op, late.StdoutACK, late.StdoutFIN, late.StderrFIN = op, 9, true, true
					switch op {
					case "keepalive", "hangup":
						late.ControlSeq = 1
					case "resize":
						late.ControlSeq, late.Resize = 1, &proto.PtyResizeEvent{Cols: 99, Rows: 31}
					case "signal":
						late.ControlSeq, late.Signal = 1, 2
					case "input":
						late.Input = []byte("undeliverable")
					}
					got, err := f.r.Handle(f.scope, late, "pty-inbox")
					if err != nil || got.TerminalID != terminal.TerminalID || got.ExitCode != 23 || got.Phase != "terminal" || got.ControlACK != 0 || got.InputAccepted != 0 {
						t.Fatalf("late %s lost terminal or executed an effect: result=%+v err=%v", op, got, err)
					}
				}
				unknown := q
				handle := *q.Handle
				handle.RequestID = "never-created"
				unknown.Handle = &handle
				if _, err := f.r.Handle(f.scope, unknown, "pty-inbox"); !errors.Is(err, procrequest.ErrUnknown) {
					t.Fatalf("unknown handle did not remain unknown: %v", err)
				}
				q.Op, q.TerminalID, q.StdoutACK, q.StdoutFIN, q.StderrFIN = "terminal-ack", terminal.TerminalID, 9, true, true
				for retry := 0; retry < 2; retry++ {
					got, err := f.r.Handle(f.scope, q, "pty-inbox")
					if err != nil || got.Phase != "settled" || got.TerminalID != terminal.TerminalID || got.ExitCode != 23 {
						t.Fatalf("terminal ACK: result=%+v err=%v", got, err)
					}
				}
				if len(sink.matching("agent: run reliable=true settled")) != 1 || f.r.ReplayBudgetUsed() != 0 {
					t.Fatal("settlement lost its receipt or retained resources")
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "retained PTY terminal requests", g0)
	testharness.AssertNoFDLeak(t, "retained PTY terminal requests", fd0, 0)
}

// origin: hot-update S4 plan §6.2 / T34; PRODUCT-R4 FX-A
// The former import-failure flake depended on a census racing Complete/Forget.
// Pin that boundary explicitly: source EOF and real Wait may happen while a
// request owns opMu, but transfer of the terminal and revocation must queue.
func TestPTYTerminalCompletionSerializesRevocation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round_%d/awaiting-observer", round), exercisePTYWaitedExitBeforeObserver)
		t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newPTYRuntime(t, 0)
			gate := filepath.Join(t.TempDir(), "exit")
			q, original := ptyCreate(t, f, "completion-revocation", []string{"/bin/sh", "-c", "printf ready; while [ ! -f \"$1\" ]; do sleep .01; done; exit 23", "fixture", gate})
			ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
			ptyRoundTrip(t, f)
			f.r.mu.Lock()
			owner := f.r.tasks[original.pid]
			f.r.mu.Unlock()
			owner.opMu.Lock()
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(owner.opMu.Unlock) }
			defer unlock()
			if err := os.WriteFile(gate, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				row, ok := f.manager.Lookup(owner.pid)
				if !ok || row.Exit != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("real Wait did not observe the child's exit")
				}
				time.Sleep(time.Millisecond)
			}
			// The observer must not bypass the protocol token, even if the child
			// is already reaped. Runtime ownership remains until transfer occurs.
			if _, ok := f.manager.Lookup(owner.pid); !ok || f.exits.Load() != 0 {
				t.Fatal("completion retired Manager underneath a request")
			}
			// Queue a status behind the observer while its initial registry view
			// is still running. When completion wins the token, that status must
			// refresh its result rather than returning the old phase/terminal.
			type statusResult struct {
				reply proto.ProcessResumeReply
				err   error
			}
			status := make(chan statusResult, 1)
			go func() { reply, err := f.r.Handle(f.scope, q, "pty-inbox"); status <- statusResult{reply, err} }()
			for {
				f.r.mu.Lock()
				pinned := f.r.ioActive >= 2
				f.r.mu.Unlock()
				if pinned {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("status did not queue behind real completion")
				}
				time.Sleep(time.Millisecond)
			}
			revoked := make(chan error, 1)
			go func() { _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); revoked <- err }()
			for {
				f.r.mu.Lock()
				marked := owner.revoked != ""
				f.r.mu.Unlock()
				if marked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("revocation was not recorded before waiting for completion")
				}
				time.Sleep(time.Millisecond)
			}
			// sleep-fixture: hold the request token beyond the revocation marker;
			// a completed Revoke here would have bypassed the live transaction.
			select {
			case err := <-revoked:
				t.Fatalf("revocation bypassed the in-flight request: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			unlock()
			select {
			case got := <-status:
				if got.err != nil || got.reply.Phase != "terminal" || got.reply.TerminalID == "" || got.reply.ExitCode != 23 {
					t.Fatalf("queued status retained its pre-completion view: %+v err=%v", got.reply, got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("queued status did not complete")
			}
			select {
			case err := <-revoked:
				if err != nil {
					t.Fatalf("completion/revocation race: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("revocation did not complete")
			}
			terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
			if terminal.ExitCode != 23 || terminal.TerminalID == "" || f.exits.Load() != 1 {
				t.Fatalf("terminal transfer changed the result: %+v", terminal)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "PTY completion and revocation", g0)
	testharness.AssertNoFDLeak(t, "PTY completion and revocation", fd0, 0)
}

// A partial activation supplies a deterministic version of the FX-A window:
// the first real actor is active and Wait can complete, while its imported
// observer has not started. No fake terminal or Manager row is injected.
func exercisePTYWaitedExitBeforeObserver(t *testing.T) {
	f := newPTYRuntime(t, 0)
	gate := filepath.Join(t.TempDir(), "exit")
	q, owner := ptyCreate(t, f, "waited-terminal", []string{"/bin/sh", "-c", "printf ready; while [ ! -f \"$1\" ]; do sleep .01; done; exit 23", "fixture", gate})
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
	other, _ := ptyCreate(t, f, "activation-failure", []string{"/bin/sh", "-c", "printf ready; sleep 30"})
	ptyStatus(t, f, other, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
	img, resources, rows := ptyCut(t, f)
	imported, _, err := Import(f.cfg, img, rows, newImageDupSource(t, resources))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	f.manager.Thaw()
	calls := 0
	f.r.activatePTY = func(actor *ptysession.Actor) error {
		calls++
		if calls == 2 {
			return errors.New("injected later activation failure")
		}
		return actor.Activate()
	}
	if err := f.r.Thaw(); err == nil || calls != 2 {
		t.Fatalf("partial activation was not established: calls=%d err=%v", calls, err)
	}
	f.r.activatePTY = nil
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		row, ok := f.manager.Lookup(owner.pid)
		if ok && row.Exit != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("active first owner did not finish real Wait")
		}
		time.Sleep(time.Millisecond)
	}
	if f.exits.Load() != 0 {
		t.Fatal("imported observer ran before activation committed")
	}
	if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
		t.Fatalf("reaped terminal was treated as a live cleanup identity: %v", err)
	}
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	if terminal.ExitCode != 23 || terminal.TerminalID == "" {
		t.Fatalf("waited result was lost: %+v", terminal)
	}
}
