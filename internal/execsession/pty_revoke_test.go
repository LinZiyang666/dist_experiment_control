//go:build linux

// Real runtime/registry/Manager and real PTYs where dependencies are present.
// Boot time and authority events are injected; broker/NATS are not exercised.
package execsession

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §6.1 / T21
func TestPTYRevokeClockFailureRetainsResponsibility(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	var broken atomic.Bool
	clock.Store(int64(time.Second))
	f, cfg := runtimeFixtureConfig(t, 64)
	cfg.Clock = func() (int64, error) {
		if broken.Load() {
			return 0, errors.New("injected boot clock failure")
		}
		return clock.Load(), nil
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { broken.Store(false); f.manager.Thaw(); f.close(t) })
	scope := f.scope
	lease, err := r.registry.Issue(scope)
	if err != nil {
		t.Fatal(err)
	}
	imageFreeze(t, r)
	broken.Store(true)
	if _, err := r.Revoke(scope.SID, "session_destroyed"); err == nil {
		t.Fatal("clock failure was not reported")
	}
	r.mu.Lock()
	mark, ok := r.revocations[scope.SID]
	r.mu.Unlock()
	if !ok || !mark.clockUnknown {
		t.Fatal("clock failure lost irreversible revocation responsibility")
	}
	// A failed clock is never replaced by a fabricated zero instant.
	img, _, err := r.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(img.Revocations) != 1 || img.Revocations[0].AtBootNS <= 0 || !img.Revocations[0].ClockUnknown || img.Revocations[0].Reason != "session_destroyed" {
		t.Fatal("unknown-clock responsibility missing from export")
	}
	if got := RequiredDomains(img); len(got) != 2 || got[1] != "pty-v1" {
		t.Fatalf("clock extension lacks domain: %v", got)
	}
	if _, err := r.Sweep(); err == nil {
		t.Fatal("failed clock allowed collection")
	}
	broken.Store(false)
	clock.Add(int64(2 * procrequest.MaxTTL))
	imported, salvage, err := Import(cfg, img, nil, newImageDupSource(t, nil))
	if err != nil || len(salvage) != 0 {
		t.Fatalf("import unknown revocation: %v", err)
	}
	if err := r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	r = imported
	if err := r.Thaw(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sweep(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	retained := r.revocations[scope.SID]
	r.mu.Unlock()
	if !retained.clockUnknown {
		t.Fatal("recovered clock collected or restamped an unknown revocation")
	}
	for _, kind := range []string{"", proto.ProcessKindPTY} {
		req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 3, Op: "create", Kind: kind, ActorFP: scope.ActorFP, AttachEpoch: 1, Handle: &proto.ProcessHandle{Lease: lease, RequestID: "revoked-old"}}
		if kind == "" {
			req.Exec = &proto.ExecReq{Argv: []string{"/bin/true"}}
		} else {
			req.Run = &proto.RunReq{Argv: []string{"/bin/true"}}
		}
		if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, ErrRevoked) {
			t.Fatalf("kind %q old lease accepted: %v", kind, err)
		}
	}
	// A later lease is not sufficient evidence to clear an unknown boundary.
	later, err := r.registry.Issue(scope)
	if err != nil {
		t.Fatal(err)
	}
	q := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 4, Op: "create", ActorFP: scope.ActorFP, AttachEpoch: 1, Handle: &proto.ProcessHandle{Lease: later, RequestID: "revoked-later"}, Exec: &proto.ExecReq{Argv: []string{"/bin/true"}}}
	if _, err := r.Handle(scope, q, "failure-inbox"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("new lease bypassed unknown authority boundary: %v", err)
	}
	broken.Store(true)
	other := clockedScope("clock-other")
	q.ActorFP = other.ActorFP
	q.Handle.Lease, err = func() (proto.ProcessLease, error) {
		broken.Store(false)
		defer broken.Store(true)
		return r.registry.Issue(other)
	}()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Handle(other, q, "failure-inbox"); err == nil || !strings.Contains(err.Error(), "clock") {
		t.Fatalf("new create ignored clock failure: %v", err)
	}
	broken.Store(false)
	t.Run("concurrent-clock-completions", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		fixture := newRuntimeFixture(t, 64)
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		fixture.r.now = func() (int64, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
				return int64(time.Second), nil
			}
			return 0, errors.New("newer revoke lost its clock")
		}
		done := make(chan error, 1)
		go func() { _, err := fixture.r.Revoke("concurrent", "first"); done <- err }()
		failureAwait(t, entered, "older revocation clock")
		if _, err := fixture.r.Revoke("concurrent", "second"); err == nil {
			t.Fatal("newer clock failure disappeared")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		fixture.r.mu.Lock()
		mark := fixture.r.revocations["concurrent"]
		fixture.r.mu.Unlock()
		if !mark.clockUnknown || mark.reason != "second" {
			t.Fatal("older clock completion cleared a newer unknown authority boundary")
		}
	})
	for _, phase := range []string{"starting", "active", "frozen"} {
		t.Run("PTY-"+phase, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exercisePTYClockFailure(t, phase)
		})
	}
}

// origin: hot-update S4 plan §4.2 / §6.2 / T20
func TestPTYRevokeBeforeQueuedInputActivation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exercisePTYRevoke(t, "frozen") // warm the real poller; an absent dependency skips the whole receipt
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, phase := range []string{"starting", "active", "frozen"} {
			exercisePTYRevoke(t, phase)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY revocation", beforeG)
	testharness.AssertNoFDLeak(t, "PTY revocation", beforeFD, 0)
}

func exercisePTYRevoke(t *testing.T, phase string) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	if phase == "starting" {
		f.beforeStart = func() { close(entered); <-release }
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
	}
	marker := t.TempDir() + "/received"
	script := "stty raw -echo; printf ready; while IFS= read -r line; do printf '%s' \"$line\" >> " + marker + "; done"
	q, owner := ptyCreate(t, f, "revoke-"+phase, []string{"/bin/sh", "-c", script})
	if phase == "starting" {
		failureAwait(t, entered, "PTY start boundary")
		if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
			t.Fatal(err)
		}
		close(release)
	} else {
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "ready")
		})
		if phase == "frozen" {
			imageFreeze(t, f.r)
			// Put a real retained suffix into the paused input replay, then import
			// that image. The next generation must revoke before it can write it.
			// Export while the actor still owns its reservations. Detach releases
			// that budget and cannot be followed by another old-owner export.
			if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
				t.Fatal(err)
			}
			img, _, err := f.r.Export()
			if err != nil {
				t.Fatal(err)
			}
			st := *img.Tasks[0].PTY
			files, err := owner.pty.Detach()
			requirePTYDependency(t, err)
			input, err := workload.RestoreReplay(&f.r.budget, st.Input, files[filePTYInput])
			if err != nil {
				t.Fatal(err)
			}
			input.Thaw()
			if _, err := input.Append([]byte("forbidden\n")); err != nil {
				t.Fatal(err)
			}
			st.Input, files[filePTYInput], err = input.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			detached, _, err := input.Detach()
			if err != nil {
				t.Fatal(err)
			}
			files[filePTYInput] = detached
			// Rebuild through the public runtime image importer, with the updated
			// Q and unchanged A, rather than mutating the actor's private queue.
			img.Tasks[0].PTY = &st
			for name, file := range files {
				img.Tasks[0].Files[name], err = fileIdentity(file)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.manager.Freeze(); err != nil {
				t.Fatal(err)
			}
			process, err := f.manager.ExportProcesses()
			if err != nil {
				t.Fatal(err)
			}
			src := &imageDupSource{files: map[string]map[string]*os.File{ptyResourcePrefix + owner.pid: files}}
			imported, salvage, err := Import(f.cfg, img, process.Records, src)
			requirePTYDependency(t, err)
			if len(salvage) != 0 {
				t.Fatal("revoked import returned salvage")
			}
			retained := imported.tasks[owner.pid]
			if retained.revoked != "session_destroyed" || retained.frozen.pty.Revoked != "session_destroyed" {
				t.Fatal("import omitted irreversible task revocation")
			}
			// The old actor was detached above, so retire its now-empty runtime
			// without closing the transferred descriptors or touching the child.
			f.r.mu.Lock()
			delete(f.r.tasks, owner.pid)
			f.r.closed = true
			f.r.cond.Broadcast()
			f.r.mu.Unlock()
			f.r = imported
			f.manager.Thaw()
			if err := f.r.Thaw(); err != nil {
				t.Fatal(err)
			}
		} else {
			input := q
			input.Op = "input"
			input.Input = []byte("before\n")
			if _, err := f.r.Handle(f.scope, input, "pty-inbox"); err != nil {
				t.Fatal(err)
			}
			ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.InputACK == uint64(len(input.Input)) })
			if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Terminal status remains collectable; mutation operations are refused even
	// after the real terminal arrives, and cannot reopen a master or renew KA.
	terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	if terminal.Error == "" {
		t.Fatal("revoked terminal lost its authority diagnostic")
	}
	if phase == "frozen" && (terminal.InputACK != 0 || terminal.InputAccepted != uint64(len("forbidden\n"))) {
		t.Fatal("revoked activation wrote queued bytes or lost the retained suffix")
	}
	before, _ := os.ReadFile(marker)
	late := q
	late.Op = "input"
	late.Input = []byte("forbidden\n")
	if _, err := f.r.Handle(f.scope, late, "pty-inbox"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revoked input admitted: %v", err)
	}
	after, _ := os.ReadFile(marker)
	if !bytes.Equal(before, after) || bytes.Contains(after, []byte("forbidden")) {
		t.Fatal("queued or late input reached the revoked slave")
	}
	if phase != "active" && len(after) != 0 {
		t.Fatalf("revoked generation applied new input: %q", after)
	}
	f.close(t)
}

func exercisePTYClockFailure(t *testing.T, phase string) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	entered, release := make(chan struct{}), make(chan struct{})
	if phase == "starting" {
		f.beforeStart = func() { close(entered); <-release }
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
	}
	q, _ := ptyCreate(t, f, "clock-"+phase, []string{"/bin/sh", "-c", "stty raw -echo; printf ready; read line"})
	if phase == "starting" {
		failureAwait(t, entered, "PTY clock start boundary")
	} else {
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "ready")
		})
	}
	if phase == "frozen" {
		imageFreeze(t, f.r)
	}
	f.clockFault.Store(true)
	killed, err := f.r.Revoke(f.scope.SID, "session_destroyed")
	if err == nil || killed != 1 {
		t.Fatalf("clock failure revoked %d tasks, error=%v", killed, err)
	}
	if _, err := f.r.Sweep(); err == nil {
		t.Fatal("clock failure allowed GC")
	}
	if phase != "frozen" {
		ka := q
		ka.Op = "keepalive"
		ka.ControlSeq = 1
		if _, err := f.r.Handle(f.scope, ka, "pty-inbox"); err == nil {
			t.Fatal("clock failure allowed keepalive")
		}
	}
	f.clockFault.Store(false)
	if phase == "starting" {
		close(release)
	}
	if phase == "frozen" {
		img, _, err := f.r.Export()
		if err != nil {
			t.Fatal(err)
		}
		if len(img.Revocations) != 1 || !img.Revocations[0].ClockUnknown {
			t.Fatal("frozen PTY lost unknown-clock marker")
		}
		if err := f.r.Thaw(); err != nil {
			t.Fatal(err)
		}
	}
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	ptyRoundTrip(t, f)
	f.clock.Add(int64(2 * procrequest.MaxTTL))
	if _, err := f.r.Sweep(); err != nil {
		t.Fatal(err)
	}
	f.r.mu.Lock()
	mark := f.r.revocations[f.scope.SID]
	f.r.mu.Unlock()
	if !mark.clockUnknown {
		t.Fatal("next generation erased unknown-clock responsibility")
	}
}
