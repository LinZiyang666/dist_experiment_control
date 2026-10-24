//go:build linux

// Real: concurrent admission, HMAC leases, one OS child and boot-clock deadlines.
// Stubbed: current broker authorization, stream delivery and runtime coordination.
// These tests prove the request registry, not end-to-end reliable execution.
// origin: docs/reviews/hot-update-plan.md §4.2.
package procrequest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

const requestTestBudget = 10 * time.Second
const requestTestTTL = time.Minute

type requestFixture struct {
	r     *Registry
	scope Scope
	now   atomic.Int64
}

func newRequestFixture(t *testing.T, limit int) *requestFixture {
	t.Helper()
	r, err := New("original-instance", limit, requestTestTTL)
	if err != nil {
		t.Fatal(err)
	}
	f := &requestFixture{r: r, scope: requestScope()}
	f.now.Store(int64(time.Hour))
	r.now = func() (int64, error) { return f.now.Load(), nil }
	return f
}

func requestScope() Scope {
	digest := sha256.Sum256([]byte("authenticated actor fixture"))
	return Scope{SID: "sessionA", NID: "nodeA", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "clientA"}
}

func requestSpec() proto.ExecReq {
	return proto.ExecReq{Argv: []string{"/bin/sh", "-c", "read -r line; exit 23"}, Env: map[string]string{"ONE": "1", "TWO": "2"}, Cwd: "/tmp", Stdin: []byte("original-input"), Safe: true}
}

func (f *requestFixture) issue(t *testing.T, requestID string) proto.ProcessHandle {
	t.Helper()
	lease, err := f.r.Issue(f.scope)
	if err != nil {
		t.Fatal(err)
	}
	return proto.ProcessHandle{Lease: lease, RequestID: requestID}
}

func (f *requestFixture) reserve(t *testing.T, handle proto.ProcessHandle, pid string) Record {
	t.Helper()
	record, fresh, err := f.r.Reserve(f.scope, handle, requestSpec(), pid, "_TINBOX.actor.reply", 1)
	if err != nil || !fresh || record.PID != pid {
		t.Fatalf("initial reservation: fresh=%t pid=%q err=%v", fresh, record.PID, err)
	}
	return record
}

func TestRegistryConcurrentReservationStartsOneOriginalChild(t *testing.T) {
	f := newRequestFixture(t, 4)
	handle := f.issue(t, "requestA")
	const contenders = 32
	type outcome struct {
		record Record
		fresh  bool
		cmd    *exec.Cmd
		err    error
	}
	ctx, cancel := context.WithTimeout(t.Context(), requestTestBudget)
	defer cancel()
	start := make(chan struct{})
	results := make(chan outcome, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func(i int) {
			ready.Done()
			<-start
			record, fresh, err := f.r.Reserve(f.scope, handle, requestSpec(), fmt.Sprintf("task%d", i), "_TINBOX.actor.reply", 1)
			var child *exec.Cmd
			if err == nil && fresh {
				// The losing callers execute exactly the same path except that
				// Reserve denies them the right to perform the real side effect.
				child = exec.CommandContext(ctx, "/bin/sh", "-c", "exit 23")
				if err = child.Start(); err == nil {
					err = f.r.Running(handle, record.PID)
				}
			}
			results <- outcome{record: record, fresh: fresh, cmd: child, err: err}
		}(i)
	}
	ready.Wait()
	close(start)
	var all []outcome
	var children []*exec.Cmd
	for i := 0; i < contenders; i++ {
		got := <-results
		all = append(all, got)
		if got.cmd != nil && got.cmd.Process != nil {
			children = append(children, got.cmd)
		}
	}
	t.Cleanup(func() {
		for _, cmd := range children {
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	})
	if len(children) != 1 {
		t.Fatalf("duplicate physical creations: got %d OS children, want 1", len(children))
	}
	child := children[0]
	originalPID := child.Process.Pid
	var winner string
	for _, got := range all {
		if got.err != nil {
			t.Fatalf("concurrent admission: %v", got.err)
		}
		if got.fresh {
			if winner != "" {
				t.Fatal("more than one caller acquired creation ownership")
			}
			winner = got.record.PID
		}
	}
	for _, got := range all {
		if got.record.PID != winner {
			t.Fatalf("duplicate caller routed to %q; original task %q", got.record.PID, winner)
		}
	}
	// Treat the first started response as lost: the caller resends the exact
	// handle. Its suggested new task ID must not replace the original owner.
	retry, fresh, err := f.r.Reserve(f.scope, handle, requestSpec(), "would-be-duplicate", "_TINBOX.actor.reply", 1)
	if err != nil || fresh || retry.PID != winner || child.Process.Pid != originalPID {
		t.Fatalf("lost-response retry replaced original: fresh=%t task=%q err=%v", fresh, retry.PID, err)
	}
	if err := child.Wait(); err == nil || child.ProcessState.ExitCode() != 23 {
		t.Fatalf("original child result: err=%v status=%v", err, child.ProcessState)
	}
	terminal := Terminal{ID: "terminalA", ExitCode: 23}
	if err := f.r.Complete(handle, winner, terminal); err != nil {
		t.Fatal(err)
	}
	retry, fresh, err = f.r.Reserve(f.scope, handle, requestSpec(), "late-duplicate", "_TINBOX.actor.reply", 1)
	if err != nil || fresh || retry.PID != winner || retry.Terminal == nil || *retry.Terminal != terminal {
		t.Fatalf("terminal retry did not retain original receipt: fresh=%t pid=%q terminal=%v err=%v", fresh, retry.PID, retry.Terminal, err)
	}
}

func TestRegistryRejectsChangedLeaseScopeAndIntent(t *testing.T) {
	f := newRequestFixture(t, 4)
	handle := f.issue(t, "requestA")
	f.reserve(t, handle, "taskA")
	if err := f.r.Running(handle, "taskA"); err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		edit func(*proto.ProcessHandle)
	}{
		{"version", func(h *proto.ProcessHandle) { h.Lease.Version++ }},
		{"lease-id", func(h *proto.ProcessHandle) { h.Lease.LeaseID += "changed" }},
		{"instance", func(h *proto.ProcessHandle) { h.Lease.TargetInstance += "changed" }},
		{"boot", func(h *proto.ProcessHandle) { h.Lease.BootID += "changed" }},
		{"session", func(h *proto.ProcessHandle) { h.Lease.SID += "changed" }},
		{"node", func(h *proto.ProcessHandle) { h.Lease.NID += "changed" }},
		{"actor", func(h *proto.ProcessHandle) { h.Lease.ActorFP = requestOtherScope().ActorFP }},
		{"lineage", func(h *proto.ProcessHandle) { h.Lease.ClientLineage += "changed" }},
		{"issued", func(h *proto.ProcessHandle) { h.Lease.IssuedBootNS-- }},
		{"expiry", func(h *proto.ProcessHandle) { h.Lease.ExpiresBootNS++ }},
		{"mac", func(h *proto.ProcessHandle) { h.Lease.MAC = "invalid" }},
		{"empty-request", func(h *proto.ProcessHandle) { h.RequestID = "" }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			bad := handle
			tc.edit(&bad)
			if _, fresh, err := f.r.Reserve(f.scope, bad, requestSpec(), "bad-task", "_TINBOX.actor.reply", 1); err == nil || fresh {
				t.Fatalf("changed signed identity accepted: fresh=%t err=%v", fresh, err)
			}
			if _, err := f.r.Lookup(f.scope, bad); err == nil {
				t.Fatal("existing-record lookup bypassed lease validation")
			}
		})
	}
	for _, field := range []string{"session", "node", "actor", "lineage"} {
		t.Run("authenticated-"+field, func(t *testing.T) {
			wrong := f.scope
			switch field {
			case "session":
				wrong.SID += "changed"
			case "node":
				wrong.NID += "changed"
			case "actor":
				wrong.ActorFP = requestOtherScope().ActorFP
			case "lineage":
				wrong.ClientLineage += "changed"
			}
			if _, err := f.r.Lookup(wrong, handle); err == nil {
				t.Fatal("record exposed across authenticated scope")
			}
			if _, err := f.r.Attach(wrong, handle, "_TINBOX.attacker.reply", 2); err == nil {
				t.Fatal("attachment stolen across authenticated scope")
			}
		})
	}
	for _, field := range []string{"argv", "env", "cwd", "stdin", "safe"} {
		t.Run("intent-"+field, func(t *testing.T) {
			spec := requestSpec()
			switch field {
			case "argv":
				spec.Argv[1] = "changed"
			case "env":
				spec.Env["ONE"] = "changed"
			case "cwd":
				spec.Cwd = "/"
			case "stdin":
				spec.Stdin[0]++
			case "safe":
				spec.Safe = false
			}
			if _, fresh, err := f.r.Reserve(f.scope, handle, spec, "bad-task", "_TINBOX.actor.reply", 1); err == nil || fresh {
				t.Fatalf("changed effectful spec accepted: fresh=%t err=%v", fresh, err)
			}
		})
	}
	// Map insertion order is not a change of intent.
	spec := requestSpec()
	spec.Env = map[string]string{"TWO": "2", "ONE": "1"}
	got, fresh, err := f.r.Reserve(f.scope, handle, spec, "unused", "_TINBOX.actor.reply", 1)
	if err != nil || fresh || got.PID != "taskA" {
		t.Fatalf("equal canonical intent changed identity: fresh=%t pid=%q err=%v", fresh, got.PID, err)
	}
}

func requestOtherScope() Scope {
	scope := requestScope()
	digest := sha256.Sum256([]byte("a different authenticated actor"))
	scope.ActorFP = "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:])
	return scope
}

func TestRegistryExpiryAndCollectionNeverRecreateOriginalRequest(t *testing.T) {
	f := newRequestFixture(t, 3)
	terminalHandle, liveHandle, startingHandle := f.issue(t, "terminal"), f.issue(t, "live"), f.issue(t, "starting")
	for _, item := range []struct {
		h   proto.ProcessHandle
		pid string
	}{{terminalHandle, "taskTerminal"}, {liveHandle, "taskLive"}, {startingHandle, "taskStarting"}} {
		f.reserve(t, item.h, item.pid)
	}
	if err := f.r.Complete(terminalHandle, "taskTerminal", Terminal{ID: "terminalA", ExitCode: 23}); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Settle(f.scope, terminalHandle, "terminalA"); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Running(liveHandle, "taskLive"); err != nil {
		t.Fatal(err)
	}
	f.now.Store(terminalHandle.Lease.ExpiresBootNS - 1)
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("collected %d records before admission deadline", n)
	}
	f.now.Store(terminalHandle.Lease.ExpiresBootNS)
	if n := f.r.Collect(); n != 1 {
		t.Fatalf("at deadline collected %d records, want terminal only", n)
	}
	for _, h := range []proto.ProcessHandle{liveHandle, startingHandle} {
		got, fresh, err := f.r.Reserve(f.scope, h, requestSpec(), "must-not-spawn", "_TINBOX.actor.reply", 1)
		if err != nil || fresh || got.PID == "must-not-spawn" {
			t.Fatalf("expired known task lost owner: fresh=%t pid=%q err=%v", fresh, got.PID, err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, fresh, err := f.r.Reserve(f.scope, terminalHandle, requestSpec(), "late-duplicate", "_TINBOX.actor.reply", 1); err == nil || fresh {
			t.Fatalf("late retry respawned collected identity: fresh=%t err=%v", fresh, err)
		}
	}
	if _, err := f.r.Freeze(); err == nil {
		t.Fatal("expired starting admission incorrectly became transferable")
	}
	if err := f.r.Complete(startingHandle, "taskStarting", Terminal{ID: "setupFailure", Error: "start failed"}); err != nil {
		t.Fatal(err)
	}
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("unacknowledged setup result was collected: %d", n)
	}
	if err := f.r.Settle(f.scope, startingHandle, "setupFailure"); err != nil {
		t.Fatal(err)
	}
	if n := f.r.Collect(); n != 1 {
		t.Fatalf("resolved expired starting receipt retained: collected %d", n)
	}
	if _, err := f.r.Lookup(f.scope, liveHandle); err != nil {
		t.Fatalf("live resume denied after lease expiry: %v", err)
	}
	// A fresh lease with the same RequestID is an explicitly different request,
	// never an automatic retry of the old operation.
	freshHandle := f.issue(t, terminalHandle.RequestID)
	if freshHandle.Lease.LeaseID == terminalHandle.Lease.LeaseID {
		t.Fatal("new lease reused stable identity")
	}
	f.reserve(t, freshHandle, "explicitNewTask")
	if _, fresh, err := f.r.Reserve(f.scope, terminalHandle, requestSpec(), "late-again", "_TINBOX.actor.reply", 1); err == nil || fresh {
		t.Fatalf("new lease revived old identity: fresh=%t err=%v", fresh, err)
	}
}

func requestCloneState(t *testing.T, state State) State {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var clone State
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func TestRegistryCapacityAndFreezeProtectAdmittedResponsibilities(t *testing.T) {
	f := newRequestFixture(t, 2)
	a, b, c := f.issue(t, "A"), f.issue(t, "B"), f.issue(t, "C")
	f.reserve(t, a, "taskA")
	if _, err := f.r.Freeze(); !errors.Is(err, ErrBusy) {
		t.Fatalf("starting snapshot: %v", err)
	}
	if err := f.r.Complete(a, "taskA", Terminal{ID: "terminalA", Error: "setup failed"}); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Settle(f.scope, a, "terminalA"); err != nil {
		t.Fatal(err)
	}
	f.reserve(t, b, "taskB")
	if err := f.r.Running(b, "taskB"); err != nil {
		t.Fatal(err)
	}
	if _, fresh, err := f.r.Reserve(f.scope, c, requestSpec(), "taskC", "_TINBOX.actor.reply", 1); !errors.Is(err, ErrCapacity) || fresh {
		t.Fatalf("full registry admitted new side effect: fresh=%t err=%v", fresh, err)
	}
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("evicted %d unexpired receipts to make room", n)
	}
	state, err := f.r.Freeze()
	if err != nil || len(state.Records) != 2 {
		t.Fatalf("freeze resolved ownership: records=%d err=%v", len(state.Records), err)
	}
	for name, op := range map[string]func() error{
		"issue": func() error { _, err := f.r.Issue(f.scope); return err },
		"reserve": func() error {
			_, _, err := f.r.Reserve(f.scope, b, requestSpec(), "other", "_TINBOX.actor.reply", 1)
			return err
		},
		"attach":   func() error { _, err := f.r.Attach(f.scope, b, "_TINBOX.actor.next", 2); return err },
		"running":  func() error { return f.r.Running(b, "taskB") },
		"complete": func() error { return f.r.Complete(b, "taskB", Terminal{ID: "terminalB", ExitCode: 23}) },
		"settle":   func() error { return f.r.Settle(f.scope, a, "terminalA") },
	} {
		t.Run(name, func(t *testing.T) {
			if err := op(); !errors.Is(err, ErrFrozen) {
				t.Fatalf("frozen mutation: %v", err)
			}
		})
	}
	f.now.Store(a.Lease.ExpiresBootNS)
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("frozen GC changed export: %d", n)
	}
	f.r.Thaw()
	if n := f.r.Collect(); n != 1 {
		t.Fatalf("thawed GC did not free settled receipt: %d", n)
	}
	// Rejected C was never admitted. At its exact deadline it must remain
	// rejected even though the table now has capacity.
	if _, fresh, err := f.r.Reserve(f.scope, c, requestSpec(), "taskC", "_TINBOX.actor.reply", 1); !errors.Is(err, ErrExpired) || fresh {
		t.Fatalf("expired rejected admission gained a slot: fresh=%t err=%v", fresh, err)
	}
	f.reserve(t, f.issue(t, "C"), "taskC")
}

func TestRegistryAttachmentAndTerminalTransitionsRemainIdempotent(t *testing.T) {
	f := newRequestFixture(t, 2)
	h := f.issue(t, "requestA")
	f.reserve(t, h, "taskA")
	if err := f.r.Running(h, "wrongTask"); !errors.Is(err, ErrIdentity) {
		t.Fatalf("running changed owner: %v", err)
	}
	if err := f.r.Complete(h, "wrongTask", Terminal{ID: "terminalA", ExitCode: 23}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("terminal changed owner: %v", err)
	}
	if err := f.r.Settle(f.scope, h, "terminalA"); !errors.Is(err, ErrConflict) {
		t.Fatalf("settled before a terminal existed: %v", err)
	}
	if err := f.r.Running(h, "taskA"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Attach(f.scope, h, "_TINBOX.actor.new", 2); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		inbox string
		epoch uint64
	}{{"_TINBOX.actor.reply", 1}, {"_TINBOX.attacker.replacement", 2}} {
		if _, err := f.r.Attach(f.scope, h, tc.inbox, tc.epoch); !errors.Is(err, ErrStale) {
			t.Fatalf("stale attachment took ownership: epoch=%d err=%v", tc.epoch, err)
		}
	}
	if _, err := f.r.Attach(f.scope, h, "_TINBOX.actor.new", 2); err != nil {
		t.Fatalf("same attachment retry: %v", err)
	}
	if _, fresh, err := f.r.Reserve(f.scope, h, requestSpec(), "unused", "_TINBOX.actor.reply", 1); !errors.Is(err, ErrStale) || fresh {
		t.Fatalf("old create retry stole new attachment: fresh=%t err=%v", fresh, err)
	}
	terminal := Terminal{ID: "terminalA", ExitCode: -1, Signal: 15}
	if err := f.r.Complete(h, "taskA", terminal); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Complete(h, "taskA", terminal); err != nil {
		t.Fatalf("identical terminal retry: %v", err)
	}
	changed := terminal
	changed.Signal = 9
	if err := f.r.Complete(h, "taskA", changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal receipt overwritten: %v", err)
	}
	if err := f.r.Running(h, "taskA"); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal task returned to running: %v", err)
	}
	if err := f.r.Settle(requestOtherScope(), h, terminal.ID); !errors.Is(err, ErrIdentity) {
		t.Fatalf("another actor settled receipt: %v", err)
	}
	if err := f.r.Settle(f.scope, h, "otherTerminal"); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong terminal settled receipt: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := f.r.Settle(f.scope, h, terminal.ID); err != nil {
			t.Fatal(err)
		}
	}
	got, err := f.r.Lookup(f.scope, h)
	if err != nil || got.Phase != "settled" || got.Inbox != "_TINBOX.actor.new" || got.AttachEpoch != 2 || got.Terminal == nil || *got.Terminal != terminal {
		t.Fatalf("settled responsibility changed: phase=%q inbox=%q epoch=%d terminal=%v err=%v", got.Phase, got.Inbox, got.AttachEpoch, got.Terminal, err)
	}
	got.Terminal.Signal = 1
	again, err := f.r.Lookup(f.scope, h)
	if err != nil || again.Terminal == nil || *again.Terminal != terminal {
		t.Fatalf("lookup returned mutable owned terminal: %v %v", again.Terminal, err)
	}
}

func requestRestorableState(t *testing.T) (*requestFixture, State) {
	t.Helper()
	f := newRequestFixture(t, 4)
	for i, phase := range []string{"running", "terminal", "settled"} {
		h := f.issue(t, phase)
		pid := fmt.Sprintf("task%d", i)
		f.reserve(t, h, pid)
		if err := f.r.Running(h, pid); err != nil {
			t.Fatal(err)
		}
		if phase != "running" {
			if err := f.r.Complete(h, pid, Terminal{ID: "terminal" + phase, ExitCode: 23}); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "settled" {
			if err := f.r.Settle(f.scope, h, "terminal"+phase); err != nil {
				t.Fatal(err)
			}
		}
	}
	state, err := f.r.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	return f, state
}

func TestRegistryRestoreRejectsPartialAndIncompatibleState(t *testing.T) {
	f, good := requestRestorableState(t)
	cases := []struct {
		name string
		edit func(*State)
	}{
		{"version", func(s *State) { s.Version++ }},
		{"instance", func(s *State) { s.Instance += "changed" }},
		{"boot", func(s *State) { s.BootID += "changed" }},
		{"missing-key", func(s *State) { s.Secret = nil }},
		{"wrong-key", func(s *State) { s.Secret[0] ^= 1 }},
		{"limit", func(s *State) { s.Limit = 1 }},
		{"ttl", func(s *State) { s.TTL = 0 }},
		{"duplicate-key", func(s *State) {
			duplicate := s.Records[0]
			duplicate.PID = "otherPID"
			s.Records = append(s.Records, duplicate)
		}},
		{"duplicate-pid", func(s *State) { s.Records[1].PID = s.Records[0].PID }},
		{"late-bad-mac", func(s *State) { s.Records[len(s.Records)-1].Handle.Lease.MAC = "invalid" }},
		{"bad-hash", func(s *State) { s.Records[0].IntentHash = "invalid" }},
		{"starting", func(s *State) { s.Records[0].Phase = "starting" }},
		{"unknown-phase", func(s *State) { s.Records[0].Phase = "future" }},
		{"live-terminal", func(s *State) { s.Records[0].Terminal = &Terminal{ID: "unexpected", ExitCode: 23} }},
		{"missing-terminal", func(s *State) { s.Records[1].Terminal = nil }},
		{"invalid-terminal", func(s *State) { s.Records[1].Terminal.Signal = 9 }},
		{"zero-epoch", func(s *State) { s.Records[0].AttachEpoch = 0 }},
		{"empty-inbox", func(s *State) { s.Records[0].Inbox = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := requestCloneState(t, good)
			tc.edit(&bad)
			candidate, err := Restore(bad)
			if err == nil || candidate != nil {
				t.Fatalf("invalid import exposed a partially installed owner: candidate=%p err=%v", candidate, err)
			}
			for _, want := range good.Records {
				got, err := f.r.Lookup(f.scope, want.Handle)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("failed import affected original owner: pid=%q err=%v", want.PID, err)
				}
			}
		})
	}
	// A valid empty snapshot retains signing identity. It differs from losing
	// an old table and cold-starting an empty replacement for it.
	empty := newRequestFixture(t, 1)
	emptyState, err := empty.r.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(emptyState); err != nil {
		t.Fatalf("valid empty hot snapshot rejected: %v", err)
	}
}

func TestRegistryRestoreOwnsCopiesAndRequiresExplicitActivation(t *testing.T) {
	f, exported := requestRestorableState(t)
	want := requestCloneState(t, exported)
	imported, err := Restore(exported)
	if err != nil {
		t.Fatal(err)
	}
	imported.now = f.r.now
	exported.Secret[0] ^= 1
	exported.Records[0].PID = "caller-mutation"
	exported.Records[1].Terminal.ExitCode = 99
	for _, owner := range []*Registry{f.r, imported} {
		for _, expected := range want.Records {
			got, err := owner.Lookup(f.scope, expected.Handle)
			if err != nil || !reflect.DeepEqual(got, expected) {
				t.Fatalf("snapshot aliases owner data: pid=%q err=%v", expected.PID, err)
			}
		}
	}
	live := want.Records[0]
	if _, fresh, err := imported.Reserve(f.scope, live.Handle, requestSpec(), "unused", live.Inbox, live.AttachEpoch); !errors.Is(err, ErrFrozen) || fresh {
		t.Fatalf("restored owner active before Thaw: fresh=%t err=%v", fresh, err)
	}
	imported.Thaw()
	imported.Thaw()
	got, fresh, err := imported.Reserve(f.scope, live.Handle, requestSpec(), "unused", live.Inbox, live.AttachEpoch)
	if err != nil || fresh || got.PID != live.PID {
		t.Fatalf("activated restore forgot original admission: fresh=%t pid=%q err=%v", fresh, got.PID, err)
	}
	lease, err := imported.Issue(f.scope)
	if err != nil {
		t.Fatal(err)
	}
	// Issuance after import must still use the exported signing identity, not
	// a new key that would strand handles issued before the image switch.
	if _, err := f.r.Lookup(f.scope, proto.ProcessHandle{Lease: lease, RequestID: "not-admitted"}); !errors.Is(err, ErrUnknown) {
		t.Fatalf("restored issuer changed lease authority: %v", err)
	}
	cold, err := New("replacement-instance", 4, requestTestTTL)
	if err != nil {
		t.Fatal(err)
	}
	// A replacement instance refuses the old lease with the one identity
	// failure a client may act on (S1: ErrInstance → process_instance_lost),
	// still without granting creation.
	if _, fresh, err := cold.Reserve(f.scope, live.Handle, requestSpec(), "duplicate", live.Inbox, live.AttachEpoch); !errors.Is(err, ErrInstance) || fresh {
		t.Fatalf("cold replacement accepted old creation identity: fresh=%t err=%v", fresh, err)
	}
	newKey, err := New(want.Instance, 4, requestTestTTL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newKey.Lookup(f.scope, live.Handle); !errors.Is(err, ErrIdentity) {
		t.Fatalf("fresh secret accepted old signed handle: %v", err)
	}
}

func TestRegistryClockFailureCannotAdmitOrDiscardResponsibilities(t *testing.T) {
	f := newRequestFixture(t, 2)
	live, unused := f.issue(t, "live"), f.issue(t, "unused")
	f.reserve(t, live, "taskA")
	if err := f.r.Complete(live, "taskA", Terminal{ID: "terminalA", Error: "setup failed"}); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Settle(f.scope, live, "terminalA"); err != nil {
		t.Fatal(err)
	}
	clockErr := errors.New("injected boot-clock failure")
	f.r.now = func() (int64, error) { return 0, clockErr }
	if _, err := f.r.Issue(f.scope); err == nil {
		t.Fatal("issued a lease without boot-clock authority")
	}
	if _, fresh, err := f.r.Reserve(f.scope, unused, requestSpec(), "taskB", "_TINBOX.actor.reply", 1); err == nil || fresh {
		t.Fatalf("clock failure admitted task: fresh=%t err=%v", fresh, err)
	}
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("clock failure discarded %d receipts", n)
	}
	if _, err := f.r.Lookup(f.scope, live); err != nil {
		t.Fatalf("clock failure hid known retained result: %v", err)
	}
	for _, now := range []int64{-1, int64(^uint64(0) >> 1)} {
		f.r.now = func() (int64, error) { return now, nil }
		if _, err := f.r.Issue(f.scope); err == nil {
			t.Fatalf("invalid boot clock %d issued a lease", now)
		}
	}
}

func TestRegistryConcurrentCollectionCannotReopenExpiredIdentity(t *testing.T) {
	f := newRequestFixture(t, 1)
	h := f.issue(t, "originalRequest")
	f.reserve(t, h, "originalTask")
	if err := f.r.Complete(h, "originalTask", Terminal{ID: "originalTerminal", ExitCode: 23}); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Settle(f.scope, h, "originalTerminal"); err != nil {
		t.Fatal(err)
	}
	f.now.Store(h.Lease.ExpiresBootNS)
	const contenders = 32
	var ready, done sync.WaitGroup
	ready.Add(contenders + 1)
	done.Add(contenders + 1)
	start := make(chan struct{})
	failures := make(chan error, contenders)
	go func() {
		defer done.Done()
		ready.Done()
		<-start
		f.r.Collect()
	}()
	for i := 0; i < contenders; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			got, fresh, err := f.r.Reserve(f.scope, h, requestSpec(), "duplicateTask", "_TINBOX.actor.reply", 1)
			if fresh || (err == nil && got.PID != "originalTask") || (err != nil && !errors.Is(err, ErrExpired)) {
				failures <- fmt.Errorf("collection race: fresh=%t pid=%q err=%v", fresh, got.PID, err)
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if _, fresh, err := f.r.Reserve(f.scope, h, requestSpec(), "lateDuplicate", "_TINBOX.actor.reply", 1); !errors.Is(err, ErrExpired) || fresh {
		t.Fatalf("collected identity acquired new creation rights: fresh=%t err=%v", fresh, err)
	}
}

func TestRegistryRepeatedLifecycleReleasesResources(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseRegistryLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "creation-registry", beforeG)
	testharness.AssertNoFDLeak(t, "creation-registry", beforeFD, 0)
}

func exerciseRegistryLifecycle(t *testing.T) {
	t.Helper()
	f, state := requestRestorableState(t)
	owner, err := Restore(state)
	if err != nil {
		t.Fatal(err)
	}
	owner.now = f.r.now
	owner.Thaw()
	for _, record := range state.Records {
		terminal := record.Terminal
		if terminal == nil {
			terminal = &Terminal{ID: "terminal" + record.PID, ExitCode: 23}
			if err := owner.Complete(record.Handle, record.PID, *terminal); err != nil {
				t.Fatal(err)
			}
		}
		if err := owner.Settle(f.scope, record.Handle, terminal.ID); err != nil {
			t.Fatal(err)
		}
	}
	f.now.Store(state.Records[0].Handle.Lease.ExpiresBootNS)
	if n := owner.Collect(); n != len(state.Records) {
		t.Fatalf("cycle retained settled state: collected=%d want=%d", n, len(state.Records))
	}
	remaining, err := owner.Freeze()
	if err != nil || len(remaining.Records) != 0 {
		t.Fatalf("cycle did not release records: records=%d err=%v", len(remaining.Records), err)
	}
}

// origin: hot-update S2 plan §4 items 7/9.
// RestoreWithClock must hand the restored table the runtime's OWN clock: a
// restored registry that silently fell back to CLOCK_BOOTTIME would measure
// lease expiry on one clock while the runtime measures abandoned-terminal
// grace and tombstone TTL on another. The proof is behavioural, not
// structural: Issue/Collect on the restored table must move with the injected
// clock and nothing else. The Secret half proves every fmt verb redacts while
// the JSON round trip still carries the real key (the sealed snapshot is the
// only place it may travel).
func TestRegistryRestoreWithClockAndRedactedSecret(t *testing.T) {
	f, exported := requestRestorableState(t)
	var fake atomic.Int64
	fake.Store(int64(48 * time.Hour)) // far from the exporter's clock (1h) so a real-clock fallback is visible
	restored, err := RestoreWithClock(exported, func() (int64, error) { return fake.Load(), nil })
	if err != nil {
		t.Fatal(err)
	}
	restored.Thaw()
	lease, err := restored.Issue(f.scope)
	if err != nil {
		t.Fatal(err)
	}
	if lease.IssuedBootNS != fake.Load() || lease.ExpiresBootNS != fake.Load()+int64(exported.TTL) {
		t.Fatalf("restored registry did not issue on the injected clock: issued=%d expires=%d want=%d", lease.IssuedBootNS, lease.ExpiresBootNS, fake.Load())
	}
	// Collect must also read the injected clock: the settled record's lease
	// expired on the EXPORTER's clock (1h + 1m); on the injected 48h clock it
	// is long gone, on the real boot clock it may or may not be.
	if n := restored.Collect(); n != 1 {
		t.Fatalf("Collect on the injected clock reclaimed %d settled records, want 1", n)
	}
	// A nil clock is the documented real-clock default, same as NewWithClock.
	if _, err := RestoreWithClock(exported, nil); err != nil {
		t.Fatalf("nil clock must select the real boot clock: %v", err)
	}

	secretHex := hex.EncodeToString(exported.Secret)
	secretB64 := base64.StdEncoding.EncodeToString(exported.Secret)
	renderings := map[string]string{
		"%v":       fmt.Sprintf("%v", exported),
		"%+v":      fmt.Sprintf("%+v", exported),
		"%s":       fmt.Sprintf("%s", exported.Secret),
		"%x":       fmt.Sprintf("%x", exported.Secret),
		"%X":       fmt.Sprintf("%X", exported.Secret),
		"%q":       fmt.Sprintf("%q", exported.Secret),
		"%v-field": fmt.Sprintf("%v", exported.Secret),
		"Error":    fmt.Errorf("wrapped: %v", exported).Error(),
	}
	for verb, text := range renderings {
		if strings.Contains(text, secretHex) || strings.Contains(strings.ToUpper(text), strings.ToUpper(secretHex)) ||
			strings.Contains(text, secretB64) || !strings.Contains(text, "[redacted]") {
			t.Fatalf("%s rendering leaks the registry secret or lacks the marker: %q", verb, text)
		}
	}
	// The raw decimal byte list ("[12 200 ...]") is what %v prints for a
	// plain []byte; a named type without Format would still print it.
	if strings.Contains(renderings["%v"], fmt.Sprintf("%v", []byte(exported.Secret))) {
		t.Fatalf("%%v prints the raw key bytes: %q", renderings["%v"])
	}
	blob, err := json.Marshal(exported)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), secretB64) {
		t.Fatalf("JSON must carry the real key for the sealed snapshot: %s", blob)
	}
	var back State
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back.Secret, exported.Secret) {
		t.Fatal("JSON round trip changed the key")
	}
	again, err := RestoreWithClock(back, f.r.now)
	if err != nil {
		t.Fatal(err)
	}
	again.Thaw()
	// The round-tripped key still verifies the exporter's handles.
	if _, err := again.Lookup(f.scope, exported.Records[0].Handle); err != nil {
		t.Fatalf("round-tripped secret no longer verifies an exported handle: %v", err)
	}
}
