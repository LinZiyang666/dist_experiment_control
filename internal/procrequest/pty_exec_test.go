//go:build linux

// Real: same-PID exec, sealed registry handoff, boot-clock lease retention.
// Stubbed: terminal allocation, stream ownership and broker ACLs. These are
// metadata-only registry records; this test does not claim PTY I/O continuity.
// origin: hot-update S4 plan §8.1 / T17
package procrequest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

const ptyRegistryMode = "TETHER_TEST_PTY_REGISTRY_MODE"
const ptyRegistryReport = "TETHER_TEST_PTY_REGISTRY_REPORT"
const ptyRegistryTestName = "TestPTYIntentAndSettledKindSurviveImage"

type ptyRegistryImage struct {
	Registry State    `json:"registry"`
	Expected []Record `json:"expected"`
	Now      int64    `json:"now"`
	OwnerPID int      `json:"owner_pid"`
}

type ptyRegistryReceipt struct {
	OwnerPID        int      `json:"owner_pid"`
	Records         int      `json:"records"`
	PTYRecords      int      `json:"pty_records"`
	Collected       int      `json:"collected"`
	RetainedKinds   []string `json:"retained_kinds"`
	SettledOnlyKind []string `json:"settled_only_kind"`
	FinalKinds      []string `json:"final_kinds"`
}

// origin: hot-update S4 plan §5.1 / §8.1 / T17
func TestPTYIntentAndSettledKindSurviveImage(t *testing.T) {
	switch mode := os.Getenv(ptyRegistryMode); mode {
	case "before":
		ptyRegistryBefore(t)
		return
	case "after":
		ptyRegistryAfter(t)
		return
	case "":
	default:
		t.Fatalf("unknown PTY registry image mode %q", mode)
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		report := filepath.Join(t.TempDir(), "receipt.json")
		ctx, cancel := context.WithTimeout(t.Context(), 4*requestTestBudget)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+ptyRegistryTestName+"$", "-test.count=1")
		cmd.Env = append(ptyRegistryEnvironment("before"), ptyRegistryReport+"="+report)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		output, err := cmd.CombinedOutput()
		cancel()
		if bytes.Contains(output, []byte(`"secret"`)) || bytes.Contains(output, []byte("[redacted]")) {
			t.Fatal("registry image was rendered outside the sealed handoff")
		}
		if err != nil {
			t.Fatalf("same-PID PTY registry image round %d: %v\n%s", round, err, output)
		}
		data, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		var got ptyRegistryReceipt
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.OwnerPID != cmd.Process.Pid || got.Records != 4 || got.PTYRecords != 3 || got.Collected != 1 ||
			!reflect.DeepEqual(got.RetainedKinds, []string{"", "pty"}) ||
			!reflect.DeepEqual(got.SettledOnlyKind, []string{"pty"}) || !reflect.DeepEqual(got.FinalKinds, []string{""}) {
			t.Fatalf("incomplete same-PID kind/lease receipt: %+v; helper PID=%d", got, cmd.Process.Pid)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "pty-registry-image", beforeG)
	testharness.AssertNoFDLeak(t, "pty-registry-image", beforeFD, 0)
}

func ptyRegistryBefore(t *testing.T) {
	t.Helper()
	f := newRequestFixture(t, 4)
	var expected []Record
	for _, phase := range []string{"running", "terminal", "settled"} {
		h := f.issue(t, phase)
		pid := "pty-" + phase
		record, fresh, err := f.r.ReservePTY(f.scope, h, ptyRequestSpec(), pid, "_TINBOX."+phase, 3)
		if err != nil || !fresh || record.Kind != "pty" {
			t.Fatalf("pre-image PTY ownership: phase=%s fresh=%t kind=%q err=%v", phase, fresh, record.Kind, err)
		}
		if err := f.r.Running(h, pid); err != nil {
			t.Fatal(err)
		}
		if phase != "running" {
			if err := f.r.Complete(h, pid, Terminal{ID: "terminal-" + phase, ExitCode: 23}); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "settled" {
			if err := f.r.Settle(f.scope, h, "terminal-settled"); err != nil {
				t.Fatal(err)
			}
		}
		record, err = f.r.Lookup(f.scope, h)
		if err != nil {
			t.Fatal(err)
		}
		assertPTYIntentRetries(t, f.r, record)
		expected = append(expected, record)
	}
	h := f.issue(t, "exec")
	f.reserve(t, h, "exec-running")
	if err := f.r.Running(h, "exec-running"); err != nil {
		t.Fatal(err)
	}
	record, err := f.r.Lookup(f.scope, h)
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, record)
	f.now.Store(h.Lease.ExpiresBootNS - 1)
	if n := f.r.Collect(); n != 0 {
		t.Fatalf("settled PTY collected before lease expiry: %d", n)
	}
	state, err := f.r.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(ptyRegistryImage{Registry: state, Expected: expected, Now: f.now.Load(), OwnerPID: os.Getpid()})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: state.Instance, Generation: 1, Data: payload}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^" + ptyRegistryTestName + "$", "-test.count=1"}, ptyRegistryEnvironment("after")); err != nil {
		t.Fatalf("replace PTY registry image: %v", err)
	}
	t.Fatal("successful exec unexpectedly returned")
}

func ptyRegistryAfter(t *testing.T) {
	t.Helper()
	image, err := handoff.Restore()
	if err != nil || image == nil {
		t.Fatalf("private PTY registry image: present=%t err=%v", image != nil, err)
	}
	defer func() { _ = image.Close() }()
	var fixture ptyRegistryImage
	if err := json.Unmarshal(image.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	image.State.Data = nil
	if fixture.OwnerPID != os.Getpid() || len(fixture.Expected) != 4 {
		t.Fatalf("image did not retain original PID and nonempty records: pid=%d records=%d", fixture.OwnerPID, len(fixture.Expected))
	}
	if second, err := handoff.Restore(); second != nil || err != nil {
		t.Fatalf("handoff was not consumed once: present=%t err=%v", second != nil, err)
	}
	now := fixture.Now
	r, err := RestoreWithClock(fixture.Registry, func() (int64, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !r.Frozen() || r.Collect() != 0 {
		t.Fatal("restored registry performed collection before activation")
	}
	first := fixture.Expected[0]
	if _, fresh, err := r.ReservePTY(scopeOf(first.Handle.Lease), first.Handle, ptyRequestSpec(), "unused", first.Inbox, first.AttachEpoch); !errors.Is(err, ErrFrozen) || fresh {
		t.Fatalf("restored PTY admitted before activation: fresh=%t err=%v", fresh, err)
	}
	r.Thaw()
	receipt := ptyRegistryReceipt{OwnerPID: os.Getpid(), Records: len(fixture.Registry.Records), RetainedKinds: r.RetainedKinds()}
	var settled Record
	for _, expected := range fixture.Expected {
		got, err := r.Lookup(scopeOf(expected.Handle.Lease), expected.Handle)
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("retained record changed through image: pid=%s err=%v", expected.PID, err)
		}
		if strings.HasPrefix(expected.PID, "pty-") {
			if got.Kind != "pty" {
				t.Fatalf("PTY kind lost through image: pid=%s kind=%q", got.PID, got.Kind)
			}
			receipt.PTYRecords++
			assertPTYIntentRetries(t, r, got)
			if got.Phase == "settled" {
				settled = got
			}
		} else {
			got, fresh, err := r.Reserve(scopeOf(expected.Handle.Lease), expected.Handle, requestSpec(), "unused", expected.Inbox, expected.AttachEpoch)
			if err != nil || fresh || got.Kind != "" || !reflect.DeepEqual(got, expected) {
				t.Fatalf("original exec intent changed through mixed image: fresh=%t err=%v", fresh, err)
			}
		}
	}
	if settled.Kind != "pty" || settled.Terminal == nil || settled.Terminal.ExitCode != 23 {
		t.Fatal("nonempty settled PTY receipt missing")
	}
	if n := r.Collect(); n != 0 {
		t.Fatalf("settled PTY collected one nanosecond before expiry: %d", n)
	}
	// A settled-only image must still require the PTY reader even though no
	// task, terminal actor, resource or FD remains to signal that requirement.
	only := fixture.Registry
	only.Records = []Record{settled}
	onlyOwner, err := RestoreWithClock(only, func() (int64, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	receipt.SettledOnlyKind = onlyOwner.RetainedKinds()
	now = settled.Handle.Lease.ExpiresBootNS
	receipt.Collected = r.Collect()
	if receipt.Collected != 1 {
		t.Fatalf("expiry collected live or unacknowledged records: %d", receipt.Collected)
	}
	if _, fresh, err := r.ReservePTY(scopeOf(settled.Handle.Lease), settled.Handle, ptyRequestSpec(), "would-respawn", settled.Inbox, settled.AttachEpoch); !errors.Is(err, ErrExpired) || fresh {
		t.Fatalf("collected PTY respawned: fresh=%t err=%v", fresh, err)
	}
	for _, expected := range fixture.Expected {
		if expected.PID == settled.PID {
			continue
		}
		got, err := r.Lookup(scopeOf(expected.Handle.Lease), expected.Handle)
		if err != nil || !reflect.DeepEqual(got, expected) {
			t.Fatalf("expiry discarded an unsettled owner: pid=%s err=%v", expected.PID, err)
		}
		if got.Kind == "pty" {
			terminal := got.Terminal
			if terminal == nil {
				terminal = &Terminal{ID: "finished-" + got.PID, ExitCode: 23}
				if err := r.Complete(got.Handle, got.PID, *terminal); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Settle(scopeOf(got.Handle.Lease), got.Handle, terminal.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := r.Collect(); n != 2 {
		t.Fatalf("settled expired PTY receipts not released: %d", n)
	}
	receipt.FinalKinds = r.RetainedKinds()
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	// Observe release while the helper remains alive; process exit would hide
	// a leaked handoff memfd and turn a no-op Close into a false success.
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && strings.Contains(target, "memfd:tether-handoff") {
			t.Fatalf("handoff descriptor remains after close: %s", target)
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(ptyRegistryReport), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func ptyRegistryEnvironment(mode string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, ptyRegistryMode+"=") && !strings.HasPrefix(entry, ptyRegistryReport+"=") {
			env = append(env, entry)
		}
	}
	if mode == "after" {
		// The report path is fixture metadata, never part of a business child
		// environment. No child is spawned by either helper role here.
		env = append(env, fmt.Sprintf("%s=%s", ptyRegistryReport, os.Getenv(ptyRegistryReport)))
	}
	return append(env, ptyRegistryMode+"="+mode)
}
