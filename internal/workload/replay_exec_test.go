//go:build linux

// Real: memfd contents, same-PID exec, handoff ownership and post-exec writes.
// Stubbed: remote ACK delivery and the application's global resource barrier.
// This proves replay resource migration, not complete application hot update.
// origin: docs/reviews/hot-update-plan.md §4.2 item 6.
package workload_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const replayImageMode = "TETHER_TEST_REPLAY_IMAGE_MODE"
const replayImageReport = "TETHER_TEST_REPLAY_IMAGE_REPORT"
const replayImageMutation = "TETHER_TEST_REPLAY_IMAGE_MUTATION"

type replayImageObject struct {
	State workload.ReplayState `json:"state"`
	Dev   uint64               `json:"dev"`
	Ino   uint64               `json:"ino"`
}

type replayImageFixture struct {
	PID     int                          `json:"pid"`
	Nonce   string                       `json:"nonce"`
	Objects map[string]replayImageObject `json:"objects"`
}

type replayImageReceipt struct {
	PID      int    `json:"pid"`
	Nonce    string `json:"nonce"`
	Before   uint64 `json:"before"`
	After    uint64 `json:"after"`
	Capacity int64  `json:"capacity"`
	FIN      bool   `json:"fin"`
}

func replayImageEnvironment(mode string) []string {
	env := handoff.CleanEnvironment(os.Environ())
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, replayImageMode+"=") {
			result = append(result, entry)
		}
	}
	return append(result, replayImageMode+"="+mode)
}

// The oracle uses absolute logical offsets, independently of physical indexing.
func replaySequence(offset uint64, length int) []byte {
	result := make([]byte, length)
	for i := range result {
		result[i] = byte(((offset+uint64(i))%251*131 + 17) % 251)
	}
	return result
}

func replayImageIdentity(t *testing.T, file *os.File, state workload.ReplayState) replayImageObject {
	t.Helper()
	identity := replayImageObject{State: state}
	replayFileControl(t, file, func(fd int) error {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			return err
		}
		if flags&unix.FD_CLOEXEC == 0 {
			return fmt.Errorf("replay descriptor left inheritable")
		}
		identity.Dev, identity.Ino = uint64(stat.Dev), stat.Ino
		return nil
	})
	return identity
}

func replayImageReferences(t *testing.T, identities map[string]replayImageObject) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		info, err := os.Stat(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // The directory reader's own descriptor is already closed.
		}
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatal("missing Linux descriptor identity")
		}
		for _, identity := range identities {
			if uint64(stat.Dev) == identity.Dev && stat.Ino == identity.Ino {
				count++
			}
		}
	}
	return count
}

func checkReplaySequence(t *testing.T, log *workload.Replay, state workload.ReplayState) {
	t.Helper()
	dst := make([]byte, int(state.End-state.Base)+11)
	n, err := log.Read(state.Base, dst)
	want := replaySequence(state.Base, int(state.End-state.Base))
	if n != len(want) || !bytes.Equal(dst[:n], want) ||
		(state.FIN && !errors.Is(err, io.EOF)) || (!state.FIN && err != nil) {
		t.Fatalf("REPLAY_ORACLE_REJECT: retained bytes/FIN changed: state=%+v n=%d err=%v", state, n, err)
	}
}

func replayImageBeforeExec(t *testing.T) {
	t.Helper()
	budget := &workload.ReplayBudget{Limit: 324}
	fixture := replayImageFixture{PID: os.Getpid(), Nonce: filepath.Base(os.Getenv(replayImageReport)), Objects: make(map[string]replayImageObject)}
	var resources []handoff.Resource
	for _, item := range []struct {
		id       string
		capacity int64
		fin      bool
	}{{"running", 257, false}, {"finished", 67, true}} {
		log, err := workload.NewReplay(budget, item.capacity)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = log.Close() }()
		if n, err := log.Append(replaySequence(0, int(item.capacity))); n != int(item.capacity) || err != nil {
			t.Fatal(n, err)
		}
		// Retain a nonempty suffix while crossing physical boundaries repeatedly.
		for i := 0; i < 40; i++ {
			state := log.Snapshot()
			advance := uint64(item.capacity/2 + 1)
			if err := log.Ack(state.Base + advance); err != nil {
				t.Fatal(err)
			}
			if n, err := log.Append(replaySequence(state.End, int(advance))); n != int(advance) || err != nil {
				t.Fatal(n, err)
			}
		}
		if err := log.Ack(log.Snapshot().Base + 11); err != nil {
			t.Fatal(err)
		}
		if item.fin {
			if err := log.Finish(); err != nil {
				t.Fatal(err)
			}
		}
		state, file, err := log.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		checkReplaySequence(t, log, state)
		fixture.Objects[item.id] = replayImageIdentity(t, file, state)
		// Negative controls change the exported object after the independent
		// oracle is captured, then cross the same real exec/import boundary.
		if item.id == "running" {
			switch os.Getenv(replayImageMutation) {
			case "drop-prefix":
				state.Base++
			case "corrupt-byte":
				bad := replaySequence(state.Base, 1)
				bad[0] ^= 0xff
				if _, err := file.WriteAt(bad, int64(state.Base%uint64(state.Capacity))); err != nil {
					t.Fatal(err)
				}
			}
		}
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		resources = append(resources, handoff.Resource{ID: item.id, Kind: "replay", Data: data, Files: map[string]*os.File{"ring": file}})
	}
	data, err := json.Marshal(fixture)
	if err != nil || len(data) > 4096 {
		t.Fatalf("unbounded metadata: bytes=%d err=%v", len(data), err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "replay-test", Instance: "same-owner", Generation: 1, Data: data}, resources)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestReplayExecSubprocess$", "-test.count=1"}, replayImageEnvironment("after")); err != nil {
		t.Fatal(err)
	}
	t.Fatal("successful exec returned")
}

func replayImageAfterExec(t *testing.T) {
	t.Helper()
	packageState, err := handoff.Restore()
	if err != nil || packageState == nil {
		t.Fatalf("restore real exec package: %v", err)
	}
	defer func() { _ = packageState.Close() }()
	var fixture replayImageFixture
	if err := json.Unmarshal(packageState.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.PID != os.Getpid() || fixture.Nonce != filepath.Base(os.Getenv(replayImageReport)) || len(fixture.Objects) != 2 || len(packageState.Resources) != 2 {
		t.Fatalf("exec changed owner or fixture: %+v", fixture)
	}
	fmt.Printf("REPLAY_IMAGE_IMPORTED pid=%d nonce=%s\n", os.Getpid(), fixture.Nonce)
	if refs := replayImageReferences(t, fixture.Objects); refs != 2 {
		t.Fatalf("exec retained hidden replay descriptors: refs=%d want 2", refs)
	}
	importedFDs := testharness.FDCount()
	budget := &workload.ReplayBudget{Limit: 324}
	logs := make(map[string]*workload.Replay)
	for _, resource := range packageState.Resources {
		var state workload.ReplayState
		if err := json.Unmarshal(resource.Data, &state); err != nil {
			t.Fatal(err)
		}
		file, err := packageState.TakeFile(resource.ID, "ring")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		if duplicate, err := packageState.TakeFile(resource.ID, "ring"); duplicate != nil || err == nil {
			t.Fatal("handoff permitted a second owner")
		}
		if got := replayImageIdentity(t, file, state); got != fixture.Objects[resource.ID] {
			t.Fatalf("REPLAY_ORACLE_REJECT: retained state/identity changed: got=%+v want=%+v", got, fixture.Objects[resource.ID])
		}
		log, err := workload.RestoreReplay(budget, state, file)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = log.Close() }()
		logs[resource.ID] = log
		if _, err := log.Append([]byte("x")); !errors.Is(err, workload.ErrFrozen) {
			t.Fatalf("restored append not frozen: %v", err)
		}
		if err := log.Ack(state.End); !errors.Is(err, workload.ErrFrozen) {
			t.Fatalf("restored ACK not frozen: %v", err)
		}
		if err := log.Finish(); !errors.Is(err, workload.ErrFrozen) {
			t.Fatalf("restored FIN not frozen: %v", err)
		}
		checkReplaySequence(t, log, state)
	}
	if err := packageState.Close(); err != nil {
		t.Fatal(err)
	}
	for _, log := range logs {
		log.Thaw()
	}
	running := logs["running"]
	before := running.Snapshot()
	for i := 0; i < 48; i++ {
		state := running.Snapshot()
		advance := uint64(73)
		if err := running.Ack(state.Base + advance); err != nil {
			t.Fatal(err)
		}
		available := int(state.Capacity) - int(state.End-state.Base-advance)
		// The retained suffix must survive both wrapping and partial pressure.
		n, err := running.Append(replaySequence(state.End, available+13))
		if n != available || !errors.Is(err, workload.ErrBackpressure) {
			t.Fatalf("restored pressure lost prefix: %d,%v want=%d", n, err, available)
		}
		checkReplaySequence(t, running, running.Snapshot())
	}
	if err := running.Finish(); err != nil {
		t.Fatal(err)
	}
	checkReplaySequence(t, running, running.Snapshot())
	finished := logs["finished"]
	checkReplaySequence(t, finished, finished.Snapshot())
	if err := finished.Ack(finished.Snapshot().End); err != nil {
		t.Fatal(err)
	}
	if n, err := finished.Read(finished.Snapshot().End, make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("imported FIN disappeared after ACK: %d,%v", n, err)
	}
	after := running.Snapshot()
	for _, log := range logs {
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if remaining := testharness.FDCount(); remaining != importedFDs-2 {
		t.Fatalf("imported descriptors leaked: before=%d after=%d", importedFDs, remaining)
	}
	if refs := replayImageReferences(t, fixture.Objects); refs != 0 {
		t.Fatalf("closed replay still has a descriptor owner: refs=%d", refs)
	}
	replacement, err := workload.NewReplay(budget, 324)
	if err != nil {
		t.Fatalf("imported reservations leaked: %v", err)
	}
	_ = replacement.Close()
	receipt := replayImageReceipt{PID: os.Getpid(), Nonce: fixture.Nonce, Before: before.End, After: after.End, Capacity: before.Capacity, FIN: after.FIN}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(replayImageReport), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReplayExecSubprocess(t *testing.T) {
	switch mode := os.Getenv(replayImageMode); mode {
	case "":
	case "before":
		replayImageBeforeExec(t)
	case "after":
		replayImageAfterExec(t)
	default:
		t.Fatalf("unknown replay image mode %q", mode)
	}
}

func exerciseReplayExec(t *testing.T, mutation string) {
	t.Helper()
	report := filepath.Join(t.TempDir(), "replay-receipt.json")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplayExecSubprocess$", "-test.count=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Dir = dir
	cmd.Env = append(replayImageEnvironment("before"), replayImageReport+"="+report, replayImageMutation+"="+mutation)
	cmd.Env = append(cmd.Env, "TMPDIR="+dir)
	output, err := cmd.CombinedOutput()
	if mutation != "" {
		if err == nil || ctx.Err() != nil || !bytes.Contains(output, []byte("REPLAY_IMAGE_IMPORTED pid=")) || !bytes.Contains(output, []byte("REPLAY_ORACLE_REJECT: retained")) {
			t.Fatalf("negative control did not reach and trip imported byte/state oracle: err=%v\n%s", err, output)
		}
		if _, err := os.Stat(report); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected migration produced success receipt: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("same-PID replay migration: %v\n%s", err, output)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	var got replayImageReceipt
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.PID != cmd.Process.Pid || got.Nonce != filepath.Base(report) || got.Capacity != 257 || got.Before < 10*uint64(got.Capacity) || got.After-got.Before < 10*uint64(got.Capacity) || !got.FIN {
		t.Fatalf("vacuous or wrong migration receipt: %+v parent PID=%d", got, cmd.Process.Pid)
	}
}

func TestReplayExecPreservesUnackedBytes(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exerciseReplayExec(t, "")
}

func TestReplayExecOracleRejectsLostData(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, mutation := range []string{"drop-prefix", "corrupt-byte"} {
		t.Run(mutation, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseReplayExec(t, mutation)
		})
	}
}

func TestReplayExecRepeatedlyReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exerciseReplayExec(t, "")
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseReplayExec(t, "")
	}
	testharness.AssertNoGoroutineLeak(t, "replay same-PID exec", goroutines)
	testharness.AssertNoFDLeak(t, "replay same-PID exec", fds, 0)
}
