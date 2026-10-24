//go:build linux

// Real: pollable pipes, bounded memfd replay, bytes, EOF and descriptor ownership.
// Controlled: one actor-start schedule and a read-only replay FD inject cancellation/storage failure.
// Does not cover agent admission, courier, control protocols or complete hot update.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const outputTestBudget = 10 * time.Second

func TestOutputBackpressureFreezeRetainsEveryByte(t *testing.T) {
	out, log, writer := outputFixture(t, 1024)
	pipeSize := outputFDValue(t, writer, unix.F_GETPIPE_SZ)
	want := outputBytes(pipeSize + 1024 + 64<<10)
	written := outputStartWriter(writer, want)
	outputAwait(t, func() bool { return log.Snapshot().End == 1024 })
	select {
	case got := <-written:
		t.Fatalf("full replay did not backpressure the original writer: %+v", got)
	default:
	}
	state, files := outputFreeze(t, out)
	if state.Replay.FIN || state.Replay.End != 1024 || len(state.Pending) != 0 || state.ReadError != "" {
		t.Fatalf("healthy paused output: %+v", state)
	}
	if _, err := files["source"].Stat(); err != nil {
		t.Fatalf("freeze closed the source: %v", err)
	}
	if err := log.Ack(state.Replay.End); !errors.Is(err, ErrFrozen) {
		t.Fatalf("freeze left ACK active: %v", err)
	}
	retained := make([]byte, 1024)
	if n, err := log.Read(0, retained); err != nil || n != len(retained) || !bytes.Equal(retained, want[:1024]) {
		t.Fatalf("paused prefix lost: n=%d err=%v", n, err)
	}
	out.Thaw()
	out.Thaw()
	got := outputCollect(t, log, 0)
	if !bytes.Equal(got, want) {
		t.Fatalf("backpressured stream changed: got=%d want=%d", len(got), len(want))
	}
	outputRequireWriter(t, written, len(want))
}

func TestOutputBlockedReadRepeatedFreezeAndOriginalEOF(t *testing.T) {
	out, log, writer := outputFixture(t, 512)
	var offset uint64
	for i := 0; i < 5; i++ {
		state, files := outputFreeze(t, out)
		if state.Replay.End != offset || state.Replay.FIN || state.ReadError != "" {
			t.Fatalf("idle freeze %d: %+v", i, state)
		}
		if _, err := files["source"].Stat(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := out.Freeze(context.Background()); err == nil {
			t.Fatal("second freeze acquired the same owner")
		}
		out.Thaw()
		out.Thaw()
		chunk := []byte(fmt.Sprintf("original-pipe-%d", i))
		if _, err := writer.Write(chunk); err != nil {
			t.Fatal(err)
		}
		end := offset + uint64(len(chunk))
		outputAwait(t, func() bool { return log.Snapshot().End == end })
		got := make([]byte, len(chunk))
		if n, err := log.Read(offset, got); err != nil || n != len(chunk) || !bytes.Equal(got, chunk) {
			t.Fatalf("round %d bytes: %q %v", i, got, err)
		}
		if err := log.Ack(end); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	tail := []byte("last bytes before the original EOF")
	if _, err := writer.Write(tail); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	outputAwait(t, func() bool { return log.Snapshot().FIN })
	got := make([]byte, len(tail)+1)
	n, err := log.Read(offset, got)
	if !errors.Is(err, io.EOF) || n != len(tail) || !bytes.Equal(got[:n], tail) {
		t.Fatalf("n>0 EOF lost terminal bytes: n=%d err=%v got=%q", n, err, got[:n])
	}
	state, _ := outputFreeze(t, out)
	if !state.Replay.FIN || state.ReadError != "" {
		t.Fatalf("original EOF was fabricated as read failure: %+v", state)
	}
}

func TestOutputCanceledFreezeKeepsOriginalOwner(t *testing.T) {
	for _, during := range []bool{false, true} {
		name := "already-canceled"
		if during {
			name = "cancel-while-waiting-for-actor"
		}
		t.Run(name, func(t *testing.T) {
			var out *Output
			var log *Replay
			var writer *os.File
			release := func() {}
			if during {
				// This is NewOutput's valid actor state before its new goroutine
				// is scheduled. Delay only scheduling, not reads, bytes or Freeze.
				reader, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				writer = w
				log, err = NewReplay(&ReplayBudget{}, 512)
				if err != nil {
					t.Fatal(err)
				}
				out = &Output{source: reader, log: log, running: true, pause: make(chan struct{}), done: make(chan struct{})}
				gate := make(chan struct{})
				var once sync.Once
				release = func() { once.Do(func() { close(gate) }) }
				go func() { <-gate; out.run() }()
				t.Cleanup(func() { release(); _ = writer.Close(); _ = out.Close() })
			} else {
				out, log, writer = outputFixture(t, 512)
			}
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := base
			if during {
				ctx = &outputCancelOnDone{Context: base, cancel: cancel}
			} else {
				cancel()
			}
			state, files, err := out.Freeze(ctx)
			if !errors.Is(err, context.Canceled) || files != nil || state.Version != 0 {
				t.Fatalf("canceled freeze exported ownership: %+v %v %v", state, files, err)
			}
			release()
			want := []byte("same owner resumes without dropping a byte")
			written := outputStartWriter(writer, want)
			got := outputCollect(t, log, 0)
			outputRequireWriter(t, written, len(want))
			if !bytes.Equal(got, want) || out.Snapshot().ReadError != "" {
				t.Fatalf("cancel changed original owner: %q %+v", got, out.Snapshot())
			}
		})
	}
}

type outputCancelOnDone struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (c *outputCancelOnDone) Done() <-chan struct{} {
	c.once.Do(c.cancel)
	return c.Context.Done()
}

func TestOutputStorageFailureRetainsBoundedPendingSuffix(t *testing.T) {
	out, log, writer := outputFixture(t, 64<<10)
	prefix := outputBytes(4096)
	if _, err := writer.Write(prefix); err != nil {
		t.Fatal(err)
	}
	outputAwait(t, func() bool { return log.Snapshot().End == uint64(len(prefix)) })
	_, files := outputFreeze(t, out)
	// Fault injection only: replace the private descriptor with a read-only
	// handle to the SAME real memfd while its owner is frozen. RestoreReplay
	// correctly rejects that descriptor; normal product callers never do this.
	readOnly := outputReopen(t, files["replay"], unix.O_RDONLY)
	log.mu.Lock()
	original := log.file
	log.file = readOnly
	log.mu.Unlock()
	_ = original.Close()
	out.Thaw()
	suffix := outputBytes(48 << 10)
	written := outputStartWriter(writer, suffix)
	outputAwait(t, func() bool { return out.Snapshot().ReadError != "" })
	state, files := outputFreeze(t, out)
	if state.Replay.FIN || state.Replay.End != uint64(len(prefix)) || len(state.Pending) == 0 || len(state.Pending) > 32<<10 || !bytes.Equal(state.Pending, suffix[:len(state.Pending)]) {
		t.Fatalf("source bytes erased on append failure: replay=%+v pending=%d err=%q", state.Replay, len(state.Pending), state.ReadError)
	}
	gotPrefix := make([]byte, len(prefix))
	if n, err := log.Read(0, gotPrefix); err != nil || n != len(prefix) || !bytes.Equal(gotPrefix, prefix) {
		t.Fatalf("retained prefix: n=%d err=%v", n, err)
	}
	pending := append([]byte(nil), state.Pending...)
	state.Pending[0] ^= 255
	if !bytes.Equal(out.Snapshot().Pending, pending) {
		t.Fatal("Snapshot aliases pending source bytes")
	}
	state.Pending = append([]byte(nil), pending...)
	// Reopen read/write for a valid candidate log; its previous failed FD is
	// not smuggled past RestoreReplay's descriptor validation.
	replayFile := outputReopen(t, files["replay"], unix.O_RDWR)
	newLog, err := RestoreReplay(&ReplayBudget{}, state.Replay, replayFile)
	if err != nil {
		_ = replayFile.Close()
		t.Fatal(err)
	}
	newSource := outputDuplicate(t, files["source"])
	newOwner, err := RestoreOutput(state, newSource, newLog)
	if err != nil {
		_ = newSource.Close()
		_ = newLog.Close()
		t.Fatal(err)
	}
	defer func() { _ = newOwner.Close() }()
	state.Pending[0] ^= 255
	if got := newOwner.Snapshot(); !bytes.Equal(got.Pending, pending) || got.ReadError == "" || got.Replay.FIN {
		t.Fatalf("restored error/pending changed: %+v", got)
	}
	// Read only from a separate inspection descriptor after BOTH actors are
	// paused. The remaining suffix must still be in the original pipe/writer.
	remainderSource := outputDuplicate(t, files["source"])
	defer func() { _ = remainderSource.Close() }()
	if err := remainderSource.SetReadDeadline(time.Now().Add(outputTestBudget)); err != nil {
		t.Fatal(err)
	}
	remainder, err := io.ReadAll(remainderSource)
	if err != nil {
		t.Fatal(err)
	}
	outputRequireWriter(t, written, len(suffix))
	if !bytes.Equal(append(pending, remainder...), suffix) {
		t.Fatal("error path lost or duplicated source bytes between pending and pipe")
	}
}

func TestOutputRestoreRejectsInvalidStateWithoutTakingResources(t *testing.T) {
	for _, edit := range []struct {
		name   string
		change func(*OutputState)
	}{
		{"version", func(s *OutputState) { s.Version++ }},
		{"different-replay", func(s *OutputState) { s.Replay.End++ }},
		{"pending-without-error", func(s *OutputState) { s.Pending = []byte("lost") }},
		{"too-large-pending", func(s *OutputState) { s.Pending = make([]byte, (32<<10)+1); s.ReadError = "failure" }},
		{"too-large-error", func(s *OutputState) { s.ReadError = string(make([]byte, 4097)) }},
		{"error-and-fin", func(s *OutputState) { s.ReadError = "failure"; s.Replay.FIN = true }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Close(); _ = writer.Close() }()
			log, err := NewReplay(&ReplayBudget{}, 512)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			replay, _, err := log.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			state := OutputState{Version: 1, Replay: replay}
			edit.change(&state)
			if candidate, err := RestoreOutput(state, reader, log); err == nil || candidate != nil {
				t.Fatalf("invalid state acquired resources: %p %v", candidate, err)
			}
			if _, err := writer.Write([]byte("still caller owned")); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len("still caller owned"))
			if _, err := io.ReadFull(reader, got); err != nil || string(got) != "still caller owned" {
				t.Fatalf("invalid import closed/drained source: %q %v", got, err)
			}
			log.Thaw()
			if n, err := log.Append([]byte("caller log")); err != nil || n != len("caller log") {
				t.Fatalf("invalid import took replay: %d %v", n, err)
			}
		})
	}
}

func TestOutputRejectsInvalidDescriptorWithoutChangingFlags(t *testing.T) {
	for _, restore := range []bool{false, true} {
		for _, mode := range []string{"write-end", "blocking", "inheritable"} {
			t.Run(fmt.Sprintf("restore-%t/%s", restore, mode), func(t *testing.T) {
				reader, writer, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reader.Close(); _ = writer.Close() }()
				source := reader
				switch mode {
				case "write-end":
					source = writer
				case "blocking":
					outputSetFDValue(t, source, unix.F_SETFL, outputFDValue(t, source, unix.F_GETFL)&^unix.O_NONBLOCK)
				case "inheritable":
					outputSetFDValue(t, source, unix.F_SETFD, outputFDValue(t, source, unix.F_GETFD)&^unix.FD_CLOEXEC)
				}
				flags, fdflags := outputFDValue(t, source, unix.F_GETFL), outputFDValue(t, source, unix.F_GETFD)
				log, err := NewReplay(&ReplayBudget{}, 512)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = log.Close() }()
				var owner *Output
				if restore {
					state, _, ferr := log.Freeze()
					if ferr != nil {
						t.Fatal(ferr)
					}
					owner, err = RestoreOutput(OutputState{Version: 1, Replay: state}, source, log)
				} else {
					owner, err = NewOutput(source, log)
				}
				if err == nil || owner != nil {
					t.Fatalf("invalid descriptor acquired: %p %v", owner, err)
				}
				if outputFDValue(t, source, unix.F_GETFL) != flags || outputFDValue(t, source, unix.F_GETFD) != fdflags {
					t.Fatal("rejected descriptor flags changed")
				}
				if _, err := writer.Write([]byte("owned")); err != nil {
					t.Fatal(err)
				}
				got := make([]byte, 5)
				if _, err := io.ReadFull(reader, got); err != nil || string(got) != "owned" {
					t.Fatalf("rejected descriptor unusable: %q %v", got, err)
				}
				log.Thaw()
				if _, err := log.Append([]byte("still mine")); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestOutputRepeatedLifecycleReleasesResources(t *testing.T) {
	exerciseOutputLifecycle(t)
	baselineG, baselineFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseOutputLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "workload output lifecycle", baselineG)
	testharness.AssertNoFDLeak(t, "workload output lifecycle", baselineFD, 0)
}

func exerciseOutputLifecycle(t *testing.T) {
	t.Helper()
	out, log, writer := outputFixture(t, 512)
	outputFreeze(t, out)
	out.Thaw()
	want := outputBytes(8192)
	written := outputStartWriter(writer, want)
	if got := outputCollect(t, log, 0); !bytes.Equal(got, want) {
		t.Fatal("resource cycle lost bytes")
	}
	outputRequireWriter(t, written, len(want))
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func outputFixture(t *testing.T, capacity int64) (*Output, *Replay, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewReplay(&ReplayBudget{}, capacity)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	out, err := NewOutput(reader, log)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		_ = log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(); _ = out.Close() })
	return out, log, writer
}

func outputFreeze(t *testing.T, out *Output) (OutputState, map[string]*os.File) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), outputTestBudget)
	defer cancel()
	state, files, err := out.Freeze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return state, files
}

func outputAwait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(outputTestBudget)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("output fixture did not reach its observable state")
		}
		time.Sleep(time.Millisecond)
	}
}

func outputCollect(t *testing.T, log *Replay, offset uint64) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), outputTestBudget)
	defer cancel()
	var got []byte
	for {
		state, changed := log.Observe()
		buf := make([]byte, int(state.Capacity))
		n, err := log.Read(offset, buf)
		got = append(got, buf[:n]...)
		offset += uint64(n)
		if n > 0 {
			if ackErr := log.Ack(offset); ackErr != nil {
				t.Fatal(ackErr)
			}
		}
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			continue
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("output consumer stalled")
		}
	}
}

type outputWriteResult struct {
	n   int
	err error
}

func outputStartWriter(writer *os.File, data []byte) <-chan outputWriteResult {
	done := make(chan outputWriteResult, 1)
	go func() { n, err := writer.Write(data); _ = writer.Close(); done <- outputWriteResult{n, err} }()
	return done
}

func outputRequireWriter(t *testing.T, done <-chan outputWriteResult, want int) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil || got.n != want {
			t.Fatalf("original writer: %+v want=%d", got, want)
		}
	case <-time.After(outputTestBudget):
		t.Fatal("original writer remained blocked")
	}
}

func outputBytes(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*17 + 3)
	}
	return data
}

func outputFDValue(t *testing.T, file *os.File, command int) int {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var value int
	var callErr error
	if err := raw.Control(func(fd uintptr) { value, callErr = unix.FcntlInt(fd, command, 0) }); err != nil {
		t.Fatal(err)
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
	return value
}

func outputSetFDValue(t *testing.T, file *os.File, command, value int) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var callErr error
	if err := raw.Control(func(fd uintptr) { _, callErr = unix.FcntlInt(fd, command, value) }); err != nil {
		t.Fatal(err)
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
}

func outputDuplicate(t *testing.T, file *os.File) *os.File {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd int
	var callErr error
	if err := raw.Control(func(old uintptr) { fd, callErr = unix.FcntlInt(old, unix.F_DUPFD_CLOEXEC, 3) }); err != nil {
		t.Fatal(err)
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
	return os.NewFile(uintptr(fd), "output-test-duplicate")
}

func outputReopen(t *testing.T, file *os.File, access int) *os.File {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd int
	var callErr error
	if err := raw.Control(func(old uintptr) {
		fd, callErr = unix.Open(fmt.Sprintf("/proc/self/fd/%d", old), access|unix.O_CLOEXEC, 0)
	}); err != nil {
		t.Fatal(err)
	}
	if callErr != nil {
		t.Fatal(callErr)
	}
	return os.NewFile(uintptr(fd), "output-test-reopened")
}

// origin: hot-update S2 plan §4 item 1 / §5.1.
// A revoked task whose log is full is aborted by the runtime (S1 P2). That
// stream must still be exportable: its retained prefix and End are exactly
// what the next image serves, and refusing it would make every such task a
// permanent "cannot upgrade" reason. Before S2 Freeze refused any paused
// actor and would even have closed the pause channel twice (panic).
func TestOutputFreezeAfterAbortExportsEndedStream(t *testing.T) {
	out, log, writer := outputFixture(t, 64)
	want := outputBytes(64 + 4096)
	written := outputStartWriter(writer, want)
	outputAwait(t, func() bool { return log.Snapshot().End == 64 })
	out.Abort("revoked: fixture")
	outputAwait(t, func() bool {
		out.mu.Lock()
		defer out.mu.Unlock()
		return !out.running
	})
	state, files := outputFreeze(t, out)
	if state.ReadError != "revoked: fixture" || state.Replay.FIN || state.Replay.End != 64 || state.Discarding {
		t.Fatalf("aborted stream exported wrong state: %+v", state)
	}
	if files["source"] == nil || files["replay"] == nil {
		t.Fatalf("aborted stream exported without both files: %v", files)
	}
	if _, _, err := out.Freeze(context.Background()); err == nil {
		t.Fatal("second Freeze of a frozen aborted stream succeeded")
	}
	retained := make([]byte, 64)
	if n, err := log.Read(0, retained); err != nil || n != 64 || !bytes.Equal(retained, want[:64]) {
		t.Fatalf("aborted prefix lost across freeze: n=%d err=%v", n, err)
	}
	// Restore side: an ended stream must not grow a resident pump.
	dupSource := outputDuplicate(t, files["source"])
	dupLog := outputDuplicate(t, files["replay"])
	restoredLog, err := RestoreReplay(&ReplayBudget{}, state.Replay, dupLog)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreOutput(state, dupSource, restoredLog)
	if err != nil {
		t.Fatal(err)
	}
	restored.Thaw()
	outputAwait(t, func() bool {
		restored.mu.Lock()
		defer restored.mu.Unlock()
		return !restored.running
	})
	if got := restored.Snapshot(); got.ReadError != state.ReadError || got.Replay != state.Replay {
		t.Fatalf("restored aborted stream changed: %+v", got)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	out.Thaw() // the old owner only thaws to close cleanly; the writer is still blocked on a full pipe
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-written:
	case <-time.After(outputTestBudget):
		t.Fatal("original writer never released after both owners closed")
	}
}

// origin: hot-update S2 plan D11 / §22.4 item 8.
// Discard is the running-abandoned bound: the stream ends for its consumer
// at the current End (aborted contract: Error set, never a FIN), the retained
// prefix stays readable, and — unlike Abort — the pump keeps draining the
// source so a child that was blocked on a full pipe writes again. Freeze
// carries the marker; a thawed restore keeps draining.
func TestOutputDiscardKeepsDrainingWithoutRetaining(t *testing.T) {
	out, log, writer := outputFixture(t, 64)
	pipeSize := outputFDValue(t, writer, unix.F_GETPIPE_SZ)
	want := outputBytes(64 + pipeSize + 4096)
	written := make(chan outputWriteResult, 1)
	go func() { n, err := writer.Write(want); written <- outputWriteResult{n, err} }() // keeps its end open
	outputAwait(t, func() bool { return log.Snapshot().End == 64 })
	select {
	case got := <-written:
		t.Fatalf("writer not blocked before discard: %+v", got)
	default:
	}
	out.Discard("abandoned: fixture")
	// The child's blocked write completes although nobody ACKed a byte.
	outputRequireWriter(t, written, len(want))
	state := out.Snapshot()
	if !state.Discarding || state.ReadError != "abandoned: fixture" || state.Replay.FIN || state.Replay.End != 64 || state.Replay.Base != 0 {
		t.Fatalf("discarding stream state: %+v", state)
	}
	retained := make([]byte, 64)
	if n, err := log.Read(0, retained); err != nil || n != 64 || !bytes.Equal(retained, want[:64]) {
		t.Fatalf("retained prefix changed by discard: n=%d err=%v", n, err)
	}
	// The writer closes its end: the draining pump sees EOF and retires
	// without fabricating a FIN.
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	outputAwait(t, func() bool {
		out.mu.Lock()
		defer out.mu.Unlock()
		return !out.running
	})
	if got := out.Snapshot(); got.Replay.FIN || got.Discarding || got.ReadError != "abandoned: fixture" {
		t.Fatalf("drained discard stream fabricated an end: %+v", got)
	}
	out.Abort("late") // an ended stream is left alone
	if got := out.Snapshot(); got.ReadError != "abandoned: fixture" {
		t.Fatalf("abort overwrote a discard: %+v", got)
	}

	// Second actor: discard while the pump is blocked in Read (pipe empty),
	// then freeze mid-discard and restore: the marker survives and the
	// restored pump drains a late write instead of retaining it.
	out2, log2, writer2 := outputFixture(t, 64)
	if _, err := writer2.Write(outputBytes(64)); err != nil {
		t.Fatal(err)
	}
	outputAwait(t, func() bool { return log2.Snapshot().End == 64 })
	out2.Discard("abandoned: second")
	state2, files2 := outputFreeze(t, out2)
	if !state2.Discarding || state2.ReadError != "abandoned: second" || state2.Replay.End != 64 {
		t.Fatalf("frozen discarding stream: %+v", state2)
	}
	restoredLog, err := RestoreReplay(&ReplayBudget{}, state2.Replay, outputDuplicate(t, files2["replay"]))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreOutput(state2, outputDuplicate(t, files2["source"]), restoredLog)
	if err != nil {
		t.Fatal(err)
	}
	bad := state2
	bad.Discarding, bad.ReadError = true, ""
	if _, err := RestoreOutput(bad, files2["source"], restoredLog); err == nil {
		t.Fatal("RestoreOutput accepted discarding without a reason")
	}
	restored.Thaw()
	late := outputBytes(pipeSize + 4096)
	lateWritten := outputStartWriter(writer2, late)
	outputRequireWriter(t, lateWritten, len(late)) // drained, not retained
	if got := restored.Snapshot(); got.Replay.End != 64 || !got.Discarding && got.ReadError == "" {
		t.Fatalf("restored discarding pump retained bytes: %+v", got)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	out2.Thaw()
}

// origin: hot-update S2 plan §4 item 2.
// Detach is the salvage path of a failed import: every descriptor goes back
// to the caller open and untouched, the budget reservation is released, and
// the actors are closed. A live pump must be refused — it would race the
// next owner on the shared open-file description.
func TestReplayOutputInputDetachKeepFilesOpen(t *testing.T) {
	var budget ReplayBudget
	out, log, writer := outputFixture(t, 128)
	if _, _, ok := out.Detach(); ok {
		t.Fatal("Detach accepted a live output pump")
	}
	if _, _, err := log.Detach(); err == nil {
		t.Fatal("Detach accepted a live replay log")
	}
	state, files := outputFreeze(t, out)
	restoredLog, err := RestoreReplay(&budget, state.Replay, outputDuplicate(t, files["replay"]))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreOutput(state, outputDuplicate(t, files["source"]), restoredLog)
	if err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 128 {
		t.Fatalf("restore reserved %d, want 128", budget.Used())
	}
	source, detachedLog, ok := restored.Detach()
	if !ok || source == nil || detachedLog != restoredLog {
		t.Fatal("Detach of a restored output refused or returned foreign resources")
	}
	file, replayState, err := detachedLog.Detach()
	if err != nil || file == nil || replayState != state.Replay {
		t.Fatalf("Detach of the restored log: file=%v state=%+v err=%v", file, replayState, err)
	}
	if budget.Used() != 0 {
		t.Fatalf("detach kept %d reserved", budget.Used())
	}
	for name, f := range map[string]*os.File{"source": source, "replay": file} {
		if _, err := f.Stat(); err != nil {
			t.Fatalf("detached %s closed: %v", name, err)
		}
		if outputFDValue(t, f, unix.F_GETFD)&unix.FD_CLOEXEC == 0 {
			t.Fatalf("detached %s lost CLOEXEC", name)
		}
	}
	if err := restored.Close(); err != nil {
		t.Fatalf("Close after Detach: %v", err)
	}
	if err := detachedLog.Close(); err != nil {
		t.Fatalf("Close after Detach: %v", err)
	}
	if _, _, err := detachedLog.Detach(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second Detach: %v", err)
	}
	_ = source.Close()
	_ = file.Close()
	_ = writer.Close()
	out.Thaw()

	// Input half: a restored input with a live target detaches both.
	reader, target0, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	inLog, err := NewReplay(&ReplayBudget{}, 128)
	if err != nil {
		t.Fatal(err)
	}
	in, err := NewInput(target0, inLog)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := in.Detach(); ok {
		t.Fatal("Detach accepted a live input pump")
	}
	ctx, cancel := context.WithTimeout(context.Background(), outputTestBudget)
	defer cancel()
	inState, inFiles, err := in.Freeze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restoredInLog, err := RestoreReplay(&budget, inState.Replay, outputDuplicate(t, inFiles["replay"]))
	if err != nil {
		t.Fatal(err)
	}
	restoredIn, err := RestoreInput(inState, outputDuplicate(t, inFiles["target"]), restoredInLog)
	if err != nil {
		t.Fatal(err)
	}
	target, gotLog, ok := restoredIn.Detach()
	if !ok || target == nil || gotLog != restoredInLog {
		t.Fatal("Detach of a restored input refused or returned foreign resources")
	}
	if _, err := target.Stat(); err != nil {
		t.Fatalf("detached target closed: %v", err)
	}
	if _, _, err := gotLog.Detach(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatalf("input detach kept %d reserved", budget.Used())
	}
	_ = target.Close()
	_ = reader.Close()
	_ = restoredIn.Close()
	in.Thaw()
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
}
