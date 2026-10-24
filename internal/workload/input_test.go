//go:build linux

// Real: kernel pipes, partial writes, replay backing files, deadlines and FD ownership.
// Stubbed: network delivery, request admission, and application upgrade coordination.
// These tests exercise the input owner, not end-to-end hot update.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const inputTestBudget = 10 * time.Second

func TestInputFreezeRetainsPartialWriteAndOriginalEOF(t *testing.T) {
	exerciseInputPartialWrite(t)
}

func exerciseInputPartialWrite(t *testing.T) {
	t.Helper()
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	payload := inputPayload(64 << 10)
	log, err := workload.NewReplay(&workload.ReplayBudget{}, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if n, err := log.Append(payload); n != len(payload) || err != nil {
		t.Fatalf("retain source bytes: n=%d err=%v", n, err)
	}
	pump, err := workload.NewInput(writer, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	inputAwait(t, func() bool { return inputQueued(t, reader) == 4096 })
	ctx, cancel := context.WithTimeout(t.Context(), inputTestBudget)
	defer cancel()
	state, files, err := pump.Freeze(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The original write was larger than PIPE_BUF and blocked after filling
	// the pipe. Its interrupted result must ACK exactly the applied prefix.
	if state.Replay.Base != 4096 || state.Replay.End != uint64(len(payload)) ||
		state.EOF || state.WriteError != "" || files["target"] != writer || len(files) != 2 {
		t.Fatalf("partial-write ownership changed: %+v files=%v", state, files)
	}
	if got := inputQueued(t, reader); uint64(got) != state.Replay.Base {
		t.Fatalf("applied offset %d differs from original pipe bytes %d", state.Replay.Base, got)
	}
	retained := make([]byte, len(payload)-int(state.Replay.Base))
	if n, err := log.Read(state.Replay.Base, retained); n != len(retained) || err != nil || !bytes.Equal(retained, payload[state.Replay.Base:]) {
		t.Fatalf("unwritten suffix lost: n=%d err=%v", n, err)
	}
	if err := log.Finish(); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen input accepted EOF: %v", err)
	}
	pump.Thaw()
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := reader.SetReadDeadline(time.Now().Add(inputTestBudget)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("original reader received loss/duplication or missing EOF: got=%d want=%d err=%v", len(got), len(payload), err)
	}
	inputAwait(t, func() bool { return pump.Snapshot().EOF })
	final := pump.Snapshot()
	if final.WriteError != "" || !final.Replay.FIN || final.Replay.Base != final.Replay.End {
		t.Fatalf("EOF receipt disagrees with delivered bytes: %+v", final)
	}
}

func TestInputCanceledFreezeKeepsOriginalWriterUsable(t *testing.T) {
	for _, during := range []bool{false, true} {
		name := "before-freeze"
		if during {
			name = "waiting-for-actor"
		}
		t.Run(name, func(t *testing.T) { exerciseInputCanceledFreeze(t, during) })
	}
}

func exerciseInputCanceledFreeze(t *testing.T, during bool) {
	t.Helper()
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	pump, err := workload.NewInput(writer, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := base
	if during {
		// Cancel precisely when Freeze evaluates its wait select, after it
		// has paused the original actor. All pipe I/O remains real.
		ctx = &inputCancelOnDone{Context: base, cancel: cancel}
	} else {
		cancel()
	}
	if _, files, err := pump.Freeze(ctx); !errors.Is(err, context.Canceled) || files != nil {
		t.Fatalf("canceled freeze transferred ownership: files=%v err=%v", files, err)
	}
	payload := []byte("same writer after canceled freeze")
	if _, err := log.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	_ = reader.SetReadDeadline(time.Now().Add(inputTestBudget))
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("canceled freeze stranded original owner: %q %v", got, err)
	}
}

type inputCancelOnDone struct {
	context.Context
	cancel context.CancelFunc
}

func (c *inputCancelOnDone) Done() <-chan struct{} {
	c.cancel()
	return c.Context.Done()
}

func TestInputRestoreDeliveredEOFHasNoWriter(t *testing.T) {
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	pump, err := workload.NewInput(writer, log)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	inputAwait(t, func() bool { return pump.Snapshot().EOF })
	state, files, err := pump.Freeze(t.Context())
	if err != nil || !state.EOF || len(files) != 1 || files["target"] != nil {
		t.Fatalf("delivered EOF retained a phantom writer: %+v %v %v", state, files, err)
	}
	copyFile := inputDup(t, files["replay"])
	restoredLog, err := workload.RestoreReplay(&workload.ReplayBudget{}, state.Replay, copyFile)
	if err != nil {
		_ = copyFile.Close()
		t.Fatal(err)
	}
	defer func() { _ = restoredLog.Close() }()
	restored, err := workload.RestoreInput(state, nil, restoredLog)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restored.Close() }()
	restored.Thaw()
	_ = reader.SetReadDeadline(time.Now().Add(inputTestBudget))
	got, err := io.ReadAll(reader)
	if err != nil || len(got) != 0 || !restored.Snapshot().EOF {
		t.Fatalf("EOF import created bytes or delayed EOF: %q %v", got, err)
	}
}

func TestInputRejectsWrongDirectionWithoutTakingOwnership(t *testing.T) {
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if pump, err := workload.NewInput(reader, log); err == nil || pump != nil {
		if pump != nil {
			_ = pump.Close()
		}
		t.Fatalf("read-only pipe gained input ownership: %v", err)
	}
	state, _, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if pump, err := workload.RestoreInput(workload.InputState{Version: 1, Replay: state}, reader, log); err == nil || pump != nil {
		if pump != nil {
			_ = pump.Close()
		}
		t.Fatalf("restored read-only pipe gained input ownership: %v", err)
	}
	if _, err := writer.Write([]byte("kept")); err != nil {
		t.Fatalf("rejection closed caller pipe: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != "kept" {
		t.Fatalf("rejection changed caller resources: %q %v", got, err)
	}
	log.Thaw()
	if _, err := log.Append([]byte("usable")); err != nil {
		t.Fatalf("rejection consumed caller replay: %v", err)
	}
}

func TestInputRestoreRejectsInvalidStateWithoutClosingFiles(t *testing.T) {
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	rs, _, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*workload.InputState)
	}{
		{"version", func(s *workload.InputState) { s.Version++ }},
		{"mismatched-offset", func(s *workload.InputState) { s.Replay.End++ }},
		{"premature-eof", func(s *workload.InputState) { s.EOF = true }},
		{"oversized-error", func(s *workload.InputState) { s.WriteError = string(make([]byte, 4097)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := workload.InputState{Version: 1, Replay: rs}
			tc.edit(&state)
			if pump, err := workload.RestoreInput(state, writer, log); err == nil || pump != nil {
				t.Fatalf("invalid input state adopted: %+v %v", state, err)
			}
			if _, err := writer.Write([]byte("x")); err != nil {
				t.Fatalf("invalid import closed original destination: %v", err)
			}
			var got [1]byte
			if _, err := io.ReadFull(reader, got[:]); err != nil || got[0] != 'x' {
				t.Fatalf("invalid import damaged pipe: %q %v", got, err)
			}
		})
	}
	if pump, err := workload.RestoreInput(workload.InputState{Version: 1, Replay: rs}, nil, log); pump != nil || err == nil {
		t.Fatal("nonterminal input restored without its original writer")
	}
}

func TestInputDescriptorFlagsRejectedWithoutMutation(t *testing.T) {
	for _, kind := range []string{"inheritable", "blocking"} {
		t.Run(kind, func(t *testing.T) {
			reader, writer := inputPipe(t)
			defer func() { _ = reader.Close() }()
			defer func() { _ = writer.Close() }()
			var wantStatus, wantDescriptor int
			inputControl(t, writer, func(fd uintptr) {
				var err error
				wantStatus, err = unix.FcntlInt(fd, unix.F_GETFL, 0)
				if err != nil {
					t.Fatal(err)
				}
				wantDescriptor, err = unix.FcntlInt(fd, unix.F_GETFD, 0)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "inheritable":
					wantDescriptor &^= unix.FD_CLOEXEC
					_, err = unix.FcntlInt(fd, unix.F_SETFD, wantDescriptor)
				case "blocking":
					wantStatus &^= unix.O_NONBLOCK
					_, err = unix.FcntlInt(fd, unix.F_SETFL, wantStatus)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			log, err := workload.NewReplay(&workload.ReplayBudget{}, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			if pump, err := workload.NewInput(writer, log); pump != nil || err == nil {
				if pump != nil {
					_ = pump.Close()
				}
				t.Fatalf("incompatible %s writer adopted: %v", kind, err)
			}
			state, _, err := log.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			if pump, err := workload.RestoreInput(workload.InputState{Version: 1, Replay: state}, writer, log); pump != nil || err == nil {
				if pump != nil {
					_ = pump.Close()
				}
				t.Fatalf("incompatible %s writer restored: %v", kind, err)
			}
			inputControl(t, writer, func(fd uintptr) {
				status, e1 := unix.FcntlInt(fd, unix.F_GETFL, 0)
				descriptor, e2 := unix.FcntlInt(fd, unix.F_GETFD, 0)
				if e1 != nil || e2 != nil || status != wantStatus || descriptor != wantDescriptor {
					t.Fatalf("rejection changed caller descriptor: status=%d/%d fd=%d/%d err=%v/%v", status, wantStatus, descriptor, wantDescriptor, e1, e2)
				}
			})
		})
	}
}

func TestInputRejectsSocketWithoutClosingPeer(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := os.NewFile(uintptr(fds[0]), "original-duplex-endpoint")
	peer := os.NewFile(uintptr(fds[1]), "original-duplex-peer")
	defer func() { _ = endpoint.Close() }()
	defer func() { _ = peer.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if pump, err := workload.NewInput(endpoint, log); pump != nil || err == nil {
		if pump != nil {
			_ = pump.Close()
		}
		t.Fatalf("process input took ownership of a duplex socket: %v", err)
	}
	state, _, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if pump, err := workload.RestoreInput(workload.InputState{Version: 1, Replay: state}, endpoint, log); pump != nil || err == nil {
		if pump != nil {
			_ = pump.Close()
		}
		t.Fatalf("restored process input took ownership of a duplex socket: %v", err)
	}
	// Both directions must remain usable; rejecting a pipe-only FIN adapter
	// must never manufacture full-close or half-close on a caller's socket.
	for _, pair := range [][2]*os.File{{endpoint, peer}, {peer, endpoint}} {
		if _, err := pair[0].Write([]byte("kept")); err != nil {
			t.Fatal(err)
		}
		_ = pair[1].SetReadDeadline(time.Now().Add(inputTestBudget))
		var got [4]byte
		if _, err := io.ReadFull(pair[1], got[:]); err != nil || string(got[:]) != "kept" {
			t.Fatalf("rejection closed a direction of the original socket: %q %v", got, err)
		}
	}
}

func TestInputBrokenPipeRetainsUndeliveredSuffixWithoutEOF(t *testing.T) {
	reader, writer := inputPipe(t)
	_ = reader.Close()
	defer func() { _ = writer.Close() }()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	payload := []byte("not delivered to a closed reader")
	if _, err := log.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	pump, err := workload.NewInput(writer, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	inputAwait(t, func() bool { return pump.Snapshot().WriteError != "" })
	state, _, err := pump.Freeze(t.Context())
	if err != nil || state.WriteError == "" || state.EOF || state.Replay.Base != 0 || state.Replay.End != uint64(len(payload)) {
		t.Fatalf("broken pipe fabricated delivery or EOF: %+v %v", state, err)
	}
	got := make([]byte, len(payload))
	if n, err := log.Read(0, got); n != len(payload) || !errors.Is(err, io.EOF) || !bytes.Equal(got, payload) {
		t.Fatalf("broken-pipe input lost responsibility for accepted bytes: n=%d err=%v", n, err)
	}
}

func TestInputRepeatedLifecycleReleasesResources(t *testing.T) {
	exerciseInputPartialWrite(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	const rounds = 5
	for i := 0; i < rounds; i++ {
		exerciseInputPartialWrite(t)
	}
	testharness.AssertNoGoroutineLeak(t, "input partial-write freeze/thaw", beforeG)
	testharness.AssertNoFDLeak(t, "input partial-write freeze/thaw", beforeFD, 0)
}

func inputPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	inputControl(t, writer, func(fd uintptr) {
		if n, err := unix.FcntlInt(fd, unix.F_SETPIPE_SZ, 4096); err != nil || n != 4096 {
			_ = reader.Close()
			t.Fatalf("set bounded pipe: size=%d err=%v", n, err)
		}
	})
	return reader, writer
}

func inputQueued(t *testing.T, file *os.File) int {
	t.Helper()
	var queued int
	inputControl(t, file, func(fd uintptr) {
		var err error
		queued, err = unix.IoctlGetInt(int(fd), unix.TIOCINQ)
		if err != nil {
			t.Fatal(err)
		}
	})
	return queued
}

func inputControl(t *testing.T, file *os.File, fn func(uintptr)) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(fn); err != nil {
		t.Fatal(err)
	}
}

func inputDup(t *testing.T, file *os.File) *os.File {
	t.Helper()
	var duplicate int
	inputControl(t, file, func(fd uintptr) {
		var err error
		duplicate, err = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3)
		if err != nil {
			t.Fatal(err)
		}
	})
	return os.NewFile(uintptr(duplicate), "input-test-owned-duplicate")
}

func inputAwait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(inputTestBudget)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("input condition did not settle")
		case <-tick.C:
		}
	}
}

func inputPayload(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((i*37 + i/251) % 256)
	}
	return data
}
