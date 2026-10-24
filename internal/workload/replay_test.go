//go:build linux

// Real: memfd bytes, seals, file flags, capacity accounting and freeze barriers.
// Stubbed: no agent, remote ACK transport or crash-persistent journal.
// The model is a linear retained byte slice; it does not mirror ring indexing.
// origin: docs/reviews/hot-update-plan.md §4.2 item 6.
package workload_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

func replayFileControl(t *testing.T, file *os.File, fn func(int) error) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var inner error
	if err := raw.Control(func(fd uintptr) { inner = fn(int(fd)) }); err != nil {
		t.Fatal(err)
	}
	if inner != nil {
		t.Fatal(inner)
	}
}

func duplicateReplayFile(t *testing.T, file *os.File) *os.File {
	t.Helper()
	var duplicate int
	replayFileControl(t, file, func(fd int) error {
		var err error
		duplicate, err = unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 3)
		return err
	})
	return os.NewFile(uintptr(duplicate), "replay-test-duplicate")
}

func makeReplayDescriptor(t *testing.T, size int64, seals int) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("replay-test", unix.MFD_ALLOW_SEALING|unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "replay-test")
	t.Cleanup(func() { _ = file.Close() })
	if err := unix.Ftruncate(fd, size); err != nil {
		t.Fatal(err)
	}
	if seals != 0 {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
			t.Fatal(err)
		}
	}
	return file
}

func TestReplayMatchesLinearModelAcrossWraps(t *testing.T) {
	for _, capacity := range []int64{1, 2, 7, 64, 4097} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			log, err := workload.NewReplay(&workload.ReplayBudget{Limit: capacity}, capacity)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			rng := rand.New(rand.NewSource(7331 + capacity))
			var retained []byte
			var base, end uint64
			fin := false
			for step := 0; step < 1400; step++ {
				if step == 1350 {
					if err := log.Finish(); err != nil {
						t.Fatal(err)
					}
					fin = true
				}
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4:
					data := make([]byte, rng.Intn(int(capacity)*2+1))
					if _, err := rng.Read(data); err != nil {
						t.Fatal(err)
					}
					n, err := log.Append(data)
					if fin {
						if n != 0 || err == nil {
							t.Fatalf("append after FIN = %d,%v", n, err)
						}
					} else {
						want := min(len(data), int(capacity)-len(retained))
						if n != want || (want < len(data) && !errors.Is(err, workload.ErrBackpressure)) || (want == len(data) && err != nil) {
							t.Fatalf("step %d append=%d,%v want prefix=%d", step, n, err, want)
						}
						retained = append(retained, data[:want]...)
						end += uint64(want)
					}
				case 5, 6:
					offset := base + uint64(rng.Intn(len(retained)+2))
					if rng.Intn(4) == 0 && base > 0 {
						offset = base - 1
					}
					err := log.Ack(offset)
					if offset > end {
						if !errors.Is(err, workload.ErrReplayOffset) {
							t.Fatalf("future ACK: %v", err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if offset > base {
							retained = append([]byte(nil), retained[offset-base:]...)
							base = offset
						}
					}
				default:
					offset := base + uint64(rng.Intn(len(retained)+2))
					if rng.Intn(4) == 0 && base > 0 {
						offset = base - 1
					}
					dst := bytes.Repeat([]byte{0xa5}, rng.Intn(int(capacity)*2+1))
					n, err := log.Read(offset, dst)
					if offset < base || offset > end {
						if n != 0 || !errors.Is(err, workload.ErrReplayOffset) {
							t.Fatalf("invalid read: %d,%v", n, err)
						}
					} else {
						want := min(len(dst), int(end-offset))
						eof := fin && offset+uint64(want) == end
						if n != want || (eof && !errors.Is(err, io.EOF)) || (!eof && err != nil) || !bytes.Equal(dst[:n], retained[offset-base:offset-base+uint64(want)]) {
							t.Fatalf("step %d offset %d read=%d,%v want=%d eof=%v", step, offset, n, err, want, eof)
						}
						if !bytes.Equal(dst[n:], bytes.Repeat([]byte{0xa5}, len(dst)-n)) {
							t.Fatal("read overwrote beyond reported bytes")
						}
					}
				}
				state := log.Snapshot()
				if state.Base != base || state.End != end || state.FIN != fin || state.Capacity != capacity {
					t.Fatalf("step %d state=%+v want base=%d end=%d FIN=%v", step, state, base, end, fin)
				}
			}
			if end < uint64(capacity)*10 {
				t.Fatalf("model never exercised enough wraps: end=%d capacity=%d", end, capacity)
			}
		})
	}
}

func TestReplayFreezePartialAppendAndFIN(t *testing.T) {
	budget := &workload.ReplayBudget{Limit: 8}
	log, err := workload.NewReplay(budget, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	_, firstWake := log.Observe()
	if n, err := log.Append([]byte("abcdef")); n != 6 || err != nil {
		t.Fatalf("append=%d,%v", n, err)
	}
	select {
	case <-firstWake:
	default:
		t.Fatal("append did not wake observer")
	}
	if err := log.Ack(3); err != nil {
		t.Fatal(err)
	}
	if n, err := log.Append([]byte("0123456789")); n != 5 || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("partial append=%d,%v", n, err)
	}
	state, file, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := log.Freeze(); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("second freeze=%v", err)
	}
	if n, err := log.Append([]byte("x")); n != 0 || !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen append=%d,%v", n, err)
	}
	if err := log.Ack(state.End); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen ack=%v", err)
	}
	if err := log.Finish(); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("frozen FIN=%v", err)
	}
	if log.Snapshot() != state {
		t.Fatal("frozen state mutated")
	}
	dst := make([]byte, 16)
	if n, err := log.Read(3, dst); n != 8 || err != nil || string(dst[:n]) != "def01234" {
		t.Fatalf("wrapped frozen bytes=%q err=%v", dst[:n], err)
	}
	replayFileControl(t, file, func(fd int) error {
		seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
		if err != nil {
			return err
		}
		if seals&(unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL) != unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL {
			return fmt.Errorf("missing fixed-size seals: %x", seals)
		}
		if err := unix.Ftruncate(fd, 9); err == nil {
			return fmt.Errorf("sealed replay grew")
		}
		if err := unix.Ftruncate(fd, 7); err == nil {
			return fmt.Errorf("sealed replay shrank")
		}
		return nil
	})
	log.Thaw()
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	if n, err := log.Read(3, dst); n != 8 || !errors.Is(err, io.EOF) || string(dst[:n]) != "def01234" {
		t.Fatalf("FIN lost bytes: %d,%v,%q", n, err, dst[:n])
	}
	if err := log.Ack(state.End); err != nil {
		t.Fatal(err)
	}
	if n, err := log.Read(state.End, dst); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty FIN=%d,%v", n, err)
	}
	_, closeWake := log.Observe()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closeWake:
	default:
		t.Fatal("close did not wake observer")
	}
	if _, err := log.Append(nil); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed append=%v", err)
	}
	if _, err := log.Read(state.End, dst); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed read=%v", err)
	}
	if err := log.Ack(state.End); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed ack=%v", err)
	}
	if err := log.Finish(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed FIN=%v", err)
	}
	replacement, err := workload.NewReplay(budget, 8)
	if err != nil {
		t.Fatalf("close did not release full reservation: %v", err)
	}
	_ = replacement.Close()
}

func TestReplayRestoreRejectsInvalidStateAndDescriptors(t *testing.T) {
	const capacity = int64(16)
	const seals = unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	for _, kind := range []string{"version", "backwards", "overfull", "zero-cap", "over-cap", "nil", "closed", "regular", "pipe", "size", "unsealed", "write-sealed", "future-write-sealed", "read-only", "append", "inheritable"} {
		t.Run(kind, func(t *testing.T) {
			state := workload.ReplayState{Version: 1, Capacity: capacity, Base: 1, End: 7}
			var file *os.File
			switch kind {
			case "nil":
			case "regular":
				var err error
				file, err = os.Create(filepath.Join(t.TempDir(), "ordinary"))
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Truncate(capacity); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close() })
			case "pipe":
				var writer *os.File
				var err error
				file, writer, err = os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = file.Close(); _ = writer.Close() })
			case "size":
				file = makeReplayDescriptor(t, capacity+1, seals)
			case "unsealed":
				file = makeReplayDescriptor(t, capacity, 0)
			case "write-sealed":
				file = makeReplayDescriptor(t, capacity, seals|unix.F_SEAL_WRITE)
			case "future-write-sealed":
				file = makeReplayDescriptor(t, capacity, seals|unix.F_SEAL_FUTURE_WRITE)
			default:
				file = makeReplayDescriptor(t, capacity, seals)
			}
			switch kind {
			case "version":
				state.Version++
			case "backwards":
				state.Base = state.End + 1
			case "overfull":
				state.End = state.Base + uint64(capacity) + 1
			case "zero-cap":
				state.Capacity = 0
			case "over-cap":
				state.Capacity = workload.DefaultReplayCapacity + 1
			case "closed":
				_ = file.Close()
			case "read-only":
				var readOnly *os.File
				replayFileControl(t, file, func(fd int) error {
					var err error
					readOnly, err = os.Open(fmt.Sprintf("/proc/self/fd/%d", fd))
					return err
				})
				file = readOnly
				t.Cleanup(func() { _ = file.Close() })
			case "append":
				replayFileControl(t, file, func(fd int) error { _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFL, unix.O_APPEND); return err })
			case "inheritable":
				replayFileControl(t, file, func(fd int) error { _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); return err })
			}
			budget := &workload.ReplayBudget{Limit: capacity}
			if imported, err := workload.RestoreReplay(budget, state, file); err == nil || imported != nil {
				if imported != nil {
					_ = imported.Close()
				}
				t.Fatalf("invalid %s accepted: %v", kind, err)
			}
			if file != nil && kind != "closed" {
				if _, err := file.Stat(); err != nil {
					t.Fatalf("rejection stole caller FD: %v", err)
				}
			}
			valid, err := workload.NewReplay(budget, capacity)
			if err != nil {
				t.Fatalf("invalid restore leaked reservation: %v", err)
			}
			_ = valid.Close()
		})
	}
}

func TestReplayBudgetReservesEmptyStreamsAndReleasesOnce(t *testing.T) {
	var budget workload.ReplayBudget
	var logs []*workload.Replay
	defer func() {
		for _, log := range logs {
			_ = log.Close()
		}
	}()
	for i := int64(0); i < workload.DefaultReplayBudget/workload.DefaultReplayCapacity; i++ {
		log, err := workload.NewReplay(&budget, workload.DefaultReplayCapacity)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, log)
	}
	if extra, err := workload.NewReplay(&budget, 1); extra != nil || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("empty logs overcommitted node budget: %v", err)
	}
	for _, log := range logs {
		if err := log.Ack(0); err != nil {
			t.Fatal(err)
		}
	}
	if extra, err := workload.NewReplay(&budget, 1); extra != nil || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("ACK freed reserved capacity: %v", err)
	}
	_ = logs[0].Close()
	_ = logs[0].Close()
	one, err := workload.NewReplay(&budget, workload.DefaultReplayCapacity)
	if err != nil {
		t.Fatal(err)
	}
	logs = append(logs, one)
	if extra, err := workload.NewReplay(&budget, 1); extra != nil || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("double close released twice: %v", err)
	}
	for _, capacity := range []int64{-1, 0, workload.DefaultReplayCapacity + 1} {
		if log, err := workload.NewReplay(&workload.ReplayBudget{}, capacity); log != nil || err == nil {
			t.Fatalf("invalid capacity %d accepted", capacity)
		}
	}
	if log, err := workload.NewReplay(nil, 1); log != nil || err == nil {
		t.Fatal("missing budget accepted")
	}
	limited := &workload.ReplayBudget{Limit: 3}
	file := makeReplayDescriptor(t, 4, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	if log, err := workload.RestoreReplay(limited, workload.ReplayState{Version: 1, Capacity: 4}, file); log != nil || !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("restore exceeded budget: %v", err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatal("budget rejection stole file")
	}
	three, err := workload.NewReplay(limited, 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = three.Close()
}

func TestReplayConcurrentReservationIsBounded(t *testing.T) {
	budget := &workload.ReplayBudget{Limit: 63}
	type result struct {
		log *workload.Replay
		err error
	}
	results := make(chan result, 24)
	var ready sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 24; i++ {
		ready.Add(1)
		go func() { ready.Done(); <-start; log, err := workload.NewReplay(budget, 7); results <- result{log, err} }()
	}
	ready.Wait()
	close(start)
	var retained []*workload.Replay
	defer func() {
		for _, log := range retained {
			_ = log.Close()
		}
	}()
	for i := 0; i < 24; i++ {
		got := <-results
		if got.err == nil {
			retained = append(retained, got.log)
		} else if !errors.Is(got.err, workload.ErrBackpressure) {
			t.Fatal(got.err)
		}
	}
	if len(retained) != 9 {
		t.Fatalf("reservation winners=%d want 9", len(retained))
	}
}

func TestReplayOffsetExhaustionDoesNotWrap(t *testing.T) {
	const capacity = int64(8)
	file := makeReplayDescriptor(t, capacity, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	state := workload.ReplayState{Version: 1, Capacity: capacity, Base: math.MaxUint64 - 3, End: math.MaxUint64 - 3}
	log, err := workload.RestoreReplay(&workload.ReplayBudget{Limit: capacity}, state, file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	log.Thaw()
	if n, err := log.Append([]byte("abc")); n != 3 || err != nil {
		t.Fatalf("last representable bytes=%d,%v", n, err)
	}
	before := log.Snapshot()
	if n, err := log.Append([]byte("d")); n != 0 || err == nil {
		t.Fatalf("offset wrapped: %d,%v", n, err)
	}
	if log.Snapshot() != before {
		t.Fatal("overflow mutated state")
	}
	dst := make([]byte, 3)
	if n, err := log.Read(state.Base, dst); n != 3 || err != nil || string(dst) != "abc" {
		t.Fatalf("near-limit retained bytes=%q,%d,%v", dst, n, err)
	}
}

func TestReplayConcurrentFreezePreservesAcceptedPrefix(t *testing.T) {
	log, err := workload.NewReplay(&workload.ReplayBudget{Limit: 4096}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	first := make(chan struct{})
	resume := make(chan struct{})
	type result struct {
		accepted int
		err      error
	}
	done := make(chan result, 1)
	go func() {
		accepted := 0
		for accepted < 2048 {
			n, err := log.Append(replaySequence(uint64(accepted), 1))
			accepted += n
			if err != nil {
				done <- result{accepted, err}
				return
			}
			if accepted == 1 {
				close(first)
				<-resume
			}
		}
		done <- result{accepted: accepted}
	}()
	// At least one byte was accepted before freeze, and the remaining producer
	// competes for the real mutex. Either complete prefix is legitimate.
	select {
	case <-first:
	case got := <-done:
		t.Fatalf("producer failed before exercising freeze: %+v", got)
	case <-time.After(5 * time.Second):
		t.Fatal("producer never entered append")
	}
	close(resume)
	state, _, err := log.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.accepted < 1 || uint64(got.accepted) != state.End || (got.err != nil && !errors.Is(got.err, workload.ErrFrozen)) {
			t.Fatalf("freeze omitted an accepted byte: state=%+v producer=%+v", state, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("frozen producer did not settle")
	}
	checkReplaySequence(t, log, state)
	if got := log.Snapshot(); got != state {
		t.Fatalf("producer mutated state after freeze: got=%+v want=%+v", got, state)
	}
}

func exerciseReplayRestore(t *testing.T) {
	t.Helper()
	old, err := workload.NewReplay(&workload.ReplayBudget{Limit: 17}, 17)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = old.Close() }()
	if n, err := old.Append([]byte("abcdefghijklmnopq")); n != 17 || err != nil {
		t.Fatal(n, err)
	}
	if err := old.Ack(13); err != nil {
		t.Fatal(err)
	}
	if n, err := old.Append([]byte("0123456789XYZ")); n != 13 || err != nil {
		t.Fatal(n, err)
	}
	state, borrowed, err := old.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	owned := duplicateReplayFile(t, borrowed)
	defer func() { _ = owned.Close() }()
	next, err := workload.RestoreReplay(&workload.ReplayBudget{Limit: 17}, state, owned)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = next.Close() }()
	if _, err := next.Append(nil); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("restore activated itself: %v", err)
	}
	_ = old.Close()
	next.Thaw()
	dst := make([]byte, 17)
	if n, err := next.Read(state.Base, dst); n != 17 || err != nil || string(dst) != "nopq0123456789XYZ" {
		t.Fatalf("transferred bytes=%q,%d,%v", dst, n, err)
	}
	if err := next.Ack(state.End); err != nil {
		t.Fatal(err)
	}
	if n, err := next.Append([]byte("continued")); n != 9 || err != nil {
		t.Fatal(n, err)
	}
	if err := next.Finish(); err != nil {
		t.Fatal(err)
	}
	if n, err := next.Read(state.End, dst); n != 9 || !errors.Is(err, io.EOF) || string(dst[:n]) != "continued" {
		t.Fatalf("restored owner did not continue: %q,%d,%v", dst[:n], n, err)
	}
}

func TestReplayRepeatedRestoreReleasesResources(t *testing.T) {
	exerciseReplayRestore(t)
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseReplayRestore(t)
	}
	testharness.AssertNoGoroutineLeak(t, "replay freeze/restore", goroutines)
	testharness.AssertNoFDLeak(t, "replay freeze/restore", fds, 0)
}

// origin: hot-update S2 plan §4 item 4.
// Used is the observation point of the image budget-conservation invariant;
// it must move with reserve/release only, and a refused reservation must not
// count.
func TestReplayBudgetUsedTracksReserveAndRelease(t *testing.T) {
	budget := &workload.ReplayBudget{Limit: 3 * 1024}
	if budget.Used() != 0 {
		t.Fatal("fresh budget reports usage")
	}
	first, err := workload.NewReplay(budget, 1024)
	if err != nil {
		t.Fatal(err)
	}
	state, file, err := first.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	second, err := workload.RestoreReplay(budget, state, duplicateReplayFile(t, file))
	if err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 2048 {
		t.Fatalf("used after New+Restore = %d, want 2048", budget.Used())
	}
	if _, err := workload.NewReplay(budget, 2048); !errors.Is(err, workload.ErrBackpressure) {
		t.Fatalf("over-limit reservation: %v", err)
	}
	if budget.Used() != 2048 {
		t.Fatalf("refused reservation changed usage: %d", budget.Used())
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 1024 {
		t.Fatalf("used after Close = %d, want 1024", budget.Used())
	}
	if _, _, err := second.Detach(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatalf("used after Detach = %d, want 0", budget.Used())
	}
	_ = file.Close()
}
