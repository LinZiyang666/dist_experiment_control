//go:build linux

package sealedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// These fixtures construct the envelope independently of New: a writer and
// reader sharing the same format defect must not certify each other.
func stateEnvelope(data []byte) []byte {
	header := []byte{'T', 'E', 'T', 'H', 'S', 'S', '5', 0, 0, 0, 0, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[12:], uint32(len(data)))
	return append(header, data...)
}

func stateDescriptor(t *testing.T, blob []byte, seals int) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("sealed-state-test", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "sealed-state-test")
	t.Cleanup(func() { _ = file.Close() })
	if _, err := file.WriteAt(blob, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
		t.Fatal(err)
	}
	return file
}

func stateControl(t *testing.T, file *os.File, fn func(uintptr)) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(fn); err != nil {
		t.Fatal(err)
	}
}

// Real kernel descriptors with each seal individually absent, wrong descriptor
// types/flags/owner and malformed envelopes; refusal preserves taken ownership.
// origin: hot-update S5 plan §5.1 / §12.2 P5
func TestSealedStateRejectsUnsealedDescriptor(t *testing.T) {
	const allSeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	for _, missing := range []int{allSeals, unix.F_SEAL_WRITE, unix.F_SEAL_GROW, unix.F_SEAL_SHRINK, unix.F_SEAL_SEAL} {
		t.Run("missing-seals-"+strconv.Itoa(missing), func(t *testing.T) {
			file := stateDescriptor(t, stateEnvelope([]byte{1, 2, 3}), allSeals&^missing)
			assertStateRefused(t, file, ErrInvalid)
		})
	}
	cases := []struct {
		name string
		make func(*testing.T) *os.File
		want error
	}{
		{"ordinary-file", func(t *testing.T) *os.File {
			file, err := os.CreateTemp(t.TempDir(), "ordinary-state")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close() })
			return file
		}, ErrInvalid},
		{"pipe", func(t *testing.T) *os.File {
			file, peer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close(); _ = peer.Close() })
			return file
		}, ErrInvalid},
		{"missing-cloexec", func(t *testing.T) *os.File {
			file := stateDescriptor(t, stateEnvelope([]byte{1}), allSeals)
			stateControl(t, file, func(fd uintptr) {
				if _, err := unix.FcntlInt(fd, unix.F_SETFD, 0); err != nil {
					t.Fatal(err)
				}
			})
			return file
		}, ErrInvalid},
		{"append", func(t *testing.T) *os.File {
			file := stateDescriptor(t, stateEnvelope([]byte{1}), allSeals)
			stateControl(t, file, func(fd uintptr) {
				if _, err := unix.FcntlInt(fd, unix.F_SETFL, unix.O_APPEND); err != nil {
					t.Fatal(err)
				}
			})
			return file
		}, ErrInvalid},
		{"write-only", func(t *testing.T) *os.File {
			file := stateDescriptor(t, stateEnvelope([]byte{1}), allSeals)
			var reopened *os.File
			stateControl(t, file, func(fd uintptr) {
				var err error
				reopened, err = os.OpenFile(filepath.Join("/proc/self/fd", strconv.FormatUint(uint64(fd), 10)), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
			})
			t.Cleanup(func() { _ = reopened.Close() })
			return reopened
		}, ErrInvalid},
		{"foreign-owner", func(t *testing.T) *os.File {
			file := stateDescriptor(t, stateEnvelope([]byte{1}), allSeals)
			stateControl(t, file, func(fd uintptr) {
				if err := unix.Fchown(int(fd), os.Geteuid()+1, -1); err != nil {
					if errors.Is(err, unix.EPERM) {
						t.Skip("foreign UID requires CAP_CHOWN")
					}
					t.Fatal(err)
				}
			})
			return file
		}, ErrInvalid},
		{"short-header", func(t *testing.T) *os.File { return stateDescriptor(t, []byte{1}, allSeals) }, ErrInvalid},
		{"wrong-magic", func(t *testing.T) *os.File {
			blob := stateEnvelope([]byte{1})
			blob[0] ^= 1
			return stateDescriptor(t, blob, allSeals)
		}, ErrInvalid},
		{"wrong-version", func(t *testing.T) *os.File {
			blob := stateEnvelope([]byte{1})
			blob[11] = 2
			return stateDescriptor(t, blob, allSeals)
		}, ErrInvalid},
		{"truncated", func(t *testing.T) *os.File {
			blob := stateEnvelope([]byte{1, 2})
			return stateDescriptor(t, blob[:len(blob)-1], allSeals)
		}, ErrInvalid},
		{"trailing", func(t *testing.T) *os.File { return stateDescriptor(t, append(stateEnvelope([]byte{1}), 2), allSeals) }, ErrInvalid},
		{"oversized", func(t *testing.T) *os.File {
			return stateDescriptor(t, stateEnvelope(make([]byte, MaxStateBytes)), allSeals)
		}, ErrCapacity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertStateRefused(t, tc.make(t), tc.want) })
	}
	t.Run("nil-file", func(t *testing.T) {
		if data, file, err := Read(&stateFileSource{}, "ss-state.cut", "state", MaxStateBytes); data != nil || file != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("missing descriptor granted progress: %v", err)
		}
	})
	t.Run("source-error", func(t *testing.T) {
		file := stateDescriptor(t, stateEnvelope([]byte{1}), allSeals)
		src := &stateFileSource{file: file, err: errors.New("private-material-never-in-errors")}
		if data, taken, err := Read(src, "ss-state.cut", "state", MaxStateBytes); data != nil || taken != file || !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), src.err.Error()) {
			t.Fatalf("source failure lost ownership or leaked details: %v", err)
		}
	})
}

func assertStateRefused(t *testing.T, file *os.File, want error) {
	t.Helper()
	src := &stateFileSource{file: file}
	data, taken, err := Read(src, "ss-state.cut", "state", MaxStateBytes)
	if data != nil || taken != file || !errors.Is(err, want) || src.calls != 1 {
		t.Fatalf("descriptor refusal = (bytes=%d, same-file=%t, calls=%d, err=%v), want %v", len(data), taken == file, src.calls, err, want)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("refusal destroyed the descriptor needed for salvage: %v", err)
	}
}

// origin: hot-update S5 plan §5.1 / §3.4
func TestSealedStateValidatesBeforeTakingFiles(t *testing.T) {
	for _, id := range []string{"", "ss-state:cut", "private\nmaterial", "non-ascii-λ", strings.Repeat("a", 257)} {
		if s, err := New(id, []byte{1}); s != nil || !errors.Is(err, ErrInvalid) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("invalid ID granted a snapshot: %v", err)
		}
		src := &stateFileSource{}
		if _, _, err := Read(src, id, "state", MaxStateBytes); !errors.Is(err, ErrInvalid) || src.calls != 0 {
			t.Fatalf("invalid ID reached TakeFile: calls=%d err=%v", src.calls, err)
		}
	}
	for _, name := range []string{"", "state:secret", strings.Repeat("a", 65)} {
		src := &stateFileSource{}
		if _, _, err := Read(src, "ss-state.cut", name, MaxStateBytes); !errors.Is(err, ErrInvalid) || src.calls != 0 {
			t.Fatalf("invalid file name reached TakeFile: calls=%d err=%v", src.calls, err)
		}
	}
	for _, tc := range []struct {
		limit int64
		want  error
	}{{-1, ErrInvalid}, {MaxStateBytes + 1, ErrCapacity}} {
		src := &stateFileSource{}
		if _, _, err := Read(src, "ss-state.cut", "state", tc.limit); !errors.Is(err, tc.want) || src.calls != 0 {
			t.Fatalf("invalid limit reached TakeFile: calls=%d err=%v", src.calls, err)
		}
	}
	if s, err := New("ss-state.cut", make([]byte, MaxStateBytes-16+1)); s != nil || !errors.Is(err, ErrCapacity) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("New exceeded the envelope-inclusive capacity: %v", err)
	}
	file := stateDescriptor(t, stateEnvelope([]byte{1, 2}), unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	if data, taken, err := Read(&stateFileSource{file: file}, "ss-state.cut", "state", 1); data != nil || taken != file || !errors.Is(err, ErrCapacity) {
		t.Fatalf("Read exceeded the caller limit: %v", err)
	}
	// Exact caps and empty bytes are legal. The maximum ID must also pass the
	// real Prepare validator rather than just our local character predicate.
	for _, length := range []int{0, MaxStateBytes - 16} {
		s, err := New(strings.Repeat("a", 256), bytes.Repeat([]byte{0x7b}, length))
		if err != nil {
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(s.Close)
		src := handofftest.DupResources(t, s.Resources)
		data, taken, err := Read(src, s.Resources[0].ID, "state", int64(length))
		if taken != nil {
			defer func(closeFunc func() error) { _ = closeFunc() }(taken.Close)
		}
		if err != nil || len(data) != length || !bytes.Equal(data, bytes.Repeat([]byte{0x7b}, length)) {
			t.Fatalf("exact-bound round trip: %v", err)
		}
		p, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "sealed-state-test", Generation: 1}, s.Resources)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Five live-process rounds: real immutable memfds, Prepare duplicates, a failed
// Prepare, import, replacement and explicit cut/imported-owner cleanup. No
// receiver exit or fixture teardown is allowed to supply the cleanup receipt.
// origin: hot-update S5 plan §8 / T51
func TestSealedStateFiveRoundLifecycle(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSealedStateLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "sealed state lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "sealed state lifecycle", beforeFDs, 0)
}

func exerciseSealedStateLifecycle(t *testing.T) {
	t.Helper()
	data := []byte("private-state-bytes-not-public-metadata")
	s, err := New("ss-state.0123456789abcdef0123456789abcdef", data)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	file := s.Owned[0]
	t.Cleanup(func() { _ = file.Close() })
	if len(s.Resources) != 1 || len(s.Resources[0].Data) != 0 || s.Resources[0].Kind != "sealed-state-v1" || s.Resources[0].Files["state"] != file {
		t.Fatal("snapshot metadata contains private data or lacks the cut descriptor")
	}
	blob := make([]byte, 16+len(data))
	if _, err := file.ReadAt(blob, 0); err != nil || !bytes.Equal(blob, stateEnvelope(data)) {
		t.Fatalf("independent envelope golden mismatch: %v", err)
	}
	stateControl(t, file, func(fd uintptr) {
		seals, err := unix.FcntlInt(fd, unix.F_GET_SEALS, 0)
		if err != nil || seals&(unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL) != unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL {
			t.Fatalf("snapshot is not fully sealed: %v", err)
		}
		if _, err := unix.Pwrite(int(fd), []byte{0}, 16); !errors.Is(err, unix.EPERM) {
			t.Fatalf("sealed overwrite was allowed: %v", err)
		}
		if err := unix.Ftruncate(int(fd), int64(len(blob)+1)); !errors.Is(err, unix.EPERM) {
			t.Fatalf("sealed growth was allowed: %v", err)
		}
		if err := unix.Ftruncate(int(fd), int64(len(blob)-1)); !errors.Is(err, unix.EPERM) {
			t.Fatalf("sealed shrink was allowed: %v", err)
		}
		if _, err := unix.FcntlInt(fd, unix.F_ADD_SEALS, unix.F_SEAL_FUTURE_WRITE); !errors.Is(err, unix.EPERM) {
			t.Fatalf("sealed configuration was mutable: %v", err)
		}
	})
	if _, err := file.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, s.Resources)
	got, taken, err := Read(src, s.Resources[0].ID, "state", MaxStateBytes)
	if taken != nil {
		t.Cleanup(func() { _ = taken.Close() })
	}
	if err != nil || taken == nil || taken == file || !bytes.Equal(got, data) || src.Remaining() != 0 {
		t.Fatalf("Read lost bytes or descriptor ownership: %v", err)
	}
	if offset, err := file.Seek(0, io.SeekCurrent); err != nil || offset != 3 {
		t.Fatalf("Read changed the shared OFD offset: %d %v", offset, err)
	}
	if again, duplicate, err := Read(src, s.Resources[0].ID, "state", MaxStateBytes); again != nil || duplicate != nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("Read took the same descriptor twice: %v", err)
	}
	p, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "sealed-state-test", Generation: 1}, s.Resources)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("Prepared.Close destroyed the original: %v", err)
	}
	bad := append([]handoff.Resource(nil), s.Resources...)
	bad[0].ID = "ss-state:invalid"
	if p, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "sealed-state-test", Generation: 1}, bad); p != nil || err == nil {
		if p != nil {
			_ = p.Close()
		}
		t.Fatal("Prepare accepted an illegal resource ID")
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("failed Prepare destroyed the original: %v", err)
	}
	replacement, err := New(s.Resources[0].ID, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("replaced cut leaked its original state FD: %v", err)
	}
	if _, err := taken.ReadAt(got, 16); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("cut Close destroyed the imported copy: %v", err)
	}
	if err := taken.Close(); err != nil {
		t.Fatal(err)
	}
	replacementFile := replacement.Owned[0]
	t.Cleanup(func() { _ = replacementFile.Close() })
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := replacementFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("replacement cut leaked its state FD: %v", err)
	}
	src.Close()
}
