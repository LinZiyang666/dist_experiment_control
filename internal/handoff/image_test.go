//go:build linux

// Real: descriptor duplication, immutable memfd snapshots and ownership cleanup.
// Stubbed: no daemon coordinator; callers of Prepare are assumed quiescent.
// Actual image replacement is exercised separately in image_exec_test.go.
package handoff

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func snapshotState() State {
	return State{Role: "agent-runtime", Instance: "test-instance", Generation: 2, Data: json.RawMessage(`{"tasks":1}`)}
}

// origin: docs/reviews/hot-update-plan.md §4.1.
func TestPreparedSnapshotIsSealedAndClosePreservesOriginal(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "original")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	p, err := Prepare(snapshotState(), []Resource{{ID: "task", Kind: "process", Files: map[string]*os.File{"output": f}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	seals, err := unix.FcntlInt(uintptr(p.manifestFD), unix.F_GET_SEALS, 0)
	if err != nil || seals&requiredSeals != requiredSeals {
		t.Fatalf("snapshot seals = %#x, %v", seals, err)
	}
	if _, err := unix.Pwrite(p.manifestFD, []byte("overwrite"), 0); !errors.Is(err, unix.EPERM) {
		t.Fatalf("sealed snapshot can be modified: %v", err)
	}
	if err := unix.Ftruncate(p.manifestFD, 0); !errors.Is(err, unix.EPERM) {
		t.Fatalf("sealed snapshot can be truncated: %v", err)
	}
	for _, fd := range append(append([]int(nil), p.fds...), p.manifestFD) {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("prepared descriptor %d inheritable: flags=%d err=%v", fd, flags, err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := f.WriteString("still owned by caller"); err != nil {
		t.Fatalf("Close closed caller's original: %v", err)
	}
}

func TestPrepareRejectsInvalidAndExcessiveState(t *testing.T) {
	cases := []struct {
		name      string
		state     State
		resources []Resource
	}{
		{"missing-role", State{Instance: "i", Generation: 1}, nil},
		{"path-identity", State{Role: "agent", Instance: "../other", Generation: 1}, nil},
		{"zero-generation", State{Role: "agent", Instance: "i"}, nil},
		{"bad-json", State{Role: "agent", Instance: "i", Generation: 1, Data: json.RawMessage(`{`)}, nil},
		{"duplicate-resource", snapshotState(), []Resource{{ID: "same", Kind: "pipe"}, {ID: "same", Kind: "pipe"}}},
		{"nil-file", snapshotState(), []Resource{{ID: "a", Kind: "pipe", Files: map[string]*os.File{"out": nil}}}},
		{"bad-resource-state", snapshotState(), []Resource{{ID: "a", Kind: "pipe", Data: json.RawMessage(`false trailing`)}}},
		{"resource-limit", snapshotState(), make([]Resource, maxResources+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Prepare(tc.state, tc.resources)
			if p != nil {
				_ = p.Close()
			}
			if err == nil || p != nil {
				t.Fatalf("Prepare invalid input = (%v, %v)", p, err)
			}
		})
	}
}

// origin: docs/reviews/hot-update-plan.md §4.1.
func TestPrepareRefusesInheritableOriginalWithoutChangingIt(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	raw, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) { _, setErr = unix.FcntlInt(fd, unix.F_SETFD, 0) }); err != nil || setErr != nil {
		t.Fatalf("create inheritable input: %v / %v", err, setErr)
	}
	defer func() {
		_ = raw.Control(func(fd uintptr) { _, _ = unix.FcntlInt(fd, unix.F_SETFD, unix.FD_CLOEXEC) })
	}()
	p, err := Prepare(snapshotState(), []Resource{{ID: "raw", Kind: "file", Files: map[string]*os.File{"original": f}}})
	if p != nil {
		_ = p.Close()
	}
	if err == nil || p != nil {
		t.Fatal("inheritable original accepted: its untracked reference would survive exec")
	}
	flags := -1
	if err := raw.Control(func(fd uintptr) { flags, setErr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil || setErr != nil || flags != 0 {
		t.Fatalf("rejection modified/closed original: flags=%d err=%v / %v", flags, err, setErr)
	}
}

func TestRestoreRejectsUntrustedSnapshotAndConsumesEnvironment(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "not-a-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := raw.Control(func(n uintptr) { fd = int(n) }); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "not-a-number:" + strings.Repeat("a", 64), "0:" + strings.Repeat("a", 64), strconv.Itoa(fd) + ":" + strings.Repeat("a", 64)} {
		t.Run(strconv.Itoa(len(value)), func(t *testing.T) {
			t.Setenv(environmentKey, value)
			r, err := Restore()
			if r != nil {
				_ = r.Close()
			}
			if err == nil || r != nil {
				t.Fatalf("untrusted snapshot accepted: %v, %v", r, err)
			}
			if _, ok := os.LookupEnv(environmentKey); ok {
				t.Fatal("private dispatch survives failed Restore")
			}
			if _, err := file.Stat(); err != nil {
				t.Fatalf("malformed dispatch closed unrelated file: %v", err)
			}
		})
	}
}

// These deliberately malformed *sealed* packages exercise the import boundary,
// rather than assuming kernel seals make semantic state or owner claims valid.
func TestRestoreRejectsSealedWrongOwnerVersionAndDescriptor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*manifest)
	}{
		{"other-process", func(m *manifest) { m.PID++ }},
		{"other-user", func(m *manifest) { m.UID++ }},
		{"wrong-token", func(m *manifest) { m.Nonce = strings.Repeat("b", 64) }},
		{"unknown-version", func(m *manifest) { m.Version++ }},
		{"duplicate-resource", func(m *manifest) { m.Resources = append(m.Resources, m.Resources[0]) }},
		{"missing-file", func(m *manifest) { m.Resources[0].Files[0].FD = 1 << 28 }},
		{"reused-file", func(m *manifest) { m.Resources[0].Files[0].Ino++ }},
		{"changed-status", func(m *manifest) { m.Resources[0].Files[0].Flags ^= unix.O_NONBLOCK }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original, err := os.Open(os.DevNull)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = original.Close() }()
			fi, err := duplicateFile("file", original)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(fi.FD) }()
			m := manifest{Version: formatVersion, PID: os.Getpid(), UID: os.Geteuid(), Nonce: strings.Repeat("a", 64), State: snapshotState(),
				Resources: []resourceImage{{ID: "resource", Kind: "file", Files: []fileImage{fi}}}}
			tc.mutate(&m)
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			fd, err := sealedSnapshot(data)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if fd >= 0 {
					_ = unix.Close(fd)
				}
			}()
			t.Setenv(environmentKey, strconv.Itoa(fd)+":"+strings.Repeat("a", 64))
			r, err := Restore()
			if m.PID == os.Getpid() && m.UID == os.Geteuid() && m.Nonce == strings.Repeat("a", 64) {
				// The import contract consumes a package once it establishes its
				// owner, including validation failures after that point. Avoid a
				// second close of a descriptor number that may be reused later.
				if _, checkErr := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(checkErr, unix.EBADF) {
					t.Fatalf("established snapshot not consumed: %v", checkErr)
				}
				fd = -1
			}
			if r != nil {
				_ = r.Close()
			}
			if err == nil || r != nil {
				t.Fatalf("invalid sealed package accepted: %v, %v", r, err)
			}
			if _, err := original.Stat(); err != nil {
				t.Fatalf("rejection consumed caller's file: %v", err)
			}
		})
	}
}

func TestRestoredFileOwnershipTransfersExactlyOnce(t *testing.T) {
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "payload"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString("persisted bytes"); err != nil {
		t.Fatal(err)
	}
	fi, err := duplicateFile("data", f)
	if err != nil {
		t.Fatal(err)
	}
	m := manifest{Version: formatVersion, PID: os.Getpid(), UID: os.Geteuid(), Nonce: strings.Repeat("c", 64), State: snapshotState(),
		Resources: []resourceImage{{ID: "work", Kind: "file", Files: []fileImage{fi}}}}
	blob, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := sealedSnapshot(blob)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(environmentKey, strconv.Itoa(fd)+":"+m.Nonce)
	r, err := Restore()
	if err != nil {
		_ = unix.Close(fi.FD)
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	taken, err := r.TakeFile("work", "data")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()
	if _, err := r.TakeFile("work", "data"); err == nil {
		t.Fatal("resource acquired by a second owner")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := taken.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(taken)
	if err != nil || string(data) != "persisted bytes" {
		t.Fatalf("bundle Close touched transferred ownership: %q, %v", data, err)
	}
}

func TestCleanEnvironmentPreservesCallerIntent(t *testing.T) {
	if CleanEnvironment(nil) != nil {
		t.Fatal("nil inheritance policy changed")
	}
	empty := CleanEnvironment([]string{})
	if empty == nil || len(empty) != 0 {
		t.Fatal("explicitly empty environment changed")
	}
	input := []string{"NORMAL=value", environmentKey + "=old", environmentKey + "_OTHER=keep", environmentKey + "=later"}
	want := []string{"NORMAL=value", environmentKey + "_OTHER=keep"}
	if got := CleanEnvironment(input); !reflect.DeepEqual(got, want) {
		t.Fatalf("clean environment = %v, want %v", got, want)
	}
	if len(input) != 4 || input[1] != environmentKey+"=old" {
		t.Fatal("caller environment modified in place")
	}
}

func FuzzHandoffDispatch(f *testing.F) {
	for _, seed := range []string{"", "3:" + strings.Repeat("a", 64), "0:" + strings.Repeat("b", 64), "-1:x", "03:" + strings.Repeat("c", 64), "3:bad"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		fd, nonce, err := parseDispatch(value)
		if err != nil {
			if fd != 0 || nonce != "" {
				t.Fatal("invalid dispatch returned partial authority")
			}
			return
		}
		if fd < 3 || len(nonce) != 64 || value != strconv.Itoa(fd)+":"+nonce {
			t.Fatalf("noncanonical dispatch accepted: %q", value)
		}
		// No syscall is made: fuzzing a descriptor parser must not close or
		// change flags on the fuzz worker's own arbitrary descriptors.
		if fd2, nonce2, err := parseDispatch(strconv.Itoa(fd) + ":" + nonce); err != nil || fd2 != fd || nonce2 != nonce {
			t.Fatalf("dispatch round trip failed: %v", err)
		}
	})
}

// origin: hot-update S6 plan §4.1 / T27
func TestPreparedFlagRestoreFailureIsReported(t *testing.T) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	prepared, err := Prepare(snapshotState(), []Resource{{ID: "flag-test", Kind: "fixture", Files: map[string]*os.File{"file": file}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	// A real EBADF both prevents inheritance and fails the later flag repair.
	// The two errors must remain distinguishable so rollback cannot assume all
	// surviving descriptors were protected merely because exec returned.
	if err = unix.Close(prepared.fds[0]); err != nil {
		t.Fatal(err)
	}
	err = prepared.Exec("/missing-flag-fixture", []string{"fixture"}, nil)
	if err == nil || !errors.Is(err, unix.EBADF) || !strings.Contains(err.Error(), "restore close-on-exec") {
		t.Fatalf("flag restore failure was swallowed: %v", err)
	}
	flags, err := unix.FcntlInt(uintptr(prepared.manifestFD), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("other surviving aliases remained inheritable: %v", err)
	}
}

// origin: hot-update S6 plan §4.1 / T27
func TestPreparedBrokerRejectsAliasedOwners(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "original")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	duplicate := -1
	var dupErr error
	if err = raw.Control(func(fd uintptr) { duplicate, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || dupErr != nil {
		t.Fatalf("dup: %v %v", err, dupErr)
	}
	alias := os.NewFile(uintptr(duplicate), "prepared-alias")
	defer func() { _ = alias.Close() }()
	state := snapshotState()
	state.Role = "broker"
	resources := []Resource{{ID: "first", Kind: "fixture", Files: map[string]*os.File{"file": file}}, {ID: "second", Kind: "fixture", Files: map[string]*os.File{"file": alias}}}
	prepared, err := Prepare(state, resources)
	if prepared != nil {
		_ = prepared.Close()
	}
	if err == nil || prepared != nil {
		t.Fatal("distinct IDs assigned one OFD two terminal owners")
	}
	if _, err = file.WriteString("original survives refusal"); err != nil {
		t.Fatal(err)
	}
}
