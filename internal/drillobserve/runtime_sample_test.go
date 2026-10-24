//go:build linux

package drillobserve_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func runtimeSampleControl() drillobserve.RuntimeCounters {
	return drillobserve.RuntimeCounters{Binding: drillobserve.RuntimeBinding{Domain: "agent", InstanceID: "instance-1", Generation: 7},
		PID: 123, StartTicks: 567, Goroutines: 9, FDs: 11, Sequence: 1, AtBootNS: 123456789,
		ExeSHA: sha256.Sum256([]byte("executing-image")), Nonce: [12]byte{1}, BootID: "boot-1", PIDNS: "pid:[17]",
		ScopeDigest: hex.EncodeToString(bytes.Repeat([]byte{3}, 32)), Unknown: drillobserve.RuntimeUnknownCounters}
}

// The parent signs five real closable socket/batch/supervisor lifecycles. The
// child separately counts the permanent init writer, which has no Close API.
// origin: hot-update S10 plan §10.2 P06 / §11.2
// origin: S7 internal review INTEG-O F8
func TestRuntimeSampleCustodyRejectsPoison(t *testing.T) {
	if mode := os.Getenv("P06_RUNTIME_HELPER"); mode != "" {
		runRuntimeCustodyHelper(t, mode)
		return
	}

	c := runtimeSampleControl()
	e, err := drillobserve.EncodeRuntimeSample(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := drillobserve.DecodeRuntimeSample(e)
	if err != nil || !reflect.DeepEqual(c, got) {
		t.Fatal("runtime sample codec lost fields")
	}
	// Cold generation zero is known. Generation and sample ordinals keep their
	// frozen uint64 range; only signed resource counts have an int64 ceiling.
	cold := c
	cold.Binding.Generation = 0
	cold.Sequence = ^uint64(0)
	if encoded, err := drillobserve.EncodeRuntimeSample(cold); err != nil {
		t.Fatal("known cold generation or uint64 ordinal rejected")
	} else if decoded, err := drillobserve.DecodeRuntimeSample(encoded); err != nil || !reflect.DeepEqual(cold, decoded) {
		t.Fatal("cold generation or uint64 ordinal lost")
	}
	for name, change := range map[string]func(*drillobserve.Event){
		"missing-kind":            func(e *drillobserve.Event) { e.Kind = "" },
		"foreign-domain":          func(e *drillobserve.Event) { e.Domain = "broker" },
		"missing-nonce":           func(e *drillobserve.Event) { e.Nonce = [12]byte{} },
		"missing-sha":             func(e *drillobserve.Event) { e.Digest = [32]byte{} },
		"zero-pid":                func(e *drillobserve.Event) { e.Values[1] = 0 },
		"large-pid":               func(e *drillobserve.Event) { e.Values[1] = 1 << 31 },
		"zero-ticks":              func(e *drillobserve.Event) { e.Values[2] = 0 },
		"zero-goroutines":         func(e *drillobserve.Event) { e.Values[4] = 0 },
		"large-goroutines":        func(e *drillobserve.Event) { e.Values[4] = 1 << 63 },
		"zero-fds":                func(e *drillobserve.Event) { e.Values[5] = 0 },
		"large-fds":               func(e *drillobserve.Event) { e.Values[5] = 1 << 31 },
		"zero-sequence":           func(e *drillobserve.Event) { e.Values[15] = 0 },
		"large-counter":           func(e *drillobserve.Event) { e.Values[6] = 1 << 63; e.Data[0] &^= 1 << 6 },
		"unknown-schema":          func(e *drillobserve.Event) { e.Values[0]++ },
		"unknown-identity":        func(e *drillobserve.Event) { e.Data[0] |= 2 },
		"unknown-cold-generation": func(e *drillobserve.Event) { e.Values[3] = 0; e.Data[0] |= 8 },
		"unknown-nonzero":         func(e *drillobserve.Event) { e.Values[6] = 1 },
		"invalid-units":           func(e *drillobserve.Event) { e.At = 1 << 63 },
		"truncated-data":          func(e *drillobserve.Event) { e.Data = e.Data[:3] },
		"short-unknown":           func(e *drillobserve.Event) { e.Data = e.Data[:1] },
		"trailing-data":           func(e *drillobserve.Event) { e.Data = append(e.Data, 0) },
		"invalid-text":            func(e *drillobserve.Event) { e.Data[4] = 0xff },
		"nonempty-flow":           func(e *drillobserve.Event) { e.Flow[0] = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := e
			bad.Data = bytes.Clone(e.Data)
			change(&bad)
			if _, err := drillobserve.DecodeRuntimeSample(bad); err == nil {
				t.Fatal("damaged sample accepted")
			}
		})
	}
	for _, invalid := range []drillobserve.RuntimeCounters{
		func() drillobserve.RuntimeCounters { bad := c; bad.Binding.InstanceID = ""; return bad }(),
		func() drillobserve.RuntimeCounters { bad := c; bad.BootID = ""; return bad }(),
		func() drillobserve.RuntimeCounters { bad := c; bad.PIDNS = ""; return bad }(),
		func() drillobserve.RuntimeCounters { bad := c; bad.ScopeDigest = ""; return bad }(),
		func() drillobserve.RuntimeCounters { bad := c; bad.CutID = "unsafe/path"; return bad }(),
		func() drillobserve.RuntimeCounters {
			bad := c
			bad.Binding.InstanceID = strings.Repeat("i", 257)
			return bad
		}(),
	} {
		if _, err := drillobserve.EncodeRuntimeSample(invalid); err == nil {
			t.Fatal("invalid runtime identity encoded")
		}
	}
	for _, request := range []struct {
		nonce [12]byte
		scope [32]byte
	}{
		{}, {nonce: [12]byte{1}}, {scope: [32]byte{1}},
	} {
		if _, err := drillobserve.RuntimeSampleRequest(request.nonce, request.scope); err == nil {
			t.Fatal("empty challenge accepted")
		}
	}
	// nil Hook is a literal fast path: even an invalid binding and open done
	// channel must return without calling a domain observer or allocating.
	done := make(chan struct{})
	if allocations := testing.AllocsPerRun(5, func() {
		drillobserve.RunRuntimeSamples(done, drillobserve.RuntimeBinding{}, func() { t.Fatal("inert observer sampled") })
	}); allocations != 0 {
		t.Fatal("inert sampler allocated")
	}
	if t.Failed() {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRuntimeSampleCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "runtime sample consumer", beforeG)
	testharness.AssertNoFDLeak(t, "runtime sample consumer", beforeFD, 0)

}

func exerciseRuntimeSampleCustody(t *testing.T) {
	t.Helper()
	exerciseRuntimeRequestPoison(t)
	exerciseRuntimeChallengeBinding(t)
	keeper, control, pid, raw, seals, cleanup := startRuntimeCustody(t, "bootstrap")
	defer cleanup()
	reader := &drillobserve.TransferCustodyReader{Identity: sha256.Sum256(raw)}
	var latest drillobserve.RuntimeCounters
	var watermark uint64
	var observedG uint64
	var previousSequence uint64
	for request := byte(1); request <= 2; request++ {
		nonce, scope := [12]byte{request}, sha256.Sum256([]byte{request})
		packet, err := drillobserve.RuntimeSampleRequest(nonce, scope)
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Sendmsg(keeper, packet, nil, nil, unix.MSG_NOSIGNAL); err != nil {
			t.Fatal(err)
		}
		for {
			packet, rights := receiveRuntimePacket(t, keeper)
			if string(packet) == "TDC1" && len(rights) == 1 {
				batch := readRuntimeBatch(t, rights[0], seals)
				events, err := reader.Accept(batch)
				if err != nil {
					t.Fatal("custody gap or poison", err)
				}
				watermark = binary.LittleEndian.Uint64(batch[56:64])
				for _, event := range events {
					if event.Kind == "domain-live" {
						observedG = event.Values[0]
					}
					if event.Kind == drillobserve.RuntimeSampleKind && event.Nonce == nonce {
						latest, err = drillobserve.DecodeRuntimeSample(event)
						if err != nil || latest.PID != uint64(pid) || latest.Unknown != drillobserve.RuntimeUnknownCounters || latest.Goroutines != observedG ||
							latest.Binding.Generation != 7 || latest.ScopeDigest != hex.EncodeToString(scope[:]) {
							t.Fatal("sampler lost actual process identity or request scope")
						}
					}
				}
				continue
			}
			for _, fd := range rights {
				_ = unix.Close(fd)
			}
			if drillobserve.ValidateRuntimeSampleACK(packet, latest, watermark) != nil {
				t.Fatal("fresh sample watermark missing")
			}
			if latest.Sequence <= previousSequence {
				t.Fatal("fresh challenge reused an old sample ordinal")
			}
			previousSequence = latest.Sequence
			if request == 1 {
				exe, err := os.Open(fmt.Sprintf("/proc/%d/exe", pid))
				if err != nil {
					t.Fatal(err)
				}
				hash := sha256.New()
				_, readErr := io.Copy(hash, exe)
				closeErr := exe.Close()
				if readErr != nil || closeErr != nil || !bytes.Equal(hash.Sum(nil), latest.ExeSHA[:]) {
					t.Fatal("sampler digest differs from executing child image")
				}
			}
			for _, offset := range []int{4, 16, 48, 56, 64} {
				bad := bytes.Clone(packet)
				bad[offset] ^= 1
				if drillobserve.ValidateRuntimeSampleACK(bad, latest, watermark) == nil {
					t.Fatal("replayed or poisoned ACK accepted")
				}
			}
			break
		}
	}
	// Removing an entire sealed batch must poison the existing cursor; a later
	// valid sample cannot make the stream complete again.
	gap := append([]byte("TDRILL1\x00"), rawDigest(raw)...)
	gap = append(gap, bytes.Repeat([]byte{1}, 16)...)
	gap = binary.LittleEndian.AppendUint64(gap, 2)
	gap = binary.LittleEndian.AppendUint64(gap, 0)
	gap = binary.LittleEndian.AppendUint32(gap, 0)
	broken := &drillobserve.TransferCustodyReader{Identity: sha256.Sum256(raw)}
	if _, err := broken.Accept(gap); err == nil {
		t.Fatal("custody sequence gap accepted")
	}
	binary.LittleEndian.PutUint64(gap[56:64], 1)
	if _, err := broken.Accept(gap); err == nil {
		t.Fatal("custody poison was cleared")
	}
	if _, err := control.Write([]byte{'p'}); err != nil {
		t.Fatal(err)
	}
	for {
		packet, rights := receiveRuntimePacket(t, keeper)
		if len(rights) == 1 {
			batch := readRuntimeBatch(t, rights[0], seals)
			if binary.LittleEndian.Uint64(batch[64:72]) != 0 {
				break
			}
			continue
		}
		for _, fd := range rights {
			_ = unix.Close(fd)
		}
		if string(packet) == "TDBR" {
			break
		}
	}
}

func rawDigest(data []byte) []byte { digest := sha256.Sum256(data); return digest[:] }

// A valid init observer with no agent sampler cannot certify requests. Floods,
// duplicates and malformed challenges poison its bounded mailbox independently
// of normal sample/ACK handling; closing stdin joins every supervised child.
// origin: hot-update S10 plan §5.3 / P06
func exerciseRuntimeRequestPoison(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"overflow", "duplicate", "zero-nonce", "truncated"} {
		func() {
			keeper, _, _, _, seals, cleanup := startRuntimeCustody(t, "bootstrap-mailbox")
			defer cleanup()
			count := 1
			if scenario == "overflow" {
				count = 17
			}
			if scenario == "duplicate" {
				count = 2
			}
			for index := 0; index < count; index++ {
				nonce := [12]byte{byte(index + 1)}
				if scenario == "duplicate" {
					nonce = [12]byte{1}
				}
				packet, err := drillobserve.RuntimeSampleRequest(nonce, [32]byte{1})
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "zero-nonce" {
					clear(packet[4:16])
				}
				if scenario == "truncated" {
					packet = packet[:16]
				}
				if err := unix.Sendmsg(keeper, packet, nil, nil, unix.MSG_NOSIGNAL); err != nil {
					t.Fatal(err)
				}
			}
			// A fresh legacy barrier yields an explicit clean counterexample if a
			// mailbox guard is removed. Mutation kills must not rely on timeout.
			drain, err := drillobserve.TransferDrainRequest([16]byte{1})
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Sendmsg(keeper, drain, nil, nil, unix.MSG_NOSIGNAL); err != nil {
				t.Fatal(err)
			}
			for {
				_, rights := receiveRuntimePacket(t, keeper)
				if len(rights) != 1 {
					for _, fd := range rights {
						_ = unix.Close(fd)
					}
					t.Fatal("mailbox poison lost sealed custody")
				}
				batch := readRuntimeBatch(t, rights[0], seals)
				cause := binary.LittleEndian.Uint64(batch[64:72])
				if cause == 0 {
					continue
				}
				if cause != 17 {
					t.Fatal("mailbox failure lost its first cause")
				}
				break
			}
		}()
	}
}

func exerciseRuntimeChallengeBinding(t *testing.T) {
	t.Helper()
	for _, command := range []byte{'s', 'w'} {
		func() {
			keeper, control, _, _, seals, cleanup := startRuntimeCustody(t, "bootstrap-mailbox")
			defer cleanup()
			scope := sha256.Sum256([]byte{1})
			request, err := drillobserve.RuntimeSampleRequest([12]byte{1}, scope)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Sendmsg(keeper, request, nil, nil, unix.MSG_NOSIGNAL); err != nil {
				t.Fatal(err)
			}
			drain, err := drillobserve.TransferDrainRequest([16]byte{1})
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Sendmsg(keeper, drain, nil, nil, unix.MSG_NOSIGNAL); err != nil {
				t.Fatal(err)
			}
			var watermark uint64
			for {
				packet, rights := receiveRuntimePacket(t, keeper)
				if len(rights) == 1 {
					batch := readRuntimeBatch(t, rights[0], seals)
					if binary.LittleEndian.Uint64(batch[64:72]) != 0 {
						t.Fatal("valid fresh request was poisoned")
					}
					watermark = binary.LittleEndian.Uint64(batch[56:64])
					continue
				}
				for _, fd := range rights {
					_ = unix.Close(fd)
				}
				if drillobserve.ValidateTransferDrainACK(packet, [16]byte{1}, watermark) != nil {
					t.Fatal("challenge registration barrier missing")
				}
				break
			}
			if _, err := control.Write([]byte{command}); err != nil {
				t.Fatal(err)
			}
			for {
				_, rights := receiveRuntimePacket(t, keeper)
				if len(rights) != 1 {
					for _, fd := range rights {
						_ = unix.Close(fd)
					}
					t.Fatal("fresh ACK certified a stale or wrong-scope sample")
				}
				batch := readRuntimeBatch(t, rights[0], seals)
				cause := binary.LittleEndian.Uint64(batch[64:72])
				if cause == 0 {
					continue
				}
				if cause != 16 {
					t.Fatal("sample binding failure lost its first poison cause")
				}
				break
			}
		}()
	}
}

var runtimeCustodyRecvmsg = unix.Recvmsg

func receiveRuntimePacket(t *testing.T, fd int) ([]byte, []int) {
	t.Helper()
	packet, ancillary := make([]byte, 128), make([]byte, unix.CmsgSpace(4))
	n, oob, flags, _, err := runtimeCustodyRecvmsg(fd, packet, ancillary, unix.MSG_CMSG_CLOEXEC)
	// Go's preemption signal can interrupt the raw syscall before any packet
	// or rights are consumed. Preserve the same receive and its strict checks.
	// origin: S10 internal review FINAL-REAL-FAILS F1
	for errors.Is(err, unix.EINTR) {
		n, oob, flags, _, err = runtimeCustodyRecvmsg(fd, packet, ancillary, unix.MSG_CMSG_CLOEXEC)
	}
	if err != nil || n == 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		t.Fatal("custody receive failed", err)
	}
	messages, err := unix.ParseSocketControlMessage(ancillary[:oob])
	if err != nil {
		t.Fatal(err)
	}
	var rights []int
	for _, message := range messages {
		fds, err := unix.ParseUnixRights(&message)
		if err != nil {
			t.Fatal(err)
		}
		rights = append(rights, fds...)
	}
	return packet[:n], rights
}

func readRuntimeBatch(t *testing.T, fd, seals int) []byte {
	t.Helper()
	defer func() {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
	}()
	got, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	var stat unix.Stat_t
	if err != nil || got&seals != seals || unix.Fstat(fd, &stat) != nil || stat.Size < 76 || stat.Size > 1<<20 {
		t.Fatal("batch not bounded and sealed")
	}
	data := make([]byte, int(stat.Size))
	if n, err := unix.Pread(fd, data, 0); err != nil || n != len(data) {
		t.Fatal("short sealed batch")
	}
	return data
}

func runRuntimeCustodyHelper(t *testing.T, mode string) {
	t.Helper()
	if strings.HasPrefix(mode, "bootstrap") {
		// The shared supervisor starts inert, then this target enables custody
		// by a real same-PID exec. Its reaper cannot steal the observer channel.
		for _, fd := range []int{3, 4} {
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
				t.Fatal(err)
			}
		}
		var env []string
		for _, entry := range drillobserve.StripEnvironment(os.Environ()) {
			if !strings.HasPrefix(entry, "P06_RUNTIME_HELPER=") {
				env = append(env, entry)
			}
		}
		env = append(env, drillobserve.EnableEnv+"=v1", drillobserve.CustodyEnv+"=3", "P06_RUNTIME_HELPER="+strings.Replace(mode, "bootstrap", "active", 1))
		if err := unix.Exec(os.Args[0], os.Args, env); err != nil {
			t.Fatal(err)
		}
	}
	if drillobserve.Hook == nil {
		t.Fatal("startup custody absent")
	}
	for _, fd := range []int{3, 4} {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatal("private custody FD inherited by business child")
		}
	}
	stack := make([]byte, 1<<20)
	// init publishes Hook before its runnable writer enters writeLoop. Observe
	// that entry within the existing custody receive budget; a second writer is
	// a failure immediately, never a transient count that polling may hide.
	// origin: S7 internal review INTEG-O F8
	if !testharness.WaitFor(t, 3*time.Second, time.Millisecond, func() bool {
		n := runtime.Stack(stack, true)
		writers := strings.Count(string(stack[:n]), "drillobserve.(*custody).writeLoop(")
		if writers > 1 {
			t.Fatalf("permanent custody writer count changed: %d", writers)
		}
		return writers == 1
	}) {
		t.Fatal("permanent custody writer never entered writeLoop")
	}
	n := runtime.Stack(stack, true)
	if strings.Count(string(stack[:n]), "drillobserve.(*custody).writeLoop(") != 1 {
		t.Fatal("permanent custody writer count changed")
	}
	if mode == "active-mailbox" {
		for {
			var command [1]byte
			if _, err := io.ReadFull(os.Stdin, command[:]); err != nil {
				break
			}
			// Deliberately synthetic private controls, never a product receipt:
			// retain a valid codec but violate the writer's request binding.
			bad := runtimeSampleControl()
			var now unix.Timespec
			if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
				t.Fatal(err)
			}
			bad.AtBootNS = uint64(now.Nano())
			if command[0] == 's' {
				bad.AtBootNS = 1
				scope := sha256.Sum256([]byte{1})
				bad.ScopeDigest = hex.EncodeToString(scope[:])
			}
			if err := drillobserve.EmitRuntimeSample(bad); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	done, joined := make(chan struct{}), make(chan struct{})
	beforeG := runtime.NumGoroutine()
	go func() {
		defer close(joined)
		drillobserve.RunRuntimeSamples(done, drillobserve.RuntimeBinding{Domain: "agent", InstanceID: "instance-1", Generation: 7}, func() {
			drillobserve.Hook(drillobserve.Event{Kind: "domain-live", Domain: "exec", Values: [16]uint64{uint64(runtime.NumGoroutine())}})
		})
	}()
	var command [1]byte
	if _, err := io.ReadFull(os.Stdin, command[:]); err != nil || command[0] != 'p' {
		t.Fatal("missing poison control")
	}
	bad := runtimeSampleControl()
	bad.Nonce = [12]byte{}
	if err := drillobserve.EmitRuntimeSample(bad); err == nil {
		t.Fatal("invalid nonce did not poison producer")
	}
	drillobserve.Flush()
	_, _ = io.Copy(io.Discard, os.Stdin)
	close(done)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("sampler outlived cancellation")
	}
	n = runtime.Stack(stack, true)
	if strings.Count(string(stack[:n]), "drillobserve.(*custody).writeLoop(") != 1 {
		t.Fatal("init writer was counted as a closable collector")
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > beforeG && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if runtime.NumGoroutine() > beforeG {
		t.Fatal("sampler goroutine remained beside permanent writer")
	}
}

func startRuntimeCustody(t *testing.T, mode string) (int, *os.File, int, []byte, int, func()) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	product, keeper := os.NewFile(uintptr(pair[0]), "product-custody"), os.NewFile(uintptr(pair[1]), "keeper-custody")
	t.Cleanup(func() { _ = product.Close(); _ = keeper.Close() })
	fd, err := unix.MemfdCreate("runtime-custody-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	config := os.NewFile(uintptr(fd), "runtime-custody-config")
	t.Cleanup(func() { _ = config.Close() })
	raw := append(binary.LittleEndian.AppendUint64([]byte("TDCFG1\x00\x00"), 4), bytes.Repeat([]byte{7}, 16)...)
	if _, err := config.Write(raw); err != nil {
		t.Fatal(err)
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
		t.Fatal(err)
	}
	input, control, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = control.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: private supervised custody child lifecycle.
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRuntimeSampleCustodyRejectsPoison$", "-test.count=1")
	cmd.Env = append(drillobserve.StripEnvironment(os.Environ()), "P06_RUNTIME_HELPER="+mode, "GORACE=atexit_sleep_ms=0")
	cmd.ExtraFiles, cmd.Stdin = []*os.File{config, product}, input
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			_ = control.Close()
			if err := join(); err != nil {
				t.Errorf("custody child join: %v: %s", err, output.String())
			}
			cancel()
			for _, file := range []*os.File{input, control, product, keeper, config} {
				if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
				}
			}
		})
	}
	t.Cleanup(cleanup)
	_ = input.Close()
	if err := unix.SetsockoptTimeval(pair[1], unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 3}); err != nil {
		t.Fatal(err)
	}
	return pair[1], control, cmd.Process.Pid, raw, seals, cleanup
}
