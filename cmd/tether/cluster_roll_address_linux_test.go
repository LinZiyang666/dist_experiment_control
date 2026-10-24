//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

type signedRollListenerReservation struct {
	owner    *testing.T
	listener net.Listener
	lease    *os.File
}

var signedRollListenerReservations = struct {
	sync.Mutex
	ports map[int]*signedRollListenerReservation
}{ports: make(map[int]*signedRollListenerReservation)}

// origin: S8 internal review ROLL-SETTLEMENT F4; hot-update S8 plan L21
func TestSignedRollFutureListenersKeepKernelCustodyUntilHandoff(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSignedRollListenerCustody(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "signed roll listener custody x5", g)
	testharness.AssertNoFDLeak(t, "signed roll listener custody x5", fds, 0)
}

func exerciseSignedRollListenerCustody(t *testing.T) {
	t.Helper()
	port := signedRollPort(t)
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err == nil {
		_ = listener.Close()
		t.Fatal("a future broker listener was released before its daemon could bind")
	}
	signedRollHandoffPort(t, port)
	listener, err = net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("reserved listener cannot bind after handoff: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close handed-off listener: %v", err)
		}
	}()
	signedRollListenerReservations.Lock()
	reservation := signedRollListenerReservations.ports[port]
	signedRollListenerReservations.Unlock()
	if reservation == nil || reservation.owner != t {
		t.Fatal("handoff forgot the original allocation before fixture cleanup")
	}
}

func signedRollPort(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		t.Fatal(err)
	}
	var first, last int
	if _, err := fmt.Sscan(string(data), &first, &last); err != nil || first <= 1024 || last < first {
		t.Fatalf("invalid kernel source-port range: %q (%v)", data, err)
	}
	limit := min(first, 30000)
	// Ordinary outbound connections can consume a released port-zero address
	// during fixture construction. Future listeners must use the separate band.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F8; plan §11.1
	for attempt := 0; attempt < 100; attempt++ {
		var random [4]byte
		if _, err := rand.Read(random[:]); err != nil {
			t.Fatal(err)
		}
		port := 1024 + int(binary.BigEndian.Uint32(random[:])%uint32(limit-1024))
		if signedRollReservePort(t, port) {
			return port
		}
	}
	t.Fatal("signed roll listener port unavailable")
	return 0
}

// A process-local allocation survives handoff only within that process. Isolated
// matrix fixtures must also exclude each other while no listener exists during
// child startup or re-exec. Keep the flock until fixture cleanup has joined them.
// origin: S8 internal review roll-recovery-stall F7; hot-update S8 plan L21
func signedRollReservePort(t *testing.T, port int) bool {
	t.Helper()
	signedRollListenerReservations.Lock()
	defer signedRollListenerReservations.Unlock()
	if signedRollListenerReservations.ports[port] != nil {
		return false
	}
	lease, err := signedRollPortLease(port)
	if err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return false
		}
		t.Fatalf("lease signed roll listener %d: %v", port, err)
	}
	// Acquire before bind: even a brief candidate listener could steal a peer's
	// handed-off address while that peer's daemon is opening its real socket.
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		_ = lease.Close()
		return false
	}
	// The separate band avoids outbound sockets, but another fixture role
	// can still choose this address. Keep kernel custody until the actual
	// listener starts and retain its allocation until this fixture joins.
	// origin: S8 internal review ROLL-SETTLEMENT F4; hot-update S8 plan L21
	reservation := &signedRollListenerReservation{owner: t, listener: listener, lease: lease}
	signedRollListenerReservations.ports[port] = reservation
	t.Cleanup(func() {
		signedRollListenerReservations.Lock()
		defer signedRollListenerReservations.Unlock()
		if signedRollListenerReservations.ports[port] == reservation {
			if reservation.listener != nil {
				_ = reservation.listener.Close()
			}
			_ = reservation.lease.Close()
			delete(signedRollListenerReservations.ports, port)
		}
	})
	return true
}

func signedRollPortLease(port int) (*os.File, error) {
	// TMPDIR and HOME are private to each isolated test process, so neither can
	// locate the shared lease namespace. Never unlink lock files: recreating an
	// inode would let a later fixture acquire a second lock for the same port.
	root := filepath.Join("/tmp", fmt.Sprintf("tether-signed-roll-ports-%d", os.Getuid()))
	if err := os.Mkdir(root, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	dir, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(dir) }()
	var stat unix.Stat_t
	if err := unix.Fstat(dir, &stat); err != nil {
		return nil, err
	}
	if stat.Uid != uint32(os.Getuid()) || stat.Mode&0777 != 0700 {
		return nil, errors.New("signed roll port lease directory is not user-private")
	}
	fd, err := unix.Openat(dir, strconv.Itoa(port), unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(root, strconv.Itoa(port)))
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func signedRollHandoffPort(t *testing.T, port int) {
	t.Helper()
	signedRollListenerReservations.Lock()
	defer signedRollListenerReservations.Unlock()
	reservation := signedRollListenerReservations.ports[port]
	if reservation == nil || reservation.owner != t || reservation.listener == nil {
		t.Fatalf("signed roll listener %d has no original reservation", port)
	}
	if err := reservation.listener.Close(); err != nil {
		t.Fatal(err)
	}
	reservation.listener = nil
}

func signedRollHandoffAddress(t *testing.T, address string) {
	t.Helper()
	if address == "" {
		return
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	signedRollHandoffPort(t, n)
}

// A future listener chosen from port zero can be consumed by an ordinary NATS
// or TLS source socket before the broker starts. The integrated scope test
// reproduced this as a Raft bind failure, before any upgrade could run.
// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F8; plan L17 / §11.1
func TestSignedRollListenersAvoidEphemeralSourcePorts(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "signed roll listener ports", goroutines)
	defer testharness.AssertNoFDLeak(t, "signed roll listener ports", fds, 0)
	for round := 0; round < 5; round++ {
		// Each fixture must retire its lease before the parent measures FDs.
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSignedRollPortRange(t) })
	}
}

func exerciseSignedRollPortRange(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		t.Fatal(err)
	}
	var first, last int
	if _, err := fmt.Sscan(string(data), &first, &last); err != nil {
		t.Fatal(err)
	}
	port := signedRollPort(t)
	defer signedRollHandoffPort(t, port)
	if first <= port && port <= last {
		t.Fatalf("future broker listener %d overlaps the kernel source-port range %d-%d", port, first, last)
	}
}

// Both supervised processes use the real allocator with different private
// TMPDIRs. The contender forces the original Raft and tunnel candidates after
// handoff, then acquires those same ports only after the owner has cleaned up.
// origin: S8 internal review roll-recovery-stall F7; hot-update S8 plan L21
func TestSignedRollPortLeaseSurvivesHandoffAcrossProcesses(t *testing.T) {
	if role := os.Getenv("_TETHER_SIGNED_ROLL_LEASE_ROLE"); role != "" {
		signedRollLeaseProcess(t, role)
		return
	}
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSignedRollPortLease(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "signed roll cross-process port lease x5", g)
	testharness.AssertNoFDLeak(t, "signed roll cross-process port lease x5", fds, 0)
}

func signedRollLeaseProcess(t *testing.T, role string) {
	t.Helper()
	input := bufio.NewReader(os.Stdin)
	if role == "owner" {
		t.Run("fixture", func(t *testing.T) {
			raft, tunnel := signedRollPort(t), signedRollPort(t)
			signedRollHandoffPort(t, raft)
			signedRollHandoffAddress(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(tunnel)))
			if _, err := fmt.Fprintf(os.Stdout, "%d %d\n", raft, tunnel); err != nil {
				t.Fatal(err)
			}
			if line, err := input.ReadString('\n'); err != nil || line != "retire\n" {
				t.Fatalf("owner retirement handshake: %q %v", line, err)
			}
		})
		// Remain alive after fixture cleanup so process exit cannot conceal a
		// forgotten lease close. The contender must acquire both original ports.
		if _, err := fmt.Fprintln(os.Stdout, "retired"); err != nil {
			t.Fatal(err)
		}
		if line, err := input.ReadString('\n'); err != nil || line != "stop\n" {
			t.Fatalf("owner stop handshake: %q %v", line, err)
		}
		return
	}
	if role != "contender" {
		t.Fatalf("unknown lease peer role %q", role)
	}
	var raft, tunnel int
	if _, err := fmt.Fscanln(input, &raft, &tunnel); err != nil {
		t.Fatal(err)
	}
	for _, port := range []int{raft, tunnel} {
		// Kernel bind must already be possible; an unreleased listener would
		// conceal a missing lease and make the exclusion assertion vacuous.
		listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatalf("original listener still owns handed-off port %d: %v", port, err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if signedRollReservePort(t, port) {
			t.Fatalf("competing fixture acquired handed-off port %d before owner cleanup", port)
		}
	}
	if _, err := fmt.Fprintln(os.Stdout, "excluded"); err != nil {
		t.Fatal(err)
	}
	if line, err := input.ReadString('\n'); err != nil || line != "released\n" {
		t.Fatalf("contender release handshake: %q %v", line, err)
	}
	for _, port := range []int{raft, tunnel} {
		if !signedRollReservePort(t, port) {
			t.Fatalf("owner cleanup retained port lease %d", port)
		}
		signedRollHandoffPort(t, port)
	}
}

type signedRollLeasePeer struct {
	input  *os.File
	output *bufio.Reader
	join   func()
}

func startSignedRollLeasePeer(t *testing.T, role string) signedRollLeasePeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inR.Close(); _ = inW.Close() })
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = outR.Close(); _ = outW.Close() })
	if err := outR.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSignedRollPortLeaseSurvivesHandoffAcrossProcesses$", "-test.count=1", "-test.timeout=20s")
	root := t.TempDir()
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "_TETHER_SIGNED_ROLL_LEASE_ROLE="+role,
		"HOME="+root, "TETHER_HOME="+root, "TMPDIR="+root)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, &diagnostics
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = inR.Close()
	_ = outW.Close()
	output := bufio.NewReader(outR)
	joined := false
	join := func() {
		t.Helper()
		if !joined {
			joined = true
			if err := wait(); err != nil {
				remaining, readErr := io.ReadAll(output)
				t.Errorf("%s lease peer: %v\n%s%s\nread output: %v", role, err, diagnostics.String(), remaining, readErr)
			}
		}
	}
	t.Cleanup(func() { cancel(); join() })
	return signedRollLeasePeer{input: inW, output: output, join: join}
}

func exerciseSignedRollPortLease(t *testing.T) {
	t.Helper()
	owner := startSignedRollLeasePeer(t, "owner")
	ports, err := owner.output.ReadString('\n')
	if err != nil {
		t.Fatalf("read owner ports: %v", err)
	}
	contender := startSignedRollLeasePeer(t, "contender")
	if _, err := fmt.Fprint(contender.input, ports); err != nil {
		t.Fatal(err)
	}
	if line, err := contender.output.ReadString('\n'); err != nil || line != "excluded\n" {
		t.Fatalf("cross-process exclusion missing: %q %v", line, err)
	}
	if _, err := fmt.Fprintln(owner.input, "retire"); err != nil {
		t.Fatal(err)
	}
	if line, err := owner.output.ReadString('\n'); err != nil || line != "retired\n" {
		t.Fatalf("owner fixture cleanup missing: %q %v", line, err)
	}
	if _, err := fmt.Fprintln(contender.input, "released"); err != nil {
		t.Fatal(err)
	}
	contender.join()
	if _, err := fmt.Fprintln(owner.input, "stop"); err != nil {
		t.Fatal(err)
	}
	owner.join()
}
