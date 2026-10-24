package infrahost

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

const identityInvocation = "0123456789abcdef0123456789abcdef"
const identityRole = "_TETHER_INFRA_IDENTITY_ROLE"

type socketReceipt struct {
	Process  infracontract.Process
	Raw      infracontract.Socket
	Accepted infracontract.Socket
	Address  string
}

// Private test receipts preserve physical before/after evidence. They explicitly
// distinguish the real child/socket sandbox from P31's full privileged census;
// a passing unit test is never a systemd/nft deployment receipt.
type identityReceipt struct {
	Version  uint16
	Name     string
	Scope    string
	Before   []infracontract.Process
	After    []infracontract.Process
	Old      infracontract.Attachment
	New      infracontract.Attachment
	Accepted infracontract.Socket
	Census   infracontract.Census
}

func writeIdentityReceipt(t *testing.T, receipt identityReceipt) {
	t.Helper()
	root := os.Getenv("_TETHER_INFRA_IDENTITY_RECEIPTS")
	if root == "" {
		return
	}
	receipt.Version = 1
	body, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, receipt.Name+".json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

// The supervisor dispatches before this hook. Fixture argv is deliberately not
// -test.*; an ordinary receiver must not depend on the Go testing flag parser.
func init() {
	role := os.Getenv(identityRole)
	if role == "" {
		return
	}
	if role == "duplicate-invocation" {
		// execve preserves duplicate raw environment entries; exec.Cmd normalizes
		// them. This adversary requires the actual ambiguous /proc environment.
		if err := syscall.Exec(os.Args[0], os.Args, []string{identityRole + "=idle-duplicate", "INVOCATION_ID=", "INVOCATION_ID=" + identityInvocation, "GORACE=atexit_sleep_ms=0"}); err != nil {
			panic(err)
		}
	}
	if role == "idle-duplicate" {
		if err := json.NewEncoder(os.Stdout).Encode(socketReceipt{}); err != nil {
			panic(err)
		}
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		os.Exit(0)
	}
	if role != "socket" && role != "backlog" && role != "dual-socket" {
		os.Exit(126)
	}
	network, address := "tcp4", "127.0.0.1:0"
	if role == "dual-socket" {
		network, address = "tcp", "[::]:0"
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		panic(err)
	}
	defer func() { _ = listener.Close() }()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		panic(err)
	}
	raw, err := net.Dial("tcp4", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		panic(err)
	}
	defer func() { _ = raw.Close() }()
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		panic(err)
	}
	p, err := ReadProcess(int32(os.Getpid()), deadline)
	if err != nil {
		panic(err)
	}
	receipt := socketReceipt{Process: p, Address: listener.Addr().String()}
	if role == "socket" || role == "dual-socket" {
		accepted, e := listener.Accept()
		if e != nil {
			panic(e)
		}
		defer func() { _ = accepted.Close() }()
		receipt.Raw = fixtureSocket(raw.(*net.TCPConn), p, deadline)
		receipt.Accepted = fixtureSocket(accepted.(*net.TCPConn), p, deadline)
	}
	if err = json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		panic(err)
	}
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	os.Exit(0)
}

func fixtureSocket(conn *net.TCPConn, p infracontract.Process, deadline infracontract.Deadline) infracontract.Socket {
	raw, err := conn.SyscallConn()
	if err != nil {
		panic(err)
	}
	var socket infracontract.Socket
	var readErr error
	if err = raw.Control(func(fd uintptr) { socket, readErr = ReadSocket(int(fd), p, deadline) }); err != nil || readErr != nil {
		panic("fixture socket identity failed")
	}
	return socket
}

func identityDeadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

type identityChild struct {
	cmd    *exec.Cmd
	input  *os.File
	wait   func() error
	joined bool
}

func startIdentityChild(t *testing.T, path string, args ...string) *identityChild {
	t.Helper()
	cmd := exec.Command(path, args...)
	cmd.Env = []string{"INVOCATION_ID=" + identityInvocation, "GORACE=atexit_sleep_ms=0"}
	return launchIdentityChild(t, cmd)
}

func launchIdentityChild(t *testing.T, cmd *exec.Cmd) *identityChild {
	t.Helper()
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = input
	wait, err := testharness.StartProcess(cmd)
	_ = input.Close()
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	child := &identityChild{cmd: cmd, input: writer, wait: wait}
	t.Cleanup(func() { child.finish(t) })
	return child
}

func (c *identityChild) finish(t *testing.T) {
	t.Helper()
	if c.joined {
		return
	}
	c.joined = true
	_ = c.input.Close()
	_ = c.cmd.Process.Kill()
	if err := c.wait(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Errorf("supervised child join: %v", err)
		}
	}
}

func launchSocketChild(t *testing.T, role string) (*identityChild, socketReceipt) {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	cmd := exec.Command(path, "infra-identity-fixture")
	cmd.Env = []string{"INVOCATION_ID=" + identityInvocation, identityRole + "=" + role, "GORACE=atexit_sleep_ms=0"}
	cmd.Stdout, cmd.Stderr = output, os.Stderr
	child := launchIdentityChild(t, cmd)
	_ = output.Close()
	if err = reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var receipt socketReceipt
	if err = json.NewDecoder(reader).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	return child, receipt
}

func copyIdentityExecutable(t *testing.T, source, destination string) infracontract.Digest {
	t.Helper()
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(destination, body, 0700); err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(body)
}

func testCensus(processes ...infracontract.Process) infracontract.Census {
	c := infracontract.Census{Complete: true, Generation: 1, Processes: processes, LiveBusiness: 4}
	c.Digest = testCensusDigest(c)
	return c
}

func testCensusDigest(c infracontract.Census) infracontract.Digest {
	c.Digest = infracontract.Digest{}
	body, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return sha256.Sum256(body)
}

// origin: hot-update S9 plan §2.3 / T05
func TestInfraProcessIncarnationCensus(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		if _, err := ReadProcess(1, identityDeadline(t)); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported platform: %v", err)
		}
		if _, err := ReadCensus(CensusScope{}, identityDeadline(t)); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported census: %v", err)
		}
		if _, err := PinProcess(infracontract.Process{}, identityDeadline(t)); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported pin: %v", err)
		}
		if _, err := ReadSocket(0, infracontract.Process{}, identityDeadline(t)); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported socket: %v", err)
		}
		if err := VerifyRetired(nil, infracontract.Census{}, false); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported retirement: %v", err)
		}
		if err := VerifyIngressProof(infracontract.Socket{}, infracontract.Socket{}, infracontract.Process{}, infracontract.Census{}); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported ingress: %v", err)
		}
		if _, err := NewIdentityBackend(CensusScope{}); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported backend: %v", err)
		}
		return
	}
	t.Run("running_image_survives_disk_promotion", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "server")
		oldSHA := copyIdentityExecutable(t, "/bin/sleep", path)
		oldChild := startIdentityChild(t, path, "30")
		old, err := ReadProcess(int32(oldChild.cmd.Process.Pid), identityDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if old.ExeSHA256 != oldSHA {
			t.Fatal("running image is not the actual executable")
		}
		newSHA := copyIdentityExecutable(t, "/bin/sh", path+".new")
		if newSHA == oldSHA {
			t.Fatal("fixture images must differ")
		}
		if err = os.Rename(path+".new", path); err != nil {
			t.Fatal(err)
		}
		current, err := ReadProcess(old.PID, identityDeadline(t))
		if err != nil || current != old {
			t.Fatalf("disk promotion changed observed running image: %v", err)
		}
		if err = VerifyRetired([]infracontract.Process{old}, testCensus(current), false); err == nil {
			t.Fatal("disk-only promotion retired a live owner")
		}
		pin, err := PinProcess(old, identityDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pin.Close() }()
		badProcesses := []infracontract.Process{old, old, old, old, old, old, old, old}
		badProcesses[0].BootID += "wrong"
		badProcesses[1].StartTicks++
		badProcesses[2].InvocationID = "ffffffffffffffffffffffffffffffff"
		badProcesses[3].ExeDevice++
		badProcesses[4].ExeInode++
		badProcesses[5].ExeSHA256 = newSHA
		badProcesses[6].Cgroup += "/other"
		badProcesses[7].InvocationID = ""
		for _, bad := range badProcesses {
			if p, e := PinProcess(bad, identityDeadline(t)); e == nil {
				_ = p.Close()
				t.Fatal("mismatched expected identity was pinned")
			}
		}
		stop := make(chan struct{})
		close(stop)
		if err = pin.Signal(infracontract.SignalStop, identityDeadline(t), stop); err == nil {
			t.Fatal("cancelled signal applied")
		}
		if err = pin.Signal(infracontract.SignalStop, 1, nil); err == nil {
			t.Fatal("expired signal applied")
		}
		if err = pin.Signal(infracontract.Stage, identityDeadline(t), nil); !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("arbitrary operation: %v", err)
		}
		if exited, e := pin.Exited(identityDeadline(t)); e != nil || exited {
			t.Fatal("live pinned child reported exited")
		}
		// A legitimate rollback can run the exact original SHA in a new process.
		copyIdentityExecutable(t, "/bin/sleep", path+".rollback")
		rollbackChild := startIdentityChild(t, path+".rollback", "30")
		rollback, e := ReadProcess(int32(rollbackChild.cmd.Process.Pid), identityDeadline(t))
		if e != nil || rollback.ExeSHA256 != old.ExeSHA256 || rollback.PID == old.PID {
			t.Fatal("rollback premise failed")
		}
		if err = pin.Signal(infracontract.SignalStop, identityDeadline(t), nil); err != nil {
			t.Fatal(err)
		}
		oldChild.finish(t)
		if exited, e := pin.Exited(identityDeadline(t)); e != nil || !exited {
			t.Fatal("dead pinned child remained live")
		}
		if err = VerifyRetired([]infracontract.Process{old}, testCensus(rollback), false); err != nil {
			t.Fatalf("same-SHA rollback refused: %v", err)
		}
		writeIdentityReceipt(t, identityReceipt{Name: "running-image-and-rollback", Scope: "real supervised processes; actual exe inode/SHA and pinned old death; no privileged cgroup/conntrack claim", Before: []infracontract.Process{old}, After: []infracontract.Process{current, rollback}})
		wrongBootBefore := old
		wrongBootBefore.BootID += "wrong"
		for _, before := range [][]infracontract.Process{nil, {infracontract.Process{}}, {wrongBootBefore}} {
			if err = VerifyRetired(before, testCensus(rollback), false); err == nil {
				t.Fatal("unknown before set certified retirement")
			}
		}
		empty := testCensus()
		incomplete := testCensus(rollback)
		incomplete.Complete = false
		incomplete.Digest = testCensusDigest(incomplete)
		corrupt := testCensus(rollback)
		corrupt.Generation++
		for _, bad := range []infracontract.Census{{}, empty, incomplete, corrupt} {
			if err = VerifyRetired([]infracontract.Process{old}, bad, false); err == nil {
				t.Fatal("incomplete/empty/corrupt census certified retirement")
			}
		}
		bad := testCensus(rollback)
		bad.OldOwnerPending = 1
		bad.Digest = testCensusDigest(bad)
		if err = VerifyRetired([]infracontract.Process{old}, bad, false); err == nil {
			t.Fatal("pending old responsibility retired")
		}
		rollbackChild.finish(t)
	})
	t.Run("same_pid_exec_retires_image_only", func(t *testing.T) {
		child := startIdentityChild(t, "/bin/sh", "-c", "read gate; exec /bin/sleep 30")
		before, err := ReadProcess(int32(child.cmd.Process.Pid), identityDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		pin, err := PinProcess(before, identityDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = pin.Close() }()
		if _, err = io.WriteString(child.input, "exec\n"); err != nil {
			t.Fatal(err)
		}
		var after infracontract.Process
		if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
			p, e := ReadProcess(before.PID, identityDeadline(t))
			if e == nil && p.ExeSHA256 != before.ExeSHA256 {
				after = p
				return true
			}
			return false
		}) {
			t.Fatal("same PID executable replacement was not observed")
		}
		if before.PID != after.PID || before.StartTicks != after.StartTicks || before.InvocationID != after.InvocationID {
			t.Fatal("exec did not retain process incarnation")
		}
		if err = VerifyRetired([]infracontract.Process{before}, testCensus(after), true); err != nil {
			t.Fatalf("launcher image retirement: %v", err)
		}
		if err = VerifyRetired([]infracontract.Process{before}, testCensus(after), false); err == nil {
			t.Fatal("server exec counted as server exit")
		}
		if err = pin.Signal(infracontract.SignalGraceful, identityDeadline(t), nil); err == nil {
			t.Fatal("stale image received graceful signal")
		}
		writeIdentityReceipt(t, identityReceipt{Name: "same-pid-exec", Scope: "real supervised same-PID shell exec; image retirement only", Before: []infracontract.Process{before}, After: []infracontract.Process{after}})
		child.finish(t)
	})
	t.Run("unknown_invocation_is_incomplete", func(t *testing.T) {
		cmd := exec.Command("/bin/sleep", "30")
		cmd.Env = []string{}
		child := launchIdentityChild(t, cmd)
		if _, err := ReadProcess(int32(child.cmd.Process.Pid), identityDeadline(t)); !errors.Is(err, infracontract.ErrIncomplete) {
			t.Fatalf("anonymous process accepted: %v", err)
		}
		child.finish(t)
		duplicate, _ := launchSocketChild(t, "duplicate-invocation")
		if _, err := ReadProcess(int32(duplicate.cmd.Process.Pid), identityDeadline(t)); !errors.Is(err, infracontract.ErrIncomplete) {
			t.Fatalf("ambiguous raw invocation environment accepted: %v", err)
		}
		duplicate.finish(t)
	})
}

// origin: hot-update S9 plan §12 P02 / T05
func TestInfraIdentityLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return
	}
	// Warm the runtime's pipe poller before measuring observer-owned resources.
	exerciseIdentity(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseIdentity(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra identity x5", beforeG)
	testharness.AssertNoFDLeak(t, "infra identity x5", beforeFD, 0)
}

func exerciseIdentity(t *testing.T) {
	t.Helper()
	child, receipt := launchSocketChild(t, "socket")
	actual, err := ReadProcess(receipt.Process.PID, identityDeadline(t))
	if err != nil || actual != receipt.Process {
		t.Fatal("independent process observation disagrees")
	}
	pin, err := PinProcess(actual, identityDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PinProcess(infracontract.Process{}, identityDeadline(t)); err == nil {
		t.Fatal("empty expected process accepted")
	}
	if err = pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err = pin.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = pin.Exited(identityDeadline(t)); err == nil {
		t.Fatal("closed pidfd was used")
	}
	child.finish(t)
}
