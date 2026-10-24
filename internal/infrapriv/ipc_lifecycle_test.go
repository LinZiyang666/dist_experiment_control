//go:build linux && amd64

// Real Unix sockets, SO_PEERCRED/pidfd, shared flocks and cancellation workers.
// The Host records effects; no systemd/nft/process mutation is substituted here.
// Real root/multiple UIDs and process-parent death supervision belong to P31/P27.
package infrapriv

import (
	"crypto/sha256"
	"encoding/binary"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §12 P03 / TestInfraIPCLifecycle
func TestInfraIPCLifecycle(t *testing.T) {
	// Initialize the runtime's permanent netpoll descriptors before taking the
	// baseline; they belong to the runtime, while every IPC OFD must be retired.
	warmA, warmB := unixPair(t)
	_ = warmA.Close()
	_ = warmB.Close()
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseIPC(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra IPC", beforeG)
	testharness.AssertNoFDLeak(t, "infra IPC", beforeFD, 0)
}

func exerciseIPC(t *testing.T) {
	t.Helper()
	exerciseFileExchange(t)
	server, request, limit, host := ipcFixture(t)
	defer closeWorkerLimit(t, limit)
	client, accepted := unixPair(t)
	defer func() { _ = client.Close() }()
	result := serveAsync(server, accepted, testDeadline(t, WorkerBudget), nil)
	data, err := c.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(client, RequestPurpose, data); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	encoded, err := readFrame(client, ReceiptPurpose)
	if err != nil {
		t.Fatalf("receipt read: %v; server: %v", err, awaitServe(t, result))
	}
	receipt, err := c.DecodeReceipt(encoded)
	if err != nil || receipt.RequestDigest != sha256.Sum256(data) {
		t.Fatalf("receipt: %v", err)
	}
	if err := awaitServe(t, result); err != nil {
		t.Fatal(err)
	}
	if host.calls() != 1 {
		t.Fatal("one request did not cause exactly one host call")
	}
	remaining, err := controlbudget.Remaining(host.lastDeadline())
	if err != nil || remaining > ActionBudget {
		t.Fatalf("backend lacks bounded action budget: %v %v", remaining, err)
	}
	exerciseWorkerSlots(t, limit)
	// Ancillary refusal is also repeated under the direct FD leak assertion,
	// so dropping close(receivedFD) cannot hide behind a successful byte refusal.
	fdClient, fdAccepted := unixPair(t)
	file, err := os.CreateTemp(t.TempDir(), "untrusted-input")
	if err != nil {
		t.Fatal(err)
	}
	fdResult := serveAsync(server, fdAccepted, testDeadline(t, time.Second), nil)
	_, _, sendErr := fdClient.WriteMsgUnix(testFrame(RequestPurpose, data), unix.UnixRights(int(file.Fd())), nil)
	if sendErr != nil {
		_ = fdClient.Close()
		_ = file.Close()
		t.Fatal(sendErr)
	}
	_ = fdClient.CloseWrite()
	fdErr := awaitServe(t, fdResult)
	_ = fdClient.Close()
	_ = file.Close()
	if fdErr == nil {
		t.Fatal("ancillary input admitted")
	}
	for _, cancelMode := range []string{"stop-partial-read", "deadline-partial-read", "stop-host", "deadline-host"} {
		client, accepted := unixPair(t)
		stop := make(chan struct{})
		dl := testDeadline(t, 100*time.Millisecond)
		started := make(chan struct{})
		if strings.HasSuffix(cancelMode, "host") {
			host.mu.Lock()
			host.onDo = func(_ c.HostRequest, deadline c.Deadline, workStop c.Stop) (c.HostReceipt, error) {
				close(started)
				left, err := controlbudget.Remaining(deadline)
				if err != nil {
					return c.HostReceipt{}, err
				}
				timer := time.NewTimer(left)
				defer timer.Stop()
				select {
				case <-workStop:
				case <-timer.C:
				}
				return c.HostReceipt{}, controlbudget.ErrExpired
			}
			host.mu.Unlock()
		}
		result := serveAsync(server, accepted, dl, stop)
		if strings.HasSuffix(cancelMode, "host") {
			if err := writeFrame(client, RequestPurpose, data); err != nil {
				_ = client.Close()
				t.Fatal(err)
			}
			_ = client.CloseWrite()
			select {
			case <-started:
			case <-time.After(time.Second):
				_ = client.Close()
				t.Fatal("host not entered")
			}
		} else {
			if _, err := client.Write([]byte{'T'}); err != nil {
				_ = client.Close()
				t.Fatal(err)
			}
		}
		if strings.HasPrefix(cancelMode, "stop") {
			close(stop)
		}
		if err := awaitServe(t, result); err == nil {
			_ = client.Close()
			t.Fatal("cancelled worker succeeded")
		}
		_ = client.Close()
		host.mu.Lock()
		host.onDo = nil
		host.mu.Unlock()
	}
}

type recordingHost struct {
	mu       sync.Mutex
	count    int
	deadline c.Deadline
	onDo     func(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error)
}

func (h *recordingHost) Do(r c.HostRequest, deadline c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	h.mu.Lock()
	h.count++
	h.deadline = deadline
	fn := h.onDo
	h.mu.Unlock()
	if fn != nil {
		return fn(r, deadline, stop)
	}
	return h.receipt(r), nil
}
func (*recordingHost) Inspect(c.ID, c.Deadline) (c.Census, error) {
	return c.Census{}, c.ErrUnsupported
}
func (*recordingHost) receipt(r c.HostRequest) c.HostReceipt {
	data, _ := c.EncodeRequest(r)
	return c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: 1, Op: r.Op, RequestDigest: sha256.Sum256(data), Journal: c.Digest{9}, Code: "test_observed"}
}
func (h *recordingHost) calls() int { h.mu.Lock(); defer h.mu.Unlock(); return h.count }
func (h *recordingHost) lastDeadline() c.Deadline {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deadline
}

func ipcFixture(t *testing.T) (*Server, c.HostRequest, *WorkerLimit, *recordingHost) {
	return ipcFixtureWithBudget(t, uint64(5*time.Second))
}

func ipcFixtureWithBudget(t *testing.T, remaining uint64) (*Server, c.HostRequest, *WorkerLimit, *recordingHost) {
	t.Helper()
	policy, keys := policyFixture(t)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	policy.HostBootID = strings.TrimSpace(string(boot))
	policy.ServiceUID, policy.ServiceGID = uint32(os.Geteuid()), uint32(os.Getegid())
	for i := range policy.Keys {
		if policy.Keys[i].Scope != RootAuthority {
			policy.Keys[i].UID, policy.Keys[i].GID = policy.ServiceUID, policy.ServiceGID
		}
	}
	store, err := NewPolicyStore(policy)
	if err != nil {
		t.Fatal(err)
	}
	limit := &WorkerLimit{dir: openTestDirectory(t, t.TempDir()), owner: uint32(os.Geteuid())}
	host := &recordingHost{}
	server, err := NewServer(store, host, limit)
	if err != nil {
		_ = limit.Close()
		t.Fatal(err)
	}
	request := requestFixture(c.Inspect)
	request.HostBootID = policy.HostBootID
	request.Expected.BootID = policy.HostBootID
	request.RemainingNS = remaining
	request = signedRequest(t, request, "local", keys[LocalAuthority])
	return server, request, limit, host
}

// closeWorkerLimit is the deferred close of a fixture's WorkerLimit. Close is
// idempotent, so an error here means the slot directory handle was closed
// behind the limit's back: the bookkeeping fault a discarded result would hide.
func closeWorkerLimit(t *testing.T, limit *WorkerLimit) {
	t.Helper()
	if err := limit.Close(); err != nil {
		t.Errorf("close worker limit: %v", err)
	}
}

func unixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	connections := make([]*net.UnixConn, 0, 2)
	for _, fd := range fds {
		file := os.NewFile(uintptr(fd), "test-unix-peer")
		conn, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn.(*net.UnixConn))
	}
	return connections[0], connections[1]
}

func testFrame(purpose string, data []byte) []byte {
	frame := make([]byte, 8+len(data))
	copy(frame, purpose)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(data)))
	copy(frame[8:], data)
	return frame
}

func serveAsync(server *Server, conn *net.UnixConn, deadline c.Deadline, stop c.Stop) <-chan error {
	result := make(chan error, 1)
	go func() { result <- server.ServeConn(conn, deadline, stop) }()
	return result
}

func awaitServe(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("worker failed to join")
		return ErrIPC
	}
}
