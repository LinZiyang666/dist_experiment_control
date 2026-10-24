//go:build linux && amd64

package infralaunch

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type handshakeOwner struct {
	backend   *Backend
	request   c.HostRequest
	transfers []*Prepared
	ack       chan error
	claimed   c.Process
}

func (h *handshakeOwner) Offer(q infrapriv.LaunchQuery, peer c.Peer, _ c.Deadline, _ c.Stop) (c.HostRequest, error) {
	r := h.request
	r.Expected, r.Nonce, r.RemainingNS = q.Expected, q.Nonce, q.RemainingNS
	if peer.PID != q.Expected.PID || peer.StartTicks != q.Expected.StartTicks {
		return r, infrapriv.ErrPermission
	}
	return r, nil
}
func (*handshakeOwner) Accept(r c.HostRequest, p c.Peer, _ []*os.File, _ c.Deadline, _ c.Stop) error {
	if p.PID != r.Expected.PID || p.StartTicks != r.Expected.StartTicks || p.BootID != r.Expected.BootID {
		return infrapriv.ErrPermission
	}
	return nil
}
func (h *handshakeOwner) Reply(r c.HostRequest, _ c.HostReceipt, _ c.Peer, _ c.Deadline, _ c.Stop) ([]*os.File, error) {
	if r.Op != c.LaunchClaim {
		return nil, nil
	}
	prepared, err := h.backend.Transfer(r)
	if err != nil {
		return nil, err
	}
	h.transfers = append(h.transfers, prepared)
	return prepared.Files()
}
func (h *handshakeOwner) Inspect(id c.ID, d c.Deadline) (c.Census, error) {
	return h.backend.Inspect(id, d)
}
func (h *handshakeOwner) Do(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if r.Op == c.LaunchAck {
		// Receiving the signed intent is insufficient. Independently observe the
		// successor before the real backend checks executable identity and OFD.
		for {
			actual, err := infrahost.ReadProcess(r.Expected.PID, d)
			if err == nil && actual == r.Expected {
				break
			}
			if _, err := controlbudget.Remaining(d); err != nil {
				h.ack <- err
				return c.HostReceipt{}, err
			}
			time.Sleep(time.Millisecond)
		} // sleep-fixture: await actual same-PID exec observation.
	}
	receipt, err := h.backend.Do(r, d, stop)
	if r.Op == c.LaunchClaim && err == nil {
		h.claimed = r.Expected
	}
	if r.Op == c.LaunchAck {
		h.ack <- err
	}
	return receipt, err
}

// origin: S9 internal review rv-infra F03 / F04
func TestAuthenticatedLaunchExecHandshake(t *testing.T) {
	if os.Getenv("_INFRA_AUTH_EXEC_CHILD") == "1" {
		authenticatedExecChild(t)
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAuthenticatedExec(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "authenticated launcher same-PID exec", beforeG)
	testharness.AssertNoFDLeak(t, "authenticated launcher SCM_RIGHTS custody", beforeFD, 0)
}
func authenticatedExecChild(t *testing.T) {
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); t.Fatal(err) }
	credentials, socketDir, descriptor := os.NewFile(3, "credentials"), os.NewFile(4, "endpoint"), os.NewFile(5, "identity")
	defer closeTestResource(t, credentials)
	defer closeTestResource(t, socketDir)
	defer closeTestResource(t, descriptor)
	var identity infrapriv.LaunchIdentity
	if err := json.NewDecoder(descriptor).Decode(&identity); err != nil {
		fail(err)
	}
	key, err := infrapriv.ReadCredentialStore(credentials, infrapriv.LocalLaunch, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		fail(err)
	}
	defer closeTestResource(t, key)
	client, err := infrapriv.NewPinnedClient(socketDir, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		fail(err)
	}
	d := launchDeadline(t)
	p, finish, err := AcquireLaunch(client, identity, key, c.NATS, d, nil)
	if err != nil {
		fail(err)
	}
	defer finish()
	defer closeTestResource(t, p)
	if err := p.Exec(d, nil); err != nil {
		fail(err)
	}
	t.Fatal("exec returned without replacing launcher")
}
func exerciseAuthenticatedExec(t *testing.T) {
	t.Helper()
	r, selection, _ := launchFixture(t, stockBinary(t))
	directory, err := os.MkdirTemp("", "auth-exec-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	}()
	anchor, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, anchor)
	if err := infrapriv.InitializeCredentialStore(anchor, uint32(os.Geteuid()), uint32(os.Getegid())); err != nil {
		t.Fatal(err)
	}
	key, err := infrapriv.ReadCredentialStore(anchor, infrapriv.LocalLaunch, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, key)
	policy := infrapriv.Policy{Version: 1, HostBootID: r.HostBootID, Domain: r.Domain, Revision: 1, AccountPublicKey: key.PublicKey(), TrustSHA256: sha256.Sum256([]byte("hermetic namespace trust")), ServiceUID: uint32(os.Geteuid()), ServiceGID: uint32(os.Getegid()), Keys: []infrapriv.PolicyKey{{KeyID: r.KeyID, PublicKey: key.PublicKey(), UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), Scope: infrapriv.LaunchAuthority}}}
	store, err := infrapriv.NewPolicyStore(policy)
	if err != nil {
		t.Fatal(err)
	}
	identity := infrapriv.LaunchIdentity{Version: 1, Domain: r.Domain, PolicyRevision: 1, KeyID: r.KeyID, HostBootID: r.HostBootID, ServiceGID: uint32(os.Getegid())}
	data, _ := json.Marshal(identity)
	if err := os.WriteFile(filepath.Join(directory, "identity"), data, 0600); err != nil {
		t.Fatal(err)
	}
	descriptor, err := os.Open(filepath.Join(directory, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, descriptor)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(directory, "control.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close() //nolint:errcheck // Worker cleanup closes and checks this listener first; this fallback covers earlier setup failures.
	if err := os.Chmod(filepath.Join(directory, "control.sock"), 0660); err != nil {
		t.Fatal(err)
	}
	limit, err := infrapriv.NewPinnedWorkerLimit(anchor)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, limit)
	h := &handshakeOwner{request: r, ack: make(chan error, 1)}
	b, err := NewBackend(Bindings{Authorize: func(request c.HostRequest, _ c.Deadline, _ c.Stop) (Selection, error) {
		if request.Payload != r.Payload || request.Store != r.Store || request.Candidate != r.Candidate || request.Config != r.Config {
			return Selection{}, ErrLaunchRejected
		}
		return selection, nil
	}, Process: func(expected c.Process, d c.Deadline) (c.Process, error) {
		return infrahost.ReadProcess(expected.PID, d)
	}, StoreCensus: func(c.Store, c.Deadline, c.Stop) (c.Census, error) { return emptyCensus(), nil }, Inspect: func(c.ID, c.Deadline) (c.Census, error) { return emptyCensus(), nil }})
	if err != nil {
		t.Fatal(err)
	}
	h.backend = b
	defer closeTestResource(t, b)
	defer func() {
		for _, p := range h.transfers {
			closeTestResource(t, p)
		}
	}()
	server, err := infrapriv.NewServer(store, h, limit, h)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	deadline := launchDeadline(t)
	go func() {
		for n := 0; n < 3; n++ {
			conn, err := listener.AcceptUnix()
			if err != nil {
				done <- err
				return
			}
			err = server.ServeConn(conn, deadline, nil)
			if err != nil && n != 2 {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() {
		closeTestResource(t, listener)
		select {
		case <-done:
		case <-time.After(25 * time.Second):
			t.Error("management workers did not join")
		}
	}()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestAuthenticatedLaunchExecHandshake$", "-test.count=1")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "INVOCATION_ID=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "_INFRA_AUTH_EXEC_CHILD=1", "INVOCATION_ID="+strings.Repeat("a", 32), "GORACE=atexit_sleep_ms=0")
	cmd.ExtraFiles = []*os.File{anchor, anchor, descriptor}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close() //nolint:errcheck // Wait closes this pipe; this fallback covers failures before StartProcess.
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close() //nolint:errcheck // Wait closes this pipe; this fallback covers failures before StartProcess.
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		// A deliberate failure kill is joined too.
		// Only the resulting ExitError is expected during cleanup.
		var exit *exec.ExitError
		if err := wait(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.As(err, &exit) {
			t.Error(err)
		}
	}()
	var witness struct {
		PID         int
		Ticks       uint64
		LockRefs    int
		ForeignRefs int
		Environment string
		SHA         [32]byte
		Race        bool
	}
	if err := json.NewDecoder(output).Decode(&witness); err != nil {
		_ = cmd.Process.Kill()
		_ = wait() // Join stderr copying before reading the diagnostic buffer.
		t.Fatalf("stock-shaped exec did not report: %v %s", err, diagnostic.String())
	}
	select {
	case err := <-h.ack:
		if err != nil {
			t.Fatal("post-exec Ack failed", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Ack observation did not complete")
	}
	if witness.PID != int(h.claimed.PID) || witness.Ticks != h.claimed.StartTicks || witness.SHA != r.Candidate.BinarySHA256 || witness.LockRefs != 1 || !witness.Race {
		t.Fatalf("exec changed identity or lost the inherited OFD: %+v", witness)
	}
	assertStoreBusy(t, selection.Directory, selection.Locks)
	if _, err := input.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatalf("stock-shaped child failed: %v %s", err, diagnostic.String())
	}
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
