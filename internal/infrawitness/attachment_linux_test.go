//go:build linux

package infrawitness

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/testharness"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type witnessDialer struct{ raw net.Conn }

func (d *witnessDialer) Dial(network, address string) (net.Conn, error) {
	raw, err := net.Dial(network, address)
	d.raw = raw
	return raw, err
}

func limit(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// origin: S9 internal review NC-EXPORTS handoff physical witness; plan §2.3 / T06
func TestPhysicalWitnessBindsOriginalSocketAndIndependentHealth(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "physical attachment", goroutines)
	defer testharness.AssertNoFDLeak(t, "physical attachment", fds, 0)
	if os.Getenv("_TETHER_WITNESS_CHILD") == "" {
		path, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(path, "-test.run=^TestPhysicalWitnessBindsOriginalSocketAndIndependentHealth$", "-test.count=1")
		cmd.Env = append(os.Environ(), "_TETHER_WITNESS_CHILD=1", "INVOCATION_ID=0123456789abcdef0123456789abcdef", "GORACE=atexit_sleep_ms=0")
		if output, err := testharness.CombinedOutput(cmd); err != nil {
			t.Fatalf("witness sandbox: %v\n%s", err, output)
		}
		return
	}
	for round := 0; round < 5; round++ {
		exercisePhysicalWitness(t)
	}
}

func exercisePhysicalWitness(t *testing.T) {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready")
	}
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	// This sandbox's independent ingress is a real TCP proxy. Its accepted fd
	// is observed separately from the SDK dial fd; no deployed cgroup claim is
	// made by the fixture's explicitly scoped census of these known sockets.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, listener)
	accepted := make(chan net.Conn, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		incoming, err := listener.Accept()
		if err != nil {
			return
		}
		defer closeTestResource(t, incoming)
		outgoing, err := net.Dial("tcp", s.Addr().String())
		if err != nil {
			return
		}
		defer closeTestResource(t, outgoing)
		accepted <- incoming
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(struct{ io.Writer }{outgoing}, struct{ io.Reader }{incoming})
			_ = outgoing.Close()
		}()
		_, _ = io.Copy(struct{ io.Writer }{incoming}, struct{ io.Reader }{outgoing})
		_ = incoming.Close()
		wg.Wait()
	}()
	dialer := &witnessDialer{}
	nc, err := nats.Connect("nats://"+listener.Addr().String(), nats.SetCustomDialer(dialer), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { nc.Close(); <-done }()
	incoming := <-accepted
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	owner, err := infrahost.ReadProcess(int32(os.Getpid()), limit(t))
	if err != nil {
		t.Fatal(err)
	}
	fd, err := incoming.(syscall.Conn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var acceptedSocket c.Socket
	var readErr error
	if err = fd.Control(func(fd uintptr) { acceptedSocket, readErr = infrahost.ReadSocket(int(fd), owner, limit(t)) }); err != nil || readErr != nil {
		t.Fatalf("accepted socket: %v %v", err, readErr)
	}
	census := c.Census{Complete: true, Generation: 9, Processes: []c.Process{owner}, Sockets: []c.Socket{acceptedSocket}}
	data, _ := json.Marshal(census)
	census.Digest = sha256.Sum256(data)
	var defect string
	witness, err := New(func(raw c.Socket, server string, cid uint64, d c.Deadline) (c.Socket, c.Census, uint64, error) {
		if raw.Cookie == 0 || server != s.ID() || cid == 0 {
			t.Error("independent ingress query lost raw/session identity")
		}
		socket, current, health := acceptedSocket, census, uint64(79)
		switch defect {
		case "empty":
			current.Sockets = nil
		case "cookie":
			socket.Cookie++
		case "owner":
			socket.Owner.StartTicks++
		case "tuple":
			socket.Remote = socket.Local
		case "health":
			health = 0
		case "closed":
			_ = dialer.raw.Close()
		}
		return socket, current, health, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client, broker := infraattach.Config{}, infraattach.BrokerInfra{}
	if err = witness.BindClient(&client); err != nil {
		t.Fatal(err)
	}
	if err = witness.BindBroker(&broker); err != nil {
		t.Fatal(err)
	}
	for _, observe := range []func(*nats.Conn, net.Conn, c.ID, uint64, c.Deadline) (c.Attachment, error){client.Observe, broker.Observe} {
		row, err := observe(nc, dialer.raw, c.ID{1}, 3, limit(t))
		if err != nil || row.Raw.Cookie == 0 || row.Raw.Inode == acceptedSocket.Inode || row.Ingress != owner || row.HealthGeneration != 79 || row.Generation != 3 {
			t.Fatalf("actual physical witness: %v %#v", err, row)
		}
	}
	for _, defect = range []string{"empty", "cookie", "owner", "tuple", "health", "closed"} {
		if _, err = witness.Observe(nc, dialer.raw, c.ID{1}, 3, limit(t)); !errors.Is(err, c.ErrIncomplete) {
			t.Fatalf("unproven %s accepted: %v", defect, err)
		}
	}
}

// closeTestResource also accepts sockets already closed to interrupt and join the proxy pumps.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Error(err)
	}
}
