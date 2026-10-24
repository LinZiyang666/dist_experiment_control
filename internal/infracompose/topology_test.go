package infracompose

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrareadiness"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// SYS, INFO, CID and endpoint observations use a real embedded server. The
// independent root census is modeled, so this test cannot certify a running
// stock executable or permission to inspect a managed systemd unit.
type topologyHost struct {
	c.Host
	census        c.Census
	err           error
	reads         int
	drift         bool
	missingBefore bool
}

func (h *topologyHost) Inspect(c.ID, c.Deadline) (c.Census, error) {
	h.reads++
	result := h.census
	if h.drift && h.reads%2 == 0 {
		result.Processes = nil
	}
	if h.missingBefore && h.reads%2 == 1 {
		result.Processes = nil
	}
	return result, h.err
}

// origin: hot-update S9 plan §3.2 / P22 / NATS-COMPOSITION
func TestManagedTopologyBindsSYSAndAcceptedProcess(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseManagedTopology(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "managed topology SYS probes", beforeG)
	testharness.AssertNoFDLeak(t, "managed topology borrowed roots", beforeFD, 0)
}

func exerciseManagedTopology(t *testing.T) {
	t.Helper()
	sys, business := ns.NewAccount("$SYS"), ns.NewAccount("business")
	server, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		ServerName: "managed-topology", JetStream: true, StoreDir: t.TempDir(), SystemAccount: "$SYS",
		Accounts: []*ns.Account{sys, business}, Users: []*ns.User{{Username: "system", Password: "fixture", Account: sys}}})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	defer func() { server.Shutdown(); server.WaitForShutdown() }()
	if !server.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded SYS server did not become ready")
	}
	nc, err := nats.Connect(server.ClientURL(), nats.UserInfo("system", "fixture"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Resolve(c.NATSBinary, nc.ConnectedServerVersion(), "linux", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	process := c.Process{BootID: "physical-boot", PID: 123, StartTicks: 5, InvocationID: "unit-invocation",
		ExeDevice: 7, ExeInode: 8, ExeSHA256: artifact.BinarySHA256, Cgroup: "/system.slice/nats-server.service"}
	accepted := c.Socket{NetNS: 2, Device: 3, Inode: 4, Cookie: 5, Local: nc.ConnectedAddr(), Remote: nc.LocalAddr(), Owner: process}
	host := &topologyHost{census: c.Census{Complete: true, Generation: 1, Digest: c.Digest{1},
		Processes: []c.Process{process}, Sockets: []c.Socket{accepted}}}
	revision := uint64(1)
	source := &ManagedTopology{Standalone: true, Nodes: []ManagedNode{{Domain: c.ID{1}, Host: host, System: nc}},
		Brokers: func(c.Deadline, c.Stop) (uint64, []infrareadiness.Broker, error) { return revision, nil, nil }}
	d := checkpointDeadline(t)
	topology, err := source.ReadTopology(c.Plan{}, d, nil)
	if err != nil || !topology.Complete || len(topology.Servers) != 1 {
		t.Fatal("SYS/accepted process join failed", err)
	}
	observed := topology.Servers[0]
	if observed.ID != server.ID() || observed.Running != process || observed.Host != process.BootID || !observed.JetStream || !observed.Online {
		t.Fatalf("B/S/J or physical failure domain was invented: %+v", observed)
	}
	for _, defect := range []string{"missing_socket", "ambiguous_socket", "wrong_socket", "disk_only", "missing_process", "missing_first_process", "unknown_census", "root_denied", "restart", "no_membership"} {
		t.Run(defect, func(t *testing.T) {
			original := host.census
			originalSource := *source
			defer func() {
				host.census, host.err, host.drift, host.reads = original, nil, false, 0
				host.missingBefore = false
				*source = originalSource
				revision = 1
			}()
			switch defect {
			case "missing_socket":
				host.census.Sockets = nil
			case "ambiguous_socket":
				host.census.Sockets = []c.Socket{accepted, accepted}
			case "wrong_socket":
				socket := accepted
				socket.Remote = "127.0.0.1:1"
				host.census.Sockets = []c.Socket{socket}
			case "disk_only":
				socket := accepted
				socket.Owner.ExeSHA256 = c.Digest{99}
				host.census.Sockets = []c.Socket{socket}
				host.census.Processes = []c.Process{socket.Owner}
			case "missing_process":
				host.census.Processes = nil
			case "missing_first_process":
				host.reads, host.missingBefore = 0, true
			case "unknown_census":
				host.census.Complete = false
			case "root_denied":
				host.err = c.ErrIncomplete
			case "restart":
				host.reads, host.drift = 0, true
			case "no_membership":
				revision = 0
			}
			if topology, err := source.ReadTopology(c.Plan{}, d, nil); err == nil || topology.Complete {
				t.Fatal("incomplete runtime source became a complete topology")
			}
		})
	}
	// A cancelled read has no authority even when every cached tuple matches.
	stop := make(chan struct{})
	close(stop)
	if topology, err := source.ReadTopology(c.Plan{}, d, stop); err == nil || topology.Complete {
		t.Fatal("cancelled topology query published cached success")
	}
}
