package transferharness

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §10.1 / L16
func TestTransferClusterInfrastructureKeepsPrivateStores(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, topology := range []string{"sole-voter", "quorum"} {
			t.Run(fmt.Sprintf("round-%d/%s", round, topology), func(t *testing.T) { exerciseTransferClusterInfrastructure(t, topology) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "transfer cluster infrastructure", g)
	testharness.AssertNoFDLeak(t, "transfer cluster infrastructure", fd, 0)
}
func exerciseTransferClusterInfrastructure(t *testing.T, topology string) {
	root := t.TempDir()
	seedDB := filepath.Join(root, "seed.db")
	db, err := storage.Open(seedDB)
	clusterMust(t, err)
	clusterMust(t, db.Close())
	authority := filepath.Join(root, "authority")
	clusterMust(t, os.Mkdir(authority, 0700))
	user, err := auth.GenerateUserSeed()
	clusterMust(t, err)
	account, err := nkeys.CreateAccount()
	clusterMust(t, err)
	defer account.Wipe()
	seed, err := account.Seed()
	clusterMust(t, err)
	clusterMust(t, os.WriteFile(filepath.Join(authority, "broker.nk"), user, 0600))
	clusterMust(t, os.WriteFile(filepath.Join(authority, "account.nk"), seed, 0600))
	c := NewCluster(t, root, topology, seedDB, authority)
	defer c.Close()
	expected := 1
	if topology == "quorum" {
		expected = 3
	}
	if c.Replicas != expected || len(c.Peers) != expected || len(c.Servers) != expected {
		t.Fatal("requested topology lost its real members")
	}
	paths := make(map[string]bool)
	for i, p := range c.Peers {
		for _, path := range []string{p.Root, p.DB, p.Binary, p.Admin, p.Secrets, filepath.Join(p.Root, "raft", "raft.db"), filepath.Join(p.Root, "jetstream")} {
			if paths[path] || !filepath.IsAbs(path) {
				t.Fatal("cluster members share private storage/custody paths")
			}
			paths[path] = true
		}
		if p.ID != c.Servers[i].Name() {
			t.Fatal("NATS ingress has the wrong member identity")
		}
		if _, err := os.Stat(p.DB); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(p.Root, "raft", "raft.db")); err != nil {
			t.Fatal(err)
		}
	}
	js, err := jetstream.New(c.connections[0])
	clusterMust(t, err)
	clusterMust(t, jsstream.EnsureHistoryStream(t.Context(), js, "infrastructure", expected))
	stream, err := js.Stream(t.Context(), jsstream.HistoryStreamName("infrastructure"))
	clusterMust(t, err)
	info, err := stream.Info(t.Context())
	clusterMust(t, err)
	if info.Config.Replicas != expected || expected == 3 && (info.Cluster == nil || len(info.Cluster.Replicas) != 2) {
		t.Fatal("history does not have the requested real replica group")
	}
}

// A broker opens its outbound NATS connection before binding its tunnel. The
// old release-and-bind fixture put both sockets in the same loopback namespace,
// allowing the kernel to choose the released tunnel port for that outbound FD.
// origin: S7 internal review INTEG-O F2 / clustered fixture port census
func TestTransferClusterListenerPortsCannotBeClaimedByOutboundNATS(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("kernel loopback namespace witness requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseTransferClusterPortIsolation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer cluster listener namespace", g)
	testharness.AssertNoFDLeak(t, "transfer cluster listener namespace", fd, 0)
}
func exerciseTransferClusterPortIsolation(t *testing.T) {
	root := t.TempDir()
	db, err := storage.Open(filepath.Join(root, "seed.db"))
	clusterMust(t, err)
	clusterMust(t, db.Close())
	authority := filepath.Join(root, "authority")
	clusterMust(t, os.Mkdir(authority, 0700))
	user, err := auth.GenerateUserSeed()
	clusterMust(t, err)
	account, err := nkeys.CreateAccount()
	clusterMust(t, err)
	defer account.Wipe()
	seed, err := account.Seed()
	clusterMust(t, err)
	clusterMust(t, os.WriteFile(filepath.Join(authority, "broker.nk"), user, 0600))
	clusterMust(t, os.WriteFile(filepath.Join(authority, "account.nk"), seed, 0600))
	c := NewCluster(t, root, "sole-voter", filepath.Join(root, "seed.db"), authority)
	defer c.Close()
	p := c.Peers[0]
	clusterMust(t, p.ReleasePorts())
	_, number, err := net.SplitHostPort(p.Tunnel)
	clusterMust(t, err)
	port, err := strconv.Atoi(number)
	clusterMust(t, err)
	dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}}
	outbound, err := dialer.DialContext(t.Context(), "tcp4", c.Servers[0].Addr().String())
	clusterMust(t, err)
	defer func() {
		if err := outbound.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	listener, err := net.Listen("tcp4", p.Tunnel)
	if err != nil {
		t.Fatalf("outbound NATS socket claimed the reserved tunnel listener: %v", err)
	}
	clusterMust(t, listener.Close())
}

// A reservation on a private loopback address does not establish that the
// same port is free on the outbound client's address. Hold both namespaces
// until launch, so the deliberate outbound bind cannot hit an unrelated FD.
// origin: S7 internal review TRANSFER-MAKETEST F5 / S7 plan §10 / L16
func TestTransferClusterReservationsExcludeOutboundPorts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("kernel loopback namespace witness requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferClusterReservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer cluster port reservations", g)
	testharness.AssertNoFDLeak(t, "transfer cluster port reservations", fd, 0)
}

func exerciseTransferClusterReservation(t *testing.T) {
	t.Helper()
	p := &ClusterPeer{Host: clusterListenerHost(t)}
	address := p.reserve(t)
	defer func() { clusterMust(t, p.ReleasePorts()) }()
	host, port, err := net.SplitHostPort(address)
	clusterMust(t, err)
	if host != p.Host {
		t.Fatalf("reservation advertised a foreign listener host: %s", address)
	}
	other, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", port))
	if err == nil {
		clusterMust(t, other.Close())
		t.Fatal("cluster reservation left its outbound namespace available to an unrelated socket")
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("outbound reservation witness failed for another reason: %v", err)
	}
}
