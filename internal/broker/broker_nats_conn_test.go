// broker_nats_conn_test.go — the broker's OWN NATS connection stays on the server it was
// configured for (gotcha #89).
//
// The failure this pins is silent and was live for months: nats.go adds every server the cluster
// advertises to the reconnect pool, and on a disconnect tries the other entries before the one it
// was on. So a broker whose local nats-server bounced (the topology reconciler's staggered hard
// restart, an operator restart, an upgrade) came back on a PEER's server and never returned — its
// own server was left without an auth_callout responder or a ctl-queue member, and drill 96.D read
// the roamed broker's successful forward (through the peer's server, around the partition of its
// own routes and raft) as a minority commit. The test runs the SAME script twice: once with the
// broker's real options (must rejoin its own server) and once with the pre-fix option set (must
// roam — the control that proves the fixture can see a roam at all).
package broker

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// routedPair starts two routed embedded servers named A and B on random ports and returns them
// together with A's options (so A can be restarted on the SAME ports later).
func routedPair(t *testing.T) (a, b *natsserver.Server, optsA natsserver.Options) {
	t.Helper()
	optsA = natstest.DefaultTestOptions
	optsA.ServerName = "A"
	optsA.Port = -1
	optsA.Cluster.Name = "pin"
	optsA.Cluster.Host = "127.0.0.1"
	optsA.Cluster.Port = -1
	a = natstest.RunServer(&optsA)
	// Freeze A's ports so a restart lands on the same addresses B and the client know.
	optsA.Port = a.Addr().(*net.TCPAddr).Port
	optsA.Cluster.Port = a.ClusterAddr().Port

	optsB := natstest.DefaultTestOptions
	optsB.ServerName = "B"
	optsB.Port = -1
	optsB.Cluster.Name = "pin"
	optsB.Cluster.Host = "127.0.0.1"
	optsB.Cluster.Port = -1
	optsB.Routes = natsserver.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", optsA.Cluster.Port))
	b = natstest.RunServer(&optsB)
	t.Cleanup(func() {
		b.Shutdown()
		b.WaitForShutdown()
	})
	for _, s := range []*natsserver.Server{a, b} {
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("routed server not ready")
		}
	}
	mustWait(t, 5*time.Second, "A and B routed", func() bool { return a.NumRoutes() >= 1 && b.NumRoutes() >= 1 })
	return a, b, optsA
}

// mustWait is the fatal form of the package's waitUntil.
func mustWait(t *testing.T, within time.Duration, what string, pred func() bool) {
	t.Helper()
	if !waitUntil(within, pred) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// bounceServerA takes A down long enough for a roaming client to land on B, then brings it back on
// the same ports. It returns the restarted server (the caller owns its shutdown).
func bounceServerA(t *testing.T, a *natsserver.Server, optsA natsserver.Options, nc *nats.Conn) *natsserver.Server {
	t.Helper()
	a.Shutdown()
	a.WaitForShutdown()
	mustWait(t, 5*time.Second, "client to notice A is gone", func() bool { return !nc.IsConnected() || nc.ConnectedServerName() != "A" })
	// With ReconnectWait at 50 ms a client that CAN roam is on B well inside this window; a client
	// that cannot is still dialing A. Either way the outcome below is decided by the option set,
	// not by the race between the restart and the reconnect loop.
	time.Sleep(400 * time.Millisecond)
	a2 := natstest.RunServer(&optsA)
	if !a2.ReadyForConnections(5 * time.Second) {
		t.Fatal("restarted A not ready")
	}
	t.Cleanup(func() {
		a2.Shutdown()
		a2.WaitForShutdown()
	})
	return a2
}

// origin: simcluster-speed review round 2 R1-F1 / R5-F2 (gotcha #89)
func TestBrokerConnectionRejoinsItsOwnServerInsteadOfRoamingToAPeer(t *testing.T) {
	fast := []nats.Option{nats.ReconnectWait(50 * time.Millisecond), nats.Timeout(time.Second)}

	t.Run("the broker's real options rejoin A", func(t *testing.T) {
		a, _, optsA := routedPair(t)
		urlA := fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port)
		b := &Broker{cfg: Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
		opts, _, err := b.brokerConnectOptions()
		if err != nil {
			t.Fatal(err)
		}
		nc, err := nats.Connect(urlA, append(opts, fast...)...)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		if got := nc.ConnectedServerName(); got != "A" {
			t.Fatalf("connected to %q, want A", got)
		}
		// The cluster DID advertise B (the routes are up); the broker's options refuse to learn it.
		if disc := nc.DiscoveredServers(); len(disc) != 0 {
			t.Fatalf("broker options let the pool learn advertised servers: %v", disc)
		}
		bounceServerA(t, a, optsA, nc)
		mustWait(t, 5*time.Second, "client back on A", func() bool { return nc.IsConnected() && nc.ConnectedServerName() == "A" })
		if got := nc.ConnectedUrl(); got != urlA {
			t.Fatalf("reconnected to %q, want its own server %q", got, urlA)
		}
	})

	t.Run("control: the pre-fix option set roams to B and never comes back", func(t *testing.T) {
		a, _, optsA := routedPair(t)
		urlA := fmt.Sprintf("nats://127.0.0.1:%d", optsA.Port)
		preFix := []nats.Option{nats.Name("tetherd"), nats.MaxReconnects(-1)}
		nc, err := nats.Connect(urlA, append(preFix, fast...)...)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		// The pool must have learned B before A goes away, or the control proves nothing.
		mustWait(t, 5*time.Second, "pre-fix client to learn B from INFO", func() bool { return len(nc.DiscoveredServers()) >= 1 })
		bounceServerA(t, a, optsA, nc)
		mustWait(t, 5*time.Second, "pre-fix client to settle", func() bool { return nc.IsConnected() })
		// A is back and healthy; the client is on B and stays there — the roam.
		time.Sleep(300 * time.Millisecond)
		if got := nc.ConnectedServerName(); got != "B" {
			t.Fatalf("control: pre-fix client is on %q; the fixture cannot see a roam, so the positive half above is vacuous", got)
		}
	})
}
