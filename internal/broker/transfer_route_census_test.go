package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type transferRouteCluster struct {
	nodes   []*cluster.Node
	conns   []*nats.Conn
	brokers []*Broker
	origins []xfercontract.Origin
}

// Each server has its own client ingress and the three SQLite/Raft owners
// replicate real commands. No process is spawned or production directory used.
func newTransferRouteCluster(t *testing.T) *transferRouteCluster {
	return newTransferRouteClusterWithStorage(t, false)
}

func newTransferRouteClusterWithStorage(t *testing.T, storage bool, configure ...func(int, *natsserver.Options) []nats.Option) *transferRouteCluster {
	t.Helper()
	c := &transferRouteCluster{}
	servers := make([]*natsserver.Server, 3)
	var routePorts [3]int
	if storage {
		for i := range routePorts {
			routePorts[i] = clusterharness.FreePort(t)
		}
	}
	for i := range servers {
		opts := natstest.DefaultTestOptions
		opts.Port, opts.Cluster.Port = -1, -1
		opts.Cluster.Host, opts.Cluster.Name = "127.0.0.1", "transfer-routing"
		opts.ServerName = fmt.Sprintf("route-%d", i)
		if storage {
			opts.JetStream, opts.StoreDir = true, t.TempDir()
			opts.Cluster.Port = routePorts[i]
			for j, port := range routePorts {
				if i != j {
					opts.Routes = append(opts.Routes, natsserver.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", port))...)
				}
			}
		}
		if i != 0 && !storage {
			opts.Routes = natsserver.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", servers[0].ClusterAddr().Port))
		}
		connectOptions := []nats.Option{nats.NoReconnect()}
		for _, setup := range configure {
			connectOptions = append(connectOptions, setup(i, &opts)...)
		}
		servers[i] = startTransferRouteServer(t, opts)
		if storage {
			routePorts[i] = servers[i].ClusterAddr().Port
		}
		srv := servers[i]
		t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
		nc, err := nats.Connect(srv.ClientURL(), connectOptions...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		c.conns = append(c.conns, nc)
	}
	mustWait(t, 5*time.Second, "three distinct routed NATS ingresses", func() bool {
		return servers[0].NumRoutes() >= 2 && servers[1].NumRoutes() >= 1 && servers[2].NumRoutes() >= 1
	})
	transports := make([]*raft.InmemTransport, 3)
	peers := make([]raft.Server, 3)
	for i := range transports {
		id := fmt.Sprintf("broker-%d", i)
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", 7400+i)))
		transports[i] = trans
		peers[i] = raft.Server{ID: raft.ServerID(id), Address: addr, Suffrage: raft.Voter}
	}
	for i, trans := range transports {
		for j, peer := range transports {
			if i != j {
				trans.Connect(peer.LocalAddr(), peer)
			}
		}
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "state.db")
		seedBrokerFixtureDBFile(t, dbPath)
		n, err := cluster.New(cluster.Config{
			LocalID: peers[i].ID, DataDir: dir, DBPath: dbPath, Transport: trans, BootstrapPeers: peers,
			Logger: silentLogger(), ApplyTimeout: 5 * time.Second,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Shutdown() })
		c.nodes = append(c.nodes, n)
		origin := xfercontract.Origin{BrokerID: n.SelfID(), InstanceID: fmt.Sprintf("instance-%d", i), BootID: "boot", Generation: 1}
		c.origins = append(c.origins, origin)
		b := &Broker{cfg: Config{DB: n.RODB(), Now: time.Now, Logger: silentLogger()}, selfID: n.SelfID(), clusterMode: true, cl: &clusterRuntime{node: n}}
		b.nc.Store(c.conns[i])
		c.brokers = append(c.brokers, b)
	}
	return c
}

// origin: hot-update S7 plan §6.3 / §8.2 / T39
func TestTransferClusterCensusRequiresOriginalFollowerProof(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferFollowerCensus(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer follower census", beforeG)
	testharness.AssertNoFDLeak(t, "transfer follower census", beforeFD, 0)
}

func exerciseTransferFollowerCensus(t *testing.T) {
	t.Helper()
	c := newTransferRouteCluster(t)
	probes := make([]clusterharness.LeaderProbe, len(c.nodes))
	for i, n := range c.nodes {
		probes[i] = n
	}
	var calls [3]atomic.Int32
	var ready atomic.Bool
	ready.Store(true)
	for i, n := range c.nodes {
		cfg := TransferClusterForwardConfig{Node: n, Origin: c.origins[i]}
		cfg.Census = func(origin xfercontract.Origin, _ controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
			calls[i].Add(1)
			return xfercontract.LedgerCensus{Origin: origin, Ready: ready.Load(), Complete: ready.Load(), Digest: "retained-version",
				Entries: []xfercontract.Protection{{Identity: xfercontract.Identity{SID: "lab", TransferID: "original", EntryNonce: "entry"}, Phase: xfercontract.PhaseCommitReserved, ObjectVersion: "original-nuid"}}}, nil
		}
		cfg.Probe = func(controlbudget.Deadline) (xfercontract.ProbeResult, error) {
			return xfercontract.ProbeResult{Version: "transfer-v1", HistoryGuardVersion: "transfer-v1", BucketConfigVersion: "transfer-v1", InstanceID: c.origins[i].InstanceID}, nil
		}
		owner, err := SubscribeTransferClusterForward(c.conns[i], cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, sub := range owner.subs {
			if sub.Queue != "" {
				t.Fatalf("original-owner RPC joined random queue %q", sub.Queue)
			}
		}
		t.Cleanup(func() {
			if err := StopTransferClusterForward(owner, transferRouteDeadline(t, time.Second)); err != nil {
				t.Error(err)
			}
		})
		// The real generic leader responder must not race its unknown-verb
		// answer ahead of the separately addressed follower's census.
		sub, err := SubscribeClusterApply(c.conns[i], n, time.Now, silentLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
		if err := c.conns[i].FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	_, err := clusterharness.WithLeader(t, probes, 5*time.Second, func(leader int) error {
		original, ingress := (leader+1)%3, (leader+2)%3
		fwd := NewForwarderWithLimit(c.conns[ingress], time.Second, 0)
		resolver := TransferOwnerResolver{Forwarder: fwd}
		census, err := resolver.Census(c.origins[original], transferRouteDeadline(t, time.Second))
		if err != nil {
			return err
		}
		if len(census.Entries) != 1 || census.Entries[0].ObjectVersion != "original-nuid" || census.Origin != c.origins[original] {
			return errors.New("original follower's live version disappeared from the census")
		}
		for i := range calls {
			if (calls[i].Load() != 0) != (i == original) {
				return fmt.Errorf("nonowner census callback ran: broker=%d calls=%d", i, calls[i].Load())
			}
		}
		for _, origin := range c.origins {
			probe, err := ProbeTransferBroker(fwd, xfercontract.Origin{BrokerID: origin.BrokerID}, transferRouteDeadline(t, time.Second))
			if err != nil || probe.InstanceID != origin.InstanceID {
				return fmt.Errorf("directed eligible-broker probe: %v", err)
			}
		}
		ready.Store(false)
		if err := forwardTransferRPC(fwd, transferForwardRequest{Origin: c.origins[original]}, transferRouteDeadline(t, time.Second)); err == nil {
			return errors.New("responder acknowledged an incomplete census")
		}
		if _, err := resolver.Census(c.origins[original], transferRouteDeadline(t, time.Second)); err == nil {
			return errors.New("incomplete census became authoritative absence")
		}
		ready.Store(true)
		lost := c.origins[original]
		lost.BootID = "old-boot"
		if _, err := resolver.Census(lost, transferRouteDeadline(t, time.Second)); err == nil {
			return errors.New("boot mismatch authorized foreign-owner absence")
		}
		lost.BrokerID = "unreachable-original"
		if _, err := resolver.Census(lost, transferRouteDeadline(t, 40*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("nonowner answered for an unreachable original instead of staying silent: %v", err)
		}
		if _, err := ForwardTransferBucketConfig(fwd, xfercontract.ConfigPatch{Bucket: "xfer-lab"}, transferRouteDeadline(t, time.Second)); err == nil {
			return errors.New("unwired coordinator fabricated a successful config seal")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A discovered route port can be occupied before Start binds it. Retry only
// that concrete bind failure, using a kernel-selected port and joining the
// failed server; setup/config failures keep their original failure verdict.
// origin: hot-update S7 plan §10.1 / testing-standards T5 / INTEG-C startup seam
func startTransferRouteServer(t *testing.T, opts natsserver.Options) *natsserver.Server {
	t.Helper()
	opts.Cluster.PoolSize = -1
	opts.Cluster.Compression.Mode = natsserver.CompressionOff
	for attempt := 0; attempt < 2; attempt++ {
		log := &transferRouteLog{fatal: make(chan string, 1)}
		srv, err := natsserver.NewServer(&opts)
		if err != nil {
			t.Fatal(err)
		}
		srv.SetLogger(log, false, false)
		started := make(chan struct{})
		go func() { defer close(started); srv.Start() }()
		stop := func() { srv.Shutdown(); srv.WaitForShutdown(); <-started }
		// Register cleanup before startup: a setup fatal/panic owns this server too.
		t.Cleanup(stop)
		deadline := time.Now().Add(10 * time.Second)
		failure := ""
		for time.Now().Before(deadline) {
			select {
			case failure = <-log.fatal:
			default:
			}
			if failure != "" {
				break
			}
			if srv.ReadyForConnections(25 * time.Millisecond) {
				return srv
			}
		}
		stop()
		if !strings.Contains(failure, "address already in use") {
			t.Fatalf("routed NATS startup failed: %s", failure)
		}
		opts.Cluster.Port = -1
	}
	t.Fatal("routed NATS startup remained unavailable after bind retry")
	return nil
}

// Only fatal startup diagnostics are needed; no protocol/key data is logged.
type transferRouteLog struct{ fatal chan string }

func (l *transferRouteLog) Fatalf(format string, args ...any) {
	select {
	case l.fatal <- fmt.Sprintf(format, args...):
	default:
	}
}
func (*transferRouteLog) Noticef(string, ...any) {}
func (*transferRouteLog) Warnf(string, ...any)   {}
func (*transferRouteLog) Errorf(string, ...any)  {}
func (*transferRouteLog) Debugf(string, ...any)  {}
func (*transferRouteLog) Tracef(string, ...any)  {}

// The collision is an actual bound listener, rather than a guessed failure or
// an extended readiness timeout. The retry must bind a different real route.
// origin: hot-update S7 plan §10.1 / testing-standards T5 / INTEG-C startup seam
func TestTransferRouteStartupRetriesOccupiedPort(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRoutePortCollision(t)
	}
	testharness.AssertNoGoroutineLeak(t, "route startup collision", g)
	testharness.AssertNoFDLeak(t, "route startup collision", fd, 0)
}
func exerciseTransferRoutePortCollision(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Cluster.Host = "127.0.0.1"
	opts.Cluster.Name = "occupied-route"
	opts.Cluster.Port = port
	srv := startTransferRouteServer(t, opts)
	defer func() { srv.Shutdown(); srv.WaitForShutdown() }()
	if srv.ClusterAddr().Port == port {
		t.Fatal("route startup did not replace the occupied port")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
}
