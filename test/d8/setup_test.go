//go:build d8_integration

// Package d8_test (gated) holds the D8 behavioral suite: replicated alert store (raise/clear
// via the leader-gated reconcile loop, cluster-level ack), re-derivable transfer audit
// (OpTransferAudit replay across an election), and tier-B object survival (OBJ_xfer at R=n
// survives killing the broker that created it). It reuses the D5 three-layer harness shape —
// a routed embedded-NATS cluster with CLUSTERED JetStream, a real mTLS raft NetworkTransport
// cluster, and the broker AuditPublisher / AlertReconciler loops. BUILD-AND-PROVE: nothing
// here runs in production (serve.go builds no cluster.Node; cutover=D9).
package d8_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// B9: both delegate to test/clusterharness — see the note in test/d3/setup_test.go. This suite spelled
// the poll helper `waitFor` while d3/d4/d5 spelled it `waitForCond`; the local names are kept so the
// call sites read unchanged, but there is now one implementation behind both spellings.
func waitFor(within time.Duration, pred func() bool) bool {
	return clusterharness.WaitForCond(within, pred)
}

func freeClusterPort(t *testing.T) int {
	t.Helper()
	return clusterharness.FreePort(t)
}

// startRoutedJS brings up n routed NATS servers with clustered JetStream (plain, no auth —
// the broker-nkey JS ACL is a D3 concern already proven; D8 proves the alert/audit/replica
// MECHANISM over a real clustered JetStream).
// startRoutedJS brings up an n-server clustered-JetStream mesh, RETRYING the whole bring-up
// on fresh ports if the embedded servers fail to become ready / mesh / form the meta-group
// within their windows under full-make-e2e host load (a documented clustered-JS flake;
// CLAUDE.md §e2e). A clean retry clears a transient startup starvation; isolation is <10s.
func startRoutedJS(t *testing.T, n int) []*natsserver.Server {
	t.Helper()
	const attempts = 3
	for attempt := 1; attempt <= attempts; attempt++ {
		servers, ok := attemptRoutedJS(t, n)
		if ok {
			t.Cleanup(func() {
				for _, s := range servers {
					s.Shutdown()
					s.WaitForShutdown()
				}
			})
			return servers
		}
		for _, s := range servers {
			if s != nil {
				s.Shutdown()
				s.WaitForShutdown()
			}
		}
		t.Logf("startRoutedJS attempt %d/%d not ready under load; retrying on fresh ports", attempt, attempts)
	}
	t.Fatalf("routed JS cluster did not come up after %d attempts", attempts)
	return nil
}

func attemptRoutedJS(t *testing.T, n int) ([]*natsserver.Server, bool) {
	t.Helper()
	ports := make([]int, n)
	routeStrs := make([]string, n)
	for i := range ports {
		ports[i] = freeClusterPort(t)
		routeStrs[i] = fmt.Sprintf("nats://127.0.0.1:%d", ports[i])
	}
	routes := natsserver.RoutesFromStr(strings.Join(routeStrs, ","))
	servers := make([]*natsserver.Server, n)
	for i := 0; i < n; i++ {
		o := &natsserver.Options{
			Host: "127.0.0.1", Port: -1,
			ServerName:         fmt.Sprintf("d8-%d", i),
			JetStream:          true,
			JetStreamMaxMemory: 1 << 30,
			JetStreamMaxStore:  32 << 30,
			StoreDir:           t.TempDir(),
			Cluster:            natsserver.ClusterOpts{Name: "tether-d8", Host: "127.0.0.1", Port: ports[i]},
			Routes:             routes,
			NoLog:              true, NoSigs: true,
		}
		s, err := natsserver.NewServer(o)
		if err != nil {
			return servers, false // freePort TOCTOU race etc. — a fresh-port retry clears it
		}
		go s.Start()
		servers[i] = s
	}
	for _, s := range servers {
		if !s.ReadyForConnections(30 * time.Second) {
			return servers, false
		}
	}
	for _, s := range servers {
		if !waitFor(20*time.Second, func() bool { return s.NumRoutes() >= n-1 }) {
			return servers, false
		}
	}
	ok := waitFor(30*time.Second, func() bool {
		leaders, complete := 0, false
		for _, s := range servers {
			if !s.JetStreamIsClustered() {
				return false
			}
			if s.JetStreamIsLeader() {
				leaders++
				if len(s.JetStreamClusterPeers()) >= n {
					complete = true
				}
			}
		}
		return leaders == 1 && complete
	})
	return servers, ok
}

// ---- mTLS raft CA ----

// B9: the route-mTLS CA moved to test/clusterharness — see the note in test/d4/setup_test.go.
//
// THIS copy is the reason sharing was worth doing rather than merely tidy: it had dropped the error
// checks on BOTH x509.CreateCertificate calls (`der, _ := ...`). A failure there yielded a nil-DER
// certificate that surfaced as a TLS handshake error inside the raft transport, one layer and several
// seconds away from the actual fault. The shared implementation checks and t.Fatalf's at the point of
// the failure, so this copy is fixed by construction.
const d8CAName = "tether-d8"

func newRouteCA(t *testing.T) *clusterharness.RouteCA {
	t.Helper()
	return clusterharness.NewRouteCA(t, d8CAName)
}

// ---- combined harness ----

type d8cluster struct {
	n       int
	servers []*natsserver.Server
	nodes   []*cluster.Node
	conns   []*nats.Conn
	js      []jetstream.JetStream
	dbs     []*sql.DB
}

func startD8Cluster(t *testing.T, n int) *d8cluster {
	t.Helper()
	servers := startRoutedJS(t, n)
	ca := newRouteCA(t)
	trans := make([]*raft.NetworkTransport, n)
	for i := range trans {
		tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.Pool, Leaf: ca.Leaf(t, d8CAName),
		})
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		trans[i] = tr
	}
	peers := make([]raft.Server, n)
	for i := range peers {
		peers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(fmt.Sprintf("d8-%d", i)), Address: trans[i].LocalAddr()}
	}
	nodes := make([]*cluster.Node, n)
	dbs := make([]*sql.DB, n)
	for i := range nodes {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "state.db")
		nd, err := cluster.New(cluster.Config{
			LocalID: raft.ServerID(fmt.Sprintf("d8-%d", i)), DataDir: dir, DBPath: dbPath,
			Transport: trans[i], BootstrapPeers: peers,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout, ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		nodes[i] = nd
		db, err := storage.Open("file:" + dbPath + "?_txlock=immediate")
		if err != nil {
			t.Fatalf("open db %d: %v", i, err)
		}
		dbs[i] = db
	}
	t.Cleanup(func() {
		for i := range nodes {
			if nodes[i] != nil {
				_ = nodes[i].Shutdown()
			}
			if dbs[i] != nil {
				_ = dbs[i].Close()
			}
		}
	})

	conns := make([]*nats.Conn, n)
	jss := make([]jetstream.JetStream, n)
	for i := range conns {
		nc, err := nats.Connect(servers[i].ClientURL(), nats.Name("tetherd-d8"), nats.Timeout(3*time.Second))
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		t.Cleanup(func() { nc.Close() })
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("js %d: %v", i, err)
		}
		conns[i] = nc
		jss[i] = js
	}
	c := &d8cluster{n: n, servers: servers, nodes: nodes, conns: conns, js: jss, dbs: dbs}
	c.leaderIdx(t)
	return c
}

func (c *d8cluster) leaderIdx(t *testing.T) int {
	t.Helper()
	idx := -1
	if !waitFor(8*time.Second, func() bool {
		for i, nd := range c.nodes {
			if nd != nil && nd.IsLeader() {
				idx = i
				return true
			}
		}
		return false
	}) {
		t.Fatal("no raft leader formed")
	}
	return idx
}

// proposeOn forwards a Plan to whichever node is leader (retrying across an election).
func (c *d8cluster) proposeOn(t *testing.T, plan func(*sql.DB) (*cluster.Command, error)) {
	t.Helper()
	if !waitFor(8*time.Second, func() bool {
		for _, nd := range c.nodes {
			if nd == nil || !nd.IsLeader() {
				continue
			}
			err := nd.Propose(plan)
			return err == nil || !cluster.IsNotLeader(err)
		}
		return false
	}) {
		t.Fatal("propose never reached a leader")
	}
}

// activeAlertOn reports whether dedupKey is ACTIVE in node i's replicated DB.
func (c *d8cluster) activeAlertOn(i int, dedupKey string) bool {
	a, err := cluster.IsAlertActive(c.dbs[i], dedupKey)
	return err == nil && a
}

// ---- leak gates (goleak deliberately NOT used; CLAUDE.md §5) ----

// assertNoGoroutineLeak delegates to internal/testharness (B9) — see the note in
// test/d4/setup_test.go. The local name stays so this suite's call sites do not churn.
func assertNoGoroutineLeak(t *testing.T, label string, before int) {
	t.Helper()
	testharness.AssertNoGoroutineLeak(t, label, before)
}
