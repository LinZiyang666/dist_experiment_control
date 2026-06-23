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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func waitFor(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

func freeClusterPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return p
}

// startRoutedJS brings up n routed NATS servers with clustered JetStream (plain, no auth —
// the broker-nkey JS ACL is a D3 concern already proven; D8 proves the alert/audit/replica
// MECHANISM over a real clustered JetStream).
func startRoutedJS(t *testing.T, n int) []*natsserver.Server {
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
			t.Fatalf("NewServer %d: %v", i, err)
		}
		go s.Start()
		servers[i] = s
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
	for _, s := range servers {
		// 30s (was 10s): under the full make e2e matrix this harness starts after every other
		// matrix, so the machine is loaded and embedded-server startup is slow (the D5 harness
		// flaked "routed JS server not ready" here). No logic change; just load headroom.
		if !s.ReadyForConnections(30 * time.Second) {
			t.Fatal("routed JS server not ready")
		}
	}
	for _, s := range servers {
		if !waitFor(20*time.Second, func() bool { return s.NumRoutes() >= n-1 }) {
			t.Fatalf("server %s never meshed", s.Name())
		}
	}
	if !waitFor(30*time.Second, func() bool {
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
	}) {
		t.Fatal("JS meta-group did not form")
	}
	return servers
}

// ---- mTLS raft CA ----

type routeCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pool *x509.CertPool
}

func newRouteCA(t *testing.T) *routeCA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-d8-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &routeCA{cert: cert, key: priv, pool: pool}
}

func (ca *routeCA) leaf(t *testing.T) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "tether-d8-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
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
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t),
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
			HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
			LeaderLeaseTimeout: 75 * time.Millisecond, ApplyTimeout: 5 * time.Second,
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

func assertNoGoroutineLeak(t *testing.T, label string, before int) {
	t.Helper()
	var last int
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		last = runtime.NumGoroutine()
		if last-before <= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	buf := make([]byte, 64*1024)
	nb := runtime.Stack(buf, true)
	t.Errorf("%s: goroutine leak: before=%d after=%d (+%d)\n%s", label, before, last, last-before, buf[:nb])
}
