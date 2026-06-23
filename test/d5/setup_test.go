//go:build d5_integration

// Package d5_test holds the D5 behavioral suite: re-derivable audit publish
// (single-writer + dedup + post-election sweep, §6.3) and JS per-session stream replica
// reconfig (§6.4/§9). It is the FIRST harness to combine THREE real layers — a routed
// embedded-NATS cluster with CLUSTERED JetStream (streams replicate over the routes), a
// real mTLS raft NetworkTransport cluster (the FSM the publisher reads committed entries
// from), and the broker audit publisher loop. BUILD-AND-PROVE: nothing here runs in
// production (serve.go builds no cluster.Node, never starts the loop; cutover=D9).
//
// The NATS servers run WITHOUT auth_callout: the production broker-nkey JS ACL is a D3
// concern already proven (test/d3); D5 proves the publish/dedup/reconfig MECHANISM over a
// real clustered JetStream, so a plain JS cluster keeps the harness deterministic.
package d5_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/hashicorp/raft"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// testReqID is a valid (32 lowercase-hex) forwarding key for the reqID-keyed dedup test
// (E-A8): a forwarded reconcile carries a deterministic ReconcileReqID, so its ack-lost
// retry (committing at a NEW raft index via appliedDedup) must re-derive the SAME dedup
// ids — keyed on the ReqID, not the raft index.
const testReqID = "d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5"

func waitForCond(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

// ---- routed NATS cluster with clustered JetStream (plain, no auth) ----

// freePort reserves an ephemeral TCP port and frees it so a server can bind it. Used to
// pre-assign the CLUSTER ports: JS clustering refuses to start without routes configured
// at startup ("JetStream cluster requires configured routes"), so the full route mesh
// must be wired BEFORE any server starts — which means knowing every cluster port up front
// (the seed-then-join pattern that works for plain NATS does not work for clustered JS).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func startRoutedJS(t *testing.T, n int) []*natsserver.Server {
	t.Helper()
	clusterPorts := make([]int, n)
	routeStrs := make([]string, n)
	for i := range clusterPorts {
		clusterPorts[i] = freePort(t)
		routeStrs[i] = fmt.Sprintf("nats://127.0.0.1:%d", clusterPorts[i])
	}
	routes := natsserver.RoutesFromStr(strings.Join(routeStrs, ","))

	servers := make([]*natsserver.Server, n)
	for i := 0; i < n; i++ {
		o := &natsserver.Options{
			Host:       "127.0.0.1",
			Port:       -1,
			ServerName: fmt.Sprintf("d5-%d", i), // REQUIRED: JS meta never forms without unique names
			JetStream:  true,
			// Quota (a config RESERVATION limit, not pre-allocated disk — empty streams use
			// ~0 real disk) must exceed the SUM of MaxBytes of streams that may land on one
			// server: history-<sid>=1 GiB, OBJ_xfer-<sid>=8 GiB. 32 GiB headroom (a far larger
			// quota destabilizes the embedded JS store on a smaller backing filesystem).
			JetStreamMaxMemory: 1 << 30,
			JetStreamMaxStore:  32 << 30,
			StoreDir:           t.TempDir(),
			Cluster:            natsserver.ClusterOpts{Name: "tether-d5", Host: "127.0.0.1", Port: clusterPorts[i]},
			Routes:             routes, // full mesh wired before start (a self-route is ignored)
			NoLog:              true,
			NoSigs:             true,
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
		// 30s (was 10s): under the full make e2e matrix this harness starts AFTER every other
		// phase + DN matrix, so the machine is loaded and embedded-server startup is slow — a
		// 10s ready-check was a load-induced flake ("routed JS server not ready"). No logic
		// change; just headroom.
		if !s.ReadyForConnections(30 * time.Second) {
			t.Fatal("routed JS server not ready")
		}
	}
	// Wait for the routes to mesh.
	for _, s := range servers {
		if !waitForCond(20*time.Second, func() bool { return s.NumRoutes() >= n-1 }) {
			t.Fatalf("server %s never meshed %d routes (have %d)", s.Name(), n-1, s.NumRoutes())
		}
	}
	// Wait for the JS meta-group: every server clustered + exactly ONE meta leader whose
	// peer set is complete. (JetStreamClusterPeers() returns the full set only on the meta
	// leader; followers report 0 — so the completeness check is leader-scoped.) Deterministic
	// readiness gate, no time.Sleep.
	if !waitForCond(30*time.Second, func() bool { // 30s (was 20s): full-matrix load headroom
		leaders, metaComplete := 0, false
		for _, s := range servers {
			if !s.JetStreamIsClustered() {
				return false
			}
			if s.JetStreamIsLeader() {
				leaders++
				if len(s.JetStreamClusterPeers()) >= n {
					metaComplete = true
				}
			}
		}
		return leaders == 1 && metaComplete
	}) {
		t.Fatal("JS meta-group did not form")
	}
	return servers
}

// ---- mTLS raft CA (mirror test/d4) ----

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
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-d5-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &routeCA{cert: cert, key: priv, pool: pool}
}

func (ca *routeCA) clusterLeaf(t *testing.T) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "tether-d5-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

// ---- the combined harness ----

type cluster5 struct {
	n       int
	servers []*natsserver.Server
	nodes   []*cluster.Node
	conns   []*nats.Conn
	js      []jetstream.JetStream
	dbPaths []string
}

// startCluster5 brings up n routed JS-enabled NATS servers + n mTLS raft nodes + a plain
// NATS conn (+ jetstream client) per server, and waits for both a raft leader and the JS
// meta-group. Fast raft timeouts (150/150/75ms) for quick elections (mirrors test/d4).
func startCluster5(t *testing.T, n int) *cluster5 {
	t.Helper()
	servers := startRoutedJS(t, n)

	ca := newRouteCA(t)
	trans := make([]*raft.NetworkTransport, n)
	for i := range trans {
		tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.clusterLeaf(t),
		})
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		trans[i] = tr
	}
	peers := make([]raft.Server, n)
	for i := range peers {
		peers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(fmt.Sprintf("d5-%d", i)), Address: trans[i].LocalAddr()}
	}
	nodes := make([]*cluster.Node, n)
	dbPaths := make([]string, n)
	for i := range nodes {
		dir := t.TempDir()
		dbPaths[i] = filepath.Join(dir, "state.db")
		nd, err := cluster.New(cluster.Config{
			LocalID: raft.ServerID(fmt.Sprintf("d5-%d", i)), DataDir: dir, DBPath: dbPaths[i],
			Transport: trans[i], BootstrapPeers: peers,
			HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
			LeaderLeaseTimeout: 75 * time.Millisecond, ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		nodes[i] = nd
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			if nd != nil {
				_ = nd.Shutdown()
			}
		}
	})

	conns := make([]*nats.Conn, n)
	jss := make([]jetstream.JetStream, n)
	for i := range conns {
		nc, err := nats.Connect(servers[i].ClientURL(), nats.Name("tetherd-d5"), nats.Timeout(3*time.Second))
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

	c := &cluster5{n: n, servers: servers, nodes: nodes, conns: conns, js: jss, dbPaths: dbPaths}
	c.leaderIdx(t)             // wait for a raft leader
	waitJSPlacementReady(t, c) // wait until an R=n stream actually PLACES (see below)
	return c
}

// waitJSPlacementReady probes that the JS meta-group can actually PLACE an R=n stream
// before any test proceeds. A freshly-formed meta-group can transiently reject placement
// with "no suitable peers for placement, peer offline" (code 10005) even after the meta
// leader reports all peers — especially under the full-matrix tail load. The JS-meta
// readiness gate in startRoutedJS is necessary but not sufficient for PLACEMENT, so we
// retry a create-an-R=n-probe-then-delete until it succeeds, guaranteeing every test's
// real R=n stream creation places cleanly.
func waitJSPlacementReady(t *testing.T, c *cluster5) {
	t.Helper()
	const probe = "d5-placement-probe"
	ok := waitForCond(25*time.Second, func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pcancel()
		_, err := c.js[0].CreateStream(pctx, jetstream.StreamConfig{
			Name: probe, Subjects: []string{"d5.placement.probe"}, Replicas: c.n,
			Storage: jetstream.FileStorage, MaxBytes: 16 << 20,
		})
		return err == nil
	})
	dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = c.js[0].DeleteStream(dctx, probe)
	dcancel()
	if !ok {
		t.Fatal("JS cluster never became ready to place an R=n stream")
	}
}

// ensureHistory / ensureEvents create-or-raise a stream, RETRYING on a transient placement
// rejection. Under sustained -race load the embedded JS peers intermittently flap "offline"
// (code 10005 "no suitable peers for placement, peer offline"), so a single create can fail
// even after the meta-group formed; EnsureHistoryStream is idempotent + raise-only, so a
// retry is safe and converges. The E-B3 capacity-gate test calls EnsureHistoryStream
// DIRECTLY (it asserts the ErrMetaGroupNotReady these helpers would otherwise spin on).
func ensureHistory(t *testing.T, c *cluster5, idx int, sid string, replicas int) {
	t.Helper()
	if !waitForCond(25*time.Second, func() bool {
		cx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		return jsstream.EnsureHistoryStream(cx, c.js[idx], sid, replicas) == nil
	}) {
		t.Fatalf("ensure history-%s R%d never placed (JS cluster overloaded)", sid, replicas)
	}
}

func ensureEvents(t *testing.T, c *cluster5, idx, replicas int) {
	t.Helper()
	if !waitForCond(25*time.Second, func() bool {
		cx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		return jsstream.EnsureEventsStream(cx, c.js[idx], replicas) == nil
	}) {
		t.Fatalf("ensure events R%d never placed", replicas)
	}
}

func (c *cluster5) leaderIdx(t *testing.T) int {
	t.Helper()
	idx := -1
	if !waitForCond(8*time.Second, func() bool {
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

// ---- JS stream helpers ----

// streamMsgs returns the committed message count of a stream (0 if it does not exist).
func streamMsgs(t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		return 0
	}
	info, err := s.Info(ctx)
	if err != nil {
		return 0
	}
	return info.State.Msgs
}

// streamReplicas returns the count of caught-up replicas (1 leader + Current,!Offline
// peers), -1 if the stream is missing.
func streamReplicas(t *testing.T, js jetstream.JetStream, name string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		return -1
	}
	info, err := s.Info(ctx)
	if err != nil {
		return -1
	}
	actual := 1
	if info.Cluster != nil {
		for _, p := range info.Cluster.Replicas {
			if p != nil && p.Current && !p.Offline {
				actual++
			}
		}
	}
	return actual
}

func waitForStreamReplicas(t *testing.T, js jetstream.JetStream, name string, want int) bool {
	t.Helper()
	return waitForCond(15*time.Second, func() bool { return streamReplicas(t, js, name) >= want })
}

// ---- goroutine / fd leak gates (copied: goleak is deliberately NOT used; CLAUDE.md §5) ----

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

func fdCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func assertNoFDLeak(t *testing.T, label string, before int) {
	t.Helper()
	if before < 0 {
		return
	}
	var last int
	for i := 0; i < 50; i++ {
		last = fdCount()
		if last-before <= 4 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("%s: fd leak: before=%d after=%d (+%d)", label, before, last, last-before)
}
