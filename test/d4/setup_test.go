// Package d4_test holds the §13.7 write-forwarding behavioral suite for D4. It is
// the FIRST harness to combine BOTH a real routed embedded-NATS cluster (the
// cluster.apply.<verb> forward bus) AND a real mTLS raft NetworkTransport cluster
// (the FSM replication) — D3 tested each in isolation (cross_server_test = routed
// NATS w/o raft; handler_node_test = raft over InmemTransport, never crossing NATS).
// BUILD-AND-PROVE: nothing here runs in production (serve.go builds no cluster.Node;
// cutover=D9).
package d4_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// testReqID is an explicit valid (32 lowercase-hex) forwarding key used by the
// dedup-MECHANISM tests. Production provision/join carry NO ReqID (external F1) — these
// tests exercise the ReqID dedup PRIMITIVE (the wire stamps env.ReqID → FSM dedups) that
// reconcile and future non-idempotent forwarded ops rely on, using provision as the
// vehicle (its Plan returns a non-nil command on retry, so the entry reaches Apply).
const testReqID = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4"

// ---- nkey / account helpers (mirror test/d3) ----

type brokerID struct {
	pub  string
	seed []byte
}

func newBrokerID(t *testing.T) brokerID {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	raw, _ := kp.Seed()
	seed := append([]byte(nil), raw...) // copy BEFORE Wipe (Seed may alias)
	kp.Wipe()
	return brokerID{pub: pub, seed: seed}
}

func newAccount(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	return kp, pub
}

func sigFromSeed(seed []byte) func([]byte) ([]byte, error) {
	s := append([]byte(nil), seed...)
	return func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(s)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
}

func jwtToServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: p.Pub.Allow, Deny: p.Pub.Deny},
		Subscribe: &natsserver.SubjectPermission{Allow: p.Sub.Allow, Deny: p.Sub.Deny},
	}
}

// B9: delegates to test/clusterharness — see the note in test/d3/setup_test.go.
func waitForCond(within time.Duration, pred func() bool) bool {
	return clusterharness.WaitForCond(within, pred)
}

// ---- routed NATS (broker nkeys = AuthUsers + RF1 PermissionsForBroker) ----

func authClusterOpts(brokers []brokerID, accountPub string, n int) []*natsserver.Options {
	allPubs := make([]string, len(brokers))
	staticUsers := make([]*natsserver.NkeyUser, len(brokers))
	for i, b := range brokers {
		allPubs[i] = b.pub
		staticUsers[i] = &natsserver.NkeyUser{Nkey: b.pub, Permissions: jwtToServerPerms(auth.PermissionsForBroker())}
	}
	out := make([]*natsserver.Options, n)
	for s := 0; s < n; s++ {
		o := natstest.DefaultTestOptions
		o.Port = -1
		o.Cluster.Name = "tether-d4"
		o.Cluster.Host = "127.0.0.1"
		o.Cluster.Port = -1
		o.Nkeys = staticUsers
		o.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: allPubs}
		out[s] = &o
	}
	return out
}

func startRouted(t *testing.T, optsList []*natsserver.Options) []*natsserver.Server {
	t.Helper()
	servers := make([]*natsserver.Server, len(optsList))
	servers[0] = natstest.RunServer(optsList[0])
	seedRoute := fmt.Sprintf("nats://127.0.0.1:%d", servers[0].ClusterAddr().Port)
	for i := 1; i < len(optsList); i++ {
		optsList[i].Routes = natsserver.RoutesFromStr(seedRoute)
		servers[i] = natstest.RunServer(optsList[i])
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
	for _, s := range servers {
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("routed server not ready")
		}
	}
	for _, s := range servers {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && s.NumRoutes() < len(servers)-1 {
			time.Sleep(20 * time.Millisecond)
		}
		if s.NumRoutes() < len(servers)-1 {
			t.Fatalf("server %s never formed %d routes (have %d)", s.Name(), len(servers)-1, s.NumRoutes())
		}
	}
	return servers
}

// ---- mTLS raft CA (mirror test/d3 route_mtls_test.go) ----

// B9: the route-mTLS CA moved to test/clusterharness. d4, d5 and d8 each carried a
// character-for-character copy of it (~45 lines apiece), differing only in the CN string — and the d8
// copy had quietly dropped two error checks. A method cannot be declared on another package's type, so
// call sites say ca.Leaf(t, d4CAName) rather than ca.clusterLeaf(t); that is the whole diff at the
// call site.
const d4CAName = "tether-d4"

func newRouteCA(t *testing.T) *clusterharness.RouteCA {
	t.Helper()
	return clusterharness.NewRouteCA(t, d4CAName)
}

// ---- the combined harness ----

type cluster4 struct {
	servers []*natsserver.Server
	nodes   []*cluster.Node
	brokers []brokerID
	conns   []*nats.Conn // broker-nkey conns (one per server), each with a cluster.apply responder
	acctKp  nkeys.KeyPair
	acctPub string
	dbPaths []string
}

// startCluster4 brings up n routed NATS servers + n mTLS raft nodes + a broker-nkey
// conn per server with a leader-only cluster.apply responder, and waits for a raft
// leader. The raft timeouts (150/150/75ms) are deliberately FAST TEST values for
// quick elections (mirroring the D3 integration harness) — they are NOT
// cluster.Multinode* (which D9 ships at 1000/1000/500ms); a lease-window-sensitive
// test (e.g. a stale-leader-in-lease gate) must pin cluster.MultinodeLeaderLeaseTimeout.
func startCluster4(t *testing.T, n int) *cluster4 {
	t.Helper()
	brokers := make([]brokerID, n)
	for i := range brokers {
		brokers[i] = newBrokerID(t)
	}
	acctKp, acctPub := newAccount(t)
	servers := startRouted(t, authClusterOpts(brokers, acctPub, n))

	ca := newRouteCA(t)
	trans := make([]*raft.NetworkTransport, n)
	for i := range trans {
		tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.Pool, Leaf: ca.Leaf(t, d4CAName),
		})
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		trans[i] = tr
	}
	peers := make([]raft.Server, n)
	for i := range peers {
		peers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(fmt.Sprintf("d4-%d", i)), Address: trans[i].LocalAddr()}
	}
	nodes := make([]*cluster.Node, n)
	dbPaths := make([]string, n)
	for i := range nodes {
		dir := t.TempDir()
		dbPaths[i] = filepath.Join(dir, "state.db")
		nd, err := cluster.New(cluster.Config{
			LocalID: raft.ServerID(fmt.Sprintf("d4-%d", i)), DataDir: dir, DBPath: dbPaths[i],
			Transport: trans[i], BootstrapPeers: peers,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout, ApplyTimeout: 5 * time.Second,
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
	for i := range conns {
		nc, err := nats.Connect(servers[i].ClientURL(), nats.Name("tetherd"),
			nats.Nkey(brokers[i].pub, sigFromSeed(brokers[i].seed)), nats.Timeout(3*time.Second))
		if err != nil {
			t.Fatalf("broker conn %d: %v", i, err)
		}
		t.Cleanup(func() { nc.Close() })
		conns[i] = nc
		if _, err := broker.SubscribeClusterApply(nc, nodes[i], time.Now, nil); err != nil {
			t.Fatalf("cluster.apply responder %d: %v", i, err)
		}
		_ = nc.Flush()
	}

	c := &cluster4{servers: servers, nodes: nodes, brokers: brokers, conns: conns, acctKp: acctKp, acctPub: acctPub, dbPaths: dbPaths}
	c.leaderIdx(t) // wait for a leader
	return c
}

func (c *cluster4) leaderIdx(t *testing.T) int {
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

func (c *cluster4) forwarder(i int) *broker.Forwarder {
	return broker.NewForwarder(c.conns[i], 2*time.Second)
}

// ---- DB seeding / read helpers (mirror test/d3) ----

func openReadHandle(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open read handle: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSession writes an ACTIVE session whose pin_hash matches pin into the DB file at
// dbPath via a separate WAL handle (the node's connection sees the committed row).
func seedSession(t *testing.T, dbPath, sid, pin string) {
	t.Helper()
	hash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at)
		 VALUES(?,?,?,?, 'ACTIVE', ?)`, sid, sid, "owner", hash, time.Now().UTC()); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

// seedNode writes an ONLINE node row (FK parent for processes) into dbPath.
func seedNode(t *testing.T, dbPath, sid, nid string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(nid,sid,status,registered_at) VALUES(?,?, 'ONLINE', ?)`,
		nid, sid, time.Now().UTC()); err != nil {
		t.Fatalf("seed node: %v", err)
	}
}

func freshClient(t *testing.T) (pub, fp string) {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ = kp.PublicKey()
	kp.Wipe()
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		t.Fatal(err)
	}
	return pub, fp
}

func signedAgentReq(t *testing.T, clientPub, sid, nid, pin string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()
	ephKp, _ := nkeys.CreateUser()
	ephPub, _ := ephKp.PublicKey()
	ephKp.Wipe()
	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = ephPub
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: "tether-agent:" + sid + ":" + nid, Nkey: clientPub, Token: pin}
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// signedCliReq builds a signed auth request for a CLI/member connect
// ("tether-cli:<sid>") so the handler routes to ensureMember -> JoinMemberWrite (the
// PIN-join member path), as opposed to signedAgentReq's agent-provision path.
func signedCliReq(t *testing.T, clientPub, sid, pin string) string {
	t.Helper()
	serverKp, _ := nkeys.CreateServer()
	serverPub, _ := serverKp.PublicKey()
	ephKp, _ := nkeys.CreateUser()
	ephPub, _ := ephKp.PublicKey()
	ephKp.Wipe()
	rc := jwt.NewAuthorizationRequestClaims(serverPub)
	rc.UserNkey = ephPub
	rc.Server = jwt.ServerID{ID: serverPub, Name: serverPub}
	rc.ConnectOptions = jwt.ConnectOptions{Name: "tether-cli:" + sid, Nkey: clientPub, Token: pin}
	tok, err := rc.Encode(serverKp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func mustDecodeResp(t *testing.T, respJWT string) *jwt.AuthorizationResponseClaims {
	t.Helper()
	resp, err := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// ---- goroutine leak gate (copied: goleak is deliberately NOT used; CLAUDE.md §5) ----

// assertNoGoroutineLeak / assertNoFDLeak delegate to the single implementation in internal/testharness
// (B9). Four suites carried byte-equivalent copies of the goroutine gate; the ±2 derivation and the ~1s
// poll window now live in one place. The local names stay so this suite's call sites do not churn.
func assertNoGoroutineLeak(t *testing.T, label string, before int) {
	t.Helper()
	testharness.AssertNoGoroutineLeak(t, label, before)
}

// fdCount returns the open-fd count for this process (-1 if unavailable, e.g.
// non-Linux). Linux exposes them under /proc/self/fd. The plan (R-13) / CLAUDE.md §5
// leak gate is NumGoroutine + fd baseline — a leaked NATS reply-inbox subscription
// can leak an fd a pooled goroutine would mask.
func fdCount() int { return testharness.FDCount() }

// d4FDTolerance is this suite's fd noise floor, kept LOCAL and passed explicitly.
//
// Three different tolerances are in use across the repo (this one, the tunnel/storage suites' tighter
// floor, and the broker suite's looser one), and each has a written derivation — the broker suite needs
// more headroom because every iteration opens a fresh NATS connection. Unifying them would either loosen
// the tight gates to the loosest number or make the loose one flake at the tightest, so
// testharness.AssertNoFDLeak takes the tolerance as a parameter permanently.
const d4FDTolerance = 4

// assertNoFDLeak compares the current fd count to a pre-recorded baseline. A -1 baseline (non-Linux)
// is a no-op.
func assertNoFDLeak(t *testing.T, label string, before int) {
	t.Helper()
	testharness.AssertNoFDLeak(t, label, before, d4FDTolerance)
}
