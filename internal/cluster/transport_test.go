package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ---- ephemeral cluster CA + leaf issuance (test-only) ---------------------

type testCA struct {
	pool *x509.CertPool
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tether-test-cluster-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{pool: pool, cert: cert, key: priv}
}

func (ca *testCA) leaf(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	// Signed BY the CA (ca.key), FOR the leaf's public key (pub).
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

// fastMultinodeCfg gives real-transport tests quick-but-stable election timing
// (the prod 1000ms values would only slow the tests; the timeout *values* are
// pinned separately by TestFenceExceedsWorstCaseElection).
func fastMultinodeCfg(id raft.ServerID, dir string, tr raft.Transport, peers []raft.Server) Config {
	return Config{
		LocalID:            id,
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          tr,
		BootstrapPeers:     peers,
		Logger:             quietLogger(),
		HeartbeatTimeout:   150 * time.Millisecond,
		ElectionTimeout:    150 * time.Millisecond,
		LeaderLeaseTimeout: 75 * time.Millisecond,
		ApplyTimeout:       5 * time.Second,
	}
}

// ---- TestD3TransportTwoNodeReplicate: real mTLS transport carries raft -----

func TestD3TransportTwoNodeReplicate(t *testing.T) {
	ca := newTestCA(t)
	ids := []raft.ServerID{"d3-a", "d3-b"}
	trans := make([]*raft.NetworkTransport, len(ids))
	dirs := make([]string, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		trans[i] = tr
		dirs[i] = t.TempDir()
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: tr.LocalAddr()}
	}

	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		n, err := New(fastMultinodeCfg(id, dirs[i], trans[i], servers))
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Shutdown()
		}
	})

	leader := waitNodeLeader(t, nodes, 6*time.Second)
	if err := leader.ApplyMetaSet("t:d3key", "d3val"); err != nil { // "t:" = D1 test-scaffolding prefix
		t.Fatalf("leader Apply over mTLS transport: %v", err)
	}

	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	replicated := false
	for i := 0; i < 200 && !replicated; i++ {
		_ = follower.BoundedStaleRead(func(db *sql.DB) error {
			var v string
			if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:d3key'`).Scan(&v); err == nil && v == "d3val" {
				replicated = true
			}
			return nil
		})
		if !replicated {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !replicated {
		t.Fatal("leader write never replicated to the follower over the mTLS transport")
	}
}

// TestD3TransportShutdownReapsListenerNoLeak proves Node.Shutdown closes the
// injected transport (R5): repeated bootstrap+shutdown must not leak the TLS
// listener fd or transport goroutines. Single-node (no BootstrapPeers) for speed.
func TestD3TransportShutdownReapsListenerNoLeak(t *testing.T) {
	ca := newTestCA(t)
	waitGoroutineBaseline(t, runtime.NumGoroutine()+8, 2*time.Second)
	gBase := runtime.NumGoroutine()
	fdBase := fdCount()

	for i := 0; i < 5; i++ {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "churn"),
		})
		if err != nil {
			t.Fatalf("transport: %v", err)
		}
		dir := t.TempDir()
		n, err := New(Config{
			LocalID: raft.ServerID(fmt.Sprintf("churn-%d", i)), DataDir: dir,
			DBPath: filepath.Join(dir, "state.db"), Transport: tr, Logger: quietLogger(),
			HeartbeatTimeout: 100 * time.Millisecond, ElectionTimeout: 100 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		_ = n.WaitForLeader(3 * time.Second)
		if err := n.Shutdown(); err != nil {
			t.Fatalf("shutdown %d: %v", i, err)
		}
	}

	waitGoroutineBaseline(t, gBase+4, 3*time.Second)
	if fdBase >= 0 {
		if leaked := fdCount() - fdBase; leaked > 4 {
			t.Fatalf("fd leak: %d fds above baseline after 5 transport cycles "+
				"(Shutdown did not reap the mTLS listener?)", leaked)
		}
	}
}

// TestD3RaftTransportRejectsForeignCA proves the mTLS is real (not
// InsecureSkipVerify-everything): a leaf that does not chain to the cluster CA is
// rejected in BOTH directions.
func TestD3RaftTransportRejectsForeignCA(t *testing.T) {
	ca1 := newTestCA(t)
	ca2 := newTestCA(t)

	srv1, cli1, err := clusterTLSConfigs(MTLSTransportConfig{CACert: ca1.pool, Leaf: ca1.leaf(t, "n1")})
	if err != nil {
		t.Fatal(err)
	}
	// Foreign CLIENT that trusts ca1 (so it accepts the server) but presents a
	// ca2-signed leaf — isolates the server's RequireAndVerifyClientCert rejection.
	_, foreignClientCert, err := clusterTLSConfigs(MTLSTransportConfig{CACert: ca1.pool, Leaf: ca2.leaf(t, "evil-client")})
	if err != nil {
		t.Fatal(err)
	}
	// Foreign SERVER trust: client trusts only ca2, so it rejects the ca1 server —
	// isolates the client's VerifyPeerCertificate rejection.
	_, foreignServerTrust, err := clusterTLSConfigs(MTLSTransportConfig{CACert: ca2.pool, Leaf: ca2.leaf(t, "evil-trust")})
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	// Server: only after a fully-verified mutual handshake does it write the OK
	// byte. A bad client cert fails Handshake() server-side (in TLS 1.2 and 1.3
	// alike — the server validates the client chain during the handshake), so no
	// byte is written and the client's Read fails. This makes the probe below
	// robust to the TLS-1.3 timing where the client's Dial can return before the
	// server has finished validating the client cert.
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				tc := c.(*tls.Conn)
				if herr := tc.Handshake(); herr != nil {
					_ = c.Close()
					return
				}
				_, _ = tc.Write([]byte{1})
				_ = c.Close()
			}(c)
		}
	}()
	addr := ln.Addr().String()

	// accepted reports whether a client config completes a mutual handshake AND
	// receives the server's post-handshake OK byte.
	accepted := func(cfg *tls.Config) bool {
		c, derr := tls.Dial("tcp", addr, cfg)
		if derr != nil {
			return false
		}
		defer func() { _ = c.Close() }()
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 1)
		n, rerr := c.Read(buf)
		return rerr == nil && n == 1
	}

	if !accepted(cli1.Clone()) {
		t.Fatal("positive control: a cluster-CA client must complete the mutual mTLS handshake")
	}
	if accepted(foreignClientCert.Clone()) {
		t.Fatal("server must reject a client whose leaf is not signed by the cluster CA")
	}
	if accepted(foreignServerTrust.Clone()) {
		t.Fatal("client must reject a server whose leaf is not signed by the cluster CA")
	}
}

// ---- PreVote re-proven over the REAL mTLS transport ------------------------

type realNode struct {
	id    raft.ServerID
	trans *raft.NetworkTransport
	r     *raft.Raft
}

func newRealTransportCluster(t *testing.T, preVoteDisabled bool, ca *testCA) []*realNode {
	t.Helper()
	ids := []raft.ServerID{"rt-a", "rt-b", "rt-c"}
	nodes := make([]*realNode, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		nodes[i] = &realNode{id: id, trans: tr}
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: tr.LocalAddr()}
	}
	cfg := raft.Configuration{Servers: servers}
	for _, n := range nodes {
		store := raft.NewInmemStore()
		snaps := raft.NewInmemSnapshotStore()
		rc := raft.DefaultConfig()
		rc.LocalID = n.id
		rc.HeartbeatTimeout = 200 * time.Millisecond
		rc.ElectionTimeout = 200 * time.Millisecond
		rc.LeaderLeaseTimeout = 100 * time.Millisecond
		rc.CommitTimeout = 20 * time.Millisecond
		rc.PreVoteDisabled = preVoteDisabled
		rc.LogOutput = io.Discard
		if err := raft.BootstrapCluster(rc, store, store, snaps, n.trans, cfg); err != nil {
			t.Fatalf("bootstrap %s: %v", n.id, err)
		}
		r, err := raft.NewRaft(rc, &raft.MockFSM{}, store, store, snaps, n.trans)
		if err != nil {
			t.Fatalf("NewRaft %s: %v", n.id, err)
		}
		n.r = r
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.r.Shutdown().Error()
			_ = n.trans.Close()
		}
	})
	return nodes
}

func waitRealLeader(t *testing.T, nodes []*realNode, within time.Duration) *realNode {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.r.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected over the real mTLS transport")
	return nil
}

// TestD3PreVoteRealTransport: a 3-node cluster forming a leader already proves
// RequestVote/RequestPreVote/AppendEntries all traverse the real mTLS transport
// (with pre-vote enabled, election cannot complete without the pre-vote round).
// Then we kill 2 of 3, leaving one alive follower as a 1-of-3 minority whose Dial
// to the closed peers fails. With pre-vote ON it must NEVER inflate its term.
func TestD3PreVoteRealTransport(t *testing.T) {
	ca := newTestCA(t)
	nodes := newRealTransportCluster(t, false, ca) // pre-vote enabled
	leader := waitRealLeader(t, nodes, 6*time.Second)

	var survivor *realNode
	for _, n := range nodes {
		if n != leader {
			survivor = n
			break
		}
	}
	for _, n := range nodes {
		if n != survivor {
			_ = n.r.Shutdown().Error()
			_ = n.trans.Close()
		}
	}
	termBefore := survivor.r.CurrentTerm()
	time.Sleep(2 * time.Second)
	if st := survivor.r.CurrentTerm(); st != termBefore {
		t.Fatalf("pre-vote must keep an isolated minority node's term pinned over the real mTLS "+
			"transport: before=%d after=%d", termBefore, st)
	}
}

// TestD3PreVoteRealTransportDisabledControl is the discriminator: with pre-vote
// DISABLED the same isolated minority node campaigns forever and inflates its
// term by a clear margin — so the test above is not a vacuous no-op.
func TestD3PreVoteRealTransportDisabledControl(t *testing.T) {
	ca := newTestCA(t)
	nodes := newRealTransportCluster(t, true, ca) // pre-vote DISABLED
	leader := waitRealLeader(t, nodes, 6*time.Second)

	var survivor *realNode
	for _, n := range nodes {
		if n != leader {
			survivor = n
			break
		}
	}
	for _, n := range nodes {
		if n != survivor {
			_ = n.r.Shutdown().Error()
			_ = n.trans.Close()
		}
	}
	termBefore := survivor.r.CurrentTerm()
	time.Sleep(2 * time.Second)
	if st := survivor.r.CurrentTerm(); st < termBefore+3 {
		t.Fatalf("control: with pre-vote disabled, an isolated node should inflate its term over "+
			"the real transport by a clear margin: before=%d after=%d", termBefore, st)
	}
}

// waitNodeLeader polls a set of cluster.Node for a leader.
func waitNodeLeader(t *testing.T, nodes []*Node, within time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected in the mTLS cluster")
	return nil
}

// TestD3FollowerProposeReturnsNotLeader proves the Node side of D3-R3: on a
// non-leader, Propose (Plan succeeds, Apply rejected) returns an error matching
// raft.ErrNotLeader / ErrLeadershipLost UNWRAPPED — which the broker's seam maps
// to authcallout.ErrNotLeader (a transient deny, never a false-allow or a local
// write).
func TestD3FollowerProposeReturnsNotLeader(t *testing.T) {
	ca := newTestCA(t)
	ids := []raft.ServerID{"np-a", "np-b"}
	trans := make([]*raft.NetworkTransport, len(ids))
	dirs := make([]string, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, string(id)),
		})
		if err != nil {
			t.Fatalf("transport %s: %v", id, err)
		}
		trans[i] = tr
		dirs[i] = t.TempDir()
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: tr.LocalAddr()}
	}
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		n, err := New(fastMultinodeCfg(id, dirs[i], trans[i], servers))
		if err != nil {
			t.Fatalf("New %s: %v", id, err)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Shutdown()
		}
	})

	leader := waitNodeLeader(t, nodes, 6*time.Second)
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	// Plan succeeds (reads the local DB), Apply is rejected on the follower.
	err := follower.ApplyMetaSet("t:np", "v")
	if err == nil {
		t.Fatal("a follower Propose/Apply must fail, not silently apply")
	}
	if !errors.Is(err, raft.ErrNotLeader) && !errors.Is(err, raft.ErrLeadershipLost) {
		t.Fatalf("follower Apply error must match raft.ErrNotLeader/ErrLeadershipLost (unwrapped, so the seam can map it); got %v", err)
	}
}

// TestD3FailingNewReapsTransportNoLeak proves the B3 fix: when cluster.New FAILS
// (here via an invalid raft timeout ordering that ValidateConfig rejects), it must
// still close the injected transport — the caller has no *Node to Shutdown, so the
// listener fd + accept goroutine would otherwise leak. Loops to beat the fd>4
// tolerance.
func TestD3FailingNewReapsTransportNoLeak(t *testing.T) {
	ca := newTestCA(t)
	waitGoroutineBaseline(t, runtime.NumGoroutine()+8, 2*time.Second)
	gBase := runtime.NumGoroutine()
	fdBase := fdCount()

	for i := 0; i < 5; i++ {
		tr, err := NewMTLSTransport(MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "failnew"),
		})
		if err != nil {
			t.Fatalf("transport: %v", err)
		}
		dir := t.TempDir()
		// LeaderLeaseTimeout (2s) > HeartbeatTimeout (1s) => raft.ValidateConfig
		// rejects inside BootstrapCluster, so New fails AFTER the transport is built.
		n, err := New(Config{
			LocalID: raft.ServerID(fmt.Sprintf("failnew-%d", i)), DataDir: dir,
			DBPath: filepath.Join(dir, "state.db"), Transport: tr, Logger: quietLogger(),
			HeartbeatTimeout: 1 * time.Second, ElectionTimeout: 1 * time.Second,
			LeaderLeaseTimeout: 2 * time.Second,
		})
		if err == nil {
			_ = n.Shutdown()
			t.Fatal("New must fail on an invalid timeout ordering")
		}
	}

	waitGoroutineBaseline(t, gBase+4, 3*time.Second)
	if fdBase >= 0 {
		if leaked := fdCount() - fdBase; leaked > 4 {
			t.Fatalf("fd leak: %d fds above baseline after 5 FAILING New calls "+
				"(New did not reap the injected transport on the error path — B3)", leaked)
		}
	}
}
