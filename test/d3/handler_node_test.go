package d3_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// ---- helpers: real cluster.Node + direct DB seeding for the integration tests ----

func openReadHandle(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatalf("open read handle: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedSession inserts an ACTIVE session whose pin_hash matches pin, written to the
// node's DB FILE via a separate WAL handle (the node's own connection sees the
// committed rows).
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

func seedProvisioning(t *testing.T, dbPath, sid, nid, fp string) {
	t.Helper()
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO agent_provisioning(sid,nid,agent_fp,joined_at) VALUES(?,?,?,?)`,
		sid, nid, fp, time.Now().UTC()); err != nil {
		t.Fatalf("seed provisioning: %v", err)
	}
}

// signedAgentReq builds a signed AuthorizationRequestClaims for an agent connect
// (mirrors what nats-server sends) so we can drive a real Handler directly.
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

func mustDecodeResp(t *testing.T, respJWT string) *jwt.AuthorizationResponseClaims {
	t.Helper()
	resp, err := jwt.DecodeAuthorizationResponseClaims(respJWT)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
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

func newRaftNode(t *testing.T, ca *routeCA, id, dir string, peers []raft.Server) *cluster.Node {
	t.Helper()
	tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
		BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.clusterLeaf(t),
	})
	if err != nil {
		t.Fatalf("transport %s: %v", id, err)
	}
	n, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID(id), DataDir: dir, DBPath: filepath.Join(dir, "state.db"),
		Transport: tr, BootstrapPeers: peers,
		HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
		LeaderLeaseTimeout: 75 * time.Millisecond, ApplyTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New %s: %v", id, err)
	}
	return n
}

// TestD3HandlerRealNodeLeaderCommitAllows (M2): a real leader Node.Propose commits
// the PIN provision through raft, and the handler then ALLOWS — proving the end-to-end
// leader-commit→allow path, not a local-write fake.
func TestD3HandlerRealNodeLeaderCommitAllows(t *testing.T) {
	ca := newRouteCA(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	node := newRaftNode(t, ca, "m2", dir, nil) // single-node => leader
	t.Cleanup(func() { _ = node.Shutdown() })
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		t.Fatalf("leader: %v", err)
	}
	seedSession(t, dbPath, "lab", "test-pin")

	readDB := openReadHandle(t, dbPath)
	h := &authcallout.Handler{DB: readDB, AccountKp: newAccountKp(t), Now: time.Now}
	h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return agentprov.PlanProvisionWithPIN(db, sid, nid, fp, pin, auth.VerifyPIN, now)
		})
	}
	clientPub, _ := freshClient(t)

	resp := mustDecodeResp(t, handle(t, h, signedAgentReq(t, clientPub, "lab", "lab-1", "test-pin")))
	if resp.Error != "" {
		t.Fatalf("leader PIN-bootstrap via real Node.Propose must ALLOW; got %q", resp.Error)
	}
	// The binding is committed through raft and visible on the local replica.
	if !waitForCond(2*time.Second, func() bool {
		_, err := agentprov.Lookup(readDB, "lab", "lab-1")
		return err == nil
	}) {
		t.Fatal("leader commit must have written the agent_provisioning row through the FSM")
	}
}

// TestD3HandlerRealNodeFenceLive (M1/T3, §13.8-MANDATORY): the handler is wired to a
// REAL Node.LeaderContactStale. While the follower is in contact, an already-provisioned
// reconnect is ALLOWED. After the leader dies (quorum lost), the reconnect is STILL
// allowed within T_fence (失 quorum 不锁死) and returns fast (no leader round-trip), then
// FENCED once the injected clock advances past T_fence.
func TestD3HandlerRealNodeFenceLive(t *testing.T) {
	ca := newRouteCA(t)
	ids := []string{"fl-a", "fl-b"}
	dirs := []string{t.TempDir(), t.TempDir()}
	// Build transports first so BootstrapPeers can name both addresses.
	trans := make([]*raft.NetworkTransport, 2)
	for i := range ids {
		tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.clusterLeaf(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		trans[i] = tr
	}
	peers := []raft.Server{
		{Suffrage: raft.Voter, ID: raft.ServerID(ids[0]), Address: trans[0].LocalAddr()},
		{Suffrage: raft.Voter, ID: raft.ServerID(ids[1]), Address: trans[1].LocalAddr()},
	}
	nodes := make([]*cluster.Node, 2)
	for i := range ids {
		n, err := cluster.New(cluster.Config{
			LocalID: raft.ServerID(ids[i]), DataDir: dirs[i], DBPath: filepath.Join(dirs[i], "state.db"),
			Transport: trans[i], BootstrapPeers: peers,
			HeartbeatTimeout: 150 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
			LeaderLeaseTimeout: 75 * time.Millisecond, ApplyTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("New %s: %v", ids[i], err)
		}
		nodes[i] = n
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Shutdown()
			}
		}
	})

	// Wait for a leader, pick the follower.
	var leaderIdx, followerIdx = -1, -1
	if !waitForCond(6*time.Second, func() bool {
		for i, n := range nodes {
			if n.IsLeader() {
				leaderIdx = i
				followerIdx = 1 - i
				return true
			}
		}
		return false
	}) {
		t.Fatal("no leader in 2-node cluster")
	}
	follower := nodes[followerIdx]

	// Seed session + provisioning into BOTH node DB files (the binding exists on the
	// follower's local replica regardless of FSM replication timing).
	clientPub, fp := freshClient(t)
	for _, d := range dirs {
		p := filepath.Join(d, "state.db")
		seedSession(t, p, "lab", "test-pin")
		seedProvisioning(t, p, "lab", "lab-1", fp)
	}

	readDB := openReadHandle(t, filepath.Join(dirs[followerIdx], "state.db"))
	var nowOffset time.Duration
	h := &authcallout.Handler{
		DB:                 readDB,
		AccountKp:          newAccountKp(t),
		Now:                func() time.Time { return time.Now().Add(nowOffset) },
		LeaderContactStale: follower.LeaderContactStale,
	}
	noPIN := func() *jwt.AuthorizationResponseClaims {
		return mustDecodeResp(t, handle(t, h, signedAgentReq(t, clientPub, "lab", "lab-1", "")))
	}

	// In contact with the leader: allowed.
	if resp := noPIN(); resp.Error != "" {
		t.Fatalf("a follower in contact must ALLOW an already-provisioned reconnect; got %q", resp.Error)
	}

	// Kill the leader → quorum lost; the follower is a minority (not leader).
	_ = nodes[leaderIdx].Shutdown()
	nodes[leaderIdx] = nil
	if !waitForCond(3*time.Second, func() bool { return !follower.IsLeader() }) {
		t.Fatal("survivor must not be leader after losing quorum")
	}

	// 失 quorum 不锁死: STILL allowed within T_fence, and FAST (no VerifyLeader round-trip).
	nowOffset = 0
	start := time.Now()
	resp := noPIN()
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("authorize must be a local read (no leader round-trip) under quorum loss; took %v", elapsed)
	}
	if resp.Error != "" {
		t.Fatalf("within T_fence of last contact the handler must STILL allow (失 quorum 不锁死); got %q", resp.Error)
	}

	// Past T_fence: fenced (fail closed).
	nowOffset = 2 * cluster.TFence
	if resp := noPIN(); resp.Error == "" {
		t.Fatal("past T_fence the handler must FENCE (deny) an already-provisioned reconnect")
	}
}

// TestD3SeamMapsRaftNotLeaderToTransientDeny (B1): cluster.IsNotLeader recognizes
// the raw raft "not leader" errors, and the broker/D9 seam pattern (map via
// cluster.IsNotLeader -> authcallout.ErrNotLeader) makes the handler classify a
// deposed-leader / follower write as a TRANSIENT deny (not a generic deny, never a
// false-allow). The handler stays raft-free (L-2); the mapping lives in the seam.
func TestD3SeamMapsRaftNotLeaderToTransientDeny(t *testing.T) {
	for _, e := range []error{raft.ErrNotLeader, raft.ErrLeadershipLost} {
		if !cluster.IsNotLeader(e) {
			t.Fatalf("cluster.IsNotLeader must recognize %v", e)
		}
	}
	if cluster.IsNotLeader(errors.New("unrelated")) {
		t.Fatal("cluster.IsNotLeader must not match unrelated errors")
	}

	for _, rawErr := range []error{raft.ErrLeadershipLost, raft.ErrNotLeader} {
		h := &authcallout.Handler{DB: openMemDB(t), AccountKp: newAccountKp(t), Now: time.Now}
		h.ProvisionAgentWrite = func(sid, nid, fp, pin string, now time.Time) error {
			// Exactly what the broker/D9 closure does over node.Propose:
			if cluster.IsNotLeader(rawErr) {
				return authcallout.ErrNotLeader
			}
			return rawErr
		}
		clientPub, _ := freshClient(t)
		resp := mustDecodeResp(t, handle(t, h, signedAgentReq(t, clientPub, "lab", "lab-1", "test-pin")))
		if resp.Error == "" {
			t.Fatalf("a mapped %v must be DENIED, not allowed", rawErr)
		}
		if !strings.Contains(resp.Error, "not_leader") {
			t.Errorf("a deposed-leader write (%v) must be classified transient not_leader; got %q", rawErr, resp.Error)
		}
		if _, err := agentprov.Lookup(h.DB, "lab", "lab-1"); err == nil {
			t.Fatalf("a transient deny must NOT write a row (raw %v)", rawErr)
		}
	}
}

func handle(t *testing.T, h *authcallout.Handler, req string) string {
	t.Helper()
	out, err := h.Handle(req)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	return out
}

func newAccountKp(t *testing.T) nkeys.KeyPair {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	return kp
}
