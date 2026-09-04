//go:build d7_integration

package d7_test

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
)

// integration_test.go — D7 §13.11/.12 multi-node drills (gated d7_integration, run as
// TestD7Matrix -race; OUT of make test). A REAL ≥2-node raft cluster over an inmem
// transport proves the membership two-phase + FSM replication + force-single offline
// recovery faithfully (the mTLS/NATS transport security is proven separately in
// D3/D5; it does not change membership semantics). The cheap op/applier/guard units
// are in make test.

type d7Cluster struct {
	ids     []string
	nodes   []*cluster.Node
	dataDir []string
	dbPath  []string
	admin   *broker.ClusterAdmin // bootstrap orchestrator; addNodeRetry rebinds to the current leader
}

// startD7Cluster bootstraps node 0 as a single voter and brings up n-1 EMPTY-state
// nodes connected over inmem transports, ready for dynamic AddVoter.
//
// The harness uses the production Multinode timing constants directly. Earlier
// 60/60/30ms and 300/300/150ms fixtures manufactured elections under -race and
// full load; aliases were also removed so the timing guard can detect drift.

func startD7Cluster(t *testing.T, n int) *d7Cluster {
	t.Helper()
	c := &d7Cluster{ids: make([]string, n), nodes: make([]*cluster.Node, n), dataDir: make([]string, n), dbPath: make([]string, n)}
	addrs := make([]raft.ServerAddress, n)
	trans := make([]*raft.InmemTransport, n)
	for i := 0; i < n; i++ {
		c.ids[i] = "d7-" + string(rune('a'+i))
		a, tr := raft.NewInmemTransport(raft.ServerAddress(c.ids[i]))
		addrs[i] = a
		trans[i] = tr
	}
	// Fully connect the transports so the leader can replicate to a dynamically added node.
	for i := range trans {
		for j := range trans {
			if i != j {
				trans[i].Connect(addrs[j], trans[j])
			}
		}
	}
	for i := 0; i < n; i++ {
		c.dataDir[i] = t.TempDir()
		c.dbPath[i] = filepath.Join(c.dataDir[i], "tether.db")
		cfg := cluster.Config{
			LocalID: raft.ServerID(c.ids[i]), DataDir: c.dataDir[i], DBPath: c.dbPath[i],
			Transport: trans[i], ApplyTimeout: 5 * time.Second,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
		}
		if i == 0 {
			cfg.BootstrapPeers = []raft.Server{{Suffrage: raft.Voter, ID: raft.ServerID(c.ids[0]), Address: addrs[0]}}
		}
		nd, err := cluster.New(cfg)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		c.nodes[i] = nd
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			if nd != nil {
				_ = nd.Shutdown()
			}
		}
	})
	// External review M4: WaitForLeader only proves a leader EXISTS. c.admin is
	// bound to nodes[0], so every seed AddNode fails with "node is not the
	// leader" if leadership landed elsewhere or moved during startup — which is
	// what made ForgedSigPoisonSkipsOnFollower flake inside the loaded e2e run
	// while passing when d7 ran alone. addNodeRetry cannot recover from it: it
	// retries the same non-leader node.
	//
	// Wait for nodes[0] to hold leadership ITSELF, which is the invariant the
	// harness actually depends on.
	if err := c.nodes[0].WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("node 0 leadership: %v", err)
	}
	selfLead := time.Now().Add(5 * time.Second)
	for !c.nodes[0].IsLeader() {
		if time.Now().After(selfLead) {
			t.Fatalf("node 0 did not become leader within 5s (a leader exists elsewhere); " +
				"the harness binds ClusterAdmin to node 0, so every seed AddNode would fail " +
				"with \"not the leader\"")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.admin = broker.NewClusterAdmin(c.nodes[0], nil)
	return c
}

func (c *d7Cluster) joinInput(t *testing.T, i int) cluster.ClusterNodeUpsertInput {
	t.Helper()
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	nonce := "nonce-" + c.ids[i]
	sig, _ := auth.SignWithSeed(seed, cluster.JoinSignBytes(c.ids[i], pub, nonce))
	// The serving-port placeholders MUST be guaranteed-unreachable so the force-single
	// ConfirmedDead TCP-liveness probe (which dials raft_addr + nats_route + tunnel_addr) treats
	// these abandoned peers as DEAD. 127.0.0.1:1 gives an immediate connection-refused everywhere;
	// the previous "x:7000" / "nats://x" placeholders resolve as ALIVE on a WSL2/fake-ip box (and
	// even 192.0.2.x TEST-NET answers there), which would split-brain-HARD-REFUSE the test.
	// raft_addr stays c.ids[i] (the InmemTransport address); it has no port so the probe dials it
	// dead too.
	return cluster.ClusterNodeUpsertInput{
		NodeID: c.ids[i], Name: c.ids[i], NodeIdentPub: pub, NatsServerID: "tether-" + c.ids[i],
		// The ROSTER's raft_addr is a replicated informational copy, not the raft
		// transport address this in-memory fixture peers on — so it can be
		// address-shaped without disturbing the cluster. It has to be: the roster
		// admission planner now validates it exactly as its own rewrite planner always
		// has (prerelease audit cluster-fsm/L3-F2).
		RaftAddr: c.ids[i] + ":7400", NatsRoute: "nats://127.0.0.1:1", TunnelAddr: "127.0.0.1:1", PublicHost: "h",
		CertFP: "sha256:ab", JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig), Now: time.Now(),
	}
}

func readPhase(t *testing.T, n *cluster.Node, nodeID string) (string, bool) {
	t.Helper()
	var phase string
	var found bool
	_ = n.BoundedStaleRead(func(db *sql.DB) error {
		err := db.QueryRow(`SELECT phase FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&phase)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return phase, found
}

func TestD7Matrix(t *testing.T) {
	t.Run("AddNodeReplicatesToFollower", testD7AddNodeReplicates)
	t.Run("ForgedSigPoisonSkipsOnFollower", testD7ForgedSigOnFollower)
	t.Run("AddNeverCatchesUpNoSilentFork", testD7AddNeverCatchesUp)
	t.Run("ForceSingleRecoverRestart", testD7ForceSingleRecover)
	t.Run("ReconcileForwardCompletesPendingVoter", testD7ReconcileMultiNode) // review M5
	t.Run("DrainRetireFollower", testD7DrainRetireFollower)                  // review M4
	t.Run("DrainLeaderTransfersAndBails", testD7DrainLeaderTransfers)        // review B5
	t.Run("DrainRefusesRebuildOff", testD7DrainRefusesRebuildOff)            // external review F3
	t.Run("FollowerStatusViewSource", testD7FollowerStatusViewSource)        // B1 review F3
}

// testD7FollowerStatusViewSource (B1 review F3): StatusReport on a real FOLLOWER must stamp
// IsLeaderView=false + ViewHost=self + LeaderID=the leader, so the user-facing footer correctly
// says "re-run on the leader". The single-node harness only ever produces IsLeaderView=true; this
// is the only test that exercises the false POPULATION path (not just serialization of a literal).
func testD7FollowerStatusViewSource(t *testing.T) {
	c := startD7Cluster(t, 2)
	in := c.joinInput(t, 1)
	caughtUp := func(barrier uint64) (bool, error) {
		cur, err := c.nodes[1].AppliedIndex()
		return cur >= barrier, err
	}
	if err := c.addNodeRetry(in, c.ids[1], caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	followerAdmin := broker.NewClusterAdmin(c.nodes[1], nil)
	// Poll: the follower must both have the replicated roster AND know the leader.
	deadline := time.Now().Add(3 * time.Second)
	for {
		rep, err := followerAdmin.StatusReport("ctl-nats")
		if err == nil && rep.LeaderID != "" {
			if rep.ViewHost != c.ids[1] {
				t.Errorf("ViewHost = %q, want follower %q", rep.ViewHost, c.ids[1])
			}
			if rep.IsLeaderView {
				t.Errorf("a follower must report IsLeaderView=false (leader=%q self=%q)", rep.LeaderID, c.ids[1])
			}
			if rep.LeaderID != c.ids[0] {
				t.Errorf("LeaderID = %q, want leader %q", rep.LeaderID, c.ids[0])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower StatusReport never populated a leader: err=%v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// testD7DrainRefusesRebuildOff (external review F3): a rebuild-OFF expose homed on
// the drained node must NOT be silently rehomed — drain refuses, enumerating it.
func testD7DrainRefusesRebuildOff(t *testing.T) {
	c := startD7Cluster(t, 3)
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		cu := func(idx int) func(uint64) (bool, error) {
			return func(b uint64) (bool, error) { cur, e := c.nodes[idx].AppliedIndex(); return cur >= b, e }
		}(i)
		if err := c.addNodeRetry(in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	// Seed a rebuild-OFF expose homed on node-2, directly into the LEADER's DB (the
	// orchestrator on node 0 reads it via BoundedStaleRead). A direct write is fine for
	// the test — migrateExposes only reads port + rebuild_on_failure.
	db, err := storage.OpenWAL("file:" + c.dbPath[0])
	if err != nil {
		t.Fatalf("open leader db: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES ('s1','s1','SHA256:o','h')`); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES ('s1','n1',?, 'ONLINE')`, time.Now().UTC()); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch, rebuild_on_failure)
		 VALUES (9090,'s1','n1','off-expose',8080,'th','ALLOCATED','SHA256:o',?,?,0,0)`,
		time.Now().UTC(), c.ids[2],
	); err != nil {
		t.Fatalf("seed rebuild-OFF port: %v", err)
	}
	_ = db.Close()

	err = c.admin.DrainNode(c.ids[2], false, true, time.Now(), nil)
	var ro *broker.ErrRebuildOffExposes
	if !errors.As(err, &ro) {
		t.Fatalf("drain with a rebuild-OFF expose must refuse (ErrRebuildOffExposes), got %v", err)
	}
	if len(ro.Ports) != 1 || ro.Ports[0] != 9090 {
		t.Fatalf("refusal must enumerate the rebuild-OFF port 9090, got %v", ro.Ports)
	}
	// The expose was NOT rehomed (still home_broker=node-2).
	db2, _ := storage.OpenReadOnly("file:" + c.dbPath[0])
	defer func() { _ = db2.Close() }()
	var home string
	_ = db2.QueryRow(`SELECT home_broker FROM port_allocations WHERE port=9090`).Scan(&home)
	if home != c.ids[2] {
		t.Fatalf("rebuild-OFF expose was silently rehomed: home_broker=%q, want %q", home, c.ids[2])
	}
}

// testD7AddNodeReplicates: a real dynamic AddNode walks node-1 to VOTER and the
// roster row REPLICATES to the new follower's own DB (the cross-node proof).
func testD7AddNodeReplicates(t *testing.T) {
	c := startD7Cluster(t, 2)
	in := c.joinInput(t, 1)
	caughtUp := func(barrier uint64) (bool, error) {
		cur, err := c.nodes[1].AppliedIndex()
		return cur >= barrier, err
	}
	if err := c.addNodeRetry(in, c.ids[1], caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// The follower (node 1) must have the roster row at VOTER (replicated via raft).
	deadline := time.Now().Add(3 * time.Second)
	for {
		p, ok := readPhase(t, c.nodes[1], c.ids[1])
		if ok && p == "VOTER" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never saw replicated VOTER row (phase=%q ok=%v)", p, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nv, _ := c.nodes[0].NumVoters(); nv != 2 {
		t.Fatalf("NumVoters = %d, want 2", nv)
	}
}

// testD7ForgedSigOnFollower: a forged ClusterNodeUpsert committed through the leader
// must POISON-SKIP on the FOLLOWER (read the follower's DB — the leader pre-verified;
// the cross-node proof is on followers). No replica panics; both stay live.
func testD7ForgedSigOnFollower(t *testing.T) {
	c := startD7Cluster(t, 2)
	// First add node-1 so it is a live follower applying the log.
	in1 := c.joinInput(t, 1)
	caughtUp := func(barrier uint64) (bool, error) { cur, e := c.nodes[1].AppliedIndex(); return cur >= barrier, e }
	if err := c.addNodeRetry(in1, c.ids[1], caughtUp, 5*time.Second); err != nil {
		t.Fatalf("seed AddNode: %v", err)
	}

	// Forge: a well-formed ClusterNodeUpsert for "evil" whose sig is from a DIFFERENT key.
	victimSeed, _ := auth.GenerateUserSeed()
	victimPub, _ := auth.PublicKeyFromSeed(victimSeed)
	advSeed, _ := auth.GenerateUserSeed()
	nonce := "nonce-evil"
	msg := cluster.JoinSignBytes("evil", victimPub, nonce)
	victimSig, _ := auth.SignWithSeed(victimSeed, msg)
	advSig, _ := auth.SignWithSeed(advSeed, msg) // verifies under advPub, NOT victimPub
	good := cluster.ClusterNodeUpsertInput{
		NodeID: "evil", Name: "evil", NodeIdentPub: victimPub, RaftAddr: "10.9.9.9:7400", NatsRoute: "nats://10.9.9.9:6222", PublicHost: "h", CertFP: "sha256:ab",
		JoinNonce: nonce, JoinSigHex: hex.EncodeToString(victimSig), Now: time.Now(),
	}
	cmd, err := cluster.PlanClusterNodeUpsert(good)
	if err != nil {
		t.Fatalf("seed forged cmd: %v", err)
	}
	// Swap the sig to the adversary's (so the cross-check passes, the verify fails).
	vHex, aHex := hex.EncodeToString(victimSig), hex.EncodeToString(advSig)
	cmd.Body[0].SQL = strings.ReplaceAll(cmd.Body[0].SQL, vHex, aHex)
	cmd.Aux = mustForgeAux(t, "evil", victimPub, nonce, aHex)

	// Commit the forged entry through the leader's raft (bypassing the pre-verify).
	if err := c.nodes[0].Apply(cmd); err != nil {
		t.Fatalf("apply forged: %v", err) // must NOT be an error/panic — poison-skip returns nil
	}
	// The FOLLOWER must have NO 'evil' row, and must still be live (apply a legit op).
	time.Sleep(150 * time.Millisecond)
	if _, ok := readPhase(t, c.nodes[1], "evil"); ok {
		t.Fatal("forged sig wrote a roster row on the FOLLOWER — must write NONE")
	}
	// Prove the follower stays live: add a legit node-... via a meta op the leader proposes.
	if err := c.nodes[0].ApplyMetaSet("t:liveness", "ok"); err != nil {
		t.Fatalf("post-forgery legit op failed (follower wedged?): %v", err)
	}
}

// testD7AddVoterFail: an add that does not complete (the joiner never catches up)
// must NEVER silently leave the roster at VOTER — the half-state is recorded as a
// NON-voter phase (PENDING / CATCHING_UP / VOTER_ADD_FAILED), so status never shows
// the "DB voter, raft non-voter" fork. AddNode must return an error (not a false ok).
func testD7AddNeverCatchesUp(t *testing.T) {
	c := startD7Cluster(t, 2)
	in := c.joinInput(t, 1)
	in.NodeID = "ghost" // not a connected transport — never catches up
	in.RaftAddr = "ghost"
	seed, _ := auth.GenerateUserSeed()
	pub, _ := auth.PublicKeyFromSeed(seed)
	sig, _ := auth.SignWithSeed(seed, cluster.JoinSignBytes("ghost", pub, in.JoinNonce))
	in.NodeIdentPub = pub
	in.JoinSigHex = hex.EncodeToString(sig)
	caughtUp := func(uint64) (bool, error) { return false, nil } // never catches up
	if err := c.admin.AddNode(in, "ghost", caughtUp, 200*time.Millisecond); err == nil {
		t.Fatal("AddNode that never catches up must return an error, not a false ok")
	}
	// No silent fork: the roster must NOT show ghost as a healthy VOTER.
	p, ok := readPhase(t, c.nodes[0], "ghost")
	if ok && p == "VOTER" {
		t.Fatalf("silent fork: ghost roster shows VOTER but the add never completed")
	}
	if ok && p != "JOIN_VERIFIED_PENDING_VOTER" && p != "CATCHING_UP" && p != "VOTER_ADD_FAILED" {
		t.Fatalf("ghost roster phase = %q — not a recognized half-state", p)
	}
}

// d7TransientAddErr reports whether an AddNode error is a TRANSIENT raft leadership change
// (the orchestrator's phase Propose raced an election) — safe to retry, since the two-phase
// membership change is idempotent. Only fires under full make e2e-parallel -race contention.
func d7TransientAddErr(err error) bool {
	if err == nil {
		return false
	}
	if cluster.IsNotLeader(err) || errors.Is(err, raft.ErrLeadershipLost) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "leadership lost") || strings.Contains(s, "not leader") || strings.Contains(s, "not the leader")
}

// addNodeRetry seeds a cluster member, retrying a TRANSIENT raft leadership change (the
// orchestrator phase Propose racing an election under full make e2e-parallel -race contention). The
// two-phase membership change is idempotent, so a re-run converges. Used by every SEED site;
// the ghost/half-state drills that ASSERT an error call AddNode directly.
// addNodeRetry retries a transient AddNode, waiting for nodes[0] to REGAIN
// leadership before each attempt.
//
// External review M4: this used to retry 6 times at a fixed 300ms with no
// leadership check, so a leadership blip during the seed phase burned all six
// attempts re-sending to a node that was still not the leader. That is what
// failed inside the loaded 18-minute e2e run while passing when d7 ran alone,
// and it got worse under -race, where elections take longer relative to the
// fixed budget. Budget is now wall-clock and each retry waits for the invariant
// the harness depends on (c.admin is bound to nodes[0]) to hold again.
func (c *d7Cluster) addNodeRetry(in cluster.ClusterNodeUpsertInput, raftAddr string, caughtUp func(uint64) (bool, error), maxWait time.Duration) error {
	deadline := time.Now().Add(30 * time.Second)
	var err error
	for {
		// Wait against the REMAINING budget rather than a fresh fixed slice.
		// Under a loaded e2e run leadership can move away and take longer than
		// any one attempt's share to come back; a per-attempt 5s made the helper
		// give up while the overall budget still had 15s left.
		// External review R5 / M4, THIRD iteration — and the first one that stops
		// fighting raft. The previous two both kept the harness's assumption that
		// nodes[0] holds leadership and tried ever harder to restore it: wait
		// longer, then actively transfer leadership back. Under load that lost 58
		// transfer attempts in 29.7s, because a voter that just won an election has
		// no reason to hand it back, and every attempt raced the next commit.
		//
		// The assumption itself was the defect. AddNode has to run ON the leader;
		// it does not have to run on nodes[0]. So bind the orchestrator to whoever
		// currently holds leadership instead of demanding leadership come to us.
		// See docs/testing-standards.md T3: re-establish the premise, do not force
		// the world to match a stale observation.
		admin, lerr := c.adminForLeader(time.Until(deadline))
		if lerr != nil {
			if err != nil {
				return fmt.Errorf("%w (last AddNode error: %v)", lerr, err)
			}
			return lerr
		}
		err = admin.AddNode(in, raftAddr, caughtUp, maxWait)
		if err == nil || !d7TransientAddErr(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("AddNode still transient after 30s: %w", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// adminForLeader returns an orchestrator bound to whichever node currently holds
// leadership, waiting up to d for one to exist.
//
// This replaces "make nodes[0] the leader again" with "talk to the leader". The
// harness only ever needed AN orchestrator on THE leader; that it happened to be
// nodes[0] was an incidental property of bootstrap, not a requirement, and
// enforcing it under load is what produced the 58-transfer-attempt failure.
func (c *d7Cluster) adminForLeader(d time.Duration) (*broker.ClusterAdmin, error) {
	if d < 2*time.Second {
		d = 2 * time.Second
	}
	deadline := time.Now().Add(d)
	for {
		for i, n := range c.nodes {
			if n != nil && n.IsLeader() {
				if i == 0 {
					return c.admin, nil
				}
				return broker.NewClusterAdmin(n, nil), nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no node held leadership within %s", d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// applyMetaSetOnLeaderAndWaitApplied commits through whichever node currently leads, then waits
// until nodes[survivorIdx] has APPLIED the value.
//
// Both halves are load-bearing. Committing through the current leader replaces the old
// `c.nodes[survivorIdx].ApplyMetaSet(...)`, which assumed the bootstrap node still led after two
// membership changes and returned "node is not the leader" under `make e2e-parallel` load. Waiting
// for the SURVIVOR to apply it is the other half: this seed exists so the restart can prove state was
// preserved exactly, and the survivor is the node whose disk is reused — a commit on some other
// leader proves nothing about what the survivor will replay.
//
// The survivor index is a PARAMETER, not nodes[0]: the caller already tracks it, and a helper that
// hardcoded 0 would silently wait on the wrong node the day the caller picks a different survivor.
func (c *d7Cluster) applyMetaSetOnLeaderAndWaitApplied(survivorIdx int, key, value string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for {
		for _, n := range c.nodes {
			if n == nil || !n.IsLeader() {
				continue
			}
			err := n.ApplyMetaSet(key, value)
			if err == nil {
				for time.Now().Before(deadline) {
					var got string
					readErr := c.nodes[survivorIdx].BoundedStaleRead(func(db *sql.DB) error {
						return db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&got)
					})
					if readErr == nil && got == value {
						return nil
					}
					if readErr != nil && !errors.Is(readErr, sql.ErrNoRows) {
						return fmt.Errorf("observe committed meta on survivor: %w", readErr)
					}
					time.Sleep(20 * time.Millisecond)
				}
				return fmt.Errorf("survivor did not apply meta %q within %s", key, d)
			}
			if !cluster.IsNotLeader(err) {
				return err
			}
			// A not-leader error means leadership moved between IsLeader() and the Apply — the same
			// snapshot race this helper exists for. Fall through to re-scan for the current leader.
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no leader committed meta %q within %s", key, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ensureSelfLeader restores the invariant the harness is built on: c.admin is
// bound to nodes[0], so nodes[0] must be the leader.
//
// External review M4, second iteration. Waiting alone is not enough — the
// failure was never "the election is slow". Under a loaded e2e run a commit can
// lose leadership mid-flight ("leadership lost while committing log") and raft
// hands it to another voter, which has no reason to give it back. nodes[0] then
// stays a follower forever and every retry, however patient, re-sends to the
// wrong node. The observed wait was 29.7s against a 30s budget: not a race that
// more time would win.
//
// So: wait briefly, and if leadership went elsewhere, ASK the current leader to
// transfer it back before retrying.
func (c *d7Cluster) ensureSelfLeader(d time.Duration) error {
	if d < 2*time.Second {
		d = 2 * time.Second
	}
	deadline := time.Now().Add(d)
	// Poll-and-nudge until the budget runs out. A single transfer attempt was
	// not enough: right after "leadership lost while committing log" there is
	// often NO leader at all for a moment, so the one attempt finds nothing to
	// ask and the remaining wait is passive. Re-checking means we catch the new
	// leader as soon as the election settles.
	for attempt := 0; ; attempt++ {
		if err := c.waitSelfLeader(500 * time.Millisecond); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node 0 did not hold leadership within %s after %d transfer attempt(s); "+
				"ClusterAdmin is bound to it (leadership moved under load — external review M4)", d, attempt)
		}
		c.nudgeLeadershipHome()
	}
}

// nudgeLeadershipHome asks whichever node in OUR configuration currently holds
// leadership to hand it back to nodes[0]. Best-effort and idempotent.
func (c *d7Cluster) nudgeLeadershipHome() {
	for i := 1; i < len(c.nodes); i++ {
		if c.nodes[i] == nil || !c.nodes[i].IsLeader() {
			continue
		}
		// Only a node that shares OUR raft configuration is holding OUR
		// leadership. startD7Cluster brings up n raft instances but bootstraps
		// only nodes[0]; the rest are un-joined singletons that legitimately
		// lead their own one-node configuration. Asking one of those to transfer
		// leadership to d7-a fails with "not in the raft configuration" — which
		// is not a symptom of anything, just the wrong node to ask.
		if !c.sharesConfigWithSelf(i) {
			continue
		}
		_ = broker.NewClusterAdmin(c.nodes[i], nil).TransferLeaderTo(c.ids[0])
		return
	}
}

// sharesConfigWithSelf reports whether nodes[i] is in the same raft
// configuration as nodes[0] — i.e. whether its leadership is ours to reclaim.
func (c *d7Cluster) sharesConfigWithSelf(i int) bool {
	cfg, err := c.nodes[i].RaftConfiguration()
	if err != nil {
		return false
	}
	for _, srv := range cfg {
		if srv.NodeID == c.ids[0] {
			return true
		}
	}
	return false
}

// waitSelfLeader blocks until nodes[0] holds leadership itself.
func (c *d7Cluster) waitSelfLeader(d time.Duration) error {
	deadline := time.Now().Add(d)
	if d <= 0 {
		d = time.Second
		deadline = time.Now().Add(d)
	}
	for !c.nodes[0].IsLeader() {
		if time.Now().After(deadline) {
			return fmt.Errorf("node 0 did not hold leadership within %s; ClusterAdmin is bound to it "+
				"(leadership moved under load — see external review M4)", d)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// testD7ForceSingleRecover: kill a 3-node cluster, force-single a survivor (peers
// dead), restart it as a writable N=1, and prove the restart does not double-apply.
func testD7ForceSingleRecover(t *testing.T) {
	c := startD7Cluster(t, 3)
	// Seed the bootstrap survivor's OWN roster row. In production every cluster node has a
	// cluster_nodes row (D9 `cluster init --from-existing` seeds the self VOTER row); force-single
	// refuses to rewrite the raft config for a self-id absent from cluster_nodes (readRoster). The
	// raft-only bootstrap here doesn't lay that row, so seed it via the normal committed upsert.
	if err := c.nodes[0].Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(c.joinInput(t, 0))
	}); err != nil {
		t.Fatalf("seed self roster row: %v", err)
	}
	// Add the other two so the survivor's roster lists them (force-single must abandon them).
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		cu := func(idx int) func(uint64) (bool, error) {
			return func(barrier uint64) (bool, error) { cur, e := c.nodes[idx].AppliedIndex(); return cur >= barrier, e }
		}(i)
		if err := c.addNodeRetry(in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	survivorIdx := 0
	// Seed a UNIQUE committed marker + record the exact roster count, so the restart
	// can prove state was preserved EXACTLY (not lost, not doubled) — review M7: the
	// old test only checked applied_index monotonicity (vacuous). The no-double-apply
	// of the replay itself is the D1 idempotent-skip property RecoverCluster reuses.
	if err := c.applyMetaSetOnLeaderAndWaitApplied(survivorIdx, "t:premark", "preval", 10*time.Second); err != nil {
		t.Fatalf("seed premark: %v", err)
	}
	appliedBefore, _ := c.nodes[survivorIdx].AppliedIndex()
	// The roster now holds the survivor's own row (seeded above) + the two added peers. Record
	// it and assert it is unchanged after recovery (no double-apply), without hardcoding the count.
	rosterBefore := countRows(t, c.nodes[survivorIdx], "cluster_nodes")

	// Stop ALL nodes (daemon stopped — the offline tool can take the disk). Null out
	// the slice entries so the t.Cleanup does not double-Shutdown.
	for i, nd := range c.nodes {
		if nd != nil {
			_ = nd.Shutdown()
		}
		c.nodes[i] = nil
	}

	// force-single on the survivor's disk. The abandoned peers' raft_addr ("d7-b"/"d7-c")
	// are not dialable TCP => treated as dead.
	abandoned, err := clusteroffline.ForceSingle(clusteroffline.ForceSingleOptions{
		DataDir: c.dataDir[survivorIdx], DBPath: c.dbPath[survivorIdx],
		SelfID: c.ids[survivorIdx], SelfRaftAddr: c.ids[survivorIdx],
		ConfirmedDead: []string{c.ids[1], c.ids[2]},
	})
	if err != nil {
		t.Fatalf("force-single: %v", err)
	}
	if len(abandoned) != 2 {
		t.Fatalf("abandoned %d peers, want 2", len(abandoned))
	}

	// Restart the survivor as a single-voter cluster — it must reach leadership + be writable,
	// and its applied_index must not regress (no double-apply / no lost commits).
	_, tr := raft.NewInmemTransport(raft.ServerAddress(c.ids[survivorIdx]))
	nd, err := cluster.New(cluster.Config{
		LocalID: raft.ServerID(c.ids[survivorIdx]), DataDir: c.dataDir[survivorIdx], DBPath: c.dbPath[survivorIdx],
		Transport: tr, ApplyTimeout: 5 * time.Second,
		HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
		LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
	})
	if err != nil {
		t.Fatalf("restart survivor: %v", err)
	}
	defer func() { _ = nd.Shutdown() }()
	if err := nd.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("survivor never regained leadership: %v", err)
	}
	// Recovery point preserved: applied_index reached the local LastIndex (RecoverCluster
	// replayed the whole local log) and did not regress.
	appliedAfter, _ := nd.AppliedIndex()
	if appliedAfter < appliedBefore {
		t.Fatalf("applied_index regressed after force-single: before=%d after=%d", appliedBefore, appliedAfter)
	}
	// State preserved EXACTLY (review M7): the seeded marker keeps its exact value (not lost, not corrupted).
	// The roster converges to the survivor's OWN row only — G2 #12: force-single PRUNES the abandoned peers
	// (rosterBefore - abandoned). The restart must NOT double-apply (re-materialize the pruned peers from
	// the replayed log), so the count stays at the pruned value EXACTLY — this also pins that an offline
	// prune survives a restart (the abandoned peers do not resurrect).
	var marker string
	_ = nd.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:premark'`).Scan(&marker)
	})
	if marker != "preval" {
		t.Fatalf("committed state lost/corrupted across force-single: t:premark = %q, want preval", marker)
	}
	wantRoster := rosterBefore - len(abandoned)
	if got := countRows(t, nd, "cluster_nodes"); got != wantRoster {
		t.Fatalf("roster row count after force-single prune + restart: %d, want %d (rosterBefore=%d - abandoned=%d; prune undone or double-apply?)", got, wantRoster, rosterBefore, len(abandoned))
	}
	// Writable: a fresh op commits.
	if err := nd.ApplyMetaSet("t:post-recover", "ok"); err != nil {
		t.Fatalf("single-voter cluster not writable after force-single: %v", err)
	}
	// force_single_active marker is set (status would report exit 3).
	var fs string
	_ = nd.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key='force_single_active'`).Scan(&fs)
	})
	if fs == "" {
		t.Error("force_single_active marker not set after force-single")
	}
}

func countRows(t *testing.T, n *cluster.Node, table string) int {
	t.Helper()
	var c int
	_ = n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&c)
	})
	return c
}

// testD7ReconcileMultiNode (review M5): on a REAL 2-node raft cluster, a roster row
// at PENDING whose node IS a raft voter (AddVoter committed, the old leader died
// before the phase bump) must be forward-completed to CATCHING_UP by the
// reconciliation pass — never RemoveServer'd, never left stuck.
func testD7ReconcileMultiNode(t *testing.T) {
	c := startD7Cluster(t, 2)
	in := c.joinInput(t, 1)
	// Phase-1 commit (roster=PENDING) THEN AddVoter directly — but DO NOT run the
	// phase bump, simulating a leader crash after AddVoter committed.
	if err := c.nodes[0].Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) }); err != nil {
		t.Fatalf("phase-1: %v", err)
	}
	if err := c.nodes[0].AddVoter(c.ids[1], c.ids[1]); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if p, _ := readPhase(t, c.nodes[0], c.ids[1]); p != "JOIN_VERIFIED_PENDING_VOTER" {
		t.Fatalf("setup: want PENDING, got %q", p)
	}
	// Reconcile: {PENDING ∧ raft-voter} -> CATCHING_UP (forward-complete, no RemoveServer).
	if err := c.admin.ReconcileMembershipOnLeadership(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if p, _ := readPhase(t, c.nodes[0], c.ids[1]); p != "CATCHING_UP" {
		t.Fatalf("reconcile did not forward-complete PENDING->CATCHING_UP, got %q", p)
	}
	if nv, _ := c.nodes[0].NumVoters(); nv != 2 {
		t.Fatalf("reconcile RemoveServer'd a mid-promote node: NumVoters=%d, want 2", nv)
	}
}

// testD7DrainRetireFollower (review M4 / external-review B5): retire a FOLLOWER
// end-to-end on a real 3-node cluster through the OPERATION path — it must leave
// both the raft configuration and the roster (the §8.1 removal order), with no
// half-state.
//
// Batch-A A13 deleted the synchronous DrainNode(retire=true) route this used to
// drive: it performed RemoveServer plus a roster delete with no crash-resumable
// deadline, no BLOCKED escape hatch and no TOCTOU recheck, and no released CLI
// could reach it. External review B5 caught the replacement being a refusal
// assertion only — which left the release matrix with NO end-to-end proof that
// retire works, while this function's name still promised one. Both halves are
// asserted now: the dead route stays refused, and the live route is driven to
// completion.
func testD7DrainRetireFollower(t *testing.T) {
	c := startD7Cluster(t, 3)
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		cu := func(idx int) func(uint64) (bool, error) {
			return func(b uint64) (bool, error) { cur, e := c.nodes[idx].AppliedIndex(); return cur >= b, e }
		}(i)
		if err := c.addNodeRetry(in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}

	// Half 1: the removed synchronous route must stay refused, and the refusal
	// must name the supported verb (a bare "no" gets worked around).
	if err := c.admin.DrainNode(c.ids[2], true, true, time.Now(), func() (bool, error) { return true, nil }); err == nil {
		t.Fatal("DrainNode still accepts --retire: the unprotected irreversible path is reachable again")
	} else if !strings.Contains(err.Error(), "cluster retire") {
		t.Fatalf("refusal must name the supported verb, got %v", err)
	}

	// Half 2: the live route. F after = N=2 (retire 3->2), F==0, so confirmed=true
	// is required. No exposes homed here -> migrateExposes no-ops.
	//
	// The readiness probe must be supplied explicitly: without it the gate fails
	// closed (external review B5), which is correct for production but would hang
	// this harness — there is no JetStream behind these raft-only nodes.
	c.admin.SetStreamsReadyProbe(func(string) (bool, error) { return true, nil })

	opID, err := c.admin.StartRetireOperation(c.ids[2], true)
	if err != nil {
		t.Fatalf("StartRetireOperation: %v", err)
	}
	if opID == "" {
		t.Fatal("StartRetireOperation returned an empty op id")
	}

	// Drive it the way the observe loop does, until the OPERATION is terminal.
	//
	// Waiting on the roster row alone is not enough: the row is deleted several
	// steps before the op finishes (NATS_ROLLED_OUT still follows), and stopping
	// there would assert a membership change whose operation record is still
	// in flight — the precise half-state this test is supposed to rule out.
	// Drive to NATS_ROLLED_OUT, which is where every IRREVERSIBLE step has
	// completed: RemoveServer has run and the roster row is deleted (§8.1 order).
	//
	// It does not run to terminal RETIRED here, and that is a property of the
	// harness, not of the product: the last hop gates on C3 topology convergence,
	// which requires every voter to report a topology generation. These are
	// raft-only nodes with no topology reconciler, so no voter ever reports one.
	// Faking that report would make the test assert a convergence that did not
	// happen — the whole class of defect this batch has been cleaning up. What is
	// asserted instead is everything that CANNOT be undone, which is what the
	// membership-change safety argument rests on. The RETIRED transition itself
	// carries no membership mutation (drain-marker clear + seed convergence) and
	// is covered by the controller unit suite.
	deadline := time.Now().Add(30 * time.Second)
	var op *cluster.Operation
	for {
		// The controller only advances an operation on the leader. A retire
		// TRANSFERS leadership as one of its steps (LEADER_TRANSFERRED), so
		// driving blindly on nodes[0] stalls there — which is what the parallel
		// e2e run surfaced. Reclaim leadership each turn before driving.
		if lerr := c.ensureSelfLeader(5 * time.Second); lerr != nil && time.Now().After(deadline) {
			t.Fatalf("retire op %s: %v", opID, lerr)
		}
		c.admin.DriveOperationsForTest()
		var oerr error
		op, oerr = cluster.OperationByID(c.nodes[0].RODB(), opID)
		if oerr != nil {
			t.Fatalf("OperationByID(%s): %v", opID, oerr)
		}
		if op != nil && (op.Terminal || op.OpState == cluster.OpStateNatsRolledOut) {
			break
		}
		if time.Now().After(deadline) {
			state := "<missing>"
			if op != nil {
				state = op.OpState
			}
			ph, _ := readPhase(t, c.nodes[0], c.ids[2])
			t.Fatalf("retire op %s stalled within 30s (op=%q, roster phase=%q)", opID, state, ph)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if op.OpState == cluster.OpStateRetireFailed {
		t.Fatalf("retire op %s ended in RETIRE_FAILED: %s", opID, op.LastError)
	}

	// The roster row must be gone (the §8.1 order's last durable step).
	if ph, ok := readPhase(t, c.nodes[0], c.ids[2]); ok {
		t.Errorf("retired follower still in the roster (phase=%q) after the op reached %q",
			ph, op.OpState)
	}

	// Gone from raft configuration too, and in the right order: the roster delete
	// above is the LAST step, so by now RemoveServer must already have happened.
	voterDeadline := time.Now().Add(10 * time.Second)
	for {
		nv, verr := c.nodes[0].NumVoters()
		if verr == nil && nv == 2 {
			break
		}
		if time.Now().After(voterDeadline) {
			nv, _ := c.nodes[0].NumVoters()
			t.Fatalf("retired follower still a raft voter: NumVoters=%d, want 2", nv)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// No half-state: the op must not have blocked or errored on its way here.
	if op.LastError != "" {
		t.Errorf("retire op %s reached %q carrying last_error=%q; the membership change completed but "+
			"the operation did not run clean", opID, op.OpState, op.LastError)
	}

	// And the record survives for the operator: a completed removal whose op row
	// vanished leaves nothing to audit afterwards.
	final, ferr := cluster.OperationByID(c.nodes[0].RODB(), opID)
	if ferr != nil {
		t.Fatalf("OperationByID(%s): %v", opID, ferr)
	}
	if final == nil {
		t.Errorf("retire op %s vanished; its terminal state is the operator's only record that the "+
			"removal completed", opID)
	}
}

// testD7DrainLeaderTransfers (review B5): draining the LEADER transfers leadership
// off it FIRST and bails (ErrLeadershipTransferred) — it must NOT half-drain
// (no marker raised, phase unchanged on the old leader).
func testD7DrainLeaderTransfers(t *testing.T) {
	c := startD7Cluster(t, 3)
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		cu := func(idx int) func(uint64) (bool, error) {
			return func(b uint64) (bool, error) { cur, e := c.nodes[idx].AppliedIndex(); return cur >= b, e }
		}(i)
		if err := c.addNodeRetry(in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	// node 0 is the leader; drain it. Must bail with ErrLeadershipTransferred (no half-drain).
	err := c.admin.DrainNode(c.ids[0], false, true, time.Now(), nil)
	var lt *broker.ErrLeadershipTransferred
	if !errors.As(err, &lt) {
		t.Fatalf("draining the leader must transfer + bail (ErrLeadershipTransferred), got %v", err)
	}
	// No half-drain: the broker_draining marker for the old leader was NOT raised (the
	// bail happens BEFORE step 1). (The bootstrapped leader has no roster row to check,
	// so we assert the marker, which DrainNode would only set after the transfer gate.)
	var drainMarker string
	_ = c.nodes[0].BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, "draining:"+c.ids[0]).Scan(&drainMarker)
	})
	if drainMarker != "" {
		t.Fatalf("draining the leader raised broker_draining before bailing (half-drain): %q", drainMarker)
	}
}

func mustForgeAux(t *testing.T, nodeID, pub, nonce, sigHex string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]string{"node_id": nodeID, "ident_pub": pub, "nonce": nonce, "sig": sigHex})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
