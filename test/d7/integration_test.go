//go:build d7_integration

package d7_test

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	admin   *broker.ClusterAdmin // leader's orchestrator (node 0)
}

// startD7Cluster bootstraps node 0 as a single voter and brings up n-1 EMPTY-state
// nodes connected over inmem transports, ready for dynamic AddVoter.
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
			HeartbeatTimeout: 60 * time.Millisecond, ElectionTimeout: 60 * time.Millisecond, LeaderLeaseTimeout: 30 * time.Millisecond,
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
	if err := c.nodes[0].WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("node 0 leadership: %v", err)
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
	return cluster.ClusterNodeUpsertInput{
		NodeID: c.ids[i], Name: c.ids[i], NodeIdentPub: pub, NatsServerID: "tether-" + c.ids[i],
		RaftAddr: c.ids[i], NatsRoute: "nats://x", TunnelAddr: "x:7000", PublicHost: "h",
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
		NodeID: "evil", Name: "evil", NodeIdentPub: victimPub, RaftAddr: "x", PublicHost: "h", CertFP: "sha256:ab",
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
// membership change is idempotent. Only fires under full make e2e -race contention.
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
// orchestrator phase Propose racing an election under full make e2e -race contention). The
// two-phase membership change is idempotent, so a re-run converges. Used by every SEED site;
// the ghost/half-state drills that ASSERT an error call AddNode directly.
func (c *d7Cluster) addNodeRetry(in cluster.ClusterNodeUpsertInput, raftAddr string, caughtUp func(uint64) (bool, error), maxWait time.Duration) error {
	var err error
	for attempt := 0; attempt < 6; attempt++ {
		err = c.admin.AddNode(in, raftAddr, caughtUp, maxWait)
		if err == nil || !d7TransientAddErr(err) {
			return err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return err
}

// testD7ForceSingleRecover: kill a 3-node cluster, force-single a survivor (peers
// dead), restart it as a writable N=1, and prove the restart does not double-apply.
func testD7ForceSingleRecover(t *testing.T) {
	c := startD7Cluster(t, 3)
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
	if err := c.nodes[survivorIdx].ApplyMetaSet("t:premark", "preval"); err != nil {
		t.Fatalf("seed premark: %v", err)
	}
	appliedBefore, _ := c.nodes[survivorIdx].AppliedIndex()
	// The bootstrapped survivor has no roster row of its own (rows come from AddNode);
	// the roster holds the two added peers. Record it and assert it is unchanged after
	// recovery (no double-apply), without hardcoding the count.
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
		HeartbeatTimeout: 60 * time.Millisecond, ElectionTimeout: 60 * time.Millisecond, LeaderLeaseTimeout: 30 * time.Millisecond,
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
	// State preserved EXACTLY (review M7): the seeded marker has its exact value (not
	// lost, not corrupted) and the roster has EXACTLY its pre-force-single rows (not
	// doubled by a replay double-apply).
	var marker string
	_ = nd.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key='t:premark'`).Scan(&marker)
	})
	if marker != "preval" {
		t.Fatalf("committed state lost/corrupted across force-single: t:premark = %q, want preval", marker)
	}
	if got := countRows(t, nd, "cluster_nodes"); got != rosterBefore {
		t.Fatalf("roster row count changed across force-single: %d, want %d (double-apply?)", got, rosterBefore)
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

// testD7DrainRetireFollower (review M4): drain+retire a FOLLOWER end-to-end on a real
// 3-node cluster — it must leave the raft config AND the roster (the §8.1 removal
// order), with no half-state.
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
	// Retire a FOLLOWER (node 2; node 0 is leader). F after = N=2 (retire 3->2), F==0,
	// so confirmed=true is required. No exposes homed here -> migrateExposes no-ops.
	if err := c.admin.DrainNode(c.ids[2], true, true, time.Now(), func() (bool, error) { return true, nil }); err != nil {
		t.Fatalf("drain --retire follower: %v", err)
	}
	// Gone from the roster (RETIRING -> ClusterNodeRemove) and from raft config.
	if _, ok := readPhase(t, c.nodes[0], c.ids[2]); ok {
		t.Fatal("retired follower still in the roster")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if nv, _ := c.nodes[0].NumVoters(); nv == 2 {
			break
		}
		if time.Now().After(deadline) {
			nv, _ := c.nodes[0].NumVoters()
			t.Fatalf("retired follower still a raft voter: NumVoters=%d, want 2", nv)
		}
		time.Sleep(20 * time.Millisecond)
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
