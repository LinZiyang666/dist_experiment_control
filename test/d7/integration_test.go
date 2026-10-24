//go:build d7_integration

package d7_test

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
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
	// Only node 0 is a voter during bootstrap. Once membership changes, every
	// leader-dependent segment must select and recheck its own acting node.
	if err := c.nodes[0].WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("bootstrap leadership: %v", err)
	}
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
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	t.Run("DrainRefusalRetriesAfterLeadershipMoves", func(t *testing.T) {
		beforeG := runtime.NumGoroutine()
		beforeFD := testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseD7LeadershipMove(t, round)
		}
		testharness.AssertNoGoroutineLeak(t, "leadership move and retry", beforeG)
		testharness.AssertNoFDLeak(t, "leadership move and retry", beforeFD, 0)
	})
	t.Run("DrainRefusesRebuildOff", testD7DrainRefusesRebuildOff)     // external review F3
	t.Run("FollowerStatusViewSource", testD7FollowerStatusViewSource) // B1 review F3
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
	if err := c.addNodeRetry(t, in, c.ids[1], caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	// Choose the follower from this attempt's leader, and return assertions so
	// WithLeader can discard them if the premise changes during StatusReport.
	_, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		follower := 1 - leader
		admin := broker.NewClusterAdmin(c.nodes[follower], nil)
		deadline := time.Now().Add(3 * time.Second)
		for {
			rep, err := admin.StatusReport("ctl-nats")
			if err == nil && rep.LeaderID != "" {
				if rep.ViewHost != c.ids[follower] {
					return fmt.Errorf("ViewHost = %q, want follower %q", rep.ViewHost, c.ids[follower])
				}
				if rep.IsLeaderView {
					return fmt.Errorf("a follower must report IsLeaderView=false (leader=%q self=%q)", rep.LeaderID, c.ids[follower])
				}
				if rep.LeaderID == c.ids[leader] {
					return nil
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("follower StatusReport never populated leader %q: err=%v", c.ids[leader], err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	if err != nil {
		t.Fatal(err)
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
		if err := c.addNodeRetry(t, in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	if _, err := c.drainRefusesRebuildOff(t, nil); err != nil {
		t.Fatal(err)
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
	if err := c.addNodeRetry(t, in, c.ids[1], caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	// Verify replication on the actual follower, even if node 1 won leadership.
	if _, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		follower := 1 - leader
		deadline := time.Now().Add(3 * time.Second)
		for {
			p, ok := readPhase(t, c.nodes[follower], c.ids[1])
			if ok && p == "VOTER" {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("follower never saw replicated VOTER row (phase=%q ok=%v)", p, ok)
			}
			time.Sleep(20 * time.Millisecond)
		}
		nv, err := c.nodes[leader].NumVoters()
		if err != nil {
			return err
		}
		if nv != 2 {
			return fmt.Errorf("NumVoters = %d, want 2", nv)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	if err := c.addNodeRetry(t, in1, c.ids[1], caughtUp, 5*time.Second); err != nil {
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

	// Commit the forged entry through the observed leader's raft (bypassing
	// pre-verify), then prove a different replica has applied a later legit op.
	// The apply barrier replaces the old sleep: absence before replication is vacuous.
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	_, err = clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		if err := c.nodes[leader].Apply(cmd); err != nil {
			return fmt.Errorf("apply forged: %w", err)
		}
		if err := c.nodes[leader].ApplyMetaSet("t:liveness", "ok"); err != nil {
			return fmt.Errorf("post-forgery legit op failed (follower wedged?): %w", err)
		}
		follower := 1 - leader
		if err := c.waitMetaApplied(follower, "t:liveness", "ok", 3*time.Second); err != nil {
			return err
		}
		if _, ok := readPhase(t, c.nodes[follower], "evil"); ok {
			return fmt.Errorf("forged sig wrote a roster row on the FOLLOWER — must write NONE")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
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
	if _, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		if err := broker.NewClusterAdmin(c.nodes[leader], nil).AddNode(in, "ghost", caughtUp, 200*time.Millisecond); err == nil {
			return fmt.Errorf("AddNode that never catches up must return an error, not a false ok")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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

// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
// addNodeRetry repeats the idempotent membership change on the current leader.
// A stable not-leader/transfer response is also retried: raft can report the
// transfer-in-progress error before its IsLeader bit changes.
func (c *d7Cluster) addNodeRetry(t *testing.T, in cluster.ClusterNodeUpsertInput, raftAddr string, caughtUp func(uint64) (bool, error), maxWait time.Duration) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := clusterharness.WithLeader(t, c.leaderProbes(), time.Until(deadline), func(leader int) error {
			return broker.NewClusterAdmin(c.nodes[leader], nil).AddNode(in, raftAddr, caughtUp, maxWait)
		})
		if !d7TransientAddErr(err) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// applyMetaSetOnLeaderAndWaitApplied commits through the current leader, then
// observes the survivor's OWN disk. A commit elsewhere cannot prove recovery.
// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
func (c *d7Cluster) applyMetaSetOnLeaderAndWaitApplied(t *testing.T, survivorIdx int, key, value string, d time.Duration) error {
	deadline := time.Now().Add(d)
	_, err := clusterharness.WithLeader(t, c.leaderProbes(), d, func(leader int) error {
		return c.nodes[leader].ApplyMetaSet(key, value)
	})
	if err != nil {
		return err
	}
	return c.waitMetaApplied(survivorIdx, key, value, time.Until(deadline))
}

// testD7ForceSingleRecover: kill a 3-node cluster, force-single a survivor (peers
// dead), restart it as a writable N=1, and prove the restart does not double-apply.
func testD7ForceSingleRecover(t *testing.T) {
	c := startD7Cluster(t, 3)
	// Seed the bootstrap survivor's OWN roster row. In production every cluster node has a
	// cluster_nodes row (D9 `cluster init --from-existing` seeds the self VOTER row); force-single
	// refuses to rewrite the raft config for a self-id absent from cluster_nodes (readRoster). The
	// raft-only bootstrap here doesn't lay that row, so seed it via the normal committed upsert.
	if _, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		return c.nodes[leader].Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterNodeUpsert(c.joinInput(t, 0))
		})
	}); err != nil {
		t.Fatalf("seed self roster row: %v", err)
	}
	// Add the other two so the survivor's roster lists them (force-single must abandon them).
	for i := 1; i < 3; i++ {
		in := c.joinInput(t, i)
		cu := func(idx int) func(uint64) (bool, error) {
			return func(barrier uint64) (bool, error) { cur, e := c.nodes[idx].AppliedIndex(); return cur >= barrier, e }
		}(i)
		if err := c.addNodeRetry(t, in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	survivorIdx := 0
	// Seed a UNIQUE committed marker + record the exact roster count, so the restart
	// can prove state was preserved EXACTLY (not lost, not doubled) — review M7: the
	// old test only checked applied_index monotonicity (vacuous). The no-double-apply
	// of the replay itself is the D1 idempotent-skip property RecoverCluster reuses.
	if err := c.applyMetaSetOnLeaderAndWaitApplied(t, survivorIdx, "t:premark", "preval", 10*time.Second); err != nil {
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
	// Phase-1 commit (roster=PENDING), then AddVoter without the phase bump.
	// Each write is idempotent, but must use the current leader after membership changes.
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	if _, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		return c.nodes[leader].Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) })
	}); err != nil {
		t.Fatalf("phase-1: %v", err)
	}
	if _, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		return c.nodes[leader].AddVoter(c.ids[1], c.ids[1])
	}); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	reconciled := false
	_, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		if err := c.nodes[leader].Barrier(5 * time.Second); err != nil {
			return err
		}
		// Keep the initial PENDING premise strict. A completed pass whose
		// leader then moves is a durable side effect, so a retry skips only setup.
		if !reconciled {
			// AddVoter can transfer leadership before the joiner's SQLite row
			// becomes visible. Wait for that row, still requiring EXACTLY PENDING.
			deadline := time.Now().Add(3 * time.Second)
			for {
				p, found := readPhase(t, c.nodes[leader], c.ids[1])
				if found && p == "JOIN_VERIFIED_PENDING_VOTER" {
					break
				}
				if found || time.Now().After(deadline) {
					return fmt.Errorf("setup: want PENDING, got %q", p)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		if err := broker.NewClusterAdmin(c.nodes[leader], nil).ReconcileMembershipOnLeadership(); err != nil {
			return err
		}
		reconciled = true
		if p, _ := readPhase(t, c.nodes[leader], c.ids[1]); p != "CATCHING_UP" {
			return fmt.Errorf("reconcile did not forward-complete PENDING->CATCHING_UP, got %q", p)
		}
		nv, err := c.nodes[leader].NumVoters()
		if err != nil {
			return err
		}
		if nv != 2 {
			return fmt.Errorf("reconcile RemoveServer'd a mid-promote node: NumVoters=%d, want 2", nv)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
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
		if err := c.addNodeRetry(t, in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}

	// Half 1: the removed synchronous route must stay refused, and the refusal
	// must name the supported verb (a bare "no" gets worked around).
	if err := broker.NewClusterAdmin(c.nodes[0], nil).DrainNode(c.ids[2], true, true, time.Now(), func() (bool, error) { return true, nil }); err == nil {
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
	var opID string
	_, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		// A successful create survives movement; never create a second op.
		if opID != "" {
			return nil
		}
		if leader == 2 {
			if err := broker.NewClusterAdmin(c.nodes[leader], nil).TransferLeaderTo(c.ids[0]); err != nil {
				return err
			}
			return fmt.Errorf("retire-follower premise moved leadership off node 2; retry on the new leader")
		}
		var err error
		opID, err = broker.NewClusterAdmin(c.nodes[leader], nil).StartRetireOperation(c.ids[2], true)
		return err
	})
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
		// Follow the leader on every tick, including the controller's own
		// LEADER_TRANSFERRED step. Reclaiming node 0 fought raft under load.
		// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
		_, oerr := clusterharness.WithLeader(t, c.leaderProbes(), time.Until(deadline), func(leader int) error {
			admin := broker.NewClusterAdmin(c.nodes[leader], nil)
			admin.SetStreamsReadyProbe(func(string) (bool, error) { return true, nil })
			admin.DriveOperationsForTest()
			if err := c.nodes[leader].Barrier(5 * time.Second); err != nil {
				return err
			}
			var err error
			op, err = cluster.OperationByID(c.nodes[leader].RODB(), opID)
			return err
		})
		if oerr != nil {
			t.Fatalf("drive retire op %s: %v", opID, oerr)
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

	// The roster deletion is the last durable membership step. Check both
	// roster and raft on the leader that has applied it, not a stale replica.
	_, err = clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		if err := c.nodes[leader].Barrier(5 * time.Second); err != nil {
			return err
		}
		if ph, ok := readPhase(t, c.nodes[leader], c.ids[2]); ok {
			return fmt.Errorf("retired follower still in the roster (phase=%q) after the op reached %q", ph, op.OpState)
		}
		nv, err := c.nodes[leader].NumVoters()
		if err != nil {
			return err
		}
		if nv != 2 {
			return fmt.Errorf("retired follower still a raft voter: NumVoters=%d, want 2", nv)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// No half-state: the op must not have blocked or errored on its way here.
	if op.LastError != "" {
		t.Errorf("retire op %s reached %q carrying last_error=%q; the membership change completed but "+
			"the operation did not run clean", opID, op.OpState, op.LastError)
	}

	// And the record survives for the operator: a completed removal whose op row
	// vanished leaves nothing to audit afterwards.
	var final *cluster.Operation
	_, ferr := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		var err error
		final, err = cluster.OperationByID(c.nodes[leader].RODB(), opID)
		return err
	})
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
		if err := c.addNodeRetry(t, in, c.ids[i], cu, 5*time.Second); err != nil {
			t.Fatalf("seed AddNode %d: %v", i, err)
		}
	}
	// origin: docs/reviews/parallel-flake-rootcause.md root cause 2
	// A successful drain intentionally moves leadership. Remember that receipt
	// across WithLeader's re-observation rather than draining every new leader.
	oldLeader := -1
	_, err := clusterharness.WithLeader(t, c.leaderProbes(), 10*time.Second, func(leader int) error {
		if oldLeader >= 0 {
			return nil
		}
		err := broker.NewClusterAdmin(c.nodes[leader], nil).DrainNode(c.ids[leader], false, true, time.Now(), nil)
		var lt *broker.ErrLeadershipTransferred
		if !errors.As(err, &lt) {
			return fmt.Errorf("draining the leader must transfer + bail (ErrLeadershipTransferred), got %v", err)
		}
		if lt.NodeID != c.ids[leader] {
			return fmt.Errorf("transfer receipt node=%q, want %q", lt.NodeID, c.ids[leader])
		}
		oldLeader = leader
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The bail happens before step 1; query the actual old leader's marker.
	var drainMarker string
	err = c.nodes[oldLeader].BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, "draining:"+c.ids[oldLeader]).Scan(&drainMarker)
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read drain marker: %v", err)
	}
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
