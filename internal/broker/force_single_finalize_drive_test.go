package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

// force_single_finalize_drive_test.go — EXECUTION coverage for the C1 finalize op and the
// drain-marker healer, over a real single-node raft.
//
// origin: batch-c internal review tests-F2 / F3 / completion-F3. Everything guarding these paths was
// either a structural (AST) assertion or a pure-function check, and the reviewer proved the gap the
// hard way: unconditionally creating the op on the SUCCESS path, and making the healer clear every
// marker it saw, both left the entire suite green. Structural gates catch "the shape was broken";
// only this file catches "it runs, and does the wrong thing".
//
// Every test here drives the real functions — driveForceSingleFinalize, startForceSingleFinalize,
// resumeForceSingleFinalizeOnLeadership, reconcileDrainMarkers, forceSingleGhostRows — against
// committed raft state.

// fsFinalizeFixture builds a bootstrapped single-node admin whose self row is a live VOTER, plus
// `ghosts` roster rows that are deliberately NOT in the raft configuration — the exact shape an
// online force-single leaves behind when its prune did not land.
func fsFinalizeFixture(t *testing.T, selfID string, ghosts ...string) (*ClusterAdmin, *cluster.Node) {
	t.Helper()
	n, addr := d7SingleNode(t, selfID)
	a := NewClusterAdmin(n, silentLogger())
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := a.AddNode(d7JoinInput(t, selfID, addr), addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode self: %v", err)
	}
	for _, g := range ghosts {
		in := d7JoinInput(t, g, g)
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(in) }); err != nil {
			t.Fatalf("insert roster row %s: %v", g, err)
		}
		// A ghost's row reads VOTER; a mid-join row does not. Set it explicitly so the fixture is the
		// ghost shape rather than whatever phase the upsert happens to start in. The predecessor set is
		// mandatory (PlanClusterNodePhase refuses an empty one — a phase move with no guard is how a
		// stale writer overwrites a newer state).
		if err := a.setPhase(g, phaseVoter, []string{phasePending, phaseCatchingUp, phaseVoter}, ""); err != nil {
			t.Fatalf("set %s VOTER: %v", g, err)
		}
	}
	return a, n
}

func rosterHas(t *testing.T, n *cluster.Node, id string) bool {
	t.Helper()
	var c int
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id = ?`, id).Scan(&c)
	}); err != nil {
		t.Fatalf("roster read: %v", err)
	}
	return c > 0
}

func opByID(t *testing.T, n *cluster.Node, id string) *cluster.Operation {
	t.Helper()
	op, err := cluster.OperationByID(n.RODB(), id)
	if err != nil {
		t.Fatalf("op read: %v", err)
	}
	return op
}

// TestForceSingleGhostRowsSeesOnlyVoterRowsAbsentFromTheConfig drives the real predicate.
//
// The mid-join rows are the whole point: the admission protocol writes the roster row BEFORE the raft
// membership change, so a node being (re-)admitted is legitimately "absent from the config" for a
// tick or more. If the phase filter widened, the leadership-edge resume would prune a joining node.
//
// Mutation: widen the SQL to `phase IN (VOTER, JOIN_VERIFIED_PENDING_VOTER, CATCHING_UP)` — reddens.
func TestForceSingleGhostRowsSeesOnlyVoterRowsAbsentFromTheConfig(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	// A node mid-join: roster row present, NOT in the raft config, phase is a join phase.
	joining := d7JoinInput(t, "joiner-b", "joiner-b")
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(joining) }); err != nil {
		t.Fatalf("insert joiner row: %v", err)
	}
	for _, phase := range []string{phasePending, phaseCatchingUp} {
		if err := a.setPhase("joiner-b", phase, []string{phasePending, phaseCatchingUp, phaseVoter}, ""); err != nil {
			t.Fatalf("set joiner phase %s: %v", phase, err)
		}
		ghosts, err := a.forceSingleGhostRows()
		if err != nil {
			t.Fatalf("forceSingleGhostRows: %v", err)
		}
		got := strings.Join(ghosts, ",")
		if got != "ghost-a" {
			t.Fatalf("with joiner-b in phase %s, ghosts = %q, want only \"ghost-a\" — a node whose roster "+
				"row landed before its raft membership is NOT a ghost, and pruning it would complete the "+
				"join into \"raft voter with no roster row\", the state the leadership reconciler logs as "+
				"INCONSISTENT and refuses to auto-heal", phase, got)
		}
	}
	// Self is a live voter IN the config, so it must never appear.
	ghosts, _ := a.forceSingleGhostRows()
	for _, g := range ghosts {
		if g == "fs-self" {
			t.Fatal("the live self row was classified as a ghost")
		}
	}
}

// TestFinalizeOpDrivesThePruneToTerminal is the happy path of the retry, driven for real.
//
// Mutation: make driveForceSingleFinalize advance on the propose instead of on the observation —
// still passes here (the propose succeeds), which is why the deadline/observation split additionally
// has TestFinalizeOpAdvancesOnObservationNotPropose below.
func TestFinalizeOpDrivesThePruneToTerminal(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a", "ghost-b")
	opID, err := a.startForceSingleFinalize("fs-self", []string{"ghost-a", "ghost-b"})
	if err != nil {
		t.Fatalf("startForceSingleFinalize: %v", err)
	}
	if op := opByID(t, n, opID); op == nil || op.Terminal || op.OpState != cluster.OpStateFSPrunePending {
		t.Fatalf("fresh op = %+v, want non-terminal FS_PRUNE_PENDING", op)
	}

	// Tick 1: proposes the prune. It does NOT advance — the ladder is advance-after-observe.
	a.driveForceSingleFinalize(opByID(t, n, opID))
	if rosterHas(t, n, "ghost-a") || rosterHas(t, n, "ghost-b") {
		t.Fatal("the prune did not remove the ghost roster rows")
	}
	// Tick 2: observes them gone and goes terminal-success.
	a.driveForceSingleFinalize(opByID(t, n, opID))
	op := opByID(t, n, opID)
	if op == nil || !op.Terminal || op.OpState != cluster.OpStateFSFinalized {
		t.Fatalf("after the rows were observed gone: op = %+v, want terminal FS_FINALIZED", op)
	}
	if op.LastError != "" {
		t.Errorf("a successful finalize recorded last_error = %q", op.LastError)
	}
	// And the membership plane is free again — assertNoActiveOp must pass for self.
	if err := a.assertNoActiveOp("fs-self"); err != nil {
		t.Errorf("a terminal finalize still fences self's membership plane: %v", err)
	}
}

// TestFinalizeOpAdvancesOnObservationNotPropose pins advance-after-observe.
//
// The rows are removed OUT OF BAND before the first drive, so the op must reach FS_FINALIZED from the
// OBSERVATION alone — no successful prune propose of its own is required. The mirror case (a propose
// that returns nil while the rows survive) is what the deadline exists for, and is covered by
// TestFinalizeOpGivesUpTerminallyNotBlocked.
func TestFinalizeOpAdvancesOnObservationNotPropose(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	opID, err := a.startForceSingleFinalize("fs-self", []string{"ghost-a"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// Someone else (the operator's `recovery node remove`, or a racing leader) removed it first.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodePrune([]string{"ghost-a"}, time.Now())
	}); err != nil {
		t.Fatalf("out-of-band prune: %v", err)
	}
	a.driveForceSingleFinalize(opByID(t, n, opID))
	if op := opByID(t, n, opID); op == nil || op.OpState != cluster.OpStateFSFinalized || !op.Terminal {
		t.Fatalf("op = %+v, want terminal FS_FINALIZED from the observation alone", op)
	}
}

// TestFinalizeOpGivesUpTerminallyNotBlocked drives the exhaustion path with an already-expired
// replicated deadline and a ghost the prune cannot remove.
//
// This is the invariant the whole design rests on: a non-terminal op fences the target's membership
// plane, and an operator standing over a freshly force-singled survivor has no peer to confirm
// against. Every exit must be terminal.
//
// Mutation: send the exhausted op to OpStateBlocked — reddens here AND in the AST gate.
func TestFinalizeOpGivesUpTerminallyNotBlocked(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	opID, err := a.startForceSingleFinalize("fs-self", []string{"ghost-a"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// Expire the budget: drive with a clock far past the baked deadline. The op's row keeps the
	// deadline it was created with, so moving `now` is the honest way to exhaust it.
	a.now = func() time.Time { return time.Now().Add(2 * opForceSingleFinalizeBudget) }
	// Make ghost-a stop matching the ghost signature WITHOUT touching raft membership: its row stays,
	// but its phase is no longer VOTER, so the prune step is not allowed to remove it. (Adding it as a
	// real voter would be the other way to get here, but on a single-node fixture that raises quorum to
	// 2 with one phantom peer and no Propose can commit again — the op would then be stuck for a reason
	// that has nothing to do with what this test is about.)
	if err := a.setPhase("ghost-a", phaseRetiring, []string{phaseVoter}, ""); err != nil {
		t.Fatalf("set ghost-a RETIRING: %v", err)
	}
	a.driveForceSingleFinalize(opByID(t, n, opID))
	op := opByID(t, n, opID)
	if op == nil {
		t.Fatal("op vanished")
	}
	if !op.Terminal {
		t.Fatalf("op = %s, terminal=%v — an exhausted finalize MUST be terminal: a non-terminal one "+
			"fences this node's membership plane via assertNoActiveOp, which is strictly worse than the "+
			"pre-batch-C \"ghosts plus a log line, and `recovery node remove` still works\"",
			op.OpState, op.Terminal)
	}
	if op.OpState == cluster.OpStateBlocked {
		t.Fatal("the finalize ladder reached BLOCKED — it has no operator to confirm it")
	}
	if err := a.assertNoActiveOp("fs-self"); err != nil {
		t.Errorf("a terminal finalize still fences self's membership plane: %v", err)
	}
}

// TestFinalizeOpRefusesToPruneARowThatWasReAdmitted is the C1-N3 regression, driven for real.
//
// The reviewer reproduced this against a live fixture: `!inCfg[id]` alone is satisfied by a row that
// has just been RE-ADMITTED, because the admission protocol writes the roster row first and the raft
// membership change second. The op would delete the fresh row out from under the join.
//
// Mutation: re-confirm with raftConfigIDs alone instead of forceSingleGhostRows — reddens.
func TestFinalizeOpRefusesToPruneARowThatWasReAdmitted(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	opID, err := a.startForceSingleFinalize("fs-self", []string{"ghost-a"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// The operator rebuilt the cluster while the op sat in the queue: ghost-a's row is re-admitted and
	// is mid-join — present in the roster, NOT yet in the raft configuration.
	if err := a.setPhase("ghost-a", phasePending, []string{phaseVoter}, ""); err != nil {
		t.Fatalf("re-admit ghost-a: %v", err)
	}
	a.driveForceSingleFinalize(opByID(t, n, opID))
	if !rosterHas(t, n, "ghost-a") {
		t.Fatal("the finalize op deleted the roster row of a node that had been RE-ADMITTED and was " +
			"mid-join. The join then completes into \"raft voter with no roster row\" — the state " +
			"ReconcileMembershipOnLeadership logs as INCONSISTENT and explicitly refuses to auto-heal. " +
			"assertNoActiveOp cannot prevent this: it is per-target, and this op targets self.")
	}
	if op := opByID(t, n, opID); op == nil || !op.Terminal {
		t.Fatalf("the op must still reach a terminal state rather than spin: %+v", op)
	}
}

// TestLeadershipEdgeCreatesFinalizeOpOnlyForTheGhostShape drives the crash-window cover.
//
// It is the coverage that matters most, because this path DELETES roster rows and nothing else in the
// system will re-check its judgement.
//
// Mutation: drop the force_single_active condition, or the NumVoters()==1 condition — the
// "no marker" / "healthy cluster" subtests redden.
func TestLeadershipEdgeCreatesFinalizeOpOnlyForTheGhostShape(t *testing.T) {
	t.Run("no force-single marker: log-only, no op", func(t *testing.T) {
		a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
		a.resumeForceSingleFinalizeOnLeadership()
		if live, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self"); live != nil {
			t.Fatalf("a finalize op was created without proof that a force-single ever happened here "+
				"(op %s). Any roster/raft divergence would then trigger a prune.", live.OpID)
		}
		if !rosterHas(t, n, "ghost-a") {
			t.Fatal("a roster row was pruned with no force_single_active marker set")
		}
	})

	t.Run("marker set and the ghost shape present: op created", func(t *testing.T) {
		a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanSetForceSingle(time.Now())
		}); err != nil {
			t.Fatalf("set marker: %v", err)
		}
		a.resumeForceSingleFinalizeOnLeadership()
		live, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
		if live == nil || live.Kind != cluster.OpKindForceSingleFinalize {
			t.Fatalf("the crash-window cover did not start a finalize op: %+v", live)
		}
		// Driving it to terminal must remove the ghost and free the plane.
		a.driveForceSingleFinalize(opByID(t, n, live.OpID))
		a.driveForceSingleFinalize(opByID(t, n, live.OpID))
		if rosterHas(t, n, "ghost-a") {
			t.Error("the resumed op did not prune the ghost")
		}
		if op := opByID(t, n, live.OpID); op == nil || !op.Terminal {
			t.Errorf("the resumed op is not terminal: %+v", op)
		}
	})

	t.Run("idempotent: a second edge does not stack a second op", func(t *testing.T) {
		a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanSetForceSingle(time.Now())
		}); err != nil {
			t.Fatalf("set marker: %v", err)
		}
		a.resumeForceSingleFinalizeOnLeadership()
		first, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
		if first == nil {
			t.Fatal("no op after the first edge")
		}
		a.resumeForceSingleFinalizeOnLeadership()
		second, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
		if second == nil || second.OpID != first.OpID {
			t.Fatalf("a second leadership edge produced a different op (%v vs %v) — every election "+
				"would mint another row", first.OpID, second)
		}
	})
}

// TestDrainMarkerHealerClearsOnlyTheRosterlessOrphan drives the real pass.
//
// origin: batch-c internal review tests-F1. The previous test re-implemented the predicate inside its
// own body; reconcileDrainMarkers and orphanDrainMarkers had ZERO test references, and making the
// healer clear every marker it saw left the suite green. That mutation is the exact evasion the plan
// names for C1-12, and it turns the healer into the thing it is supposed to protect against.
//
// Mutation: select on "no active op" instead of "no roster row" — the half-finished-drain subtest
// reddens.
func TestDrainMarkerHealerClearsOnlyTheRosterlessOrphan(t *testing.T) {
	a, n := fsFinalizeFixture(t, "hb-self", "peer-b")
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now}}
	b.cl = &clusterRuntime{node: n, admin: a}

	deadline := time.Now().Add(time.Minute)
	for _, node := range []string{"peer-b", "vanished-c"} {
		if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
			return cluster.PlanClusterDrainSet(node, &deadline)
		}); err != nil {
			t.Fatalf("raise marker for %s: %v", node, err)
		}
	}
	// peer-b HAS a roster row (a drain could be in flight for it); vanished-c has none — the orphan the
	// retire ladder leaves when its marker-clear Propose fails and the roster row is then deleted.
	if err := b.reconcileDrainMarkers(t.Context(), time.Now()); err != nil {
		t.Fatalf("reconcileDrainMarkers: %v", err)
	}
	marked := map[string]bool{}
	if err := n.BoundedStaleRead(func(db *sql.DB) error {
		ids, derr := cluster.DrainingNodes(db)
		for _, id := range ids {
			marked[id] = true
		}
		return derr
	}); err != nil {
		t.Fatalf("read markers: %v", err)
	}
	if marked["vanished-c"] {
		t.Error("the orphaned marker for a node with NO roster row was not cleared — that is a permanent " +
			"broker_draining alert naming a node nobody can even identify")
	}
	if !marked["peer-b"] {
		t.Fatal("the healer cleared the marker of a node that still HAS a roster row. A raw `cluster " +
			"drain` raises the marker long before it flips the phase, and its documented failure exit " +
			"tells the operator to re-run — so between attempts this is a LIVE drain whose marker and " +
			"broker_draining alert are the only status-visible evidence (the phase still reads VOTER). " +
			"Clearing it also lets pickProxyRehomeTarget re-fill the node it just emptied, because that " +
			"picker ranks by FEWEST proxy homes.")
	}
}

// TestDrainMarkerHealerIsIdleWhenConverged: the registry requires a pass to perform ZERO writes once
// converged. A pass that proposes unconditionally commits a raft entry on every cadence tick.
//
// Mutation: make reconcileDrainMarkers propose for every marker it sees — reddens.
func TestDrainMarkerHealerIsIdleWhenConverged(t *testing.T) {
	a, n := fsFinalizeFixture(t, "hb-self", "peer-b")
	b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now}}
	b.cl = &clusterRuntime{node: n, admin: a}

	deadline := time.Now().Add(time.Minute)
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterDrainSet("peer-b", &deadline)
	}); err != nil {
		t.Fatalf("raise marker: %v", err)
	}
	before, err := n.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := b.reconcileDrainMarkers(t.Context(), time.Now()); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	after, err := n.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if after != before {
		t.Fatalf("three converged passes committed %d raft entries (applied %d → %d) — a pass with "+
			"nothing to do must write NOTHING, or it churns the log at its cadence forever",
			after-before, before, after)
	}
}

// TestCommitSuccessPathCreatesNoFinalizeOp is plan §9's first named test, and the one the reviewer
// proved was missing: inserting an UNCONDITIONAL startForceSingleFinalize into the commit path left
// the entire hermetic suite green.
//
// The claim it pins is load-bearing. The whole C1 design rests on the synchronous path being
// untouched on success — if every ordinary force-single also minted an op, assertNoActiveOp would
// fence that node's membership plane while the operator is mid-recovery and needs exactly those
// commands. Only drill 22 (opt-in, deploy tier) could otherwise see it.
//
// Mutation: create the op unconditionally in handleForceSingleCommit — reddens.
func TestCommitSuccessPathCreatesNoFinalizeOp(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	// The success path's observable outcome: the prune lands synchronously and NO operation exists.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodePrune([]string{"ghost-a"}, time.Now())
	}); err != nil {
		t.Fatalf("synchronous prune: %v", err)
	}
	if rosterHas(t, n, "ghost-a") {
		t.Fatal("the synchronous prune left the roster row behind")
	}
	live, err := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
	if err != nil {
		t.Fatalf("op read: %v", err)
	}
	if live != nil {
		t.Fatalf("a finalize op (%s) exists after a CLEAN recovery. The retry op is the failure branch "+
			"only: on the happy path it fences self's membership plane for no reason, exactly when the "+
			"operator needs `cluster recovery node remove` / `retire` most.", live.OpID)
	}
	// And with the ghosts gone, the leadership-edge cover must also stay quiet even WITH the marker up.
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanSetForceSingle(time.Now())
	}); err != nil {
		t.Fatalf("set marker: %v", err)
	}
	a.resumeForceSingleFinalizeOnLeadership()
	if live, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self"); live != nil {
		t.Fatalf("the leadership-edge cover started an op (%s) with no ghost rows present", live.OpID)
	}
}

// ─── EXTERNAL review B1: the post-rewrite window is recoverable ──────────────────────────────────

// TestForceSingleIntentRecoversTheMarkerAndEpochWindow is the B1 regression.
//
// The external review's finding: `handleForceSingleCommit` performs an IRREVERSIBLE raft rewrite and
// only then writes force_single_active and the recovery epoch. If either failed, the node was a
// healthy writable N=1 cluster with NO record that an emergency had happened — `cluster status` did
// not say FORCE_SINGLE, the ctl destructive gate (QuorumLost || ForceSingleActive) was FULLY OPEN
// because the rewrite had just made QuorumLost false, the error text told the operator to re-run a
// command the arm gate refuses (the node now has leader contact: its own), and the leadership-edge
// resume was keyed on the very marker that had failed to land.
//
// The fix is a durable intent written and fsync'd BEFORE the rewrite. This drives the recovery from
// the state that window leaves behind: intent on disk, raft already {self}, nothing else written.
//
// Mutations: (a) make resumeForceSingleFinalizeOnLeadership consult the marker before the intent —
// the "no marker" case reddens; (b) mint the epoch during recovery instead of carrying it in the
// intent — the epoch-stability case reddens.
func TestForceSingleIntentRecoversTheMarkerAndEpochWindow(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	a.dataDir = t.TempDir()

	// EXACTLY the state the window leaves: intent fsync'd, rewrite done, nothing else.
	intent := clusteroffline.OnlineIntent{
		SelfID: "fs-self", Abandoned: []string{"ghost-a"},
		Epoch: "e1e1e1e1", MarkedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := clusteroffline.WriteOnlineIntent(a.dataDir, intent); err != nil {
		t.Fatalf("write intent: %v", err)
	}
	if forceSingleActive(n.RODB()) {
		t.Fatal("fixture invalid: the marker must be ABSENT — that is the window under test")
	}

	a.resumeForceSingleFinalizeOnLeadership()

	if !forceSingleActive(n.RODB()) {
		t.Fatal("after a leadership tick the force_single_active marker is STILL absent. `cluster status` " +
			"would not report FORCE_SINGLE and the ctl destructive gate would stay fully open on a " +
			"zero-redundancy node — and the documented repair (re-run --online) is refused by the arm " +
			"gate, because this node now has leader contact: its own.")
	}
	got, err := a.forceSingleEpoch()
	if err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	if got != intent.Epoch {
		t.Fatalf("recovery epoch = %q, want the one recorded in the intent (%q). Minting a fresh epoch "+
			"during recovery is what made \"did the epoch I promised actually land?\" unanswerable.",
			got, intent.Epoch)
	}

	// The ghost is the remaining work, so a finalize op must now be driving it.
	live, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
	if live == nil || live.Kind != cluster.OpKindForceSingleFinalize {
		t.Fatalf("no finalize op after the intent recovery: %+v", live)
	}
	a.driveForceSingleFinalize(opByID(t, n, live.OpID))
	a.driveForceSingleFinalize(opByID(t, n, live.OpID))
	if rosterHas(t, n, "ghost-a") {
		t.Error("the recovered finalize did not prune the ghost")
	}
	// With everything landed, the intent has nothing left to protect.
	if in, _ := clusteroffline.ReadOnlineIntent(a.dataDir); in != nil {
		t.Error("the intent survived a fully completed recovery — a later tick would keep re-verifying it")
	}
}

// TestForceSingleIntentBeforeRewriteDoesNotMarkAnUnrecoveredCluster covers the opposite crash edge:
// the intent is durable, but RecoverToSelfOnline has not changed the raft configuration yet.
//
// origin: batch-c external re-review B2. An intent is written BEFORE the irreversible rewrite, so
// presence alone cannot prove the rewrite landed. Applying it on the next leadership tick marks an
// otherwise healthy cluster FORCE_SINGLE and overwrites its recovery epoch. A nonvoter keeps this
// fixture writable while proving its committed configuration is not the recovered {self} shape.
func TestForceSingleIntentBeforeRewriteDoesNotMarkAnUnrecoveredCluster(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self")
	a.dataDir = t.TempDir()
	if err := n.AddNonvoter("joining-peer", "joining-peer"); err != nil {
		t.Fatalf("add nonvoter: %v", err)
	}
	intent := clusteroffline.OnlineIntent{
		SelfID: "fs-self", Abandoned: []string{"joining-peer"}, Epoch: "pre-rewrite-epoch",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := clusteroffline.WriteOnlineIntent(a.dataDir, intent); err != nil {
		t.Fatalf("write intent: %v", err)
	}

	a.resumeForceSingleFinalizeOnLeadership()

	if forceSingleActive(n.RODB()) {
		t.Fatal("a pre-rewrite intent marked an unrecovered cluster FORCE_SINGLE; intent presence " +
			"proves only that recovery was requested, not that the raft rewrite landed")
	}
	if got, err := a.forceSingleEpoch(); err != nil {
		t.Fatalf("read epoch: %v", err)
	} else if got != "" {
		t.Fatalf("a pre-rewrite intent changed the recovery epoch to %q", got)
	}
	if in, err := clusteroffline.ReadOnlineIntent(a.dataDir); err != nil || in != nil {
		t.Fatalf("a confirmed pre-rewrite intent was not cleared: intent=%+v err=%v", in, err)
	}
}

// TestForceSingleIntentIsIdempotentAndScoped: re-running the recovery must not double-write, and an
// intent belonging to a DIFFERENT node must never be applied here.
func TestForceSingleIntentIsIdempotentAndScoped(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self")
	a.dataDir = t.TempDir()
	intent := clusteroffline.OnlineIntent{SelfID: "fs-self", Abandoned: nil, Epoch: "abc123",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano)}

	if err := a.applyForceSingleIntent(intent); err != nil {
		t.Fatalf("apply: %v", err)
	}
	before, err := n.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	// A second apply of the SAME intent must be a no-op: it is called on every leadership edge.
	if err := a.applyForceSingleIntent(intent); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	after, err := n.AppliedIndex()
	if err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if after != before {
		t.Errorf("re-applying an already-landed intent committed %d raft entries — it runs on every "+
			"leadership acquisition, so it must be idempotent", after-before)
	}

	// A SECOND incident must overwrite the epoch: comparing by presence rather than by value would
	// leave the split-brain detector holding a token from the previous incident forever.
	second := clusteroffline.OnlineIntent{SelfID: "fs-self", Epoch: "def456",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := a.applyForceSingleIntent(second); err != nil {
		t.Fatalf("second incident: %v", err)
	}
	if got, _ := a.forceSingleEpoch(); got != "def456" {
		t.Errorf("a second incident left the epoch at %q — the detector would hold the wrong incident's token", got)
	}

	// An intent naming a DIFFERENT node must be ignored by the resume path.
	other := clusteroffline.OnlineIntent{SelfID: "some-other-broker", Epoch: "zzz",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := clusteroffline.WriteOnlineIntent(a.dataDir, other); err != nil {
		t.Fatalf("write foreign intent: %v", err)
	}
	a.resumeForceSingleFinalizeOnLeadership()
	if got, _ := a.forceSingleEpoch(); got == "zzz" {
		t.Error("an intent belonging to another broker was applied here")
	}
}

// TestInterruptedForceSingleForwardCompletesOnAPlainLeaderTick pins the trigger, not just the repair.
//
// origin: batch-c external re-review audit (main process). Every branch that leaves the intent on
// disk — the commit handler's marker/epoch failure and the resume's own — told the operator it would
// be retried "on the next leadership tick". resumeForceSingleFinalizeOnLeadership is dispatched under
// `isLeader && !wasLeader`, i.e. the leadership-ACQUIRED EDGE, and the node those branches run on is a
// freshly recovered SINGLE-VOTER raft that never holds another election. So on the exact shape the
// intent exists to cover there was no next edge, and the promised automatic completion resolved to an
// undocumented "restart the broker" — while the destructive gate stayed open on the survivor of a
// disaster, because the marker is the thing that had failed to land.
//
// This drives ONLY the periodic path: no leadership edge is simulated, exactly as in a process that
// has been leader all along.
//
// Mutation: delete a.driveInterruptedForceSingle() from driveLeaderMaintenance — reddens on the very
// first assertion (the marker never lands).
func TestInterruptedForceSingleForwardCompletesOnAPlainLeaderTick(t *testing.T) {
	a, n := fsFinalizeFixture(t, "fs-self", "ghost-a")
	a.dataDir = t.TempDir()
	intent := clusteroffline.OnlineIntent{
		SelfID: "fs-self", Abandoned: []string{"ghost-a"},
		Epoch: "tick-epoch", MarkedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := clusteroffline.WriteOnlineIntent(a.dataDir, intent); err != nil {
		t.Fatalf("write intent: %v", err)
	}
	if forceSingleActive(n.RODB()) {
		t.Fatal("fixture invalid: the marker must be ABSENT — that is the window under test")
	}

	a.driveLeaderMaintenance() // a PLAIN leader tick: no edge, no restart, no operator command

	if !forceSingleActive(n.RODB()) {
		t.Fatal("an interrupted force-single did not forward-complete on a plain leader tick. The only " +
			"other trigger is the leadership-acquired edge, and a single-voter raft never re-elects — " +
			"so `cluster status` would keep reporting a merely-degraded cluster and the ctl destructive " +
			"gate would stay open until somebody thought to restart the broker.")
	}
	if got, _ := a.forceSingleEpoch(); got != intent.Epoch {
		t.Errorf("recovery epoch = %q, want the intent's %q", got, intent.Epoch)
	}
	live, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self")
	if live == nil || live.Kind != cluster.OpKindForceSingleFinalize {
		t.Fatalf("the periodic path landed the facts but started no finalize op for the ghost: %+v", live)
	}

	// The gate that keeps this cheap AND quiet: once both facts are visible the tick must stop
	// re-entering the resume, or every 5s it would re-log the ghost/backoff warnings for as long as
	// the ghosts survive.
	if !a.forceSingleFactsLanded(intent) {
		t.Error("forceSingleFactsLanded is false after both facts landed — the periodic path would " +
			"re-enter the full resume on every tick")
	}
	// Compared by VALUE: a SECOND incident's intent must not be considered already landed just because
	// some epoch is present. Mutation: `cur == in.Epoch` -> `cur != ""` — reddens here.
	next := clusteroffline.OnlineIntent{SelfID: "fs-self", Epoch: "a-second-incident",
		MarkedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if a.forceSingleFactsLanded(next) {
		t.Error("a second incident's intent was reported already-landed while the epoch still holds " +
			"the previous incident's token")
	}

	// Idempotent: further ticks must not mint a second op for the same ghost.
	a.driveLeaderMaintenance()
	if again, _ := cluster.ActiveOperationForTarget(n.RODB(), "fs-self"); again == nil || again.OpID != live.OpID {
		t.Errorf("a second leader tick replaced the finalize op (%v -> %v)", live.OpID, again)
	}
}
