package broker

import (
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
)

// retire_step_bounds_test.go — every step failure inside driveRetire has to END somewhere.
//
// origin: prerelease audit broker-cluster-ops/L-BCO-F1 (+ verifier correction 3, which
// found six sibling sites the finding had not listed).
//
// recordOpError is non-terminal FOREVER and does not touch the attempt counter. Nine
// sites in driveRetire called it and returned, so any of them could hold the op — and
// therefore the whole membership plane, since assertNoActiveOp fences every other
// cluster operation behind the in-flight one — with nothing but a last_error line to
// show for it. driveJoin's AddVoter had been bounded for exactly this reason; its
// mirror image here, RemoveServer, had not, and RemoveServer is the irreversible one.

// bounded builds an admin plus an op parked in a chosen state, with a controllable clock.
func retireBoundsAdmin(t *testing.T, opID, state string, now *time.Time) (*ClusterAdmin, *cluster.Operation) {
	t.Helper()
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	admin.now = func() time.Time { return *now }

	in := cluster.OpStartInput{
		OpID: opID, Kind: cluster.OpKindRetire, TargetNode: "ctl-2",
		InitState: state, Confirmed: true,
		Timeline: `[{"s":"` + state + `"}]`,
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, *now)
	}); err != nil {
		t.Fatalf("seed %s: %v", opID, err)
	}
	op, err := cluster.OperationByID(n.RODB(), opID)
	if err != nil || op == nil {
		t.Fatalf("read back seeded op: %v", err)
	}
	return admin, op
}

// TestARetriedRetireStepEndsInBlocked pins the bound itself: the same failure repeated
// opMaxAttempts times must transition the op to BLOCKED, which is a NONZERO terminal
// that `cluster retire --wait` reports and `cluster ops confirm/abort` can act on.
func TestARetriedRetireStepEndsInBlocked(t *testing.T) {
	now := time.Now().UTC()
	admin, op := retireBoundsAdmin(t, "op-bounded", cluster.OpStateNoNewHome, &now)

	transient := errors.New("raft: dial tcp: connection refused")
	for i := 0; i < opMaxAttempts; i++ {
		if st, _, _ := opState(t, admin, op.OpID); st == cluster.OpStateBlocked {
			t.Fatalf("op blocked after %d attempt(s) — the bound must be opMaxAttempts, not fewer: "+
				"a genuinely transient failure deserves its retries", i)
		}
		retireStepFailed(admin, op, "raft RemoveServer", transient)
	}
	st, _, lastErr := opState(t, admin, op.OpID)
	if st != cluster.OpStateBlocked {
		t.Fatalf("after %d identical failures the op is state=%s.\n\n"+
			"An unbounded hold fences the entire membership plane (assertNoActiveOp) behind an "+
			"operation that will never finish, and the only evidence is a last_error field nobody "+
			"is watching. BLOCKED is loud and operator-recoverable.", opMaxAttempts, st)
	}
	if lastErr == "" {
		t.Error("BLOCKED with no last_error: the operator is told to fix something without being told what")
	}
}

// TestADeterministicRetireStepBlocksImmediately is the other half. Some failures are
// terminal states, not transient ones: a rebuild-OFF expose row does not disappear
// because we asked again, and a second eligible VOTER does not appear because we
// waited. Re-issuing a side effect whose outcome cannot change is noise in the op's
// last_error and delay in front of a human — the controller ticks every 5s, so the cost
// is seconds rather than the "fifty minutes" this comment once claimed (round 2, G-4).
func TestADeterministicRetireStepBlocksImmediately(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"rebuild-OFF exposes", &ErrRebuildOffExposes{NodeID: "ctl-2", Ports: []int{14001}}},
		{"no eligible rehome target", ErrNoMigrationTarget},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			admin, op := retireBoundsAdmin(t, "op-det-"+tc.name[:4], cluster.OpStateRehomeExposes, &now)

			retireStepFailed(admin, op, "migrate exposes", tc.err)

			st, _, lastErr := opState(t, admin, op.OpID)
			if st != cluster.OpStateBlocked {
				t.Fatalf("a deterministic failure left the op state=%s after ONE attempt.\n\n"+
					"Retrying %v cannot change its outcome. Every retry is another controller tick "+
					"before the operator is shown a problem only they can fix.", st, tc.err)
			}
			if lastErr == "" {
				t.Error("blocked without recording why")
			}
		})
	}
}

// TestEveryRetireStepFailureIsRouted is the coverage assertion, and it is the one that
// would have caught this class in the first place. The finding named three sites; the
// verifier found six more. A per-site test would have grown one case at a time and
// missed the same way, so this counts the sites in the source instead.
func TestEveryRetireStepFailureIsRouted(t *testing.T) {
	src := readSourceForRetireScan(t)
	// recordOpError inside driveRetire is the shape that holds forever. The convergence
	// hold is the ONE legitimate one: it has its own crash-safe replicated deadline
	// (boundRehomeConvergence) and is only reached after that deadline was stamped.
	body := functionBody(t, src, "driveRetire")

	// The ALLOW-LIST, with the reason each one earns its place. Anything else that calls
	// recordOpError inside driveRetire is a step failure that can hold forever, and this
	// gate exists to make adding one a decision rather than an accident.
	//
	// A WAIT IS NOT A FAILURE, and the distinction is the whole of this list. The three
	// below are conditions that can still become true — the data plane can converge,
	// replication can catch up — so re-driving them is the correct behaviour and an
	// attempt counter would turn a healthy slow cluster into a blocked one. The nine
	// sites that were routed to retireStepFailed are failures of a step that was
	// ATTEMPTED: a Propose that was rejected, a RemoveServer that errored.
	//
	// STATED RESIDUAL, so a reader does not have to discover it: the two stream-readiness
	// waits have no deadline of their own, unlike the convergence hold. They can therefore
	// hold the membership plane for as long as a stream stays below its post-removal
	// target. That is deliberate — after #15 the condition means retiring really would
	// drop the cluster below its replica floor, and holding is the safe answer — and the
	// operator's exit is `cluster ops abort`, the same one BLOCKED offers. Giving them a
	// deadline is a separate change: CatchupDeadline is already re-purposed by
	// boundRehomeConvergence for an EARLIER state of the same op, so a second user of that
	// field would inherit a deadline stamped for a different wait.
	allowed := map[string]string{
		"a.recordOpError(op, holdErr)": "data-plane convergence — carries boundRehomeConvergence's own replicated deadline",
		`a.recordOpError(op, errors.New("streams not at target replica count yet"))`:                 "replication catch-up; a condition that can still become true",
		`a.recordOpError(op, errors.New("streams regressed below target before removal — holding"))`: "same condition, re-checked immediately before the irreversible step",
	}
	total := countOccurrences(body, "a.recordOpError(op,")
	accounted := 0
	for call := range allowed {
		n := countOccurrences(body, call)
		if n == 0 {
			t.Errorf("the allow-list names a call that is no longer in driveRetire:\n  %s\n\n"+
				"An allow-list entry that matches nothing is a permanent exemption for a site that "+
				"may have been replaced by an unbounded one.", call)
		}
		accounted += n
	}
	if total != accounted {
		t.Fatalf("driveRetire has %d recordOpError call(s); %d are accounted for by the allow-list.\n\n"+
			"recordOpError is non-terminal forever and does not count attempts, so an unaccounted one "+
			"can fence the whole membership plane (assertNoActiveOp) behind an operation that will "+
			"never finish. Route it through retireStepFailed, or add it here WITH the reason it is a "+
			"wait rather than a failure.", total, accounted)
	}
}

// TestRetireReadinessAsksAboutTheClusterAfterRemoval is #15.
//
// The gate used to ask whether every stream was at the target implied by the CURRENT
// voter count. With a voter already down that has no yes — three voters imply a target
// of 3, the dead one means an actual of 2 — so retire was refused on every tick,
// forever. Retiring a dead voter is what cluster-runbook.md §2 documents and what
// operators actually reach this gate for, and it was structurally impossible.
func TestRetireReadinessAsksAboutTheClusterAfterRemoval(t *testing.T) {
	stream := func(target, actual int) jsstream.StreamReplicaState {
		return jsstream.StreamReplicaState{Name: "history-lab", Target: target, Actual: actual, Ready: actual >= target}
	}
	cases := []struct {
		name          string
		votersAfter   int
		streams       []jsstream.StreamReplicaState
		wantPermitted bool
	}{
		{"3 voters, all healthy, retiring a live one", 2, []jsstream.StreamReplicaState{stream(3, 3)}, true},
		{"3 voters, one already dead — the case that used to deadlock", 2, []jsstream.StreamReplicaState{stream(3, 2)}, true},
		{"3 voters, TWO dead — must still refuse", 2, []jsstream.StreamReplicaState{stream(3, 1)}, false},
		{"one healthy stream and one laggard", 2, []jsstream.StreamReplicaState{stream(3, 3), stream(3, 1)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := ReplicaReport{Streams: tc.streams, Observed: true}
			got := replicaReportAllAtLeast(rep, jsstream.ReplicasFor(tc.votersAfter))
			if got != tc.wantPermitted {
				t.Fatalf("permitted=%v, want %v.\n\n"+
					"The question is whether every stream will STILL be at target once the node is "+
					"gone, not whether it is at target while the node is still counted. Getting this "+
					"wrong in one direction deadlocks the documented recovery; in the other it "+
					"permits a retire that drops below the replica floor.", got, tc.wantPermitted)
			}
		})
	}

	// Fail-closed is preserved exactly: an unobserved or empty report is never ready,
	// whatever the target.
	if replicaReportAllAtLeast(ReplicaReport{Observed: false, Streams: []jsstream.StreamReplicaState{stream(3, 3)}}, 1) {
		t.Error("an UNOBSERVED report was reported ready")
	}
	if replicaReportAllAtLeast(ReplicaReport{Observed: true}, 1) {
		t.Error("an EMPTY report was reported ready — a fresh cluster must not permit retire")
	}
}

// readSourceForRetireScan / functionBody / countOccurrences are the small source-scan
// helpers TestEveryRetireStepFailureIsRouted needs. Kept here, next to their only
// caller, rather than in a shared file: they encode nothing reusable, and a shared
// helper is how a scanner silently stops matching.
func readSourceForRetireScan(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("cluster_operation_controller.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	return string(b)
}

func functionBody(t *testing.T, src, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Body == nil {
			continue
		}
		start := fset.Position(fn.Body.Pos()).Offset
		end := fset.Position(fn.Body.End()).Offset
		return src[start:end]
	}
	t.Fatalf("SELF-CHECK FAILED: %s not found — this guard is scanning for a function that no "+
		"longer exists, so it can never report anything", name)
	return ""
}

func countOccurrences(hay, needle string) int {
	return strings.Count(hay, needle)
}

// origin: prerelease audit round 2, G-1.
//
// THE GATE MUST KNOW WHERE THE REPLICAS ARE, NOT JUST HOW MANY.
//
// #15 relaxed the retire readiness question from "is every stream at the CURRENT target"
// to "at the POST-REMOVAL target", which is the right question — and then answered it
// with a tally while discarding the nodeID it was asked about. So a caught-up replica
// living ON the node about to be removed counted toward the floor it was supposed to
// survive: three voters, one lagging peer, a stream at Actual=2 passes a target of 2 and
// drops to 1 the moment the retire completes. The comment claiming "no conservatism was
// traded away" was false.
func TestRetireReadinessExcludesReplicasOnTheNodeBeingRemoved(t *testing.T) {
	// Three voters; post-removal target is 2.
	const postTarget = 2
	stream := func(servers ...string) jsstream.StreamReplicaState {
		return jsstream.StreamReplicaState{
			Name: "history-lab", Target: 3, Actual: len(servers), Ready: len(servers) >= 3,
			CaughtUpServers: servers,
		}
	}
	cases := []struct {
		name    string
		streams []jsstream.StreamReplicaState
		want    bool
	}{
		{"all three caught up, retiring one of them", []jsstream.StreamReplicaState{stream("a", "b", "c")}, true},
		{"the dead voter holds nothing — the case #15 exists for", []jsstream.StreamReplicaState{stream("b", "c")}, true},
		{
			"two caught up but ONE OF THEM is the node being removed",
			[]jsstream.StreamReplicaState{stream("a", "b")},
			false,
		},
		{"placement unreported — cannot attribute, so refuse", []jsstream.StreamReplicaState{{
			Name: "history-lab", Target: 3, Actual: 3, Ready: true,
		}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := ReplicaReport{Streams: tc.streams, Observed: true}
			got := replicaReportSurvivesRemoval(rep, "a", postTarget)
			if got != tc.want {
				t.Fatalf("permitted=%v, want %v.\n\n"+
					"The question is how many caught-up replicas SURVIVE removing this node. A tally "+
					"cannot answer that, and answering it with a tally lets a retire drop a stream "+
					"below its replica floor while the gate reports ready.", got, tc.want)
			}
		})
	}

	// Fail-closed is preserved exactly.
	if replicaReportSurvivesRemoval(ReplicaReport{Observed: false, Streams: []jsstream.StreamReplicaState{stream("a", "b", "c")}}, "z", 1) {
		t.Error("an UNOBSERVED report was reported ready")
	}
	if replicaReportSurvivesRemoval(ReplicaReport{Observed: true}, "z", 1) {
		t.Error("an EMPTY report was reported ready")
	}
}

// origin: prerelease audit round 2, G-2.
//
// THE WIRING, not the helper. The #15 guard exercised replicaReportAllAtLeast directly
// and nothing asserted that clusterStreamsReady — its only production consumer — called
// it, so reverting the fix to `rep.AllAtTarget()` left the entire suite green. A verifier
// proved that by doing exactly the revert.
//
// This asserts the shape in the source, which is what a revert would change. It is a
// weaker instrument than a behavioural test and it is chosen deliberately: driving
// clusterStreamsReady needs a wired auditPub and a live raft, which is a deploy-tier
// fixture, and an unwired guard is worse than a source-shape one.
func TestClusterStreamsReadyUsesThePlacementAwarePredicate(t *testing.T) {
	src, err := os.ReadFile("clusterwrite.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	body := functionBody(t, string(src), "clusterStreamsReady")

	if !strings.Contains(body, "replicaReportSurvivesRemoval(") {
		t.Fatal("clusterStreamsReady does not call replicaReportSurvivesRemoval.\n\n" +
			"Whatever the helper does, production is not asking it — which is exactly how #15's " +
			"first version passed every gate while the fix could be reverted at the call site.")
	}
	// And it must still USE the node it is asked about, or it is back to a tally.
	if strings.Contains(body, "func (b *Broker) clusterStreamsReady(string)") {
		t.Error("the nodeID parameter is unnamed again — the gate cannot exclude a node it does not know")
	}
	if !strings.Contains(body, "natsServerIDFor(b, nodeID)") {
		t.Error("clusterStreamsReady no longer resolves the retiring node's NATS server name, so it " +
			"cannot tell which replicas live on it")
	}
}
