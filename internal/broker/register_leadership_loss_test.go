package broker

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-external-final-review.md X4
//
// A REGISTER THAT LOSES THE LEADERSHIP RACE MUST BE RETRIABLE, NOT TERMINAL.
//
// This is the one gate the whole increment lacked, and the gap had already been
// paid for once. handleRegister is leader-only, but leadership can be lost
// between its isClusterFollower() check and raft.Apply — an ordinary election,
// a quorum blip, a `cluster drain`. proposeOrForward exists partly to map that
// window's raft error onto cluster.ErrForwardNotLeader, and the mapping is not
// cosmetic: internal/agent's register loop retries a transient code and EXITS
// THE PROCESS on a terminal one. A raw raft error surfacing as store_error
// therefore takes down every agent that happens to re-register during a routine
// failover — the fleet, not one node.
//
// The final external review's X4 rewrote registerNode to call cl.node.Propose
// directly (to fold the process-row refile into the same raft command) and lost
// that mapping in the process. The refile idea was right; the call site was not.
// Nothing caught it, because every existing test runs on a broker that keeps its
// leadership for the whole test.
//
// So this builds a REAL raft node, makes it genuinely lose quorum, and asserts
// on the error class an agent would act on.
//
// MUTATION: swap proposeOrForward back for a bare b.cl.node.Propose in
// registerNode and this goes red.
func TestRegisterDuringLeadershipLossIsRetriableNotTerminal(t *testing.T) {
	n := leadershipLossNode(t, "brk-x4")

	// A REAL forwarder pointed at a bus nobody answers on. That is the honest
	// shape of the window under test: once this node has stepped down, a
	// register is supposed to be FORWARDED to whoever holds leadership now —
	// and when the answer does not come, the outcome must still be the
	// retriable sentinel rather than a raft error.
	//
	// It also pins something the bare-Propose rewrite silently removed: with no
	// forward branch at all, a leader that steps down mid-register fails the
	// register outright instead of handing it to the new leader.
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	b := &Broker{clusterMode: true, cl: &clusterRuntime{
		node:      n,
		forwarder: NewForwarder(nc, 500*time.Millisecond),
	}}
	b.cfg = Config{DB: testharness.OpenDB(t), Logger: testharness.SilentLog(), Now: time.Now}

	err = b.registerNode(node.RegisterInput{
		SID: "lab", NID: "gpu1", ProtoVersion: proto.ProtoVersion,
	}, "", nil)

	if err == nil {
		t.Fatal("a register proposed with no quorum must not report success")
	}
	// THE ASSERTION THAT MATTERS: the class, not the text. cluster.ErrForwardNotLeader
	// is what the register path translates into a retriable reply; a raw raft
	// error is what it translates into a terminal one.
	if !errors.Is(err, cluster.ErrForwardNotLeader) {
		t.Fatalf("register during a leadership loss returned %v (%T), which is NOT the retriable "+
			"sentinel. The agent's register loop treats a terminal rejection as a config error and "+
			"EXITS, so one ordinary raft election would take down every agent that re-registered "+
			"during it. proposeOrForward carries this mapping — bypassing it is a fleet outage.", err, err)
	}
	// And it must not be mistaken for a raw raft error by anything downstream.
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
		t.Fatalf("the raw raft error leaked through as %v; cluster stays raft-free above this "+
			"boundary so the broker adapter (not the wire) does the translation", err)
	}
}

// origin: same X4 review.
//
// The refile statements X4 added ride the SAME raft command as the identity row,
// which is what makes an adoption atomic without a new op an un-upgraded replica
// would not know. That property is worth a guard of its own: if the moves ever
// became a second command, an apply could land the rename without the rows (or
// the rows without the rename) and `tether ps <lease>` would disagree with the
// node list for as long as the gap lasted.
func TestAdoptedProcessMovesRideTheRegisterCommand(t *testing.T) {
	db := testharness.OpenDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		"lab", "lab", "SHA256:owner", "hash"); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	cmd, err := planRegisterWithRefiles(db,
		node.RegisterInput{SID: "lab", NID: "gpu1-02", ProtoVersion: proto.ProtoVersion},
		"gpu1",
		[]proto.LocalProcess{{PID: "p2", State: "running"}, {PID: "p1", State: "running"}},
		time.Now().UTC())
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if cmd.Op != cluster.OpNodeRegister {
		t.Fatalf("the moves must ride the EXISTING register op (an un-upgraded replica knows no "+
			"other); got op %v", cmd.Op)
	}
	if len(cmd.Body) < 2 {
		t.Fatalf("expected the identity statement plus one move per re-presented pid; got %d "+
			"statement(s)", len(cmd.Body))
	}
	// DETERMINISM: raft replays this on every replica, so the statement order
	// must not depend on Go's map iteration. p1 sorts before p2 regardless of
	// the order the agent listed them in above.
	first, second := -1, -1
	for i, st := range cmd.Body {
		switch {
		case first < 0 && strings.Contains(st.SQL, "'p1'"):
			first = i
		case second < 0 && strings.Contains(st.SQL, "'p2'"):
			second = i
		}
	}
	if first < 0 || second < 0 {
		t.Fatalf("both re-presented pids must be moved; body=%v", cmd.Body)
	}
	if first > second {
		t.Fatal("the moves are not ordered: raft applies this command on every replica, so a " +
			"map-iteration order would make replicas diverge on identical input")
	}
}

// leadershipLossNode returns a real raft node that has genuinely lost quorum:
// bootstrapped alone (so it wins), then given a second voter that does not
// exist, which costs it the majority it needs to commit anything.
func leadershipLossNode(t *testing.T, id string) *cluster.Node {
	t.Helper()
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	dir := t.TempDir()
	n, err := cluster.New(cluster.Config{
		LocalID:            raft.ServerID(id),
		DataDir:            dir,
		DBPath:             filepath.Join(dir, "state.db"),
		Transport:          trans,
		ApplyTimeout:       2 * time.Second,
		HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
		ElectionTimeout:    cluster.MultinodeElectionTimeout,
		LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	// A voter at an address nothing answers on. The cluster is now 2 nodes with
	// 1 reachable, so this node cannot hold a majority and steps down.
	if err := n.AddVoter("ghost", "ghost-addr-nothing-listens"); err != nil {
		t.Fatalf("add ghost voter: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { return !n.IsLeader() }) {
		t.Fatal("the node kept leadership after losing quorum; this fixture cannot produce the " +
			"race the test is about")
	}
	return n
}

func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
