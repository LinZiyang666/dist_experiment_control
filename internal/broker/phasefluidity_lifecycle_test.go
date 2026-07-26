//go:build phasefluidity_integration

// Package broker phase-fluidity LIFECYCLE drills (external-review R5). Gated out of `make test`
// (a real raft node + kill/restart); run with `go test -tags phasefluidity_integration -race
// ./internal/broker/ -run TestPhaseFluidityLifecycle`. This file proves the failed-join cleanup
// SURVIVES a leader kill/restart — the gap R1/F1's sequential unit tests could not reach.
package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// pfRestartableNode creates a single-voter InmemTransport node at a STABLE dataDir so it can be shut
// down and re-created (a leader "kill/restart"). bootstrap=true only on the FIRST creation; a restart
// re-reads the persisted raft configuration + SQLite from dir.
func pfRestartableNode(t *testing.T, id, dir string, bootstrap bool) *cluster.Node {
	t.Helper()
	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	cfg := cluster.Config{
		LocalID: raft.ServerID(id), DataDir: dir, DBPath: filepath.Join(dir, "state.db"),
		Transport: trans, ApplyTimeout: 30 * time.Second,
		HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout, LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
	}
	if bootstrap {
		cfg.BootstrapPeers = []raft.Server{{Suffrage: raft.Voter, ID: raft.ServerID(id), Address: addr}}
	}
	n, err := cluster.New(cfg)
	if err != nil {
		t.Fatalf("new node %s (bootstrap=%v): %v", id, bootstrap, err)
	}
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatalf("node %s leadership: %v", id, err)
	}
	return n
}

// TestPhaseFluidityLifecycleFailedJoinSurvivesLeaderRestart is the R5 failed-join kill/restart drill.
// A failed grow stages an unreachable joiner as a nonvoter; the operator aborts; the LEADER is killed
// and restarted from its persisted store. After the restart the controller must NOT promote the
// aborted join (the R1 hazard, now also across a restart), and the staged nonvoter must still be
// online-removable (F1) — never a force-single dead end.
func TestPhaseFluidityLifecycleFailedJoinSurvivesLeaderRestart(t *testing.T) {
	dir := t.TempDir()
	n := pfRestartableNode(t, "pf-leader", dir, true)
	admin := NewClusterAdmin(n, nil)
	admin.caughtUpFn = func(string, uint64) (bool, error) { return false, nil }

	// stage a failed join (unreachable joiner → CATCHING_UP nonvoter).
	bundle, _ := makeJoinBundle(t, "pf-joiner", "10.0.0.50:7400", "pf-nonce")
	opID, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("StartJoinOperation: %v", err)
	}
	for i := 0; i < 8; i++ {
		admin.driveInFlightOperations()
	}
	if sub, _ := admin.readSubstrate("pf-joiner"); !sub.inRaft || sub.isVoter || sub.phase != phaseCatchingUp {
		t.Fatalf("setup did not stage a nonvoter/CATCHING_UP: %+v", sub)
	}

	// abort, then KILL + RESTART the leader from its persisted store.
	if err := admin.AbortOp(opID); err != nil {
		t.Fatalf("AbortOp: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatalf("leader shutdown: %v", err)
	}
	n = pfRestartableNode(t, "pf-leader", dir, false)
	t.Cleanup(func() { _ = n.Shutdown() })
	admin = NewClusterAdmin(n, nil)
	// Adversarial: even if the joiner now "reports" caught-up, an ABORTED op must never be promoted.
	admin.caughtUpFn = func(string, uint64) (bool, error) { return true, nil }

	for i := 0; i < 4; i++ {
		admin.driveInFlightOperations()
	}
	sub, err := admin.readSubstrate("pf-joiner")
	if err != nil {
		t.Fatalf("readSubstrate post-restart: %v", err)
	}
	if sub.isVoter {
		t.Fatalf("aborted join was promoted to voter AFTER a leader restart: %+v", sub)
	}
	if err := admin.RemoveNode("pf-joiner", false); err != nil {
		t.Fatalf("aborted failed join must be online-removable after restart (no force-single): %v", err)
	}
	if err := n.ApplyMetaSet("t:pf-postclean", "ok"); err != nil {
		t.Fatalf("cluster not writable after the failed-join cleanup: %v", err)
	}
}
