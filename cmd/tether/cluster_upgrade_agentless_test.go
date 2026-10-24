package main

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// cluster_upgrade_agentless_test.go — P3. `tether cluster upgrade` was structurally unable to finish on a
// broker host that runs no co-located agent. `serve --colocated-agent-nid` is optional and empty by
// default, so that is a supported production shape (this project's own live brokers run that way).
//
// The failure chain, end to end:
//     AgentVer "" ⇒ AtTarget always false ⇒ every host gets an UPGRADE step ⇒ agentVersionOf returns
//     ok=false ⇒ reexec-agent is sent UNCONDITIONALLY ⇒ the broker falls back to its own node_id as the
//     agent nid ⇒ nobody answers ⇒ agent_no_responders ⇒ haltUpgrade.
// Observed in the field: three brokers with the new binary on disk, ONE self-reporting the new version,
// and a roll lock blocking every subsequent grow/retire.

// TestResolveColocatedAgentClassifiesAllThreeStates pins the ctl-side classifier. This is where (c) is
// distinguished from (b); everything downstream just consumes the answer.
func TestResolveColocatedAgentClassifiesAllThreeStates(t *testing.T) {
	cases := []struct {
		name         string
		brokerID     string
		health       proto.ClusterHealthResp
		agentRelease map[string]string
		wantNID      string
		wantPresence clusterupgrade.AgentPresence
		why          string
	}{
		{
			name:         "declared agent, registered",
			brokerID:     "brk1",
			health:       proto.ClusterHealthResp{NodeID: "brk1", ColocatedAgentNID: "node-a"},
			agentRelease: map[string]string{"node-a": "v2"},
			wantNID:      "node-a",
			wantPresence: clusterupgrade.AgentPresent,
		},
		{
			name:         "declared agent that is NOT in the node list stays PRESENT",
			brokerID:     "brk1",
			health:       proto.ClusterHealthResp{NodeID: "brk1", ColocatedAgentNID: "node-a"},
			agentRelease: map[string]string{},
			wantNID:      "node-a",
			wantPresence: clusterupgrade.AgentPresent,
			why: "presence is a CONFIGURATION fact when declared; downgrading it to broker-only would let a roll " +
				"silently skip a real agent and print success on a half-upgraded host",
		},
		{
			name:         "undeclared but an agent registered under node_id==nid",
			brokerID:     "brk1",
			health:       proto.ClusterHealthResp{NodeID: "brk1"},
			agentRelease: map[string]string{"brk1": "v1"},
			wantNID:      "brk1",
			wantPresence: clusterupgrade.AgentPresent,
			why:          "the pre-G5 convention still has to work",
		},
		{
			name:     "undeclared and the agent is merely OFFLINE — still PRESENT",
			brokerID: "brk1",
			health:   proto.ClusterHealthResp{NodeID: "brk1"},
			// handleNodeListReq projects every node EVER registered (ONLINE|STALE|OFFLINE), so a
			// stopped agent is still listed — with an empty release if it never reported one.
			agentRelease: map[string]string{"brk1": ""},
			wantNID:      "brk1",
			wantPresence: clusterupgrade.AgentPresent,
			why:          "a stopped agent must fail the roll loudly, not be reclassified as 'this host has no agent'",
		},
		{
			name:         "undeclared and no agent has ever registered — ABSENT",
			brokerID:     "brk1",
			health:       proto.ClusterHealthResp{NodeID: "brk1"},
			agentRelease: map[string]string{"some-other-node": "v2"},
			wantNID:      "",
			wantPresence: clusterupgrade.AgentAbsent,
			why:          "this is the dedicated broker host — P3's whole point",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nid, presence := resolveColocatedAgent(tc.brokerID, tc.health, tc.agentRelease)
			if nid != tc.wantNID || presence != tc.wantPresence {
				t.Fatalf("resolveColocatedAgent = (%q, %v), want (%q, %v) — %s",
					nid, presence, tc.wantNID, tc.wantPresence, tc.why)
			}
		})
	}
}

// TestAgentlessClusterCompletesAWholeRoll is P3's exit assertion, end to end through the real planner and
// the real executor: a cluster whose broker hosts run no agent must be able to FINISH a rolling upgrade,
// with every host self-reporting the target afterwards.
//
// Mutation checks this is not a tautology:
//   - drop Step.BrokerOnly from the drive loop  → reexec-agent is sent → agent_no_responders → HALT.
//   - revert AtTarget to require AgentVer==target → after the roll the hosts still never read "at target",
//     and the idempotency sub-test below (a re-run planning zero work) fails.
//
// origin: S8 internal review upgrade-maketest-2 F2; hot-update S8 plan §3.1 / §9.4
func TestAgentlessClusterCompletesAWholeRoll(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	binary := signedRollBuild(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	// Independent clusters share only the immutable artifact. Join both lanes
	// before the leak census; serial rounds measured above the 420s split limit.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F5; plan L17 / §11.1
	lanes := make(chan struct{}, 2)
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				exerciseSignedAgentlessRoll(t, binary)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "signed agentless roll", beforeG)
	testharness.AssertNoFDLeak(t, "signed agentless roll", beforeFD, 0)
}

// TestARealAgentStillGetsReExecdOnAMixedCluster is the anti-over-correction guard: P3 must not turn into
// "never re-exec agents". A host that DOES have a co-located agent still gets its agent leg, on the very
// same roll as an agentless one.
// origin: S8 internal review upgrade-maketest-2 F3; hot-update S8 plan §5.1 / §9.4
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3
func TestARealAgentStillGetsReExecdOnAMixedCluster(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	binary := signedRollBuild(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	// Three isolated real clusters may progress concurrently. The group joins
	// every round (including process cleanup) before the shared leak census.
	// Every parent still uses its original product deadline, with five rounds
	// inside the unchanged 600s matrix unit.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan L21
	// The two-lane current-tree rerun measured 429s; retain the same five
	// independent exercises with one more lane, below the 420s split threshold.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F5; plan L17 / §11.1
	lanes := make(chan struct{}, 3)
	t.Run("rounds", func(t *testing.T) {
		// Explicitly join these isolated subtests: t.Parallel alone inherits
		// GOMAXPROCS=2 from the matrix and cannot use the third fixture lane.
		var rounds sync.WaitGroup
		for round := 0; round < 5; round++ {
			rounds.Go(func() {
				lanes <- struct{}{}
				defer func() { <-lanes }()
				t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseSignedMixedRoll(t, binary) })
			})
		}
		rounds.Wait()
	})
	testharness.AssertNoGoroutineLeak(t, "signed roll", beforeG)
	testharness.AssertNoFDLeak(t, "signed roll", beforeFD, 0)
}

// TestAgentlessDriveIsGovernedByThePlanNotALiveProbe pins WHERE the broker-only decision comes from. If
// the executor re-derived it from a live probe, a registration flap mid-roll could flip a host between
// whole-host and broker-only halfway through its own step, and the operator's dry-run would stop being a
// promise. The plan decides once; the driver obeys.
// origin: S8 internal review upgrade-maketest-2 F4; hot-update S8 plan §3.1 / §9.4
func TestAgentlessDriveIsGovernedByThePlanNotALiveProbe(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	binary := signedRollBuild(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	// Independent clusters share only the immutable artifact. Join both lanes
	// before the leak census; serial rounds measured above the 420s split limit.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F5; plan L17 / §11.1
	lanes := make(chan struct{}, 2)
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				exerciseSignedPlanGovernedRoll(t, binary)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "signed roll", beforeG)
	testharness.AssertNoFDLeak(t, "signed roll", beforeFD, 0)
}

// TestUpgradeAgentlessRollFinishesPromptly is a cheap watchdog: P3's pre-fix symptom was an
// agent_no_responders HALT, but a regression that instead made the roll spin would be just as broken.
func TestUpgradeAgentlessRollFinishesPromptly(t *testing.T) {
	if testing.Short() {
		t.Skip("timing watchdog")
	}
	start := time.Now()
	t.Cleanup(func() {
		if d := time.Since(start); d > 90*time.Second {
			t.Errorf("the agentless roll suite took %s — a roll that no longer HALTs but spins is not a fix", d)
		}
	})
}
