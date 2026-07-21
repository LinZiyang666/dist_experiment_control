package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
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

// agentlessRollFixture is a three-broker cluster in which NO host runs a co-located agent. It answers
// cluster-health for all three and the signed upgrade-trigger subject, tracking exactly what the roll did.
type agentlessRollFixture struct {
	mu sync.Mutex
	// per-broker self-reported release; a `reload` flips it to the target, like a real re-exec.
	version map[string]string
	leader  string
	reloads []string
	reexecs []string // MUST stay empty: a broker-only host has no agent leg
	homes   int
}

func (f *agentlessRollFixture) snapshot() []proto.ClusterHealthResp {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := []string{"brk1", "brk2", "brk3"}
	out := make([]proto.ClusterHealthResp, 0, len(ids))
	for _, id := range ids {
		out = append(out, proto.ClusterHealthResp{
			NodeID: id, ReleaseVersion: f.version[id],
			WritableLeaderConfirmed: id == f.leader, LeaderID: f.leader,
			IsVoter: true, AppliedIndex: 100, CommandVer: 3,
			// ColocatedAgentNID deliberately EMPTY on every host — this is the agentless shape.
		})
	}
	return out
}

// startAgentlessRollFixture wires the fixture onto NATS and returns it.
func startAgentlessRollFixture(t *testing.T, nc *nats.Conn, actor string, from string) *agentlessRollFixture {
	t.Helper()
	f := &agentlessRollFixture{
		version: map[string]string{"brk1": from, "brk2": from, "brk3": from},
		leader:  "brk3", // leader-last: the roll must transfer off it
	}
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		// Each broker answers separately, exactly as in production.
		for _, h := range f.snapshot() {
			body, _ := json.Marshal(h)
			_ = nc.Publish(m.Reply, body)
		}
	})
	// The session node list: NO agent has ever registered here. This is the observation that makes the
	// hosts agentless rather than "agent temporarily missing".
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, "sid-agentless"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{}})
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		resp := proto.ClusterUpgradeResp{OK: true}
		f.mu.Lock()
		switch req.Op {
		case "reload":
			f.reloads = append(f.reloads, req.TargetNode)
			f.version[req.TargetNode] = req.ToVersion
		case "transfer-leader":
			f.leader = req.TransferTo
			resp.NewLeader = req.TransferTo
		case "reexec-agent":
			f.reexecs = append(f.reexecs, req.TargetNode)
			// Verbatim production behaviour: internal/broker forwards to the broker's own node_id when
			// colocated_agent_nid is unset, and on an agentless host nobody is listening.
			resp = proto.ClusterUpgradeResp{Code: "agent_no_responders", Error: "nats: no responders available for request"}
		case upgradeHomesConvergedOp:
			f.homes++
		}
		f.mu.Unlock()
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

// TestAgentlessClusterCompletesAWholeRoll is P3's exit assertion, end to end through the real planner and
// the real executor: a cluster whose broker hosts run no agent must be able to FINISH a rolling upgrade,
// with every host self-reporting the target afterwards.
//
// Mutation checks this is not a tautology:
//   - drop Step.BrokerOnly from the drive loop  → reexec-agent is sent → agent_no_responders → HALT.
//   - revert AtTarget to require AgentVer==target → after the roll the hosts still never read "at target",
//     and the idempotency sub-test below (a re-run planning zero work) fails.
func TestAgentlessClusterCompletesAWholeRoll(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-p3-agentless"
	const from, to = "v0.0.0-simcluster", "v0.0.0-simcluster-next"

	f := startAgentlessRollFixture(t, nc, actor, from)

	ctx := context.Background()
	var planOut bytes.Buffer
	nodes, err := buildUpgradeNodes(ctx, nc, actor, "sid-agentless", "", &planOut)
	if err != nil {
		t.Fatalf("buildUpgradeNodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("folded %d nodes, want 3", len(nodes))
	}
	for _, n := range nodes {
		if n.Agent != clusterupgrade.AgentAbsent {
			t.Fatalf("%s classified %v — with no agent declared and none in the node list the only correct "+
				"answer is AgentAbsent; anything else re-creates P3", n.ID, n.Agent)
		}
	}
	if !strings.Contains(planOut.String(), "runs no co-located tether agent") {
		t.Errorf("the broker-only classification was silent; the operator must be told why a host's verdict "+
			"skips the agent leg. got:\n%s", planOut.String())
	}

	plan := clusterupgrade.Compute(nodes, to)
	if len(plan.Refused) != 0 {
		t.Fatalf("refused: %v", plan.Refused)
	}
	if plan.Upgrades() != 3 {
		t.Fatalf("planned %d upgrades, want 3", plan.Upgrades())
	}

	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(ctx)

	if err := driveUpgrade(cmd, nc, actor, "sid-agentless", seed, plan, to, "", ""); err != nil {
		t.Fatalf("P3: the roll HALTED on an agentless cluster: %v\n\n%s", err, out.String())
	}

	f.mu.Lock()
	reloads, reexecs, homes := append([]string(nil), f.reloads...), append([]string(nil), f.reexecs...), f.homes
	versions := map[string]string{}
	for k, v := range f.version {
		versions[k] = v
	}
	f.mu.Unlock()

	if len(reexecs) != 0 {
		t.Fatalf("the roll sent reexec-agent to %v on hosts that have NO agent — that request can only ever "+
			"come back agent_no_responders and HALT the roll. This is the P3 chain verbatim.", reexecs)
	}
	if len(reloads) != 3 {
		t.Fatalf("reloaded %v, want all three hosts", reloads)
	}
	// The field symptom: brk1=old / brk2=new / brk3=old. Assert the whole cluster converged.
	for _, id := range []string{"brk1", "brk2", "brk3"} {
		if versions[id] != to {
			t.Errorf("%s self-reports %q, want %q — the cluster is stranded on MIXED versions, which is exactly "+
				"what P3 produced in the field (only one host ever got upgraded)", id, versions[id], to)
		}
	}
	if homes == 0 {
		t.Error("the roll never asked for the data-plane verdict — the R8a rc gate was skipped")
	}

	// Idempotency / convergence: re-planning against the post-roll state must now find NOTHING to do.
	// Before P3 this stayed at 3 forever, so `cluster upgrade` could never report completion and the
	// stale-lock self-heal inside `plan.Upgrades()==0` was unreachable on agentless hosts.
	var reOut bytes.Buffer
	after, err := buildUpgradeNodes(ctx, nc, actor, "sid-agentless", "", &reOut)
	if err != nil {
		t.Fatalf("re-fold: %v", err)
	}
	if again := clusterupgrade.Compute(after, to); again.Upgrades() != 0 {
		t.Fatalf("a second run still plans %d upgrade(s) after every host reached %s — the roll never converges",
			again.Upgrades(), to)
	}
}

// TestARealAgentStillGetsReExecdOnAMixedCluster is the anti-over-correction guard: P3 must not turn into
// "never re-exec agents". A host that DOES have a co-located agent still gets its agent leg, on the very
// same roll as an agentless one.
func TestARealAgentStillGetsReExecdOnAMixedCluster(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-p3-mixed"
	const from, to = "v1", "v2"

	var mu sync.Mutex
	version := map[string]string{"brk1": from, "brk2": from}
	agentVer := from
	leader := "brk2"
	var reexecs []string

	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		mu.Lock()
		hs := []proto.ClusterHealthResp{
			// brk1 DECLARES a co-located agent; brk2 declares none and has none registered.
			{NodeID: "brk1", ReleaseVersion: version["brk1"], ColocatedAgentNID: "agent-1",
				WritableLeaderConfirmed: leader == "brk1", LeaderID: leader, IsVoter: true, AppliedIndex: 10, CommandVer: 3},
			{NodeID: "brk2", ReleaseVersion: version["brk2"],
				WritableLeaderConfirmed: leader == "brk2", LeaderID: leader, IsVoter: true, AppliedIndex: 10, CommandVer: 3},
		}
		mu.Unlock()
		for _, h := range hs {
			body, _ := json.Marshal(h)
			_ = nc.Publish(m.Reply, body)
		}
	})
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, "sid-mixed"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		mu.Lock()
		av := agentVer
		mu.Unlock()
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{
			{NID: "agent-1", Status: "ONLINE", ReleaseVersion: av},
		}})
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		resp := proto.ClusterUpgradeResp{OK: true}
		mu.Lock()
		switch req.Op {
		case "reload":
			version[req.TargetNode] = req.ToVersion
		case "transfer-leader":
			leader = req.TransferTo
			resp.NewLeader = req.TransferTo
		case "reexec-agent":
			reexecs = append(reexecs, req.TargetNode)
			agentVer = to
		}
		mu.Unlock()
		body, _ := json.Marshal(resp)
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	nodes, err := buildUpgradeNodes(ctx, nc, actor, "sid-mixed", "", &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]clusterupgrade.Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	if byID["brk1"].Agent != clusterupgrade.AgentPresent {
		t.Fatal("a host that DECLARED a co-located agent must classify AgentPresent")
	}
	if byID["brk2"].Agent != clusterupgrade.AgentAbsent {
		t.Fatal("a host with no declaration and no registered agent must classify AgentAbsent")
	}

	plan := clusterupgrade.Compute(nodes, to)
	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetContext(ctx)
	if err := driveUpgrade(cmd, nc, actor, "sid-mixed", seed, plan, to, "", ""); err != nil {
		t.Fatalf("mixed roll HALTED: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), reexecs...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "brk1" {
		t.Fatalf("agent re-execs = %v, want exactly [brk1]. P3 must skip the agent leg only where there IS no "+
			"agent; skipping it on a host that has one leaves a half-upgraded host and reports success.", got)
	}
}

// TestAgentlessDriveIsGovernedByThePlanNotALiveProbe pins WHERE the broker-only decision comes from. If
// the executor re-derived it from a live probe, a registration flap mid-roll could flip a host between
// whole-host and broker-only halfway through its own step, and the operator's dry-run would stop being a
// promise. The plan decides once; the driver obeys.
func TestAgentlessDriveIsGovernedByThePlanNotALiveProbe(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	actor := "Uactor-p3-planwins"
	const to = "v2"

	var mu sync.Mutex
	var reexecs int
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.ClusterHealthResp{
			NodeID: "brk1", ReleaseVersion: to, WritableLeaderConfirmed: true, LeaderID: "brk1",
			IsVoter: true, AppliedIndex: 1, CommandVer: 3,
		})
		_ = m.Respond(body)
	})
	// A live probe WOULD see an agent here (node_id==nid). The plan says broker-only; the plan must win.
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, "sid-plan"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{
			{NID: "brk1", Status: "ONLINE", ReleaseVersion: "v1"},
		}})
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op == "reexec-agent" {
			mu.Lock()
			reexecs++
			mu.Unlock()
		}
		body, _ := json.Marshal(proto.ClusterUpgradeResp{OK: true})
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	plan := clusterupgrade.Plan{Steps: []clusterupgrade.Step{
		{Kind: clusterupgrade.StepUpgrade, NodeID: "brk1", BrokerOnly: true},
	}}
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetContext(context.Background())
	if err := driveUpgrade(cmd, nc, actor, "sid-plan", seed, plan, to, "", ""); err != nil {
		t.Fatalf("roll HALTED: %v\n%s", err, out.String())
	}
	mu.Lock()
	n := reexecs
	mu.Unlock()
	if n != 0 {
		t.Fatalf("the driver sent %d reexec-agent request(s) for a step the PLAN marked broker-only — it is "+
			"re-deriving the decision from a live probe instead of obeying the plan", n)
	}
	if !strings.Contains(out.String(), "broker-only host") {
		t.Errorf("a broker-only host must say so in the roll output; got:\n%s", out.String())
	}
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
