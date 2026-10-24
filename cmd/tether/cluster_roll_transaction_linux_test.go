//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Each original command invariant now crosses the authenticated transaction
// route into the real daemons. Five independent clusters keep async cleanup
// from hiding a journal, subscription or process lifetime leak.
// origin: hot-update S8 plan §3.1 / §4.2 / §9.4; S8 internal review upgrade-maketest-2 F2
func exerciseSignedAgentlessRoll(t *testing.T, binary string) {
	t.Helper()
	f := startSignedRollFixture(t, binary, false)
	var planOut bytes.Buffer
	nodes, err := buildUpgradeNodes(t.Context(), f.nc, f.actor, "signed-roll", "", &planOut)
	if err != nil {
		t.Fatalf("buildUpgradeNodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("folded %d nodes, want 3", len(nodes))
	}
	for _, n := range nodes {
		if n.Agent != clusterupgrade.AgentAbsent {
			t.Fatalf("%s classified %v, want AgentAbsent", n.ID, n.Agent)
		}
	}
	if !strings.Contains(planOut.String(), "runs no co-located tether agent") {
		t.Errorf("the broker-only classification was silent: %s", planOut.String())
	}
	health, err := cli.QueryBrokerRollHealthForVoters(t.Context(), f.nc, f.actor, []string{"brk1", "brk2", "brk3"})
	if err != nil {
		t.Fatal(err)
	}
	plan := cli.PlanClusterRoll(nodes, signedRollRelease, clusterupgrade.ScopeWholeHost, f.sha, health)
	if len(plan.Refused) != 0 || plan.Upgrades() != 3 {
		t.Fatalf("invalid three-host plan: %+v", plan)
	}
	cmd := f.command(t)
	if err := driveUpgrade(cmd, f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, ""); err != nil {
		t.Logf("wire: %+v", f.wire())
		t.Fatalf("the roll HALTED on an agentless cluster: %v", err)
	}
	f.assertImages(t)
	cuts, homes := map[string]int{}, 0
	for _, r := range f.wire() {
		if r.Op == "reload" || r.Op == "reexec-agent" {
			t.Fatalf("agentless roll used legacy dispatch: %+v", r)
		}
		if r.Inner == "upgrade.start" {
			cuts[r.Target]++
		}
		if r.Inner == "roll.home" {
			homes++
		}
	}
	if len(cuts) != 3 {
		t.Fatalf("cut %v, want all three hosts", cuts)
	}
	for _, p := range f.processes {
		if cuts[p.id] != 1 {
			t.Errorf("%s cuts=%d, want exactly one", p.id, cuts[p.id])
		}
	}
	if homes == 0 {
		t.Error("the roll never asked for the data-plane verdict")
	}
	f.assertParent(t)
	// Convergence uses both the original no-agent classification and independently
	// measured target images; release equality alone cannot make a re-run idle.
	after, err := buildUpgradeNodes(t.Context(), f.nc, f.actor, "signed-roll", "", &bytes.Buffer{})
	if err != nil {
		t.Fatalf("re-fold: %v", err)
	}
	if again := clusterupgrade.Compute(after, signedRollRelease); again.Upgrades() != 0 {
		t.Fatalf("a second run still plans %d upgrade(s)", again.Upgrades())
	}
	before := len(f.wire())
	if err := driveUpgrade(f.command(t), f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, ""); err != nil {
		t.Fatalf("fresh no-op: %v", err)
	}
	for _, r := range f.wire()[before:] {
		if r.Inner == "upgrade.start" || r.Inner == "roll.reserve_cluster" {
			t.Fatalf("fresh no-op acquired execution authority: %+v", r)
		}
	}
}

func (f *signedRollFixture) assertParent(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.home, "upgrades", "grants"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("cached parent grants=%d, want one", len(entries))
	}
	grant, err := upgradecli.RecallGrant(f.home, strings.TrimSuffix(entries[0].Name(), ".json"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rollcontrol.Request{Op: "roll.load", Parent: grant})
	if err != nil {
		t.Fatal(err)
	}
	response, err := sendUpgradeTriggerOnce(t.Context(), f.nc, f.actor, f.seed, &proto.ClusterUpgradeReq{Op: "upgrade-tx", TargetNode: strings.TrimSuffix(grant.Handle.Key.ConfiguredNID, "@roll"), ActorFP: grant.ActorFP, Holder: grant.Handle.TxID, Upgrade: raw})
	if err != nil {
		t.Fatal(err)
	}
	var result rollcontrol.Response
	if err = json.Unmarshal(response.Upgrade, &result); err != nil {
		t.Fatal(err)
	}
	if !response.OK || result.Code != "" || result.View.Historical.Forward.Phase != upgradetx.PhaseCompleted || result.View.Historical.Forward.Outcome != upgradetx.OutcomeSuccess {
		t.Fatalf("durable parent did not complete: err=%v result=%+v", err, result)
	}
}

// origin: hot-update S8 plan §5.1 / §9.4; S8 internal review upgrade-maketest-2 F3
func exerciseSignedMixedRoll(t *testing.T, binary string) {
	t.Helper()
	f := startSignedRollFixture(t, binary, true)
	nodes, err := buildUpgradeNodes(t.Context(), f.nc, f.actor, "signed-roll", "", &bytes.Buffer{})
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
	health, err := cli.QueryBrokerRollHealthForVoters(t.Context(), f.nc, f.actor, []string{"brk1", "brk2", "brk3"})
	if err != nil {
		t.Fatal(err)
	}
	plan := cli.PlanClusterRoll(nodes, signedRollRelease, clusterupgrade.ScopeWholeHost, f.sha, health)
	if err = driveUpgrade(f.command(t), f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, ""); err != nil {
		t.Logf("agent log: %s", f.agent.logs.String())
		wire := f.wire()
		if len(wire) > 20 {
			wire = wire[len(wire)-20:]
		}
		t.Logf("wire tail: %+v", wire)
		t.Fatalf("mixed roll HALTED: %v", err)
	}
	starts := map[string]int{}
	for _, r := range f.wire() {
		if r.Op == "reexec-agent" {
			t.Fatal("mixed roll used the unauthorized legacy agent route")
		}
		if r.Inner == "upgrade.start" && r.Target == "agent-1" {
			starts[r.Target]++
		}
	}
	if len(starts) != 1 || starts["agent-1"] != 1 {
		t.Fatalf("agent re-execs = %v, want exactly [brk1]. Skipping a real agent leaves a half-upgraded host and reports success", starts)
	}
	if actual := signedRollSHA(t, fmt.Sprintf("/proc/%d/exe", f.agent.cmd.Process.Pid)); actual != f.sha {
		t.Fatalf("the real agent still runs %s, want %s", actual, f.sha)
	}
	owner, err := upgradestage.ProcessOwner(f.agent.cmd.Process.Pid)
	if err != nil || owner != f.agent.original {
		t.Fatalf("agent cut changed its kernel lineage: %+v -> %+v (%v)", f.agent.original, owner, err)
	}
	f.assertImages(t)
	f.assertParent(t)
}

// A registered live agent is visible to the ordinary node-list route, while
// every immutable Step says BrokerOnly. Drive must never reclassify that plan.
// origin: hot-update S8 plan §3.1 / §9.4; S8 internal review upgrade-maketest-2 F4
func exerciseSignedPlanGovernedRoll(t *testing.T, binary string) {
	t.Helper()
	f := startSignedRollFixture(t, binary, true)
	plan := clusterupgrade.Plan{}
	for _, p := range f.processes {
		plan.Steps = append(plan.Steps, clusterupgrade.Step{Kind: clusterupgrade.StepUpgrade, NodeID: p.id, BrokerOnly: true})
	}
	cmd := f.command(t)
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := driveUpgrade(cmd, f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, ""); err != nil {
		t.Fatalf("roll HALTED: %v\n%s", err, out.String())
	}
	for _, r := range f.wire() {
		if r.Op == "reexec-agent" || r.Target == "agent-1" {
			t.Fatalf("the driver dispatched an agent leg the PLAN marked broker-only: %+v", r)
		}
	}
	if !strings.Contains(out.String(), "broker-only host") {
		t.Errorf("a broker-only host must say so in the roll output; got:\n%s", out.String())
	}
	if actual := signedRollSHA(t, fmt.Sprintf("/proc/%d/exe", f.agent.cmd.Process.Pid)); actual != signedRollSHA(t, binary) {
		t.Fatal("plan-governed drive changed its unplanned agent")
	}
	f.assertImages(t)
	f.assertParent(t)
}

// Scope remains independent of the older agent-absence fact. The two skip
// entries are still participants; they must not silently become broker cuts.
// origin: hot-update S8 plan §4.2 / §5.1 / §9.4; S8 internal review upgrade-maketest-2 F5
func exerciseSignedBrokerDaemonRoll(t *testing.T, binary string) {
	t.Helper()
	// A saved release-label skip over old running bytes must fail before any
	// parent or cut is accepted. Preserve this original stale-source premise
	// as an explicit counterexample alongside the one-authorized-cut success.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2 / IL14
	t.Run("stale-skip", func(t *testing.T) {
		stale := startSignedRollFixture(t, binary, true)
		plan := clusterupgrade.Plan{Scope: clusterupgrade.ScopeBrokerDaemon, Steps: []clusterupgrade.Step{{Kind: clusterupgrade.StepUpgrade, NodeID: "brk1"}, {Kind: clusterupgrade.StepSkip, NodeID: "brk2"}, {Kind: clusterupgrade.StepSkip, NodeID: "brk3"}}}
		err := driveUpgrade(stale.command(t), stale.nc, stale.actor, stale.seed, plan, signedRollRelease, stale.sha, "")
		if err == nil || !strings.Contains(err.Error(), "saved skip participant is not running the target image") {
			t.Fatalf("stale skip was not refused: %v", err)
		}
		for _, r := range stale.wire() {
			if r.Inner == "upgrade.start" || r.Inner == "roll.prepare_parent" {
				t.Fatalf("stale skip acquired execution authority: %+v", r)
			}
		}
	})
	f := startSignedRollFixture(t, binary, true, "brk2", "brk3")
	for _, p := range f.processes[1:] {
		if actual := signedRollSHA(t, fmt.Sprintf("/proc/%d/exe", p.cmd.Process.Pid)); actual != f.sha {
			t.Fatalf("skip participant %s runs %s, want %s", p.id, actual, f.sha)
		}
	}
	// The requested daemon and its real co-located agent share brk1. Pin the
	// cold fixture's leadership through the normal signed transfer before taking
	// the roll trace, so the original one-transfer assertion remains nonvacuous.
	f.makeLeader(t, "brk1")
	before := len(f.wire())
	plan := clusterupgrade.Plan{Scope: clusterupgrade.ScopeBrokerDaemon, Steps: []clusterupgrade.Step{{Kind: clusterupgrade.StepUpgrade, NodeID: "brk1", TransferTo: "brk3"}, {Kind: clusterupgrade.StepSkip, NodeID: "brk2"}, {Kind: clusterupgrade.StepSkip, NodeID: "brk3"}}}
	if err := driveUpgrade(f.command(t), f.nc, f.actor, f.seed, plan, signedRollRelease, f.sha, ""); err != nil {
		t.Fatalf("broker scope roll: %v", err)
	}
	agentCalls, transfers, cuts := 0, 0, 0
	holder := ""
	for _, r := range f.wire()[before:] {
		if r.Op == "reexec-agent" || r.Op == "node-list" || r.Target == "agent-1" {
			agentCalls++
		}
		if r.Op == "transfer-leader" {
			transfers++
		}
		if r.Inner == "upgrade.start" {
			cuts++
		}
		if r.Inner == "roll.reserve_cluster" {
			holder = r.Holder
		}
	}
	owner, err := upgradestage.ProcessOwner(f.agent.cmd.Process.Pid)
	if actual := signedRollSHA(t, fmt.Sprintf("/proc/%d/exe", f.agent.cmd.Process.Pid)); err != nil || owner != f.agent.original || actual != signedRollSHA(t, binary) {
		t.Fatal("broker daemon roll changed its co-located agent")
	}
	if agentCalls != 0 || transfers != 1 || cuts != 1 || holder == "" {
		t.Fatalf("scope/holder/transfer: agents=%d transfers=%d reloads=%d holder=%q", agentCalls, transfers, cuts, holder)
	}
}

func (f *signedRollFixture) makeLeader(t *testing.T, target string) {
	t.Helper()
	rows, err := cli.QueryBrokerRollHealthForVoters(t.Context(), f.nc, f.actor, []string{"brk1", "brk2", "brk3"})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if !row.WritableLeaderConfirmed || row.NodeID == target {
			continue
		}
		resp, err := sendUpgradeTriggerOnce(t.Context(), f.nc, f.actor, f.seed, &proto.ClusterUpgradeReq{Op: "transfer-leader", TargetNode: row.NodeID, TransferTo: target})
		if err != nil || resp == nil || !resp.OK {
			t.Fatalf("cold setup leadership transfer: %v %+v", err, resp)
		}
	}
	f.await(t, 20*time.Second, func() bool {
		rows, err := cli.QueryBrokerRollHealthForVoters(t.Context(), f.nc, f.actor, []string{"brk1", "brk2", "brk3"})
		if err != nil {
			return false
		}
		for _, row := range rows {
			if row.NodeID == target && row.WritableLeaderConfirmed {
				return true
			}
		}
		return false
	})
	done := make(chan struct{})
	if err := f.nc.Barrier(func() { close(done) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

// The original counterexamples documented the following failure mechanisms.
// Their former scripted responders are replaced by the product fixture above.
// A deliberately SLOW health responder: it holds the roll inside waitLeader long enough for the
// compressed renew cadence to fire many times.
// Refuse, so the roll HALTs — the abandonment path.
// One step that transfers leadership first: acquire-lock → (keeper starts) → waitLeader spins on the
// slow health probe → transfer-leader refused → HALT.
// The HALT path must have stopped the keeper (driveUpgrade defers Stop on every return). If it had
// not, renewals would keep arriving after the function returned — an abandoned roll whose lock never
// decays, i.e. the permanent fence, restored.
// The field symptom: brk1=old / brk2=new / brk3=old. Assert the whole cluster converged.
// Idempotency / convergence: re-planning against the post-roll state must now find NOTHING to do.
// Before P3 this stayed at 3 forever, so `cluster upgrade` could never report completion and the
// stale-lock self-heal inside `plan.Upgrades()==0` was unreachable on agentless hosts.
// brk1 DECLARES a co-located agent; brk2 declares none and has none registered.
// A live probe WOULD see an agent here (node_id==nid). The plan says broker-only; the plan must win.
// A wrong agent leg must fail promptly rather than waiting for the
// fake agent's unchanged version. The plan still decides admission.
// Every broker shares the target version but is running old bytes. Product
// scope must not rely on AgentAbsent, and the old transfer hint is ignored.
// Keep all configured voters in the plan, including already-complete hosts.
