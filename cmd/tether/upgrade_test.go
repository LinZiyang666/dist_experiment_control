package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// The S8 transaction verbs are siblings of `node upgrade`, never its children:
// a node legitimately named "status", "wait" or "rollback" must still be the
// positional NID of `node upgrade`, and each verb takes the NID positionally.
// origin: hot-update S8 plan §7.1 / DC06 / §11.2 P19 ("NID=status taken" mutant)
func TestNodeTransactionVerbsKeepEveryNIDAddressable(t *testing.T) {
	root := newRootCmd()
	upgrade, _, err := root.Find([]string{"node", "upgrade"})
	if err != nil || upgrade.Name() != "upgrade" || len(upgrade.Commands()) != 0 {
		t.Fatalf("`node upgrade` grew subcommands that can capture a NID: %v %v", err, upgrade.Commands())
	}
	verbs := []string{"status", "wait", "rollback", "cancel", "archive"}
	for _, nid := range append(append([]string{}, verbs...), "upgrade-status", "help-me") {
		argv := []string{"node", "upgrade", nid, "--tx-id", strings.Repeat("a", 32), "--artifact-receipt", "r.json"}
		cmd, rest, err := root.Find(argv)
		if err != nil || cmd != upgrade || len(rest) == 0 || rest[0] != nid {
			t.Fatalf("NID %q was captured by %q (rest %v, err %v)", nid, cmd.CommandPath(), rest, err)
		}
		if err := cmd.ParseFlags(rest); err != nil || cmd.Flags().Arg(0) != nid {
			t.Fatalf("NID %q did not reach `node upgrade` positionally: %v %v", nid, err, cmd.Flags().Args())
		}
		for _, verb := range verbs {
			sibling, rest, err := root.Find([]string{"node", "upgrade-" + verb, nid, "--tx-id", strings.Repeat("a", 32)})
			if err != nil || sibling.Name() != "upgrade-"+verb || sibling.Parent().Name() != "node" || len(rest) == 0 || rest[0] != nid {
				t.Fatalf("`node upgrade-%s %s` did not keep the NID positional: %v %v", verb, nid, rest, err)
			}
			if flag := sibling.Flags().Lookup("tx-id"); flag == nil {
				t.Fatalf("`node upgrade-%s` lacks --tx-id", verb)
			}
		}
	}
}

// A release string is never a skip witness (S8 plan §4.2 / IL14): the planner
// keeps every agent-bearing host in a whole-host roll and only running-image
// proof skips a broker; the executor always runs the agent leg.
// origin: hot-update S8 plan §4.2 / IL14 / §11.2 P19 ("skip by release" mutant)
func TestClusterRollNeverSkipsByRelease(t *testing.T) {
	sha := strings.Repeat("a", 64)
	nodes := []clusterupgrade.Node{{ID: "brk1", Voter: true, IsLeader: true, CaughtUp: true, BrokerVer: "v2", AgentVer: "v2", Agent: clusterupgrade.AgentPresent}}
	running := []proto.ClusterHealthResp{{NodeID: "brk1", BrokerImage: &proto.BrokerImageStatus{RunningSHA256: sha, Generation: 4, Phase: "serving", Mode: "single"}}}
	if p := cli.PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeWholeHost, "", nil); p.Upgrades() != 1 {
		t.Fatalf("a release-equal host was skipped without any running-image proof: %+v", p)
	}
	if p := cli.PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeWholeHost, sha, running); p.Upgrades() != 1 {
		t.Fatalf("an agent-bearing host was skipped because its agent reports the target release: %+v", p)
	}
	if p := cli.PlanClusterRoll(nodes, "v2", clusterupgrade.ScopeBrokerDaemon, sha, running); p.Upgrades() != 0 {
		t.Fatalf("running-image proof no longer skips an exact broker: %+v", p)
	}

	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	seed, _ := cliExternalAccount(t)
	const actor, sid = "Uactor-s8-release-skip", "sid-release"
	var mu sync.Mutex
	var reexecs []string
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk1", ColocatedAgentNID: "agent-1", WritableLeaderConfirmed: true, LeaderID: "brk1", IsVoter: true})
		_ = nc.Publish(m.Reply, body)
	})
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, sid), func(m *nats.Msg) {
		// The agent ALREADY reports the target release before its leg runs.
		body, _ := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "agent-1", Status: "ONLINE", ReleaseVersion: "v2"}}})
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterUpgrade(actor), func(m *nats.Msg) {
		var req proto.ClusterUpgradeReq
		_ = json.Unmarshal(m.Data, &req)
		if req.Op == "reexec-agent" {
			mu.Lock()
			reexecs = append(reexecs, req.TargetNode)
			mu.Unlock()
		}
		body, _ := json.Marshal(proto.ClusterUpgradeResp{OK: true})
		_ = m.Respond(body)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	step := clusterupgrade.Step{Kind: clusterupgrade.StepUpgrade, NodeID: "brk1"}
	if err := driveAgentUpgradeStep(t.Context(), nc, actor, sid, seed, step, clusterupgrade.ScopeWholeHost, "", "v2", sha, "holder", &out); err != nil {
		t.Fatalf("agent leg failed: %v\n%s", err, out.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reexecs) != 1 || reexecs[0] != "brk1" || strings.Contains(out.String(), "already at") {
		t.Fatalf("the agent leg was skipped because the agent reported the target release: reexecs=%v\n%s", reexecs, out.String())
	}
}

// upgrade_test.go (formerly g5_upgrade_test.go) — G5 #13/#14 W4: the testable orchestrator cores. The live drive loop (signed
// triggers over NATS + converge waits) is exercised by the N=3 sim drill; here we pin the dry-run plan
// render and — critically — that the ctl signer produces a signature the broker verifier accepts.

func TestRenderUpgradePlan(t *testing.T) {
	var buf bytes.Buffer
	plan := clusterupgrade.Plan{
		Steps: []clusterupgrade.Step{
			{Kind: clusterupgrade.StepSkip, NodeID: "a"},
			{Kind: clusterupgrade.StepUpgrade, NodeID: "c"},
			{Kind: clusterupgrade.StepUpgrade, NodeID: "b", TransferTo: "c"},
		},
		N2WriteFence: true,
	}
	renderUpgradePlan(&buf, plan, "v2")
	s := buf.String()
	for _, want := range []string{"SKIP    a", "UPGRADE c", "transfer to c first", "N=2"} {
		if !strings.Contains(s, want) {
			t.Fatalf("plan render missing %q:\n%s", want, s)
		}
	}
}

// TestSignUpgradeTriggerVerifiable is the cross-package auth contract: the ctl signer (W4) must produce a
// signature the broker responder (W2b) accepts, and any field tamper must break it.
func TestSignUpgradeTriggerVerifiable(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", ToVersion: "v2", ExpectSHA256: "abc"}
	data, err := signUpgradeTrigger(seed, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var parsed proto.ClusterUpgradeReq
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.IssuedAt == "" {
		t.Fatal("the signer must stamp issued_at (replay bound)")
	}
	sig, err := hex.DecodeString(parsed.Sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifySignature(pub, proto.CanonicalUpgradeReqBytes(&parsed), sig); err != nil {
		t.Fatalf("the ctl signer must produce a broker-verifiable signature: %v", err)
	}
	parsed.ToVersion = "v3-tampered"
	if auth.VerifySignature(pub, proto.CanonicalUpgradeReqBytes(&parsed), sig) == nil {
		t.Fatal("a field tampered after signing must invalidate the signature")
	}
}
