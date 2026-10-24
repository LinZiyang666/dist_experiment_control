package clusterroll

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review cluster-roll F4
func TestClusterResumeLoadsOnlyTheRecordedParentRoute(t *testing.T) {
	home := t.TempDir()
	parent := upgradetx.Grant{Handle: upgradetx.Handle{TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 1}, ActorFP: "SHA256:actor"}
	parent.Handle.Key = upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfiguredNID: "original-coordinator@roll", ConfigPath: "/fixture/broker.yaml"}
	if err := upgradecli.RememberGrant(home, parent); err != nil {
		t.Fatal(err)
	}
	calls := 0
	ports := ClusterPorts{Home: home, ActorFP: parent.ActorFP, Trigger: func(_ context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		calls++
		var request rollcontrol.Request
		if err := json.Unmarshal(r.Upgrade, &request); err != nil {
			t.Fatal(err)
		}
		if r.TargetNode != "original-coordinator" || r.ActorFP != parent.ActorFP || request.Op != "roll.plan" || request.Parent != parent || request.Expected != parent.Handle {
			t.Fatalf("resume replanned or changed route: %+v", request)
		}
		out, _ := json.Marshal(rollcontrol.Response{Code: upgradetx.CodeStateCorrupt, Detail: "original plan is corrupt"})
		return &proto.ClusterUpgradeResp{Code: upgradetx.CodeStateCorrupt, Upgrade: out}, nil
	}}
	if _, err := ResumeCluster(t.Context(), ports, parent.Handle.TxID); upgradecli.ExitCode(err) != 74 || calls != 1 {
		t.Fatalf("resume did not retain the original journal refusal: calls=%d err=%v", calls, err)
	}
	ports.ActorFP = "another-actor"
	if _, err := ResumeCluster(t.Context(), ports, parent.Handle.TxID); err == nil || calls != 1 {
		t.Fatal("another actor reached the parent journal")
	}
}

// origin: S8 internal review cluster-roll F9
func TestClusterPreflightPrecedesTheFixedMaintenanceHold(t *testing.T) {
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: "domain", ConfigPath: "/fixture/broker.yaml"}, TxID: strings.Repeat("a", 32)}, ParentSlot: "node:original-slot"}
	called := 0
	c := &clusterClient{plan: upgradetx.Plan{Steps: []upgradetx.Step{{Grant: grant}}}, ports: ClusterPorts{Health: func(context.Context) ([]proto.ClusterHealthResp, error) {
		t.Fatal("maintenance/topology started before inert preflight was accepted")
		return nil, nil
	}, Trigger: func(_ context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		called++
		var request rollcontrol.Request
		if err := json.Unmarshal(r.Upgrade, &request); err != nil {
			t.Fatal(err)
		}
		if request.Op != "roll.preflight" || request.Step.Grant != grant {
			t.Fatal("preflight changed the original grant")
		}
		raw, _ := json.Marshal(rollcontrol.Response{Code: upgradetx.CodeDeferred, Detail: "fixture preflight refusal"})
		return &proto.ClusterUpgradeResp{Code: upgradetx.CodeDeferred, Upgrade: raw}, nil
	}}}
	if _, err := c.startBroker(t.Context(), grant, upgradeartifact.Receipt{}); err == nil || called != 1 {
		t.Fatalf("preflight refusal did not stop the cut: calls=%d err=%v", called, err)
	}
}
