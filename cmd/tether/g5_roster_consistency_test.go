package main

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// g5_roster_consistency_test.go — round3 B1 companion: the version-agnostic responder↔roster consistency
// check must REJECT a responder absent from the roster (the reviewer's regression), but must NOT
// over-refuse a responder the roster KNOWS (e.g. a pre-G5 learner answering health) — only voters are
// rolled, learners are simply not in the plan.

func TestBuildUpgradeNodesAllowsRosterKnownPreG5Learner(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	seed, pub := cliExternalAccount(t)
	now := time.Now().UTC()
	// Roster KNOWS both brokers: brk-a is a VOTER, brk-b is a CATCHING_UP learner.
	roster, err := clusterroster.Build(seed, pub, 1, []proto.RosterBroker{
		{NodeID: "brk-a", PublicHost: "a.example.com", Phase: proto.RosterPhaseVoter},
		{NodeID: "brk-b", PublicHost: "b.example.com", Phase: proto.RosterPhaseCatchUp},
	}, now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: roster})
	actor, sid := "Uactor-g5-learner", "lab"
	mustSub(t, nc, proto.SubjCtrlClusterRoster(actor), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(manifest)
		}
	})
	mustSub(t, nc, proto.SubjCtrlNodeList(actor, sid), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeListResp{})
		_ = m.Respond(body)
	})
	mustSub(t, nc, proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		// Both answer health; brk-b is pre-G5 (no IsVoter field). Both are in the roster, so neither is
		// "unexpected" — the plan simply omits the non-voter learner.
		a, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-a", ReleaseVersion: "v1", IsVoter: true})
		b, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-b", ReleaseVersion: "v1"})
		_ = m.Respond(a)
		_ = m.Respond(b)
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	nodes, err := buildUpgradeNodes(context.Background(), nc, actor, sid, pub, io.Discard)
	if err != nil {
		t.Fatalf("a roster-known learner answering health must NOT be refused: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != "brk-a" || !nodes[0].Voter {
		t.Fatalf("plan must contain exactly the voter brk-a (learners are not rolled), got %+v", nodes)
	}
}

func mustSub(t *testing.T, nc *nats.Conn, subj string, cb nats.MsgHandler) {
	t.Helper()
	if _, err := nc.Subscribe(subj, cb); err != nil {
		t.Fatalf("subscribe %s: %v", subj, err)
	}
}
